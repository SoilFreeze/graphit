import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import time
import re
import requests

# ===============================================================
# 1. CONFIGURATION, GLOBAL CONSTANTS & SESSION STATE
# ===============================================================
def initialize_app():
    """Sets up page config and global session state variables."""
    st.set_page_config(page_title="SF Engineering Admin", page_icon="🛠️", layout="wide")
    
    if 'unit_mode' not in st.session_state:
        st.session_state['unit_mode'] = "Fahrenheit"
    
    # Unified core data paths to prevent variable drift inside main()
    return "Temperature", "sensorpush-export"

DATASET_ID, PROJECT_ID = initialize_app()
display_tz = "America/Los_Angeles" 

# ===============================================================
# 2. DATABASE CLIENT
# ===============================================================
@st.cache_resource
def get_bq_client():
    """Initializes and returns the BigQuery client."""
    try:
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(info)
            return bigquery.Client(credentials=credentials, project=info["project_id"])
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"❌ Database Link Offline: {e}")
        return None

client = get_bq_client()

# ===============================================================
# 3. NAVIGATION & CONTEXT ENGINE
# ===============================================================
def render_sidebar():
    """Renders main command panel controls and saves target site profile parameters."""
    st.sidebar.title("🛠️ Admin Command Center")
    
    admin_page = st.sidebar.radio(
        "Management Tool", 
        [
            "🛠️ Node Manager",      
            "📡 Setup Node Tool", 
            "🔍 Sensor Status",
            "📦 Bulk Registry Manager",
            "📡 Data Recovery",
            "⚙️ Project Master", 
            "📈 Ref Curve Library", 
            "🧨 Data Management"
        ],
        key="main_admin_nav"
    )
    
    is_dev = st.sidebar.toggle("🧪 Use Registry Playground", value=False)
    target_registry = f"{PROJECT_ID}.{DATASET_ID}.node_registry" + ("_dummy" if is_dev else "")

    # Fetch available active validation spaces
    proj_q = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE ProjectStatus != 'Archived'"
    try:
        proj_df = client.query(proj_q).to_dataframe()
        proj_list = sorted(proj_df['Project'].tolist())
        
        selected_project = st.sidebar.selectbox("🎯 Target Project Context", proj_list)
        
        if not proj_df.empty:
            metadata = proj_df[proj_df['Project'] == selected_project].iloc[0].to_dict()
            st.session_state['project_metadata'] = metadata
            
        return admin_page, target_registry, selected_project, proj_list
    except Exception as e:
        st.sidebar.error(f"Error loading projects: {e}")
        return admin_page, target_registry, "None", ["Office"]

# ===============================================================
# 4. GLOBAL UTILITIES & CORE LOADERS
# ===============================================================
@st.cache_data(ttl=600)
def load_registry_data(target_table):
    """Queries active schema inventory data safely."""
    try:
        df = client.query(f"SELECT * FROM `{target_table}`").to_dataframe()
        if 'PhysicalID' in df.columns:
            df['PhysicalID'] = df['PhysicalID'].astype(str).replace(['nan', 'None', '<NA>'], '')
        return df
    except Exception as e:
        st.error(f"Error loading registry: {e}")
        return pd.DataFrame()

def get_trend_arrow(current, previous):
    if pd.isnull(current) or pd.isnull(previous): 
        return "N/A"
    delta = current - previous
    if delta > 0.1: return f"🔺 +{delta:.1f}"
    if delta < -0.1: return f"🔹 {delta:.1f}"
    return "➡️ 0.0"

def fmt_temp(val, unit_mode, unit_label):
    if pd.isnull(val): 
        return "N/A"
    v = (val - 32) * 5/9 if unit_mode == "Celsius" else val
    return f"{v:.1f}{unit_label}"

def get_unit_labels():
    unit_mode = st.session_state['unit_mode']
    unit_label = "°C" if unit_mode == "Celsius" else "°F"
    return unit_mode, unit_label
    
def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower()
            for text in re.split('([0-9]+)', str(s))]
    
# ===============================================================
# Function: Status Dashboard
# ===============================================================
def render_project_status_dashboard(client, selected_project, unit_label, target_registry):
    st.subheader("📊 Project Status Summary")
    
    # Updated to use dynamic target_registry from sidebar
    query = f"""
        SELECT 
            n.NodeNum, n.Bank, n.Location, n.Depth,
            CASE 
                WHEN (n.Bank LIKE 'S%' OR n.Location LIKE 'S%') AND (n.Bank NOT LIKE '%Amb%' AND n.Location NOT LIKE '%Amb%') THEN 'Supply'
                WHEN (n.Bank LIKE 'R%' OR n.Location LIKE 'R%') AND (n.Bank NOT LIKE '%Amb%' AND n.Location NOT LIKE '%Amb%') THEN 'Return'
                WHEN (n.Bank LIKE '%Amb%' OR n.Location LIKE '%Amb%') THEN 'Ambient'
                WHEN n.Depth IS NOT NULL THEN 'TempPipes'
                ELSE 'Other'
            END as hardware_type,
            AVG(CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN m.temperature END) as avg_now,
            MIN(CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN m.temperature END) as min_now,
            MAX(CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN m.temperature END) as max_now,
            MIN(CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN m.temperature END) as min_24h,
            MAX(CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN m.temperature END) as max_24h,
            AVG(CASE WHEN m.timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN m.temperature END) as avg_1h_prev,
            AVG(CASE WHEN m.timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR) THEN m.temperature END) as avg_6h_prev,
            COUNTIF(m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) as pings_24h,
            ARRAY_AGG(m.temperature ORDER BY m.timestamp DESC LIMIT 1)[OFFSET(0)] as latest_temp,
            MAX(m.timestamp) as latest_ts
        FROM `{target_registry}` n
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.master_data_view` m ON n.NodeNum = m.NodeNum
        WHERE n.Project = @proj_id AND n.End_Date IS NULL
        GROUP BY 1, 2, 3, 4, 5
    """
    
    try:
        df = client.query(query, job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)]
        )).to_dataframe()
    except Exception as e:
        st.error(f"Dashboard Query Failed: {e}")
        return

    if df.empty:
        st.info("No active nodes found for dashboard summary.")
        return

    cols = st.columns(4)
    type_map = {"Supply": (cols[0], "📥"), "Return": (cols[1], "📤"), "TempPipes": (cols[2], "📏"), "Ambient": (cols[3], "☁️")}
    now_utc = pd.Timestamp.now(tz='UTC')

    for h_type, (col, icon) in type_map.items():
        g_df = df[df['hardware_type'] == h_type]
        with col:
            st.markdown(f"#### {icon} {h_type}")
            if g_df.empty or g_df['latest_ts'].isna().all():
                st.caption("No recent data")
                continue
            
            # Robust Timestamp Comparison
            latest_time = g_df['latest_ts'].max()
            if latest_time.tzinfo is None:
                latest_time = latest_time.tz_localize('UTC')
            
            lag_hrs = (now_utc - latest_time).total_seconds() / 3600
            val = g_df['avg_now'].mean() if pd.notnull(g_df['avg_now'].mean()) else g_df['latest_temp'].mean()
            
            if lag_hrs > 1.1: 
                st.subheader(f"⚠️ Offline {int(lag_hrs)}h")
            else: 
                st.title(f"{val:.1f}{unit_label}")
            
            # Simplified Summary Stats
            active_1h = int(g_df['avg_now'].notnull().sum())
            active_24h = int((g_df['pings_24h'] > 0).sum())
            st.write(f"**{active_1h}/{len(g_df)}** (1h) | **{active_24h}/{len(g_df)}** (24h)")
            
            st.caption(f"Cur: {g_df['min_now'].min():.1f} to {g_df['max_now'].max():.1f}{unit_label}")
            st.caption(f"24h: {g_df['min_24h'].min():.1f} to {g_df['max_24h'].max():.1f}{unit_label}")
            
            t_row = st.columns(2)
            t_row[0].caption(f"1h\n{get_trend_arrow(val, g_df['avg_1h_prev'].mean())}")
            t_row[1].caption(f"6h\n{get_trend_arrow(val, g_df['avg_6h_prev'].mean())}")
            
# ===============================================================
# Function: Hardware integrity table
# ===============================================================
def render_hardware_integrity_table(client, selected_project, unit_mode, unit_label, target_registry):
    """
    Renders a detailed table showing connectivity, coverage, and recent activity.
    Now includes natural sorting for improved readability.
    """
    st.subheader("📋 Hardware Integrity & Connectivity")
    
    query = f"""
        SELECT 
            n.NodeNum, n.Location, n.Bank, n.Depth, n.SensorStatus,
            MAX(m.timestamp) as last_ping,
            ARRAY_AGG(m.temperature ORDER BY m.timestamp DESC LIMIT 1)[OFFSET(0)] as last_temp,
            COUNTIF(m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) as pings_1h,
            COUNTIF(m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)) as pings_6h,
            COUNTIF(m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) as pings_24h,
            (COUNT(DISTINCT CASE 
                WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) 
                THEN TIMESTAMP_TRUNC(m.timestamp, HOUR) 
             END) / 24.0) * 100 as coverage_24h,
            AVG(CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN m.temperature END) as avg_now,
            AVG(CASE WHEN m.timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN m.temperature END) as avg_1h_prev
        FROM `{target_registry}` n
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.master_data_view` m ON n.NodeNum = m.NodeNum
        WHERE n.Project = @proj_id AND n.End_Date IS NULL
        GROUP BY 1, 2, 3, 4, 5
    """
    
    df = client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)]
    )).to_dataframe()

    if df.empty: 
        st.info("No active nodes found for connectivity table.")
        return

    # Natural Sorting Logic
    df['bank_sort'] = df['Bank'].apply(lambda x: tuple(natural_sort_key(x)))
    df = df.sort_values(by=['Location', 'bank_sort', 'Depth']).drop(columns=['bank_sort'])

    now_utc = pd.Timestamp.now(tz='UTC')

    def row_processor(row):
        ping = row['last_ping']
        if pd.isnull(ping):
            txt, style = "❌ Never", "background-color: #d3d3d3"
        else:
            ts = ping if ping.tzinfo else ping.tz_localize('UTC')
            diff = (now_utc - ts).total_seconds() / 60
            if diff <= 15: 
                txt, style = f"{int(diff)}m ago", "background-color: #ccffcc; color: black"
            elif diff <= 60: 
                txt, style = f"{int(diff)}m ago", "background-color: #ffe4b5; color: black"
            else: 
                txt, style = f"{round(diff/60, 1)}h ago", "background-color: #ffcccb; color: black"
        
        pos = f"{row['Depth']}ft" if (pd.notnull(row['Depth']) and row['Depth'] != 0) else f"Bank {row['Bank']}"
        trend = get_trend_arrow(row['avg_now'], row['avg_1h_prev'])
        return pd.Series([txt, style, pos, trend])

    df[['Seen_Text', 'Seen_Style', 'Pos_Label', 'Trend']] = df.apply(row_processor, axis=1)

    display_df = pd.DataFrame({
        "Node ID": df['NodeNum'],
        "Location": df['Location'],
        "Position": df['Pos_Label'],
        "Last Seen": df['Seen_Text'],
        "24h Coverage": df['coverage_24h'].apply(lambda x: f"{x:.1f}%"),
        "1h Change": df['Trend'],
        "Last Temp": df['last_temp'].apply(lambda x: fmt_temp(x, unit_mode, unit_label)),
        "1h Pings": df['pings_1h'],
        "6h Pings": df['pings_6h'],
        "24h Pings": df['pings_24h']
    })

    def diagnostic_styler(data):
        style_df = pd.DataFrame('', index=data.index, columns=data.columns)
        ref = df.reset_index(drop=True)
        for i, row in data.iterrows():
            if ref.loc[i, 'SensorStatus'] == 'Diagnostic':
                style_df.loc[i, 'Node ID'] = 'background-color: #ff4b4b; color: white; font-weight: bold;'
            style_df.loc[i, 'Last Seen'] = ref.loc[i, 'Seen_Style']
        return style_df

    st.dataframe(
        display_df.style.apply(diagnostic_styler, axis=None), 
        use_container_width=True, 
        hide_index=True
    )

# ===============================================================
# PAGE MODULE: 🛠️ NODE MANAGER
# ===============================================================

def render_node_selector(reg_df, proj_list):
    """
    Renders an active inventory node selection engine.
    """
    st.subheader("🎯 Active Node Registry")
    
    hide_archived = st.checkbox("Hide Archived Records", value=True, key="ns_hide_archived_toggle")
    
    df = reg_df.copy()
    if hide_archived:
        df = df[
            (df['SensorStatus'].str.lower() != "archived") & 
            (df['Location'].str.contains("Archive", case=False, na=False) == False)
        ]

    # Layout Filter Row
    c1, c2, c3 = st.columns(3)
    with c1:
        f_proj = st.selectbox("Filter by Project Space", ["All", "Unassigned"] + proj_list, key="ns_proj_f")
    with c2:
        if f_proj == "All":
            loc_opts = df['Location'].dropna().unique().tolist()
        elif f_proj == "Unassigned":
            loc_opts = df[df['Project'].isna() | (df['Project'] == "") | (df['Project'] == "Office") | (df['Location'] == "Office Stock")]['Location'].dropna().unique().tolist()
        else:
            loc_opts = df[df['Project'] == f_proj]['Location'].dropna().unique().tolist()
            
        f_loc = st.selectbox("Filter by Physical Location", ["All"] + sorted(loc_opts), key="ns_loc_f")
    with c3:
        search_term = st.text_input("Global Search (Node ID)", "", key="ns_search_f")

    # Execute Cascading Filters
    if f_proj == "Unassigned":
        df = df[df['Project'].isna() | (df['Project'] == "") | (df['Project'] == "Office")]
    elif f_proj != "All":
        df = df[df['Project'] == f_proj]
        
    if f_loc != "All":
        df = df[df['Location'] == f_loc]
        
    if search_term:
        df = df[df['NodeNum'].str.contains(search_term, case=False, na=False)]

    if df.empty:
        st.info("No matching nodes located under current filter parameters.")
        return None

    # Render interactive row choosing engine via checkboxes
    df.insert(0, "Select", False)
    edited_df = st.data_editor(
        df,
        hide_index=True,
        use_container_width=True,
        column_config={"Select": st.column_config.CheckboxColumn("Select", default=False, required=True)},
        disabled=[col for col in df.columns if col != "Select"],
        key="node_registry_editor"
    )

    selected_rows = edited_df[edited_df["Select"] == True]
    if not selected_rows.empty:
        return selected_rows.iloc[0].drop("Select").to_dict()
    
    return None


def render_node_historical_graph(client, node_id):
    """Fetches and displays the trailing 7-day thermal chart for the chosen node context."""
    st.markdown(f"### 📈 Trailing Thermal Trace: **{node_id}**")
    
    hist_q = f"""
        SELECT timestamp, temperature 
        FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` 
        WHERE NodeNum = '{node_id}' 
          AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        ORDER BY timestamp ASC
    """
    try:
        with st.spinner("Retrieving telemetric history logs..."):
            tel_df = client.query(hist_q).to_dataframe()
        
        if not tel_df.empty:
            fig = go.Figure(go.Scatter(
                x=tel_df['timestamp'], 
                y=tel_df['temperature'], 
                mode='lines', 
                line=dict(color='#00d4ff', width=2),
                name="Thermal Curve"
            ))
            fig.update_layout(
                height=250, 
                template="plotly_dark", 
                margin=dict(l=20, r=20, t=10, b=20),
                xaxis_title="Timeline Logs",
                yaxis_title="Temperature"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.caption("ℹ️ No telemetric data markers discovered for this hardware footprint in the last 7 days.")
    except Exception as e:
        st.error(f"Failed generating historical context graph: {e}")


def render_node_action_manager(client, selected_node_data, reg_df, proj_list, target_registry):
    """
    Displays chart, interactive historical log selector, full attribute configuration overrides,
    operational task panels, and administrative pipeline delete tools.
    """
    node_id = selected_node_data['NodeNum']
    start_dt = selected_node_data['Start_Date']

    # 1. SHOW THE GRAPH
    render_node_historical_graph(client, node_id)
    st.divider()

    # 2. CHOOSE THE HISTORIC ASSIGNMENT TO ALTER
    st.markdown(f"### 📜 Assignment History Library: **{node_id}**")
    st.info("💡 Check the box next to any assignment below (active or archived) to populate and alter its fields in the editor.")
    
    history_df = reg_df[reg_df['NodeNum'] == node_id].sort_values(by='Start_Date', ascending=False).copy()
    history_df.insert(0, "Edit Target", False)
    
    edited_hist_df = st.data_editor(
        history_df,
        hide_index=True,
        use_container_width=True,
        column_config={"Edit Target": st.column_config.CheckboxColumn("Edit Target", default=False, required=True)},
        disabled=[col for col in history_df.columns if col != "Edit Target"],
        key=f"hist_editor_{node_id}"
    )
    
    chosen_rows = edited_hist_df[edited_hist_df["Edit Target"] == True]
    
    if not chosen_rows.empty:
        target_record = chosen_rows.iloc[0].drop("Edit Target").to_dict()
        st.success(f"✏️ Currently Editing Chosen Assignment row starting on: `{target_record['Start_Date']}`")
    else:
        target_record = selected_node_data
        st.info(f"✏️ Currently Editing Active Assignment row starting on: `{target_record['Start_Date']}`")
        
    st.divider()

    # 3. EDITOR WITH HARDENED MUTUAL EXCLUSION (BANK VS DEPTH)
    st.markdown("### 🛠️ Modify Assignment Attributes")
    
    with st.form("global_node_editor_form"):
        col1, col2, col3 = st.columns(3)
        edit_nodenum = col1.text_input("Node ID (NodeNum)", value=str(target_record.get('NodeNum', '')))
        edit_proj = col2.selectbox("Project", [""] + proj_list, index=proj_list.index(target_record['Project']) + 1 if target_record['Project'] in proj_list else 0)
        edit_loc = col3.text_input("Location", value=str(target_record.get('Location', '')))
        
        col4, col5, col6 = st.columns(3)
        edit_bank = col4.text_input("Bank", value=str(target_record.get('Bank', '')) if pd.notnull(target_record.get('Bank')) else "", help="Writing a bank value automatically wipes Depth to NULL.")
        
        raw_depth = target_record.get('Depth')
        edit_depth = col5.number_input("Depth (ft)", value=float(raw_depth) if (pd.notnull(raw_depth) and str(raw_depth).strip() != '') else 0.0)
        
        status_options = ["On Project", "Available", "Diagnostic", "Dead", "Archived"]
        curr_stat = target_record.get('SensorStatus', 'On Project')
        s_idx = status_options.index(curr_stat) if curr_stat in status_options else 0
        edit_status = col6.selectbox("SensorStatus", status_options, index=s_idx)
        
        col7, col8 = st.columns(2)
        edit_start = col7.date_input("Start Date", value=pd.to_datetime(target_record.get('Start_Date')).date() if pd.notnull(target_record.get('Start_Date')) else datetime.now().date())
        edit_end = col8.date_input("End Date", value=pd.to_datetime(target_record.get('End_Date')).date() if pd.notnull(target_record.get('End_Date')) else None)
        
        if st.form_submit_button("💾 Save Changes"):
            if edit_bank.strip() != "":
                sql_depth = "NULL"
            else:
                sql_depth = "NULL" if edit_depth == 0.0 else f"{edit_depth}"
                
            sql_end = f"DATE('{edit_end.isoformat()}')" if edit_end else "NULL"
            sql_bank = f"'{edit_bank.strip()}'" if edit_bank.strip() != "" else "NULL"
            
            update_sql = f"""
                UPDATE `{target_registry}`
                SET NodeNum = '{edit_nodenum.strip()}',
                    Project = '{edit_proj.strip()}',
                    Location = '{edit_loc.strip()}',
                    Bank = {sql_bank},
                    Depth = {sql_depth},
                    SensorStatus = '{edit_status}',
                    Start_Date = DATE('{edit_start.isoformat()}'),
                    End_Date = {sql_end}
                WHERE NodeNum = '{target_record['NodeNum']}' 
                  AND Start_Date = DATE('{pd.to_datetime(target_record['Start_Date']).strftime('%Y-%m-%d')}')
            """
            try:
                client.query(update_sql).result()
                st.success("✅ Configuration modifications saved directly to BigQuery records.")
                st.cache_data.clear()
                time.sleep(1)
                st.rerun()
            except Exception as e:
                st.error(f"Failed to modify chosen timeline record: {e}")

    # 4. OPERATIONAL TASK PANEL
    st.markdown("##### Quick Operational Tasks")
    c_act1, c_act2, c_act3 = st.columns(3)
    
    # --- END ASSIGNMENT ---
    with c_act1.expander("🔚 End Assignment"):
        end_date_input = st.date_input("Decommission Date Selection", value=datetime.now().date(), key="end_assign_dt")
        end_status_input = st.selectbox("Return Stock Status Parameter", ["Available", "Diagnostic", "Dead"], key="end_assign_st")
        
        if st.button("Execute End Assignment", type="primary", use_container_width=True):
            date_iso = end_date_input.isoformat()
            bulk_sql = f"""
                BEGIN TRANSACTION;
                UPDATE `{target_registry}` 
                SET End_Date = DATE('{date_iso}'), SensorStatus = 'Archived' 
                WHERE NodeNum = '{node_id}' AND End_Date IS NULL;
                
                INSERT INTO `{target_registry}` (NodeNum, Project, Location, Bank, Depth, SensorStatus, Start_Date)
                VALUES ('{node_id}', 'Office', 'Office Stock', '{node_id}', NULL, '{end_status_input}', DATE('{date_iso}'));
                COMMIT;
            """
            try:
                client.query(bulk_sql).result()
                st.success(f"✅ Node {node_id} ended and transferred to Office stock records.")
                st.cache_data.clear()
                time.sleep(1)
                st.rerun()
            except Exception as e:
                st.error(f"Transaction execution failed: {e}")

    # --- CHANGE SENSOR ---
    with c_act2.expander("🔄 Change Sensor"):
        swap_node_input = st.text_input("Replacement Node ID (NodeNum)", placeholder="e.g., TP-0105", key="swap_sensor_input")
        swap_date_input = st.date_input("Swap Execution Date", value=datetime.now().date(), key="swap_sensor_dt")
        
        if st.button("Execute Change Sensor", type="primary", use_container_width=True):
            if not swap_node_input.strip():
                st.error("Please insert a valid target hardware replacement ID.")
            elif swap_node_input.strip() == node_id:
                st.error("The replacement Node ID cannot be identical to the sensor currently assigned.")
            else:
                date_str = swap_date_input.isoformat()
                new_node = swap_node_input.strip()
                
                if new_node.upper().startswith("TP"):
                    old_sensor_restock_loc = "Office Stock"
                elif new_node.upper().startswith("SP"):
                    old_sensor_restock_loc = "Ambient Stock"
                else:
                    old_sensor_restock_loc = "Office Stock"

                swap_sql = f"""
                    BEGIN TRANSACTION;
                    
                    -- STEP 1: End the current assignment for the old sensor
                    UPDATE `{target_registry}`
                    SET End_Date = DATE('{date_str}'), 
                        SensorStatus = 'Archived'
                    WHERE NodeNum = '{node_id}' 
                      AND End_Date IS NULL;

                    -- STEP 2: Create a brand-new entry sending the old sensor back to Office as Available
                    INSERT INTO `{target_registry}` (NodeNum, Project, Location, Bank, Depth, SensorStatus, Start_Date)
                    VALUES ('{node_id}', 'Office', '{old_sensor_restock_loc}', '{node_id}', NULL, 'Available', DATE('{date_str}'));

                    -- STEP 3: Terminate the incoming sensor's previous active assignment lineage
                    UPDATE `{target_registry}`
                    SET End_Date = DATE('{date_str}'), 
                        SensorStatus = 'Archived'
                    WHERE NodeNum = '{new_node}' 
                      AND End_Date IS NULL;

                    -- STEP 4: Make a new entry for the new sensor, identical to the old assignment minus dates and nodenum
                    INSERT INTO `{target_registry}` (NodeNum, Project, Location, Bank, Depth, SensorStatus, Start_Date)
                    VALUES (
                        '{new_node}', 
                        '{selected_node_data['Project']}', 
                        '{selected_node_data['Location']}', 
                        {f"'{selected_node_data['Bank']}'" if pd.notnull(selected_node_data.get('Bank')) and selected_node_data.get('Bank') != 'None' else "NULL"}, 
                        {selected_node_data['Depth'] if pd.notnull(selected_node_data.get('Depth')) and str(selected_node_data.get('Depth')).strip() != '' else "NULL"}, 
                        'On Project', 
                        DATE('{date_str}')
                    );
                    
                    COMMIT;
                """
                try:
                    with st.spinner("Processing dual-sensor swap execution matrices..."):
                        client.query(swap_sql).result()
                    st.success(f"🔄 Change Sensor complete: {node_id} returned to {old_sensor_restock_loc}. {new_node} initialized onto deployment timeline successfully.")
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    st.error(f"Sensor change transaction execution routine failed: {e}")
                    st.code(swap_sql, language="sql")

    # --- DELETE ENTRY ---
    with c_act3.expander("🗑️ Delete Entry"):
        st.warning("⚠️ Danger Zone: This drops the targeted assignment entry row completely out of your BigQuery system logs.")
        confirm_check = st.checkbox("Confirm permanent deletion of this row", key=f"del_confirm_{target_record['Start_Date']}")
        
        if st.button("Delete Selected Assignment Record", type="primary", use_container_width=True):
            if not confirm_check:
                st.error("Please click the confirmation checkbox to authorize the database removal transaction.")
            else:
                delete_sql = f"""
                    DELETE FROM `{target_registry}`
                    WHERE NodeNum = '{target_record['NodeNum']}'
                      AND Start_Date = DATE('{pd.to_datetime(target_record['Start_Date']).strftime('%Y-%m-%d')}')
                """
                try:
                    client.query(delete_sql).result()
                    st.warning(f"🗑️ Assignment row deleted for Node {target_record['NodeNum']} starting on {target_record['Start_Date']}.")
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to execute row delete query logic: {e}")

def render_data_checker(client, reg_df):
    """
    Scans node deployment timelines to isolate configuration patterns and pipeline errors.
    """
    st.markdown("---")
    st.subheader("🔍 Data Checker Diagnostics")
    
    c1, c2, c3 = st.tabs([
        "⏱️ Gaps in Data (Missing Office Time)", 
        "🚨 Orphaned Nodes (Missing Next Assignment)",
        "🚨 Multiple / Duplicate Assignments"
    ])
    
    df = reg_df.copy()
    df['Start_Date'] = pd.to_datetime(df['Start_Date']).dt.date
    df['End_Date'] = pd.to_datetime(df['End_Date']).dt.date
    
    grouped = df.groupby('NodeNum')
    
    gaps_in_data = []
    orphaned_nodes = []
    duplicate_assignments = []
    
    for node_id, group in grouped:
        sorted_group = group.sort_values(by='Start_Date')
        records = sorted_group.to_dict('records')
        
        has_gap = False
        is_orphaned = False
        has_duplicate = False
        
        active_count = 0
        
        for i in range(len(records)):
            current_rec = records[i]
            
            if pd.isnull(current_rec['Start_Date']):
                has_gap = True
                continue
            
            if pd.isnull(current_rec['End_Date']):
                active_count += 1
                
            # --- OVERLAPPING / DUPLICATE ASSIGNMENT CHECK ---
            for j in range(i + 1, len(records)):
                compare_rec = records[j]
                if pd.notnull(compare_rec['Start_Date']):
                    # Hardened Overlap Logic: Flags rows where the next assignment begins 
                    # strictly BEFORE the current assignment has ended.
                    if pd.isnull(current_rec['End_Date']) or current_rec['End_Date'] > compare_rec['Start_Date']:
                        has_duplicate = True

            # --- CHRONOLOGICAL GAP & ORPHAN CHECKS ---
            if i < len(records) - 1:
                next_rec = records[i+1]
                if pd.notnull(current_rec['End_Date']) and pd.notnull(next_rec['Start_Date']):
                    if (next_rec['Start_Date'] - current_rec['End_Date']).days > 1:
                        has_gap = True
            else:
                if pd.notnull(current_rec['End_Date']):
                    is_orphaned = True
                    
        # Flagging if multiple rows are left running without an End_Date simultaneously
        if active_count > 1:
            has_duplicate = True

        # Route the Node IDs to their respective diagnostic panels
        if has_duplicate:
            duplicate_assignments.append(node_id)
        if has_gap:
            gaps_in_data.append(node_id)
        elif is_orphaned and not has_duplicate:
            orphaned_nodes.append(node_id)

    # ===============================================================
    # TAB 1: Gaps in Data
    # ===============================================================
    with c1:
        st.markdown("##### Nodes with a chronological gap where they were not assigned—requires unmonitored time to be added to Office")
        if gaps_in_data:
            gap_display_df = df[df['NodeNum'].isin(gaps_in_data)].sort_values(['NodeNum', 'Start_Date'])
            st.dataframe(
                gap_display_df[['NodeNum', 'Project', 'Location', 'Start_Date', 'End_Date', 'SensorStatus']], 
                use_container_width=True, 
                hide_index=True
            )
        else:
            st.success("✅ No timeline gaps or missing 'Office' storage windows detected across node history logs.")

    # ===============================================================
    # TAB 2: Orphaned Nodes
    # ===============================================================
    with c2:
        st.markdown("##### Nodes that have an end date on their last assignment but did not get transferred into a new project or Office stock")
        if orphaned_nodes:
            orphan_display_df = df[df['NodeNum'].isin(orphaned_nodes)].sort_values(['NodeNum', 'Start_Date'])
            last_entries = orphan_display_df.groupby('NodeNum').last().reset_index()
            st.dataframe(
                last_entries[['NodeNum', 'Project', 'Location', 'Start_Date', 'End_Date', 'SensorStatus']], 
                use_container_width=True, 
                hide_index=True
            )
        else:
            st.success("✅ Clean terminations verified. All decommissioned nodes successfully occupy new project profiles or Office stock rows.")

    # ===============================================================
    # TAB 3: MULTIPLE / DUPLICATE ASSIGNMENTS
    # ===============================================================
    with c3:
        st.markdown("##### Nodes featuring multiple active assignments or overlapping deployment timeline records")
        if duplicate_assignments:
            dupe_display_df = df[df['NodeNum'].isin(duplicate_assignments)].sort_values(['NodeNum', 'Start_Date'])
            st.dataframe(
                dupe_display_df[['NodeNum', 'Project', 'Location', 'Start_Date', 'End_Date', 'SensorStatus']],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.success("✅ No conflicting overlap metrics or duplicate concurrent project entries found.")

    # ===============================================================
    # TAB 1: Gaps in Data
    # ===============================================================
    with c1:
        st.markdown("##### Nodes with a chronological gap where they were not assigned—requires unmonitored time to be added to Office")
        if gaps_in_data:
            gap_display_df = df[df['NodeNum'].isin(gaps_in_data)].sort_values(['NodeNum', 'Start_Date'])
            st.dataframe(
                gap_display_df[['NodeNum', 'Project', 'Location', 'Start_Date', 'End_Date', 'SensorStatus']], 
                use_container_width=True, 
                hide_index=True
            )
        else:
            st.success("✅ No timeline gaps or missing 'Office' storage windows detected across node history logs.")

    # ===============================================================
    # TAB 2: Orphaned Nodes
    # ===============================================================
    with c2:
        st.markdown("##### Nodes that have an end date on their last assignment but did not get transferred into a new project or Office stock")
        if orphaned_nodes:
            orphan_display_df = df[df['NodeNum'].isin(orphaned_nodes)].sort_values(['NodeNum', 'Start_Date'])
            last_entries = orphan_display_df.groupby('NodeNum').last().reset_index()
            st.dataframe(
                last_entries[['NodeNum', 'Project', 'Location', 'Start_Date', 'End_Date', 'SensorStatus']], 
                use_container_width=True, 
                hide_index=True
            )
        else:
            st.success("✅ Clean terminations verified. All decommissioned nodes successfully occupy new project profiles or Office stock rows.")

    # ===============================================================
    # TAB 3: MULTIPLE / DUPLICATE ASSIGNMENTS
    # ===============================================================
    with c3:
        st.markdown("##### Nodes featuring multiple active assignments or overlapping deployment timeline records")
        if duplicate_assignments:
            dupe_display_df = df[df['NodeNum'].isin(duplicate_assignments)].sort_values(['NodeNum', 'Start_Date'])
            st.dataframe(
                dupe_display_df[['NodeNum', 'Project', 'Location', 'Start_Date', 'End_Date', 'SensorStatus']],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.success("✅ No conflicting overlap metrics or duplicate concurrent project entries found.")
# ===============================================================
# PAGE MODULE: 📡 PROJECT OVERVIEW (Formerly Setup Node Tool)
# ===============================================================

def render_project_status_dashboard(client, selected_project, unit_label, target_registry):
    """Renders high-level data aggregation metrics alongside custom thermal threshold distributions."""
    st.subheader("📊 Project Status Summary")
    
    query = f"""
        SELECT 
            n.NodeNum, n.Bank, n.Location, n.Depth,
            CASE 
                WHEN (n.Bank LIKE 'S%' OR n.Location LIKE 'S%') AND (n.Bank NOT LIKE '%Amb%' AND n.Location NOT LIKE '%Amb%') THEN 'Supply'
                WHEN (n.Bank LIKE 'R%' OR n.Location LIKE 'R%') AND (n.Bank NOT LIKE '%Amb%' AND n.Location NOT LIKE '%Amb%') THEN 'Return'
                WHEN (n.Bank LIKE '%Amb%' OR n.Location LIKE '%Amb%') THEN 'Ambient'
                WHEN n.Depth IS NOT NULL THEN 'TempPipes'
                ELSE 'Other'
            END as hardware_type,
            AVG(CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN m.temperature END) as avg_now,
            MIN(CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN m.temperature END) as min_now,
            MAX(CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN m.temperature END) as max_now,
            MIN(CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN m.temperature END) as min_24h,
            MAX(CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN m.temperature END) as max_24h,
            AVG(CASE WHEN m.timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN m.temperature END) as avg_1h_prev,
            AVG(CASE WHEN m.timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR) THEN m.temperature END) as avg_6h_prev,
            COUNTIF(m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) as pings_24h,
            ARRAY_AGG(m.temperature ORDER BY m.timestamp DESC LIMIT 1)[OFFSET(0)] as latest_temp,
            MAX(m.timestamp) as latest_ts
        FROM `{target_registry}` n
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.master_data_view` m ON n.NodeNum = m.NodeNum
        WHERE n.Project = @proj_id AND n.End_Date IS NULL
        GROUP BY 1, 2, 3, 4, 5
    """
    try:
        df = client.query(query, job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)]
        )).to_dataframe()
    except Exception as e:
        st.error(f"Dashboard Query Failed: {e}")
        return

    if df.empty:
        st.info("No active nodes found for dashboard summary.")
        return

    cols = st.columns(4)
    type_map = {"Supply": (cols[0], "📥"), "Return": (cols[1], "📤"), "TempPipes": (cols[2], "📏"), "Ambient": (cols[3], "☁️")}
    now_utc = pd.Timestamp.now(tz='UTC')

    for h_type, (col, icon) in type_map.items():
        g_df = df[df['hardware_type'] == h_type]
        with col:
            st.markdown(f"#### {icon} {h_type}")
            if g_df.empty or g_df['latest_ts'].isna().all():
                st.caption("No recent data available")
                continue
            
            latest_time = g_df['latest_ts'].max()
            if latest_time.tzinfo is None:
                latest_time = latest_time.tz_localize('UTC')
            
            lag_hrs = (now_utc - latest_time).total_seconds() / 3600
            val = g_df['avg_now'].mean() if pd.notnull(g_df['avg_now'].mean()) else g_df['latest_temp'].mean()
            
            if lag_hrs > 1.1: 
                st.subheader(f"⚠️ Offline {int(lag_hrs)}h")
            else: 
                st.title(f"{val:.1f}{unit_label}")
            
            active_1h = int(g_df['avg_now'].notnull().sum())
            active_24h = int((g_df['pings_24h'] > 0).sum())
            st.write(f"**{active_1h}/{len(g_df)}** (1h) | **{active_24h}/{len(g_df)}** (24h)")
            
            st.caption(f"Cur: {g_df['min_now'].min():.1f} to {g_df['max_now'].max():.1f}{unit_label}")
            st.caption(f"24h: {g_df['min_24h'].min():.1f} to {g_df['max_24h'].max():.1f}{unit_label}")
            
            t_row = st.columns(2)
            t_row[0].caption(f"1h\n{get_trend_arrow(val, g_df['avg_1h_prev'].mean())}")
            t_row[1].caption(f"6h\n{get_trend_arrow(val, g_df['avg_6h_prev'].mean())}")
            
            # --- CUSTOM ENGINEERING RANGE DISTRIBUTIONS ---
            st.markdown("---")
            temps = g_df['latest_temp'].dropna()
            
            if h_type == "Supply":
                sub_0 = sum(temps < 0)
                sub_10 = sum(temps < -10)
                sub_15 = sum(temps < -15)
                st.markdown(f"❄️ **Below 0°F:** `{sub_0}/{len(g_df)}`")
                st.markdown(f"🥶 **Below -10°F:** `{sub_10}/{len(g_df)}`")
                st.markdown(f"🧊 **Below -15°F:** `{sub_15}/{len(g_df)}`")
                
            elif h_type == "Return":
                sub_10 = sum(temps < 10)
                sub_0 = sum(temps < 0)
                sub_10_neg = sum(temps < -10)
                st.markdown(f"🟢 **Below 10°F:** `{sub_10}/{len(g_df)}`")
                st.markdown(f"❄️ **Below 0°F:** `{sub_0}/{len(g_df)}`")
                st.markdown(f"🥶 **Below -10°F:** `{sub_10_neg}/{len(g_df)}`")
                
            elif h_type == "TempPipes":
                sub_freezing = sum(temps < 32)
                sub_20 = sum(temps < 20)
                sub_0 = sum(temps < 0)
                st.markdown(f"💧 **Below Freezing:** `{sub_freezing}/{len(g_df)}`")
                st.markdown(f"❄️ **Below 20°F:** `{sub_20}/{len(g_df)}`")
                st.markdown(f"🥶 **Below 0°F:** `{sub_0}/{len(g_df)}`")


def render_hardware_integrity_table(client, selected_project, unit_mode, unit_label, target_registry):
    st.subheader("📋 Hardware Integrity & Connectivity")
    query = f"""
        SELECT 
            n.NodeNum, n.Location, n.Bank, n.Depth, n.SensorStatus,
            MAX(m.timestamp) as last_ping,
            ARRAY_AGG(m.temperature ORDER BY m.timestamp DESC LIMIT 1)[OFFSET(0)] as last_temp,
            COUNTIF(m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) as pings_1h,
            COUNTIF(m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)) as pings_6h,
            COUNTIF(m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) as pings_24h,
            (COUNT(DISTINCT CASE 
                WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) 
                THEN TIMESTAMP_TRUNC(m.timestamp, HOUR) 
             END) / 24.0) * 100 as coverage_24h,
            AVG(CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN m.temperature END) as avg_now,
            AVG(CASE WHEN m.timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN m.temperature END) as avg_1h_prev
        FROM `{target_registry}` n
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.master_data_view` m ON n.NodeNum = m.NodeNum
        WHERE n.Project = @proj_id AND n.End_Date IS NULL
        GROUP BY 1, 2, 3, 4, 5
    """
    df = client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)]
    )).to_dataframe()

    if df.empty: 
        st.info("No active nodes found for connectivity table.")
        return

    df['bank_sort'] = df['Bank'].apply(lambda x: tuple(natural_sort_key(x)))
    df = df.sort_values(by=['Location', 'bank_sort', 'Depth']).drop(columns=['bank_sort'])
    now_utc = pd.Timestamp.now(tz='UTC')

    def row_processor(row):
        ping = row['last_ping']
        if pd.isnull(ping):
            txt, style = "❌ Never", "background-color: #d3d3d3"
        else:
            ts = ping if ping.tzinfo else ping.tz_localize('UTC')
            diff = (now_utc - ts).total_seconds() / 60
            if diff <= 15: txt, style = f"{int(diff)}m ago", "background-color: #ccffcc; color: black"
            elif diff <= 60: txt, style = f"{int(diff)}m ago", "background-color: #ffe4b5; color: black"
            else: txt, style = f"{round(diff/60, 1)}h ago", "background-color: #ffcccb; color: black"
        
        pos = f"{row['Depth']}ft" if (pd.notnull(row['Depth']) and row['Depth'] != 0) else f"Bank {row['Bank']}"
        trend = get_trend_arrow(row['avg_now'], row['avg_1h_prev'])
        return pd.Series([txt, style, pos, trend])

    df[['Seen_Text', 'Seen_Style', 'Pos_Label', 'Trend']] = df.apply(row_processor, axis=1)

    display_df = pd.DataFrame({
        "Node ID": df['NodeNum'],
        "Location": df['Location'],
        "Position": df['Pos_Label'],
        "Last Seen": df['Seen_Text'],
        "24h Coverage": df['coverage_24h'].apply(lambda x: f"{x:.1f}%"),
        "1h Change": df['Trend'],
        "Last Temp": df['last_temp'].apply(lambda x: fmt_temp(x, unit_mode, unit_label)),
        "1h Pings": df['pings_1h'],
        "6h Pings": df['pings_6h'],
        "24h Pings": df['pings_24h']
    })

    def diagnostic_styler(data):
        style_df = pd.DataFrame('', index=data.index, columns=data.columns)
        ref = df.reset_index(drop=True)
        for i, row in data.iterrows():
            if ref.loc[i, 'SensorStatus'] == 'Diagnostic':
                style_df.loc[i, 'Node ID'] = 'background-color: #ff4b4b; color: white; font-weight: bold;'
            style_df.loc[i, 'Last Seen'] = ref.loc[i, 'Seen_Style']
        return style_df

    st.dataframe(display_df.style.apply(diagnostic_styler, axis=None), use_container_width=True, hide_index=True)

# ===============================================================
# PAGE MODULE: 🔍 SENSOR STATUS
# ===============================================================

def calculate_custom_metrics(row):
    """
    Evaluates individual sensor behavior by comparing its current state 
    to peer averages and checking historical temperature volatility bounds.
    """
    # 1. PEER TREND ANALYSIS (Checks how close node is to other nodes in the same location)
    peer_diff = abs(row['current_temp'] - row['current_peer_avg'])
    if peer_diff < 2.0:
        trend = "🎯 In-Line"
    elif peer_diff < 5.0:
        trend = "⚠️ Drifting"
    else:
        trend = "🚨 Outlier"

    # 2. PERFORMANCE SCORING (Evaluates temperature swing stability thresholds)
    loc_upper = str(row['Location']).upper()
    is_sr = any(x in loc_upper for x in ['S', 'R']) and 'AMB' not in loc_upper
    
    s2, s24 = row['swing_2h'], row['swing_24h']
    
    if is_sr:
        perf = "❌ Volatile" if (s2 > 5.0 or s24 > 20.0) else "✅ Stable"
    else:
        perf = "❌ Unsteady" if (s2 > 1.0 or s24 > 2.0) else "✅ Solid"
        
    return pd.Series([trend, perf])


def render_sensor_status_charts(client, node_id, project_id):
    """
    Fetches and renders a dual-trace chart comparing the individual sensor 
    against its physical location's baseline peer average over 7 days.
    """
    st.markdown(f"### 📊 Comparative Analysis: **{node_id}** vs. Location Baseline")
    
    # Bug Fix: Referencing the global constants explicitly via string concatenation safely
    chart_q = f"""
        WITH TimedPeers AS (
            SELECT 
                timestamp,
                temperature,
                Location,
                NodeNum,
                AVG(temperature) OVER (PARTITION BY Location, timestamp) as peer_avg
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view`
            WHERE Project = @proj_id
              AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        )
        SELECT timestamp, temperature, peer_avg, Location
        FROM TimedPeers
        WHERE NodeNum = @node_id
        ORDER BY timestamp ASC
    """
    
    try:
        with st.spinner("Compiling comparative timeline history..."):
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("proj_id", "STRING", project_id),
                    bigquery.ScalarQueryParameter("node_id", "STRING", node_id)
                ]
            )
            data_df = client.query(chart_q, job_config=job_config).to_dataframe()
            
        if not data_df.empty:
            loc_label = data_df['Location'].iloc[0]
            
            fig = go.Figure()
            
            # Trace 1: The Selected Node
            fig.add_trace(go.Scatter(
                x=data_df['timestamp'],
                y=data_df['temperature'],
                mode='lines',
                name=f"Sensor {node_id}",
                line=dict(color='#00d4ff', width=2.5)
            ))
            
            # Trace 2: The Location Baseline Peer Average
            fig.add_trace(go.Scatter(
                x=data_df['timestamp'],
                y=data_df['peer_avg'],
                mode='lines',
                name=f"Location Mean ({loc_label})",
                line=dict(color='orange', width=2, dash='dash')
            ))
            
            fig.update_layout(
                height=350,
                template="plotly_dark",
                margin=dict(l=20, r=20, t=30, b=20),
                xaxis_title="Timeline Logs",
                yaxis_title="Temperature (°F)",
                legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01)
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("ℹ️ No recent matching telemetry logs found to plot for this sensor window.")
    except Exception as e:
        st.error(f"Failed to build visual deep-dive chart: {e}")


def render_sensor_status(client, selected_project, unit_label, unit_mode, display_tz):
    """
    Queries historical windows to analyze peer drift trends and 
    stability performance scoring across active project deployments.
    """
    p_meta = st.session_state.get('project_metadata')
    if not p_meta or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to view sensor health.")
        return

    p_name = p_meta.get('ProjectName', selected_project)
    f_date = p_meta.get('Date_Freezedown')
    st.title(f"❄️ {p_name}")
    
    if pd.notnull(f_date):
        days = (pd.Timestamp.now(tz=display_tz).date() - pd.to_datetime(f_date).date()).days
        st.markdown(f"## 🗓️ Day **{max(0, days)}** of Freezedown")
    st.divider()

    query = f"""
        WITH BaseReporting AS (
            SELECT 
                m.NodeNum, m.timestamp, m.temperature, m.Location, m.Bank, m.Depth,
                AVG(m.temperature) OVER (PARTITION BY m.Location, m.timestamp) as peer_avg
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
            WHERE m.Project = @proj_id
        ),
        HistoricalStats AS (
            SELECT 
                NodeNum, Location, Bank, Depth,
                MAX(timestamp) AS last_ping,
                ARRAY_AGG(temperature ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS current_temp,
                ARRAY_AGG(peer_avg ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS current_peer_avg,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR) THEN temperature END) - 
                MIN(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR) THEN temperature END) as swing_2h,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature END) - 
                MIN(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature END) as swing_24h,
                (COUNT(DISTINCT CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN TIMESTAMP_TRUNC(timestamp, HOUR) END) / 24.0) * 100 as coverage_24h
            FROM BaseReporting 
            GROUP BY NodeNum, Location, Bank, Depth
        )
        SELECT * FROM HistoricalStats
    """
    try:
        df = client.query(query, job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)]
        )).to_dataframe()

        if df.empty:
            st.warning("No data found for this project.")
            return

        # Map clean metric evaluation classifications
        df[['Peer Trend', 'Performance']] = df.apply(calculate_custom_metrics, axis=1)
        now_local = pd.Timestamp.now(tz=display_tz)
        
        df['hrs_lag'] = df['last_ping'].apply(
            lambda x: (now_local - pd.to_datetime(x).tz_convert(display_tz)).total_seconds() / 3600 if pd.notnull(x) else 999.0
        )
        df['Status'] = df['hrs_lag'].apply(lambda x: f"🟢 {x:.1f}h" if x <= 1.1 else f"🔴 {x:.1f}h")

        st.subheader("🔍 Detailed Sensor Audit")
        
        # UI Upgrade: Convert static dataframe presentation into an interactive checkbox table
        display_df = df[["Location", "NodeNum", "Peer Trend", "Performance", "Status", "coverage_24h"]].sort_values(['Location', 'NodeNum']).copy()
        display_df.insert(0, "Select", False)
        
        edited_df = st.data_editor(
            display_df,
            hide_index=True,
            use_container_width=True,
            column_config={"Select": st.column_config.CheckboxColumn("Select", default=False, required=True)},
            disabled=[col for col in display_df.columns if col != "Select"],
            key="sensor_status_editor"
        )

        # Resolve interactive row checkbox choice to generate comparative charts
        selected_rows = edited_df[edited_df["Select"] == True]
        
        if not selected_rows.empty:
            st.divider()
            target_node = selected_rows.iloc[0]["NodeNum"]
            render_sensor_status_charts(client, target_node, selected_project)
        else:
            st.info("💡 **Tip:** Use the checkbox in the audit table above to instantly pull up a comparative analysis graph for any sensor.")

    except Exception as e:
        st.error(f"Sensor Status Error: {e}")

# ===============================================================
# PAGE: BULK REGISTRY MANAGER
# ===============================================================

def render_bulk_registry_page(client, proj_list):
    """Main entry point for Bulk Registry Operations."""
    st.header("📦 Bulk Registry Operations")
    
    target_registry = f"{PROJECT_ID}.{DATASET_ID}.node_registry"
    bt1, bt2 = st.tabs(["📥 Site Deployment (CSV)", "🔚 Bulk Site Decommission"])

    with bt1:
        render_bulk_deployment_tab(client, target_registry)

    with bt2:
        render_bulk_decommission_tab(client, proj_list, target_registry)


def render_bulk_deployment_tab(client, target_registry):
    """Handles the UI for uploading new site configurations via CSV."""
    st.subheader("Initialize New Site Registry")
    st.info("Upload a CSV to register all sensors for a new project at once.")
    
    with st.expander("📊 View Required CSV Format (PhysicalID Removed)"):
        st.code("NodeNum,Project,Location,Bank,Depth,Start_Date,SensorStatus")
        st.caption("Note: PhysicalID column is no longer required and will be ignored if present.")
    
    u_csv = st.file_uploader("Upload Deployment CSV", type="csv")
    
    if u_csv:
        df_upload = pd.read_csv(u_csv)
        st.write("### Preview Data")
        st.dataframe(df_upload.head(), use_container_width=True)
        
        if st.button("🚀 Commit New Project Hardware"):
            process_bulk_upload(client, df_upload, target_registry)


def process_bulk_upload(client, df, target_registry):
    """Validates and uploads the dataframe to BigQuery."""
    try:
        # Strict validation of core columns
        required = {'NodeNum', 'Project', 'Location'}
        if not required.issubset(df.columns):
            st.error(f"Missing required columns: {required - set(df.columns)}")
            return

        with st.spinner("Uploading to BigQuery..."):
            # 1. Ensure Start_Date is valid
            if 'Start_Date' in df.columns:
                df['Start_Date'] = pd.to_datetime(df['Start_Date']).dt.date
            else:
                df['Start_Date'] = datetime.now().date()
            
            # 2. Force Status to 'On Project' if missing
            if 'SensorStatus' not in df.columns:
                df['SensorStatus'] = 'On Project'

            # 3. Clean up any PhysicalID columns if they were included by mistake
            if 'PhysicalID' in df.columns:
                df = df.drop(columns=['PhysicalID'])
            
            # 4. BigQuery Load
            job_config = bigquery.LoadTableConfig(write_disposition="WRITE_APPEND")
            client.load_table_from_dataframe(df, target_registry, job_config=job_config).result()
            
        st.success(f"Successfully registered {len(df)} nodes.")
        st.cache_data.clear() # Clear cache to update the Node Manager table
        st.balloons()
    except Exception as e:
        st.error(f"Upload Failed: {e}")


def render_bulk_decommission_tab(client, proj_list, target_registry):
    """Handles retiring an entire project's worth of sensors."""
    st.subheader("Project-Wide Decommission")
    st.warning("Warning: This ends all active records for a project and moves hardware to 'Office' stock.")
    
    # Filter out 'Office' from the retirement list
    active_field_projects = [p for p in proj_list if p != "Office"]
    ret_p = st.selectbox("Select Project to Retire", ["-- Select --"] + active_field_projects)
    
    c1, c2 = st.columns(2)
    ret_date = c1.date_input("Decommission Date", value=datetime.now().date())
    ret_stat = c2.selectbox("Return Status for Hardware", ["Available", "Diagnostic", "Dead"])
    
    if st.button("🔚 Retire All Nodes and Move to Office", type="primary"):
        if ret_p == "-- Select --":
            st.error("Please select a valid Project ID.")
        else:
            execute_bulk_decommission(client, ret_p, ret_date, ret_stat, target_registry)


def execute_bulk_decommission(client, project_id, decommission_date, return_status, target_registry):
    """
    Executes a Multi-Step Transaction:
    1. Ends all active records for the Project.
    2. Inserts new 'Office' records for every sensor that was retired.
    """
    date_iso = decommission_date.isoformat()
    
    # This SQL handles the entire transition in one transaction
    bulk_sql = f"""
        BEGIN TRANSACTION;
        
        -- 1. Archive the existing deployments
        UPDATE `{target_registry}` 
        SET End_Date = DATE('{date_iso}'), 
            SensorStatus = 'Archived' 
        WHERE Project = '{project_id}' 
          AND End_Date IS NULL;

        -- 2. Insert the hardware back into Office Stock
        -- We select from the records we just archived to ensure a perfect 1-to-1 move
        INSERT INTO `{target_registry}` (NodeNum, Project, Location, Bank, Depth, SensorStatus, Start_Date)
        SELECT 
            NodeNum, 
            'Office' as Project, 
            'Office' as Location, 
            NodeNum as Bank, -- Reset Bank to NodeNum for stock
            NULL as Depth,   -- Clear Depth for stock
            '{return_status}' as SensorStatus,
            DATE('{date_iso}') as Start_Date
        FROM `{target_registry}`
        WHERE Project = '{project_id}' AND End_Date = DATE('{date_iso}');
        
        COMMIT;
    """
    
    try:
        with st.spinner(f"Processing Bulk Retirement for {project_id}..."):
            query_job = client.query(bulk_sql)
            query_job.result()
            
        st.success(f"Project {project_id} decommissioned. Hardware moved to Office Stock as '{return_status}'.")
        st.cache_data.clear()
    except Exception as e:
        st.error(f"Bulk Decommission Failed: {e}")
        st.code(bulk_sql)
        
# ===============================================================
# PAGE: DATA RECOVERY (SensorPush API Bridge)
# ===============================================================

def render_data_recovery_page(reg_df):
    """Main entry point for the Data Recovery page."""
    st.header("📡 Data Recovery")
    st.info("Triggers the Cloud Run service to backfill missing telemetry from the SensorPush API.")

    # 1. GATEWAY: Filter for SensorPush hardware only (TP-Prefix) 
    # Use only active sensors to keep the selection list manageable
    sp_reg = reg_df[
        (reg_df['NodeNum'].str.startswith('TP', na=False)) & 
        (reg_df['End_Date'].isna())
    ].copy()

    # 2. FILTERING UI
    selected_nodes = render_recovery_filters(sp_reg)

    # 3. DATE RANGE & TRIGGER
    st.divider()
    c_d1, c_d2 = st.columns(2)
    with c_d1:
        # Default to last 3 days
        start_date = st.date_input("Recovery Start Date", value=datetime.now() - timedelta(days=3))
    with c_d2:
        end_date = st.date_input("Recovery End Date", value=datetime.now())

    if st.button("🚀 Run Recovery Service", type="primary"):
        if not selected_nodes:
            st.error("Please select at least one node.")
        else:
            handle_recovery_trigger(selected_nodes, start_date, end_date)

    # 4. SYSTEM LOGIC FOOTER
    render_recovery_logic_footer()


def render_recovery_filters(sp_reg):
    """Renders hierarchical filters and returns selected Node IDs."""
    st.subheader("🔍 Select Target Hardware")
    col_f1, col_f2, col_f3 = st.columns(3)
    
    with col_f1:
        u_projects = ["All"] + sorted(sp_reg['Project'].unique().tolist())
        rec_proj = st.selectbox("Filter by Project", u_projects, key="rec_proj_sel")
    
    proj_filtered = sp_reg if rec_proj == "All" else sp_reg[sp_reg['Project'] == rec_proj]
    
    with col_f2:
        u_locs = ["All"] + sorted(proj_filtered['Location'].unique().tolist(), key=lambda x: tuple(natural_sort_key(x)))
        rec_loc = st.selectbox("Filter by Location", u_locs, key="rec_loc_sel")
        
    with col_f3:
        loc_filtered = proj_filtered if rec_loc == "All" else proj_filtered[proj_filtered['Location'] == rec_loc]
        available_nodes = sorted(loc_filtered['NodeNum'].unique().tolist(), key=natural_sort_key)
        
        selected_nodes = st.multiselect(
            "Select Node Numbers", 
            available_nodes, 
            # Default to None to prevent accidental massive API requests
            default=None,
            help="Choose the specific sensors to backfill."
        )
    return selected_nodes


def handle_recovery_trigger(selected_nodes, start_date, end_date):
    """Manages the API request to the Cloud Run recovery service."""
    cloud_run_url = "https://sensorpushtobigquery-1013288934882.us-west1.run.app/recover_data"
    
    payload = {
        "start": start_date.strftime("%Y-%m-%d"),
        "end": end_date.strftime("%Y-%m-%d"),
        "nodes": ",".join(selected_nodes)
    }

    with st.spinner(f"Triggering backfill for {len(selected_nodes)} sensors..."):
        try:
            # Note: 300s timeout is good for larger batches
            response = requests.get(cloud_run_url, params=payload, timeout=300)
            
            if response.status_code == 200:
                st.success("✅ Recovery Triggered Successfully")
                st.info("The service is processing. Data will appear in BigQuery shortly.")
                if response.text:
                    st.code(response.text)
            else:
                st.error(f"Cloud Service Error ({response.status_code}): {response.text}")
        except Exception as e:
            st.error(f"Connectivity Failure: {e}")


def render_recovery_logic_footer():
    """Renders documentation on how the recovery process functions."""
    st.divider()
    with st.expander("🛠️ How the Recovery Engine Works"):
        st.markdown("""
        1. **Filtered Registry**: Accesses active `TP` (SensorPush) sensors only.
        2. **API Handshake**: Sends date parameters and a comma-separated node list to the Cloud Run endpoint.
        3. **Background Processing**: Cloud Run fetches data from SensorPush and pushes to `raw_sensorpush` in BigQuery.
        4. **Propagation**: Data updates the `master_data_view` automatically via the existing SQL view logic.
        """)
        
# ===============================================================
# PAGE: PROJECT MASTER
# ===============================================================

def render_project_master_page(client, selected_project):
    """
    Main entry point for Project Lifecycle Management.
    """
    st.header("⚙️ Project Lifecycle Management")
    
    # Updated tab navigation labeling schemas
    action = st.radio("Action", ["📋 Project List", "🏗️ New Project", "🔧 Edit Project Metadata"], horizontal=True)
    table_projects = f"{PROJECT_ID}.{DATASET_ID}.project_registry"

    if action == "📋 Project List":
        render_project_overview(client, table_projects)

    elif action == "🏗️ New Project":
        render_new_project_form(client, table_projects)

    elif action == "🔧 Edit Project Metadata":
        render_update_project_form(client, selected_project, table_projects)


def render_project_overview(client, table_projects):
    """
    Displays all core schema fields across all registered system environments.
    """
    st.subheader("📋 Complete Project Registry Table")
    
    query = f"SELECT * FROM `{table_projects}` ORDER BY Project ASC"
    try:
        df = client.query(query).to_dataframe()
        if not df.empty:
            # Clean up date display formatting paradigms for data tables
            for col in ['Date_Freezedown', 'Date_Completion']:
                if col in df.columns:
                    df[col] = pd.to_dataframe_datetime_if_needed_or_date(df[col])
            
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.caption(f"Total Combined Tracking Configurations in Registry: {len(df)}")
        else:
            st.info("The central project tracking configuration registry is currently empty.")
    except Exception as e:
        st.error(f"Failed to extract historical project configuration records: {e}")


def pd_to_dataframe_datetime_if_needed_or_date(series):
    """Helper formatting string parser utility."""
    return pd.to_datetime(series).dt.date


def render_new_project_form(client, table_projects):
    """
    UI for registering a brand new job environment, supporting optional 
    metadata replication from an existing project template for phased builds.
    """
    st.subheader("🏗️ Initialize New Project Profile")
    
    # Phase Duplicate Template Configuration Interface Block
    try:
        all_p_q = f"SELECT Project FROM `{table_projects}` ORDER BY Project ASC"
        existing_p_list = client.query(all_p_q).to_dataframe()['Project'].tolist()
    except Exception:
        existing_p_list = []

    use_template = st.checkbox("📋 Clone settings from an existing project template? (e.g., Phase 2 expansions)")
    template_source = None
    template_data = {}
    
    if use_template and existing_p_list:
        template_source = st.selectbox("Select Project to Clone From", existing_p_list)
        if template_source:
            try:
                t_res = client.query(f"SELECT * FROM `{table_projects}` WHERE Project = '{template_source}'").to_dataframe()
                if not t_res.empty:
                    template_data = t_res.iloc[0].to_dict()
                    st.info(f"Loaded configurations for template base **{template_source}**. Fill out the unique identifiers below to inherit matching metadata.")
            except Exception as e:
                st.error(f"Error reading configuration profile parameters: {e}")

    with st.form("new_project_form"):
        col1, col2 = st.columns(2)
        n_code = col1.text_input("Project ID / Job # (e.g., 2541-Phase 2)*")
        n_name = col2.text_input("Friendly Project Name", value=template_data.get('ProjectName', ''))
        
        c_g1, c_g2 = st.columns(2)
        n_city = c_g1.text_input("City Deployment Field", value=template_data.get('City', ''))
        n_tz = c_g2.text_input("Operational Timezone Reference", value=template_data.get('Timezone', 'America/Los_Angeles'))
        
        n_up_notes = st.text_input("Automated Pipeline Sync Notes (UploadNote)", value=template_data.get('UploadNote', 'Data will be uploaded once per business day by 4pm Pacific Time.'))
        n_as_built = st.text_input("Engineering Archive ID (AsBuiltFile)", value=template_data.get('AsBuiltFile', ''))
        n_notes = st.text_area("Initial Site Engineering Field Notes", value=template_data.get('EngNotes', ''))
        
        if st.form_submit_button("🚀 Commit New Project Entry"):
            if not n_code.strip():
                st.error("Unique Internal Project Identifier ID string reference required.")
            else:
                check_q = f"SELECT Project FROM `{table_projects}` WHERE Project = '{n_code.strip()}'"
                if not client.query(check_q).to_dataframe().empty:
                    st.error(f"Project context path parameter '{n_code.strip()}' already occupies active table blocks.")
                else:
                    insert_q = f"""
                        INSERT INTO `{table_projects}` (Project, ProjectName, ProjectStatus, City, Timezone, UploadNote, AsBuiltFile, EngNotes)
                        VALUES (
                            '{n_code.strip()}', '{n_name.strip()}', 'Initialized', 
                            '{n_city.strip()}', '{n_tz.strip()}', '{n_up_notes.strip()}', 
                            '{n_as_built.strip()}', '{n_notes.strip()}'
                        )
                    """
                    try:
                        client.query(insert_q).result()
                        st.success(f"Project profile parsing entry context complete: Registered **{n_code.strip()}** successfully.")
                        st.cache_data.clear()
                        time.sleep(1)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed parsing database initialization sequences: {e}")


def render_update_project_form(client, selected_project, table_projects):
    """
    Form for altering existing project profiles. Grants comprehensive variable coverage 
    across fields like City, Timezone, UploadNote, and AsBuilt fields. Includes an execution sequence 
    for deleting incorrectly set-up fields.
    """
    st.subheader(f"🔧 Configuration Editor: {selected_project}")
    
    proj_q = f"SELECT * FROM `{table_projects}` WHERE Project = '{selected_project}'"
    p_res = client.query(proj_q).to_dataframe()
    
    if p_res.empty:
        st.error("Please pick an active verification footprint in the sidebar to modify metadata metrics.")
        return

    p_data = p_res.iloc[0].to_dict()
    
    with st.form("comprehensive_edit_project"):
        # 1. Identity & Name Context Paths
        c1, c2 = st.columns(2)
        u_project_id = c1.text_input("Project ID (Internal Storage Primary Key)", value=p_data.get('Project', ''), disabled=True)
        u_project_name = c2.text_input("Friendly Project Name", value=p_data.get('ProjectName', ''))

        # 2. Geographic Parameters & Sync Schedule Attributes
        c3, c4 = st.columns(2)
        u_city = c3.text_input("City Deployment Field", value=p_data.get('City', ''))
        u_tz = c4.text_input("Operational Timezone Reference", value=p_data.get('Timezone', 'America/Los_Angeles'))
        
        u_up_notes = st.text_input("Automated Pipeline Sync Notes (UploadNote)", value=p_data.get('UploadNote', ''))
        u_as_built = st.text_input("Engineering Archive ID (AsBuiltFile)", value=p_data.get('AsBuiltFile', ''))

        # 3. Status & Lifecycle Configuration
        c5, c6 = st.columns(2)
        status_options = ["Initialized", "Pre-freeze", "Freezedown", "Maintenance", "Archived"]
        curr_status = p_data.get('ProjectStatus', 'Initialized')
        s_idx = status_options.index(curr_status) if curr_status in status_options else 0
        u_status = c5.selectbox("Lifecycle Status Tier", status_options, index=s_idx)
        
        # 4. Critical Engineering Event Schedules
        def safe_date(d): return pd.to_datetime(d).date() if pd.notnull(d) else None
        u_date_freeze = c5.date_input("Date Freezedown Started", value=safe_date(p_data.get('Date_Freezedown')))
        u_date_comp = c6.date_input("Date Project Completed", value=safe_date(p_data.get('Date_Completion')))

        # 5. Text Notes Area
        u_notes = st.text_area("Engineering & Site Notes Logs", value=p_data.get('EngNotes', ''))

        if st.form_submit_button("💾 Overwrite Project Registry Information", type="primary"):
            freeze_val = f"DATE('{u_date_freeze}')" if u_date_freeze else "NULL"
            comp_val = f"DATE('{u_date_comp}')" if u_date_comp else "NULL"
            
            update_q = f"""
                UPDATE `{table_projects}` 
                SET 
                    ProjectName = '{u_project_name.strip()}',
                    ProjectStatus = '{u_status}',
                    City = '{u_city.strip()}',
                    Timezone = '{u_tz.strip()}',
                    UploadNote = '{u_up_notes.strip()}',
                    AsBuiltFile = '{u_as_built.strip()}',
                    EngNotes = '{u_notes.strip()}',
                    Date_Freezedown = {freeze_val},
                    Date_Completion = {comp_val}
                WHERE Project = '{selected_project}'
            """
            try:
                client.query(update_q).result()
                st.success(f"✅ Configuration data modification transaction verified for: {selected_project}")
                st.cache_data.clear()
                time.sleep(1)
                st.rerun()
            except Exception as e:
                st.error(f"Database translation pipeline update failure: {e}")

    # Administrative Context Removal Tool (Separated from form updates to maintain safety scopes)
    st.markdown("---")
    with st.expander("🧨 Administrative Removal Tool Area"):
        st.warning(f"Danger Zone: Executing this function completely drops the project ID context for '{selected_project}' from the central database schema registry tracker.")
        confirm_token = st.text_input(f"Type out '{selected_project}' to authorize dropping the dataset registry target context entirely:")
        
        if st.button(f"Permanently Delete Project Profile {selected_project}", type="primary"):
            if confirm_token.strip() == selected_project:
                delete_q = f"DELETE FROM `{table_projects}` WHERE Project = '{selected_project}'"
                try:
                    client.query(delete_q).result()
                    st.warning(f"Registry mapping target dropped safely: **{selected_project}** is no longer tracked.")
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed processing delete statement parameters: {e}")
            else:
                st.error("Authorization verification mismatch token error.")


# ===============================================================
# PAGE: REF CURVE LIBRARY
# ===============================================================

def render_ref_curve_library_page(client):
    """Main entry point for Theoretical Curve Management."""
    st.header("📈 Theoretical Curve Management")
    
    table_curves = f"{PROJECT_ID}.{DATASET_ID}.reference_curves"
    
    # 1. FETCH INVENTORY
    inventory_df = fetch_curve_inventory(client, table_curves)
    
    st.divider()

    # 2. MANAGEMENT TOOLS
    render_curve_management_tools(client, inventory_df, table_curves)

    st.divider()

    # 3. UPLOAD ENGINE
    render_curve_upload_engine(client, table_curves, inventory_df)


def fetch_curve_inventory(client, table_curves):
    """
    Fetches current library stats with robust column handling.
    Includes an automatic fallback query to handle schema variance seamlessly.
    """
    # Standard query attempting to include the upload_date string column
    inv_q = f"""
        SELECT 
            CurveID, 
            MAX(Day) as Max_Day, 
            COUNT(*) as Total_Points,
            MAX(upload_date) as Last_Upload
        FROM `{table_curves}`
        GROUP BY CurveID
        ORDER BY CurveID ASC
    """
    
    try:
        inventory_df = client.query(inv_q).to_dataframe()
    except Exception as primary_error:
        # Fallback Query: If upload_date column doesn't exist yet, fetch standard core components
        fallback_q = f"""
            SELECT 
                CurveID, 
                MAX(Day) as Max_Day, 
                COUNT(*) as Total_Points,
                'N/A' as Last_Upload
            FROM `{table_curves}`
            GROUP BY CurveID
            ORDER BY CurveID ASC
        """
        try:
            inventory_df = client.query(fallback_q).to_dataframe()
        except Exception as secondary_error:
            st.error(f"❌ Complete Database Schema Access Failure: {secondary_error}")
            return pd.DataFrame()

    st.subheader("📚 Theoretical Library Inventory")
    if not inventory_df.empty:
        st.dataframe(
            inventory_df.rename(columns={
                "CurveID": "Curve Identifier",
                "Max_Day": "Duration (Days)",
                "Total_Points": "Data Density",
                "Last_Upload": "Upload Date"
            }),
            use_container_width=True,
            hide_index=True
        )
        return inventory_df
    else:
        st.info("The library is currently empty. Upload curve CSVs below.")
        return pd.DataFrame()


def render_curve_management_tools(client, inventory_df, table_curves):
    """UI for deleting curves or purging the library."""
    c1, c2 = st.columns(2)
    
    with c1.expander("🗑️ Individual Curve Delete"):
        if not inventory_df.empty:
            to_delete = st.selectbox("Select Curve to Remove", sorted(inventory_df['Curve Identifier'].tolist()))
            if st.button(f"Permanently Delete {to_delete}", type="primary"):
                client.query(f"DELETE FROM `{table_curves}` WHERE CurveID = '{to_delete}'").result()
                st.success(f"Removed {to_delete}")
                st.cache_data.clear()
                time.sleep(1)
                st.rerun()

    with c2.expander("🧨 Library Wipe"):
        st.warning("This will delete EVERY theoretical curve in the database.")
        if st.button("EXECUTE TOTAL PURGE", key="purge_all"):
            client.query(f"TRUNCATE TABLE `{table_curves}`").result()
            st.success("Library wiped.")
            st.cache_data.clear()
            time.sleep(1)
            st.rerun()


def render_curve_upload_engine(client, table_curves, inventory_df):
    """Handles CSV uploads with duplicate protection."""
    st.subheader("📤 Upload New Curves")
    u_files = st.file_uploader(
        "Upload Curve CSVs", 
        type=['csv'], 
        accept_multiple_files=True,
        help="Format: Day in Col 1, Temp in Col 2. Data starts Row 3."
    )

    if u_files:
        existing_ids = inventory_df['Curve Identifier'].tolist() if not inventory_df.empty else []
        
        # Filter out files that already exist to prevent duplicates
        valid_files = [f for f in u_files if f.name.rsplit('.', 1)[0] not in existing_ids]
        dupes = [f.name for f in u_files if f.name.rsplit('.', 1)[0] in existing_ids]

        if dupes:
            st.warning(f"⚠️ Skipping {len(dupes)} files that already exist: {', '.join(dupes)}")

        if valid_files:
            if st.button(f"🚀 Commit {len(valid_files)} New Curves"):
                process_curve_uploads(client, valid_files, table_curves)


def process_curve_uploads(client, u_files, table_curves):
    """Parses and appends cleaned data to BigQuery."""
    today_str = datetime.now().strftime('%Y-%m-%d')
    total_imported = 0
    
    for f in u_files:
        try:
            # Day, Temp mapping
            df = pd.read_csv(f, skiprows=2, usecols=[0, 1], names=['Day', 'Temp'])
            df['CurveID'] = f.name.rsplit('.', 1)[0]
            df['upload_date'] = today_str
            
            # Clean
            df['Day'] = pd.to_numeric(df['Day'], errors='coerce')
            df['Temp'] = pd.to_numeric(df['Temp'], errors='coerce')
            df = df.dropna(subset=['Day', 'Temp'])

            if not df.empty:
                job_config = bigquery.LoadTableConfig(
                    schema=[
                        bigquery.SchemaField("Day", "FLOAT"),
                        bigquery.SchemaField("Temp", "FLOAT"),
                        bigquery.SchemaField("CurveID", "STRING"),
                        bigquery.SchemaField("upload_date", "STRING"),
                    ],
                    write_disposition="WRITE_APPEND"
                )
                client.load_table_from_dataframe(df, table_curves, job_config=job_config).result()
                total_imported += 1
        except Exception as e:
            st.error(f"Error processing {f.name}: {e}")

    if total_imported > 0:
        st.success(f"✅ Imported {total_imported} curves.")
        st.cache_data.clear()
        time.sleep(1.5)
        st.rerun()

# ===============================================================
# PAGE MODULE: 🧨 DATA MANAGEMENT (Manual Rejections)
# ===============================================================

def render_management_controls():
    """Renders the top-level scope selection and target flag status inputs."""
    c1, c2 = st.columns(2)
    with c1:
        target_scope = st.radio(
            "Target Scope", 
            ["Project Wide", "Specific Location", "Specific Node"], 
            horizontal=True, 
            key="mgmt_target_scope"
        )
    with c2:
        new_status = st.selectbox(
            "Set Approval Status To:", 
            ["TRUE", "BadData", "Masked", "Office"], 
            key="mgmt_new_status"
        )
    return target_scope, new_status


def render_verification_step(client, where_str, telemetry_table, rejections_table):
    """Queries BigQuery to show count and current status of data points."""
    status_q = "Query not yet generated." 
    
    if st.button("🔍 Step 1: Verify Match Count & Current Status", key="mgmt_verify_btn"):
        # Resolve ambiguity by aliasing columns to 't'
        aliased_where = where_str.replace("NodeNum", "t.NodeNum").replace("timestamp", "t.timestamp").replace("temperature", "t.temperature")
        
        status_q = f"""
            SELECT 
                t.NodeNum,
                COUNT(*) as Point_Count,
                COALESCE(r.approve, 'TRUE') as current_status
            FROM `{telemetry_table}` t
            LEFT JOIN `{rejections_table}` r 
                ON t.NodeNum = r.NodeNum AND t.timestamp = r.timestamp
            WHERE {aliased_where}
            GROUP BY t.NodeNum, current_status
        """
        
        try:
            with st.spinner("Analyzing current database flags..."):
                res = client.query(status_q).to_dataframe()
            
            if not res.empty:
                st.subheader("📊 Current Data Profile")
                st.info("The table below shows how many points are currently 'TRUE' vs. already flagged.")
                st.dataframe(res, use_container_width=True, hide_index=True)
                
                total_points = res['Point_Count'].sum()
                st.metric("Total Points in Selection", f"{total_points:,}")
            else:
                st.warning("No data points found for this selection. Check your date and hour ranges.")
                
        except Exception as e:
            st.error(f"Verification Failed: {e}")
            st.code(status_q, language="sql")


def render_rejection_execution_step(client, where_str, new_status, target_table, telemetry_table):
    """Executes the actual database update via DELETE or MERGE transaction."""
    st.info(f"Target Status for these points: **{new_status}**")
    
    if st.checkbox("I confirm these changes to the rejection library.", key="confirm_mgmt"):
        if st.button(f"🚀 Execute Set to {new_status}", key="exec_mgmt_btn"):
            aliased_where = where_str.replace("NodeNum", "t.NodeNum").replace("timestamp", "t.timestamp").replace("temperature", "t.temperature")
            
            if new_status == "TRUE":
                sql = f"DELETE FROM `{target_table}` WHERE {where_str}"
            else:
                sql = f"""
                    MERGE `{target_table}` T
                    USING (SELECT NodeNum, timestamp FROM `{telemetry_table}` t WHERE {aliased_where}) S
                    ON T.NodeNum = S.NodeNum AND T.timestamp = S.timestamp
                    WHEN MATCHED THEN
                        UPDATE SET approve = '{new_status}'
                    WHEN NOT MATCHED THEN
                        INSERT (NodeNum, timestamp, approve) VALUES (S.NodeNum, S.timestamp, '{new_status}')
                """
            try:
                with st.spinner("Executing database merge..."):
                    job = client.query(sql)
                    job.result()
                st.success(f"Successfully processed {job.num_dml_affected_rows:,} records.")
                st.cache_data.clear()
                st.balloons()
            except Exception as e:
                st.error(f"Execution Error: {e}")
                st.code(sql, language="sql")


def render_management_filters(reg_df, selected_project, target_scope):
    """
    Renders hierarchical filters. Includes hardened Hour Sliders to handle precise 
    timestamp window tracking safely without timezone offset compilation leaks.
    """
    col_f1, col_f2, col_f3 = st.columns(3)
    
    with col_f1:
        temporal_dir = st.selectbox("Temporal Direction", ["Between Range", "Older Than", "Newer Than"])
        
        # Split Date Input and Hour Sliders for safe isolation strings
        s_date = st.date_input("Start Date", value=datetime.now().date() - timedelta(days=7))
        s_hour = st.slider("Start Hour Bracket", 0, 23, 0, help="0 = Midnight, 12 = Noon, 23 = 11 PM")
        
        e_date = st.date_input("End Date", value=datetime.now().date())
        e_hour = st.slider("End Hour Bracket", 0, 23, 23)

    with col_f2:
        val_filter = st.selectbox("Value Filter", ["No Threshold", "Above Threshold", "Below Threshold"])
        threshold = st.number_input("Threshold Value (°F)", value=100.0)

    with col_f3:
        scope_val = None
        
        if target_scope == "Project Wide":
            st.info(f"Targeting all nodes in **{selected_project}**")
            scope_val = selected_project

        elif target_scope == "Specific Location":
            u_locs = sorted(reg_df[reg_df['Project'] == selected_project]['Location'].unique().tolist(), key=natural_sort_key)
            scope_val = st.selectbox("Select Location", u_locs)

        elif target_scope == "Specific Node":
            u_locs = sorted(reg_df[reg_df['Project'] == selected_project]['Location'].unique().tolist(), key=natural_sort_key)
            selected_loc = st.selectbox("First, Select Location", u_locs)
            
            u_nodes = sorted(
                reg_df[(reg_df['Project'] == selected_project) & (reg_df['Location'] == selected_loc)]['NodeNum'].unique().tolist(),
                key=natural_sort_key
            )
            scope_val = st.selectbox("Then, Select Node", u_nodes)
            
    return {
        "temporal_dir": temporal_dir, 
        "s_date": s_date, "s_hour": s_hour,
        "e_date": e_date, "e_hour": e_hour,
        "val_filter": val_filter, "threshold": threshold, "scope_val": scope_val
    }


def build_management_where_clause(reg_df, selected_project, target_scope, f):
    """
    Constructs a WHERE clause using custom-built hour timestamp syntax mapping matching BigQuery schemas.
    """
    proj_nodes = reg_df[reg_df['Project'] == selected_project]['NodeNum'].unique().tolist()
    if not proj_nodes:
        return "NodeNum = 'NONE'"

    # 1. Base Target Scope Context Logic
    if target_scope == "Specific Node":
        where_clauses = [f"NodeNum = '{f['scope_val']}'"]
    elif target_scope == "Specific Location":
        loc_nodes = reg_df[(reg_df['Project'] == selected_project) & 
                           (reg_df['Location'] == f['scope_val'])]['NodeNum'].unique().tolist()
        nodes_str = ", ".join([f"'{n}'" for n in loc_nodes])
        where_clauses = [f"NodeNum IN ({nodes_str})"]
    else:
        nodes_str = ", ".join([f"'{n}'" for n in proj_nodes])
        where_clauses = [f"NodeNum IN ({nodes_str})"]

    # 2. Hardened ISO Timestamp String Serialization (Bypasses local time parsing drift)
    start_ts_str = f"{f['s_date'].strftime('%Y-%m-%d')} {f['s_hour']:02d}:00:00"
    end_ts_str = f"{f['e_date'].strftime('%Y-%m-%d')} {f['e_hour']:02d}:59:59"

    if f["temporal_dir"] == "Between Range":
        where_clauses.append(f"timestamp BETWEEN '{start_ts_str}' AND '{end_ts_str}'")
    elif f["temporal_dir"] == "Older Than" or f["temporal_dir"] == "Newer Than":
        op = "<" if f["temporal_dir"] == "Older Than" else ">"
        where_clauses.append(f"timestamp {op} '{start_ts_str}'")
    
    # 3. Value Constraints
    if f["val_filter"] == "Above Threshold":
        where_clauses.append(f"temperature > {f['threshold']}")
    elif f["val_filter"] == "Below Threshold":
        where_clauses.append(f"temperature < {f['threshold']}")

    return " AND ".join(where_clauses)


def render_data_management_page(client, reg_df, selected_project):
    """Main administrative block executing targeted telemetry rejections."""
    st.header("🧨 Data Management (Manual Rejections)")
    
    target_table = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections" 
    telemetry_table = f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush" 

    target_scope, new_status = render_management_controls()
    st.divider()

    filters = render_management_filters(reg_df, selected_project, target_scope)
    where_str = build_management_where_clause(reg_df, selected_project, target_scope, filters)
    
    # 1. Step 1: Verification
    render_verification_step(client, where_str, telemetry_table, target_table)
    st.divider()
    # 2. Step 2: Execution
    render_rejection_execution_step(client, where_str, new_status, target_table, telemetry_table)


# ===============================================================
# FINAL INTEGRATED EXECUTION BLOCK
# ===============================================================

def main():
    """
    Unified entry point. Routes to specific tools based on sidebar selection.
    """
    # 1. Initialize Sidebar controls and pull active session path configurations
    admin_page, target_registry, selected_project, proj_list = render_sidebar()
    
    # 2. Extract unit preferences and global timezone configurations
    unit_mode, unit_label = get_unit_labels()
    
    # 3. Load active hardware inventory tracking logs (Cached BigQuery extraction)
    reg_df = load_registry_data(target_registry)

    # --- ROUTING LOGIC PIPELINE ---

    if admin_page == "🛠️ Node Manager":
        selected_node_data = render_node_selector(reg_df, proj_list)
        if selected_node_data is not None:
            st.divider()
            render_node_action_manager(client, selected_node_data, reg_df, proj_list, target_registry)
        else:
            st.divider()
            st.info("💡 **Tip:** Use the checkbox in the active table above to choose a node context to modify.")
            
        # Run systemic structural data checker evaluations at the footer frame
        render_data_checker(client, reg_df)

    elif admin_page == "📡 Setup Node Tool":
        # Core data dashboard renamed globally to Project Overview mapping
        render_project_status_dashboard(client, selected_project, unit_label, target_registry)
        st.divider()
        render_hardware_integrity_table(client, selected_project, unit_mode, unit_label, target_registry)

    elif admin_page == "🔍 Sensor Status":
        render_sensor_status(client, selected_project, unit_label, unit_mode, display_tz)
        
    elif admin_page == "📦 Bulk Registry Manager":
        render_bulk_registry_page(client, proj_list)

    elif admin_page == "📡 Data Recovery":
        render_data_recovery_page(reg_df)

    elif admin_page == "⚙️ Project Master":
        render_project_master_page(client, selected_project)

    elif admin_page == "📈 Ref Curve Library":
        render_ref_curve_library_page(client)

    elif admin_page == "🧨 Data Management":
        render_data_management_page(client, reg_df, selected_project)
      
# ===============================================================
# EXECUTION ENTRY POINT
# ===============================================================
if __name__ == "__main__":
    main()
