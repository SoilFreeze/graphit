import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import time
import re

# ===============================================================
# 1. CONFIGURATION & SESSION STATE
# ===============================================================
def initialize_app():
    """Sets up page config and global session state variables."""
    st.set_page_config(page_title="SF Engineering Admin", page_icon="🛠️", layout="wide")
    
    if 'unit_mode' not in st.session_state:
        st.session_state['unit_mode'] = "Fahrenheit"
    
    return "Temperature", "sensorpush-export"

DATASET_ID, PROJECT_ID = initialize_app()

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
# 3. DATA LOADING & SIDEBAR
# ===============================================================
def load_registry_data(target_table):
    """Loads the node registry into a dataframe."""
    try:
        return client.query(f"SELECT * FROM `{target_table}`").to_dataframe()
    except:
        return pd.DataFrame()

def render_sidebar():
    """Renders the sidebar navigation and project selection."""
    st.sidebar.title("🛠️ Admin Command Center")
    
    admin_page = st.sidebar.radio("Management Tool", [
        "📡 Setup Node Tool", 
        "🔍 Sensor Status",
        "🔄 Sensor Replace",      
        "🩹 Sensor Switch",       
        "📝 Sensor Edit",         
        "📦 Bulk Registry Manager",
        "📡 Data Recovery",
        "⚙️ Project Master", 
        "📈 Ref Curve Library", 
        "🧨 Data Management"
    ])

    is_dev = st.sidebar.toggle("🧪 Use Registry Playground", value=True)
    target_registry = f"{PROJECT_ID}.{DATASET_ID}.node_registry" + ("_dummy" if is_dev else "")

    # Global Project Selection
    proj_q = f"SELECT Project FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE ProjectStatus != 'Archived'"
    proj_list = sorted(client.query(proj_q).to_dataframe()['Project'].tolist())
    selected_project = st.sidebar.selectbox("🎯 Target Project Context", proj_list)
    
    return admin_page, target_registry, selected_project, proj_list

# ===============================================================
# 4. GLOBAL HELPERS
# ===============================================================
def get_trend_arrow(current, previous):
    """Generates trend icons based on temperature change."""
    if pd.isnull(current) or pd.isnull(previous): 
        return "N/A"
    delta = current - previous
    if delta > 0.1: return f"🔺 +{delta:.1f}"
    if delta < -0.1: return f"🔹 {delta:.1f}"
    return "➡️ 0.0"

def fmt_temp(val, unit_mode, unit_label):
    """Standardized temperature formatter with unit labels."""
    if pd.isnull(val): 
        return "N/A"
    v = (val - 32) * 5/9 if unit_mode == "Celsius" else val
    return f"{v:.1f}{unit_label}"

def get_unit_labels():
    """Returns the current unit mode and its corresponding string label."""
    unit_mode = st.session_state['unit_mode']
    unit_label = "°C" if unit_mode == "Celsius" else "°F"
    return unit_mode, unit_label
    

# ===============================================================
# Function: Status Dashboard
# ===============================================================
def render_project_status_dashboard(client, selected_project, unit_label):
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
        FROM `{PROJECT_ID}.{DATASET_ID}.node_registry` n
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.master_data_view` m ON n.NodeNum = m.NodeNum
        WHERE n.Project = @proj_id AND n.End_Date IS NULL
        GROUP BY 1, 2, 3, 4, 5
    """
    
    df = client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)]
    )).to_dataframe()

    if df.empty:
        st.error("No active nodes found for dashboard summary.")
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
            
            # Tile Logic
            latest_time = g_df['latest_ts'].max()
            ts_check = latest_time if latest_time.tzinfo else latest_time.tz_localize('UTC')
            lag_hrs = (now_utc - ts_check).total_seconds() / 3600
            
            val = g_df['avg_now'].mean() if pd.notnull(g_df['avg_now'].mean()) else g_df['latest_temp'].mean()
            
            if lag_hrs > 1.1: 
                st.subheader(f"⚠️ Offline {int(lag_hrs)}h")
            else: 
                st.title(f"{val:.1f}{unit_label}")
            
            st.write(f"**{int(g_df['avg_now'].notnull().sum())} / {len(g_df)}** Active (1h)")
            st.write(f"**{int((g_df['pings_24h'] > 0).sum())} / {len(g_df)}** Active (24h)")
            
            st.caption(f"Cur: {g_df['min_now'].min():.1f} to {g_df['max_now'].max():.1f}{unit_label}")
            st.caption(f"24h: {g_df['min_24h'].min():.1f} to {g_df['max_24h'].max():.1f}{unit_label}")
            
            t_row = st.columns(2)
            t_row[0].caption(f"1h\n{get_trend_arrow(val, g_df['avg_1h_prev'].mean())}")
            t_row[1].caption(f"6h\n{get_trend_arrow(val, g_df['avg_6h_prev'].mean())}")
# ===============================================================
# Function: Hardware integrity table
# ===============================================================
def render_hardware_integrity_table(client, selected_project, unit_mode, unit_label):
    """
    Renders a detailed table showing connectivity, coverage, and recent activity 
    for all active nodes in the selected project.
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
            (COUNT(DISTINCT TIMESTAMP_TRUNC(m.timestamp, HOUR)) / 24.0) * 100 as coverage_24h,
            AVG(CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN m.temperature END) as avg_now,
            AVG(CASE WHEN m.timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN m.temperature END) as avg_1h_prev
        FROM `{PROJECT_ID}.{DATASET_ID}.node_registry` n
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.master_data_view` m ON n.NodeNum = m.NodeNum
        WHERE n.Project = @proj_id AND n.End_Date IS NULL
        GROUP BY 1, 2, 3, 4, 5
    """
    
    df = client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)]
    )).to_dataframe()

    if df.empty: 
        return

    now_utc = pd.Timestamp.now(tz='UTC')

    def row_processor(row):
        # Last Seen Text/Style
        ping = row['last_ping']
        if pd.isnull(ping):
            txt, style = "❌ Never", "background-color: #d3d3d3"
        else:
            diff = (now_utc - (ping if ping.tzinfo else ping.tz_localize('UTC'))).total_seconds() / 60
            if diff <= 15: 
                txt, style = f"{int(diff)}m ago", "background-color: #ccffcc; color: black"
            elif diff <= 60: 
                txt, style = f"{int(diff)}m ago", "background-color: #ffe4b5; color: black"
            else: 
                txt, style = f"{round(diff/60, 1)}h ago", "background-color: #ffcccb; color: black"
        
        pos = f"{row['Depth']}ft" if pd.notnull(row['Depth']) else f"Bank {row['Bank']}"
        trend = get_trend_arrow(row['avg_now'], row['avg_1h_prev'])
        return pd.Series([txt, style, pos, trend])

    df[['Seen_Text', 'Seen_Style', 'Pos_Label', 'Trend']] = df.apply(row_processor, axis=1)

    # Table Layout
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

    # Diagnostic Styler
    def diagnostic_styler(data):
        style_df = pd.DataFrame('', index=data.index, columns=data.columns)
        ref = df.set_index('NodeNum')
        for i, row in data.iterrows():
            nid = row['Node ID']
            # Highlight Node ID if in Diagnostic status
            if ref.loc[nid, 'SensorStatus'] == 'Diagnostic':
                style_df.loc[i, 'Node ID'] = 'background-color: #ff4b4b; color: white; font-weight: bold;'
            # Apply the connectivity color coding to the Last Seen column
            style_df.loc[i, 'Last Seen'] = ref.loc[nid, 'Seen_Style']
        return style_df

    st.dataframe(
        display_df.style.apply(diagnostic_styler, axis=None), 
        use_container_width=True, 
        hide_index=True
    )
    
# ===============================================================
# PAGE: SENSOR STATUS (Modular Functions)
# ===============================================================

def render_sensor_status_page(client, reg_df, selected_project, PROJECT_ID, DATASET_ID):
    """Main entry point for the Sensor Status page."""
    st.header("🔍 Sensor Status & Performance Overview")
    
    # Direct production target
    target_registry = f"{PROJECT_ID}.{DATASET_ID}.node_registry"

    # 1. Fleet Inventory Metrics
    render_fleet_inventory_metrics(reg_df)
    st.divider()

    # 2. Location Drill-Down
    render_location_drilldown(client, reg_df, selected_project, target_registry, PROJECT_ID, DATASET_ID)
    st.divider()

    # 3. Hardware Investigator
    render_hardware_investigator(client, reg_df, target_registry, PROJECT_ID, DATASET_ID)
    st.divider()

    # 4. Registry Health
    render_registry_health_check(client, target_registry)


def render_fleet_inventory_metrics(reg_df):
    """Calculates and displays top-level fleet statistics."""
    if not reg_df.empty:
        # Pre-process dates for filtering
        reg_df['End_Date'] = pd.to_datetime(reg_df['End_Date'], errors='coerce')
        active_mask = reg_df['End_Date'].isna()
        
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Unique Sensors", reg_df['NodeNum'].nunique())
        m2.metric("Currently Assigned", len(reg_df[active_mask & (reg_df['Project'] != 'Office')]))
        m3.metric("Available In Stock", len(reg_df[active_mask & (reg_df['Project'] == 'Office')]))
        
        # Count critical statuses
        bad_status_count = len(reg_df[active_mask & reg_df['SensorStatus'].isin(['Dead', 'Flagged', 'Diagnostic'])])
        m4.metric("Diagnostic/Dead", bad_status_count)


def render_location_drilldown(client, reg_df, selected_project, target_registry, PROJECT_ID, DATASET_ID):
    """Displays project locations and allows expanding to see individual nodes."""
    st.subheader(f"📍 Location Drill-Down: {selected_project}")
    
    loc_q = f"""
        SELECT 
            n.Location, 
            COUNT(n.NodeNum) as pipe_count,
            AVG(m.temperature) as avg_temp,
            COUNTIF(m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)) as seen_6h
        FROM `{target_registry}` n
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.master_data_view` m ON n.NodeNum = m.NodeNum
        WHERE n.Project = @proj_id AND n.End_Date IS NULL
        GROUP BY n.Location
    """
    
    loc_df = client.query(loc_q, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)]
    )).to_dataframe()

    if loc_df.empty:
        st.info("No active locations found for this project.")
        return

    for _, row in loc_df.iterrows():
        loc_name = row['Location']
        avg_t = f"{row['avg_temp']:.1f}°F" if pd.notnull(row['avg_temp']) else "N/A"
        
        with st.expander(f"📁 {loc_name} | {row['pipe_count']} Pipes | Avg: {avg_t}"):
            # Filter main registry for specific nodes in this location
            pipe_df = reg_df[(reg_df['Project'] == selected_project) & 
                             (reg_df['Location'] == loc_name) & 
                             (reg_df['End_Date'].isna())].copy()
            
            display_pipes = pipe_df[['NodeNum', 'Bank', 'Depth', 'SensorStatus']].rename(
                columns={'NodeNum': 'Pipe ID', 'SensorStatus': 'Status'}
            )
            
            # Highlight diagnostic nodes in red
            st.dataframe(
                display_pipes.style.map(
                    lambda val: 'color: red' if val == 'Diagnostic' else 'color: black', 
                    subset=['Status']
                ),
                use_container_width=True,
                hide_index=True
            )


def render_hardware_investigator(client, reg_df, target_registry, PROJECT_ID, DATASET_ID):
    """Provides a global search tool to investigate a specific Node ID's history and data."""
    st.subheader("🔦 Global Hardware Investigator")
    search_node = st.text_input("Quick Search Node ID (e.g., TP-0009)").strip().upper()
    
    if not search_node:
        return

    match = reg_df[reg_df['NodeNum'].astype(str).str.upper() == search_node]
    
    if match.empty:
        st.error(f"Node '{search_node}' not found in registry.")
        return

    # A. Current Assignment info
    curr = match[match['End_Date'].isna()]
    if not curr.empty:
        st.info(f"📍 **Current Assignment:** {curr.iloc[0]['Project']} | {curr.iloc[0]['Location']} ({curr.iloc[0]['SensorStatus']})")
    
    # B. Historical Deployment Table
    st.markdown("### 📜 Deployment History")
    history_q = f"""
        SELECT Project, Location, Start_Date, End_Date, SensorStatus,
        (SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m 
         WHERE m.NodeNum = r.NodeNum AND m.timestamp BETWEEN CAST(r.Start_Date AS TIMESTAMP) AND IFNULL(CAST(r.End_Date AS TIMESTAMP), CURRENT_TIMESTAMP())) as pings
        FROM `{target_registry}` r WHERE NodeNum = '{search_node}' ORDER BY Start_Date DESC
    """
    st.dataframe(client.query(history_q).to_dataframe(), use_container_width=True, hide_index=True)

    # C. Plotly Lifetime Graph
    st.markdown("### 📈 Lifetime Thermal Profile")
    tel_df = client.query(
        f"SELECT timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` WHERE NodeNum = '{search_node}' ORDER BY timestamp ASC"
    ).to_dataframe()
    
    if not tel_df.empty:
        fig = go.Figure(go.Scatter(x=tel_df['timestamp'], y=tel_df['temperature'], mode='lines', line=dict(color='#00d4ff')))
        fig.update_layout(height=300, template="plotly_dark", margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.caption("No telemetry data available for this node.")


def render_registry_health_check(client, target_registry):
    """Checks for data integrity issues in the registry table."""
    with st.expander("🛠️ Registry Integrity Check"):
        health_df = client.query(
            f"SELECT NodeNum, PhysicalID, Project, Start_Date FROM `{target_registry}` WHERE Start_Date IS NULL"
        ).to_dataframe()
        
        if health_df.empty:
            st.success("✅ Registry Integrity looks good!")
        else:
            st.warning("⚠️ Found orphaned records (Missing Start Dates):")
            st.dataframe(health_df, use_container_width=True)

# ===============================================================
# PAGE: SENSOR REPLACE (Physical Swap Logic)
# ===============================================================

def render_sensor_replace_page(client, PROJECT_ID, DATASET_ID):
    """Main entry point for the Sensor Replace management tool."""
    st.header("🔄 Sensor Replace")
    st.info("Use this tool when physically swapping out hardware on-site while maintaining the same Node ID.")
    
    target_registry = f"{PROJECT_ID}.{DATASET_ID}.node_registry"
    
    # 1. LOAD ACTIVE INVENTORY
    active_reg = client.query(f"SELECT * FROM `{target_registry}` WHERE End_Date IS NULL").to_dataframe()

    # 2. SEARCH & IDENTIFY
    search_node = st.text_input("🔍 Search Node ID or Current Serial", placeholder="e.g., TP-0001 or 12345.67")
    found_row = None
    
    if search_node:
        found_row = identify_active_node(active_reg, search_node)

    if found_row is not None:
        st.divider()
        st.subheader(f"⚡ Verification: {found_row['NodeNum']}")
        
        # 3. VISUAL OVERLAP CHECK
        new_sn = render_comparison_charts(client, found_row, PROJECT_ID, DATASET_ID)

        # 4. FINAL TRANSACTION FORM
        render_replacement_form(client, found_row, new_sn, target_registry)


def identify_active_node(active_reg, search_term):
    """Filters the active registry to find a specific node by ID or Physical Serial."""
    search_clean = str(search_term).strip().upper()
    match = active_reg[
        (active_reg['NodeNum'].astype(str).str.upper().str.contains(search_clean)) | 
        (active_reg['PhysicalID'].astype(str).str.contains(search_clean))
    ]
    if not match.empty:
        row = match.iloc[0]
        st.success(f"📍 Node identified at: **{row['Project']} | {row['Location']}**")
        return row
    else:
        st.error("No active record found matching that ID.")
        return None


def render_comparison_charts(client, found_row, PROJECT_ID, DATASET_ID):
    """Renders charts for old vs new hardware to verify telemetry before committing."""
    col_g1, col_g2 = st.columns(2)
    
    with col_g1:
        st.markdown(f"**Old Hardware** (S/N: {found_row['PhysicalID']})")
        old_q = f"""
            SELECT timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` 
            WHERE NodeNum = '{found_row['NodeNum']}' 
            AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 3 DAY 
            ORDER BY timestamp
        """
        old_data = client.query(old_q).to_dataframe()
        if not old_data.empty:
            fig_old = go.Figure(go.Scatter(x=old_data['timestamp'], y=old_data['temperature'], name="Old Node", line=dict(color='#888888')))
            fig_old.update_layout(height=200, margin=dict(t=0,b=0), template="plotly_dark")
            st.plotly_chart(fig_old, use_container_width=True)

    new_sn = st.text_input("Enter NEW Hardware Serial Number (Physical ID)")

    with col_g2:
        if new_sn:
            st.markdown(f"**New Hardware** (S/N: {new_sn})")
            new_q = f"""
                SELECT timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` 
                WHERE SAFE_CAST(PhysicalID AS STRING) LIKE '%{new_sn}%' 
                AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 3 DAY 
                ORDER BY timestamp
            """
            new_data = client.query(new_q).to_dataframe()
            if not new_data.empty:
                fig_new = go.Figure(go.Scatter(x=new_data['timestamp'], y=new_data['temperature'], name="New Node", line=dict(color='orange')))
                fig_new.update_layout(height=200, margin=dict(t=0,b=0), template="plotly_dark")
                st.plotly_chart(fig_new, use_container_width=True)
            else:
                st.caption("No recent telemetry seen for this new Serial Number yet.")
    
    return new_sn


def render_replacement_form(client, found_row, new_sn, target_registry):
    """Renders the final confirmation form and executes the BQ transaction."""
    st.divider()
    with st.form("replacement_commit_form"):
        st.write("### 🚀 Commit Hardware Swap")
        st.warning("This will end the current record and create a new assignment for this Node ID.")
        
        replace_date = st.date_input("Actual Swap Date", value=datetime.now().date())
        confirm_check = st.checkbox("I verify the new hardware is communicating and the old hardware is removed.")
        
        if st.form_submit_button("EXECUTE REPLACEMENT"):
            clean_sn = re.sub(r'[^0-9.]', '', str(new_sn))
            
            if not clean_sn or not confirm_check:
                st.error("Missing Serial Number or verification checkbox.")
            else:
                execute_replacement_transaction(client, found_row, clean_sn, replace_date, target_registry)


def execute_replacement_transaction(client, found_row, clean_sn, replace_date, target_registry):
    """Performs the SQL transaction to close the old node record and open the new one."""
    try:
        date_str = replace_date.isoformat()
        sql = f"""
        BEGIN TRANSACTION;
        
        -- 1. Close old assignment
        UPDATE `{target_registry}` 
        SET End_Date = DATE('{date_str}'), 
            SensorStatus = 'Replaced' 
        WHERE NodeNum = '{found_row['NodeNum']}' 
          AND Project = '{found_row['Project']}'
          AND End_Date IS NULL;
        
        -- 2. Open new assignment
        INSERT INTO `{target_registry}` 
        (NodeNum, PhysicalID, Project, Location, Bank, Depth, Start_Date, SensorStatus)
        VALUES (
            '{found_row['NodeNum']}', 
            SAFE_CAST('{clean_sn}' AS FLOAT64), 
            '{found_row['Project']}', 
            '{found_row['Location']}', 
            '{found_row.get('Bank', '')}', 
            {float(found_row['Depth']) if pd.notnull(found_row['Depth']) else 'NULL'}, 
            DATE('{date_str}'), 
            'Active'
        );
        
        COMMIT;
        """
        client.query(sql).result()
        st.success(f"Successfully replaced {found_row['NodeNum']} with S/N {clean_sn}")
        st.balloons()
        time.sleep(1.5)
        st.rerun()
        
    except Exception as e:
        st.error(f"Hardware Replacement Failed: {e}")

# ===============================================================
# PAGE: SENSOR SWITCH (Correction Logic)
# ===============================================================

def render_sensor_switch_page(client, PROJECT_ID, DATASET_ID):
    """
    Main entry point for the Sensor Switch tool. 
    Used for metadata corrections (typos) without affecting history.
    """
    st.header("🩹 Sensor Designation Switch")
    st.info("""
        **Purpose:** Use this for metadata corrections only (e.g., a typo during setup). 
        This will update the existing active record without changing start dates or history.
    """)
    
    target_registry = f"{PROJECT_ID}.{DATASET_ID}.node_registry"
    
    node_id = st.text_input("Enter Node ID to correct (e.g., TP-0001)").strip().upper()
    
    if node_id:
        process_sensor_switch(client, node_id, target_registry)


def process_sensor_switch(client, node_id, target_registry):
    """Handles the lookup and update logic for correcting a sensor's Physical ID."""
    # We only want to switch IDs for active assignments
    query = f"SELECT * FROM `{target_registry}` WHERE NodeNum = '{node_id}' AND End_Date IS NULL"
    df = client.query(query).to_dataframe()
    
    if not df.empty:
        row = df.iloc[0]
        
        # Display current state
        st.subheader(f"Current Config: {node_id}")
        c1, c2, c3 = st.columns(3)
        c1.write(f"**Project:** {row['Project']}")
        c2.write(f"**Location:** {row['Location']}")
        c3.write(f"**Physical ID:** `{row['PhysicalID']}`")
        
        st.divider()
        
        # Input for correction
        new_id = st.text_input("Enter Corrected Physical ID (Serial Number)")
        
        if st.button("🚀 Apply Designation Correction"):
            if not new_id:
                st.warning("Please provide a new Physical ID.")
            else:
                execute_switch_update(client, node_id, row['Project'], new_id, target_registry)
    else:
        st.error(f"No active record found for Node '{node_id}'. Verify the ID or check the Sensor Status page.")


def execute_switch_update(client, node_id, project, new_id, target_registry):
    """Executes the SQL update to correct the PhysicalID in BigQuery."""
    # Clean the input to ensure it's numeric for FLOAT64 column
    clean_id = re.sub(r'[^0-9.]', '', str(new_id))
    
    update_sql = f"""
        UPDATE `{target_registry}`
        SET PhysicalID = SAFE_CAST('{clean_id}' AS FLOAT64)
        WHERE NodeNum = '{node_id}' 
          AND Project = '{project}'
          AND End_Date IS NULL
    """
    
    try:
        with st.spinner("Correcting designation..."):
            client.query(update_sql).result()
        st.success(f"Successfully updated {node_id} to Physical ID: {clean_id}")
        time.sleep(1.5)
        st.rerun()
    except Exception as e:
        st.error(f"Update failed: {e}")
# ===============================================================
# PAGE: SENSOR EDIT (Interactive Registry Editor)
# ===============================================================

def render_sensor_edit_page(client, reg_df, PROJECT_ID, DATASET_ID):
    """Main entry point for the interactive registry editor."""
    st.header("📝 Sensor Edit")
    st.info("Use this tool to modify location metadata or remove incorrect registry entries.")
    
    target_registry = f"{PROJECT_ID}.{DATASET_ID}.node_registry"

    # 1. Advanced Filtering UI
    filtered_df = render_sensor_edit_filters(reg_df)

    # 2. Interactive Table Selection
    st.write(f"Showing **{len(filtered_df)}** matching records.")
    
    selected_rows = st.dataframe(
        filtered_df,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row"
    )

    # 3. Form Logic for Selected Record
    if len(selected_rows.selection.rows) > 0:
        row_index = selected_rows.selection.rows[0]
        selected_data = filtered_df.iloc[row_index]
        render_edit_record_form(client, selected_data, target_registry)
    else:
        st.info("💡 Select a row in the table above to edit its details.")


def render_sensor_edit_filters(reg_df):
    """Renders the filter sidebar/columns and returns the filtered dataframe."""
    st.subheader("🔍 Find & Select Record")
    
    show_archived = st.checkbox("Show Archived/Historical Data", value=False)
    
    col_f1, col_f2, col_f3, col_f4 = st.columns(4)
    
    with col_f1:
        u_projects = ["All"] + sorted(reg_df['Project'].unique().tolist())
        sel_proj = st.selectbox("Filter by Project", u_projects)
        
    with col_f2:
        search_node = st.text_input("Search Node ID", "").strip().upper()

    with col_f3:
        u_locs = ["All"] + sorted(reg_df['Location'].unique().tolist())
        sel_loc = st.selectbox("Filter by Location", u_locs)
        
    with col_f4:
        u_status = ["All"] + sorted(reg_df['SensorStatus'].unique().tolist())
        sel_stat = st.selectbox("Filter by Status", u_status)

    # Apply Logic
    df = reg_df.copy()
    
    if not show_archived:
        df = df[df['End_Date'].isna()]
        df = df[df['SensorStatus'] != "Archived"]

    if sel_proj != "All":
        df = df[df['Project'] == sel_proj]
    if sel_loc != "All":
        df = df[df['Location'] == sel_loc]
    if sel_stat != "All":
        df = df[df['SensorStatus'] == sel_stat]
    if search_node:
        df = df[df['NodeNum'].str.upper().str.contains(search_node)]
        
    return df


def render_edit_record_form(client, data, target_registry):
    """Renders the update/delete form for a specific registry record."""
    st.divider()
    with st.form("edit_entry_form"):
        st.subheader(f"🛠️ Modifying {data['NodeNum']}")
        st.caption(f"Entry ID: {data['PhysicalID']} | Started: {data['Start_Date']}")
        
        # Edit inputs
        new_loc = st.text_input("Update Location", value=str(data['Location']))
        
        status_list = ["Active", "Available", "Archived", "Dead", "Diagnostic", "Moved", "Replaced"]
        try:
            current_stat_idx = status_list.index(data['SensorStatus'])
        except ValueError:
            current_stat_idx = 0
            
        new_status = st.selectbox("Update Status", status_list, index=current_stat_idx)
        
        c1, c2 = st.columns(2)
        
        if c1.form_submit_button("💾 Save Changes"):
            execute_record_update(client, data, new_loc, new_status, target_registry)

        if c2.form_submit_button("🗑️ DELETE RECORD", type="primary"):
            execute_record_delete(client, data, target_registry)


def execute_record_update(client, data, new_loc, new_status, target_registry):
    """Executes the BigQuery UPDATE for a specific record."""
    update_sql = f"""
        UPDATE `{target_registry}`
        SET Location = '{new_loc}', 
            SensorStatus = '{new_status}'
        WHERE NodeNum = '{data['NodeNum']}' 
          AND Start_Date = DATE('{data['Start_Date']}')
          AND PhysicalID = {data['PhysicalID']}
    """
    try:
        client.query(update_sql).result()
        st.success(f"Successfully updated {data['NodeNum']}")
        time.sleep(1)
        st.rerun()
    except Exception as e:
        st.error(f"Update failed: {e}")


def execute_record_delete(client, data, target_registry):
    """Executes the BigQuery DELETE for a specific record."""
    delete_sql = f"""
        DELETE FROM `{target_registry}` 
        WHERE NodeNum = '{data['NodeNum']}' 
          AND Start_Date = DATE('{data['Start_Date']}')
          AND PhysicalID = {data['PhysicalID']}
    """
    try:
        client.query(delete_sql).result()
        st.warning(f"Deleted {data['NodeNum']} record from registry.")
        time.sleep(1)
        st.rerun()
    except Exception as e:
        st.error(f"Deletion failed: {e}")
# ===============================================================
# PAGE: DATA RECOVERY (SensorPush API Bridge)
# ===============================================================

def render_data_recovery_page(reg_df):
    """Main entry point for the Data Recovery page."""
    st.header("📡 Data Recovery")
    st.info("Triggers the Cloud Run service to backfill missing telemetry from the SensorPush API.")

    # 1. GATEWAY: Filter for SensorPush hardware only (TP-Prefix)
    sp_reg = reg_df[reg_df['NodeNum'].str.startswith('TP', na=False)].copy()

    # 2. FILTERING UI
    selected_nodes = render_recovery_filters(sp_reg)

    # 3. DATE RANGE & TRIGGER
    st.divider()
    c_d1, c_d2 = st.columns(2)
    with c_d1:
        start_date = st.date_input("Recovery Start Date", value=datetime.now() - timedelta(days=3))
    with c_d2:
        end_date = st.date_input("Recovery End Date", value=datetime.now())

    if st.button("🚀 Run Recovery Service", type="primary"):
        handle_recovery_trigger(selected_nodes, start_date, end_date)

    # 4. SYSTEM LOGIC FOOTER
    render_recovery_logic_footer()


def render_recovery_filters(sp_reg):
    """Renders the hierarchical filters and returns the list of selected Node IDs."""
    st.subheader("🔍 Select Target Hardware")
    col_f1, col_f2, col_f3 = st.columns(3)
    
    with col_f1:
        u_projects = ["All"] + sorted(sp_reg['Project'].unique().tolist())
        rec_proj = st.selectbox("Filter by Project", u_projects)
    
    proj_filtered = sp_reg if rec_proj == "All" else sp_reg[sp_reg['Project'] == rec_proj]
    
    with col_f2:
        u_locs = ["All"] + sorted(proj_filtered['Location'].unique().tolist())
        rec_loc = st.selectbox("Filter by Location", u_locs)
        
    with col_f3:
        loc_filtered = proj_filtered if rec_loc == "All" else proj_filtered[proj_filtered['Location'] == rec_loc]
        available_nodes = sorted(loc_filtered['NodeNum'].unique().tolist())
        
        selected_nodes = st.multiselect(
            "Select Node Numbers", 
            available_nodes, 
            default=available_nodes if len(available_nodes) < 10 else None,
            help="Choose the specific sensors to backfill."
        )
    return selected_nodes


def handle_recovery_trigger(selected_nodes, start_date, end_date):
    """Manages the API request to the Cloud Run recovery service."""
    if not selected_nodes:
        st.error("Operation Aborted: No sensors selected for recovery.")
        return

    cloud_run_url = "https://sensorpushtobigquery-1013288934882.us-west1.run.app/recover_data"
    
    payload = {
        "start": start_date.strftime("%Y-%m-%d"),
        "end": end_date.strftime("%Y-%m-%d"),
        "nodes": ",".join(selected_nodes)
    }

    with st.spinner(f"Requesting data for {len(selected_nodes)} sensors..."):
        try:
            import requests
            response = requests.get(cloud_run_url, params=payload, timeout=300)
            
            if response.status_code == 200:
                st.success("✅ Recovery Triggered Successfully")
                st.code(response.text)
            else:
                st.error(f"Cloud Service Error ({response.status_code}): {response.text}")
        except ImportError:
            st.error("System Error: 'requests' library is not installed. Contact Engineering.")
        except Exception as e:
            st.error(f"Connectivity Failure: {e}")


def render_recovery_logic_footer():
    """Renders documentation on how the recovery process functions."""
    st.divider()
    with st.expander("🛠️ How the Recovery Engine Works"):
        st.markdown(f"""
        1. **Filtered Registry**: This tool only views sensors starting with `TP` (SensorPush).
        2. **API Handshake**: The app sends the `start`, `end`, and `nodes` parameters to a secure GCP Cloud Run endpoint.
        3. **Processing**: Cloud Run fetches raw data from the SensorPush Cloud API and pushes it directly into BigQuery `raw_sensorpush`.
        4. **Verification**: Once finished, data will propagate to the `master_data_view` within minutes.
        """)
    
# ===============================================================
# PAGE: PROJECT MASTER
# ===============================================================

def render_project_master_page(client, selected_project, PROJECT_ID, DATASET_ID):
    """Main entry point for Project Lifecycle Management."""
    st.header("⚙️ Project Lifecycle Management")
    
    action = st.radio("Action", ["Overview", "New Project", "Update Existing"], horizontal=True)
    table_projects = f"{PROJECT_ID}.{DATASET_ID}.project_registry"

    if action == "Overview":
        render_project_overview(client, table_projects)

    elif action == "New Project":
        render_new_project_form(client, table_projects)

    elif action == "Update Existing":
        render_update_project_form(client, selected_project, table_projects)


def render_project_overview(client, table_projects):
    """Displays a list of all active (non-archived) projects."""
    st.subheader("📋 Project Fleet Status")
    query = f"""
        SELECT Project, ProjectStatus, Date_Freezedown, EngNotes 
        FROM `{table_projects}` 
        WHERE ProjectStatus != 'Archived' 
        ORDER BY Project ASC
    """
    df = client.query(query).to_dataframe()
    
    if not df.empty:
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No active projects found in the registry.")


def render_new_project_form(client, table_projects):
    """UI and logic for registering a new project ID."""
    st.subheader("🏗️ Register New Project Code")
    with st.form("new_project_form"):
        n_code = st.text_input("Project ID (e.g., 2538)")
        n_notes = st.text_area("Initial Engineering Notes")
        
        if st.form_submit_button("🚀 Initialize Project"):
            if not n_code:
                st.error("Project ID is required.")
            else:
                # Check for duplicates
                check_q = f"SELECT Project FROM `{table_projects}` WHERE Project = '{n_code}'"
                if not client.query(check_q).to_dataframe().empty:
                    st.error(f"Project {n_code} already exists in the registry.")
                else:
                    insert_q = f"""
                        INSERT INTO `{table_projects}` (Project, ProjectStatus, EngNotes)
                        VALUES ('{n_code}', 'Initialized', '{n_notes}')
                    """
                    client.query(insert_q).result()
                    st.success(f"Project {n_code} successfully initialized.")
                    time.sleep(1)
                    st.rerun()


def render_update_project_form(client, selected_project, table_projects):
    """UI and logic for updating lifecycle status and notes for the selected project."""
    st.subheader(f"⚙️ Modifying: {selected_project}")
    
    proj_q = f"SELECT * FROM `{table_projects}` WHERE Project = '{selected_project}'"
    p_res = client.query(proj_q).to_dataframe()
    
    if p_res.empty:
        st.error("Project not found in registry.")
        return

    p_data = p_res.iloc[0]
    status_options = ["Initialized", "Pre-freeze", "Freezedown", "Maintenance", "Archived"]
    
    # Safe index logic for current status
    current_status = p_data.get('ProjectStatus', 'Initialized')
    try:
        default_status_idx = status_options.index(current_status)
    except ValueError:
        default_status_idx = 0

    with st.form("edit_project"):
        u_status = st.selectbox("Update Lifecycle Status", status_options, index=default_status_idx)
        u_notes = st.text_area("Update Engineering Notes", value=p_data.get('EngNotes', ''))
        
        if st.form_submit_button("💾 Save Project Rules"):
            # Automated date stamping for Freezedown
            date_sql = ""
            if u_status == "Freezedown" and pd.isnull(p_data['Date_Freezedown']):
                date_sql = ", Date_Freezedown = CURRENT_DATE()"
            
            update_q = f"""
                UPDATE `{table_projects}` 
                SET ProjectStatus='{u_status}', EngNotes='{u_notes}' {date_sql} 
                WHERE Project='{selected_project}'
            """
            client.query(update_q).result()
            st.success(f"✅ Project {selected_project} updated.")
            time.sleep(1)
            st.rerun()
# ===============================================================
# PAGE: PROJECT MASTER
# ===============================================================

def render_project_master_page(client, selected_project, PROJECT_ID, DATASET_ID):
    """
    Main entry point for Project Lifecycle Management.
    """
    st.header("⚙️ Project Lifecycle Management")
    
    # Navigation matching the project lifecycle flow
    action = st.radio("Action", ["Overview", "New Project", "Update Existing"], horizontal=True)
    table_projects = f"{PROJECT_ID}.{DATASET_ID}.project_registry"

    if action == "Overview":
        render_project_overview(client, table_projects)

    elif action == "New Project":
        render_new_project_form(client, table_projects)

    elif action == "Update Existing":
        render_update_project_form(client, selected_project, table_projects)


def render_project_overview(client, table_projects):
    """
    Displays a scannable list of all active (non-archived) projects.
    """
    st.subheader("📋 Project Fleet Status")
    
    # Fetch all projects not yet archived
    query = f"""
        SELECT Project, ProjectStatus, Date_Freezedown, EngNotes 
        FROM `{table_projects}` 
        WHERE ProjectStatus != 'Archived' 
        ORDER BY Project ASC
    """
    try:
        df = client.query(query).to_dataframe()
        if not df.empty:
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No active projects found in the registry.")
    except Exception as e:
        st.error(f"Failed to load project overview: {e}")


def render_new_project_form(client, table_projects):
    """
    UI and logic for registering a new project ID.
    """
    st.subheader("🏗️ Register New Project Code")
    with st.form("new_project_form"):
        n_code = st.text_input("Project ID (e.g., 2538)")
        n_notes = st.text_area("Initial Engineering Notes")
        
        if st.form_submit_button("🚀 Initialize Project"):
            if not n_code:
                st.error("Project ID is required.")
            else:
                # Check for duplicates to maintain registry integrity
                check_q = f"SELECT Project FROM `{table_projects}` WHERE Project = '{n_code}'"
                if not client.query(check_q).to_dataframe().empty:
                    st.error(f"Project {n_code} already exists in the registry.")
                else:
                    insert_q = f"""
                        INSERT INTO `{table_projects}` (Project, ProjectStatus, EngNotes)
                        VALUES ('{n_code}', 'Initialized', '{n_notes}')
                    """
                    try:
                        client.query(insert_q).result()
                        st.success(f"Project {n_code} successfully initialized.")
                        time.sleep(1)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to initialize project: {e}")


def render_update_project_form(client, selected_project, table_projects):
    """
    UI and logic for updating lifecycle status and notes for a selected project.
    """
    st.subheader(f"⚙️ Modifying: {selected_project}")
    
    # Fetch current data for the sidebar-selected project
    proj_q = f"SELECT * FROM `{table_projects}` WHERE Project = '{selected_project}'"
    p_res = client.query(proj_q).to_dataframe()
    
    if p_res.empty:
        st.error("Project not found in registry.")
        return

    p_data = p_res.iloc[0]
    status_options = ["Initialized", "Pre-freeze", "Freezedown", "Maintenance", "Archived"]
    
    # Safe index logic for current status
    current_status = p_data.get('ProjectStatus', 'Initialized')
    try:
        default_status_idx = status_options.index(current_status)
    except ValueError:
        default_status_idx = 0

    with st.form("edit_project"):
        u_status = st.selectbox("Update Lifecycle Status", status_options, index=default_status_idx)
        u_notes = st.text_area("Update Engineering Notes", value=p_data.get('EngNotes', ''))
        
        if st.form_submit_button("💾 Save Project Rules"):
            # Automated date stamping for Freezedown logic
            date_sql = ""
            if u_status == "Freezedown" and pd.isnull(p_data['Date_Freezedown']):
                date_sql = ", Date_Freezedown = CURRENT_DATE()"
            
            update_q = f"""
                UPDATE `{table_projects}` 
                SET ProjectStatus='{u_status}', EngNotes='{u_notes}' {date_sql} 
                WHERE Project='{selected_project}'
            """
            try:
                client.query(update_q).result()
                st.success(f"✅ Project {selected_project} updated.")
                time.sleep(1)
                st.rerun()
            except Exception as e:
                st.error(f"Failed to update project: {e}")
# ===============================================================
# PAGE: REF CURVE LIBRARY
# ===============================================================

def render_ref_curve_library_page(client, PROJECT_ID, DATASET_ID):
    """Main entry point for Theoretical Curve Management."""
    st.header("📈 Theoretical Curve Management")
    
    table_curves = f"{PROJECT_ID}.{DATASET_ID}.reference_curves"
    
    # 1. DATABASE SCHEMA CHECK & INVENTORY FETCH
    inventory_df = fetch_curve_inventory(client, table_curves, PROJECT_ID, DATASET_ID)
    
    st.divider()

    # 2. MANAGEMENT TOOLS (Delete & Wipe)
    render_curve_management_tools(client, inventory_df, table_curves)

    st.divider()

    # 3. BULK UPLOAD ENGINE
    render_curve_upload_engine(client, table_curves)


def fetch_curve_inventory(client, table_curves, PROJECT_ID, DATASET_ID):
    """Checks schema for compatibility and fetches current library stats."""
    inventory_df = pd.DataFrame()
    try:
        # Check for upload_date column to prevent query errors during transition
        schema_q = f"""
            SELECT column_name FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS` 
            WHERE table_name = 'reference_curves' AND column_name = 'upload_date'
        """
        has_date_col = not client.query(schema_q).to_dataframe().empty
        date_select = "MAX(upload_date)" if has_date_col else "CAST(NULL AS STRING)"
        
        inv_q = f"""
            SELECT 
                CurveID, 
                MAX(Day) as Max_Day, 
                COUNT(*) as Total_Points,
                {date_select} as Last_Upload
            FROM `{table_curves}`
            GROUP BY CurveID
            ORDER BY CurveID ASC
        """
        inventory_df = client.query(inv_q).to_dataframe()
        
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
        else:
            st.info("The library is currently empty. Upload curve CSVs below.")
    except Exception as e:
        st.error(f"Inventory Sync Error: {e}")
    
    return inventory_df


def render_curve_management_tools(client, inventory_df, table_curves):
    """Renders UI for deleting individual curves or purging the library."""
    c1, c2 = st.columns(2)
    
    with c1.expander("🗑️ Individual Curve Delete"):
        if not inventory_df.empty:
            to_delete = st.selectbox("Select Curve to Remove", sorted(inventory_df['CurveID'].tolist()))
            if st.button(f"Permanently Delete {to_delete}", type="primary"):
                try:
                    client.query(f"DELETE FROM `{table_curves}` WHERE CurveID = '{to_delete}'").result()
                    st.success(f"Removed {to_delete} from library.")
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    st.error(f"Delete failed: {e}")

    with c2.expander("🧨 Library Wipe"):
        st.warning("This will delete EVERY theoretical curve in the database.")
        if st.button("EXECUTE TOTAL PURGE", key="purge_all"):
            try:
                client.query(f"TRUNCATE TABLE `{table_curves}`").result()
                st.success("Library wiped successfully.")
                time.sleep(1)
                st.rerun()
            except Exception as e:
                st.error(f"Purge failed: {e}")


def render_curve_upload_engine(client, table_curves):
    """Handles CSV file uploads and BigQuery ingestion for new curves."""
    st.subheader("📤 Upload New Curves")
    u_files = st.file_uploader(
        "Upload Curve CSVs", 
        type=['csv'], 
        accept_multiple_files=True,
        help="Format: Data starts on Row 3. Column 1: Day, Column 2: Temp."
    )

    if u_files:
        if st.button("🚀 Commit Uploads to Database"):
            process_curve_uploads(client, u_files, table_curves)


def process_curve_uploads(client, u_files, table_curves):
    """Parses uploaded CSVs and appends cleaned data to BigQuery."""
    today_str = datetime.now().strftime('%Y-%m-%d')
    total_imported = 0
    
    for f in u_files:
        try:
            # Skip header rows (usually site info), take Day and Temp
            df = pd.read_csv(f, skiprows=2, usecols=[0, 1], names=['Day', 'Temp'])
            df['CurveID'] = f.name.rsplit('.', 1)[0]
            df['upload_date'] = today_str
            
            # Data Cleaning
            df['Day'] = pd.to_numeric(df['Day'], errors='coerce')
            df['Temp'] = pd.to_numeric(df['Temp'], errors='coerce')
            df = df.dropna(subset=['Day', 'Temp'])

            if not df.empty:
                job_config = bigquery.LoadTableConfig(write_disposition="WRITE_APPEND")
                client.load_table_from_dataframe(df, table_curves, job_config=job_config).result()
                total_imported += 1
        except Exception as e:
            st.error(f"Error processing {f.name}: {e}")

    if total_imported > 0:
        st.success(f"✅ Successfully imported {total_imported} curves.")
        time.sleep(1.5)
        st.rerun()

# ===============================================================
# PAGE: DATA MANAGEMENT (Flagging & Maintenance)
# ===============================================================

def render_data_management_page(client, reg_df, selected_project, PROJECT_ID, DATASET_ID):
    """Main entry point for Data Management (Approval & Flagging)."""
    st.header("🧨 Data Management (Approval & Flagging)")
    st.info("Use this tool to flag data as 'Bad' or 'Restricted' for engineering analysis without deleting the underlying records.")

    # 1. SCOPE & ACTION (Top Row)
    target_scope, mode = render_management_controls()
    st.divider()

    # 2. FILTERS (Middle Section)
    filters = render_management_filters(reg_df, selected_project, target_scope)
    
    # 3. SQL CONSTRUCTION
    where_str = build_management_where_clause(selected_project, target_scope, filters)
    
    # 4. VERIFICATION STEP
    render_verification_step(client, where_str, PROJECT_ID, DATASET_ID)

    # 5. EXECUTION STEP
    render_execution_step(client, where_str, mode, PROJECT_ID, DATASET_ID)


def render_management_controls():
    """Renders the top-level radio buttons for scope and action type."""
    c1, c2 = st.columns(2)
    with c1:
        target_scope = st.radio("Target Scope", ["Project Wide", "Specific Location", "Specific Node"], horizontal=True)
    with c2:
        mode = st.radio("Action Type", ["🚫 Mask (Flag as Bad)", "✅ Approve (Restore)"], horizontal=True)
    return target_scope, mode


def render_management_filters(reg_df, selected_project, target_scope):
    """Renders temporal, value, and scope-specific filters."""
    col_f1, col_f2, col_f3 = st.columns(3)
    
    with col_f1:
        temporal_dir = st.selectbox("Temporal Direction", ["Between Range", "Older Than", "Newer Than"])
        s_date = st.date_input("Start Date", value=datetime.now() - timedelta(days=7))
        e_date = st.date_input("End Date", value=datetime.now())

    with col_f2:
        val_filter = st.selectbox("Value Filter", ["No Threshold", "Above Threshold", "Below Threshold"])
        threshold = st.number_input("Threshold Value (°F)", value=100.0)

    with col_f3:
        scope_val = None
        if target_scope == "Specific Location":
            u_locs = sorted(reg_df[reg_df['Project'] == selected_project]['Location'].unique().tolist())
            scope_val = st.selectbox("Select Location", u_locs)
        elif target_scope == "Specific Node":
            u_nodes = sorted(reg_df[reg_df['Project'] == selected_project]['NodeNum'].unique().tolist())
            scope_val = st.selectbox("Select Node", u_nodes)
        else:
            st.write(f"**Target:** All nodes in {selected_project}")
            
    return {
        "temporal_dir": temporal_dir, "s_date": s_date, "e_date": e_date,
        "val_filter": val_filter, "threshold": threshold, "scope_val": scope_val
    }


def build_management_where_clause(selected_project, target_scope, f):
    """Constructs the WHERE string for the BigQuery UPDATE/SELECT queries."""
    where_clauses = [f"Project = '{selected_project}'"]
    
    # Temporal Logic
    if f["temporal_dir"] == "Between Range":
        where_clauses.append(f"timestamp BETWEEN '{f['s_date']}' AND '{f['e_date']}'")
    elif f["temporal_dir"] == "Older Than":
        where_clauses.append(f"timestamp < '{f['s_date']}'")
    elif f["temporal_dir"] == "Newer Than":
        where_clauses.append(f"timestamp > '{f['s_date']}'")
    
    # Threshold Logic
    if f["val_filter"] == "Above Threshold":
        where_clauses.append(f"temperature > {f['threshold']}")
    elif f["val_filter"] == "Below Threshold":
        where_clauses.append(f"temperature < {f['threshold']}")

    # Scope Logic
    if target_scope == "Specific Location":
        where_clauses.append(f"Location = '{f['scope_val']}'")
    elif target_scope == "Specific Node":
        where_clauses.append(f"NodeNum = '{f['scope_val']}'")

    return " AND ".join(where_clauses)


def render_verification_step(client, where_str, PROJECT_ID, DATASET_ID):
    """Queries BigQuery to show the user how many rows will be affected."""
    if st.button("🔍 Step 1: Verify Match Count"):
        count_q = f"SELECT COUNT(*) as total FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` WHERE {where_str}"
        try:
            res = client.query(count_q).to_dataframe()
            count = res.iloc[0]['total']
            st.metric("Points Found", f"{count:,}")
            st.session_state['data_ready'] = True if count > 0 else False
        except Exception as e:
            st.error(f"Verification Query Failed: {e}")


def render_execution_step(client, where_str, mode, PROJECT_ID, DATASET_ID):
    """Renders the final confirmation and executes the UPDATE query."""
    if st.checkbox("I confirm these data points should be flagged/updated in the master registry."):
        new_status = "Bad" if "Mask" in mode else "Approved"
        
        if st.button(f"🚀 Execute {mode}"):
            update_sql = f"""
                UPDATE `{PROJECT_ID}.{DATASET_ID}.master_data_view`
                SET ApprovalStatus = '{new_status}'
                WHERE {where_str}
            """
            try:
                with st.spinner("Updating data flags..."):
                    client.query(update_sql).result()
                st.success(f"Successfully flagged matching data as '{new_status}'.")
                st.balloons()
            except Exception as e:
                st.error(f"Execution Failed: {e}")

# ===============================================================
# FINAL EXECUTION BLOCK
# ===============================================================

def main():
    """
    This replaces the loose 'if admin_page' logic.
    It calls the sidebar, gets the variables, and routes to functions.
    """
    # 1. Initialize Sidebar and get context
    # This defines the variables that were causing the NameError
    admin_page, target_registry, selected_project, proj_list = render_sidebar()
    
    # 2. Get Unit Preferences
    unit_mode, unit_label = get_unit_labels()
    
    # 3. Load Registry Data for the pages that need it
    reg_df = load_registry_data(target_registry)

    # 4. Route to the appropriate function
    if admin_page == "📡 Setup Node Tool":
        render_project_status_dashboard(client, selected_project, unit_label)
        st.divider()
        render_hardware_integrity_table(client, selected_project, unit_mode, unit_label)

    elif admin_page == "🔍 Sensor Status":
        render_sensor_status_page(client, reg_df, selected_project, PROJECT_ID, DATASET_ID)

    elif admin_page == "🔄 Sensor Replace":
        render_sensor_replace_page(client, PROJECT_ID, DATASET_ID)

    elif admin_page == "🩹 Sensor Switch":
        render_sensor_switch_page(client, PROJECT_ID, DATASET_ID)

    elif admin_page == "📝 Sensor Edit":
        render_sensor_edit_page(client, reg_df, PROJECT_ID, DATASET_ID)

    elif admin_page == "📡 Data Recovery":
        render_data_recovery_page(reg_df)

    elif admin_page == "📦 Bulk Registry Manager":
        render_bulk_registry_page(client, proj_list, PROJECT_ID, DATASET_ID)

    elif admin_page == "⚙️ Project Master":
        render_project_master_page(client, selected_project, PROJECT_ID, DATASET_ID)

    elif admin_page == "📈 Ref Curve Library":
        render_ref_curve_library_page(client, PROJECT_ID, DATASET_ID)

    elif admin_page == "🧨 Data Management":
        render_data_management_page(client, reg_df, selected_project, PROJECT_ID, DATASET_ID)

# Start the app
if __name__ == "__main__":
    if client:
        main()
    else:
        st.error("Critical Error: BigQuery Client failed to initialize.")
        
# ===============================================================
# EXECUTION ENTRY POINT
# ===============================================================

if __name__ == "__main__":
    if client:
        main()
    else:
        st.error("Application cannot start: Database connection unavailable.")
