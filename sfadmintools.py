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

def render_sidebar():
    st.sidebar.title("🛠️ Admin Command Center")
    
    # Updated Navigation to prioritize the Unified Node Manager
    admin_page = st.sidebar.radio(
        "Management Tool", 
        [
            "🛠️ Node Manager",      # Primary tool
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

    # 1. Fetch Project List
    proj_q = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE ProjectStatus != 'Archived'"
    try:
        proj_df = client.query(proj_q).to_dataframe()
        proj_list = sorted(proj_df['Project'].tolist())
        
        # 2. Project Selection
        selected_project = st.sidebar.selectbox("🎯 Target Project Context", proj_list)
        
        # 3. Store metadata
        if not proj_df.empty:
            metadata = proj_df[proj_df['Project'] == selected_project].iloc[0].to_dict()
            st.session_state['project_metadata'] = metadata
            
        return admin_page, target_registry, selected_project, proj_list
    except Exception as e:
        st.sidebar.error(f"Error loading projects: {e}")
        return admin_page, target_registry, "None", ["Office"]

# ===============================================================
# 3. DATA LOADING
# ===============================================================
@st.cache_data(ttl=600)
def load_registry_data(target_table):
    """
    Queries BigQuery and ensures PhysicalID is handled correctly (casted to string or dropped).
    """
    try:
        df = client.query(f"SELECT * FROM `{target_table}`").to_dataframe()
        
        # Ensure PhysicalID doesn't cause float display issues if it exists
        if 'PhysicalID' in df.columns:
            df['PhysicalID'] = df['PhysicalID'].astype(str).replace(['nan', 'None', '<NA>'], '')
            
        return df
    except Exception as e:
        st.error(f"Error loading registry: {e}")
        return pd.DataFrame()

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
    
def natural_sort_key(s):
    """Helper to sort strings with numbers (e.g., Bank 2 before Bank 10)."""
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
# PAGE: SENSOR STATUS (Modular Functions)
# ===============================================================

def render_sensor_status(client, selected_project, unit_label, unit_mode, display_tz):
    """
    Enhanced Sensor Status: Peer Trend Analysis and Performance Scoring.
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

    # Optimized Query: Peer Trend partitioning
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
                
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR) THEN temperature END) - 
                MIN(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR) THEN temperature END) as swing_6h,
                
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

        def calculate_custom_metrics(row):
            peer_diff = abs(row['current_temp'] - row['current_peer_avg'])
            trend = "🎯 In-Line" if peer_diff < 2.0 else "⚠️ Drifting" if peer_diff < 5.0 else "🚨 Outlier"

            loc_upper = str(row['Location']).upper()
            is_sr = any(x in loc_upper for x in ['S', 'R']) and 'AMB' not in loc_upper
            s2, s6, s24 = row['swing_2h'], row['swing_6h'], row['swing_24h']
            
            if is_sr:
                perf = "❌ Volatile" if (s2 > 5 or s6 > 10 or s24 > 20) else "✅ Stable"
            else:
                perf = "❌ Unsteady" if (s2 > 1 or s6 > 1 or s24 > 2) else "✅ Solid"
            return pd.Series([trend, perf])

        df[['Peer Trend', 'Performance']] = df.apply(calculate_custom_metrics, axis=1)

        now_local = pd.Timestamp.now(tz=display_tz)
        df['hrs_lag'] = df['last_ping'].apply(
            lambda x: (now_local - (x if x.tzinfo else x.tz_localize('UTC')).tz_convert(display_tz)).total_seconds() / 3600
        )
        df['Status'] = df['hrs_lag'].apply(lambda x: f"🟢 {x:.1f}h" if x <= 1.1 else f"🔴 {x:.1f}h")

        st.subheader("🔍 Detailed Sensor Audit")
        st.dataframe(
            df[["Location", "NodeNum", "Peer Trend", "Performance", "Status", "coverage_24h"]].sort_values(['Location', 'NodeNum']),
            use_container_width=True, hide_index=True
        )

    except Exception as e:
        st.error(f"Sensor Status Error: {e}")

def render_fleet_inventory_metrics(reg_df):
    """Displays high-level fleet statistics."""
    if not reg_df.empty:
        active_mask = reg_df['End_Date'].isna()
        
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Unique Sensors", reg_df['NodeNum'].nunique())
        m2.metric("Currently Assigned", len(reg_df[active_mask & (reg_df['Project'] != 'Office')]))
        m3.metric("Available In Stock", len(reg_df[active_mask & (reg_df['Project'] == 'Office')]))
        
        bad_status_count = len(reg_df[active_mask & reg_df['SensorStatus'].isin(['Dead', 'Flagged', 'Diagnostic'])])
        m4.metric("Diagnostic/Dead", bad_status_count)

def render_hardware_investigator(client, reg_df, target_registry, PROJECT_ID, DATASET_ID):
    """Global Node ID search tool."""
    st.subheader("🔦 Global Hardware Investigator")
    search_node = st.text_input("Quick Search Node ID").strip().upper()
    
    if not search_node: return

    match = reg_df[reg_df['NodeNum'].astype(str).str.upper() == search_node]
    if match.empty:
        st.error(f"Node '{search_node}' not found.")
        return

    curr = match[match['End_Date'].isna()]
    if not curr.empty:
        st.info(f"📍 **Current Assignment:** {curr.iloc[0]['Project']} | {curr.iloc[0]['Location']} ({curr.iloc[0]['SensorStatus']})")
    
    st.markdown("### 📜 Deployment History")
    history_q = f"""
        SELECT Project, Location, Start_Date, End_Date, SensorStatus
        FROM `{target_registry}` WHERE NodeNum = '{search_node}' ORDER BY Start_Date DESC
    """
    st.dataframe(client.query(history_q).to_dataframe(), use_container_width=True, hide_index=True)

    st.markdown("### 📈 Lifetime Thermal Profile")
    tel_df = client.query(
        f"SELECT timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` WHERE NodeNum = '{search_node}' ORDER BY timestamp ASC"
    ).to_dataframe()
    
    if not tel_df.empty:
        fig = go.Figure(go.Scatter(x=tel_df['timestamp'], y=tel_df['temperature'], mode='lines', line=dict(color='#00d4ff')))
        fig.update_layout(height=300, template="plotly_dark", margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

def render_registry_health_check(client, target_registry):
    """Data integrity check (PhysicalID removed)."""
    with st.expander("🛠️ Registry Integrity Check"):
        health_df = client.query(
            f"SELECT NodeNum, Project, Start_Date FROM `{target_registry}` WHERE Start_Date IS NULL"
        ).to_dataframe()
        
        if health_df.empty:
            st.success("✅ Registry Integrity looks good!")
        else:
            st.warning("⚠️ Orphaned records (Missing Start Dates):")
            st.dataframe(health_df, use_container_width=True)

def execute_combined_correction(client, data, new_proj, new_loc, new_bank, new_depth, new_status, target_registry):
    """Corrects active record metadata by NodeNum only."""
    sql_depth = "NULL" if (pd.isna(new_depth) or new_depth == 0.0) else f"{float(new_depth)}"

    update_sql = f"""
        UPDATE `{target_registry}`
        SET Project = '{new_proj}', Location = '{new_loc}', Bank = '{new_bank}',
            Depth = {sql_depth}, SensorStatus = '{new_status}'
        WHERE NodeNum = '{data['NodeNum']}' AND End_Date IS NULL
    """
    
    try:
        client.query(update_sql).result()
        st.success(f"✅ Record updated for {data['NodeNum']}")
        st.cache_data.clear()
        time.sleep(1)
        st.rerun()
    except Exception as e:
        st.error(f"Correction failed: {e}")
        st.code(update_sql)

# ===============================================================
# PAGE: SENSOR REPLACE (Physical Swap Logic)
# ===============================================================

def render_comparison_charts(client, found_row, PROJECT_ID, DATASET_ID):
    """
    Renders charts for old vs new hardware to verify telemetry before committing.
    Now searches by NodeNum (the user-facing ID) instead of Serial Number.
    """
    col_g1, col_g2 = st.columns(2)
    
    with col_g1:
        st.markdown(f"**Old Hardware** ({found_row['NodeNum']})")
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

    # Search for the NEW hardware being prepared
    new_hw_node = st.text_input("Enter NEW Hardware Node ID (e.g., TP-XXXX)")

    with col_g2:
        if new_hw_node:
            st.markdown(f"**New Hardware** ({new_hw_node})")
            new_q = f"""
                SELECT timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` 
                WHERE NodeNum = '{new_hw_node}' 
                AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 3 DAY 
                ORDER BY timestamp
            """
            new_data = client.query(new_q).to_dataframe()
            if not new_data.empty:
                fig_new = go.Figure(go.Scatter(x=new_data['timestamp'], y=new_data['temperature'], name="New Node", line=dict(color='orange')))
                fig_new.update_layout(height=200, margin=dict(t=0,b=0), template="plotly_dark")
                st.plotly_chart(fig_new, use_container_width=True)
            else:
                st.caption(f"No recent telemetry seen for '{new_hw_node}' yet.")
    
    return new_hw_node

def execute_switch_correction(client, data, new_proj, new_loc, new_bank, new_depth, new_status, target_registry):
    """
    Metadata Correction: Strictly uses NodeNum. 
    """
    sql_depth = "NULL" if (pd.isna(new_depth) or new_depth == 0.0) else f"{float(new_depth)}"

    update_sql = f"""
        UPDATE `{target_registry}`
        SET 
            Project = '{new_proj}',
            Location = '{new_loc}', 
            Bank = '{new_bank}',
            Depth = {sql_depth},
            SensorStatus = '{new_status}'
        WHERE 
            NodeNum = '{data['NodeNum']}' 
            AND End_Date IS NULL
    """
    
    try:
        client.query(update_sql).result()
        st.success(f"✅ Metadata corrected for {data['NodeNum']}")
        st.cache_data.clear()
        time.sleep(1)
        st.rerun()
    except Exception as e:
        st.error("Correction Failed")
        st.code(update_sql, language="sql")
        st.error(str(e))

def render_replacement_form(client, found_row, new_hw_node, target_registry):
    """Renders final confirmation form for swapping hardware."""
    st.divider()
    with st.form("replacement_commit_form"):
        st.write("### 🚀 Commit Hardware Swap")
        st.warning(f"This will end the current record for {found_row['NodeNum']} and start a new assignment for {new_hw_node}.")
        
        replace_date = st.date_input("Actual Swap Date", value=datetime.now().date())
        confirm_check = st.checkbox("I verify the new hardware is communicating and the old hardware is removed.")
        
        if st.form_submit_button("EXECUTE REPLACEMENT"):
            if not new_hw_node or not confirm_check:
                st.error("Missing New Node ID or verification checkbox.")
            else:
                execute_replacement_transaction(client, found_row, new_hw_node, replace_date, target_registry)

def execute_replacement_transaction(client, data, new_node_num, swap_date, target_registry):
    """
    Hardware Swap: Closes current entry and starts a NEW one.
    PhysicalID is completely removed from this transaction.
    """
    date_str = swap_date.isoformat()

    sql = f"""
        BEGIN TRANSACTION;
        
        -- 1. Close the old hardware record
        UPDATE `{target_registry}`
        SET End_Date = DATE('{date_str}'), 
            SensorStatus = 'Archived'
        WHERE NodeNum = '{data['NodeNum']}' 
          AND End_Date IS NULL;

        -- 2. Start the new hardware record at the same spot
        INSERT INTO `{target_registry}` (
            NodeNum, Project, Location, Bank, Depth, SensorStatus, Start_Date
        )
        VALUES (
            '{new_node_num}', 
            '{data['Project']}', 
            '{data['Location']}', 
            '{data.get('Bank', '')}', 
            {data.get('Depth', 'NULL') if pd.notnull(data.get('Depth')) else 'NULL'}, 
            'On Project', 
            DATE('{date_str}')
        );
        
        COMMIT;
    """
    try:
        client.query(sql).result()
        st.success(f"🔄 Swapped {data['NodeNum']} for {new_node_num}")
        st.cache_data.clear()
        time.sleep(1)
        st.rerun()
    except Exception as e:
        st.error("Swap Transaction Failed")
        st.code(sql, language="sql")
        st.error(str(e))

# ===============================================================
# PAGE: SENSOR SWITCH (Correction Logic)
# ===============================================================

def render_sensor_switch_page(client, PROJECT_ID, DATASET_ID):
    """
    Repurposed for metadata corrections (typos) using only NodeNum.
    This page is now a 'Quick Fix' tool for active deployments.
    """
    st.header("🩹 Metadata Quick-Fix")
    st.info("""
        **Purpose:** Use this to fix typos (Location, Bank, Depth) on an active node 
        without changing history or start dates.
    """)
    
    target_registry = f"{PROJECT_ID}.{DATASET_ID}.node_registry"
    
    node_id = st.text_input("Enter Node ID to correct (e.g., TP-0001)").strip().upper()
    
    if node_id:
        # Search for the active record
        query = f"SELECT * FROM `{target_registry}` WHERE NodeNum = '{node_id}' AND End_Date IS NULL"
        df = client.query(query).to_dataframe()
        
        if not df.empty:
            row = df.iloc[0]
            st.subheader(f"Current Config: {node_id}")
            
            with st.form("quick_fix_form"):
                c1, c2 = st.columns(2)
                # Allow editing of the core metadata
                new_loc = c1.text_input("Corrected Location", value=row['Location'])
                new_bank = c2.text_input("Corrected Bank", value=row['Bank'])
                
                c3, c4 = st.columns(2)
                new_depth = c3.number_input("Corrected Depth (ft)", value=float(row['Depth']) if pd.notnull(row['Depth']) else 0.0)
                new_status = c4.selectbox("Corrected Status", ["On Project", "Available", "Diagnostic", "Dead"], index=0)

                if st.form_submit_button("🚀 Apply Corrections"):
                    execute_combined_correction(
                        client, row, row['Project'], new_loc, 
                        new_bank, new_depth, new_status, target_registry
                    )
        else:
            st.error(f"No active record found for Node '{node_id}'.")

# Note: This function now calls 'execute_combined_correction' 
# which we already updated to ignore PhysicalID.
# ===============================================================
# PAGE: SENSOR EDIT (Interactive Registry Editor)
# ===============================================================

def render_sensor_edit_filters(reg_df):
    """
    Unified Filtering: Hierarchical Project -> Location search.
    Strips PhysicalID from view and applies strict status rules.
    """
    st.subheader("🔍 Find & Select Record")
    
    # Refresh logic
    if st.button("🔄 Refresh Table Data"):
        st.cache_data.clear()
        st.rerun()
    
    show_archived = st.checkbox("Show Archived/Historical Data", value=False)
    df = reg_df[reg_df['End_Date'].isna()].copy() if not show_archived else reg_df.copy()

    col_f1, col_f2, col_f3, col_f4 = st.columns(4)
    
    with col_f1:
        u_projects = sorted(df['Project'].unique().tolist())
        sel_proj = st.selectbox("Search by Project", ["All"] + u_projects)
        
    with col_f2:
        if sel_proj != "All":
            u_locs = sorted(df[df['Project'] == sel_proj]['Location'].unique().tolist(), key=lambda x: tuple(natural_sort_key(x)))
        else:
            u_locs = sorted(df['Location'].unique().tolist(), key=lambda x: tuple(natural_sort_key(x)))
        sel_loc = st.selectbox("Search by Location", ["All"] + u_locs)

    with col_f3:
        # Use our strict status list for filtering
        u_status = ["On Project", "Available", "Diagnostic", "Dead", "Archived"]
        sel_stat = st.selectbox("Filter by Status", ["All"] + u_status)
        
    with col_f4:
        search_node = st.text_input("Search Node ID").strip().upper()

    # Application of filters
    if sel_proj != "All": df = df[df['Project'] == sel_proj]
    if sel_loc != "All": df = df[df['Location'] == sel_loc]
    if sel_stat != "All": df = df[df['SensorStatus'] == sel_stat]
    if search_node: df = df[df['NodeNum'].str.contains(search_node)]
        
    # Standardize sort and strip PhysicalID from view
    if not df.empty:
        df['bank_sort'] = df['Bank'].apply(lambda x: tuple(natural_sort_key(x)))
        df = df.sort_values(by=['Location', 'bank_sort']).drop(columns=['bank_sort'])
        if 'PhysicalID' in df.columns:
            df = df.drop(columns=['PhysicalID'])
            
    return df

def render_edit_record_form(client, data, reg_df, proj_list, target_registry):
    """
    Form for editing existing records. Physical ID is removed.
    Implements custom Office locations and strict status dropdowns.
    """
    st.divider()
    st.subheader(f"🛠️ Managing {data['NodeNum']}")

    # 1. Project Selection
    try: p_idx = proj_list.index(data['Project'])
    except ValueError: p_idx = 0
    new_proj = st.selectbox("Assign to Project", proj_list, index=p_idx, key=f"edit_proj_{data['NodeNum']}")

    # 2. Dynamic Location (Text input for Office, selectbox for sites)
    if new_proj == 'Office':
        new_loc = st.text_input("Office Sub-Location", value=data['Location'] if data['Project'] == 'Office' else "Desk")
    else:
        existing_locs = sorted(reg_df[reg_df['Project'] == new_proj]['Location'].unique().tolist(), key=lambda x: tuple(natural_sort_key(x)))
        try: l_idx = existing_locs.index(data['Location'])
        except ValueError: l_idx = 0
        new_loc = st.selectbox("Assign to Location", existing_locs, index=l_idx)

    # 3. Form for Bank, Depth, Status
    with st.form(key=f"edit_form_{data['NodeNum']}"):
        c1, c2 = st.columns(2)
        new_bank = c1.text_input("Bank", value=str(data.get('Bank', '')))
        new_depth = c2.number_input("Depth (ft)", value=float(data['Depth']) if pd.notnull(data['Depth']) else 0.0)
        
        status_list = ["On Project", "Available", "Diagnostic", "Dead", "Archived"]
        try: s_idx = status_list.index(data['SensorStatus'])
        except ValueError: s_idx = 0
        new_status = st.selectbox("Update Status", status_list, index=s_idx)

        # Actions
        cols = st.columns([1, 1, 1])
        if cols[0].form_submit_button("💾 Save Assignment"):
            execute_combined_correction(client, data, new_proj, new_loc, new_bank, new_depth, new_status, target_registry)

        if cols[1].form_submit_button("🔚 Decommission"):
            # This triggers the standard decommission workflow
            st.info("Please use the Decommission section in Node Manager for full audit trail.")

        if cols[2].form_submit_button("🗑️ Delete Record", type="primary"):
            execute_record_delete(client, data, target_registry)

def execute_record_delete(client, data, target_registry):
    """Deletes record using NodeNum only."""
    delete_sql = f"""
        DELETE FROM `{target_registry}` 
        WHERE NodeNum = '{data['NodeNum']}' 
          AND Start_Date = DATE('{data['Start_Date']}')
          AND End_Date IS NULL
    """
    try:
        client.query(delete_sql).result()
        st.warning(f"Deleted {data['NodeNum']} from registry.")
        st.cache_data.clear()
        time.sleep(1)
        st.rerun()
    except Exception as e:
        st.error(f"Deletion failed: {e}")

def execute_decommission_node(client, data, target_registry, decom_dt, stock_status, d_office_loc):
    """
    Finalized Decommission: No PhysicalID reliance.
    """
    date_str = decom_dt.strftime('%Y-%m-%d')
    node_num = data['NodeNum']

    sql = f"""
        BEGIN TRANSACTION;
        UPDATE `{target_registry}`
        SET End_Date = DATE('{date_str}'), SensorStatus = 'Archived'
        WHERE NodeNum = '{node_num}' AND End_Date IS NULL;

        INSERT INTO `{target_registry}` (NodeNum, Project, Location, Bank, Depth, SensorStatus, Start_Date)
        VALUES ('{node_num}', 'Office', '{d_office_loc}', '{node_num}', NULL, '{stock_status}', DATE('{date_str}'));
        COMMIT;
    """
    try:
        client.query(sql).result()
        st.success(f"✅ {node_num} moved to {d_office_loc}.")
        st.cache_data.clear()
        time.sleep(1)
        st.rerun()
    except Exception as e:
        st.error("Audit transaction failed.")
        st.code(sql, language="sql")

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
    import requests # Standardize import at top of file if preferred
    
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

def render_project_master_page(client, selected_project, PROJECT_ID, DATASET_ID):
    """Main entry point for Project Lifecycle Management."""
    st.header("⚙️ Project Lifecycle Management")
    
    action = st.radio("Action", ["📋 Fleet Overview", "🏗️ New Project", "🔧 Edit Project Metadata"], horizontal=True)
    table_projects = f"{PROJECT_ID}.{DATASET_ID}.project_registry"

    if action == "📋 Fleet Overview":
        render_project_overview(client, table_projects)

    elif action == "🏗️ New Project":
        render_new_project_form(client, table_projects)

    elif action == "🔧 Edit Project Metadata":
        render_update_project_form(client, selected_project, table_projects)


def render_project_overview(client, table_projects):
    """Displays all registry information for all projects (including Archived)."""
    st.subheader("📋 Complete Project Registry")
    
    # We remove the Status filter so you can see 'Archived' projects too
    query = f"SELECT * FROM `{table_projects}` ORDER BY Project ASC"
    df = client.query(query).to_dataframe()
    
    if not df.empty:
        # Standardize date display for the table
        for col in ['Date_Freezedown', 'Date_Completion']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).dt.date
        
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.caption(f"Total Projects in Registry: {len(df)}")
    else:
        st.info("Registry table is empty.")


def render_new_project_form(client, table_projects):
    """UI for registering a new project ID."""
    st.subheader("🏗️ Initialize New Project")
    with st.form("new_project_form"):
        col1, col2 = st.columns(2)
        n_code = col1.text_input("Project ID / Job # (e.g., 2538)")
        n_name = col2.text_input("Project Friendly Name (e.g., Warehouse A)")
        
        n_notes = st.text_area("Initial Engineering Notes")
        
        if st.form_submit_button("🚀 Create Project Entry"):
            if not n_code:
                st.error("Project ID is required.")
            else:
                check_q = f"SELECT Project FROM `{table_projects}` WHERE Project = '{n_code}'"
                if not client.query(check_q).to_dataframe().empty:
                    st.error(f"Project {n_code} already exists.")
                else:
                    insert_q = f"""
                        INSERT INTO `{table_projects}` (Project, ProjectName, ProjectStatus, EngNotes)
                        VALUES ('{n_code}', '{n_name}', 'Initialized', '{n_notes}')
                    """
                    client.query(insert_q).result()
                    st.success(f"Project {n_code} initialized.")
                    time.sleep(1)
                    st.rerun()


def render_update_project_form(client, selected_project, table_projects):
    """
    Comprehensive Edit Form: Allows viewing and replacing ALL registry fields
    for the selected project.
    """
    st.subheader(f"🔧 Editing Project: {selected_project}")
    
    proj_q = f"SELECT * FROM `{table_projects}` WHERE Project = '{selected_project}'"
    p_res = client.query(proj_q).to_dataframe()
    
    if p_res.empty:
        st.error("Select a valid project from the sidebar.")
        return

    p_data = p_res.iloc[0].to_dict()
    
    with st.form("comprehensive_edit_project"):
        # 1. Identity & Name
        c1, c2 = st.columns(2)
        u_project_id = c1.text_input("Project ID (Internal Key)", value=p_data.get('Project', ''), disabled=True)
        u_project_name = c2.text_input("Friendly Project Name", value=p_data.get('ProjectName', ''))

        # 2. Status & Lifecycle
        c3, c4 = st.columns(2)
        status_options = ["Initialized", "Pre-freeze", "Freezedown", "Maintenance", "Archived"]
        curr_status = p_data.get('ProjectStatus', 'Initialized')
        s_idx = status_options.index(curr_status) if curr_status in status_options else 0
        u_status = c3.selectbox("Lifecycle Status", status_options, index=s_idx)
        
        # 3. Dates
        # Logic to handle existing dates or default to None
        def safe_date(d): return pd.to_datetime(d).date() if pd.notnull(d) else None

        u_date_freeze = c3.date_input("Date Freezedown Started", value=safe_date(p_data.get('Date_Freezedown')))
        u_date_comp = c4.date_input("Date Project Completed", value=safe_date(p_data.get('Date_Completion')))

        # 4. Engineering Notes
        u_notes = st.text_area("Engineering & Site Notes", value=p_data.get('EngNotes', ''))

        if st.form_submit_button("💾 Overwrite Project Registry Information"):
            # Prepare SQL fragments for dates
            freeze_val = f"DATE('{u_date_freeze}')" if u_date_freeze else "NULL"
            comp_val = f"DATE('{u_date_comp}')" if u_date_comp else "NULL"
            
            update_q = f"""
                UPDATE `{table_projects}` 
                SET 
                    ProjectName = '{u_project_name}',
                    ProjectStatus = '{u_status}',
                    EngNotes = '{u_notes}',
                    Date_Freezedown = {freeze_val},
                    Date_Completion = {comp_val}
                WHERE Project = '{selected_project}'
            """
            
            try:
                client.query(update_q).result()
                st.success(f"✅ Successfully updated all registry data for {selected_project}")
                time.sleep(1)
                st.rerun()
            except Exception as e:
                st.error(f"Update failed: {e}")
                st.code(update_q)
# ===============================================================
# PAGE: BULK REGISTRY MANAGER
# ===============================================================

def render_bulk_registry_page(client, proj_list, PROJECT_ID, DATASET_ID):
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
        # We removed PhysicalID from the required columns
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
# PAGE: PROJECT MASTER
# ===============================================================

def render_project_master_page(client, selected_project, PROJECT_ID, DATASET_ID):
    """
    Main entry point for Project Lifecycle Management.
    """
    st.header("⚙️ Project Lifecycle Management")
    
    # Expanded navigation to cover the full management scope
    action = st.radio("Action", ["📋 Fleet Overview", "🏗️ New Project", "🔧 Edit Project Metadata"], horizontal=True)
    table_projects = f"{PROJECT_ID}.{DATASET_ID}.project_registry"

    if action == "📋 Fleet Overview":
        render_project_overview(client, table_projects)

    elif action == "🏗️ New Project":
        render_new_project_form(client, table_projects)

    elif action == "🔧 Edit Project Metadata":
        render_update_project_form(client, selected_project, table_projects)


def render_project_overview(client, table_projects):
    """
    Displays all registry information for all projects, including Archived.
    """
    st.subheader("📋 Complete Project Registry")
    
    # Removed the status filter so you can see every project ever registered
    query = f"SELECT * FROM `{table_projects}` ORDER BY Project ASC"
    try:
        df = client.query(query).to_dataframe()
        if not df.empty:
            # Clean up date display for the UI
            for col in ['Date_Freezedown', 'Date_Completion']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col]).dt.date
            
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.caption(f"Total Projects in Registry: {len(df)}")
        else:
            st.info("Registry table is empty.")
    except Exception as e:
        st.error(f"Failed to load project registry: {e}")


def render_new_project_form(client, table_projects):
    """
    UI for registering a new project ID and Name.
    """
    st.subheader("🏗️ Initialize New Project")
    with st.form("new_project_form"):
        col1, col2 = st.columns(2)
        n_code = col1.text_input("Project ID / Job # (e.g., 2538)")
        n_name = col2.text_input("Friendly Project Name (e.g., Cold Storage A)")
        
        n_notes = st.text_area("Initial Engineering Notes")
        
        if st.form_submit_button("🚀 Create Project Entry"):
            if not n_code:
                st.error("Project ID is required.")
            else:
                check_q = f"SELECT Project FROM `{table_projects}` WHERE Project = '{n_code}'"
                if not client.query(check_q).to_dataframe().empty:
                    st.error(f"Project {n_code} already exists.")
                else:
                    insert_q = f"""
                        INSERT INTO `{table_projects}` (Project, ProjectName, ProjectStatus, EngNotes)
                        VALUES ('{n_code}', '{n_name}', 'Initialized', '{n_notes}')
                    """
                    try:
                        client.query(insert_q).result()
                        st.success(f"Project {n_code} initialized.")
                        time.sleep(1)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to create project: {e}")


def render_update_project_form(client, selected_project, table_projects):
    """
    Comprehensive Edit Form: Allows viewing and replacing ALL registry fields.
    """
    st.subheader(f"🔧 Editing Project: {selected_project}")
    
    proj_q = f"SELECT * FROM `{table_projects}` WHERE Project = '{selected_project}'"
    p_res = client.query(proj_q).to_dataframe()
    
    if p_res.empty:
        st.error("Please select a project from the sidebar to edit.")
        return

    p_data = p_res.iloc[0].to_dict()
    
    with st.form("comprehensive_edit_project"):
        # 1. Identity & Name
        c1, c2 = st.columns(2)
        # ID is disabled to prevent breaking foreign keys in sensor data
        u_project_id = c1.text_input("Project ID (Internal Key)", value=p_data.get('Project', ''), disabled=True)
        u_project_name = c2.text_input("Friendly Project Name", value=p_data.get('ProjectName', ''))

        # 2. Status & Lifecycle
        c3, c4 = st.columns(2)
        status_options = ["Initialized", "Pre-freeze", "Freezedown", "Maintenance", "Archived"]
        curr_status = p_data.get('ProjectStatus', 'Initialized')
        s_idx = status_options.index(curr_status) if curr_status in status_options else 0
        u_status = c3.selectbox("Lifecycle Status", status_options, index=s_idx)
        
        # 3. Dates
        def safe_date(d): return pd.to_datetime(d).date() if pd.notnull(d) else None
        u_date_freeze = c3.date_input("Date Freezedown Started", value=safe_date(p_data.get('Date_Freezedown')))
        u_date_comp = c4.date_input("Date Project Completed", value=safe_date(p_data.get('Date_Completion')))

        # 4. Notes
        u_notes = st.text_area("Engineering & Site Notes", value=p_data.get('EngNotes', ''))

        if st.form_submit_button("💾 Overwrite Project Registry Information"):
            # Construct SQL fragments for date null-safety
            freeze_val = f"DATE('{u_date_freeze}')" if u_date_freeze else "NULL"
            comp_val = f"DATE('{u_date_comp}')" if u_date_comp else "NULL"
            
            update_q = f"""
                UPDATE `{table_projects}` 
                SET 
                    ProjectName = '{u_project_name}',
                    ProjectStatus = '{u_status}',
                    EngNotes = '{u_notes}',
                    Date_Freezedown = {freeze_val},
                    Date_Completion = {comp_val}
                WHERE Project = '{selected_project}'
            """
            
            try:
                client.query(update_q).result()
                st.success(f"✅ Successfully updated all registry data for {selected_project}")
                time.sleep(1)
                st.rerun()
            except Exception as e:
                st.error(f"Update failed: {e}")
# ===============================================================
# PAGE: REF CURVE LIBRARY
# ===============================================================

def render_ref_curve_library_page(client, PROJECT_ID, DATASET_ID):
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
    """Fetches current library stats with robust column handling."""
    try:
        # Simplified query: If upload_date doesn't exist, BigQuery will return an error 
        # which we catch to identify if the table is uninitialized.
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
            return inventory_df
        else:
            st.info("The library is currently empty. Upload curve CSVs below.")
    except Exception:
        st.warning("⚠️ Reference table uninitialized or missing 'upload_date' column.")
    
    return pd.DataFrame()


def render_curve_management_tools(client, inventory_df, table_curves):
    """UI for deleting curves or purging the library."""
    c1, c2 = st.columns(2)
    
    with c1.expander("🗑️ Individual Curve Delete"):
        if not inventory_df.empty:
            to_delete = st.selectbox("Select Curve to Remove", sorted(inventory_df['CurveID'].tolist()))
            if st.button(f"Permanently Delete {to_delete}", type="primary"):
                client.query(f"DELETE FROM `{table_curves}` WHERE CurveID = '{to_delete}'").result()
                st.success(f"Removed {to_delete}")
                time.sleep(1)
                st.rerun()

    with c2.expander("🧨 Library Wipe"):
        st.warning("This will delete EVERY theoretical curve in the database.")
        if st.button("EXECUTE TOTAL PURGE", key="purge_all"):
            client.query(f"TRUNCATE TABLE `{table_curves}`").result()
            st.success("Library wiped.")
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
        existing_ids = inventory_df['CurveID'].tolist() if not inventory_df.empty else []
        
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
# PAGE: DATA MANAGEMENT (Flagging & Maintenance)
# ===============================================================

def render_data_management_page(client, reg_df, selected_project, PROJECT_ID, DATASET_ID):
    st.header("🧨 Data Management (Manual Rejections)")
    
    # Target the physical side-table
    target_table = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections" 

    target_scope, new_status = render_management_controls()
    st.divider()

    filters = render_management_filters(reg_df, selected_project, target_scope)
    
    # Build the WHERE clause (which targets NodeNum, timestamp, etc.)
    where_str = build_management_where_clause(reg_df, selected_project, target_scope, filters)
    
    # For rejections, we show how many points currently exist in the main data
    # that match this filter to be moved into the rejection table.
    telemetry_table = f"{PROJECT_ID}.{DATASET_ID}.master_data"
    render_verification_step(client, where_str, telemetry_table)

    render_rejection_execution_step(client, where_str, new_status, target_table, telemetry_table)
    
    # 3. SQL CONSTRUCTION
    # Note: We target the base table, not the view, for DML updates
    target_table = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"
    where_str = build_management_where_clause(reg_df, selected_project, target_scope, filters)
    
    # 5. EXECUTION STEP
    render_execution_step(client, where_str, new_status, target_table)


def render_management_controls():
    """Renders radio buttons for scope and the new specific status types."""
    c1, c2 = st.columns(2)
    with c1:
        target_scope = st.radio("Target Scope", ["Project Wide", "Specific Location", "Specific Node"], horizontal=True)
    with c2:
        # User defined status types
        new_status = st.selectbox("Set Approval Status To:", ["TRUE", "BadData", "Masked", "Office"])
        
    return target_scope, new_status


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


def build_management_where_clause(reg_df, selected_project, target_scope, f):
    """
    Constructs a WHERE clause by looking up NodeNums for the project in reg_df.
    """
    # Get the list of nodes assigned to this project
    proj_nodes = reg_df[reg_df['Project'] == selected_project]['NodeNum'].unique().tolist()
    
    if not proj_nodes:
        # Return a clause that matches nothing if the project is empty to avoid SQL errors
        return "NodeNum = 'NONE'"

    # Create the IN list for SQL
    nodes_str = ", ".join([f"'{n}'" for n in proj_nodes])
    where_clauses = [f"NodeNum IN ({nodes_str})"]
    
    # 2. Start building the query with the Node list
    if target_scope == "Specific Node":
        where_clauses = [f"NodeNum = '{f['scope_val']}'"]
    else:
        # Filter by all nodes in the project
        nodes_str = ", ".join([f"'{n}'" for n in proj_nodes])
        where_clauses = [f"NodeNum IN ({nodes_str})"]
    
    # 3. Scope Logic for Specific Location
    if target_scope == "Specific Location":
        # Get nodes only at that specific location
        loc_nodes = reg_df[(reg_df['Project'] == selected_project) & 
                           (reg_df['Location'] == f['scope_val'])]['NodeNum'].unique().tolist()
        nodes_str = ", ".join([f"'{n}'" for n in loc_nodes])
        where_clauses = [f"NodeNum IN ({nodes_str})"]

    # 4. Temporal Logic (Handling "Up to a date")
    # Using 'f['e_date']' as the cutoff for "Up to a date"
    if f["temporal_dir"] == "Between Range":
        where_clauses.append(f"timestamp BETWEEN '{f['s_date']}' AND '{f['e_date']}'")
    elif f["temporal_dir"] == "Older Than" or f["temporal_dir"] == "Newer Than":
        # For "Up to a date", ensure you select "Older Than" in the UI
        op = "<" if f["temporal_dir"] == "Older Than" else ">"
        where_clauses.append(f"timestamp {op} '{f['s_date']}'")
    
    # 5. Threshold Logic
    if f["val_filter"] == "Above Threshold":
        where_clauses.append(f"temperature > {f['threshold']}")
    elif f["val_filter"] == "Below Threshold":
        where_clauses.append(f"temperature < {f['threshold']}")

    return " AND ".join(where_clauses)


def render_verification_step(client, where_str, target_table):
    """Shows the user how many rows will be affected before they commit."""
    if st.button("🔍 Step 1: Verify Match Count"):
        count_q = f"SELECT COUNT(*) as total FROM `{target_table}` WHERE {where_str}"
        try:
            res = client.query(count_q).to_dataframe()
            count = res.iloc[0]['total']
            st.metric("Points Found", f"{count:,}")
            if count == 0:
                st.warning("No data points found with these filters.")
        except Exception as e:
            st.error(f"Verification Query Failed: {e}")


def render_rejection_execution_step(client, where_str, new_status, target_table, telemetry_table):
    st.warning(f"Targeting status: **{new_status}**")
    
    if st.checkbox("I confirm this change to the manual rejections table."):
        if st.button(f"🚀 Execute Status Update"):
            if new_status == "TRUE":
                # "TRUE" means it's NOT rejected. Delete from the rejection table.
                sql = f"DELETE FROM `{target_table}` WHERE {where_str}"
            else:
                # Flagging: Insert matching records from telemetry into the rejection table
                sql = f"""
                    INSERT INTO `{target_table}` (NodeNum, timestamp, temperature, Project, Location, ApprovalStatus)
                    SELECT NodeNum, timestamp, temperature, Project, Location, '{new_status}'
                    FROM `{telemetry_table}`
                    WHERE {where_str}
                """
            
            try:
                with st.spinner("Processing rejection records..."):
                    job = client.query(sql)
                    job.result()
                st.success(f"Successfully processed {job.num_dml_affected_rows} records.")
                st.balloons()
            except Exception as e:
                st.error(f"Database Error: {e}")
                st.code(sql)
# ===============================================================
# FINAL INTEGRATED EXECUTION BLOCK
# ===============================================================

def main():
    """
    Unified entry point. Routes to specific tools based on sidebar selection.
    """
    # 1. Initialize Sidebar and get context
    admin_page, target_registry, selected_project, proj_list = render_sidebar()
    
    # 2. Get Unit Preferences 
    unit_mode, unit_label = get_unit_labels()
    display_tz = "America/Los_Angeles"
    
    # Global BigQuery Pathing constants
    # (Ensure these match your actual GCP environment)
    PROJECT_ID = "sensorpush-export"
    DATASET_ID = "Temperature"
    
    # 3. Load Registry Data (Cached)
    reg_df = load_registry_data(target_registry)

    # --- ROUTING LOGIC ---

    if admin_page == "🛠️ Node Manager":
        # Using the split functions for Selection and Action
        selected_node_data = render_node_selector(reg_df, proj_list)
        
        if selected_node_data is not None:
            render_node_action_manager(client, selected_node_data, reg_df, proj_list, target_registry)
        else:
            st.divider()
            st.info("💡 **Tip:** Select a record in the table above to Edit, Swap, or Decommission that node.")

    elif admin_page == "📡 Setup Node Tool":
        render_project_status_dashboard(client, selected_project, unit_label, target_registry)
        st.divider()
        render_hardware_integrity_table(client, selected_project, unit_mode, unit_label, target_registry)

    elif admin_page == "🔍 Sensor Status":
        render_sensor_status(client, selected_project, unit_label, unit_mode, display_tz)
        
    elif admin_page == "📦 Bulk Registry Manager":
        render_bulk_registry_page(client, proj_list, PROJECT_ID, DATASET_ID)

    elif admin_page == "📡 Data Recovery":
        render_data_recovery_page(reg_df)

    elif admin_page == "⚙️ Project Master":
        render_project_master_page(client, selected_project, PROJECT_ID, DATASET_ID)

    elif admin_page == "📈 Ref Curve Library":
        render_ref_curve_library_page(client, PROJECT_ID, DATASET_ID)

    elif admin_page == "🧨 Data Management":
        render_data_management_page(client, reg_df, selected_project, PROJECT_ID, DATASET_ID)

    # LEGACY / REDUNDANT TOOLS (Optional: Remove if Node Manager covers these)
    elif admin_page == "🩹 Sensor Switch":
        render_sensor_switch_page(client, PROJECT_ID, DATASET_ID)
        
    elif admin_page == "🔄 Sensor Replace":
        # Repurposed to use the new Node ID centric logic
        render_sensor_replace_page(client, PROJECT_ID, DATASET_ID)

# ===============================================================
# EXECUTION ENTRY POINT
# ===============================================================
if __name__ == "__main__":
    main()
