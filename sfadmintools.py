import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import time
import re
import requests
import numpy as np

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
    """
    Queries active schema inventory data safely, merging real-time 'Last Seen' 
    lag hours, computing lifetime project reporting efficiency, and scrubbing 
    legacy PhysicalID markers.
    """
    try:
        # High-performance query that tracks latest ping AND counts actual project pings
        master_query = f"""
            WITH LatestTelemetry AS (
                SELECT 
                    NodeNum, 
                    MAX(timestamp) as last_ping
                FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view`
                GROUP BY NodeNum
            ),
            
            AssignmentWindows AS (
                SELECT 
                    NodeNum,
                    Start_Date,
                    -- Calculate expected hours from start up to end date (or right now if still active)
                    COALESCE(End_Date, CURRENT_DATE()) AS Effective_End,
                    DATE_DIFF(COALESCE(End_Date, CURRENT_DATE()), Start_Date, DAY) * 24 AS Expected_Hours
                FROM `{target_table}`
                WHERE Project != 'Dead'
            ),
            
            ActualProjectPings AS (
                SELECT 
                    m.NodeNum,
                    a.Start_Date,
                    COUNT(m.timestamp) AS Actual_Pings_Logged
                FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
                INNER JOIN AssignmentWindows a 
                  ON m.NodeNum = a.NodeNum 
                  -- Count pings that occurred strictly within the project timeline window
                  AND EXTRACT(DATE FROM m.timestamp) BETWEEN a.Start_Date AND a.Effective_End
                GROUP BY m.NodeNum, a.Start_Date
            )
            
            SELECT 
                R.*,
                T.last_ping,
                A.Expected_Hours,
                COALESCE(P.Actual_Pings_Logged, 0) AS Actual_Pings_Logged
            FROM `{target_table}` R
            LEFT JOIN LatestTelemetry T ON R.NodeNum = T.NodeNum
            LEFT JOIN AssignmentWindows A 
              ON R.NodeNum = A.NodeNum AND R.Start_Date = A.Start_Date
            LEFT JOIN ActualProjectPings P 
              ON R.NodeNum = P.NodeNum AND R.Start_Date = P.Start_Date
        """
        df = client.query(master_query).to_dataframe()
        
        # Calculate precise decimal hour latency relative to current execution time
        now_utc = pd.Timestamp.now(tz='UTC')
        
        if not df.empty and 'last_ping' in df.columns:
            # Step A: Create the hidden raw float column for sorting and styling
            df['hours_hidden'] = df['last_ping'].apply(
                lambda x: (now_utc - pd.to_datetime(x).tz_convert('UTC')).total_seconds() / 3600.0
                if pd.notnull(x) else np.nan
            )
            
            # Safely handle any infinity padding transformations before formatting text
            df['hours_hidden'] = pd.to_numeric(df['hours_hidden'], errors='coerce').fillna(float('inf'))
            
            # Step B: Create the pristine text display column for user readability
            def format_last_seen(hours):
                if pd.isna(hours) or hours == float('inf'):
                    return "❌ Never"
                elif hours < 1.0:
                    mins = int(hours * 60)
                    return f"{mins}m ago" if mins > 0 else "Just now"
                else:
                    return f"{hours:.1f}h ago"
            
            df['Last Seen'] = df['hours_hidden'].apply(format_last_seen)
        else:
            df['hours_hidden'] = float('inf')
            df['Last Seen'] = "❌ Never"
            
        # =============================================================================
        # SAFE VECTOR CALCULATION FOR REPORTING EFFICIENCY PERCENTAGE COLUMN
        # =============================================================================
        if not df.empty and 'Expected_Hours' in df.columns:
            # Enforce clean numerical typing and fill any missing values with 0
            exp_hours = pd.to_numeric(df['Expected_Hours'], errors='coerce').fillna(0)
            act_pings = pd.to_numeric(df['Actual_Pings_Logged'], errors='coerce').fillna(0)
            
            # Run vector calculation safely without ambiguous NA evaluations
            raw_eff = np.where(
                exp_hours <= 0, 
                0.0, 
                np.minimum(100.0, np.round((act_pings / exp_hours) * 100, 1))
            )
            
            df['Reporting Efficiency'] = [f"{x:.1f}%" for x in raw_eff]
        else:
            df['Reporting Efficiency'] = "0.0%"

        # Absolute force-scrub of legacy tracking keys and query metrics from final table
        cols_to_drop = ['physicalID', 'PhysicalID', 'last_ping', 'Expected_Hours', 'Actual_Pings_Logged']
        df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors='ignore')
        
        return df
    except Exception as e:
        st.error(f"Error loading registry: {e}")
        return pd.DataFrame()
        
# =============================================================================
# 1. Helper functions & Styling Engine
# =============================================================================
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

def assign_row_color(hours):
    """
    Returns the CSS background color based on the age of the data in hours.
    """
    if hours is None or pd.isna(hours) or hours == float('inf'):  # Handles "Never" seen / offline nodes safely
        return "background-color: #d1d5db; color: #1f2937;"  # Gray
    elif hours < 1:
        return "background-color: #d1fae5; color: #065f46;"  # Green (<1 hr)
    elif 1 <= hours <= 6:
        return "background-color: #fef08a; color: #854d0e;"  # Yellow (1-6 hrs)
    elif 6 < hours <= 12:
        return "background-color: #fed7aa; color: #9a3412;"  # Orange (6-12 hrs)
    elif 12 < hours <= 24:
        return "background-color: #fca5a5; color: #991b1b;"  # Red (12-24 hrs)
    else:
        return "background-color: #d1d5db; color: #1f2937;"  # Gray (>24 hrs)

def style_dataframe(row):
    """
    Scans row lag floats and cleanly overlays hex coloring alerts to visible cells.
    """
    try:
        val = row['hours_hidden']
        # Convert explicit infinity or null structures to None for our color rules function
        hours_val = None if (val == float('inf') or pd.isnull(val)) else float(val)
        color_style = assign_row_color(hours_val)
    except Exception:
        color_style = "background-color: transparent;" # Safe backup overlay
        
    return [color_style] * len(row)



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
            else:
                latest_time = latest_time.tz_convert('UTC')
            
            lag_hrs = (now_utc - latest_time).total_seconds() / 3600
            val = g_df['avg_now'].mean() if pd.notnull(g_df['avg_now'].mean()) else g_df['latest_temp'].mean()
            
            if lag_hrs > 1.1: 
                st.subheader(f"⚠️ Offline {int(lag_hrs)}h")
            else: 
                # Convert reading dynamically to reflect current global Celsius / Fahrenheit scale configurations
                unit_mode = st.session_state.get('unit_mode', 'Fahrenheit')
                display_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
                st.title(f"{display_val:.1f}{unit_label}")
            
            # Simplified Summary Stats
            active_1h = int(g_df['avg_now'].notnull().sum())
            active_24h = int((g_df['pings_24h'] > 0).sum())
            st.write(f"**{active_1h}/{len(g_df)}** (1h) | **{active_24h}/{len(g_df)}** (24h)")
            
            # Safely check boundaries to prevent crash conditions on missing datasets
            min_now_val = g_df['min_now'].min()
            max_now_val = g_df['max_now'].max()
            min_24h_val = g_df['min_24h'].min()
            max_24h_val = g_df['max_24h'].max()
            
            # Convert status thresholds for displaying localized caption text units cleanly
            if pd.notnull(min_now_val) and pd.notnull(max_now_val):
                mn = (min_now_val - 32) * 5/9 if st.session_state.get('unit_mode') == "Celsius" else min_now_val
                mx = (max_now_val - 32) * 5/9 if st.session_state.get('unit_mode') == "Celsius" else max_now_val
                st.caption(f"Cur: {mn:.1f} to {mx:.1f}{unit_label}")
            else:
                st.caption(f"Cur: N/A to N/A")
                
            if pd.notnull(min_24h_val) and pd.notnull(max_24h_val):
                mn24 = (min_24h_val - 32) * 5/9 if st.session_state.get('unit_mode') == "Celsius" else min_24h_val
                mx24 = (max_24h_val - 32) * 5/9 if st.session_state.get('unit_mode') == "Celsius" else max_24h_val
                st.caption(f"24h: {mn24:.1f} to {mx24:.1f}{unit_label}")
            else:
                st.caption(f"24h: N/A to N/A")
            
            # Safely parse historical variance indexes inside secondary row allocations
            t_row = st.columns(2)
            try:
                prev_1h = g_df['avg_1h_prev'].mean()
                arrow_1h = get_trend_arrow(val, prev_1h) if pd.notnull(prev_1h) else "➡️ N/A"
                t_row[0].caption(f"1h\n{arrow_1h}")
            except Exception:
                t_row[0].caption("1h\n➡️ N/A")
                
            try:
                prev_6h = g_df['avg_6h_prev'].mean()
                arrow_6h = get_trend_arrow(val, prev_6h) if pd.notnull(prev_6h) else "➡️ N/A"
                t_row[1].caption(f"6h\n{arrow_6h}")
            except Exception:
                t_row[1].caption("6h\n➡️ N/A")
            
# ===============================================================
# Function: Hardware integrity table
# ===============================================================
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
    
    try:
        df = client.query(query, job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)]
        )).to_dataframe()
    except Exception as e:
        st.error(f"Hardware Table Query Failed: {e}")
        return

    if df.empty: 
        st.info("No active nodes found for connectivity table.")
        return

    now_utc = pd.Timestamp.now(tz='UTC')

    def row_processor(row):
        ping = row['last_ping']
        
        if pd.isnull(ping):
            hours_hidden = float('inf')
            txt = "❌ Never"
            style = "background-color: #d1d5db; color: #1f2937;"
        else:
            ts = ping if ping.tzinfo else ping.tz_localize('UTC')
            diff_mins = (now_utc - ts).total_seconds() / 60.0
            hours_hidden = diff_mins / 60.0
            
            if hours_hidden < 1.0:
                txt = f"{int(diff_mins)}m ago" if diff_mins >= 1.0 else "Just now"
                style = "background-color: #d1fae5; color: #065f46;"
            elif 1.0 <= hours_hidden <= 6.0:
                txt = f"{hours_hidden:.1f}h ago"
                style = "background-color: #fef08a; color: #854d0e;"
            elif 6.0 < hours_hidden <= 12.0:
                txt = f"{hours_hidden:.1f}h ago"
                style = "background-color: #fed7aa; color: #9a3412;"
            elif 12.0 < hours_hidden <= 24.0:
                txt = f"{hours_hidden:.1f}h ago"
                style = "background-color: #fca5a5; color: #991b1b;"
            else:
                txt = f"{hours_hidden:.1f}h ago"
                style = "background-color: #d1d5db; color: #1f2937;"
        
        pos = f"{row['Depth']}ft" if (pd.notnull(row['Depth']) and row['Depth'] != 0) else f"Bank {row['Bank']}"
        trend = get_trend_arrow(row['avg_now'], row['avg_1h_prev'])
        
        return pd.Series([txt, style, pos, trend, hours_hidden])

    df[['Seen_Text', 'Seen_Style', 'Pos_Label', 'Trend', 'hours_hidden']] = df.apply(row_processor, axis=1)

    df['hours_hidden'] = pd.to_numeric(df['hours_hidden'], errors='coerce').fillna(float('inf'))
    df = df.sort_values(by='hours_hidden', ascending=True).reset_index(drop=True)

    display_df = pd.DataFrame({
        "Node ID": df['NodeNum'],
        "Location": df['Location'],
        "Position": df['Pos_Label'],
        "Last Seen": df['Seen_Text'],
        "24h Coverage": df['coverage_24h'],
        "1h Change": df['Trend'],
        "Last Temp": df['last_temp'].apply(lambda x: fmt_temp(x, unit_mode, unit_label)),
        "1h Pings": df['pings_1h'],
        "6h Pings": df['pings_6h'],
        "24h Pings": df['pings_24h']
    })

    def diagnostic_styler(data):
        style_df = pd.DataFrame('', index=data.index, columns=data.columns)
        
        for i in data.index:
            style_df.loc[i, 'Last Seen'] = df.loc[i, 'Seen_Style']
            
            if df.loc[i, 'SensorStatus'] == 'Diagnostic':
                style_df.loc[i, 'Node ID'] = 'background-color: #ff4b4b; color: white; font-weight: bold;'
                
        return style_df

    st.dataframe(
        display_df.style.apply(diagnostic_styler, axis=None), 
        use_container_width=True, 
        hide_index=True,
        column_config={
            "24h Coverage": st.column_config.ProgressColumn(
                "24h Coverage", 
                format="%.1f%%", 
                min_value=0, 
                max_value=100
            ),
            "1h Pings": st.column_config.NumberColumn("1h Pings", format="%d"),
            "6h Pings": st.column_config.NumberColumn("6h Pings", format="%d"),
            "24h Pings": st.column_config.NumberColumn("24h Pings", format="%d"),
        }
    )

# =============================================================================
# PAGE MODULE: 🛠️ NODE MANAGER
# =============================================================================

def render_node_selector(reg_df, proj_list):
    st.subheader("🎯 Active Node Registry")
    
    hide_archived = st.checkbox("Hide Archived Records", value=True, key="ns_hide_archived_toggle")
    
    df = reg_df.copy()
    if hide_archived and 'SensorStatus' in df.columns:
        df = df[
            (df['SensorStatus'].str.lower() != "archived") & 
            (df['Location'].str.contains("Archive", case=False, na=False) == False)
        ]

    c1, c2, c3 = st.columns(3)
    with c1:
        f_proj = st.selectbox("Filter by Project Space", ["All", "Unassigned"] + proj_list, key="ns_proj_f")
    with c2:
        if f_proj == "All":
            loc_opts = df['Location'].dropna().unique().tolist()
        elif f_proj == "Unassigned":
            loc_opts = df[df['Project'].isna() | (df['Project'] == "") | (df['Project'] == "Office") | (df['Location'] == "Office")]['Location'].dropna().unique().tolist()
        else:
            loc_opts = df[df['Project'] == f_proj]['Location'].dropna().unique().tolist()
            
        f_loc = st.selectbox("Filter by Physical Location", ["All"] + sorted(loc_opts), key="ns_loc_f")
    with c3:
        search_term = st.text_input("Global Search (Node ID)", "", key="ns_search_f")

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

    if 'hours_hidden' in df.columns:
        df['hours_hidden'] = pd.to_numeric(df['hours_hidden'], errors='coerce').fillna(float('inf'))
        df = df.sort_values(by='hours_hidden', ascending=True).reset_index(drop=True)
    else:
        df['hours_hidden'] = float('inf')

    st.markdown("### 📡 Hardware Inventory Fleet Breakdown")
    
    def classify_hardware_family(node):
        node_str = str(node).lower()
        if "-ch" in node_str:
            return "Lord"
        elif node_str.startswith("sp"):
            return "SP"
        elif node_str.startswith("tp"):
            return "TP"
        else:
            return "None of the Above"

    summary_df = reg_df.copy()
    summary_df['Hardware Family'] = summary_df['NodeNum'].apply(classify_hardware_family)
    
    summary_df['Parent ID'] = summary_df['NodeNum'].apply(
        lambda x: re.split(r'(?i)-ch', str(x))[0] if "-ch" in str(x).lower() else x
    )
    
    if 'End_Date' in summary_df.columns:
        summary_df['is_active'] = summary_df['End_Date'].isna()
    else:
        summary_df['is_active'] = True
        
    sort_keys = ['Parent ID', 'is_active']
    sort_asc = [True, False]
    
    if 'Start_Date' in summary_df.columns:
        sort_keys.append('Start_Date')
        sort_asc.append(False)
        
    summary_df = summary_df.sort_values(by=sort_keys, ascending=sort_asc)
    
    deduped_units = summary_df.drop_duplicates(subset=['Parent ID']).copy()
    
    try:
        fleet_pivot = deduped_units.groupby(['Hardware Family', 'SensorStatus']).size().unstack(fill_value=0)
        desired_order = ["TP", "SP", "Lord", "None of the Above"]
        fleet_pivot = fleet_pivot.reindex(desired_order, fill_value=0)
        fleet_pivot['Total Units'] = fleet_pivot.sum(axis=1)
        
        st.dataframe(fleet_pivot, use_container_width=True)
    except Exception as pivot_err:
        st.info("💡 Inventory matrix is populating. Assign statuses to your hardware to generate totals.")
        
    st.markdown("---")

    st.markdown("### 📋 Current Asset Allocation Matrix")

    if "last_selected_node" not in st.session_state:
        st.session_state["last_selected_node"] = None
    if "active_selected_node_record" not in st.session_state:
        st.session_state["active_selected_node_record"] = None

    ed_key = "node_registry_editor"
    if ed_key in st.session_state and "edited_rows" in st.session_state[ed_key]:
        changed_rows = st.session_state[ed_key]["edited_rows"]
        newly_checked = [idx for idx, changes in changed_rows.items() if changes.get("Select") == True]
        
        if newly_checked and not df.empty:
            latest_idx = newly_checked[-1]
            if latest_idx != st.session_state["last_selected_node"]:
                st.session_state["last_selected_node"] = latest_idx
                
                rec_dict = df.loc[latest_idx].drop(["hours_hidden"], errors='ignore').to_dict()
                rec_dict["Select"] = True
                st.session_state["active_selected_node_record"] = rec_dict
                st.session_state[ed_key]["edited_rows"] = {}
                st.rerun()
        
        elif any(changes.get("Select") == False for idx, changes in changed_rows.items()):
            st.session_state["last_selected_node"] = None
            st.session_state["active_selected_node_record"] = None
            st.session_state[ed_key]["edited_rows"] = {}
            st.rerun()

    df.insert(0, "Select", False)
    if st.session_state["last_selected_node"] is not None and st.session_state["last_selected_node"] < len(df):
        df.loc[st.session_state["last_selected_node"], "Select"] = True

    def node_manager_styler(data):
        style_canvas = pd.DataFrame('', index=data.index, columns=data.columns)
        for i in data.index:
            try:
                val = data.loc[i, 'hours_hidden']
                hours_val = None if (val == float('inf') or pd.isnull(val)) else float(val)
                color_style = assign_row_color(hours_val)
            except Exception:
                color_style = "background-color: transparent;"
            
            for col in data.columns:
                if col != "Select":
                    style_canvas.loc[i, col] = color_style
        return style_canvas

    edited_df = st.data_editor(
        df.style.apply(node_manager_styler, axis=None) if not df.empty else df,
        hide_index=True,
        use_container_width=True,
        column_config={
            "Select": st.column_config.CheckboxColumn("Select", default=False, required=True),
            "NodeNum": "Node ID",
            "Last Seen": st.column_config.TextColumn("Last Seen", help="Hours since last server telemetry ping"),
            "Reporting Efficiency": st.column_config.TextColumn("Reporting Efficiency", help="Telemetry reporting yield percentage"),
            "coverage_24h": st.column_config.ProgressColumn("24h Coverage", format="%.1f%%", min_value=0, max_value=100)
        },
        disabled=[col for col in df.columns if col != "Select"],
        column_order=["Select", "Location", "NodeNum", "Peer Trend", "Performance", "Status", "Reporting Efficiency", "coverage_24h"], 
        key=ed_key
    )

    if st.session_state["active_selected_node_record"] is not None:
        selected_returned_row = st.session_state["active_selected_node_record"].copy()
        if "Select" in selected_returned_row:
            del selected_returned_row["Select"]
    else:
        selected_returned_row = None
            
    st.markdown("---")
    with st.expander("🧨 Danger Zone: Sync Playground Staging Table Directly to Production"):
        st.error("⚠️ CRITICAL WARNING: This action will completely erase ALL records in your live production `node_registry` and overwrite them with an exact snapshot copy of your `node_registry_dummy` table.")
        
        confirm_token = st.text_input(
            "Type out 'OVERWRITE' to authorize replacing your production environment data models:", 
            value="", 
            key="force_production_overwrite_token_input"
        )
        
        if st.button("💥 Wipe Production & Clone Playground Table", type="primary", use_container_width=True):
            if confirm_token.strip() != "OVERWRITE":
                st.error("Authorization token verification failed. Action aborted.")
            else:
                prod_table = f"{PROJECT_ID}.{DATASET_ID}.node_registry"
                dummy_table = f"{PROJECT_ID}.{DATASET_ID}.node_registry_dummy"
                
                job_config = bigquery.QueryJobConfig(
                    write_disposition="WRITE_TRUNCATE",
                    destination=prod_table
                )
                
                sql = f"SELECT * FROM `{dummy_table}`"
                
                try:
                    with st.spinner("Executing complete environment teardown and reconstruction workflows..."):
                        query_job = client.query(sql, job_config=job_config)
                        query_job.result()
                        
                    st.success("🔥 Production registry completely reset and replaced with dummy playground snapshot!")
                    st.cache_data.clear()
                    time.sleep(1.5)
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to copy staging parameters: {e}")
                    st.code(sql, language="sql")
                    
    return selected_returned_row

# =============================================================================
# 1. HISTORICAL TELEMETRY GRAPH COMPONENT
# =============================================================================
def render_node_historical_graph(client, node_id):
    """Fetches and displays the complete historical thermal chart for the chosen node context."""
    st.markdown(f"### 📈 Historic Data: **{node_id}**")
    
    hist_q = f"""
        SELECT timestamp, temperature 
        FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` 
        WHERE NodeNum = '{node_id}' 
        ORDER BY timestamp ASC
    """
    try:
        with st.spinner("Retrieving complete historical telemetric data logs..."):
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
            st.caption("ℹ️ No historical telemetric data markers discovered for this hardware footprint.")
    except Exception as e:
        st.error(f"Failed generating historical context graph: {e}")

# =============================================================================
# 2. BULK ORCHESTRATOR TABS
# =============================================================================
def render_bulk_registry_page(client, proj_list):
    """
    Main orchestrator page for the Bulk Registry Manager toolset.
    Splits layout between file ingestion and mass decommissioning.
    """
    st.title("📦 Bulk Registry Manager")
    st.markdown(
        """
        Execute large-scale database operations across entire project lifecycles. 
        Use the tabs below to initialize a fresh site deployment or decommission a field project.
        """
    )
    
    target_registry = f"{PROJECT_ID}.{DATASET_ID}.node_registry"
    tab_upload, tab_retire = st.tabs(["📥 Bulk Upload Deployment", "🔚 Project-Wide Decommission"])
    
    with tab_upload:
        render_bulk_deployment_tab(client, target_registry)
        
    with tab_retire:
        render_bulk_decommission_tab(client, proj_list, target_registry)

# =============================================================================
# 3. INTERACTIVE ATTRIBUTE & ACTION MANAGER
# =============================================================================
def render_node_action_manager(client, selected_node_data, reg_df, proj_list, target_registry):
    """
    Displays chart, interactive historical log selector with relative time tracking metrics,
    full attribute configuration overrides, operational task panels, and administrative 
    pipeline delete tools.
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
    now_utc = pd.Timestamp.now(tz='UTC')
    
    # ---------------------------------------------------------------
    # CHRONOLOGICAL AGE CALCULATION LAYER
    # ---------------------------------------------------------------
    if 'last_ping' in history_df.columns:
        history_df['hours_hidden'] = history_df['last_ping'].apply(
            lambda x: (now_utc - pd.to_datetime(x).tz_convert('UTC')).total_seconds() / 3600.0
            if pd.notnull(x) else np.nan
        )
    elif 'hrs_lag' in history_df.columns:
        history_df['hours_hidden'] = pd.to_numeric(history_df['hrs_lag'], errors='coerce')
    else:
        try:
            ping_q = f"SELECT MAX(timestamp) as lp FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` WHERE NodeNum = '{node_id}'"
            lp_res = client.query(ping_q).to_dataframe()
            if not lp_res.empty and pd.notnull(lp_res['lp'].iloc[0]):
                history_df['hours_hidden'] = (now_utc - pd.to_datetime(lp_res['lp'].iloc[0]).tz_convert('UTC')).total_seconds() / 3600.0
            else:
                history_df['hours_hidden'] = np.nan
        except Exception:
            history_df['hours_hidden'] = np.nan

    # Generate the readable text display from our calculated float
    def format_history_lag(hours):
        if pd.isna(hours) or hours == float('inf'):
            return "No Pings"
        elif hours < 1.0:
            mins = int(hours * 60)
            return f"{mins}m ago" if mins > 0 else "Just now"
        else:
            return f"{hours:.1f}h ago"

    history_df['hours_hidden'] = pd.to_numeric(history_df['hours_hidden'], errors='coerce').fillna(float('inf'))
    history_df['Hours Since Last Seen'] = history_df['hours_hidden'].apply(format_history_lag)
    
    # Pre-sort chronologically (active pings up top, missing links at the bottom)
    history_df = history_df.sort_values(by='hours_hidden', ascending=True).reset_index(drop=True)

    # Scrub physical tracking hash columns from screen presentation
    cols_to_drop = ['physicalID', 'PhysicalID', 'last_ping', 'hrs_lag']
    history_df = history_df.drop(columns=[c for c in cols_to_drop if c in history_df.columns], errors='ignore')
    
    # Inject our interactive control check column
    history_df.insert(0, "Edit Target", False)
    
    # ---------------------------------------------------------------
    # HISTORY GRID CELL-LEVEL BACKGROUND STYLER
    # ---------------------------------------------------------------
    def assignment_history_styler(data):
        canvas = pd.DataFrame('', index=data.index, columns=data.columns)
        for i in data.index:
            try:
                val = data.loc[i, 'hours_hidden']
                hours_val = None if (val == float('inf') or pd.isnull(val)) else float(val)
                color_style = assign_row_color(hours_val)
            except Exception:
                color_style = "background-color: transparent;"
            
            for col in data.columns:
                if col != "Edit Target":
                    canvas.loc[i, col] = color_style
        return canvas

    styled_history_df = history_df.style.apply(assignment_history_styler, axis=None)
    
    edited_hist_df = st.data_editor(
        styled_history_df,
        hide_index=True,
        use_container_width=True,
        column_config={
            "Edit Target": st.column_config.CheckboxColumn("Edit Target", default=False, required=True),
            "Hours Since Last Seen": st.column_config.TextColumn("Hours Since Last Seen")
        },
        disabled=[col for col in history_df.columns if col != "Edit Target"],
        column_order=[c for c in history_df.columns if c != "hours_hidden"],
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

    # =============================================================================
    # 3. EDITOR WITH DYNAMIC PROJECT LOCATION DROPDOWN
    # =============================================================================
    st.markdown("### 🛠️ Modify Assignment Attributes")
    
    edit_proj = st.selectbox(
        "Project Space", 
        [""] + proj_list, 
        index=proj_list.index(target_record['Project']) + 1 if target_record['Project'] in proj_list else 0,
        key="global_editor_project_selector"
    )
    
    if edit_proj == "Office":
        location_input_type = "text"
        default_loc_val = str(target_record.get('Location', 'Office'))
    else:
        location_input_type = "dropdown"
        existing_project_locations = sorted(reg_df[reg_df['Project'] == edit_proj]['Location'].dropna().unique().tolist(), key=natural_sort_key)
        if not existing_project_locations:
            existing_project_locations = ["Unassigned"]
        
        try:
            curr_loc_idx = existing_project_locations.index(target_record.get('Location'))
        except ValueError:
            curr_loc_idx = 0

    # --- HERE IS WHERE THE IS_TARGET_LORD LOGIC STARTS ---
    is_target_lord = "-ch" in str(target_record.get('NodeNum', ''))
    base_logger_id = str(target_record.get('NodeNum')).split("-ch")[0] if is_target_lord else ""

    if is_target_lord:
        st.warning(f"📡 Multi-Channel Logger Context: This channel belongs to Lord Logger **{base_logger_id}**.")

    with st.form("global_node_editor_form"):
        col1, col2 = st.columns(2)
        edit_nodenum = col1.text_input("Node ID (NodeNum)", value=str(target_record.get('NodeNum', '')))
        
        if location_input_type == "text":
            edit_loc = col2.text_input("Office Sub-Location", value=default_loc_val)
        else:
            edit_loc = col2.selectbox("Assign to Location", existing_project_locations, index=curr_loc_idx)
        
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
        
        is_open_ended = col8.checkbox("Open-Ended (No End Date)", value=pd.isnull(target_record.get('End_Date')))
        if is_open_ended:
            edit_end = None
            col8.caption("ℹ️ This assignment will remain active with no set expiration.")
        else:
            edit_end = col8.date_input("End Date", value=pd.to_datetime(target_record.get('End_Date')).date() if pd.notnull(target_record.get('End_Date')) else datetime.now().date())
        
        # UI Checkbox toggle inside the form boundaries
        apply_all_channels = False
        if is_target_lord:
            st.markdown("---")
            apply_all_channels = st.checkbox(
                f"🔗 Bulk Update Option: Apply these changes to ALL 12 channels on {base_logger_id}?",
                value=False,
                help="Checking this will update Project, Location, Status, and Dates for all channels belonging to this logger. Individual depths/banks will remain distinct unless explicitly configured."
            )

        # Generate a distinct runtime key for the submit button to eliminate duplicate ID collisions
        clean_start_str = pd.to_datetime(target_record.get('Start_Date')).strftime('%Y%m%d') if pd.notnull(target_record.get('Start_Date')) else "new"
        submit_btn_key = f"submit_changes_{node_id}_{clean_start_str}"

        # SINGLE SUBMISSION POINT WITH UNIQUE KEY
        if st.form_submit_button("💾 Save Changes", key=submit_btn_key):
            if edit_bank.strip() != "":
                sql_depth = "NULL"
            else:
                sql_depth = "NULL" if edit_depth == 0.0 else f"{edit_depth}"
                
            # If marked dead, force the end date to be the exact execution start date
            if edit_status == "Dead":
                sql_end = f"DATE('{edit_start.isoformat()}')"
            else:
                sql_end = "NULL" if is_open_ended or not edit_end else f"DATE('{edit_end.isoformat()}')"
                
            sql_bank = f"'{edit_bank.strip()}'" if edit_bank.strip() != "" else "NULL"
            
            # =============================================================================
            # APPLICATION-LAYER AUTOMATION FOR "DEAD" & "OFFICE" ROUTING
            # =============================================================================
            # Intercepts and overrides target project/location parameters cleanly
            if edit_status == "Dead":
                final_project = "Dead"
                final_location = "Dead"  # Stripped "Stock"
            else:
                final_project = edit_proj.strip()
                final_location = edit_loc.strip() if hasattr(edit_loc, 'strip') else edit_loc
                if final_location == "Office Stock":
                    final_location = "Office"  # Stripped "Stock"

            if is_target_lord and apply_all_channels:
                # =============================================================================
                # BULK LORD LOGGER UPDATE (PRESERVES DEPTH/BANK + ENFORCES 'DEAD' END_DATE)
                # =============================================================================
                update_sql = f"""
                    BEGIN TRANSACTION;
                    
                    UPDATE `{target_registry}`
                    SET 
                        Project = '{final_project}',
                        Location = '{final_location}',
                        SensorStatus = '{edit_status}',
                        Start_Date = DATE('{edit_start.isoformat()}'),
                        End_Date = {sql_end}
                    WHERE NodeNum LIKE '{base_logger_id}-ch%' 
                      AND End_Date IS NULL;
                    
                    COMMIT;
                """
            else:
                # =============================================================================
                # STANDARD SINGLE-ROW ISOLATION UPDATE RULES
                # =============================================================================
                where_bank = f"Bank = '{target_record['Bank']}'" if pd.notnull(target_record.get('Bank')) and str(target_record.get('Bank')).strip() != '' else "Bank IS NULL"
                where_depth = f"Depth = {target_record['Depth']}" if pd.notnull(target_record.get('Depth')) and str(target_record.get('Depth')).strip() != '' else "Depth IS NULL"
                where_end = f"End_Date = DATE('{pd.to_datetime(target_record['End_Date']).strftime('%Y-%m-%d')}')" if pd.notnull(target_record.get('End_Date')) else "End_Date IS NULL"

                update_sql = f"""
                    BEGIN TRANSACTION;
                    
                    DELETE FROM `{target_registry}`
                    WHERE NodeNum = '{target_record['NodeNum']}'
                      AND Start_Date = DATE('{pd.to_datetime(target_record['Start_Date']).strftime('%Y-%m-%d')}')
                      AND Project = '{target_record['Project']}'
                      AND Location = '{target_record['Location']}'
                      AND {where_bank}
                      AND {where_depth}
                      AND {where_end};
                    
                    INSERT INTO `{target_registry}` (NodeNum, Project, Location, Bank, Depth, SensorStatus, Start_Date, End_Date)
                    VALUES (
                      '{edit_nodenum.strip()}',
                      '{final_project}',
                      '{final_location}',
                      {sql_bank},
                      {sql_depth},
                      '{edit_status}',
                      DATE('{edit_start.isoformat()}'),
                      {sql_end}
                    );
                    
                    COMMIT;
                """
            try:
                client.query(update_sql).result()
                st.success(f"✅ Changes committed successfully. Status: '{edit_status}' | Project: '{final_project}'")
                st.cache_data.clear()
                time.sleep(1)
                st.rerun()
            except Exception as e:
                st.error(f"Failed to safely modify database record tables: {e}")

    # ===============================================================
    # 4. OPERATIONAL TASK PANEL
    # ===============================================================
    st.markdown("##### Quick Operational Tasks")
    c_act1, c_act2, c_act3, c_act4 = st.columns(4)
    
    # --- END ASSIGNMENT ---
    with c_act1:
        with st.expander("🔚 End Assignment"):
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
                    VALUES ('{node_id}', 'Office', 'Office', '{node_id}', NULL, '{end_status_input}', DATE('{date_iso}'));
                    COMMIT;
                """
                try:
                    client.query(bulk_sql).result()
                    st.success(f"✅ Node {node_id} ended and transferred to Office records.")
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    st.error(f"Transaction execution failed: {e}")

    # --- CHANGE SENSOR ---
    with c_act2:
        with st.expander("🔄 Change Sensor / Entire Lord Loggers"):
            is_lord = "-ch" in node_id
            
            if is_lord:
                base_lord_id = node_id.split("-ch")[0]
                st.warning(f"📡 Multi-Channel Detected: This sensor belongs to Lord Logger **{base_lord_id}**. Swapping will perform a straight-across trade for all 12 channels.")
                swap_node_input = st.text_input("Replacement Base Lord ID (No channel suffix)", placeholder="e.g., LRD02", key="swap_sensor_input")
            else:
                swap_node_input = st.text_input("Replacement Node ID (NodeNum)", placeholder="e.g., TP-0105", key="swap_sensor_input")
                
            swap_date_input = st.date_input("Swap Execution Date", value=datetime.now().date(), key="swap_sensor_dt")
            
            if st.button("Execute Change Sensor", type="primary", use_container_width=True):
                new_input_clean = swap_node_input.strip()
                if not new_input_clean:
                    st.error("Please insert a valid target hardware replacement ID.")
                elif new_input_clean == node_id or (is_lord and new_input_clean == base_lord_id):
                    st.error("The replacement ID cannot be identical to the sensor hardware currently deployed.")
                else:
                    date_str = swap_date_input.isoformat()
                    
                    if new_input_clean.upper().startswith("TP"):
                        old_sensor_restock_loc = "Office"
                    elif new_input_clean.upper().startswith("SP"):
                        old_sensor_restock_loc = "Ambient Stock"
                    else:
                        old_sensor_restock_loc = "Office"

                    if is_lord:
                        lord_channels_q = f"""
                            SELECT NodeNum, Location, Bank, Depth 
                            FROM `{target_registry}` 
                            WHERE NodeNum LIKE '{base_lord_id}-ch%' AND End_Date IS NULL
                        """
                        try:
                            active_ch_df = client.query(lord_channels_q).to_dataframe()
                        except Exception as e:
                            st.error(f"Failed pulling Lord channel layout matrices: {e}")
                            active_ch_df = pd.DataFrame()

                        if active_ch_df.empty:
                            st.error(f"No active deployment rows discovered for channels under {base_lord_id}.")
                        else:
                            bulk_swap_sql = ["BEGIN TRANSACTION;"]
                            
                            for _, ch_row in active_ch_df.iterrows():
                                old_ch_node = ch_row['NodeNum']
                                ch_suffix = old_ch_node.split("-ch")[-1]
                                new_ch_node = f"{new_input_clean}-ch{ch_suffix}"
                                
                                bulk_swap_sql.append(f"""
                                    UPDATE `{target_registry}`
                                    SET End_Date = DATE('{date_str}'), SensorStatus = 'Archived'
                                    WHERE NodeNum = '{old_ch_node}' AND End_Date IS NULL;
                                """)
                                bulk_swap_sql.append(f"""
                                    INSERT INTO `{target_registry}` (NodeNum, Project, Location, Bank, Depth, SensorStatus, Start_Date)
                                    VALUES ('{old_ch_node}', 'Office', '{old_sensor_restock_loc}', '{old_ch_node}', NULL, 'Available', DATE('{date_str}'));
                                """)
                                bulk_swap_sql.append(f"""
                                    UPDATE `{target_registry}`
                                    SET End_Date = DATE('{date_str}'), SensorStatus = 'Archived'
                                    WHERE NodeNum = '{new_ch_node}' AND End_Date IS NULL;
                                """)
                                
                                sql_bank = f"'{ch_row['Bank']}'" if pd.notnull(ch_row['Bank']) and ch_row['Bank'] != 'None' and ch_row['Bank'] != '' else "NULL"
                                sql_depth = f"{ch_row['Depth']}" if pd.notnull(ch_row['Depth']) and str(ch_row['Depth']).strip() != '' else "NULL"
                                
                                bulk_swap_sql.append(f"""
                                    INSERT INTO `{target_registry}` (NodeNum, Project, Location, Bank, Depth, SensorStatus, Start_Date)
                                    VALUES ('{new_ch_node}', '{selected_node_data['Project']}', '{ch_row['Location']}', {sql_bank}, {sql_depth}, 'On Project', DATE('{date_str}'));
                                """)
                                
                            bulk_swap_sql.append("COMMIT;")
                            combined_lord_sql = "\n".join(bulk_swap_sql)
                            
                            try:
                                with st.spinner(f"Processing straight-across trade for all 12 channels ({base_lord_id} ➡️ {new_input_clean})..."):
                                    client.query(combined_lord_sql).result()
                                st.success(f"✅ Full Lord Logger Swap Complete: All channels transferred to base `{new_input_clean}` seamlessly.")
                                st.cache_data.clear()
                                time.sleep(1)
                                st.rerun()
                            except Exception as e:
                                st.error(f"Lord multi-channel swap failure: {e}")
                    else:
                        swap_sql = f"""
                            BEGIN TRANSACTION;
                            UPDATE `{target_registry}`
                            SET End_Date = DATE('{date_str}'), SensorStatus = 'Archived'
                            WHERE NodeNum = '{node_id}' AND End_Date IS NULL;

                            INSERT INTO `{target_registry}` (NodeNum, Project, Location, Bank, Depth, SensorStatus, Start_Date)
                            VALUES ('{node_id}', 'Office', '{old_sensor_restock_loc}', '{node_id}', NULL, 'Available', DATE('{date_str}'));

                            UPDATE `{target_registry}`
                            SET End_Date = DATE('{date_str}'), SensorStatus = 'Archived'
                            WHERE NodeNum = '{new_node}' AND End_Date IS NULL;

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
                            with st.spinner("Processing individual sensor swap..."):
                                client.query(swap_sql).result()
                            st.success(f"🔄 Change Sensor complete: {node_id} swapped with {new_node}.")
                            st.cache_data.clear()
                            time.sleep(1)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Sensor swap failed: {e}")

    # --- ADD NEW MANUAL ASSIGNMENT ---
    with c_act3:
        with st.expander("➕ Add New Manual Assignment"):
            st.markdown("##### Force-Insert a Fresh Assignment Record Lineage")
            
            add_proj = st.selectbox("Manual Target Project", proj_list, key="manual_add_proj")
            
            if add_proj == "Office":
                add_loc = st.text_input("Manual Office Sub-Location", value="Office", key="manual_add_loc_text")
            else:
                add_loc_opts = sorted(reg_df[reg_df['Project'] == add_proj]['Location'].dropna().unique().tolist(), key=natural_sort_key)
                if not add_loc_opts:
                    add_loc_opts = ["Unassigned"]
                add_loc = st.selectbox("Manual Target Location", add_loc_opts, key="manual_add_loc_drop")
                
            with st.form("manual_assignment_sub_form"):
                c_add1, c_add2 = st.columns(2)
                add_bank = c_add1.text_input("Manual Bank Field", value="")
                add_depth = c_add2.number_input("Manual Depth (ft)", value=0.0)
                add_start = st.date_input("Manual Start Date", value=datetime.now().date())
                
                if st.form_submit_button("Commit Manual Assignment Row", use_container_width=True):
                    if add_bank.strip() != "":
                        sql_manual_depth = "NULL"
                    else:
                        sql_manual_depth = "NULL" if add_depth == 0.0 else f"{add_depth}"
                        
                    sql_manual_bank = f"'{add_bank.strip()}'" if add_bank.strip() != "" else "NULL"
                    
                    insert_manual_sql = f"""
                        INSERT INTO `{target_registry}` (NodeNum, Project, Location, Bank, Depth, SensorStatus, Start_Date)
                        VALUES (
                            '{node_id}', 
                            '{add_proj}', 
                            '{add_loc.strip() if hasattr(add_loc, 'strip') else add_loc}', 
                            {sql_manual_bank}, 
                            {sql_manual_depth}, 
                            'On Project', 
                            DATE('{add_start.isoformat()}')
                        )
                    """
                    try:
                        client.query(insert_manual_sql).result()
                        st.success(f"✅ Clean assignment forced entry added for Node {node_id} on project {add_proj}.")
                        st.cache_data.clear()
                        time.sleep(1)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Manual override insert statement failed: {e}")

    # --- DELETE ENTRY ---
    with c_act4:
        with st.expander("🗑️ Delete Entry"):
            st.warning("⚠️ Danger Zone: This drops the targeted assignment entry row completely out of your BigQuery system logs.")
            confirm_check = st.checkbox("Confirm permanent deletion of this row", key=f"del_confirm_{target_record['Start_Date']}")
            
            if st.button("Delete Selected Assignment Record", type="primary", use_container_width=True):
                if not confirm_check:
                    st.error("Please click the confirmation checkbox to authorize the database removal transaction.")
                else:
                    where_bank = f"Bank = '{target_record['Bank']}'" if pd.notnull(target_record.get('Bank')) and str(target_record.get('Bank')).strip() != '' else "Bank IS NULL"
                    where_depth = f"Depth = {target_record['Depth']}" if pd.notnull(target_record.get('Depth')) and str(target_record.get('Depth')).strip() != '' else "Depth IS NULL"
                    where_end = f"End_Date = DATE('{pd.to_datetime(target_record['End_Date']).strftime('%Y-%m-%d')}')" if pd.notnull(target_record.get('End_Date')) else "End_Date IS NULL"

                    delete_sql = f"""
                        DELETE FROM `{target_registry}`
                        WHERE NodeNum = '{target_record['NodeNum']}'
                          AND Start_Date = DATE('{pd.to_datetime(target_record['Start_Date']).strftime('%Y-%m-%d')}')
                          AND Project = '{target_record['Project']}'
                          AND Location = '{target_record['Location']}'
                          AND {where_bank}
                          AND {where_depth}
                          AND {where_end}
                    """
                    try:
                        client.query(delete_sql).result()
                        st.warning(f"🗑️ Assignment row deleted for Node {target_record['NodeNum']} on {target_record['Project']}.")
                        st.cache_data.clear()
                        time.sleep(1)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to execute row delete query logic: {e}")
# =============================================================================
# FUNCTION: DATA CHECKER DIAGNOSTICS MODULE
# =============================================================================
def render_data_checker(client, reg_df):
    """
    Scans node deployment timelines to isolate configuration patterns, 
    pipeline errors, parallel sensor position conflicts, and distinct duplication modes.
    Applies real-time alert row-level color coding to all diagnostic grids.
    """
    st.markdown("---")
    st.subheader("🔍 Data Checker Diagnostics")
    
    c1, c2, c3, c4 = st.tabs([
        "⏱️ Gaps in Data (Missing Office Time)", 
        "🚨 Orphaned Nodes (Missing Next Assignment)",
        "🚨 Multiple / Duplicate Assignments",
        "🚨 Location & Position Overlaps"
    ])
    
    df = reg_df.copy()
    df['Start_Date'] = pd.to_datetime(df['Start_Date']).dt.date
    df['End_Date'] = pd.to_datetime(df['End_Date']).dt.date
    
    grouped = df.groupby('NodeNum')
    
    gaps_in_data = []
    orphaned_nodes = []
    
    # Track conflicting records directly instead of just Node IDs
    identity_duplicate_rows = []
    cross_project_splits = []
    
    today = datetime.now().date()
    
    for node_id, group in grouped:
        sorted_group = group.sort_values(by='Start_Date')
        records = sorted_group.to_dict('records')
        
        has_gap = False
        is_orphaned = False
        
        # Track active profiles for the cross-project verification loop
        active_projects_assigned = set()
        
        for i in range(len(records)):
            current_rec = records[i]
            
            if pd.isnull(current_rec['Start_Date']):
                has_gap = True
                continue
            
            # Count toward cross-project splits if the record is currently active
            is_currently_active = pd.isnull(current_rec['End_Date']) or (current_rec['End_Date'] >= today)
            if is_currently_active and pd.notnull(current_rec['Project']):
                active_projects_assigned.add(current_rec['Project'])
                
            # --- OPTION A MATH: IDENTITY DUPLICATE OVERLAPS (STRICT MATCH) ---
            for j in range(i + 1, len(records)):
                compare_rec = records[j]
                if pd.notnull(compare_rec['Start_Date']):
                    same_proj = current_rec['Project'] == compare_rec['Project']
                    same_start = current_rec['Start_Date'] == compare_rec['Start_Date']
                    same_end = current_rec['End_Date'] == compare_rec['End_Date']
                    
                    # If an exact matching row pair is found, isolate both rows directly
                    if same_proj and same_start and same_end:
                        identity_duplicate_rows.append(current_rec)
                        identity_duplicate_rows.append(compare_rec)

            # --- CHRONOLOGICAL GAP & ORPHAN CHECKS ---
            if i < len(records) - 1:
                next_rec = records[i+1]
                if pd.notnull(current_rec['End_Date']) and pd.notnull(next_rec['Start_Date']):
                    if (next_rec['Start_Date'] - current_rec['End_Date']).days > 1:
                        has_gap = True
            else:
                if pd.notnull(current_rec['End_Date']):
                    is_orphaned = True
            
        # OPTION B MATH: Assigned to more than one active project right now
        if len(active_projects_assigned) > 1:
            cross_project_splits.append(node_id)
            
        if has_gap:
            gaps_in_data.append(node_id)
        elif is_orphaned and len(identity_duplicate_rows) == 0 and node_id not in cross_project_splits:
            orphaned_nodes.append(node_id)

    # --- POSITION OVERLAPS CALCULATION FOR TAB 4 ---
    active_df = df[df['Start_Date'] <= today].copy()
    active_df = active_df[active_df['End_Date'].isna() | (active_df['End_Date'] >= today)]
    active_field_df = active_df[active_df['Project'] != 'Office'].copy()
    
    active_field_df['Bank'] = active_field_df['Bank'].fillna('').astype(str).str.strip()
    active_field_df['Depth'] = active_field_df['Depth'].fillna(0.0).astype(float)
    
    position_groups = active_field_df.groupby(['Project', 'Location', 'Bank', 'Depth'])
    conflicting_rows_list = []
    for position_key, pos_group in position_groups:
        if pos_group['NodeNum'].nunique() > 1:
            conflicting_rows_list.append(pos_group)
            
    position_conflicts_df = pd.concat(conflicting_rows_list).sort_values(['Project', 'Location', 'Bank', 'Depth']) if conflicting_rows_list else pd.DataFrame()

    # =============================================================================
    # INTERNAL REUSABLE ROW STYLING NESTED LOGIC
    # =============================================================================
    def apply_diagnostic_row_colors(target_df):
        """
        Parses hours_hidden floats within data checker subsets to return a styled matrix object.
        """
        working_df = target_df.copy()
        if 'hours_hidden' not in working_df.columns:
            working_df['hours_hidden'] = float('inf')
        else:
            working_df['hours_hidden'] = pd.to_numeric(working_df['hours_hidden'], errors='coerce').fillna(float('inf'))
            
        def row_styler(row):
            try:
                val = row['hours_hidden']
                hours_val = None if (val == float('inf') or pd.isnull(val)) else float(val)
                color_style = assign_row_color(hours_val)
            except Exception:
                color_style = "background-color: transparent;"
            return [color_style] * len(row)
            
        return working_df.style.apply(row_styler, axis=1)

    # ===============================================================
    # TAB 1: Gaps in Data
    # ===============================================================
    with c1:
        st.markdown("##### Nodes with a chronological gap where they were not assigned—requires unmonitored time to be added to Office")
        if gaps_in_data:
            gap_display_df = df[df['NodeNum'].isin(gaps_in_data)].sort_values(['NodeNum', 'Start_Date'])
            styled_gaps = apply_diagnostic_row_colors(gap_display_df)
            st.dataframe(
                styled_gaps, 
                use_container_width=True, 
                hide_index=True,
                column_order=['NodeNum', 'Project', 'Location', 'Start_Date', 'End_Date', 'SensorStatus'],
                column_config={"NodeNum": "Node ID"}
            )
        else:
            st.success("✅ No timeline gaps or missing 'Office' storage windows detected across node history logs.")

    # ===============================================================
    # TAB 2: Orphaned Nodes (MODIFIED TO IGNORE DEAD SENSORS)
    # ===============================================================
    with c2:
        st.markdown("##### Nodes that have an end date on their last assignment but did not get transferred into a new project or Office stock")
        if orphaned_nodes:
            orphan_display_df = df[df['NodeNum'].isin(orphaned_nodes)].sort_values(['NodeNum', 'Start_Date'])
            
            # CRITICAL FILTER LAYER: Strip out any entry containing Dead parameters explicitly
            if not orphan_display_df.empty:
                orphan_display_df = orphan_display_df[
                    (orphan_display_df['SensorStatus'].str.upper() != 'DEAD') &
                    (orphan_display_df['Project'].str.upper() != 'DEAD') &
                    (orphan_display_df['Location'].str.upper() != 'DEAD')
                ]
                
            if not orphan_display_df.empty:
                last_entries = orphan_display_df.groupby('NodeNum').last().reset_index()
                styled_orphans = apply_diagnostic_row_colors(last_entries)
                st.dataframe(
                    styled_orphans, 
                    use_container_width=True, 
                    hide_index=True,
                    column_order=['NodeNum', 'Project', 'Location', 'Start_Date', 'End_Date', 'SensorStatus'],
                    column_config={"NodeNum": "Node ID"}
                )
            else:
                st.success("✅ Clean terminations verified. No active field nodes currently stand orphaned.")
        else:
            st.success("✅ Clean terminations verified. All decommissioned nodes successfully occupy new project profiles or Office stock rows.")
    # ===============================================================
    # TAB 3: MULTIPLE / DUPLICATE ASSIGNMENTS
    # ===============================================================
    with c3:
        dupe_mode = st.radio(
            "Select Duplication Diagnostic View Filter Mode:",
            ["View Timeline Overlaps (Same Project / Same Start & End Dates)", "View Cross-Project Splits (Assigned to > 1 Project Concurrently)"],
            horizontal=True,
            key="dupe_diagnostic_mode_toggle"
        )
        
        if "Same Project" in dupe_mode:
            st.markdown("##### 🚨 Identity Overlaps: Displaying only the exact duplicate row entries causing database conflicts.")
            if identity_duplicate_rows:
                display_dupe_df = pd.DataFrame(identity_duplicate_rows).drop_duplicates().sort_values(['NodeNum', 'Start_Date'])
                styled_dupes = apply_diagnostic_row_colors(display_dupe_df)
                st.dataframe(
                    styled_dupes, 
                    use_container_width=True, 
                    hide_index=True,
                    column_order=['NodeNum', 'Project', 'Location', 'Start_Date', 'End_Date', 'SensorStatus'],
                    column_config={"NodeNum": "Node ID"}
                )
            else:
                st.success("✅ Clean database entries. No duplicate entries discovered with identical project and date windows.")
        else:
            st.markdown("##### 🚨 Split Deployments: Physical sensors that hold more than one active project assignment row simultaneously.")
            if cross_project_splits:
                display_split_df = df[(df['NodeNum'].isin(cross_project_splits)) & (df['Start_Date'] <= today) & (df['End_Date'].isna() | (df['End_Date'] >= today))].sort_values(['NodeNum', 'Project'])
                styled_splits = apply_diagnostic_row_colors(display_split_df)
                st.dataframe(
                    styled_splits, 
                    use_container_width=True, 
                    hide_index=True,
                    column_order=['NodeNum', 'Project', 'Location', 'Start_Date', 'End_Date', 'SensorStatus'],
                    column_config={"NodeNum": "Node ID"}
                )
            else:
                st.success("✅ Clean single-project allocation. All system sensors map to a maximum of one active project environment.")

    # ===============================================================
    # TAB 4: Location & Position Overlaps
    # ===============================================================
    with c4:
        st.markdown("##### 🚨 Position Conflicts: Multiple physical sensors assigned to the same Location and Bank/Depth concurrently")
        if not position_conflicts_df.empty:
            display_conflict_df = position_conflicts_df.copy()
            display_conflict_df['Depth'] = display_conflict_df['Depth'].apply(lambda x: f"{x} ft" if x > 0 else "-")
            display_conflict_df['Bank'] = display_conflict_df['Bank'].apply(lambda x: x if x != "" else "-")
            
            styled_conflicts = apply_diagnostic_row_colors(display_conflict_df)
            st.dataframe(
                styled_conflicts, 
                use_container_width=True, 
                hide_index=True,
                column_order=['Project', 'Location', 'Bank', 'Depth', 'NodeNum', 'Start_Date', 'SensorStatus'],
                column_config={"NodeNum": "Node ID"}
            )
        else:
            st.success("✅ Perfect grid alignment. Every active physical installation coordinate holds exactly one distinct hardware sensor entity.")

# =============================================================================
# PAGE MODULE: 📡 PROJECT OVERVIEW
# =============================================================================

def render_project_status_dashboard(client, selected_project, unit_label, target_registry):
    """
    Renders high-level data aggregation metrics alongside custom thermal threshold distributions.
    Dynamically scales soil freezing engineering targets based on Fahrenheit or Celsius units.
    """
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
    unit_mode = st.session_state.get('unit_mode', 'Fahrenheit')

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
            else:
                latest_time = latest_time.tz_convert('UTC')
            
            lag_hrs = (now_utc - latest_time).total_seconds() / 3600
            val = g_df['avg_now'].mean() if pd.notnull(g_df['avg_now'].mean()) else g_df['latest_temp'].mean()
            
            if lag_hrs > 1.1: 
                st.subheader(f"⚠️ Offline {int(lag_hrs)}h")
            else: 
                display_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
                st.title(f"{display_val:.1f}{unit_label}")
            
            active_1h = int(g_df['avg_now'].notnull().sum())
            active_24h = int((g_df['pings_24h'] > 0).sum())
            st.write(f"**{active_1h}/{len(g_df)}** (1h) | **{active_24h}/{len(g_df)}** (24h)")
            
            # Extract boundaries safely, filtering out missing values
            min_now_val = g_df['min_now'].dropna().min()
            max_now_val = g_df['max_now'].dropna().max()
            min_24h_val = g_df['min_24h'].dropna().min()
            max_24h_val = g_df['max_24h'].dropna().max()
            
            if pd.notnull(min_now_val) and pd.notnull(max_now_val):
                mn = (min_now_val - 32) * 5/9 if unit_mode == "Celsius" else min_now_val
                mx = (max_now_val - 32) * 5/9 if unit_mode == "Celsius" else max_now_val
                st.caption(f"Cur: {mn:.1f} to {mx:.1f}{unit_label}")
            else:
                st.caption("Cur: N/A to N/A")
                
            if pd.notnull(min_24h_val) and pd.notnull(max_24h_val):
                mn24 = (min_24h_val - 32) * 5/9 if unit_mode == "Celsius" else min_24h_val
                mx24 = (max_24h_val - 32) * 5/9 if unit_mode == "Celsius" else max_24h_val
                st.caption(f"24h: {mn24:.1f} to {mx24:.1f}{unit_label}")
            else:
                st.caption("24h: N/A to N/A")
            
            t_row = st.columns(2)
            try:
                prev_1h = g_df['avg_1h_prev'].mean()
                arrow_1h = get_trend_arrow(val, prev_1h) if pd.notnull(prev_1h) else "➡️ N/A"
                t_row[0].caption(f"1h\n{arrow_1h}")
            except Exception:
                t_row[0].caption("1h\n➡️ N/A")
                
            try:
                prev_6h = g_df['avg_6h_prev'].mean()
                arrow_6h = get_trend_arrow(val, prev_6h) if pd.notnull(prev_6h) else "➡️ N/A"
                t_row[1].caption(f"6h\n{arrow_6h}")
            except Exception:
                t_row[1].caption("6h\n➡️ N/A")
            
            # =============================================================================
            # DYNAMIC FREEZING THRESHOLD EVALUATION
            # =============================================================================
            st.markdown("---")
            temps = g_df['latest_temp'].dropna()
            total_sensors = len(g_df)
            
            # Calculate metrics dynamically using converted Fahrenheit values
            if unit_mode == "Celsius":
                converted_temps = (temps - 32) * 5/9
                t_0, t_neg10, t_neg15, t_32, t_20 = -17.8, -23.3, -26.1, 0.0, -6.7
            else:
                converted_temps = temps
                t_0, t_neg10, t_neg15, t_32, t_20 = 0.0, -10.0, -15.0, 32.0, 20.0
                
            if h_type == "Supply":
                sub_0 = sum(converted_temps < t_0)
                sub_10 = sum(converted_temps < t_neg10)
                sub_15 = sum(converted_temps < t_neg15)
                st.markdown(f"❄️ **Below 0°F / -17.8°C:** `{sub_0}/{total_sensors}`")
                st.markdown(f"🥶 **Below -10°F / -23.3°C:** `{sub_10}/{total_sensors}`")
                st.markdown(f"🧊 **Below -15°F / -26.1°C:** `{sub_15}/{total_sensors}`")
                
            elif h_type == "Return":
                # For return line arrays, look for target thresholds (10°F, 0°F, -10°F)
                t_10_target = -12.2 if unit_mode == "Celsius" else 10.0
                sub_10 = sum(converted_temps < t_10_target)
                sub_0 = sum(converted_temps < t_0)
                sub_10_neg = sum(converted_temps < t_neg10)
                st.markdown(f"🟢 **Below 10°F / -12.2°C:** `{sub_10}/{total_sensors}`")
                st.markdown(f"❄️ **Below 0°F / -17.8°C:** `{sub_0}/{total_sensors}`")
                st.markdown(f"🥶 **Below -10°F / -23.3°C:** `{sub_10_neg}/{total_sensors}`")
                
            elif h_type == "TempPipes":
                sub_freezing = sum(converted_temps < t_32)
                sub_20 = sum(converted_temps < t_20)
                sub_0 = sum(converted_temps < t_0)
                st.markdown(f"💧 **Below Freezing:** `{sub_freezing}/{total_sensors}`")
                st.markdown(f"❄️ **Below 20°F / -6.7°C:** `{sub_20}/{total_sensors}`")
                st.markdown(f"🥶 **Below 0°F / -17.8°C:** `{sub_0}/{total_sensors}`")

# =============================================================================
# FUNCTION: HARDWARE INTEGRITY TABLE
# =============================================================================
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
    
    try:
        df = client.query(query, job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)]
        )).to_dataframe()
    except Exception as e:
        st.error(f"Hardware Table Query Failed: {e}")
        return

    if df.empty: 
        st.info("No active nodes found for connectivity table.")
        return

    now_utc = pd.Timestamp.now(tz='UTC')

    def row_processor(row):
        ping = row['last_ping']
        
        if pd.isnull(ping):
            hours_hidden = float('inf')
            txt = "❌ Never"
            style = "background-color: #d1d5db; color: #1f2937;" 
        else:
            ts = ping if ping.tzinfo else ping.tz_localize('UTC')
            diff_mins = (now_utc - ts).total_seconds() / 60.0
            hours_hidden = diff_mins / 60.0
            
            if hours_hidden < 1.0:
                txt = f"{int(diff_mins)}m ago" if diff_mins >= 1.0 else "Just now"
                style = "background-color: #d1fae5; color: #065f46;" 
            elif 1.0 <= hours_hidden <= 6.0:
                txt = f"{hours_hidden:.1f}h ago"
                style = "background-color: #fef08a; color: #854d0e;" 
            elif 6.0 < hours_hidden <= 12.0:
                txt = f"{hours_hidden:.1f}h ago"
                style = "background-color: #fed7aa; color: #9a3412;" 
            elif 12.0 < hours_hidden <= 24.0:
                txt = f"{hours_hidden:.1f}h ago"
                style = "background-color: #fca5a5; color: #991b1b;" 
            else:
                txt = f"{hours_hidden:.1f}h ago"
                style = "background-color: #d1d5db; color: #1f2937;" 
        
        pos = f"{row['Depth']}ft" if (pd.notnull(row['Depth']) and row['Depth'] != 0) else f"Bank {row['Bank']}"
        trend = get_trend_arrow(row['avg_now'], row['avg_1h_prev'])
        
        return pd.Series([txt, style, pos, trend, hours_hidden])

    df[['Seen_Text', 'Seen_Style', 'Pos_Label', 'Trend', 'hours_hidden']] = df.apply(row_processor, axis=1)

    df['hours_hidden'] = pd.to_numeric(df['hours_hidden'], errors='coerce').fillna(float('inf'))
    df = df.sort_values(by='hours_hidden', ascending=True).reset_index(drop=True)

    display_df = pd.DataFrame({
        "Node ID": df['NodeNum'],
        "Location": df['Location'],
        "Position": df['Pos_Label'],
        "Last Seen": df['Seen_Text'],
        "24h Coverage": df['coverage_24h'], 
        "1h Change": df['Trend'],
        "Last Temp": df['last_temp'].apply(lambda x: fmt_temp(x, unit_mode, unit_label)),
        "1h Pings": df['pings_1h'],
        "6h Pings": df['pings_6h'],
        "24h Pings": df['pings_24h']
    })

    def diagnostic_styler(data):
        style_df = pd.DataFrame('', index=data.index, columns=data.columns)
        for i in data.index:
            style_df.loc[i, 'Last Seen'] = df.loc[i, 'Seen_Style']
            
            if df.loc[i, 'SensorStatus'] == 'Diagnostic':
                style_df.loc[i, 'Node ID'] = 'background-color: #ff4b4b; color: white; font-weight: bold;'
                
        return style_df

    st.dataframe(
        display_df.style.apply(diagnostic_styler, axis=None), 
        use_container_width=True, 
        hide_index=True,
        column_config={
            "24h Coverage": st.column_config.ProgressColumn(
                "24h Coverage", 
                format="%.1f%%", 
                min_value=0, 
                max_value=100
            ),
            "1h Pings": st.column_config.NumberColumn("1h Pings", format="%d"),
            "6h Pings": st.column_config.NumberColumn("6h Pings", format="%d"),
            "24h Pings": st.column_config.NumberColumn("24h Pings", format="%d"),
        }
    )

# =============================================================================
# PAGE MODULE: 🔍 SENSOR STATUS
# =============================================================================

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
    unit_mode = st.session_state.get('unit_mode', 'Fahrenheit')
    unit_label = "°C" if unit_mode == "Celsius" else "°F"
    
    st.markdown(f"### 📊 Comparative Analysis: **{node_id}** vs. Location Baseline")
    
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
            
            if unit_mode == "Celsius":
                data_df['temperature'] = (data_df['temperature'] - 32) * 5/9
                data_df['peer_avg'] = (data_df['peer_avg'] - 32) * 5/9
            
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=data_df['timestamp'],
                y=data_df['temperature'],
                mode='lines',
                name=f"Sensor {node_id}",
                line=dict(color='#00d4ff', width=2.5)
            ))
            
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
                yaxis_title=f"Temperature ({unit_label})",
                legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01)
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("ℹ️ No recent matching telemetry logs found to plot for this sensor window.")
    except Exception as e:
        st.error(f"Failed to build visual deep-dive chart: {e}")


def render_sensor_status(client, selected_project, unit_label, unit_mode, display_tz, target_registry):
    """
    Queries historical windows to analyze peer drift trends, stability performance scoring,
    and reporting efficiencies across project scopes filtered dynamically by Project and Location.
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

    # Enhanced Single-Pass BigQuery Extraction checking actual uptime pings vs expected hours
    query = f"""
        WITH BaseReporting AS (
            SELECT 
                m.NodeNum, m.timestamp, m.temperature, m.Location, m.Bank, m.Depth, m.Project,
                AVG(m.temperature) OVER (PARTITION BY m.Location, m.timestamp) as peer_avg
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
            WHERE m.Project = @proj_id
        ),
        
        AssignmentWindows AS (
            SELECT 
                NodeNum,
                Start_Date,
                COALESCE(End_Date, CURRENT_DATE()) AS Effective_End,
                DATE_DIFF(COALESCE(End_Date, CURRENT_DATE()), Start_Date, DAY) * 24 AS Expected_Hours
            FROM `{target_registry}`
            WHERE Project = @proj_id AND Project != 'Dead'
        ),
        
        ActualProjectPings AS (
            SELECT 
                m.NodeNum,
                a.Start_Date,
                COUNT(m.timestamp) AS Actual_Pings_Logged
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
            INNER JOIN AssignmentWindows a 
              ON m.NodeNum = a.NodeNum 
              AND EXTRACT(DATE FROM m.timestamp) BETWEEN a.Start_Date AND a.Effective_End
            WHERE m.Project = @proj_id
            GROUP BY m.NodeNum, a.Start_Date
        ),

        HistoricalStats AS (
            SELECT 
                b.NodeNum, b.Location, b.Bank, b.Depth,
                MAX(b.timestamp) AS last_ping,
                ARRAY_AGG(b.temperature ORDER BY b.timestamp DESC LIMIT 1)[OFFSET(0)] AS current_temp,
                ARRAY_AGG(b.peer_avg ORDER BY b.timestamp DESC LIMIT 1)[OFFSET(0)] AS current_peer_avg,
                MAX(CASE WHEN b.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR) THEN b.temperature END) - 
                MIN(CASE WHEN b.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR) THEN b.temperature END) as swing_2h,
                MAX(CASE WHEN b.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN b.temperature END) - 
                MIN(CASE WHEN b.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN b.temperature END) as swing_24h,
                (COUNT(DISTINCT CASE WHEN b.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN TIMESTAMP_TRUNC(b.timestamp, HOUR) END) / 24.0) * 100 as coverage_24h
            FROM BaseReporting b
            GROUP BY b.NodeNum, b.Location, b.Bank, b.Depth
        )
        
        SELECT 
            h.*,
            a.Expected_Hours,
            COALESCE(p.Actual_Pings_Logged, 0) AS Actual_Pings_Logged
        FROM HistoricalStats h
        LEFT JOIN AssignmentWindows a ON h.NodeNum = a.NodeNum
        LEFT JOIN ActualProjectPings p ON h.NodeNum = p.NodeNum AND a.Start_Date = p.Start_Date
    """
    try:
        # Fetch initial dataset from BigQuery 
        df = client.query(query, job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)]
        )).to_dataframe()

        if df.empty:
            st.warning("No sensor profiles found matching this project sequence.")
            return

        # Map metric evaluation classifications
        df[['Peer Trend', 'Performance']] = df.apply(calculate_custom_metrics, axis=1)
        now_local = pd.Timestamp.now(tz=display_tz)
        
        # Chronological sorting layer processing
        def age_processor(x):
            if pd.isnull(x):
                return float('inf')
            return (now_local - pd.to_datetime(x).tz_convert(display_tz)).total_seconds() / 3600.0

        df['hours_hidden'] = df['last_ping'].apply(age_processor)
        df['hours_hidden'] = pd.to_numeric(df['hours_hidden'], errors='coerce').fillna(float('inf'))

        def generate_status_text(hours):
            if hours == float('inf'):
                return "❌ Never"
            elif hours < 1.0:
                mins = int(hours * 60)
                return f"🟢 {mins}m ago" if mins > 0 else "🟢 Just now"
            elif hours <= 1.1:
                return f"🟢 {hours:.1f}h ago"
            else:
                return f"🔴 {hours:.1f}h ago"

        df['Status'] = df['hours_hidden'].apply(generate_status_text)

        # Vectorized calculation for the lifetime project telemetry tracking efficiency metric
        exp_h = pd.to_numeric(df['Expected_Hours'], errors='coerce').fillna(0)
        act_p = pd.to_numeric(df['Actual_Pings_Logged'], errors='coerce').fillna(0)
        df['raw_eff'] = np.where(exp_h <= 0, 0.0, np.minimum(100.0, np.round((act_p / exp_h) * 100, 1)))
        df['Reporting Efficiency'] = [f"{x:.1f}%" for x in df['raw_eff']]

        # =============================================================================
        # 📋 LIVE LOCATION FILTER DROPDOWN
        # =============================================================================
        st.subheader("📋 Segment Allocation View")
        available_locations = sorted(df['Location'].dropna().unique().tolist())
        selected_view_location = st.selectbox("Filter Data Table by Location", ["All Locations"] + available_locations, key="sensor_status_loc_filter")
        
        filtered_df = df.copy()
        if selected_view_location != "All Locations":
            filtered_df = filtered_df[filtered_df['Location'] == selected_view_location]

        # =============================================================================
        # 🔃 DYNAMIC CORES SORTING MANIFEST (MATCHING NODE MANAGER ENGINE)
        # =============================================================================
        st.markdown("##### 🔃 Adjust Grid Sequence Ordering")
        sort_col1, sort_col2 = st.columns(2)
        
        sort_metric = sort_col1.selectbox(
            "Primary Sort Category", 
            ["Hours Since Last Seen (Default)", "Node ID", "Location Area", "Uptime Efficiency Yield"], 
            key="sensor_status_primary_sort_metric"
        )
        
        sort_order = sort_col2.selectbox(
            "Sequence Direction", 
            ["Ascending / Smallest First", "Descending / Largest First"], 
            key="sensor_status_sort_direction"
        )
        
        is_asc = (sort_order == "Ascending / Smallest First")

        # Execute dataframe sorting sequences dynamically before canvas assembly
        if not filtered_df.empty:
            if "Hours Since Last Seen" in sort_metric:
                filtered_df = filtered_df.sort_values(by="hours_hidden", ascending=is_asc).reset_index(drop=True)
            elif "Node ID" in sort_metric:
                filtered_df['sort_key'] = filtered_df['NodeNum'].apply(natural_sort_key)
                filtered_df = filtered_df.sort_values(by="sort_key", ascending=is_asc).drop(columns=['sort_key']).reset_index(drop=True)
            elif "Location Area" in sort_metric:
                filtered_df = filtered_df.sort_values(by=["Location", "hours_hidden"], ascending=[is_asc, True]).reset_index(drop=True)
            elif "Uptime Efficiency Yield" in sort_metric:
                filtered_df = filtered_df.sort_values(by="raw_eff", ascending=is_asc).reset_index(drop=True)

        st.subheader("🔍 Detailed Sensor Audit")
        
        # Prepare presentation dataframe blueprint
        display_df = filtered_df[["Location", "NodeNum", "Peer Trend", "Performance", "Status", "Reporting Efficiency", "coverage_24h", "hours_hidden"]].copy()
        display_df.insert(0, "Select", False)

        # -----------------------------------------------------------
        # ST.FRAGMENT INNER FUNCTION: CALL ISOLATION CONTAINER
        # -----------------------------------------------------------
        @st.fragment
        def render_interactive_audit_grid(data_source_df):
            """Isolates the interactive data editor state from resetting page loops."""
            
            def sensor_status_styler(data):
                canvas = pd.DataFrame('', index=data.index, columns=data.columns)
                for i in data.index:
                    try:
                        val = data.loc[i, 'hours_hidden']
                        hours_val = None if (val == float('inf') or pd.isnull(val)) else float(val)
                        color_style = assign_row_color(hours_val)
                    except Exception:
                        color_style = "background-color: transparent;"
                    
                    for col in data.columns:
                        if col != "Select":
                            canvas.loc[i, col] = color_style
                return canvas

            # FIXED: Canvas now safely maps locally defined styler function signature
            styled_audit_df = data_source_df.style.apply(sensor_status_styler, axis=None)

            edited_df = st.data_editor(
                styled_audit_df,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Select": st.column_config.CheckboxColumn("Select", default=False, required=True),
                    "NodeNum": "Node ID",
                    "Reporting Efficiency": st.column_config.TextColumn("Reporting Efficiency", help="Lifetime percentage of expected hourly pings received while on project"),
                    "coverage_24h": st.column_config.ProgressColumn("24h Coverage", format="%.1f%%", min_value=0, max_value=100)
                },
                disabled=[col for col in data_source_df.columns if col != "Select"],
                column_order=["Select", "Location", "NodeNum", "Peer Trend", "Performance", "Status", "Reporting Efficiency", "coverage_24h"],
                key="sensor_status_editor"
            )

            selected_rows = edited_df[edited_df["Select"] == True]
            if not selected_rows.empty:
                st.divider()
                target_node = selected_rows.iloc[0]["NodeNum"]
                render_sensor_status_charts(client, target_node, selected_project)
            else:
                st.info("💡 **Tip:** Use the checkbox in the audit table above to instantly pull up a comparative analysis graph for any sensor.")

        # Run our newly isolated fragment render cycle passing the pre-fetched data
        render_interactive_audit_grid(display_df)

    except Exception as e:
        st.error(f"Sensor Status Error: {e}")
# ===============================================================
# PAGE: BULK REGISTRY MANAGER
# ===============================================================
def render_active_node_registry_page(client, target_registry=None, **kwargs):
    """
    Renders the master Active Node Registry inventory data grid, calculating
    real-time 'Last Seen' telemetry latencies, distinct hardware fleet breakdowns,
    interactive multi-column sorting, and project reporting efficiencies.
    """
    # SAFETY LAYER: Safely capture the table path regardless of parameter naming mismatches
    table_path = target_registry if target_registry is not None else kwargs.get('target_table')
    if table_path is None:
        st.error("❌ Critical Application Error: No active database target table identifier was provided.")
        return

    st.header("🎯 Active Node Registry")
    
    # 1. READ CONFIGURATION FILTER PARAMETERS
    hide_archived = st.checkbox("Hide Archived Records", value=True, key="registry_hide_archived_toggle")
    
    # -----------------------------------------------------------------
    # ENHANCED SINGLE-PASS TELEMETRY & EFFICIENCY PIPELINE
    # -----------------------------------------------------------------
    # -----------------------------------------------------------------
    # ENHANCED SINGLE-PASS TELEMETRY & EFFICIENCY PIPELINE
    # -----------------------------------------------------------------
    master_query = f"""
        WITH LatestTelemetry AS (
            SELECT 
                NodeNum, 
                MAX(timestamp) as last_ping
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view`
            GROUP BY NodeNum
        ),
        
        AssignmentWindows AS (
            SELECT 
                NodeNum,
                Start_Date,
                COALESCE(End_Date, CURRENT_DATE()) AS Effective_End,
                DATE_DIFF(COALESCE(End_Date, CURRENT_DATE()), Start_Date, DAY) * 24 AS Expected_Hours
            FROM `{table_path}`
            WHERE Project != 'Dead'
        ),
        
        ActualProjectPings AS (
            SELECT 
                m.NodeNum,
                a.Start_Date,
                COUNT(m.timestamp) AS Actual_Pings_Logged
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
            INNER JOIN AssignmentWindows a 
              ON m.NodeNum = a.NodeNum 
              AND EXTRACT(DATE FROM m.timestamp) BETWEEN a.Start_Date AND a.Effective_End
            GROUP BY m.NodeNum, a.Start_Date
        )
        
        SELECT 
            R.*,
            T.last_ping,
            A.Expected_Hours,
            COALESCE(P.Actual_Pings_Logged, 0) AS Actual_Pings_Logged
        FROM `{table_path}` R
        LEFT JOIN LatestTelemetry T ON R.NodeNum = T.NodeNum
        LEFT JOIN AssignmentWindows A 
          ON R.NodeNum = A.NodeNum AND R.Start_Date = A.Start_Date
        LEFT JOIN ActualProjectPings P 
          ON R.NodeNum = P.NodeNum AND R.Start_Date = P.Start_Date
    """
    
    try:
        with st.spinner("Assembling structural timeline registry and telemetry profiles..."):
            reg_df = client.query(master_query).to_dataframe()
            
        if reg_df.empty:
            st.info("The node registry directory is currently empty.")
            return
            
        # =============================================================================
        # ASSET FLEET SUMMARY METRICS PIPELINE (EXACT 4-ROW SPECIFICATION)
        # =============================================================================
        st.markdown("### 📡 Hardware Inventory Fleet Breakdown")
        
        def classify_hardware_family(node):
            node_str = str(node).lower()
            if "-ch" in node_str:
                return "Lord"
            elif node_str.startswith("sp"):
                return "SP"
            elif node_str.startswith("tp"):
                return "TP"
            else:
                return "None of the Above"

        summary_df = reg_df.copy()
        
        # Deduplicate Lord Channels to count distinct physical box units instead of split channels
        summary_df['Parent ID'] = summary_df['NodeNum'].apply(
            lambda x: re.split(r'(?i)-ch', str(x))[0] if "-ch" in str(x).lower() else x
        )
        
        # Chronological Sorting Layer to prioritize active rows over history
        if 'End_Date' in summary_df.columns:
            summary_df['is_active'] = summary_df['End_Date'].isna()
        else:
            summary_df['is_active'] = True
            
        sort_keys = ['Parent ID', 'is_active']
        sort_asc = [True, False]
        if 'Start_Date' in summary_df.columns:
            sort_keys.append('Start_Date')
            sort_asc.append(False)
            
        summary_df = summary_df.sort_values(by=sort_keys, ascending=sort_asc)
        deduped_df = summary_df.drop_duplicates(subset=['Parent ID']).copy()
        deduped_df['Hardware Family'] = deduped_df['Parent ID'].apply(classify_hardware_family)
        
        try:
            fleet_pivot = deduped_df.groupby(['Hardware Family', 'SensorStatus']).size().unstack(fill_value=0)
            desired_order = ["TP", "SP", "Lord", "None of the Above"]
            fleet_pivot = fleet_pivot.reindex(desired_order, fill_value=0)
            fleet_pivot['Total Units'] = fleet_pivot.sum(axis=1)
            
            st.dataframe(fleet_pivot, use_container_width=True)
        except Exception as pivot_err:
            st.info("💡 Inventory matrix is populating. Assign statuses to your hardware to generate totals.")
            
        st.markdown("---")
            
        # 2. RUN REAL-TIME DURATION LAG CALCULATION & EFFICIENCY VECTOR PROCESSING
        now_utc = pd.Timestamp.now(tz='UTC')
        
        # Step A: Latency processing values
        reg_df['hours_hidden'] = reg_df['last_ping'].apply(
            lambda x: (now_utc - pd.to_datetime(x).tz_convert('UTC')).total_seconds() / 3600.0
            if pd.notnull(x) else np.nan
        )
        reg_df['hours_hidden'] = pd.to_numeric(reg_df['hours_hidden'], errors='coerce').fillna(float('inf'))
        
        def format_last_seen(hours):
            if pd.isna(hours) or hours == float('inf'):
                return "❌ Never"
            elif hours < 1.0:
                mins = int(hours * 60)
                return f"{mins}m ago" if mins > 0 else "Just now"
            else:
                return f"{hours:.1f}h ago"
        
        reg_df['Last Seen'] = reg_df['hours_hidden'].apply(format_last_seen)
        
        # Step B: Telemetry yield processing values
        if 'Expected_Hours' in reg_df.columns:
            exp_hours = pd.to_numeric(reg_df['Expected_Hours'], errors='coerce').fillna(0)
            act_pings = pd.to_numeric(reg_df['Actual_Pings_Logged'], errors='coerce').fillna(0)
            
            raw_eff = np.where(
                exp_hours <= 0, 
                0.0, 
                np.minimum(100.0, np.round((act_pings / exp_hours) * 100, 1))
            )
            reg_df['Reporting Efficiency'] = [f"{x:.1f}%" for x in raw_eff]
        else:
            reg_df['Reporting Efficiency'] = "0.0%"
        
        # 3. SCRUB PHYSICAL ID AND INTERNAL COLUMNS FROM THE INTERFACE
        cols_to_drop = ['physicalID', 'PhysicalID', 'last_ping', 'Expected_Hours', 'Actual_Pings_Logged']
        reg_df = reg_df.drop(columns=[c for c in cols_to_drop if c in reg_df.columns], errors='ignore')
        
        # 4. APPLY ON-SCREEN FILTERS (e.g., Hide Archived Records)
        if hide_archived and 'SensorStatus' in reg_df.columns:
            reg_df = reg_df[reg_df['SensorStatus'] != 'Archived']
            
        # =============================================================================
        # INTERACTIVE DATAFRAME SORTING HUB
        # =============================================================================
        st.markdown("##### 🔃 Adjust Registry View Sequence")
        sort_col1, sort_col2 = st.columns(2)
        
        sort_metric = sort_col1.selectbox(
            "Primary Sort Category", 
            ["Hours Since Last Seen (Default)", "Node ID", "Project Space", "Location Location"], 
            key="registry_primary_sort_metric"
        )
        
        sort_order = sort_col2.selectbox(
            "Sequence Direction", 
            ["Ascending / Active First", "Descending / Missing First"], 
            key="registry_sort_direction"
        )
        
        is_asc = (sort_order == "Ascending / Active First")

        # Execute dataframe sorting permutations dynamically before table rendering
        if not reg_df.empty:
            if "Hours Since Last Seen" in sort_metric:
                reg_df = reg_df.sort_values(by="hours_hidden", ascending=is_asc).reset_index(drop=True)
            elif "Node ID" in sort_metric:
                reg_df['sort_key'] = reg_df['NodeNum'].apply(natural_sort_key)
                reg_df = reg_df.sort_values(by="sort_key", ascending=is_asc).drop(columns=['sort_key']).reset_index(drop=True)
            elif "Project Space" in sort_metric:
                reg_df = reg_df.sort_values(by=["Project", "hours_hidden"], ascending=[is_asc, True]).reset_index(drop=True)
            elif "Location Location" in sort_metric:
                reg_df = reg_df.sort_values(by=["Location", "hours_hidden"], ascending=[is_asc, True]).reset_index(drop=True)

        # 5. RENDER THE INTERACTIVE SELECTION GRID (Matching your layout)
        st.markdown("### 📋 Current Asset Allocation Matrix")
        
        display_df = reg_df.copy()
        
        # Persistent selection synchronization management block
        if "last_selected_node" not in st.session_state:
            st.session_state["last_selected_node"] = None
        if "active_selected_node_record" not in st.session_state:
            st.session_state["active_selected_node_record"] = None

        ed_key = "master_node_registry_interactive_grid"
        if ed_key in st.session_state and "edited_rows" in st.session_state[ed_key]:
            changed_rows = st.session_state[ed_key]["edited_rows"]
            newly_checked = [idx for idx, changes in changed_rows.items() if changes.get("Select") == True]
            
            if newly_checked and not display_df.empty:
                latest_idx = newly_checked[-1]
                if latest_idx != st.session_state["last_selected_node"]:
                    st.session_state["last_selected_node"] = latest_idx
                    
                    rec_dict = display_df.loc[latest_idx].drop(["hours_hidden"], errors='ignore').to_dict()
                    st.session_state["active_selected_node_record"] = rec_dict
                    st.session_state[ed_key]["edited_rows"] = {}
                    st.rerun()
            
            elif any(changes.get("Select") == False for idx, changes in changed_rows.items()):
                st.session_state["last_selected_node"] = None
                st.session_state["active_selected_node_record"] = None
                st.session_state[ed_key]["edited_rows"] = {}
                st.rerun()

        display_df.insert(0, "Select", False)
        if st.session_state["last_selected_node"] is not None and st.session_state["last_selected_node"] < len(display_df):
            display_df.loc[st.session_state["last_selected_node"], "Select"] = True

        edited_registry_df = st.data_editor(
            display_df.style.apply(node_selector_styler, axis=None) if not display_df.empty else display_df,
            hide_index=True,
            use_container_width=True,
            column_config={
                "Select": st.column_config.CheckboxColumn("Select", default=False, required=True),
                "NodeNum": "Node ID",
                "Last Seen": st.column_config.TextColumn("Last Seen", help="Hours since last server telemetry ping"),
                "Reporting Efficiency": st.column_config.TextColumn("Reporting Efficiency", help="Telemetry reporting yield percentage")
            },
            disabled=[col for col in display_df.columns if col != "Select"],
            column_order=[c for c in display_df.columns if c != "hours_hidden"],
            key=ed_key
        )
        
        # Route checked rows into the detailed editor panel via persistent session memory
        if st.session_state["active_selected_node_record"] is not None:
            st.divider()
            target_node_record = st.session_state["active_selected_node_record"].copy()
            proj_list = sorted(reg_df['Project'].dropna().unique().tolist())
            
            render_node_action_manager(client, target_node_record, reg_df, proj_list, table_path)
            
    except Exception as e:
        st.error(f"Failed to compile master node registry view grid: {e}")

def render_playground_staging_tab(client, target_registry, table_playground):
    """Provides a safe space to view staging configurations and push to production."""
    st.subheader("🎮 Playground Pre-Update Staging Workspace")
    st.markdown(
        """
        Review your pre-update configurations below. Clicking the button matches and syncs your 
        **Playground** asset states straight into the active production registry.
        """
    )
    
    # Pull staging data for instant on-screen audit
    try:
        play_df = client.query(f"SELECT * FROM `{table_playground}` ORDER BY NodeNum ASC, Start_Date DESC").to_dataframe()
        if not play_df.empty:
            st.caption("📋 Current Staging Inventory (Playground View)")
            st.dataframe(play_df, use_container_width=True, hide_index=True)
            st.metric("Total Staged Records Pending Push", f"{len(play_df)}")
        else:
            st.info("The playground staging database table is currently empty.")
            play_df = pd.DataFrame()
    except Exception as e:
        st.error(f"Failed to scan playground staging records: {e}")
        play_df = pd.DataFrame()

    st.divider()
    
    if not play_df.empty:
        st.warning("⚠️ Action Check: This will override live production node assignments with the metrics staged in your playground table.")
        if st.checkbox("I verify that these staging configurations match my field criteria.", key="confirm_playground_push"):
            if st.button("🚀 Push Playground Data Live to Production", type="primary", use_container_width=True):
                
                # HARDENED COMPOSITE KEY MERGE: Incorporates Project to isolate multi-project concurrent assignments
                sync_sql = f"""
                    MERGE `{target_registry}` T
                    USING (
                        SELECT * FROM (
                            SELECT *,
                                   ROW_NUMBER() OVER(
                                       PARTITION BY NodeNum, Start_Date, Project 
                                       ORDER BY End_Date DESC NULLS FIRST, SensorStatus DESC
                                   ) as rn
                            FROM `{table_playground}`
                        )
                        WHERE rn = 1
                    ) S
                    ON T.NodeNum = S.NodeNum 
                       AND T.Start_Date = S.Start_Date
                       AND T.Project = S.Project
                    
                    -- Update production fields if matching record exists
                    WHEN MATCHED THEN
                        UPDATE SET 
                            T.Location = S.Location,
                            T.Bank = S.Bank,
                            T.Depth = S.Depth,
                            T.SensorStatus = S.SensorStatus,
                            T.End_Date = S.End_Date
                            
                    -- Insert record if it doesn't exist in production yet
                    WHEN NOT MATCHED THEN
                        INSERT (NodeNum, Project, Location, Bank, Depth, SensorStatus, Start_Date, End_Date)
                        VALUES (S.NodeNum, S.Project, S.Location, S.Bank, S.Depth, S.SensorStatus, S.Start_Date, S.End_Date);
                """
                
                try:
                    with st.spinner("Processing production synchronization merge transaction..."):
                        job = client.query(sync_sql)
                        job.result()
                        
                    st.success(f"✅ Sync Successful! Processed and updated {job.num_dml_affected_rows:,} records inside production registry.")
                    st.cache_data.clear()  # Drop active cache paradigms so page datasets refresh instantly
                    time.sleep(1.5)
                    st.rerun()
                except Exception as e:
                    st.error(f"Staging pipeline merge execution failure: {e}")
                    st.code(sync_sql, language="sql")

def force_overwrite_production_with_playground(client):
    """
    Completely erases the live production node registry table and
    replaces it with a perfect copy of the dummy playground state.
    """
    prod_table = f"{PROJECT_ID}.{DATASET_ID}.node_registry"
    dummy_table = f"{PROJECT_ID}.{DATASET_ID}.node_registry_dummy"
    
    # Configure the load job to overwrite the destination table directly
    from google.cloud import bigquery
    job_config = bigquery.QueryJobConfig(
        write_disposition="WRITE_TRUNCATE"
    )
    
    sql = f"SELECT * FROM `{dummy_table}`"
    
    try:
        with st.spinner("💥 Wiping production and copying playground matrices..."):
            # Direct the query results to overwrite production
            query_job = client.query(sql, job_config=job_config)
            # Set the destination table explicitly
            query_job._properties['configuration']['query']['destinationTable'] = {
                'projectId': PROJECT_ID,
                'datasetId': DATASET_ID,
                'tableId': 'node_registry'
            }
            query_job.result()
            
        st.success("🔥 Production registry completely reset and replaced with dummy staging copy!")
        st.cache_data.clear()
        time.sleep(1.5)
        st.rerun()
    except Exception as e:
        st.error(f"Failed to force clear and replace table profiles: {e}")

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

        -- 2. Insert the hardware back into Office
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
            
        st.success(f"Project {project_id} decommissioned. Hardware moved to Office as '{return_status}'.")
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
    Main entry point for Project Lifecycle Management workspace dashboards.
    """
    st.header("⚙️ Project Lifecycle Management")
    
    # Navigation mapping matrix choices
    action = st.radio("Action", ["📋 Project List", "🏗️ New Project", "🔧 Edit Project Metadata"], horizontal=True)
    table_projects = f"{PROJECT_ID}.{DATASET_ID}.project_registry"

    if action == "📋 Project List":
        render_project_overview(client, table_projects)

    elif action == "🏗️ New Project":
        render_new_project_form(client, table_projects)

    elif action == "🔧 Edit Project Metadata":
        render_update_project_form(client, selected_project, table_projects)


def local_datetime_converter(series):
    """Helper formatting string parser utility converting datetimes to clean dates."""
    return pd.to_datetime(series, errors='coerce').dt.date


def render_project_overview(client, table_projects):
    """
    Displays all core schema fields across all registered system environments.
    """
    st.subheader("📋 Complete Project Registry Table")
    
    query = f"SELECT * FROM `{table_projects}` ORDER BY Project ASC"
    try:
        with st.spinner("Extracting structural project lists..."):
            df = client.query(query).to_dataframe()
            
        if not df.empty:
            # FIXED: Calling the corrected local utility function cleanly instead of patching the pd module namespace
            for col in ['Date_Freezedown', 'Date_Completion']:
                if col in df.columns:
                    df[col] = local_datetime_converter(df[col])
            
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.caption(f"Total Combined Tracking Configurations in Registry: {len(df)}")
        else:
            st.info("The central project tracking configuration registry is currently empty.")
    except Exception as e:
        st.error(f"Failed to extract historical project configuration records: {e}")


def render_new_project_form(client, table_projects):
    """
    UI for registering a brand new job environment, supporting optional 
    metadata replication from an existing project template for phased builds.
    """
    st.subheader("🏗️ Initialize New Project Profile")
    
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
            
            # FIXED: Removed Date_Completion from the UPDATE string to prevent the 400 error.
            # If you add this column to BigQuery later, you can add "Date_Completion = {comp_val}" back here.
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
                    Date_Freezedown = {freeze_val}
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
    """
    Main workspace manager tab for tracking, graphing, and importing 
    engineered baseline calibration reference curves.
    """
    st.subheader("📈 Reference Curve Library Matrix")
    
    # Updated to point exactly to your table name
    table_curves = f"{PROJECT_ID}.{DATASET_ID}.reference_curves"
    inventory_df = pd.DataFrame()
    
    # Extract live curve records with built-in 404 missing table safety
    try:
        inventory_df = client.query(f"SELECT * FROM `{table_curves}` ORDER BY 1 ASC").to_dataframe()
    except NotFound:
        # Table doesn't exist yet - intercept error and display a clean setup message
        st.info("ℹ️ Reference curve library initialized. Ready for your first base profile import setup below.")
    except Exception as e:
        st.error(f"Failed to extract live reference curve database logs: {e}")

    # Layout structural workspaces split between view pane and upload form
    tab_view, tab_upload, tab_delete = st.tabs(["📊 View Active Curves", "📥 Upload / Overwrite Curve File", "🗑️ Remove Profile"])

    with tab_view:
        if not inventory_df.empty:
            st.dataframe(inventory_df, use_container_width=True, hide_index=True)
        else:
            st.info("The reference curve asset register is currently unpopulated.")

    with tab_upload:
        render_curve_upload_form(client, table_curves)

    with tab_delete:
        render_curve_management_tools(client, inventory_df, table_curves)


def render_curve_upload_form(client, table_curves):
    """
    Handles parsing and automated overwriting routines for imported 
    CSV/XLSX reference curve datasets. Automatically derives headers from 
    shifted positions and handles unlabelled row index templates safely.
    """
    st.markdown("##### 📥 Import Engineering Calibration Profile")
    st.info("💡 Overwrite rule active: Uploading a file with an identical curve identifier will wipe its old historical data blocks and replace them completely.")

    uploaded_file = st.file_uploader("Choose Curve Dataset File", type=["csv", "xlsx"], key="curve_file_uploader_stream")

    if uploaded_file is not None:
        try:
            # 1. ENCODING SAFE PARSING LAYER
            if uploaded_file.name.endswith('.csv'):
                try:
                    uploaded_file.seek(0)
                    uploaded_df = pd.read_csv(uploaded_file)
                except UnicodeDecodeError:
                    uploaded_file.seek(0)  
                    uploaded_df = pd.read_csv(uploaded_file, encoding='ISO-8859-1')
            else:
                uploaded_file.seek(0)
                uploaded_df = pd.read_excel(uploaded_file)

            if uploaded_df is None or uploaded_df.empty:
                st.error("Uploaded dataset structure contains no parsable rows. Stream pointer empty.")
                return

            # -----------------------------------------------------------------
            # DYNAMIC ROW HEADER PROMOTER (Fixes the Empty Preview)
            # -----------------------------------------------------------------
            # If row 1 column headers are completely unlabelled/blank:
            if all(str(col).startswith("Unnamed:") for col in uploaded_df.columns):
                # Check if row 2 contains the actual column names (like Time (d) or Temperature)
                if not uploaded_df.empty and any(any(x in str(val) for x in ['Time', 'Temp', '°']) for val in uploaded_df.iloc[0].values):
                    # Extract row 2 values to serve as the real headers
                    real_headers = [str(val).strip() for val in uploaded_df.iloc[0].values]
                    uploaded_df.columns = real_headers
                    uploaded_df = uploaded_df.iloc[1:].reset_index(drop=True)
                    st.caption("🧹 Detected and removed empty placeholder row at the top. Promoted text metrics to headers.")

            # Drop any remaining unmapped junk columns safely without wiping the valid data
            unnamed_cols = [col for col in uploaded_df.columns if str(col).startswith("Unnamed:")]
            if unnamed_cols and len(unnamed_cols) < len(uploaded_df.columns):
                uploaded_df = uploaded_df.drop(columns=unnamed_cols)

            # Strip completely empty separator lines out of the frame
            uploaded_df = uploaded_df.dropna(how='all')

            # Force standardized names for BigQuery schema alignment
            rename_dict = {}
            for col in uploaded_df.columns:
                c_upper = str(col).upper()
                if 'TIME' in c_upper or 'DAY' in c_upper:
                    rename_dict[col] = 'Day'
                elif 'TEMP' in c_upper or '°' in c_upper:
                    rename_dict[col] = 'Temp'
            
            if rename_dict:
                uploaded_df = uploaded_df.rename(columns=rename_dict)

            # Final check to guarantee data rows are ready for preview display
            if len(uploaded_df) == 0:
                st.error("❌ File parsing yielded zero records. Check your column headers.")
                return

            st.caption(f"🔍 Previewing Verified Dataset Elements ({len(uploaded_df)} total data rows found):")
            st.dataframe(uploaded_df.head(5), use_container_width=True, hide_index=True)

            # 2. IDENTIFIER RESOLUTION LOGIC (Filename Fallback Extraction)
            possible_id_cols = ['Curve Identifier', 'Curve_Identifier', 'CurveID', 'Curve_ID', 'Curve']
            found_id_col = next((c for c in possible_id_cols if c in uploaded_df.columns), None)

            if found_id_col:
                target_curve_identity = str(uploaded_df[found_id_col].iloc[0]).strip()
                sql_id_match_col = found_id_col
            else:
                target_curve_identity = uploaded_file.name.rsplit('.', 1)[0].strip()
                sql_id_match_col = "CurveID" 
                uploaded_df[sql_id_match_col] = target_curve_identity
                st.info(f"📋 Derived unique tracking reference identifier from filename: **{target_curve_identity}**")

            with st.form("confirm_curve_overwrite_upload_form"):
                st.warning(f"Target Identity Scheduled for Overwrite: **{target_curve_identity}**")
                
                if st.form_submit_button("🚀 Commit & Overwrite Live Target Records"):
                    table_exists = True
                    try:
                        client.get_table(table_curves)
                    except NotFound:
                        table_exists = False

                    if table_exists:
                        purge_sql = f"DELETE FROM `{table_curves}` WHERE {sql_id_match_col} = '{target_curve_identity}'"
                        client.query(purge_sql).result()

                    # Clean data formatting variables to match decimal configurations
                    if 'Day' in uploaded_df.columns and 'Temp' in uploaded_df.columns:
                        uploaded_df['Day'] = pd.to_numeric(uploaded_df['Day'], errors='coerce')
                        uploaded_df['Temp'] = pd.to_numeric(uploaded_df['Temp'], errors='coerce')
                        uploaded_df = uploaded_df.dropna(subset=['Day', 'Temp'])

                    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                    load_job = client.load_table_from_dataframe(uploaded_df, table_curves, job_config=job_config)
                    load_job.result()

                    st.success(f"✅ Overwrite complete! Baseline parameters for **{target_curve_identity}** updated cleanly.")
                    st.cache_data.clear()
                    time.sleep(1.5)
                    st.rerun()

        except Exception as file_parse_err:
            st.error(f"Failed parsing file interface pipelines: {file_parse_err}")
            
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
    """
    Administrative deletion panel toolset for stripping unwanted historical 
    curve reference matrix strings out of the primary dataset registry.
    """
    st.subheader("🗑️ Individual Curve Delete")
    st.markdown("Select a baseline calibration profile curve map from the library to remove it from the system.")

    if inventory_df.empty:
        st.info("No active curves available for configuration modifications.")
        return

    # 1. HARDENED COLUMN DETECTOR MATCH METHOD
    # Scans the dataframe to dynamically find the correct column string name variant
    possible_identifier_cols = ['Curve Identifier', 'Curve_Identifier', 'CurveID', 'Curve_ID', 'Curve']
    target_id_col = None

    for col in possible_identifier_cols:
        if col in inventory_df.columns:
            target_id_col = col
            break

    # Fallback to the very first dataframe column if none of our expected keys match perfectly
    if not target_id_col:
        target_id_col = inventory_df.columns[0]

    # 2. SAFE DROPDOWN EXECUTOR LOGIC
    try:
        # Pull drop selector list values cleanly using our audited key locator string
        curve_dropdown_options = sorted(inventory_df[target_id_col].dropna().unique().tolist())
        
        if not curve_dropdown_options:
            st.warning("No unique curve mapping strings found inside the targeted data layer.")
            return

        to_delete = st.selectbox(
            "Select Curve to Remove", 
            curve_dropdown_options,
            key="individual_curve_deletion_selector"
        )
        
    except Exception as parse_err:
        st.error(f"Failed to assemble selection dropdown bounds list structure: {parse_err}")
        return

    # 3. SECURED DELETION FORM ACTION TRANSACTION
    with st.expander("⚠️ Confirm Deletion Parameters"):
        st.warning(f"This will permanently drop all calibration records linked to: **{to_delete}**")
        confirm_check = st.checkbox("Verify permanent removal of this curve sequence.", key="curve_delete_auth_token_check")
        
        if st.button("Delete Selected Reference Curve", type="primary", use_container_width=True):
            if not confirm_check:
                st.error("Please acknowledge the warning checkbox before executing this database deletion workflow.")
            else:
                # Target the actual BigQuery column string key context dynamically
                delete_sql = f"""
                    DELETE FROM `{table_curves}`
                    WHERE {target_id_col} = '{to_delete}'
                """
                try:
                    with st.spinner("Processing structural catalog deletion index updates..."):
                        client.query(delete_sql).result()
                    st.success(f"🗑️ Reference curve **{to_delete}** has been successfully dropped from the database.")
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()
                except Exception as query_err:
                    st.error(f"Failed to drop target curve profile row instance from server storage: {query_err}")


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
    
    # Core database table path definitions
    target_table = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections" 
    telemetry_table = f"{PROJECT_ID}.{DATASET_ID}.master_data_view" 

    target_scope, new_status = render_management_controls()
    st.divider()

    filters = render_management_filters(reg_df, selected_project, target_scope)
    where_str = build_management_where_clause(reg_df, selected_project, target_scope, filters)
    
    # 1. Step 1: Verification & Current Status Breakdown Matrix
    render_verification_step(client, where_str, telemetry_table, target_table)
    st.divider()
    
    # 2. Step 2: Execution Status Update Transaction
    render_rejection_execution_step(client, where_str, new_status, target_table, telemetry_table)


def render_verification_step(client, where_str, telemetry_table, rejections_table):
    """Queries BigQuery to show matching data metrics grouped by active flag states."""
    if st.button("🔍 Step 1: Verify Match Count & Current Status", key="mgmt_verify_btn"):
        # Resolve potential field ambiguity by aliasing table pointers cleanly
        aliased_where = where_str.replace("NodeNum", "t.NodeNum").replace("timestamp", "t.timestamp").replace("temperature", "t.temperature")
        
        # Pulls metrics and explicitly falls back to 'TRUE' for unflagged base records
        status_q = f"""
            SELECT 
                COALESCE(r.approve, 'TRUE') as Designation,
                COUNT(*) as Point_Count
            FROM `{telemetry_table}` t
            LEFT JOIN `{rejections_table}` r 
                ON t.NodeNum = r.NodeNum AND t.timestamp = r.timestamp
            WHERE {aliased_where}
            GROUP BY Designation
            ORDER BY Point_Count DESC
        """
        
        try:
            with st.spinner("Analyzing active database designation profiles..."):
                res = client.query(status_q).to_dataframe()
            
            if not res.empty:
                st.subheader("📊 Active Designation Profile Summary")
                st.info("The table below displays the exact distribution of how your selected points are currently classified inside the library.")
                
                # Format presentation table for easy reading
                st.dataframe(
                    res.rename(columns={"Designation": "Current Designation Status", "Point_Count": "Total Data Points"}), 
                    use_container_width=True, 
                    hide_index=True
                )
                
                total_points = res['Point_Count'].sum()
                st.metric("Total Consolidated Points in Selection", f"{total_points:,}")
            else:
                st.warning("No telemetry points found matching this configuration window. Check your date filter scopes or threshold parameters.")
                
        except Exception as e:
            st.error(f"Verification Matrix Compilation Failed: {e}")
            st.code(status_q, language="sql")


def render_rejection_execution_step(client, where_str, new_status, target_table, telemetry_table):
    """Executes a collision-free update/insert designation merge mapping directly into rejections."""
    st.info(f"Target Designation Status for selected coordinates: **{new_status}**")
    
    if st.checkbox("I authorize updating these data markers to the target parameters specified.", key="confirm_mgmt"):
        if st.button(f"🚀 Execute Status Override to {new_status}", key="exec_mgmt_btn"):
            aliased_where = where_str.replace("NodeNum", "t.NodeNum").replace("timestamp", "t.timestamp").replace("temperature", "t.temperature")
            
            if new_status == "TRUE":
                # Moving points back to TRUE means dropping their override row so they fall back to default
                sql = f"DELETE FROM `{target_table}` WHERE {where_str}"
            else:
                # HARDENED COLLISION DEFENSE: DISTINCT filters out redundant telemetry timestamps
                # directly inside the source block 'S' to prevent the 400 error.
                sql = f"""
                    MERGE `{target_table}` T
                    USING (
                        SELECT DISTINCT NodeNum, timestamp 
                        FROM `{telemetry_table}` t 
                        WHERE {aliased_where}
                    ) S
                    ON T.NodeNum = S.NodeNum AND T.timestamp = S.timestamp
                    WHEN MATCHED THEN
                        UPDATE SET approve = '{new_status}'
                    WHEN NOT MATCHED THEN
                        INSERT (NodeNum, timestamp, approve) VALUES (S.NodeNum, S.timestamp, '{new_status}')
                """
            try:
                with st.spinner("Processing database merge mapping vectors..."):
                    job = client.query(sql)
                    job.result()
                st.success(f"✅ Reclassification successful! Processed and updated {job.num_dml_affected_rows:,} records inside the rejection catalog.")
                st.cache_data.clear()
                st.balloons()
                time.sleep(1.5)
                st.rerun()
            except Exception as e:
                st.error(f"Execution Error: {e}")
                st.code(sql, language="sql")

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
        # FIXED ROUTE: Runs the interactive sorting and performance audit table view
        render_active_node_registry_page(client, target_registry)
        
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
