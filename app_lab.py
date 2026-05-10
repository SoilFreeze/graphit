import streamlit as st
import pandas as pd
import time
import plotly.express as px
import plotly.graph_objects as go  # This defines 'go'
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta, timezone, time as dt_time
import pytz
import traceback
import io
import re
from streamlit_plotly_events import plotly_events

##################################
# - 1. CONFIGURATION & STYLING - #
##################################
st.set_page_config(page_title="SoilFreeze Data Lab", layout="wide")

# Database Constants
DATASET_ID = "Temperature" 
PROJECT_ID = "sensorpush-export"
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"

# MASTER VISIBILITY SWITCHES
PROJECT_VISIBILITY_MASKS = {
    "Office": "2026-03-03 15:00:00", 
    "Main_Site": "2026-01-01 00:00:00",
    "2527": "2026-01-01 00:00:00"
}

@st.cache_resource
def get_bq_client():
    """
    Initializes and caches the BigQuery connection.
    Prioritizes Service Account info from st.secrets.
    """
    try:
        # 1. Define the required permissions
        SCOPES = [
            "https://www.googleapis.com/auth/bigquery", 
            "https://www.googleapis.com/auth/drive" # Required for Google Sheet backed tables
        ]
        
        # 2. Check for Service Account in Streamlit Secrets
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(
                info, 
                scopes=SCOPES
            )
            # Use the project ID defined in the service account JSON
            return bigquery.Client(credentials=credentials, project=info["project_id"])
        
        # 3. Fallback: Local Authentication (GCloud CLI / Environment Variables)
        # This uses the PROJECT_ID constant defined at the top of your script
        return bigquery.Client(project=PROJECT_ID)

    except Exception as e:
        # We use st.error here because if this fails, the whole app is dead
        st.error(f"❌ BigQuery Authentication Failed: {e}")
        st.info("Check your Streamlit secrets or local gcloud credentials.")
        return None

# Global shortcut to use in non-cached functions
client = get_bq_client()

############################
# - 2. DATA ENGINE LOGIC - #
############################

@st.cache_data(ttl=600)
def get_universal_portal_data(_client, project_id, view_mode="engineering"):
    """
    The underscore in _client is the KEY fix here. 
    It tells Streamlit: 'Don't try to hash the BigQuery connection object.'
    """
    if view_mode == "client":
        filter_sql = "AND UPPER(CAST(approval_status AS STRING)) IN ('TRUE', '1')"
    else:
        filter_sql = "AND UPPER(COALESCE(CAST(approval_status AS STRING), 'PENDING')) NOT IN ('FALSE', '0', 'MASKED')"

    query = f"""
        SELECT * FROM `sensorpush-export.Temperature.master_data_view`
        WHERE Project = '{project_id}'
        {filter_sql}
        ORDER BY Location ASC, timestamp ASC
    """
    
    try:
        return _client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Database Error: {e}")
        return pd.DataFrame()
        
###########################
# - SIDEBAR NAVIGATION -  #
###########################
st.sidebar.title("❄️ SoilFreeze Lab")

# --- SIDEBAR NAVIGATION (Updated Names) ---
page = st.sidebar.selectbox(
    "Navigation", 
    [
        "Summary",             # Previously: Landing Page
        "Time vs Temp",        # Previously: Global Overview
        "Sensor Status",       # Previously: Executive Summary
        "Depth Charts", 
        "Node Diagnostics", 
        "Client Portal", 
        "Data Intake Lab", 
        "Admin Tools"
    ]
)

st.sidebar.divider()

# --- SECTION 2: PROJECT SELECTION ---
selected_project = "All Projects"
project_metadata = None  # To hold info for title blocks

if client is not None:
    try:
        # Fetching names from project_registry
        # Querying ProjectStatus to eventually group by 'Pre-freeze', 'Maintenance', etc.
        proj_q = f"SELECT Project, ProjectName, Timezone, ProjectStatus FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE ProjectStatus != 'Archived'"
        proj_df = client.query(proj_q).to_dataframe()
        proj_list = sorted(proj_df['Project'].dropna().unique())
        
        selected_project = st.sidebar.selectbox(
            "🎯 Active Project", 
            ["All Projects"] + proj_list, 
            key="sidebar_proj_picker_global"
        )
        
        # Store metadata for the selected project to use in titles/headers
        if selected_project != "All Projects":
            project_metadata = proj_df[proj_df['Project'] == selected_project].iloc[0]
            
    except Exception as e:
        st.sidebar.error(f"Registry Link Offline: {e}")

st.sidebar.divider()

# --- SECTION 3: UNIT & MEASUREMENT ---
unit_mode = st.sidebar.radio("Temperature Unit", ["Fahrenheit", "Celsius"], horizontal=True)
unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"

st.sidebar.divider()

# --- SECTION 4: TIME & DISPLAY ---
st.sidebar.subheader("📱 Display & Time")

# If project has a specific timezone in the registry, we can default to it
default_tz_index = 2 # Pacific
if project_metadata is not None and project_metadata['Timezone'] == "US/Eastern":
    default_tz_index = 1

tz_lookup = {
    "UTC": "UTC", 
    "Local (US/Eastern)": "US/Eastern", 
    "Local (US/Pacific)": "US/Pacific"
}

tz_mode = st.sidebar.selectbox(
    "Timezone Display", 
    list(tz_lookup.keys()), 
    index=default_tz_index 
)

display_tz = tz_lookup[tz_mode]
st.session_state["tz_selection"] = tz_mode 

mobile_optimized = st.sidebar.toggle("Mobile Layout", value=False, key="mobile_optimized_toggle")

st.sidebar.divider()

# --- SECTION 5: REFERENCE LINES ---
st.sidebar.subheader("📏 Reference Lines")
active_refs = [] 

if st.sidebar.checkbox("Freezing (32°F)", value=True): 
    active_refs.append((32.0, "Freezing"))
if st.sidebar.checkbox("Type B (26.6°F)", value=False): 
    active_refs.append((26.6, "Type B"))
if st.sidebar.checkbox("Type A (10.2°F)", value=False): 
    active_refs.append((10.2, "Type A"))
# --- END OF SIDEBAR ---

#############
# - Graph - #
#############

def build_high_speed_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label, display_tz="UTC", mobile_mode=False):
    """
    Stabilized Engine: Combines relational status-styling with 
    high-fidelity grid formatting.
    """
    if df.empty:
        return go.Figure().update_layout(title="No data available for the selected period.")

    plot_df = df.copy()
    
    # 1. TIMEZONE & UNIT CONVERSION
    if plot_df['timestamp'].dt.tz is None:
        plot_df['timestamp'] = plot_df['timestamp'].dt.tz_localize('UTC')
    plot_df['timestamp'] = plot_df['timestamp'].dt.tz_convert(display_tz)
    
    start_local = start_view.tz_convert(display_tz) if start_view.tzinfo else start_view.tz_localize('UTC').tz_convert(display_tz)
    end_local = end_view.tz_convert(display_tz) if end_view.tzinfo else end_view.tz_localize('UTC').tz_convert(display_tz)
    now_local = pd.Timestamp.now(tz=display_tz)
    
    # Restored x-axis buffer from working version
    range_start = start_local - pd.Timedelta(days=1)
    range_end = end_local + pd.Timedelta(days=1)
    
    if unit_mode == "Celsius":
        plot_df['temperature'] = (plot_df['temperature'] - 32) * 5/9
        y_range, dt_major, dt_minor = [-30, 30], 10, 5
    else:
        y_range, dt_major, dt_minor = [-20, 80], 10, 5

    # 2. LABELING & SORTING
    def get_sort_info(r):
        if pd.notnull(r.get('Depth')):
            return f"{r['Depth']}ft", float(r['Depth'])
        if pd.notnull(r.get('Bank')) and str(r['Bank']).strip() != "":
            return f"Bank {r['Bank']}", 999.0
        return f"Node {r.get('NodeNum', '??')}", 1000.0

    plot_df[['depth_label', 'sort_val']] = plot_df.apply(lambda x: pd.Series(get_sort_info(x)), axis=1)
    
    # 3. TRACE GENERATION
    fig = go.Figure()
    is_surgical = any(word in title for word in ["Scrubbing", "Surgical", "Diag"])
    unique_groups = plot_df[['depth_label', 'sort_val']].drop_duplicates().sort_values('sort_val')
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']

    for i, (_, g_row) in enumerate(unique_groups.iterrows()):
        group_lbl = g_row['depth_label']
        group_data = plot_df[plot_df['depth_label'] == group_lbl]
        color = colors[i % len(colors)]
        sensors = group_data['NodeNum'].unique()
        
        for j, sn in enumerate(sensors):
            s_df = group_data[group_data['NodeNum'] == sn].sort_values('timestamp')
            
            # --- STATUS-BASED STYLING (RETAINED) ---
            current_status = s_df['SensorStatus'].iloc[0] if 'SensorStatus' in s_df.columns else 'Active'
            line_dash = 'solid' if current_status == 'Active' else 'dot'
            opacity = 1.0 if current_status == 'Active' else 0.6
            
            if not is_surgical:
                s_df['gap_hrs'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
                gap_mask = s_df['gap_hrs'] > 6.0
                if gap_mask.any():
                    gaps = s_df[gap_mask].copy()
                    gaps['temperature'] = None
                    gaps['timestamp'] = gaps['timestamp'] - pd.Timedelta(minutes=1)
                    s_df = pd.concat([s_df, gaps]).sort_values('timestamp')

            fig.add_trace(go.Scatter(
                x=s_df['timestamp'], 
                y=s_df['temperature'], 
                name=f"{group_lbl} ({sn}) - {current_status}", 
                legendgroup=group_lbl,
                showlegend=True if j == 0 else False,
                mode='lines+markers' if not is_surgical else 'markers',
                connectgaps=False, 
                line=dict(color=color, width=1.5, dash=line_dash),
                marker=dict(size=4, opacity=opacity),
                hovertemplate=f"<b>{group_lbl} ({sn})</b><br>Status: {current_status}<br>Temp: %{{y:.1f}}{unit_label}<extra></extra>"
            ))

    # 4. REFERENCE LINES & NOW MARKER
    for val, ref_label in active_refs:
        c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
        fig.add_hline(y=c_val, line_dash="dash", line_color="RoyalBlue", 
                      annotation_text=ref_label, annotation_position="top right")

    fig.add_vline(x=now_local, line_width=2, line_color="Red", layer='above', line_dash="dash")

    # 5. RESTORED GRID HIERARCHY & LAYOUT
    if mobile_mode:
        legend_cfg = dict(orientation="h", yanchor="top", y=-0.25, xanchor="center", x=0.5)
        margin_cfg = dict(t=80, l=40, r=20, b=160)
    else:
        legend_cfg = dict(orientation="v", x=1.02, y=1, xanchor="left", yanchor="top")
        margin_cfg = dict(t=80, l=50, r=180, b=50)

    fig.update_layout(
        title={'text': f"<b>{title}</b>", 'x': 0},
        plot_bgcolor='white', hovermode="x unified", height=600,
        margin=margin_cfg,
        legend=legend_cfg,
        xaxis=dict(
            range=[range_start, range_end], 
            showline=True, mirror=True, linecolor='black',
            showgrid=True, dtick="D1", gridcolor='DarkGray', gridwidth=1,
            minor=dict(dtick=6*60*60*1000, showgrid=True, gridcolor='Gainsboro', griddash='dash'),
            tickformat='%b %d\n%H:%M'
        ),
        yaxis=dict(
            title=f"Temperature ({unit_label})", range=y_range, dtick=dt_major, 
            gridcolor='DarkGray', showline=True, mirror=True, linecolor='black',
            minor=dict(dtick=dt_minor, showgrid=True, gridcolor='whitesmoke')
        )
    )
    
    # 6. RESTORED MONDAY VERTICAL LINES
    mondays = pd.date_range(start=range_start, end=range_end, freq='W-MON', tz=display_tz)
    for mon in mondays:
        fig.add_vline(x=mon, line_width=2, line_color="dimgray", layer="below")

    return fig


##################
# Page Functions #
##################

###########
# - 5. PAGE: GLOBAL OVERVIEW - #
###########

def render_global_overview(selected_project, project_metadata, display_tz):
    """
    Shows all pipes/banks for a selected project in one scrolling view.
    Renamed to 'Time vs Temp' in the UI.
    """
    
    # --- FIX: SAFE METADATA ACCESS ---
    # We check if it's a DataFrame and get the first row, or fall back to empty string
    stage_suffix = ""
    # --- FIX: ROBUST DATAFRAME ACCESS ---
    stage_suffix = ""
    if project_metadata is not None:
        if isinstance(project_metadata, pd.DataFrame) and not project_metadata.empty:
            # Safely grab the first row and first column match
            status = project_metadata['ProjectStatus'].iloc[0]
            stage_suffix = f" [{status}]"
        elif isinstance(project_metadata, dict):
            # Fallback if it's a dictionary
            status = project_metadata.get('ProjectStatus', '')
            stage_suffix = f" [{status}]" if status else ""

    # Updated Header Name
    st.header(f"📈 Time vs Temp {stage_suffix}")
    
    # 1. Sidebar State Management
    mobile_mode = st.session_state.get("mobile_optimized_toggle", False)
    # Ensure active_refs exists in session state (used by the graphing engine)
    active_refs = st.session_state.get("active_refs", [])
    # Global unit settings
    unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
    unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"
    
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to begin.")
        return

    # 2. Data Fetching
    # Inside render_global_overview
    with st.spinner(f"Syncing {selected_project} (Engineering View)..."):
        # Pass 'client' as the first argument; it matches '_client' in the definition
        p_df = get_universal_portal_data(client, selected_project, view_mode="engineering")

    if not p_df.empty:
        # 3. View Constraints
        lookback = st.sidebar.slider("Lookback (Weeks)", 1, 12, 4, key="global_lookback_slider")
        
        # Snap time window
        now_local = pd.Timestamp.now(tz=display_tz)
        
        # End view snaps to the upcoming Monday at midnight for clean weekly reporting
        end_view = (now_local + pd.Timedelta(days=(7-now_local.weekday())%7 or 7)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        start_view = end_view - timedelta(weeks=lookback)

        # 4. Render Graphs by Location
        # Using 'Location' from node_registry to separate the charts
        locations = sorted(p_df['Location'].dropna().unique())
        
        for loc in locations:
            # We use an expander to keep the long page manageable
            with st.expander(f"📍 Location: {loc}", expanded=True):
                loc_df = p_df[p_df['Location'] == loc]
                
                # Call the central graphing engine
                fig = build_high_speed_graph(
                    df=loc_df, 
                    title=f"Project: {selected_project} | Location: {loc}", 
                    start_view=start_view, 
                    end_view=end_view, 
                    active_refs=tuple(active_refs), 
                    unit_mode=unit_mode, 
                    unit_label=unit_label, 
                    display_tz=display_tz,
                    mobile_mode=mobile_mode 
                )
                
                st.plotly_chart(
                    fig, 
                    use_container_width=True, 
                    key=f"tvt_{selected_project}_{loc}" # Updated key prefix for 'Time vs Temp'
                )
    else:
        st.warning(f"No engineering data found for '{selected_project}' in the registry.")
        st.info("Check **Admin Tools > Node Registry** to ensure sensors are mapped to this project and location.")
###########
# - 6. PAGE: EXECUTIVE SUMMARY - #
###########

def render_executive_summary(client, selected_project, unit_label, display_tz):
    # Consolidate header to prevent double-rendering
    st.header(f"🏠 Executive Summary: Health Monitor")
    
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to view health metrics.")
        return

    # FULL COMPREHENSIVE QUERY: Reconstructs all metrics shown in your desired table
    query = f"""
        WITH BaseReporting AS (
            SELECT 
                NodeNum, timestamp, temperature, Location, Bank, Depth, SensorStatus
            FROM `sensorpush-export.Temperature.master_data_view`
            WHERE Project = '{selected_project}'
        ),
        GapAnalysis AS (
            SELECT 
                *,
                LAG(timestamp) OVER (PARTITION BY NodeNum ORDER BY timestamp) AS prev_ts
            FROM BaseReporting
        ),
        HistoricalStats AS (
            SELECT 
                NodeNum, Location, Bank, Depth, SensorStatus,
                MAX(timestamp) AS last_ping,
                ARRAY_AGG(temperature ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS current_temp,
                -- Rolling 24h temperature extremes
                MIN(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature ELSE NULL END) AS low_24h,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature ELSE NULL END) AS high_24h,
                -- Connectivity gaps
                MAX(TIMESTAMP_DIFF(timestamp, prev_ts, HOUR)) AS max_gap_7d,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN TIMESTAMP_DIFF(timestamp, prev_ts, HOUR) ELSE 0 END) AS gap_24h,
                -- Uptime counters
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN 1 ELSE 0 END) as seen_24h,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR) THEN 1 ELSE 0 END) as seen_6h,
                COUNT(DISTINCT CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN TIMESTAMP_TRUNC(timestamp, HOUR) END) as hours_24h,
                COUNT(DISTINCT CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) THEN TIMESTAMP_TRUNC(timestamp, HOUR) END) as hours_7d
            FROM GapAnalysis 
            GROUP BY NodeNum, Location, Bank, Depth, SensorStatus
        )
        SELECT * FROM HistoricalStats
    """
    
    try:
        raw_df = client.query(query).to_dataframe()
        if raw_df.empty:
            st.warning("No data found for this project in the registry.")
            return

        # Numeric Enforcement
        for col in ['Depth', 'low_24h', 'high_24h', 'current_temp']:
            if col in raw_df.columns:
                raw_df[col] = pd.to_numeric(raw_df[col], errors='coerce')

        now_local = pd.Timestamp.now(tz=display_tz)

        def fmt_temp(val):
            if pd.isnull(val): return "N/A"
            c_val = (val - 32) * 5/9 if unit_label == "°C" else val
            return f"{round(c_val, 1)}{unit_label}"

        # --- 1. LOCATION OVERVIEW (TOP TABLE) ---
        summary_df = raw_df.groupby(['Location']).agg(
            Nodes=('NodeNum', 'count'),
            Seen_24h=('seen_24h', 'sum'),
            Sum_Hrs_24=('hours_24h', 'sum'),
            Sum_Hrs_7d=('hours_7d', 'sum'),
            Gap_24h=('gap_24h', 'max'),
            Min_24h_All=('low_24h', 'min'), 
            Max_24h_All=('high_24h', 'max'), 
            Latest_Ping=('last_ping', 'max')
        ).reset_index()

        def format_summary_table(row):
            latest = row['Latest_Ping']
            last_seen_str = "Never"
            if pd.notnull(latest):
                if latest.tzinfo is None: latest = latest.tz_localize('UTC')
                lag_hrs = (now_local - latest.tz_convert(display_tz)).total_seconds() / 3600
                last_seen_str = f"{round(lag_hrs, 1)}h ago"

            # Reliability % Logic
            avg_24h = (row['Sum_Hrs_24'] / (row['Nodes'] * 24)) * 100
            avg_7d = (row['Sum_Hrs_7d'] / (row['Nodes'] * 168)) * 100

            return pd.Series({
                "Location": row['Location'], 
                "Min (24h)": fmt_temp(row['Min_24h_All']), 
                "Max (24h)": fmt_temp(row['Max_24h_All']),
                "Nodes": int(row['Nodes']), 
                "Seen (24h)": int(row['Seen_24h']),
                "% Active (24h)": f"{round(avg_24h, 1)}%", 
                "% Active (7d)": f"{round(avg_7d, 1)}%",
                "Last Seen": last_seen_str, 
                "Max Gap (24h)": f"{int(row['Gap_24h'])}h"
            })

        st.subheader("📍 Location Overview")
        st.dataframe(summary_df.apply(format_summary_table, axis=1), use_container_width=True, hide_index=True)

        # --- 2. SENSOR DRILL-DOWN (BOTTOM TABLE) ---
        st.divider()
        st.subheader("🔍 Sensor Drill-Down")
        loc_list = sorted(raw_df['Location'].unique().tolist())
        selected_loc = st.selectbox(
            "Detailed view for:", 
            ["--- Select Location ---"] + loc_list,
            key=f"summary_drilldown_select_{selected_project}"
        )

        if selected_loc != "--- Select Location ---":
            sensor_df = raw_df[raw_df['Location'] == selected_loc].copy()
            
            def format_sensor_row(row):
                ping = row['last_ping']
                lag = 0.0
                if pd.notnull(ping):
                    if ping.tzinfo is None: ping = ping.tz_localize('UTC')
                    lag = round((now_local - ping.tz_convert(display_tz)).total_seconds() / 3600, 1)

                return pd.Series({
                    "Node ID": row['NodeNum'], 
                    "Bank": row['Bank'] or "N/A",
                    "Depth": f"{row['Depth']}ft" if pd.notnull(row['Depth']) else "nanft",
                    "Current Temp": fmt_temp(row['current_temp']), 
                    "High (24h)": fmt_temp(row['high_24h']),
                    "Low (24h)": fmt_temp(row['low_24h']),
                    "Seen (24h)": "✅" if row['seen_24h'] > 0 else "❌",
                    "Seen (6h)": "✅" if row['seen_6h'] > 0 else "❌",
                    "% Active (24h)": f"{round((row['hours_24h'] / 24) * 100, 1)}%", 
                    "% Active (7d)": f"{round((row['hours_7d'] / 168) * 100, 1)}%",
                    "Gap (24h)": f"{int(row['gap_24h'])}h",
                    "Gap (7d)": f"{int(row['max_gap_7d'])}h",
                    "Status": f"{lag}h {'🟢' if lag < 6 else ('🟡' if lag < 24 else '🔴')}"
                })
            
            st.dataframe(sensor_df.apply(format_sensor_row, axis=1), use_container_width=True, hide_index=True)
            
    except Exception as e:
        st.error(f"Executive Summary Error: {e}")
        
###########
# - 7. PAGE: CLIENT PORTAL - #
###########

def render_client_portal(selected_project, project_metadata, display_tz, unit_mode, unit_label, active_refs):
    """
    Complete Client-facing portal. 
    Matches the professional layout with full Depth Profile logic restored.
    """
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to view client data.")
        return

    # --- 1. DYNAMIC HEADER SECTION (From Project Registry) ---
    display_name = project_metadata.get('ProjectName', selected_project)
    project_status = project_metadata.get('ProjectStatus', 'Active')
    city = project_metadata.get('City', 'Unknown Location')
    tz_info = project_metadata.get('Timezone', 'UTC')
    
    registry_disclaimer = project_metadata.get('ClientDisclaimer') 
    eng_notes = project_metadata.get('EngNotes')
    asbuilt_filename = project_metadata.get('AsBuiltFile')

    # Header Rendering
    st.markdown(f"## 📊 {display_name}")
    st.markdown(
        f"<p style='color: #6d6d6d; font-size: 18px; margin-top: -15px;'>"
        f"Project {selected_project} Status: {project_status}</p>", 
        unsafe_allow_html=True
    )
    st.markdown(f"**Location:** {city} | **Timezone:** {tz_info}")

    # Disclaimer logic
    if pd.notnull(registry_disclaimer) and str(registry_disclaimer).strip() != "":
        st.markdown(f"### **{registry_disclaimer}**")
    else:
        st.markdown("### **Data will be uploaded once per business day by 4pm Pacific Time.**")

    # Engineering Notes logic
    if pd.notnull(eng_notes) and str(eng_notes).strip() != "":
        with st.expander("📝 Engineering & Site Notes", expanded=True):
            st.write(eng_notes)

    st.write("") 

    # --- 2. DATA FETCHING ---
    with st.spinner("Synchronizing approved records..."):
        p_df = get_universal_portal_data(selected_project, view_mode="client")
    
    if p_df.empty:
        st.warning(f"⚠️ No data marked as 'Approved' found for {selected_project}.")
        return

    # --- 3. TABS NAVIGATION ---
    tab_time, tab_depth, tab_table, tab_built = st.tabs([
        "📈 Timeline Analysis", "📏 Depth Profile", "📋 Summary Table", "🗺️ As-Built Plan"
    ])

    # --- TAB 1: TIMELINE ANALYSIS ---
    with tab_time:
        weeks_view = st.sidebar.slider("Weeks to View", 1, 12, 6, key="client_weeks_slider")
        now_utc = pd.Timestamp.now(tz='UTC')
        start_view = now_utc - timedelta(weeks=weeks_view)
        
        locations = sorted(p_df['Location'].dropna().unique())
        for loc in locations:
            with st.expander(f"📍 {loc}", expanded=(len(locations) == 1)):
                loc_data = p_df[p_df['Location'] == loc].copy()
                fig = build_high_speed_graph(
                    df=loc_data, 
                    title=f"{loc} Thermal Trends", 
                    start_view=start_view, 
                    end_view=now_utc, 
                    active_refs=tuple(active_refs), 
                    unit_mode=unit_mode, 
                    unit_label=unit_label, 
                    display_tz=display_tz 
                )
                st.plotly_chart(fig, use_container_width=True, key=f"portal_grid_{loc}")

    # --- TAB 2: DEPTH PROFILE (Restored Detailed Logic) ---
    with tab_depth:
        st.subheader("📏 Vertical Temperature Profile")
        
        # Scaling logic
        x_min_f, x_max_f, ref_f = -20, 60, 32.0
        if unit_label == "°C":
            x_min, x_max, ref_val = (x_min_f-32)*5/9, (x_max_f-32)*5/9, 0.0
        else:
            x_min, x_max, ref_val = x_min_f, x_max_f, ref_f

        p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
        depth_only = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
        
        if depth_only.empty:
            st.info("Vertical profile data is not applicable for this project's sensor configuration.")
        else:
            for loc in sorted(depth_only['Location'].unique()):
                with st.expander(f"📏 {loc} Weekly Snapshots", expanded=False):
                    loc_data = depth_only[depth_only['Location'] == loc].copy()
                    fig_d = go.Figure()
                    
                    # Generate snapshots for the last 6 Mondays
                    mondays = pd.date_range(end=pd.Timestamp.now(tz='UTC'), periods=6, freq='W-MON')
                    for m_date in mondays:
                        target_ts = m_date.replace(hour=6, minute=0, second=0)
                        # 12-hour window to find the closest data point
                        window = loc_data[(loc_data['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & 
                                         (loc_data['timestamp'] <= target_ts + pd.Timedelta(hours=12))]
                        
                        if not window.empty:
                            snap_df = (
                                window.assign(diff=(window['timestamp'] - target_ts).abs())
                                .sort_values(['NodeNum', 'diff'])
                                .drop_duplicates('NodeNum')
                                .sort_values('Depth_Num')
                            )
                            
                            conv_temps = snap_df['temperature'].apply(
                                lambda x: (x - 32) * 5/9 if unit_mode == "Celsius" else x
                            )
                            
                            fig_d.add_trace(go.Scatter(
                                x=conv_temps, 
                                y=snap_df['Depth_Num'], 
                                mode='lines+markers', 
                                name=target_ts.strftime('%m/%d/%y'),
                                line=dict(shape='spline', smoothing=0.5)
                            ))

                    fig_d.add_vline(x=ref_val, line_dash="dash", line_color="RoyalBlue", 
                                    annotation_text="Freezing", annotation_position="top right")

                    y_limit = int(((loc_data['Depth_Num'].max() // 10) + 1) * 10) if not loc_data.empty else 50
                    fig_d.update_layout(
                        plot_bgcolor='white', height=600,
                        xaxis=dict(title=f"Temp ({unit_label})", range=[x_min, x_max], gridcolor='Gainsboro'),
                        yaxis=dict(title="Depth (ft)", range=[y_limit, 0], dtick=10, gridcolor='Silver'),
                        legend=dict(orientation="h", y=-0.2)
                    )
                    st.plotly_chart(fig_d, use_container_width=True, key=f"portal_depth_{loc}")

    # --- TAB 3: SUMMARY TABLE ---
    with tab_table:
        latest = p_df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        latest['Current Temp'] = latest['temperature'].apply(
            lambda x: f"{round((x - 32) * 5/9 if unit_mode == 'Celsius' else x, 1)}{unit_label}"
        )
        
        def get_pos(r):
            if pd.notnull(r.get('Depth')): return f"{r['Depth']} ft"
            if pd.notnull(r.get('Bank')): return f"Bank {r['Bank']}"
            return "Surface"

        latest['Position'] = latest.apply(get_pos, axis=1)
        
        st.dataframe(
            latest[['Location', 'Position', 'Current Temp', 'NodeNum', 'timestamp']].sort_values(['Location', 'Position']), 
            use_container_width=True, hide_index=True,
            column_config={"timestamp": st.column_config.DatetimeColumn("Last Updated", format="MM/DD/YY HH:mm")}
        )

    # --- TAB 4: AS-BUILT PLAN ---
    with tab_built:
        st.subheader("🗺️ Project Layout & Sensor Map")
        if pd.notnull(asbuilt_filename) and str(asbuilt_filename).strip() != "":
            try:
                st.image(f"assets/asbuilts/{asbuilt_filename}", caption=f"Site Map: {display_name}", use_column_width=True)
            except Exception:
                st.error(f"Image '{asbuilt_filename}' not found in assets/asbuilts/ folder.")
        else:
            st.info("The as-built site plan for this project is currently being finalized.")            

###########
# - 8. PAGE: NODE DIAGNOSTICS - #
###########

def render_node_diagnostics(selected_project, display_tz, unit_label):
    st.header(f"📡 Real-Time Commissioning: {selected_project}")
    st.write("Live connectivity audit and data density check for all assigned nodes.")

    # High-Performance Diagnostic Query leveraging the enriched master_data_view
    diag_q = f"""
        WITH Stats AS (
            SELECT 
                NodeNum,
                MAX(timestamp) as last_ping,
                -- Get the absolute latest temperature reading directly from the view
                ARRAY_AGG(temperature ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] as last_temp,
                -- Count check-ins in specific rolling windows for data density
                COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) as count_1h,
                COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)) as count_6h
            FROM `sensorpush-export.Temperature.master_data_view`
            WHERE Project = '{selected_project}'
            GROUP BY NodeNum
        )
        SELECT 
            n.Location, 
            n.NodeNum, 
            n.Bank, 
            n.Depth,
            n.SensorStatus, -- pulling hardware health status
            s.last_ping,
            s.last_temp,
            COALESCE(s.count_1h, 0) as count_1h,
            COALESCE(s.count_6h, 0) as count_6h
        FROM `sensorpush-export.Temperature.node_registry` n
        LEFT JOIN Stats s ON n.NodeNum = s.NodeNum
        WHERE n.Project = '{selected_project}' 
        -- Engineering view shows all assigned sensors regardless of health status
    """
    
    try:
        df = client.query(diag_q).to_dataframe()
        
        if df.empty:
            st.warning("No sensors found for this project in the Node Registry.")
            return

        now = pd.Timestamp.now(tz='UTC')

        # Helper for Latency Category
        def get_latency_info(row):
            ping = row['last_ping']
            if pd.isnull(ping): 
                return "❌ Never", "Never Seen"
            
            if ping.tzinfo is None: ping = ping.tz_localize('UTC')
            diff_mins = (now - ping).total_seconds() / 60
            
            if diff_mins <= 15: cat = "🟢 0-15 Mins"
            elif diff_mins <= 30: cat = "🟡 15-30 Mins"
            elif diff_mins <= 60: cat = "🔴 45-60 Mins"
            else: cat = "⏳ > 1 Hour"
            
            return cat, f"{round(diff_mins/60, 1)}h ago"

        # Apply logic
        df[['Latency_Cat', 'Time_Ago']] = df.apply(lambda x: pd.Series(get_latency_info(x)), axis=1)
        
        # Format Temperatures
        def fmt_temp(val):
            if pd.isnull(val): return "N/A"
            c_val = (val - 32) * 5/9 if unit_label == "°C" else val
            return f"{round(c_val, 1)}{unit_label}"

        # Build final display table
        display_df = pd.DataFrame({
            "Location": df['Location'],
            "Node ID": df['NodeNum'],
            "Health": df['SensorStatus'], # Displaying Diagnostic/Need Repair/Dead/Active
            "Position": df.apply(lambda r: f"{r['Depth']}ft" if pd.notnull(r['Depth']) else f"Bank {r['Bank']}", axis=1),
            "Connectivity": df['Latency_Cat'],
            "Last Seen": df['Time_Ago'],
            "Last Temp": df['last_temp'].apply(fmt_temp),
            "Pings (1h)": df['count_1h'],
            "Pings (6h)": df['count_6h']
        })

        # Sort by Status (Freshness) then Health then Location
        order = ["🟢 0-15 Mins", "🟡 15-30 Mins", "🔴 45-60 Mins", "⏳ > 1 Hour", "Never Seen"]
        display_df['Connectivity'] = pd.Categorical(display_df['Connectivity'], categories=order, ordered=True)
        display_df = display_df.sort_values(['Connectivity', 'Health', 'Location'])

        # Display with conditional formatting
        st.dataframe(
            display_df, 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "Health": st.column_config.TextColumn(help="Current hardware state: Active, Diagnostic, Need Repair, Dead"),
                "Pings (1h)": st.column_config.NumberColumn(help="Target: ~1 for SensorPush, ~60 for Lord"),
                "Pings (6h)": st.column_config.NumberColumn(help="Check for sustained data density"),
            }
        )
        
    except Exception as e:
        st.error(f"Diagnostics Error: {e}")
    
###########
# - 9. PAGE: DATA INTAKE LAB - #
###########

def render_data_intake_page(selected_project):
    st.header("📤 Data Ingestion Lab")
    tab_upload, tab_export = st.tabs(["📄 Upload", "📥 Export"])
    
    with tab_upload:
        st.subheader("📄 Manual File Ingestion")
        st.info("Standardized Rule: Lord IDs use '-' (e.g., 58014-ch1). SensorPush IDs are numeric.")
        
        u_file = st.file_uploader("Upload Data File", type=['csv', 'xlsx'], key="manual_upload_main")
        
        if u_file is not None:
            try:
                # --- 1. DETECTION FOR SENSORCONNECT (WIDE) ---
                is_sensorconnect, skip_rows = False, 0
                if u_file.name.endswith('.csv'):
                    u_file.seek(0)
                    for i, line in enumerate(u_file):
                        if b"DATA_START" in line:
                            is_sensorconnect, skip_rows = True, i + 1
                            break
                    u_file.seek(0)

                # --- 2. INITIAL READ ---
                if is_sensorconnect:
                    st.info("Format Detected: Lord SensorConnect (Wide)")
                    df_raw = pd.read_csv(u_file, encoding='latin1', skiprows=skip_rows, dtype=str)
                elif u_file.name.endswith('.csv'):
                    df_raw = pd.read_csv(u_file, encoding='latin1', dtype=str)
                else:
                    df_raw = pd.read_excel(u_file, dtype=str)

                if not df_raw.empty:
                    df_processed = pd.DataFrame()
                    actual_headers = list(df_raw.columns)
                    clean_headers = [str(h).strip().lower() for h in actual_headers]
                    
                    # --- BRANCH A: SENSORCONNECT (Wide) ---
                    if is_sensorconnect:
                        time_col = [h for h in actual_headers if 'time' in h.lower()][0]
                        value_vars = [h for h in actual_headers if h != time_col]
                        df_melted = df_raw.melt(id_vars=[time_col], value_vars=value_vars, var_name='NodeNum', value_name='temperature')
                        df_processed['timestamp'] = pd.to_datetime(df_melted[time_col], format='mixed')
                        df_processed['NodeNum'] = df_melted['NodeNum'].str.strip().str.replace(':', '-')
                        df_processed['temperature'] = pd.to_numeric(df_melted['temperature'], errors='coerce')

                    # --- BRANCH B: LORD (Long/Narrow) ---
                    elif any(k in clean_headers for k in ['channel', 'node']) and any('time' in h for h in clean_headers):
                        st.info("Format Detected: Lord (Long)")
                        time_h = actual_headers[next(i for i, h in enumerate(clean_headers) if 'time' in h)]
                        node_h = actual_headers[next(i for i, h in enumerate(clean_headers) if 'channel' in h or 'node' in h)]
                        temp_h = [h for h in actual_headers if 'temp' in h.lower()][0]
                        df_processed['timestamp'] = pd.to_datetime(df_raw[time_h], format='mixed')
                        df_processed['NodeNum'] = df_raw[node_h].str.strip().str.replace(':', '-')
                        df_processed['temperature'] = pd.to_numeric(df_raw[temp_h], errors='coerce')

                    # --- BRANCH C: SENSORPUSH ---
                    else:
                        st.info("Format Detected: SensorPush")
                        t_match = [h for h in actual_headers if 'timestamp' in h.lower()][0]
                        v_match = [h for h in actual_headers if 'temp' in h.lower()][0]
                        match = re.search(r'^([^ \(\.]+)', u_file.name)
                        df_processed['timestamp'] = pd.to_datetime(df_raw[t_match], format='mixed')
                        df_processed['temperature'] = pd.to_numeric(df_raw[v_match], errors='coerce')
                        df_processed['NodeNum'] = match.group(1) if match else "Unknown"

                    # --- 3. UPLOAD ---
                    if not df_processed.empty:
                        df_processed = df_processed.dropna(subset=['timestamp', 'temperature'])
                        st.success(f"✅ Ready: IDs: {', '.join(df_processed['NodeNum'].unique())}")
                        
                        # Logic to determine target table based on Node ID format
                        target_table = "raw_lord" if ("-" in str(df_processed['NodeNum'].iloc[0])) else "raw_sensorpush"
                        
                        if st.button("🚀 Push to BigQuery"):
                            with st.spinner("Uploading..."):
                                table_id = f"sensorpush-export.Temperature.{target_table}"
                                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                                client.load_table_from_dataframe(df_processed, table_id, job_config=job_config).result()
                                st.success(f"Uploaded {len(df_processed)} rows to {target_table}!")
                                st.cache_data.clear()

            except Exception as e:
                st.error(f"Ingestion Error: {e}")

    with tab_export:
        st.subheader("📥 Export Project Data")
        if not selected_project or selected_project == "All Projects":
            st.warning("⚠️ Select a project in the sidebar first.")
        else:
            c1, c2 = st.columns(2)
            e_start = c1.date_input("Start Date", value=datetime.now() - timedelta(days=30))
            e_end = c2.date_input("End Date", value=datetime.now())
            
            export_scope = st.radio("Export Scope", ["Whole Project", "Specific Location"], horizontal=True)
            
            with st.spinner("Fetching engineering records..."):
                # Using the relational engine to pull mapped data from master_data_view
                full_df = get_universal_portal_data(selected_project, view_mode="engineering")
            
            if not full_df.empty:
                if export_scope == "Specific Location":
                    target_loc = st.selectbox("Select Location", sorted(full_df['Location'].unique()))
                    full_df = full_df[full_df['Location'] == target_loc]

                if st.button("📦 Generate CSV"):
                    # Filter for dates after removing timezone info for comparison
                    mask = (full_df['timestamp'].dt.date >= e_start) & (full_df['timestamp'].dt.date <= e_end)
                    export_df = full_df.loc[mask].copy()
                    
                    if export_df.empty:
                        st.warning("No data found for this date range.")
                    else:
                        # Convert to string format for standard CSV read
                        export_df['timestamp'] = export_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                        csv = export_df.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="💾 Download CSV",
                            data=csv,
                            file_name=f"{selected_project}_Export.csv",
                            mime="text/csv"
                        )
                        
###########
# - 10. PAGE: ADMIN TOOLS - #
###########
def render_admin_page(selected_project, display_tz, unit_mode, unit_label, active_refs):
    st.header("🛠️ Admin Tools")
    
    # 1. GLOBAL REGISTRY FETCH
    reg_q = f"""
        SELECT 
            n.*, 
            p.ProjectName, p.City, p.Timezone, p.ProjectStatus as MasterProjectStatus
        FROM `sensorpush-export.Temperature.node_registry` n
        LEFT JOIN `sensorpush-export.Temperature.project_registry` p ON n.Project = p.Project
    """
    try:
        full_reg_df = client.query(reg_q).to_dataframe()
        # Clean numeric columns to prevent editor crashes
        for col in ['Depth', 'PhysicalID']:
            if col in full_reg_df.columns:
                full_reg_df[col] = pd.to_numeric(full_reg_df[col], errors='coerce')
    except Exception as e:
        st.error(f"Error joining registries: {e}")
        full_reg_df = pd.DataFrame()
    
    # Context for the selected project
    active_project_df = pd.DataFrame()
    if not full_reg_df.empty:
        active_project_df = full_reg_df[(full_reg_df['Project'] == selected_project) & (full_reg_df['End_Date'].isna())]
    
    loc_options = ["All Locations"] + sorted([str(l) for l in active_project_df['Location'].unique() if pd.notnull(l)]) if not active_project_df.empty else ["All Locations"]

    # --- 2. UNIFIED NAVIGATION ---
    # Ensure "tab_project" is explicitly named in this list
    tab_bulk, tab_registry, tab_project, tab_scrub, tab_surgical, tab_audit = st.tabs([
        "✅ Bulk Approval", 
        "📋 Node Registry", 
        "⚙️ Project Master", 
        "🧹 Scrub", 
        "🧨 Surgical", 
        "🕒 Audit"
    ])

    # --- TAB 1: BULK APPROVAL ---
    with tab_bulk:
        st.subheader("✅ Range-Based Bulk Approval")
        sel_loc = st.selectbox("Target Location", loc_options, key="bulk_loc_main")
        c1, c2 = st.columns(2)
        b_s = c1.date_input("Start", value=datetime.now()-timedelta(7))
        b_e = c2.date_input("End", value=datetime.now())
        
        if st.button("🚀 Execute Bulk Approval", use_container_width=True):
            loc_f = f"AND n.Location = '{sel_loc}'" if sel_loc != "All Locations" else ""
            sql = f"""
                INSERT INTO `{OVERRIDE_TABLE}` (NodeNum, timestamp, approve)
                SELECT DISTINCT r.NodeNum, TIMESTAMP_TRUNC(r.timestamp, HOUR), 'TRUE'
                FROM (SELECT NodeNum, timestamp FROM `sensorpush-export.Temperature.raw_sensorpush` UNION ALL SELECT NodeNum, timestamp FROM `sensorpush-export.Temperature.raw_lord`) AS r
                INNER JOIN `sensorpush-export.Temperature.node_registry` AS n ON r.NodeNum = n.NodeNum
                WHERE n.Project = '{selected_project}' {loc_f} 
                AND r.timestamp BETWEEN n.Start_Date AND COALESCE(n.End_Date, CURRENT_TIMESTAMP())
                AND r.timestamp BETWEEN '{b_s}' AND '{b_e}'
                AND NOT EXISTS (SELECT 1 FROM `{OVERRIDE_TABLE}` x WHERE x.NodeNum = r.NodeNum AND x.timestamp = TIMESTAMP_TRUNC(r.timestamp, HOUR))
            """
            client.query(sql).result()
            st.success(f"Approved records for {sel_loc}.")
            st.cache_data.clear()

    # --- TAB 2: NODE REGISTRY (THE REWRITTEN SECTION) ---
    with tab_registry:
        st.subheader("📋 Hardware Assignment Manager")
        with st.expander("🔍 Filter Hardware View", expanded=False):
            f1, f2 = st.columns(2)
            raw_projs = full_reg_df['Project'].unique().tolist() if not full_reg_df.empty else []
            clean_projs = sorted([str(p) for p in raw_projs if pd.notnull(p)])
            p_filter = f1.selectbox("View Project", ["All"] + clean_projs, key="reg_filter_proj")
            
            raw_stats = full_reg_df['SensorStatus'].unique().tolist() if not full_reg_df.empty else []
            clean_stats = sorted([str(s) for s in raw_stats if pd.notnull(s)])
            s_filter = f2.selectbox("View Health Status", ["All"] + clean_stats, key="reg_filter_status")
            
            view_df = full_reg_df.copy()
            if p_filter != "All": view_df = view_df[view_df['Project'] == p_filter]
            if s_filter != "All": view_df = view_df[view_df['SensorStatus'] == s_filter]

        node_cols = ['NodeNum', 'Project', 'Location', 'Bank', 'Depth', 'Start_Date', 'End_Date', 'SensorStatus']
        edited_df = st.data_editor(
            view_df[node_cols].sort_values(['Project', 'Location']), 
            num_rows="dynamic", key="node_registry_editor_master", use_container_width=True
        )
        
        if st.button("💾 Sync Registry Changes", type="primary", use_container_width=True):
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            client.load_table_from_dataframe(edited_df, f"{PROJECT_ID}.{DATASET_ID}.node_registry", job_config=job_config).result()
            st.success("Node Registry synchronized.")
            st.cache_data.clear()
            st.rerun()

    # --- TAB 3: PROJECT MASTER ---
    with tab_project:
        st.subheader("⚙️ Project Management & Lifecycle")
        
        # 1. Action Toggle
        p_mode = st.radio("Primary Action", ["Project Overview", "Initialize New Project", "Update Project Info"], horizontal=True)
        
        # Fetch current registry for all sub-actions
        proj_reg_df = client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.project_registry`").to_dataframe()
    
        # --- ACTION: PROJECT OVERVIEW ---
        if p_mode == "Project Overview":
            st.markdown("### 📋 Active Project Fleet")
            # Selecting key columns for the high-level view
            overview_cols = ['Project', 'ProjectName', 'ProjectStatus', 'City', 'Date_Initialized', 'Date_Freezedown']
            # Filter for columns that actually exist in your DF
            existing_cols = [c for c in overview_cols if c in proj_reg_df.columns]
            
            st.dataframe(
                proj_reg_df[existing_cols].sort_values('Date_Initialized', ascending=False),
                use_container_width=True,
                hide_index=True
            )

        # --- ACTION: INITIALIZE NEW PROJECT ---
        elif p_mode == "Initialize New Project":
            st.markdown("### 🆕 Register New Project")
            with st.form("init_project_form"):
                c1, c2 = st.columns(2)
                new_id = c1.text_input("Project ID (e.g., 2542-Sample)")
                new_name = c2.text_input("Project Name (e.g., Pump Station 17)")
                new_city = c1.text_input("City")
                new_tz = c2.selectbox("Default Timezone", ["US/Pacific", "US/Eastern", "UTC"])
                
                if st.form_submit_button("🚀 Create Project Entry"):
                    if not new_id or not new_name:
                        st.error("Project ID and Name are required.")
                    else:
                        today = datetime.now().strftime('%Y-%m-%d')
                        # Set status to 'Initialized' and stamp the initiation date
                        sql = f"""
                            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.project_registry` 
                            (Project, ProjectName, City, Timezone, ProjectStatus, Date_Initialized)
                            VALUES ('{new_id}', '{new_name}', '{new_city}', '{new_tz}', 'Initialized', '{today}')
                        """
                        try:
                            client.query(sql).result()
                            st.success(f"Project {new_id} successfully initialized.")
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Initialization Failed: {e}")
    
        # --- ACTION: UPDATE PROJECT INFO (MILESTONE TRACKING) ---
        elif p_mode == "Update Project Info" and not proj_reg_df.empty:
            target_proj = st.selectbox("Select Project to Edit", sorted([str(p) for p in proj_reg_df['Project'].unique()]))
            p_data = proj_reg_df[proj_reg_df['Project'] == target_proj].iloc[0]
    
            with st.form("p_update_form"):
                c1, c2 = st.columns(2)
                u_name = c1.text_input("Project Name", value=p_data.get('ProjectName', ''))
                
                # Status Mapping for Milestone Dates
                status_options = ["Initialized", "Pre-freeze", "Freezedown", "Maintenance", "Post-freeze", "Finished", "Archived"]
                current_status = p_data.get('ProjectStatus', 'Initialized')
                try:
                    status_idx = status_options.index(current_status)
                except ValueError:
                    status_idx = 0
                
                u_status = c2.selectbox("Update Project Stage", status_options, index=status_idx)
                u_city = c1.text_input("City", value=p_data.get('City', ''))
                u_tz = c2.selectbox("Timezone", ["US/Pacific", "US/Eastern", "UTC"], index=0)
                u_disclaimer = st.text_area("Client Portal Disclaimer", value=p_data.get('ClientDisclaimer', ''))
                u_asbuilt = st.text_input("As-Built Filename (e.g. site_map.png)", value=p_data.get('AsBuiltFile', ''))
                u_eng = st.text_area("Engineering Notes (Overwrites previous)", value=p_data.get('EngNotes', ''))
                
                if st.form_submit_button("💾 Save Project Settings"):
                    # Milestone Date Logic
                    date_col_mapping = {
                        "Pre-freeze": "Date_PreFreeze",
                        "Freezedown": "Date_Freezedown",
                        "Maintenance": "Date_Maintenance",
                        "Post-freeze": "Date_PostFreeze",
                        "Archived": "Date_Archived"
                    }
                    
                    target_date_col = date_col_mapping.get(u_status)
                    today_str = datetime.now().strftime('%Y-%m-%d')
                    
                    # Only stamp the date if the column exists and is currently empty
                    date_update_sql = ""
                    if target_date_col and target_date_col in proj_reg_df.columns:
                        if pd.isnull(p_data.get(target_date_col)):
                            date_update_sql = f", {target_date_col}='{today_str}'"
    
                    sql = f"""
                        UPDATE `{PROJECT_ID}.{DATASET_ID}.project_registry` 
                        SET 
                            ProjectName='{u_name}', ProjectStatus='{u_status}', City='{u_city}', 
                            Timezone='{u_tz}', EngNotes='{u_eng}', ClientDisclaimer='{u_disclaimer}',
                            AsBuiltFile='{u_asbuilt}' {date_update_sql}
                        WHERE Project='{target_proj}'
                    """
                    client.query(sql).result()
                    st.success(f"Updated {target_proj}. Stamped {u_status} date if newly reached.")
                    st.cache_data.clear()
                    st.rerun()

    # --- TAB 4: SCRUB ---
    with tab_scrub:
        st.subheader("🧹 Data Averaging")
        target = st.radio("Target Table", ["SensorPush", "Lord"], horizontal=True)
        if st.button("🧨 Execute Hourly Averaging"):
            t_tab = f"{PROJECT_ID}.{DATASET_ID}.raw_{target.lower()}"
            sql = f"CREATE OR REPLACE TABLE `{t_tab}` AS SELECT TIMESTAMP_TRUNC(TIMESTAMP_ADD(timestamp, INTERVAL 30 MINUTE), HOUR) as timestamp, NodeNum, AVG(temperature) as temperature FROM `{t_tab}` WHERE NodeNum IN (SELECT NodeNum FROM `{PROJECT_ID}.{DATASET_ID}.node_registry` WHERE Project = '{selected_project}') GROUP BY 1, 2"
            client.query(sql).result()
            st.success("Scrubbed data successfully.")

    # --- TAB 5: SURGICAL ---
    with tab_surgical:
        render_surgical_cleaner(selected_project, display_tz, unit_mode, unit_label)

    # --- TAB 6: AUDIT ---
    with tab_audit:
        st.subheader("🕒 Registry Audit Log")
        st.dataframe(full_reg_df.sort_values('Start_Date', ascending=False), use_container_width=True, hide_index=True)

###########
# - 11. SURGICAL CLEANER FUNCTIONS - #
###########

def render_surgical_cleaner(selected_project, display_tz, unit_mode, unit_label):
    st.subheader("🧨 Unified Data Management (Mask & Purge)")
    
    # 1. SCOPE & ACTION MODE
    c1, c2 = st.columns(2)
    with c1:
        scope = st.radio("Target Scope", ["Project Wide", "Specific Location", "Specific Node"], horizontal=True)
    with c2:
        action_mode = st.radio("Action Type", ["🚫 Mask (Soft Hide)", "🔥 Purge (Hard Delete)"], horizontal=True)

    # RE-MAPPED: Fetch from node_registry utilizing new schema
    reg_q = f"SELECT NodeNum, Location FROM `sensorpush-export.Temperature.node_registry` WHERE Project = '{selected_project}'"
    reg_df = client.query(reg_q).to_dataframe()
    
    target_node, target_loc = None, None
    if not reg_df.empty:
        if scope == "Specific Location":
            target_loc = st.selectbox("Select Location", sorted(reg_df['Location'].unique()))
        elif scope == "Specific Node":
            target_node = st.selectbox("Select Node ID", sorted(reg_df['NodeNum'].unique()))
    else:
        st.warning("No nodes found in registry for this project.")
        return

    # 2. TEMPORAL LOGIC (Simplified UI)
    st.divider()
    t_col1, t_col2 = st.columns([1, 2])
    direction = t_col1.selectbox("Temporal Direction", ["Between Range", "Everything Older Than", "Everything Newer Than"])
    
    with t_col2:
        if direction == "Between Range":
            sc1, sc2 = st.columns(2)
            s_dt = datetime.combine(sc1.date_input("Start Date", value=datetime.now() - timedelta(days=7)), dt_time(0,0))
            e_dt = datetime.combine(sc2.date_input("End Date", value=datetime.now()), dt_time(23,59))
        else:
            anchor_dt = datetime.combine(st.date_input("Anchor Date"), st.time_input("Anchor Time", value=dt_time(6,0)))
            s_dt = datetime(2000, 1, 1) if direction == "Everything Older Than" else anchor_dt
            e_dt = anchor_dt if direction == "Everything Older Than" else datetime(2100, 1, 1)

    # 3. THRESHOLD Logic
    thr_col1, thr_col2 = st.columns([1, 2])
    operator = thr_col1.selectbox("Value Filter", ["No Threshold", "Greater Than (>)", "Less Than (<)"])
    thresh_val = thr_col2.number_input(f"Threshold Value ({unit_label})", value=100.0)
    thresh_val_f = (thresh_val * 9/5) + 32 if unit_mode == "Celsius" else thresh_val

    # 4. SQL LOGIC CONSTRUCTION (Targeting node_registry 'n' for Tenure Safety)
    if scope == "Project Wide":
        where_clause = f"n.Project = '{selected_project}'"
    elif scope == "Specific Location":
        where_clause = f"n.Project = '{selected_project}' AND n.Location = '{target_loc}'"
    else:
        where_clause = f"n.NodeNum = '{target_node}' AND n.Project = '{selected_project}'"

    threshold_clause = ""
    if operator == "Greater Than (>)": threshold_clause = f"AND r.temperature > {thresh_val_f}"
    elif operator == "Less Than (<)": threshold_clause = f"AND r.temperature < {thresh_val_f}"

    s_str, e_str = s_dt.strftime('%Y-%m-%d %H:%M:%S'), e_dt.strftime('%Y-%m-%d %H:%M:%S')

    # 5. EXECUTION GATE
    st.divider()
    if st.button("🔍 Step 1: Verify Match Count", use_container_width=True):
        # This query ensures we only touch data that matches the project tenure
        status_q = f"""
            SELECT COALESCE(rej.approve, 'PENDING') as status, COUNT(*) as point_count
            FROM (
                SELECT NodeNum, timestamp, temperature FROM `sensorpush-export.Temperature.raw_sensorpush` 
                UNION ALL 
                SELECT NodeNum, timestamp, temperature FROM `sensorpush-export.Temperature.raw_lord`
            ) AS r
            INNER JOIN `sensorpush-export.Temperature.node_registry` AS n ON r.NodeNum = n.NodeNum
            LEFT JOIN `{OVERRIDE_TABLE}` AS rej ON r.NodeNum = rej.NodeNum AND TIMESTAMP_TRUNC(r.timestamp, HOUR) = rej.timestamp
            WHERE {where_clause} 
            AND r.timestamp BETWEEN n.Start_Date AND COALESCE(n.End_Date, CURRENT_TIMESTAMP())
            AND r.timestamp BETWEEN '{s_str}' AND '{e_str}'
            {threshold_clause}
            GROUP BY status
        """
        st.session_state["purge_staged_df"] = client.query(status_q).to_dataframe()

    if "purge_staged_df" in st.session_state:
        staged_df = st.session_state["purge_staged_df"]
        total = staged_df['point_count'].sum() if not staged_df.empty else 0
        
        if total > 0:
            st.warning(f"### ⚠️ Action Staged: {total} Points")
            st.table(staged_df.set_index('status'))
            confirm = st.checkbox(f"Confirm {action_mode} for these records.")
            
            if st.button(f"🚀 Execute {action_mode}", use_container_width=True, disabled=not confirm):
                # Masking logic
                if "Mask" in action_mode:
                    sql = f"""
                        MERGE `{OVERRIDE_TABLE}` T
                        USING (
                            SELECT DISTINCT r.NodeNum, TIMESTAMP_TRUNC(r.timestamp, HOUR) as ts
                            FROM (SELECT NodeNum, timestamp, temperature FROM `sensorpush-export.Temperature.raw_sensorpush` UNION ALL SELECT NodeNum, timestamp, temperature FROM `sensorpush-export.Temperature.raw_lord`) AS r
                            INNER JOIN `sensorpush-export.Temperature.node_registry` AS n ON r.NodeNum = n.NodeNum
                            WHERE {where_clause} 
                            AND r.timestamp BETWEEN n.Start_Date AND COALESCE(n.End_Date, CURRENT_TIMESTAMP())
                            AND r.timestamp BETWEEN '{s_str}' AND '{e_str}'
                            {threshold_clause}
                        ) S ON T.NodeNum = S.NodeNum AND T.timestamp = S.ts
                        WHEN MATCHED THEN UPDATE SET approve = 'MASKED'
                        WHEN NOT MATCHED THEN INSERT (NodeNum, timestamp, approve) VALUES (S.NodeNum, S.ts, 'MASKED')
                    """
                # Hard Delete logic
                else:
                    sql = f"""
                        BEGIN TRANSACTION;
                        DELETE FROM `sensorpush-export.Temperature.raw_sensorpush` r 
                        WHERE EXISTS (
                            SELECT 1 FROM `sensorpush-export.Temperature.node_registry` n 
                            WHERE r.NodeNum = n.NodeNum AND {where_clause} 
                            AND r.timestamp BETWEEN n.Start_Date AND COALESCE(n.End_Date, CURRENT_TIMESTAMP())
                        ) AND r.timestamp BETWEEN '{s_str}' AND '{e_str}' {threshold_clause};
                        
                        DELETE FROM `sensorpush-export.Temperature.raw_lord` r 
                        WHERE EXISTS (
                            SELECT 1 FROM `sensorpush-export.Temperature.node_registry` n 
                            WHERE r.NodeNum = n.NodeNum AND {where_clause} 
                            AND r.timestamp BETWEEN n.Start_Date AND COALESCE(n.End_Date, CURRENT_TIMESTAMP())
                        ) AND r.timestamp BETWEEN '{s_str}' AND '{e_str}' {threshold_clause};
                        COMMIT;
                    """
                client.query(sql).result()
                st.success(f"Successfully processed {total} points.")
                del st.session_state["purge_staged_df"]
                st.cache_data.clear()
                st.rerun()

###########
# - 11. SURGICAL CLEANER HELPERS - #
###########

def update_records(pts, df, val):
    """
    Writes status updates (TRUE, FALSE, MASKED) to the manual_rejections table.
    Aligned with the new sensorpush-export.Temperature schema.
    """
    recs = []
    for p in pts:
        try:
            # 1. Capture the timestamp from the click event
            # Use tz_localize if the graph data is naive, or convert to UTC
            ts_raw = pd.to_datetime(p['x'])
            if ts_raw.tzinfo is None:
                ts = ts_raw.tz_localize(display_tz).tz_convert('UTC').floor('h')
            else:
                ts = ts_raw.tz_convert('UTC').floor('h')
            
            # 2. Grab the NodeNum directly from the dataframe row that was clicked
            node = df.iloc[p['point_index']]['NodeNum']
            
            recs.append({
                "NodeNum": str(node), 
                "timestamp": ts, 
                "approve": val 
            })
        except Exception:
            continue
    
    if recs:
        # 3. Deduplicate to avoid PK violations in the rejection table
        status_df = pd.DataFrame(recs).drop_duplicates(subset=['NodeNum', 'timestamp'])
        
        try:
            # 4. APPEND the new status to the manual_rejections table
            # OVERRIDE_TABLE = "sensorpush-export.Temperature.manual_rejections"
            job = client.load_table_from_dataframe(status_df, OVERRIDE_TABLE)
            job.result() 
            
            # 5. UI Feedback & Cache Clearing
            st.session_state.locked_selection = []
            st.cache_data.clear() # Forces graphs to redraw with the new MASK/HIDE status
            st.success(f"✅ Successfully marked {len(status_df)} records as {val}")
            
            # Brief pause so the user sees the success message before rerun
            time.sleep(0.5) 
            st.rerun()
        except Exception as e:
            st.error(f"Failed to update database: {e}")

###########
# - 13. PAGE: LANDING PAGE - #
###########
def render_landing_page(client, unit_label, unit_mode):
    # Updated Header to "Summary"
    st.header("🌐 Global Project Summary")
    
    # 1. Query: Filtering for Freezedown/Maintenance + 25h Window
    summary_q = f"""
        WITH active_projects AS (
            SELECT Project, ProjectName, ProjectStatus 
            FROM `{PROJECT_ID}.{DATASET_ID}.project_registry`
            WHERE ProjectStatus IN ('Freezedown', 'Maintenance')
        ),
        raw_data AS (
            SELECT 
                n.Project, n.Bank, n.Location, n.Depth, m.temperature, m.timestamp
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
            JOIN `{PROJECT_ID}.{DATASET_ID}.node_registry` n ON m.NodeNum = n.NodeNum
            WHERE m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 25 HOUR)
        )
        SELECT 
            p.Project, p.ProjectName, p.ProjectStatus,
            ld.Bank, ld.Location, ld.Depth,
            AVG(CASE WHEN ld.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN ld.temperature END) as avg_now,
            AVG(CASE WHEN ld.timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN ld.temperature END) as avg_1h,
            AVG(CASE WHEN ld.timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR) THEN ld.temperature END) as avg_6h,
            AVG(CASE WHEN ld.timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 25 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN ld.temperature END) as avg_24h,
            MIN(ld.temperature) as min_24h,
            MAX(ld.temperature) as max_24h
        FROM active_projects p
        LEFT JOIN raw_data ld ON p.Project = ld.Project
        GROUP BY 1, 2, 3, 4, 5, 6
    """
    
    try:
        df = client.query(summary_q).to_dataframe()
    except Exception as e:
        st.error(f"Dashboard Query Failed: {e}")
        return

    if df.empty:
        st.warning("No projects currently in Freezedown or Maintenance with active data.")
        return

    for project in sorted(df['Project'].unique()):
        p_df = df[df['Project'] == project]
        p_name = p_df['ProjectName'].iloc[0]
        p_status = p_df['ProjectStatus'].iloc[0]
        
        with st.container(border=True):
            st.subheader(f"🏗️ {p_name} ({project})")
            
            # 4-Column Layout: Supply, Return, TempPipes, Ambient
            c1, c2, c3, c4 = st.columns(4)
            
            # Grouping Logic
            is_ambient = p_df['Bank'].str.contains('Amb', case=False, na=False) | p_df['Location'].str.contains('Amb', case=False, na=False)
            is_supply = (p_df['Bank'].str.startswith('S', na=False) | p_df['Location'].str.startswith('S', na=False)) & ~is_ambient
            is_return = (p_df['Bank'].str.startswith('R', na=False) | p_df['Location'].str.startswith('R', na=False)) & ~is_ambient
            # TempPipes specifically targets monitoring with depth
            is_temppipe = p_df['Depth'].notnull() & ~is_supply & ~is_return & ~is_ambient

            groups = [
                (c1, "📥 Supply (S)", p_df[is_supply]),
                (c2, "📤 Return (R)", p_df[is_return]),
                (c3, "📏 TempPipes", p_df[is_temppipe]),
                (c4, "☁️ Ambient", p_df[is_ambient])
            ]
            
            for col, title, group_df in groups:
                with col:
                    st.markdown(f"### {title}")
                    if group_df.empty or group_df['avg_now'].isnull().all():
                        st.caption("No data available")
                        continue
                    
                    # Calculations
                    now = group_df['avg_now'].mean()
                    prev_1h = group_df['avg_1h'].mean()
                    prev_6h = group_df['avg_6h'].mean()
                    prev_24h = group_df['avg_24h'].mean()
                    mn, mx = group_df['min_24h'].min(), group_df['max_24h'].max()
                    
                    # Conversion
                    if unit_mode == "Celsius":
                        now, prev_1h, prev_6h, prev_24h, mn, mx = [(x - 32) * 5/9 if pd.notnull(x) else None for x in [now, prev_1h, prev_6h, prev_24h, mn, mx]]
                    
                    # 1. Current Metric
                    st.metric("Current", f"{now:.1f}{unit_label}")
                    
                    # 2. Range Line
                    st.markdown(f"**Range:** {mn:.1f} to {mx:.1f}{unit_label}")
                    
                    # 3. Trend Below
                    st.write("**Trend**")
                    t1, t2, t3 = st.columns(3)
                    t1.caption(f"1h\n{get_trend_arrow(now, prev_1h)}")
                    t2.caption(f"6h\n{get_trend_arrow(now, prev_6h)}")
                    t3.caption(f"24h\n{get_trend_arrow(now, prev_24h)}")


def get_trend_arrow(current, previous):
    if pd.isnull(current) or pd.isnull(previous): return "N/A"
    delta = current - previous
    if delta > 0.1: return f"🔺 +{delta:.1f}"
    if delta < -0.1: return f"🔹 {delta:.1f}"
    return "➡️ 0.0"

###########
# - 12. MAIN ROUTER - #
###########

if page == "Summary":
    render_landing_page(client, unit_label, unit_mode)

elif page == "Time vs Temp":
    render_global_overview(client, unit_label, unit_mode)

elif page == "Sensor Status":
    render_executive_summary(client, unit_label, unit_mode)

elif page == "Depth Charts":
    render_depth_charts(selected_project, unit_label, display_tz)

elif page == "Node Diagnostics":
    render_node_diagnostics(selected_project, display_tz, unit_label)

elif page == "Client Portal":
    render_client_portal(selected_project, project_metadata, display_tz, unit_mode, unit_label, active_refs)

# --- PASSWORD PROTECTED SECTIONS ---
elif page in ["Data Intake Lab", "Admin Tools"]:
    if st.session_state.get('authenticated', False):
        if page == "Data Intake Lab":
            render_data_intake_page(selected_project)
        else:
            render_admin_page(selected_project, display_tz, unit_mode, unit_label, active_refs)
    else:
        st.subheader("🔐 Restricted Access")
        pwd = st.text_input("Enter Authorized Password", type="password")
        if st.button("Unlock Access"):
            if pwd == st.secrets["admin_password"]:
                st.session_state['authenticated'] = True
                st.rerun()
            else:
                st.error("Incorrect password. Please contact the administrator.")
