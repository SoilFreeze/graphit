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

# NOTE: PROJECT_VISIBILITY_MASKS has been removed. 
# Visibility is now handled dynamically via 'Date_Freezedown' in the project_registry.

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
            "https://www.googleapis.com/auth/drive" 
        ]
        
        # 2. Check for Service Account in Streamlit Secrets
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(
                info, 
                scopes=SCOPES
            )
            return bigquery.Client(credentials=credentials, project=info["project_id"])
        
        # 3. Fallback: Local Authentication
        return bigquery.Client(project=PROJECT_ID)

    except Exception as e:
        st.error(f"❌ BigQuery Authentication Failed: {e}")
        return None

############################
# - 2. DATA ENGINE LOGIC - #
############################

@st.cache_data(ttl=600)
def get_universal_portal_data(project_id, view_mode="engineering"):
    client = get_bq_client()
    if client is None:
        return pd.DataFrame()

    # 1. Classification & Visibility Logic
    if view_mode == "client":
        filter_sql = "AND UPPER(CAST(m.approval_status AS STRING)) IN ('TRUE', '1')"
        # Client ONLY sees data from Freezedown onwards
        visibility_sql = "AND m.timestamp >= CAST(p.Date_Freezedown AS TIMESTAMP)"
    else:
        # Engineering sees everything NOT masked
        filter_sql = "AND UPPER(COALESCE(CAST(m.approval_status AS STRING), 'PENDING')) NOT IN ('FALSE', '0', 'MASKED')"
        # Engineering sees ALL data (including pre-freeze baselines)
        visibility_sql = ""

    query = f"""
        SELECT m.* FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
        JOIN `{PROJECT_ID}.{DATASET_ID}.project_registry` p ON m.Project = p.Project
        WHERE m.Project = '{project_id}'
        {visibility_sql}
        {filter_sql}
        ORDER BY m.Location ASC, m.timestamp ASC
    """
    
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"⚠️ Data Sync Error for '{project_id}': {e}")
        return pd.DataFrame()
        
###########################
# - SIDEBAR NAVIGATION -  #
###########################
st.sidebar.title("❄️ SoilFreeze Lab")

# --- SIDEBAR NAVIGATION ---
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
project_metadata = None  

# FIX: Fetch the client locally since the global 'client' is gone
sidebar_client = get_bq_client()

if sidebar_client is not None:
    try:
        # Fetching names from project_registry
        proj_q = f"SELECT Project, ProjectName, Timezone, ProjectStatus FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE ProjectStatus != 'Archived'"
        proj_df = sidebar_client.query(proj_q).to_dataframe()
        proj_list = sorted(proj_df['Project'].dropna().unique())
        
        selected_project = st.sidebar.selectbox(
            "🎯 Active Project", 
            ["All Projects"] + proj_list, 
            key="sidebar_proj_picker_global"
        )
        
        # --- CRITICAL FIX: SYNC TO SESSION STATE ---
        st.session_state['selected_project'] = selected_project
        
        if selected_project != "All Projects":
            # Filter the metadata for the ONE selected project
            meta_df = proj_df[proj_df['Project'] == selected_project]
            st.session_state['project_metadata_df'] = meta_df
            project_metadata = meta_df.iloc[0] # For local use in sidebar
            
    except Exception as e:
        st.sidebar.error(f"Registry Link Offline: {e}")
        
st.sidebar.divider()

# --- SECTION 3: UNIT & MEASUREMENT ---
unit_mode = st.sidebar.radio("Temperature Unit", ["Fahrenheit", "Celsius"], horizontal=True)
unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"
# Sync to session state for the graphing engine
st.session_state["unit_mode"] = unit_mode

st.sidebar.divider()

# --- SECTION 4: TIME & DISPLAY ---
st.sidebar.subheader("📱 Display & Time")

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
st.session_state["display_tz"] = display_tz

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

# Store in session state for the graphing functions
st.session_state["active_refs"] = active_refs
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
    
    # --- DYNAMIC RANGE ADJUSTMENT ---
    # NEW: Ensure the graph start isn't earlier than the first actual data point 
    # (Respecting the Date_Freezedown mask from our SQL)
    actual_min_data = plot_df['timestamp'].min()
    effective_start = max(start_local, actual_min_data)
    
    range_start = min(start_local, actual_min_data) - pd.Timedelta(hours=12) # Reduced buffer for tighter view
    range_end = end_local + pd.Timedelta(days=1)
    
    if unit_mode == "Celsius":
        plot_df['temperature'] = (plot_df['temperature'] - 32) * 5/9
        y_range, dt_major, dt_minor = [-30, 30], 10, 5
    else:
        # Adjusted y_range for ground freezing (colder focus)
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
    # Standard engineering color palette
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']

    for i, (_, g_row) in enumerate(unique_groups.iterrows()):
        group_lbl = g_row['depth_label']
        group_data = plot_df[plot_df['depth_label'] == group_lbl]
        color = colors[i % len(colors)]
        sensors = group_data['NodeNum'].unique()
        
        for j, sn in enumerate(sensors):
            s_df = group_data[group_data['NodeNum'] == sn].sort_values('timestamp')
            
            # --- STATUS-BASED STYLING ---
            current_status = s_df['SensorStatus'].iloc[0] if 'SensorStatus' in s_df.columns else 'Active'
            line_dash = 'solid' if current_status == 'Active' else 'dot'
            opacity = 1.0 if current_status == 'Active' else 0.6
            
            # Data Gap Handling: break line if gap > 6 hours
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
                name=f"{group_lbl} ({sn})", 
                legendgroup=group_lbl,
                showlegend=True if j == 0 else False,
                mode='lines+markers' if not is_surgical else 'markers',
                connectgaps=False, 
                line=dict(color=color, width=1.8 if current_status == 'Active' else 1.0, dash=line_dash),
                marker=dict(size=4, opacity=opacity),
                hovertemplate=f"<b>{group_lbl} ({sn})</b><br>Status: {current_status}<br>Temp: %{{y:.1f}}{unit_label}<extra></extra>"
            ))

    # 4. REFERENCE LINES & NOW MARKER
    for val, ref_label in active_refs:
        c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
        fig.add_hline(y=c_val, line_dash="dash", line_color="RoyalBlue", 
                      annotation_text=ref_label, annotation_position="top right")

    # Current time marker
    fig.add_vline(x=now_local, line_width=2, line_color="Red", layer='above', line_dash="dash")

    # 5. LAYOUT CONFIGURATION
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
    
    # 6. MONDAY VERTICAL LINES (Weekly Markers)
    mondays = pd.date_range(start=range_start, end=range_end, freq='W-MON', tz=display_tz)
    for mon in mondays:
        fig.add_vline(x=mon, line_width=2, line_color="dimgray", layer="below")

    return fig


##################
# Page Functions #
##################

###########
# - 5. PAGE: TIME vs TEMP - #
###########

def render_global_overview(selected_project, project_metadata, display_tz):
    """
    Shows all pipes/banks for a selected project in one scrolling view.
    """
    
    # 1. Safe Metadata Extraction
    stage_suffix = ""
    if project_metadata is not None:
        try:
            if isinstance(project_metadata, pd.DataFrame) and not project_metadata.empty:
                status = project_metadata['ProjectStatus'].iloc[0]
            else:
                status = project_metadata.get('ProjectStatus', '')
            if status:
                stage_suffix = f" [{status}]"
        except (KeyError, IndexError):
            stage_suffix = ""

    st.header(f"📈 Time vs Temp {stage_suffix}")
    
    # UI State Management
    mobile_mode = st.session_state.get("mobile_optimized_toggle", False)
    active_refs = st.session_state.get("active_refs", [])
    unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
    unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"
    
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to begin.")
        return

    # 2. Data Fetching
    with st.spinner(f"Syncing {selected_project} (Engineering View)..."):
        # FIX: Removed 'client' argument to prevent hashing errors
        p_df = get_universal_portal_data(selected_project, view_mode="engineering")

    if not p_df.empty:
        # --- NEW: FRESHNESS CHECK ---
        last_reading = p_df['timestamp'].max()
        # Ensure timestamp is offset-aware for comparison
        if last_reading.tzinfo is None:
            last_reading = last_reading.tz_localize('UTC')
        
        now_utc = pd.Timestamp.now(tz='UTC')
        hours_since_data = (now_utc - last_reading).total_seconds() / 3600
        
        if hours_since_data > 24:
            st.error(f"⚠️ **Stale Data Warning:** No new data has been received for this project in the last {int(hours_since_data)} hours.")
            st.info("This is common for Lord nodes that only upload on business mornings.")

        # --- Inside render_global_overview ---

        # 3. View Constraints
        # Added '0' as an option for "Full History"
        lookback = st.sidebar.slider("Lookback (Weeks)", 0, 52, 4, key="global_lookback_slider", help="Select 0 for Full History")
        
        now_local = pd.Timestamp.now(tz=display_tz)
        end_view = (now_local + pd.Timedelta(days=(7-now_local.weekday())%7 or 7)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        
        if lookback == 0:
            # Set start_view to the beginning of the dataframe for baselines
            start_view = p_df['timestamp'].min()
        else:
            start_view = end_view - timedelta(weeks=lookback)

        # 4. Render Graphs by Location
        locations = sorted(p_df['Location'].dropna().unique())
        
        for loc in locations:
            with st.expander(f"📍 Location: {loc}", expanded=True):
                loc_df = p_df[p_df['Location'] == loc]
                
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
                
                st.plotly_chart(fig, use_container_width=True, key=f"tvt_{selected_project}_{loc}")
    else:
        st.warning(f"No engineering data found for '{selected_project}' in the registry.")
        st.info("Check **Admin Tools > Node Registry** to ensure sensors are mapped to this project and location.")
        
###########
# - 6. PAGE: SENSOR STATUS - #
###########

def render_executive_summary(selected_project, unit_label, unit_mode, display_tz):
    """
    Page Name: Sensor Status
    Provides a high-level health and reliability audit of the sensor fleet.
    """
    st.header(f"🏠 Sensor Status: Health Monitor")
    
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to view health metrics.")
        return

    # FIX: Fetch client internally
    client = get_bq_client()
    if client is None: return

    # Updated SQL: Enriched with snapshots for trend analysis
    query = f"""
        WITH BaseReporting AS (
            SELECT 
                m.NodeNum, m.timestamp, m.temperature, m.Location, m.Bank, m.Depth, m.SensorStatus
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
            JOIN `{PROJECT_ID}.{DATASET_ID}.project_registry` p ON m.Project = p.Project
            WHERE m.Project = '{selected_project}'
            AND m.timestamp >= CAST(p.Date_Freezedown AS TIMESTAMP)
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
                
                -- Snapshots for trend calculation
                AVG(CASE WHEN timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN temperature END) as avg_1h,
                AVG(CASE WHEN timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR) THEN temperature END) as avg_6h,
                AVG(CASE WHEN timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 25 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature END) as avg_24h,

                MIN(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature ELSE NULL END) AS low_24h,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature ELSE NULL END) AS high_24h,
                MAX(TIMESTAMP_DIFF(timestamp, prev_ts, HOUR)) AS max_gap_7d,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN TIMESTAMP_DIFF(timestamp, prev_ts, HOUR) ELSE 0 END) AS gap_24h,
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
            st.warning("No data found for this project. Check that sensors are assigned in the Node Registry.")
            return

        for col in ['Depth', 'low_24h', 'high_24h', 'current_temp', 'avg_1h', 'avg_6h', 'avg_24h']:
            if col in raw_df.columns:
                raw_df[col] = pd.to_numeric(raw_df[col], errors='coerce')

        now_local = pd.Timestamp.now(tz=display_tz)

        # Helper for Trend Arrows
        def get_trend_arrow(current, previous):
            if pd.isnull(current) or pd.isnull(previous): return "N/A"
            delta = current - previous
            if delta > 0.1: return f"🔺 +{delta:.1f}"
            if delta < -0.1: return f"🔹 {delta:.1f}"
            return "➡️ 0.0"

        def fmt_temp(val):
            if pd.isnull(val): return "N/A"
            c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
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
        st.subheader("🔍 Sensor Drill-Down & Trends")
        loc_list = sorted(raw_df['Location'].unique().tolist())
        selected_loc = st.selectbox(
            "Detailed view for:", 
            ["--- Select Location ---"] + loc_list,
            key=f"status_drilldown_{selected_project}"
        )

        if selected_loc != "--- Select Location ---":
            sensor_df = raw_df[raw_df['Location'] == selected_loc].copy()
            
            def format_sensor_row(row):
                ping = row['last_ping']
                lag = 0.0
                if pd.notnull(ping):
                    if ping.tzinfo is None: ping = ping.tz_localize('UTC')
                    lag = round((now_local - ping.tz_convert(display_tz)).total_seconds() / 3600, 1)

                # Process Trends
                cur = row['current_temp']
                t1 = row['avg_1h']
                t6 = row['avg_6h']
                t24 = row['avg_24h']

                # Apply unit conversion for the trend values if needed
                if unit_mode == "Celsius":
                    cur, t1, t6, t24 = [(x - 32) * 5/9 if pd.notnull(x) else x for x in [cur, t1, t6, t24]]

                return pd.Series({
                    "Node ID": row['NodeNum'], 
                    "Depth": f"{row['Depth']}ft" if pd.notnull(row['Depth']) else "N/A",
                    "Current Temp": fmt_temp(row['current_temp']), 
                    "1h Trend": get_trend_arrow(cur, t1),
                    "6h Trend": get_trend_arrow(cur, t6),
                    "24h Trend": get_trend_arrow(cur, t24),
                    "High (24h)": fmt_temp(row['high_24h']),
                    "Low (24h)": fmt_temp(row['low_24h']),
                    "Seen (24h)": "✅" if row['seen_24h'] > 0 else "❌",
                    "Gap (24h)": f"{int(row['gap_24h'])}h",
                    "Status": f"{lag}h {'🟢' if lag < 6 else ('🟡' if lag < 24 else '🔴')}"
                })
            
            st.dataframe(sensor_df.apply(format_sensor_row, axis=1), use_container_width=True, hide_index=True)
            
    except Exception as e:
        st.error(f"Sensor Status Error: {e}")
        
###########
# - 7. PAGE: CLIENT PORTAL - #
###########

def render_client_portal(selected_project, project_metadata, display_tz, unit_mode, unit_label, active_refs):
    """
    Client-facing portal with approved thermal trends and vertical profiles.
    """
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to view client data.")
        return

    # --- 1. DYNAMIC HEADER SECTION ---
    # Safe extraction from metadata DataFrame or Dictionary
    meta = {}
    if project_metadata is not None:
        if isinstance(project_metadata, pd.DataFrame):
            if not project_metadata.empty:
                meta = project_metadata.iloc[0].to_dict()
        elif isinstance(project_metadata, pd.Series):
            meta = project_metadata.to_dict()
        elif isinstance(project_metadata, dict):
            meta = project_metadata

    display_name = meta.get('ProjectName', selected_project)
    project_status = meta.get('ProjectStatus', 'Active')
    city = meta.get('City', 'Unknown Location')
    tz_info = meta.get('Timezone', 'UTC')
    
    registry_disclaimer = meta.get('ClientDisclaimer') 
    eng_notes = meta.get('EngNotes')
    asbuilt_filename = meta.get('AsBuiltFile')
    # Header Rendering
    st.markdown(f"## 📊 {display_name}")
    st.markdown(
        f"<p style='color: #6d6d6d; font-size: 18px; margin-top: -15px;'>"
        f"Project {selected_project} | Status: {project_status}</p>", 
        unsafe_allow_html=True
    )
    
    with st.expander("📍 Site Information", expanded=False):
        st.write(f"**Location:** {city}")
        st.write(f"**Timezone:** {tz_info}")
        if pd.notnull(eng_notes) and str(eng_notes).strip() != "":
            st.divider()
            st.write(f"**Site Notes:** {eng_notes}")

    # Disclaimer logic
    if pd.notnull(registry_disclaimer) and str(registry_disclaimer).strip() != "":
        st.info(f"ℹ️ {registry_disclaimer}")
    else:
        st.info("ℹ️ Data is typically synchronized once per business day.")

    # --- 2. DATA FETCHING (APPROVED ONLY) ---
    with st.spinner("Synchronizing approved records..."):
        # This uses the view_mode="client" filter we built in the SQL engine
        p_df = get_universal_portal_data(selected_project, view_mode="client")
    
    if p_df.empty:
        st.warning(f"⚠️ No approved data records available for {display_name} yet.")
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

    # --- TAB 2: DEPTH PROFILE ---
    with tab_depth:
        st.subheader("📏 Vertical Temperature Profile")
        
        
        # Determine Freezing Line for Chart
        ref_val = 0.0 if unit_mode == "Celsius" else 32.0
        x_range = [-20, 40] if unit_mode == "Celsius" else [-10, 80]

        p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
        depth_only = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
        
        if depth_only.empty:
            st.info("Vertical profile data is not applicable for this project's sensor configuration.")
        else:
            for loc in sorted(depth_only['Location'].unique()):
                with st.expander(f"📏 {loc} Weekly Snapshots", expanded=False):
                    loc_data = depth_only[depth_only['Location'] == loc].copy()
                    fig_d = go.Figure()
                    
                    # Last 6 Mondays Snapshots
                    mondays = pd.date_range(end=pd.Timestamp.now(tz='UTC'), periods=6, freq='W-MON')
                    for m_date in mondays:
                        target_ts = m_date.replace(hour=6, minute=0, second=0)
                        window = loc_data[(loc_data['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & 
                                         (loc_data['timestamp'] <= target_ts + pd.Timedelta(hours=12))]
                        
                        if not window.empty:
                            snap_df = (
                                window.assign(diff=(window['timestamp'] - target_ts).abs())
                                .sort_values(['NodeNum', 'diff'])
                                .drop_duplicates('NodeNum')
                                .sort_values('Depth_Num')
                            )
                            
                            # Conversion applied to snapshot
                            c_temps = snap_df['temperature']
                            if unit_mode == "Celsius":
                                c_temps = (c_temps - 32) * 5/9
                            
                            fig_d.add_trace(go.Scatter(
                                x=c_temps, 
                                y=snap_df['Depth_Num'], 
                                mode='lines+markers', 
                                name=target_ts.strftime('%m/%d/%y'),
                                line=dict(shape='spline', smoothing=0.5)
                            ))

                    fig_d.add_vline(x=ref_val, line_dash="dash", line_color="RoyalBlue", 
                                    annotation_text="Freezing", annotation_position="top right")

                    max_d = depth_only['Depth_Num'].max()
                    y_limit = int(((max_d // 10) + 1) * 10) if pd.notnull(max_d) else 50
                    
                    fig_d.update_layout(
                        plot_bgcolor='white', height=600,
                        xaxis=dict(title=f"Temp ({unit_label})", range=x_range, gridcolor='Gainsboro'),
                        yaxis=dict(title="Depth (ft)", range=[y_limit, 0], dtick=10, gridcolor='Silver'),
                        legend=dict(orientation="h", y=-0.2)
                    )
                    st.plotly_chart(fig_d, use_container_width=True, key=f"portal_depth_{loc}")

    # --- TAB 3: SUMMARY TABLE ---
    with tab_table:
        latest = p_df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        
        def get_pos(r):
            if pd.notnull(r.get('Depth')): return f"{r['Depth']} ft"
            if pd.notnull(r.get('Bank')): return f"Bank {r['Bank']}"
            return "Surface"

        latest['Position'] = latest.apply(get_pos, axis=1)
        
        # Display simplified client-friendly table
        st.dataframe(
            latest[['Location', 'Position', 'temperature', 'timestamp']].sort_values(['Location', 'Position']), 
            use_container_width=True, hide_index=True,
            column_config={
                "temperature": st.column_config.NumberColumn(f"Temp ({unit_label})", format="%.1f"),
                "timestamp": st.column_config.DatetimeColumn("Last Updated", format="MM/DD/YY HH:mm")
            }
        )

    # --- TAB 4: AS-BUILT PLAN ---
    with tab_built:
        if pd.notnull(asbuilt_filename) and str(asbuilt_filename).strip() != "":
            st.image(f"assets/asbuilts/{asbuilt_filename}", caption=f"Sensor Layout: {display_name}")
        else:
            st.info("The as-built site plan for this project is currently being finalized.")

###########
# - 8. PAGE: NODE DIAGNOSTICS - #
###########

def render_node_diagnostics(selected_project, display_tz, unit_label):
    """
    Page Name: Node Diagnostics
    Live connectivity audit and data density check for all assigned nodes.
    """
    st.header(f"📡 Real-Time Commissioning: {selected_project}")
    st.write("Live connectivity audit and data density check for all assigned nodes.")

    # FIX: Fetch client internally to prevent hashing errors
    client = get_bq_client()
    if client is None: return

    # Diagnostic Query
    diag_q = f"""
        WITH Stats AS (
            SELECT 
                NodeNum,
                MAX(timestamp) as last_ping,
                ARRAY_AGG(temperature ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] as last_temp,
                COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) as count_1h,
                COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)) as count_6h
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view`
            WHERE Project = '{selected_project}'
            GROUP BY NodeNum
        )
        SELECT 
            n.Location, 
            n.NodeNum, 
            n.Bank, 
            n.Depth,
            n.SensorStatus, 
            s.last_ping,
            s.last_temp,
            COALESCE(s.count_1h, 0) as count_1h,
            COALESCE(s.count_6h, 0) as count_6h
        FROM `{PROJECT_ID}.{DATASET_ID}.node_registry` n
        LEFT JOIN Stats s ON n.NodeNum = s.NodeNum
        WHERE n.Project = '{selected_project}' 
    """
    
    try:
        df = client.query(diag_q).to_dataframe()
        
        if df.empty:
            st.warning("No sensors found for this project in the Node Registry.")
            return

        now = pd.Timestamp.now(tz='UTC')

        def get_latency_info(row):
            ping = row['last_ping']
            if pd.isnull(ping): 
                return "❌ Never", "Never Seen"
            
            if ping.tzinfo is None: ping = ping.tz_localize('UTC')
            diff_mins = (now - ping).total_seconds() / 60
            
            # Smart Latency: Thresholds adjusted for business-day batch uploads
            if diff_mins <= 15: cat = "🟢 0-15 Mins"
            elif diff_mins <= 60: cat = "🟡 15-60 Mins"
            elif diff_mins <= 1440: cat = "⏳ < 24 Hours" # Typical for daily Lord uploads
            else: cat = "🔴 > 24 Hours"
            
            return cat, f"{round(diff_mins/60, 1)}h ago"

        df[['Latency_Cat', 'Time_Ago']] = df.apply(lambda x: pd.Series(get_latency_info(x)), axis=1)
        
        def fmt_temp(val):
            if pd.isnull(val): return "N/A"
            c_val = (val - 32) * 5/9 if unit_label == "°C" else val
            return f"{round(c_val, 1)}{unit_label}"

        # Build final display table
        display_df = pd.DataFrame({
            "Location": df['Location'],
            "Node ID": df['NodeNum'],
            "Health": df['SensorStatus'], 
            "Position": df.apply(lambda r: f"{r['Depth']}ft" if pd.notnull(r['Depth']) else f"Bank {r['Bank']}", axis=1),
            "Connectivity": df['Latency_Cat'],
            "Last Seen": df['Time_Ago'],
            "Last Temp": df['last_temp'].apply(fmt_temp),
            "Pings (1h)": df['count_1h'],
            "Pings (6h)": df['count_6h']
        })

        # Sort Logic: Prioritize troubleshooting (Stale/Dead sensors first)
        order = ["🔴 > 24 Hours", "⏳ < 24 Hours", "🟡 15-60 Mins", "🟢 0-15 Mins", "❌ Never"]
        display_df['Connectivity'] = pd.Categorical(display_df['Connectivity'], categories=order, ordered=True)
        display_df = display_df.sort_values(['Connectivity', 'Health', 'Location'])

        st.dataframe(
            display_df, 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "Health": st.column_config.TextColumn(help="Hardware state: Active, Diagnostic, Need Repair, Dead"),
                "Pings (1h)": st.column_config.NumberColumn(help="Target: ~1 (SPush), ~60 (Lord)"),
                "Pings (6h)": st.column_config.NumberColumn(help="Check for sustained density"),
            }
        )
        
    except Exception as e:
        st.error(f"Diagnostics Error: {e}")
    
###########
# - 9. PAGE: DATA INTAKE LAB - #
###########

def render_data_intake_page(selected_project):
    """
    Handles manual file ingestion for Lord (Wide/Long) and SensorPush formats.
    """
    st.header("📤 Data Ingestion Lab")
    
    # FIX: Fetch client internally
    client = get_bq_client()
    
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
                        
                        target_table = "raw_lord" if ("-" in str(df_processed['NodeNum'].iloc[0])) else "raw_sensorpush"
                        
                        if st.button("🚀 Push to BigQuery"):
                            if client is None: return
                            with st.spinner("Uploading..."):
                                table_id = f"{PROJECT_ID}.{DATASET_ID}.{target_table}"
                                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                                client.load_table_from_dataframe(df_processed, table_id, job_config=job_config).result()
                                
                                st.success(f"Uploaded {len(df_processed)} rows to {target_table}!")
                                # CRITICAL: Clear cache so the new data shows up on other pages immediately
                                st.cache_data.clear()

            except Exception as e:
                st.error(f"Ingestion Error: {e}")

    with tab_export:
        st.subheader("📥 Export Project Data (Custom Wide Format)")
        if not selected_project or selected_project == "All Projects":
            st.warning("⚠️ Select a project in the sidebar first.")
        else:
            c1, c2 = st.columns(2)
            # --- Inside render_data_intake_page Export Tab ---
            e_start = c1.date_input("Start Date", value=datetime.now() - timedelta(days=30), key="export_start_date")
            e_end = c2.date_input("End Date", value=datetime.now(), key="export_end_date")
            
            with st.spinner("Fetching engineering records..."):
                full_df = get_universal_portal_data(selected_project, view_mode="engineering")
            
            if not full_df.empty:
                # 1. Scope Filter: Allow users to pick specific Locations/Banks
                all_locs = sorted(full_df['Location'].unique().tolist())
                selected_locs = st.multiselect(
                    "🎯 Filter by Location/Bank (Leave empty for ALL)", 
                    options=all_locs,
                    help="Pick specific monitoring points to include in the columns."
                )

                # Apply Filters (Date + Location)
                mask = (full_df['timestamp'].dt.date >= e_start) & (full_df['timestamp'].dt.date <= e_end)
                if selected_locs:
                    mask = mask & (full_df['Location'].isin(selected_locs))
                
                export_df = full_df.loc[mask].copy()
                
                if export_df.empty:
                    st.warning("No data found for this selection.")
                else:
                    # 2. THE TRANSFORMATION (Long to Wide)
                    # Label columns as "Location (NodeID)"
                    export_df['Sensor_Label'] = export_df['Location'] + " (" + export_df['NodeNum'].astype(str) + ")"
                    
                    # Create the Grid: Timestamp on the left, Sensors across the top
                    wide_df = export_df.pivot_table(
                        index='timestamp', 
                        columns='Sensor_Label', 
                        values='temperature',
                        aggfunc='first'
                    ).reset_index()

                    # Format timestamp for Excel compatibility
                    wide_df['timestamp'] = wide_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

                    # 3. Final Download
                    st.success(f"Generated report with {len(wide_df.columns)-1} sensor columns.")
                    csv = wide_df.to_csv(index=False).encode('utf-8')
                    
                    st.download_button(
                        label=f"💾 Download {selected_project} Wide Export",
                        data=csv,
                        file_name=f"{selected_project}_Custom_Export.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
            else:
                st.warning("No project data available in the registry.")
                        
###########
# - 10. PAGE: ADMIN TOOLS - #
###########

def render_admin_page(selected_project, display_tz, unit_mode, unit_label, active_refs):
    """
    Central hub for registry management, bulk approvals, and project lifecycle.
    """
    st.header("🛠️ Admin Tools")
    
    # FIX: Fetch client internally
    client = get_bq_client()
    if client is None: return

    # 1. GLOBAL REGISTRY FETCH
    # Joins hardware nodes with project-level metadata
    reg_q = f"""
        SELECT 
            n.*, 
            p.ProjectName, p.City, p.Timezone, p.ProjectStatus as MasterProjectStatus
        FROM `{PROJECT_ID}.{DATASET_ID}.node_registry` n
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.project_registry` p ON n.Project = p.Project
    """
    try:
        full_reg_df = client.query(reg_q).to_dataframe()
        for col in ['Depth', 'PhysicalID']:
            if col in full_reg_df.columns:
                full_reg_df[col] = pd.to_numeric(full_reg_df[col], errors='coerce')
    except Exception as e:
        st.error(f"Error joining registries: {e}")
        full_reg_df = pd.DataFrame()
    
    active_project_df = pd.DataFrame()
    if not full_reg_df.empty:
        active_project_df = full_reg_df[(full_reg_df['Project'] == selected_project) & (full_reg_df['End_Date'].isna())]
    
    loc_options = ["All Locations"] + sorted([str(l) for l in active_project_df['Location'].unique() if pd.notnull(l)]) if not active_project_df.empty else ["All Locations"]

    # --- 2. ADMIN NAVIGATION ---
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
        st.write(f"Mass-approving data for **{selected_project}**.")
        sel_loc = st.selectbox("Target Location", loc_options, key="bulk_loc_main")
        c1, c2 = st.columns(2)
        b_s = c1.date_input("Start Date", value=datetime.now()-timedelta(7))
        b_e = c2.date_input("End Date", value=datetime.now())
        
        if st.button("🚀 Execute Bulk Approval", use_container_width=True):
            loc_f = f"AND n.Location = '{sel_loc}'" if sel_loc != "All Locations" else ""
            sql = f"""
                INSERT INTO `{OVERRIDE_TABLE}` (NodeNum, timestamp, approve)
                SELECT DISTINCT r.NodeNum, TIMESTAMP_TRUNC(r.timestamp, HOUR), 'TRUE'
                FROM (
                    SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` 
                    UNION ALL 
                    SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
                ) AS r
                INNER JOIN `{PROJECT_ID}.{DATASET_ID}.node_registry` AS n ON r.NodeNum = n.NodeNum
                WHERE n.Project = '{selected_project}' {loc_f} 
                AND r.timestamp BETWEEN '{b_s}' AND '{b_e}'
                AND NOT EXISTS (
                    SELECT 1 FROM `{OVERRIDE_TABLE}` x 
                    WHERE x.NodeNum = r.NodeNum AND x.timestamp = TIMESTAMP_TRUNC(r.timestamp, HOUR)
                )
            """
            client.query(sql).result()
            st.success(f"Successfully approved records for {sel_loc}.")
            st.cache_data.clear()

    # --- TAB 2: NODE REGISTRY ---
    with tab_registry:
        st.subheader("📋 Hardware Assignment Manager")
        # Filters to help find specific sensors in a large fleet
        with st.expander("🔍 Filter Hardware View", expanded=False):
            f1, f2 = st.columns(2)
            raw_projs = full_reg_df['Project'].unique().tolist() if not full_reg_df.empty else []
            p_filter = f1.selectbox("View Project", ["All"] + sorted([str(p) for p in raw_projs if pd.notnull(p)]), key="reg_filter_proj")
            raw_stats = full_reg_df['SensorStatus'].unique().tolist() if not full_reg_df.empty else []
            s_filter = f2.selectbox("View Health Status", ["All"] + sorted([str(s) for s in raw_stats if pd.notnull(s)]), key="reg_filter_status")
            
            view_df = full_reg_df.copy()
            if p_filter != "All": view_df = view_df[view_df['Project'] == p_filter]
            if s_filter != "All": view_df = view_df[view_df['SensorStatus'] == s_filter]

        node_cols = ['NodeNum', 'Project', 'Location', 'Bank', 'Depth', 'Start_Date', 'End_Date', 'SensorStatus']
        # The Data Editor allows for inline changes to the BigQuery table
        edited_df = st.data_editor(
            view_df[node_cols].sort_values(['Project', 'Location']), 
            num_rows="dynamic", key="node_registry_editor_master", use_container_width=True
        )
        
        if st.button("💾 Sync Registry Changes", type="primary", use_container_width=True):
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            client.load_table_from_dataframe(edited_df, f"{PROJECT_ID}.{DATASET_ID}.node_registry", job_config=job_config).result()
            st.success("Node Registry synchronized with BigQuery.")
            st.cache_data.clear()
            st.rerun()

    # --- TAB 3: PROJECT MASTER ---
    with tab_project:
        st.subheader("⚙️ Project Management & Lifecycle")
        p_mode = st.radio("Action", ["Overview", "New Project", "Edit Project"], horizontal=True)
        proj_reg_df = client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.project_registry`").to_dataframe()
    
        if p_mode == "Overview":
            st.dataframe(proj_reg_df.sort_values('Date_Initialized', ascending=False), use_container_width=True, hide_index=True)

        elif p_mode == "New Project":
            with st.form("init_project_form"):
                c1, c2 = st.columns(2)
                new_id = c1.text_input("Project ID (e.g. 2538-Ferndale)")
                new_name = c2.text_input("Project Name")
                new_city = c1.text_input("City")
                new_tz = c2.selectbox("Timezone", ["US/Pacific", "US/Eastern", "UTC"])
                if st.form_submit_button("🚀 Create Project"):
                    today = datetime.now().strftime('%Y-%m-%d')
                    sql = f"INSERT INTO `{PROJECT_ID}.{DATASET_ID}.project_registry` (Project, ProjectName, City, Timezone, ProjectStatus, Date_Initialized) VALUES ('{new_id}', '{new_name}', '{new_city}', '{new_tz}', 'Initialized', '{today}')"
                    client.query(sql).result()
                    st.success(f"Project {new_id} Created.")
                    st.cache_data.clear()
                    st.rerun()
    
        elif p_mode == "Edit Project" and not proj_reg_df.empty:
            target_proj = st.selectbox("Select Project", sorted([str(p) for p in proj_reg_df['Project'].unique()]))
            p_data = proj_reg_df[proj_reg_df['Project'] == target_proj].iloc[0]
            with st.form("p_update_form"):
                u_status = st.selectbox("Stage", ["Initialized", "Pre-freeze", "Freezedown", "Maintenance", "Post-freeze", "Archived"], index=0)
                u_eng = st.text_area("Site Notes", value=p_data.get('EngNotes', ''))
                u_disclaim = st.text_area("Client Disclaimer", value=p_data.get('ClientDisclaimer', ''))
                
                if st.form_submit_button("💾 Save Settings"):
                    # Dynamic visibility logic: stamp the date if moving to a new phase
                    date_update = ""
                    if u_status == "Freezedown" and pd.isnull(p_data.get('Date_Freezedown')):
                        date_update = f", Date_Freezedown='{datetime.now().strftime('%Y-%m-%d')}'"
                    
                    sql = f"UPDATE `{PROJECT_ID}.{DATASET_ID}.project_registry` SET ProjectStatus='{u_status}', EngNotes='{u_eng}', ClientDisclaimer='{u_disclaim}' {date_update} WHERE Project='{target_proj}'"
                    client.query(sql).result()
                    st.success("Project updated.")
                    st.cache_data.clear()
                    st.rerun()

    # --- TAB 4: SCRUB ---
    with tab_scrub:
        st.subheader("🧹 Data Averaging")
        target = st.radio("Target Table", ["SensorPush", "Lord"], horizontal=True)
        if st.button("🧨 Execute Hourly Averaging"):
            t_tab = f"{PROJECT_ID}.{DATASET_ID}.raw_{target.lower()}"
            # This query collapses high-frequency data into clean hourly buckets
            sql = f"""
                CREATE OR REPLACE TABLE `{t_tab}` AS 
                SELECT TIMESTAMP_TRUNC(timestamp, HOUR) as timestamp, NodeNum, AVG(temperature) as temperature 
                FROM `{t_tab}` 
                GROUP BY 1, 2
            """
            client.query(sql).result()
            st.success("Data scrubbed to hourly averages.")
            st.cache_data.clear()

    # --- TAB 5: SURGICAL ---
    with tab_surgical:
        # Assumes render_surgical_cleaner is defined elsewhere in your utils
        render_surgical_cleaner(selected_project, display_tz, unit_mode, unit_label)

    # --- TAB 6: AUDIT ---
    with tab_audit:
        st.subheader("🕒 Full Registry Audit")
        st.dataframe(full_reg_df.sort_values('Start_Date', ascending=False), use_container_width=True)

###########
# - 11. SURGICAL CLEANER FUNCTIONS - #
###########

def render_surgical_cleaner(selected_project, display_tz, unit_mode, unit_label):
    """
    🧨 Unified Data Management (Mask & Purge)
    Precision tool for hiding or deleting bad data points.
    """
    from datetime import time as dt_time
    import re
    import time

    st.subheader("🧨 Unified Data Management (Mask & Purge)")
    
    # 0. INITIALIZE DATABASE CLIENT
    client = get_bq_client()
    if client is None:
        st.error("Database connection unavailable.")
        return

    # 1. SCOPE & ACTION MODE
    c1, c2 = st.columns(2)
    with c1:
        scope = st.radio(
            "Target Scope", 
            ["Project Wide", "Specific Location", "Specific Node"], 
            horizontal=True, 
            key="surg_scope_toggle"
        )
    with c2:
        action_mode = st.radio(
            "Action Type", 
            ["🚫 Mask (Soft Hide)", "🔥 Purge (Hard Delete)"], 
            horizontal=True, 
            key="surg_action_toggle"
        )

    # Fetch Registry for Filtering
    reg_q = f"SELECT NodeNum, Location FROM `{PROJECT_ID}.{DATASET_ID}.node_registry` WHERE Project = '{selected_project}'"
    reg_df = client.query(reg_q).to_dataframe()
    
    target_node, target_loc = None, None
    if not reg_df.empty:
        if scope == "Specific Location":
            target_loc = st.selectbox("Select Location", sorted(reg_df['Location'].unique()), key="surg_loc_select")
        elif scope == "Specific Node":
            target_node = st.selectbox("Select Node ID", sorted(reg_df['NodeNum'].unique()), key="surg_node_select")
    else:
        st.warning("No nodes found in registry for this project.")
        return

    # 2. TEMPORAL LOGIC
    st.divider()
    t_col1, t_col2 = st.columns([1, 2])
    direction = t_col1.selectbox(
        "Temporal Direction", 
        ["Between Range", "Everything Older Than", "Everything Newer Than"],
        key="surg_time_direction"
    )
    
    with t_col2:
        if direction == "Between Range":
            sc1, sc2 = st.columns(2)
            s_dt = datetime.combine(sc1.date_input("Start Date", value=datetime.now() - timedelta(days=7), key="surg_start"), dt_time(0,0))
            e_dt = datetime.combine(sc2.date_input("End Date", value=datetime.now(), key="surg_end"), dt_time(23,59))
        else:
            anchor_dt = datetime.combine(
                st.date_input("Anchor Date", key="surg_anchor_d"), 
                st.time_input("Anchor Time", value=dt_time(6,0), key="surg_anchor_t")
            )
            s_dt = datetime(2000, 1, 1) if direction == "Everything Older Than" else anchor_dt
            e_dt = anchor_dt if direction == "Everything Older Than" else datetime(2100, 1, 1)

    # 3. THRESHOLD LOGIC (Defined BEFORE SQL construction to prevent NameError)
    thr_col1, thr_col2 = st.columns([1, 2])
    operator = thr_col1.selectbox(
        "Value Filter", 
        ["No Threshold", "Greater Than (>)", "Less Than (<)"], 
        key="surg_val_op"
    )
    thresh_val = thr_col2.number_input(f"Threshold Value ({unit_label})", value=100.0, key="surg_val_input")
    
    # Convert for BQ
    thresh_val_f = (thresh_val * 9/5) + 32 if unit_mode == "Celsius" else thresh_val

    # 4. SQL CONSTRUCTION
    if scope == "Project Wide":
        where_clause = f"n.Project = '{selected_project}'"
    elif scope == "Specific Location":
        where_clause = f"n.Project = '{selected_project}' AND n.Location = '{target_loc}'"
    else:
        where_clause = f"n.NodeNum = '{target_node}' AND n.Project = '{selected_project}'"

    threshold_clause = ""
    if operator == "Greater Than (>)": 
        threshold_clause = f"AND r.temperature > {thresh_val_f}"
    elif operator == "Less Than (<)": 
        threshold_clause = f"AND r.temperature < {thresh_val_f}"

    s_str, e_str = s_dt.strftime('%Y-%m-%d %H:%M:%S'), e_dt.strftime('%Y-%m-%d %H:%M:%S')

    # 5. EXECUTION GATE
    st.divider()
    if st.button("🔍 Step 1: Verify Match Count", use_container_width=True, key="surg_verify_btn"):
        status_q = f"""
            SELECT COALESCE(rej.approve, 'PENDING') as status, COUNT(*) as point_count
            FROM (
                SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` 
                UNION ALL 
                SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
            ) AS r
            INNER JOIN `{PROJECT_ID}.{DATASET_ID}.node_registry` AS n ON r.NodeNum = n.NodeNum
            LEFT JOIN `{OVERRIDE_TABLE}` AS rej ON r.NodeNum = rej.NodeNum AND TIMESTAMP_TRUNC(r.timestamp, HOUR) = rej.timestamp
            WHERE {where_clause} 
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
            confirm = st.checkbox(f"Confirm {action_mode} for these records.", key="surg_confirm_check")
            
            if st.button(f"🚀 Execute {action_mode}", use_container_width=True, disabled=not confirm, key="surg_exec_btn"):
                if "Mask" in action_mode:
                    sql = f"""
                        MERGE `{OVERRIDE_TABLE}` T
                        USING (
                            SELECT DISTINCT r.NodeNum, TIMESTAMP_TRUNC(r.timestamp, HOUR) as ts
                            FROM (
                                SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` 
                                UNION ALL 
                                SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
                            ) AS r
                            INNER JOIN `{PROJECT_ID}.{DATASET_ID}.node_registry` AS n ON r.NodeNum = n.NodeNum
                            WHERE {where_clause} 
                            AND r.timestamp BETWEEN '{s_str}' AND '{e_str}'
                            {threshold_clause}
                        ) S ON T.NodeNum = S.NodeNum AND T.timestamp = S.ts
                        WHEN MATCHED THEN UPDATE SET approve = 'MASKED'
                        WHEN NOT MATCHED THEN INSERT (NodeNum, timestamp, approve) VALUES (S.NodeNum, S.ts, 'MASKED')
                    """
                else:
                    # Hard Delete logic with Multi-Table Transaction
                    sql = f"""
                        BEGIN TRANSACTION;
                        DELETE FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` r 
                        WHERE NodeNum IN (SELECT NodeNum FROM `{PROJECT_ID}.{DATASET_ID}.node_registry` n WHERE {where_clause})
                        AND r.timestamp BETWEEN '{s_str}' AND '{e_str}' {threshold_clause};
                        
                        DELETE FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord` r 
                        WHERE NodeNum IN (SELECT NodeNum FROM `{PROJECT_ID}.{DATASET_ID}.node_registry` n WHERE {where_clause})
                        AND r.timestamp BETWEEN '{s_str}' AND '{e_str}' {threshold_clause};
                        COMMIT;
                    """
                
                try:
                    client.query(sql).result()
                    st.success(f"Successfully processed {total} points.")
                    del st.session_state["purge_staged_df"]
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    st.error(f"Surgical execution failed: {e}")
        else:
            st.info("No matching records found for the selected criteria.")

###########
# - 11. SURGICAL CLEANER HELPERS - #
###########

def update_records(pts, df, val, display_tz):
    """
    Writes status updates (TRUE, FALSE, MASKED) to the manual_rejections table.
    Ensures timezone alignment so clicked points match database timestamps.
    """
    import time # Required for the sleep pause
    
    # FIX: Fetch client internally
    client = get_bq_client()
    if client is None: return

    recs = []
    for p in pts:
        try:
            # 1. Capture the timestamp from the Plotly click event
            ts_raw = pd.to_datetime(p['x'])
            
            # Logic: If the graph is showing Pacific/Eastern time, 
            # we must convert back to UTC before writing to BigQuery.
            if ts_raw.tzinfo is None:
                ts = ts_raw.tz_localize(display_tz).tz_convert('UTC').floor('h')
            else:
                ts = ts_raw.tz_convert('UTC').floor('h')
            
            # 2. Grab the NodeNum from the dataframe row using the point index
            node = df.iloc[p['point_index']]['NodeNum']
            
            recs.append({
                "NodeNum": str(node), 
                "timestamp": ts, 
                "approve": val 
            })
        except Exception as e:
            # Silently skip points that can't be parsed
            continue
    
    if recs:
        # 3. Deduplicate within this batch to prevent redundant writes
        status_df = pd.DataFrame(recs).drop_duplicates(subset=['NodeNum', 'timestamp'])
        
        try:
            # 4. APPEND the new status to the manual_rejections table
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            job = client.load_table_from_dataframe(status_df, OVERRIDE_TABLE, job_config=job_config)
            job.result() 
            
            # 5. UI Feedback & State Reset
            # Clear the 'clicked' selection so the dots disappear from the UI
            if "locked_selection" in st.session_state:
                st.session_state.locked_selection = []
            
            # CRITICAL: Clear cache so the graphs pull the new MASKED status immediately
            st.cache_data.clear() 
            
            st.success(f"✅ Successfully marked {len(status_df)} records as {val}")
            
            # Brief pause for user feedback before the app refreshes
            time.sleep(0.5) 
            st.rerun()
            
        except Exception as e:
            st.error(f"Failed to update database: {e}")

###########
# - 13. PAGE: LANDING PAGE - #
###########

###########
# - 12. PAGE: DEPTH CHARTS (ENGINEERING) - #
###########

def render_depth_charts(selected_project, unit_label, display_tz):
    """
    Engineering-grade Vertical Temperature Profiles.
    Shows the thermal gradient across soil depths without date masking.
    """
    st.header(f"📏 Depth Profile Analysis: {selected_project}")
    
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to view depth profiles.")
        return

    # 1. Fetch Engineering Data (No Date Masking)
    with st.spinner("Fetching full project history for vertical analysis..."):
        # We use engineering mode here to bypass the Date_Freezedown mask
        p_df = get_universal_portal_data(selected_project, view_mode="engineering")

    if p_df.empty:
        st.warning("No data found. Ensure sensors have 'Depth' values in the Node Registry.")
        return

    # 2. Pre-process Depth Data
    p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
    depth_df = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
    
    if depth_df.empty:
        st.info("No sensors with valid Depth values found for this project.")
        return

    # 3. UI Controls for the Profile
    st.sidebar.subheader("📐 Profile Settings")
    # Engineering lookback defaults to 0 (Full History) to see baselines
    lookback = st.sidebar.slider("Historical Snapshots (Weeks)", 0, 24, 8, key="depth_lookback")
    
    unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
    ref_val = 0.0 if unit_mode == "Celsius" else 32.0
    x_range = [-20, 40] if unit_mode == "Celsius" else [-10, 80]

    # 4. Generate Snapshots (Mondays)
    now_utc = pd.Timestamp.now(tz='UTC')
    # If lookback is 0, we still show at least the current week
    num_snapshots = max(lookback, 1)
    mondays = pd.date_range(end=now_utc, periods=num_snapshots, freq='W-MON')

    locations = sorted(depth_df['Location'].unique())
    
    for loc in locations:
        with st.expander(f"📍 Location: {loc}", expanded=True):
            loc_data = depth_df[depth_df['Location'] == loc].copy()
            fig = go.Figure()
            
            for m_date in mondays:
                target_ts = m_date.replace(hour=6, minute=0, second=0)
                # Find data within a 12-hour window of each Monday morning
                window = loc_data[(loc_data['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & 
                                 (loc_data['timestamp'] <= target_ts + pd.Timedelta(hours=12))]
                
                if not window.empty:
                    # Get the reading closest to the target Monday morning for every node
                    snap = (
                        window.assign(diff=(window['timestamp'] - target_ts).abs())
                        .sort_values(['NodeNum', 'diff'])
                        .drop_duplicates('NodeNum')
                        .sort_values('Depth_Num')
                    )
                    
                    temps = snap['temperature']
                    if unit_mode == "Celsius":
                        temps = (temps - 32) * 5/9
                    
                    fig.add_trace(go.Scatter(
                        x=temps, 
                        y=snap['Depth_Num'], 
                        mode='lines+markers', 
                        name=target_ts.strftime('%Y-%m-%d'),
                        line=dict(shape='spline', smoothing=0.3)
                    ))

            # Add the Freezing Reference Line
            fig.add_hline(y=0, line_width=1, line_color="black") # Ground level
            fig.add_vline(x=ref_val, line_dash="dash", line_color="RoyalBlue", 
                          annotation_text="Freezing", annotation_position="top right")

            # Determine Y-Axis (Depth) scale
            max_d = depth_df['Depth_Num'].max()
            y_limit = int(((max_d // 10) + 1) * 10) if pd.notnull(max_d) else 50

            fig.update_layout(
                title=f"Vertical Thermal Gradient - {loc}",
                plot_bgcolor='white', height=700,
                xaxis=dict(title=f"Temperature ({unit_label})", range=x_range, gridcolor='Gainsboro', showline=True, linecolor='black'),
                yaxis=dict(title="Depth (ft)", range=[y_limit, 0], dtick=5, gridcolor='Silver', showline=True, linecolor='black'),
                legend=dict(orientation="h", y=-0.15)
            )
            
            st.plotly_chart(fig, use_container_width=True, key=f"depth_chart_{loc}")

###########
# - 11. PAGE: SUMMARY (GLOBAL) - #
###########

def render_landing_page(unit_label, unit_mode):
    """
    The main Dashboard. Shows active project health and temperature trends.
    """
    st.header("🌐 Global Project Summary")
    
    # FIX: Fetch client internally
    client = get_bq_client()
    if client is None: return

    # Improved Query: 
    # 1. Joins project_registry for status.
    # 2. Uses a wider window (48h) to catch Lord sensors that might be slightly delayed.
    summary_q = f"""
        WITH active_projects AS (
            SELECT Project, ProjectName, ProjectStatus 
            FROM `{PROJECT_ID}.{DATASET_ID}.project_registry`
            WHERE ProjectStatus IN ('Freezedown', 'Maintenance', 'Pre-freeze')
        ),
        raw_data AS (
            SELECT 
                n.Project, n.Bank, n.Location, n.Depth, m.temperature, m.timestamp
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
            JOIN `{PROJECT_ID}.{DATASET_ID}.node_registry` n ON m.NodeNum = n.NodeNum
            WHERE m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
        )
        SELECT 
            p.Project, p.ProjectName, p.ProjectStatus,
            ld.Bank, ld.Location, ld.Depth,
            AVG(CASE WHEN ld.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN ld.temperature END) as avg_now,
            AVG(CASE WHEN ld.timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN ld.temperature END) as avg_1h,
            AVG(CASE WHEN ld.timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR) THEN ld.temperature END) as avg_6h,
            AVG(CASE WHEN ld.timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 25 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN ld.temperature END) as avg_24h,
            -- Latest known temperature if 'avg_now' is null (for Lord batch nodes)
            ARRAY_AGG(ld.temperature ORDER BY ld.timestamp DESC LIMIT 1)[OFFSET(0)] as last_known_temp,
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
        p_name = p_df['ProjectName'].iloc[0] if not p_df['ProjectName'].isnull().all() else project
        
        with st.container(border=True):
            st.subheader(f"🏗️ {p_name} ({project})")
            
            c1, c2, c3, c4 = st.columns(4)
            
            # Classification Logic
            is_ambient = p_df['Bank'].str.contains('Amb', case=False, na=False) | p_df['Location'].str.contains('Amb', case=False, na=False)
            is_supply = (p_df['Bank'].str.startswith('S', na=False) | p_df['Location'].str.startswith('S', na=False)) & ~is_ambient
            is_return = (p_df['Bank'].str.startswith('R', na=False) | p_df['Location'].str.startswith('R', na=False)) & ~is_ambient
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
                    if group_df.empty:
                        st.caption("No sensors assigned")
                        continue
                    
                    # Core Metrics - Using last_known_temp as fallback for avg_now
                    now = group_df['avg_now'].mean()
                    if pd.isnull(now):
                        now = group_df['last_known_temp'].mean()
                        st.caption("Using last known (Lord)")

                    prev_1h = group_df['avg_1h'].mean()
                    prev_6h = group_df['avg_6h'].mean()
                    prev_24h = group_df['avg_24h'].mean()
                    mn, mx = group_df['min_24h'].min(), group_df['max_24h'].max()
                    
                    # Unit Conversion
                    if unit_mode == "Celsius":
                        now, prev_1h, prev_6h, prev_24h, mn, mx = [
                            (x - 32) * 5/9 if pd.notnull(x) else None 
                            for x in [now, prev_1h, prev_6h, prev_24h, mn, mx]
                        ]
                    
                    if pd.notnull(now):
                        st.metric("Current Avg", f"{now:.1f}{unit_label}")
                        st.markdown(f"**24h Range:** {mn:.1f} to {mx:.1f}{unit_label}")
                        
                        # Trends
                        st.write("**Thermal Delta**")
                        t1, t2, t3 = st.columns(3)
                        t1.caption(f"1h\n{get_trend_arrow(now, prev_1h)}")
                        t2.caption(f"6h\n{get_trend_arrow(now, prev_6h)}")
                        t3.caption(f"24h\n{get_trend_arrow(now, prev_24h)}")
                    else:
                        st.caption("No data in window")

def get_trend_arrow(current, previous):
    if pd.isnull(current) or pd.isnull(previous): return "N/A"
    delta = current - previous
    if delta > 0.1: return f"🔺 +{delta:.1f}"
    if delta < -0.1: return f"🔹 {delta:.1f}"
    return "➡️ 0.0"

###########
# - 12. MAIN ROUTER - #
###########

# --- PAGE ROUTING LOGIC ---

if page == "Summary":
    # Removed 'client' - function now calls get_bq_client() internally
    render_landing_page(unit_label, unit_mode)

elif page == "Time vs Temp":
    # Removed 'client' - updated to match the new 3-parameter definition
    render_global_overview(selected_project, project_metadata, display_tz)

elif page == "Sensor Status":
    # FIX: Add unit_mode to the call
    render_executive_summary(
        selected_project, 
        unit_label, 
        unit_mode,    # <--- Add this here
        display_tz
    )

elif page == "Depth Charts":
    # Ensure this function exists in your utils; updated to standard parameters
    render_depth_charts(selected_project, unit_label, display_tz)

elif page == "Node Diagnostics":
    render_node_diagnostics(selected_project, display_tz, unit_label)

elif page == "Client Portal":
    render_client_portal(
        st.session_state.get('selected_project'), 
        st.session_state.get('project_metadata_df'), # This is your DataFrame
        display_tz, 
        unit_mode, 
        unit_label, 
        active_refs
    )

# --- PASSWORD PROTECTED SECTIONS ---
elif page in ["Data Intake Lab", "Admin Tools"]:
    if st.session_state.get('authenticated', False):
        if page == "Data Intake Lab":
            render_data_intake_page(selected_project)
        else:
            render_admin_page(selected_project, display_tz, unit_mode, unit_label, active_refs)
    else:
        st.subheader("🔐 Restricted Access")
        st.info("Administrative and Ingestion tools require authorization.")
        
        # Center the password box
        col_a, col_b, col_c = st.columns([1, 2, 1])
        with col_b:
            pwd = st.text_input("Enter Authorized Password", type="password")
            if st.button("Unlock Access", use_container_width=True):
                if pwd == st.secrets["admin_password"]:
                    st.session_state['authenticated'] = True
                    # Clear cache on auth to ensure admin sees fresh registry data
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error("Incorrect password. Please contact the administrator.")
