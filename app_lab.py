import streamlit as st
import pandas as pd
import time
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta, timezone, date, time as dt_time
import pytz
import traceback
import io
import re

# 1. CONFIGURATION & STYLING
st.set_page_config(
    page_title="SoilFreeze Data Lab", 
    page_icon="❄️", 
    layout="wide"
)

# Global Database Constants
DATASET_ID = "Temperature" 
PROJECT_ID = "sensorpush-export"
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"

@st.cache_resource
def get_bq_client():
    """
    Initializes and caches the BigQuery connection.
    Prioritizes Service Account info from st.secrets for Streamlit Cloud.
    """
    try:
        SCOPES = [
            "https://www.googleapis.com/auth/bigquery", 
            "https://www.googleapis.com/auth/drive" 
        ]
        
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(
                info, 
                scopes=SCOPES
            )
            return bigquery.Client(credentials=credentials, project=info["project_id"])
        
        return bigquery.Client(project=PROJECT_ID)

    except Exception as e:
        st.error(f"❌ BigQuery Authentication Failed: {e}")
        return None
        
############################
# - 2. DATA ENGINE LOGIC - #
############################

@st.cache_data(ttl=600)
def get_universal_portal_data(project_id, view_mode="engineering"):
    """
    Core data fetcher with built-in visibility logic for Client vs Engineering.
    """
    client = get_bq_client()
    if client is None:
        return pd.DataFrame()

    # 1. Classification & Visibility Logic
    if view_mode == "client":
        # Clients only see data marked as approved
        filter_sql = "AND UPPER(CAST(m.approval_status AS STRING)) IN ('TRUE', '1')"
        # Client ONLY sees data from the official Freezedown date onwards
        visibility_sql = "AND m.timestamp >= CAST(p.Date_Freezedown AS TIMESTAMP)"
    else:
        # Engineering sees everything except what was explicitly MASKED
        filter_sql = "AND UPPER(COALESCE(CAST(m.approval_status AS STRING), 'PENDING')) NOT IN ('FALSE', '0', 'MASKED')"
        visibility_sql = ""

    query = f"""
        SELECT m.* FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
        JOIN `{PROJECT_ID}.{DATASET_ID}.project_registry` p ON m.Project = p.Project
        WHERE m.Project = @project_id
        {visibility_sql}
        {filter_sql}
        ORDER BY m.Location ASC, m.timestamp ASC
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("project_id", "STRING", project_id)
        ]
    )
    
    try:
        query_job = client.query(query, job_config=job_config)
        return query_job.to_dataframe()
    except Exception as e:
        st.error(f"⚠️ Data Sync Error for '{project_id}': {e}")
        print(traceback.format_exc())
        return pd.DataFrame()
        
###########################
# - SIDEBAR NAVIGATION -  #
###########################
###########################
# - SIDEBAR NAVIGATION -  #
###########################

st.sidebar.title("❄️ SoilFreeze Lab")

# --- NAVIGATION ---
page = st.sidebar.selectbox(
    "Navigation", 
    [
        "Summary",             
        "Time vs Temp",        
        "Sensor Status",       
        "Depth Charts", 
        "Node Diagnostics", 
        "Client Portal", 
        "Data Intake Lab", 
        "Admin Tools"
    ],
    key="nav_page"
)

st.sidebar.divider()

# --- PROJECT SELECTION ---
selected_project = "All Projects"
project_metadata = None  

sidebar_client = get_bq_client()

if sidebar_client is not None:
    try:
        # UPDATED: Added SoilType to the selection
        proj_q = f"""
            SELECT Project, ProjectName, Timezone, ProjectStatus, Date_Freezedown, SoilType 
            FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` 
            WHERE ProjectStatus != 'Archived'
        """
        proj_df = sidebar_client.query(proj_q).to_dataframe()
        proj_list = sorted(proj_df['Project'].dropna().unique().tolist())
        
        selected_project = st.sidebar.selectbox(
            "🎯 Active Project", 
            ["All Projects"] + proj_list, 
            key="sidebar_proj_picker_global"
        )
        
        st.session_state['selected_project'] = selected_project
        
        if selected_project != "All Projects":
            meta_row = proj_df[proj_df['Project'] == selected_project]
            if not meta_row.empty:
                project_metadata = meta_row.iloc[0].to_dict()
                st.session_state['project_metadata'] = project_metadata
        else:
            st.session_state['project_metadata'] = None
            
    except Exception as e:
        st.sidebar.error(f"Registry Link Offline: {e}")
        
st.sidebar.divider()

# --- UNIT & MEASUREMENT ---
unit_mode = st.sidebar.radio(
    "Temperature Unit", 
    ["Fahrenheit", "Celsius"], 
    horizontal=True,
    key="unit_toggle"
)
unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"
st.session_state["unit_mode"] = unit_mode
st.session_state["unit_label"] = unit_label

st.sidebar.divider()

# --- TIME & DISPLAY ---
st.sidebar.subheader("📱 Display & Time")

default_tz_index = 2 # Default to Pacific
if project_metadata and project_metadata.get('Timezone') == "US/Eastern":
    default_tz_index = 1

tz_lookup = {
    "UTC": "UTC", 
    "Local (US/Eastern)": "US/Eastern", 
    "Local (US/Pacific)": "US/Pacific"
}

tz_mode = st.sidebar.selectbox(
    "Timezone Display", 
    list(tz_lookup.keys()), 
    index=default_tz_index,
    key="tz_picker"
)

display_tz = tz_lookup[tz_mode]
st.session_state["display_tz"] = display_tz

mobile_optimized = st.sidebar.toggle(
    "Mobile Layout", 
    value=False, 
    key="mobile_optimized_toggle"
)

st.sidebar.divider()

# --- REFERENCE LINES ---
st.sidebar.subheader("📏 Reference Lines")
active_refs = [] 

if st.sidebar.checkbox("Freezing (32°F)", value=True, key="ref_freezing"): 
    active_refs.append((32.0, "Freezing"))
if st.sidebar.checkbox("Type B (26.6°F)", value=False, key="ref_type_b"): 
    active_refs.append((26.6, "Type B"))
if st.sidebar.checkbox("Type A (10.2°F)", value=False, key="ref_type_a"): 
    active_refs.append((10.2, "Type A"))

st.session_state["active_refs"] = tuple(active_refs)

#############
# - Graph - #
#############
def build_high_speed_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label, 
                           display_tz="UTC", mobile_mode=False, f_start_date=None, curve_id=None):
    """
    Final Master Function: 
    - 15 High-Contrast Colors
    - Dark Gray (60% Opacity) Goals plotted LAST (Top Layer)
    - Full Black Engineering Box Borders (LineWidth 2)
    - Gap Detection & Spline Smoothing
    """
    import plotly.graph_objects as go
    
    if df.empty:
        return go.Figure().update_layout(title="No data available")

    client = get_bq_client()
    plot_df = df.copy() 
    fig = go.Figure()

    # 1. TIMEZONE & UNIT CONVERSION
    if plot_df['timestamp'].dt.tz is None:
        plot_df['timestamp'] = plot_df['timestamp'].dt.tz_localize('UTC')
    plot_df['timestamp'] = plot_df['timestamp'].dt.tz_convert(display_tz)
    
    if unit_mode == "Celsius":
        plot_df['temperature'] = (plot_df['temperature'] - 32) * 5/9
        y_range = [-30, 30]
    else:
        y_range = [-20, 80]

    # 2. SENSOR TRACE GENERATION (Bottom Layer)
    # 15-Color Engineering Palette
    extended_colors = [
        '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', 
        '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf', 
        '#FF1493', '#00CED1', '#FFD700', '#8A2BE2', '#32CD32'  
    ]

    # Labeling Logic
    plot_df['depth_label'] = "Node " + plot_df['NodeNum'].astype(str)
    plot_df['sort_val'] = 1000.0
    depth_mask = plot_df['Depth'].notnull()
    plot_df.loc[depth_mask, 'depth_label'] = plot_df.loc[depth_mask, 'Depth'].astype(str) + "ft"
    plot_df.loc[depth_mask, 'sort_val'] = pd.to_numeric(plot_df.loc[depth_mask, 'Depth'], errors='coerce')
    
    unique_groups = plot_df[['depth_label', 'sort_val']].drop_duplicates().sort_values('sort_val')

    for i, (_, g_row) in enumerate(unique_groups.iterrows()):
        group_lbl = g_row['depth_label']
        group_data = plot_df[plot_df['depth_label'] == group_lbl]
        color = extended_colors[i % len(extended_colors)]
        
        for sn in group_data['NodeNum'].unique():
            s_df = group_data[group_data['NodeNum'] == sn].sort_values('timestamp')
            
            # Gap Handling
            s_df['gap'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
            if (s_df['gap'] > 6.0).any():
                gaps = s_df[s_df['gap'] > 6.0].copy()
                gaps['temperature'] = None
                gaps['timestamp'] = gaps['timestamp'] - pd.Timedelta(minutes=1)
                s_df = pd.concat([s_df, gaps]).sort_values('timestamp')

            fig.add_trace(go.Scatter(
                x=s_df['timestamp'], y=s_df['temperature'],
                name=f"{group_lbl} (N:{sn})",
                mode='lines',
                line=dict(shape='spline', smoothing=1.3, width=2, color=color),
                connectgaps=False,
                hovertemplate="<b>%{fullData.name}</b><br>Temp: %{y:.1f}" + unit_label + "<extra></extra>"
            ))

    # 3. THEORETICAL REFERENCE CURVES (Top Layer)
    # Plotted after sensors so they appear on top.
    if curve_id and curve_id != "None" and f_start_date:
        try:
            # Fuzzy Logic: Match Project and Location
            parts = str(curve_id).split('-')
            p_id, l_id = parts[0], parts[1] if len(parts) > 1 else parts[0]
            
            ref_q = f"""
                SELECT CurveID, Day, Temp 
                FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` 
                WHERE UPPER(CurveID) LIKE UPPER('%{p_id}%') 
                  AND UPPER(CurveID) LIKE UPPER('%{l_id}%')
                ORDER BY Day
            """
            ref_df = client.query(ref_q).to_dataframe()
            
            if not ref_df.empty:
                dash_patterns = ['dash', 'dot', 'dashdot', 'longdash']
                for idx, (full_cid, g_df) in enumerate(ref_df.groupby('CurveID')):
                    clean_name = full_cid.split('-')[-1] if '-' in full_cid else full_cid
                    
                    g_df['timestamp'] = g_df['Day'].apply(
                        lambda d: pd.Timestamp(f_start_date) + pd.Timedelta(days=d)
                    )
                    
                    ref_y = g_df['Temp']
                    if unit_mode == "Celsius":
                        ref_y = (ref_y - 32) * 5/9

                    fig.add_trace(go.Scatter(
                        x=g_df['timestamp'], y=ref_y,
                        name=f"<b>GOAL: {clean_name}</b>",
                        mode='lines',
                        line=dict(
                            color='rgba(40, 40, 40, 0.6)', # Dark Gray, 60% Opacity
                            width=4,                       # Bold Shape
                            dash=dash_patterns[idx % len(dash_patterns)]
                        ),
                        legendrank=1 # Force Goals to top of legend
                    ))
        except: pass

    # 4. REFERENCE LINES (Freezing/Now)
    for val, ref_label in active_refs:
        c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
        fig.add_hline(y=c_val, line_dash="dash", line_color="RoyalBlue", 
                      annotation_text=ref_label, annotation_position="top right", layer="below")

    now_local = pd.Timestamp.now(tz=display_tz)
    fig.add_vline(x=now_local, line_width=2, line_color="Red", line_dash="dash", layer='above')

    # 5. LAYOUT & BOX BORDER
    l_cfg = dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5) if mobile_mode else \
            dict(orientation="v", x=1.02, y=1, xanchor="left", yanchor="top")
    
    fig.update_layout(
        title={'text': f"<b>{title}</b>", 'x': 0.02, 'y': 0.95, 'font': {'size': 18}},
        plot_bgcolor='white', 
        hovermode="x unified", 
        height=650,
        legend=l_cfg,
        margin=dict(l=60, r=20, t=80, b=60),
        # FULL ENGINEERING BORDER
        xaxis=dict(
            range=[start_view, end_view], 
            showgrid=True, gridcolor='Gainsboro', 
            showline=True, mirror=True, linecolor='black', linewidth=2,
            tickformat='%b %d\n%H:%M'
        ),
        yaxis=dict(
            title=f"Temperature ({unit_label})", 
            range=y_range, 
            showgrid=True, gridcolor='Gainsboro',
            showline=True, mirror=True, linecolor='black', linewidth=2,
            zeroline=False
        )
    )
    
    return fig

def get_soil_reference_curves(soil_type, start_date, unit_mode):
    """
    Fallback function for hardcoded soil types.
    """
    references = {
        "Silty Sand": [(0, 50), (5, 32), (14, 20), (30, 10), (60, 5)],
        "Clay":       [(0, 50), (10, 32), (25, 25), (45, 15), (90, 10)]
    }
    
    curve = references.get(soil_type, [])
    if not curve:
        return None, None
        
    x_times = [pd.Timestamp(start_date) + pd.Timedelta(days=d) for d, t in curve]
    y_temps = [t if unit_mode == "Fahrenheit" else (t - 32) * 5/9 for d, t in curve]
    
    return x_times, y_temps
##################
# Page Functions #
##################

###########
# - 5. PAGE: TIME vs TEMP - #
###########

def render_global_overview(selected_project, project_metadata, display_tz):
    """
    Shows all pipes/banks for a selected project in one scrolling view.
    Includes the Master Switch for Reference Curves and logic for chart borders.
    """
    # 1. INITIALIZE UI STATE VARIABLES
    mobile_mode = st.session_state.get("mobile_optimized_toggle", False)
    active_refs = st.session_state.get("active_refs", [])
    unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
    unit_label = st.session_state.get("unit_label", "°F")

    # 2. EXTRACT PROJECT METADATA
    p_name = selected_project
    status = "Active"
    f_start_date = None
    assigned_curve = "None"

    if project_metadata:
        p_name = project_metadata.get('ProjectName', selected_project)
        status = project_metadata.get('ProjectStatus', 'Active')
        assigned_curve = project_metadata.get('SoilType', 'None')
        
        raw_f_date = project_metadata.get('Date_Freezedown')
        if pd.notnull(raw_f_date):
            f_start_date = pd.to_datetime(raw_f_date).date()

    # 3. HEADER & FREEZEDOWN TRACKER
    st.header(f"📈 Time vs Temp: {p_name} [{status}]")
    
    if f_start_date:
        today = pd.Timestamp.now(tz=display_tz).date()
        days_since = (today - f_start_date).days
        st.markdown(f"### 🗓️ Day **{max(0, days_since)}** of Freezedown")
    else:
        st.caption("ℹ️ Freeze start date not yet initialized in Project Master.")

    # 4. SIDEBAR TOGGLES (The Master Switch)
    st.sidebar.subheader("👁️ Visibility Settings")
    
    # The Switch to turn curves ON/OFF
    show_ref = st.sidebar.toggle("Show Theoretical Curves", value=True, help="Toggle background reference curves for TP locations.")
    show_masked = st.sidebar.toggle("Show Masked Points", value=False)

    # 5. DATA PRE-FLIGHT
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Select a project in the sidebar to view engineering trends.")
        return

    with st.spinner(f"Syncing {p_name} telemetry..."):
        p_df = get_universal_portal_data(selected_project, view_mode="engineering")

    if p_df.empty:
        st.warning(f"No engineering data found for '{p_name}'.")
        return

    # 6. MASKING FILTER
    if not show_masked and 'approve' in p_df.columns:
        p_df = p_df[p_df['approve'] != 'MASKED'].copy()

    # 7. TIMELINE CONFIG (With 1-Day Cushion)
    st.sidebar.subheader("📅 Timeline Controls")
    lookback = st.sidebar.slider("Lookback (Weeks)", 0, 52, 4, key="global_lookback_slider")
    
    now_local = pd.Timestamp.now(tz=display_tz)
    # Set end_view to tomorrow at midnight for a visual cushion
    end_view = (now_local + pd.Timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    
    if lookback == 0:
        start_view = p_df['timestamp'].min()
    else:
        start_view = end_view - pd.Timedelta(weeks=lookback)

    # 8. DEFINE LOCATIONS
    locations = sorted([str(loc) for loc in p_df['Location'].dropna().unique()])

    # 9. LOCATION-BASED PLOTTING LOOP
    for loc in locations:
        with st.expander(f"📍 Location: {loc}", expanded=True):
            loc_df = p_df[p_df['Location'] == loc].copy()
            
            # --- THE FIX ---
            # We only want the ID (2527), not the full name (2527-Elizabeth)
            # This extracts '2527' from '2527-Elizabeth'
            clean_proj_id = str(selected_project).split('-')[0] 
            
            # This creates "2527-TP4" (matching your library files)
            search_id = f"{clean_proj_id}-{loc}" 
            
            is_temp_pipe = any(x in loc.upper() for x in ["TP", "T", "PIPE", "TEMP"])
            
            fig = build_high_speed_graph(
                df=loc_df, 
                title=f"Thermal Trends: {loc}", 
                start_view=start_view, 
                end_view=end_view, 
                active_refs=active_refs, 
                unit_mode=unit_mode, 
                unit_label=unit_label, 
                display_tz=display_tz,
                f_start_date=f_start_date,
                curve_id=search_id if (show_ref and is_temp_pipe) else None
            )
            
            st.plotly_chart(fig, use_container_width=True, key=f"tvt_{selected_project}_{loc}")

###########
# - 6. PAGE: SENSOR STATUS - #
###########

def render_sensor_status(client, selected_project, unit_label, unit_mode, display_tz):
    """
    Page Name: Sensor Status
    Strictly locked to: project_registry, master_data_view, and manual_rejections.
    """
    # 1. HEADER LOGIC (Source: project_registry via Sidebar Session State)
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

    # 2. TELEMETRY & COVERAGE QUERY (Uses master_data_view)
    query = f"""
        WITH BaseReporting AS (
            SELECT m.NodeNum, m.timestamp, m.temperature, m.Location, m.Bank, m.Depth
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
            WHERE m.Project = @proj_id
        ),
        GapAnalysis AS (
            SELECT *, LAG(timestamp) OVER (PARTITION BY NodeNum ORDER BY timestamp) AS prev_ts
            FROM BaseReporting
        ),
        HistoricalStats AS (
            SELECT 
                NodeNum, Location, Bank, Depth,
                MAX(timestamp) AS last_ping,
                ARRAY_AGG(temperature ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS current_temp,
                AVG(CASE WHEN timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN temperature END) as avg_1h,
                AVG(CASE WHEN timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 25 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature END) as avg_24h,
                
                -- Pulse Check Flags
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN 1 ELSE 0 END) as seen_1h_f,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR) THEN 1 ELSE 0 END) as seen_6h_f,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN 1 ELSE 0 END) as seen_24h_f,

                -- Hourly Coverage Calculation (Distinct hours seen / Total hours in period)
                (COUNT(DISTINCT CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN TIMESTAMP_TRUNC(timestamp, HOUR) END) / 24.0) * 100 as coverage_24h,
                (COUNT(DISTINCT CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 168 HOUR) THEN TIMESTAMP_TRUNC(timestamp, HOUR) END) / 168.0) * 100 as coverage_7d,

                -- Extremes & Gaps
                MIN(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature END) AS low_24h,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature END) AS high_24h,
                MAX(TIMESTAMP_DIFF(timestamp, prev_ts, HOUR)) AS max_gap_7d
            FROM GapAnalysis 
            GROUP BY NodeNum, Location, Bank, Depth
        )
        SELECT * FROM HistoricalStats
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)]
    )

    try:
        df = client.query(query, job_config=job_config).to_dataframe()
        if df.empty:
            st.warning("No data found in master_data_view for this project.")
            return

        # 3. STATUS & LAG CALCULATIONS
        now_local = pd.Timestamp.now(tz=display_tz)
        def get_lag(ts):
            if pd.isnull(ts): return 999.0
            ts_aware = ts if ts.tzinfo else ts.tz_localize('UTC')
            return (now_local - ts_aware.tz_convert(display_tz)).total_seconds() / 3600

        df['last_seen_hrs'] = df['last_ping'].apply(get_lag)

        # 4. FORMATTING HELPERS
        def get_status_icon(hrs):
            if hrs <= 1.0: return f"🟢 {hrs:.1f}h"
            if hrs <= 6.0: return f"🟠 {hrs:.1f}h"
            return f"🔴 {hrs:.1f}h"

        def fmt_t(val):
            if pd.isnull(val): return "N/A"
            v = (val - 32) * 5/9 if unit_mode == "Celsius" else val
            return f"{v:.1f}{unit_label}"

        def get_arrow(cur, prev):
            if pd.isnull(cur) or pd.isnull(prev): return "N/A"
            d = cur - prev
            return f"🔺 +{d:.1f}" if d > 0.1 else f"🔹 {d:.1f}" if d < -0.1 else "➡️ 0.0"

        # 5. LOCATION SUMMARY (High-Resolution Spread)
        st.subheader("📍 Location Performance Summary")
        
        summary_df = df.groupby('Location').apply(lambda x: pd.Series({
            'Total Nodes': len(x),
            'Seen 1h': int(x['seen_1h_f'].sum()),
            'Seen 6h': int(x['seen_6h_f'].sum()),
            'Seen 24h': int(x['seen_24h_f'].sum()),
            '24h Coverage': f"{x['coverage_24h'].mean():.1f}%",
            '7d Coverage': f"{x['coverage_7d'].mean():.1f}%",
            'Avg Temp': fmt_t(x['current_temp'].mean()),
            'Low 24h': fmt_t(x['low_24h'].min()),
            'High 24h': fmt_t(x['high_24h'].max()),
            'Best Seen': get_status_icon(x['last_seen_hrs'].min()),
            'Worst Seen': get_status_icon(x['last_seen_hrs'].max())
        })).reset_index()

        st.dataframe(summary_df, use_container_width=True, hide_index=True)

        # 6. DETAILED SENSOR AUDIT
        st.divider()
        st.subheader("🔍 Detailed Sensor Audit")
        
        selected_loc = st.selectbox("Filter Audit by Location:", ["--- All ---"] + sorted(df['Location'].unique()))
        audit_df = df.copy() if selected_loc == "--- All ---" else df[df['Location'] == selected_loc]
        
        rows = []
        for _, r in audit_df.sort_values(['Location', 'Depth', 'Bank']).iterrows():
            rows.append({
                "Location": r['Location'],
                "Node": r['NodeNum'],
                "Pos": f"{r['Depth']}ft" if pd.notnull(r['Depth']) else f"B:{r['Bank']}",
                "Temp": fmt_t(r['current_temp']),
                "1h Δ": get_arrow(r['current_temp'], r['avg_1h']),
                "24h Δ": get_arrow(r['current_temp'], r['avg_24h']),
                "24h Low": fmt_t(r['low_24h']),
                "24h High": fmt_t(r['high_24h']),
                "24h Coverage": f"{r['coverage_24h']:.1f}%",
                "7d Coverage": f"{r['coverage_7d']:.1f}%",
                "Last Seen": get_status_icon(r['last_seen_hrs']),
                "Max Gap": f"{r['max_gap_7d']:.1f}h"
            })
        
        st.dataframe(rows, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Sensor Status Error: {e}")        
#####################
# Depth Charts #
#####################

def render_depth_charts(selected_project, unit_label, display_tz):
    """
    Engineering-grade Vertical Temperature Profiles.
    Fixed: Full 4-sided frame and dynamic Baseline date label.
    """
    # 1. HEADER
    st.header(f"📏 Depth Profile Analysis: {selected_project}")
    
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a project to view profiles.")
        return

    # 2. SIDEBAR SETTINGS
    st.sidebar.subheader("📐 Profile Settings")
    lookback_weeks = st.sidebar.slider("Historical Snapshots (Weeks)", 1, 24, 8, key="depth_lookback")

    with st.spinner("Fetching historical telemetry..."):
        p_df = get_universal_portal_data(selected_project, view_mode="engineering")

    if p_df is None or p_df.empty:
        st.warning("No data found for this project.")
        return

    # 3. PRE-PROCESS DATA
    p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
    depth_df = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
    
    if depth_df.empty:
        st.info("No sensors with valid 'Depth' values found.")
        return

    unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
    freeze_pt = 0 if unit_mode == "Celsius" else 32
    
    # 4. GENERATE SNAPSHOTS
    now_utc = pd.Timestamp.now(tz='UTC')
    mondays = pd.date_range(end=now_utc, periods=lookback_weeks, freq='W-MON')
    locations = sorted(depth_df['Location'].unique())
    
    for loc in locations:
        with st.expander(f"📍 Temp vs Depth - {loc}", expanded=True):
            loc_data = depth_df[depth_df['Location'] == loc].copy()
            fig = go.Figure()

            # --- A. PLOT BASELINE (With Date Label) ---
            baseline_raw = loc_data.sort_values('timestamp', ascending=True)
            if not baseline_raw.empty:
                # Extract the earliest date for the label
                b_date_str = baseline_raw['timestamp'].min().strftime('%Y-%m-%d')
                
                baseline_snap = (
                    baseline_raw.drop_duplicates('NodeNum')
                    .sort_values('Depth_Num')
                )
                
                b_temps = baseline_snap['temperature']
                if unit_mode == "Celsius": b_temps = (b_temps - 32) * 5/9
                
                fig.add_trace(go.Scatter(
                    x=b_temps, y=baseline_snap['Depth_Num'], 
                    mode='lines+markers', 
                    name=f'Baseline ({b_date_str})',
                    line=dict(color='black', width=2.5),
                    marker=dict(size=7, symbol='diamond'),
                    hovertemplate=f"Baseline: {b_date_str}<br>Depth: %{y}ft<br>Temp: %{x:.1f}" + unit_label
                ))
            
            # --- B. PLOT WEEKLY SNAPSHOTS ---
            for m_date in mondays:
                target_ts = m_date.replace(hour=6, minute=0, second=0)
                window = loc_data[
                    (loc_data['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & 
                    (loc_data['timestamp'] <= target_ts + pd.Timedelta(hours=12))
                ]
                
                if not window.empty:
                    snap = (
                        window.assign(diff=(window['timestamp'] - target_ts).abs())
                        .sort_values(['NodeNum', 'diff'])
                        .drop_duplicates('NodeNum')
                        .sort_values('Depth_Num')
                    )
                    
                    temps = snap['temperature']
                    if unit_mode == "Celsius": temps = (temps - 32) * 5/9
                    
                    fig.add_trace(go.Scatter(
                        x=temps, y=snap['Depth_Num'], 
                        mode='lines+markers', 
                        name=target_ts.strftime('%Y-%m-%d'),
                        line=dict(shape='spline', smoothing=1.1, width=1.5),
                        marker=dict(size=4),
                        hovertemplate="%{fullData.name}<br>Depth: %{y}ft<br>Temp: %{x:.1f}" + unit_label
                    ))

            # --- C. FREEZING REFERENCE LINE ---
            fig.add_vline(x=freeze_pt, line_width=2, line_dash="solid", line_color="cyan")

            # --- D. DYNAMIC X-AXIS CALCULATION ---
            current_max = loc_data['temperature'].max()
            if unit_mode == "Celsius":
                current_max = (current_max - 32) * 5/9
                temp_upper = max(20, current_max + 5)
                temp_lower = -20
            else:
                temp_upper = max(60, current_max + 5)
                temp_lower = -10

            # --- E. LAYOUT & BOX FRAME ---
            max_depth = loc_data['Depth_Num'].max()
            y_limit = int(((max_depth // 10) + 1) * 10) if pd.notnull(max_depth) else 50

            fig.update_layout(
                title=f"<b>Temp vs Depth - {loc}</b>",
                plot_bgcolor='white', 
                height=800,
                xaxis=dict(
                    title=f"Temperature ({unit_label})", 
                    range=[temp_lower, temp_upper],
                    dtick=10,
                    minor=dict(dtick=2, showgrid=True, gridcolor='#f0f0f0'),
                    gridcolor='Gainsboro', 
                    showline=True, 
                    linewidth=2, 
                    linecolor='black',
                    mirror=True  # This forces the line to the TOP as well
                ),
                yaxis=dict(
                    title="Depth (ft)", 
                    range=[y_limit, 0], 
                    dtick=10,
                    minor=dict(dtick=2, showgrid=True, gridcolor='#f0f0f0'),
                    gridcolor='Silver', 
                    showline=True, 
                    linewidth=2, 
                    linecolor='black',
                    mirror=True  # This forces the line to the RIGHT as well
                ),
                legend=dict(orientation="h", y=-0.1, xanchor="center", x=0.5)
            )
            
            st.plotly_chart(fig, use_container_width=True, key=f"depth_cht_{selected_project}_{loc}")            

##############################            
# - 7. PAGE: CLIENT PORTAL - #
##############################

def render_client_portal(selected_project, project_metadata, display_tz, unit_mode, unit_label, active_refs):
    """
    Client-facing portal with approved thermal trends and vertical profiles.
    Includes Theoretical Goal overlays and professional chart borders.
    """
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to view client data.")
        return

    # 1. METADATA & HEADER
    meta = project_metadata if isinstance(project_metadata, dict) else {}
    display_name = meta.get('ProjectName', selected_project)
    project_status = meta.get('ProjectStatus', 'Active')
    f_start_date = pd.to_datetime(meta.get('Date_Freezedown')).date() if pd.notnull(meta.get('Date_Freezedown')) else None
    
    asbuilt_filename = meta.get('AsBuiltFile')
    registry_disclaimer = meta.get('ClientDisclaimer') 

    st.markdown(f"## 📊 {display_name}")
    st.markdown(f"<p style='color: #6d6d6d; font-size: 18px; margin-top: -15px;'>Status: {project_status}</p>", unsafe_allow_html=True)

    if pd.notnull(registry_disclaimer) and str(registry_disclaimer).strip() != "":
        st.info(f"ℹ️ {registry_disclaimer}")

    # 2. DATA FETCHING (CLIENT MODE)
    with st.spinner("Synchronizing official records..."):
        p_df = get_universal_portal_data(selected_project, view_mode="client")
    
    if p_df.empty:
        st.warning(f"⚠️ No approved data records available for {display_name} yet.")
        return

    # 3. TABS
    tab_time, tab_depth, tab_table, tab_built = st.tabs([
        "📈 Timeline Analysis", "📏 Depth Profile", "📋 Summary Table", "🗺️ As-Built Plan"
    ])

    # --- TAB 1: TIMELINE ANALYSIS ---
    with tab_time:
        st.sidebar.subheader("📅 Portal View Options")
        weeks_view = st.sidebar.slider("Timeline Span (Weeks)", 1, 12, 6, key="client_weeks_slider")
        show_ref = st.sidebar.toggle("Show Progress Goals", value=True)
        
        now_utc = pd.Timestamp.now(tz='UTC')
        start_view = now_utc - timedelta(weeks=weeks_view)
        
        locations = sorted([str(loc) for loc in p_df['Location'].dropna().unique()])
        for loc in locations:
            with st.expander(f"📍 {loc} Thermal Trend", expanded=True):
                loc_data = p_df[p_df['Location'] == loc].copy()
                
                # Smart Match for Client: e.g., 2527-TP8
                cid = f"{selected_project}-{loc}" if show_ref else None

                fig = build_high_speed_graph(
                    df=loc_data, 
                    title=f"{loc}: {weeks_view}-Week Trend", 
                    start_view=start_view, 
                    end_view=now_utc, 
                    active_refs=active_refs, 
                    unit_mode=unit_mode, 
                    unit_label=unit_label, 
                    display_tz=display_tz,
                    f_start_date=f_start_date,
                    curve_id=cid
                )
                st.plotly_chart(fig, use_container_width=True, key=f"portal_grid_{loc}")

    # --- TAB 2: DEPTH PROFILE ---
    with tab_depth:
        st.subheader("📏 Vertical Temperature Profile")
        p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
        depth_only = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
        
        if depth_only.empty:
            st.info("Vertical profile data is not available for this project.")
        else:
            x_range = [-20, 40] if unit_mode == "Celsius" else [-10, 80]
            
            for loc in sorted(depth_only['Location'].unique()):
                with st.expander(f"📏 Temp vs Depth - {loc}", expanded=True):
                    loc_data = depth_only[depth_only['Location'] == loc].copy()
                    fig_d = go.Figure()
                    
                    # Weekly Snapshots
                    mondays = pd.date_range(end=pd.Timestamp.now(tz='UTC'), periods=4, freq='W-MON')
                    for m_date in mondays:
                        target_ts = m_date.replace(hour=6, minute=0, second=0)
                        window = loc_data[(loc_data['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & 
                                         (loc_data['timestamp'] <= target_ts + pd.Timedelta(hours=12))]
                        
                        if not window.empty:
                            snap_df = window.assign(diff=(window['timestamp'] - target_ts).abs()).sort_values(['NodeNum', 'diff']).drop_duplicates('NodeNum').sort_values('Depth_Num')
                            c_temps = snap_df['temperature'] if unit_mode == "Fahrenheit" else (snap_df['temperature'] - 32) * 5/9
                            
                            fig_d.add_trace(go.Scatter(
                                x=c_temps, y=snap_df['Depth_Num'], 
                                mode='lines+markers', name=target_ts.strftime('%m/%d/%y'),
                                line=dict(shape='spline', smoothing=0.5)
                            ))

                    # --- ADD THEORETICAL GOAL TO DEPTH CHART ---
                    if show_ref and f_start_date:
                        try:
                            client = get_bq_client()
                            today_day = (pd.Timestamp.now().date() - f_start_date).days
                            ref_q = f"SELECT Temp FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` WHERE UPPER(CurveID) LIKE UPPER('%{selected_project}-{loc}%') AND Day <= {today_day} ORDER BY Day DESC LIMIT 1"
                            res = client.query(ref_q).to_dataframe()
                            if not res.empty:
                                goal_temp = res.iloc[0]['Temp'] if unit_mode == "Fahrenheit" else (res.iloc[0]['Temp'] - 32) * 5/9
                                fig_d.add_vline(x=goal_temp, line_dash="dot", line_color="Red", annotation_text="Target Goal")
                        except: pass

                    max_d = depth_only['Depth_Num'].max()
                    y_limit = int(((max_d // 10) + 1) * 10) if pd.notnull(max_d) else 50
                    
                    fig_d.update_layout(
                        plot_bgcolor='white', height=600,
                        # FULL BOARDER
                        xaxis=dict(title=f"Temp ({unit_label})", range=x_range, showline=True, mirror=True, linecolor='black'),
                        yaxis=dict(title="Depth (ft)", range=[y_limit, 0], showline=True, mirror=True, linecolor='black'),
                        legend=dict(orientation="h", y=-0.2, xanchor="center", x=0.5)
                    )
                    st.plotly_chart(fig_d, use_container_width=True, key=f"portal_depth_{loc}")

    # --- TAB 3: SUMMARY TABLE ---
    with tab_table:
        latest = p_df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        latest['Position'] = latest.apply(lambda r: f"{r['Depth']} ft" if pd.notnull(r.get('Depth')) else (f"Bank {r['Bank']}" if pd.notnull(r.get('Bank')) else "Surface"), axis=1)
        st.dataframe(latest[['Location', 'Position', 'temperature', 'timestamp']].sort_values(['Location', 'Position']), use_container_width=True, hide_index=True)

    # --- TAB 4: AS-BUILT PLAN ---
    with tab_built:
        if pd.notnull(asbuilt_filename):
            st.image(f"assets/asbuilts/{asbuilt_filename}", caption=f"Site Plan: {display_name}")
        else:
            st.info("The as-built site plan is currently being processed.")
###########
# - 8. PAGE: NODE DIAGNOSTICS - #
###########

def render_node_diagnostics(selected_project, display_tz, unit_label):
    """
    Page Name: Node Diagnostics
    Live connectivity audit and data density check for all assigned nodes.
    """
    st.header(f"📡 Commissioning Audit: {selected_project}")
    st.write("Real-time audit of sensor connectivity and packet density.")

    # Fetch client internally
    client = get_bq_client()
    if client is None: 
        st.error("Database connection lost.")
        return

    # 1. DIAGNOSTIC QUERY
    # We calculate pings over the last 1h and 6h to verify signal stability
    diag_q = f"""
        WITH Stats AS (
            SELECT 
                NodeNum,
                MAX(timestamp) as last_ping,
                ARRAY_AGG(temperature ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] as last_temp,
                COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) as count_1h,
                COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)) as count_6h
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view`
            WHERE Project = @proj_id
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
        WHERE n.Project = @proj_id
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)]
    )
    
    try:
        df = client.query(diag_q, job_config=job_config).to_dataframe()
        
        if df.empty:
            st.warning("No sensors found in Node Registry for this project. Map sensors in Admin Tools first.")
            return

        now_utc = pd.Timestamp.now(tz='UTC')

        # 2. LATENCY CATEGORIZATION
        def get_latency_info(row):
            ping = row['last_ping']
            if pd.isnull(ping): 
                return "❌ Never", "Never Seen"
            
            # Ensure localized UTC comparison
            ping_utc = ping if ping.tzinfo else ping.tz_localize('UTC')
            diff_mins = (now_utc - ping_utc).total_seconds() / 60
            
            if diff_mins <= 15: cat = "🟢 0-15 Mins"
            elif diff_mins <= 60: cat = "🟡 15-60 Mins"
            elif diff_mins <= 1440: cat = "⏳ < 24 Hours"
            else: cat = "🔴 > 24 Hours"
            
            # Formatting the "Time Ago" string
            if diff_mins < 60:
                time_str = f"{int(diff_mins)}m ago"
            elif diff_mins < 1440:
                time_str = f"{round(diff_mins/60, 1)}h ago"
            else:
                time_str = f"{int(diff_mins/1440)}d ago"
            
            return cat, time_str

        df[['Latency_Cat', 'Time_Ago']] = df.apply(lambda x: pd.Series(get_latency_info(x)), axis=1)
        
        # 3. UNIT FORMATTING
        def fmt_temp(val):
            if pd.isnull(val): return "N/A"
            unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
            c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
            return f"{round(c_val, 1)}{unit_label}"

        # 4. TABLE CONSTRUCTION
        display_df = pd.DataFrame({
            "Location": df['Location'],
            "Node ID": df['NodeNum'],
            "Status": df['SensorStatus'], 
            "Position": df.apply(lambda r: f"{r['Depth']}ft" if pd.notnull(r['Depth']) else f"Bank {r['Bank']}", axis=1),
            "Connectivity": df['Latency_Cat'],
            "Last Seen": df['Time_Ago'],
            "Last Temp": df['last_temp'].apply(fmt_temp),
            "Pings (1h)": df['count_1h'],
            "Pings (6h)": df['count_6h']
        })

        # TROUBLESHOOTING SORT: Dead/Stale sensors first
        order = ["❌ Never", "🔴 > 24 Hours", "⏳ < 24 Hours", "🟡 15-60 Mins", "🟢 0-15 Mins"]
        display_df['Connectivity'] = pd.Categorical(display_df['Connectivity'], categories=order, ordered=True)
        display_df = display_df.sort_values(['Connectivity', 'Status', 'Location'])

        # Display full audit table
        st.dataframe(
            display_df, 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "Status": st.column_config.TextColumn(help="Hardware state: Active, Diagnostic, Dead, etc."),
                "Connectivity": st.column_config.TextColumn(help="Time since last data packet received."),
                "Pings (1h)": st.column_config.NumberColumn(help="Telemetry Density. High density = Better signal stability."),
                "Pings (6h)": st.column_config.NumberColumn(help="Historical Density for identifying intermittent signal drops."),
            }
        )
        
    except Exception as e:
        st.error(f"Diagnostics Audit Failed: {e}")
    
###########
# - 9. PAGE: DATA INTAKE LAB - #
###########

def render_data_intake_page(selected_project):
    """
    Handles manual file ingestion (Lord/SensorPush) and custom wide-format exports.
    """
    st.header("📤 Data Ingestion Lab")
    
    client = get_bq_client()
    if client is None:
        st.error("Database connection unavailable.")
        return
    
    tab_upload, tab_export = st.tabs(["📄 Upload Telemetry", "📥 Export Report"])
    
    # --- TAB 1: UPLOAD LOGIC ---
    with tab_upload:
        st.subheader("📄 Manual File Ingestion")
        st.info("Rule: Lord IDs use '-' (58014-ch1). SensorPush IDs are numeric.")
        
        u_file = st.file_uploader("Select CSV or Excel file", type=['csv', 'xlsx'], key="manual_upload_main")
        
        if u_file is not None:
            try:
                # 1. FORMAT DETECTION
                is_sensorconnect, skip_rows = False, 0
                if u_file.name.endswith('.csv'):
                    u_file.seek(0)
                    for i, line in enumerate(u_file):
                        if b"DATA_START" in line:
                            is_sensorconnect, skip_rows = True, i + 1
                            break
                    u_file.seek(0)

                # 2. DATA READING
                if is_sensorconnect:
                    st.info("Detected Format: Lord SensorConnect (Wide)")
                    df_raw = pd.read_csv(u_file, encoding='latin1', skiprows=skip_rows, dtype=str)
                elif u_file.name.endswith('.csv'):
                    df_raw = pd.read_csv(u_file, encoding='latin1', dtype=str)
                else:
                    df_raw = pd.read_excel(u_file, dtype=str)

                if not df_raw.empty:
                    df_processed = pd.DataFrame()
                    actual_headers = list(df_raw.columns)
                    clean_headers = [str(h).strip().lower() for h in actual_headers]
                    
                    # BRANCH A: Lord SensorConnect (Melt Wide to Long)
                    if is_sensorconnect:
                        time_col = [h for h in actual_headers if 'time' in h.lower()][0]
                        value_vars = [h for h in actual_headers if h != time_col]
                        df_melted = df_raw.melt(id_vars=[time_col], value_vars=value_vars, var_name='NodeNum', value_name='temperature')
                        df_processed['timestamp'] = pd.to_datetime(df_melted[time_col], format='mixed')
                        df_processed['NodeNum'] = df_melted['NodeNum'].str.strip().str.replace(':', '-')
                        df_processed['temperature'] = pd.to_numeric(df_melted['temperature'], errors='coerce')

                    # BRANCH B: Lord SensorCloud (Standard Long Format)
                    elif any(k in clean_headers for k in ['channel', 'node']) and any('time' in h for h in clean_headers):
                        st.info("Detected Format: Lord (Standard Long)")
                        time_h = actual_headers[next(i for i, h in enumerate(clean_headers) if 'time' in h)]
                        node_h = actual_headers[next(i for i, h in enumerate(clean_headers) if 'channel' in h or 'node' in h)]
                        temp_h = [h for h in actual_headers if 'temp' in h.lower()][0]
                        df_processed['timestamp'] = pd.to_datetime(df_raw[time_h], format='mixed')
                        df_processed['NodeNum'] = df_raw[node_h].str.strip().str.replace(':', '-')
                        df_processed['temperature'] = pd.to_numeric(df_raw[temp_h], errors='coerce')

                    # BRANCH C: SensorPush
                    else:
                        st.info("Detected Format: SensorPush")
                        t_match = [h for h in actual_headers if 'timestamp' in h.lower()][0]
                        v_match = [h for h in actual_headers if 'temp' in h.lower()][0]
                        match = re.search(r'^([^ \(\.]+)', u_file.name)
                        df_processed['timestamp'] = pd.to_datetime(df_raw[t_match], format='mixed')
                        df_processed['temperature'] = pd.to_numeric(df_raw[v_match], errors='coerce')
                        df_processed['NodeNum'] = match.group(1) if match else "Unknown"

                    # 3. DB COMMIT
                    if not df_processed.empty:
                        df_processed = df_processed.dropna(subset=['timestamp', 'temperature'])
                        st.success(f"✅ Prepared {len(df_processed)} records for Node(s): {', '.join(df_processed['NodeNum'].unique())}")
                        
                        is_lord = "-" in str(df_processed['NodeNum'].iloc[0])
                        target_table = "raw_lord" if is_lord else "raw_sensorpush"
                        
                        if st.button(f"🚀 Upload to {target_table}"):
                            with st.spinner("Writing to BigQuery..."):
                                table_id = f"{PROJECT_ID}.{DATASET_ID}.{target_table}"
                                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                                client.load_table_from_dataframe(df_processed, table_id, job_config=job_config).result()
                                st.success("Upload Complete!")
                                st.cache_data.clear() 

            except Exception as e:
                st.error(f"Ingestion Failed: {e}")

    # --- TAB 2: EXPORT LOGIC ---
    with tab_export:
        st.subheader("📥 Wide-Format Data Export")
        if not selected_project or selected_project == "All Projects":
            st.warning("⚠️ Select a specific project in the sidebar to export data.")
        else:
            c1, c2 = st.columns(2)
            e_start = c1.date_input("Start Date", value=datetime.now() - timedelta(days=30))
            e_end = c2.date_input("End Date", value=datetime.now())
            
            with st.spinner("Processing engineering records..."):
                full_df = get_universal_portal_data(selected_project, view_mode="engineering")
            
            if not full_df.empty:
                all_locs = sorted(full_df['Location'].unique().tolist())
                selected_locs = st.multiselect("Filter by Location (Leave empty for ALL)", options=all_locs)

                mask = (full_df['timestamp'].dt.date >= e_start) & (full_df['timestamp'].dt.date <= e_end)
                if selected_locs:
                    mask = mask & (full_df['Location'].isin(selected_locs))
                
                export_df = full_df.loc[mask].copy()
                
                if export_df.empty:
                    st.warning("No data found for the selected criteria.")
                else:
                    export_df['Sensor'] = export_df['Location'] + " (" + export_df['NodeNum'].astype(str) + ")"
                    
                    # Pivot Long to Wide
                    wide_df = export_df.pivot_table(
                        index='timestamp', columns='Sensor', values='temperature', aggfunc='first'
                    ).reset_index()

                    wide_df['timestamp'] = wide_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

                    st.success(f"Report Ready: {len(wide_df.columns)-1} columns generated.")
                    csv_data = wide_df.to_csv(index=False).encode('utf-8')
                    
                    st.download_button(
                        label="💾 Download Custom CSV Export",
                        data=csv_data,
                        file_name=f"{selected_project}_Export_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                        
###########
# - 10. PAGE: ADMIN TOOLS - #
###########
def render_admin_page(selected_project, display_tz, unit_mode, unit_label, active_refs):
    """
    Advanced Admin Tools: Transactional Node Logistics, 
    Bulk Staging, Project Management, Ref Curve Library, and Maintenance.
    """
    st.header("🛠️ Admin Tools")
    
    client = get_bq_client()
    if client is None: 
        st.error("Database connection unavailable.")
        return

    # 1. GLOBAL DATA FETCH
    try:
        reg_q = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.node_registry`"
        full_reg_df = client.query(reg_q).to_dataframe()
        
        proj_reg_q = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.project_registry`"
        proj_reg_df = client.query(proj_reg_q).to_dataframe()

        # Fetch Reference Curve List for dropdowns
        try:
            lib_df = client.query(f"SELECT DISTINCT CurveID FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves`").to_dataframe()
            available_curves = ["None"] + sorted(lib_df['CurveID'].tolist())
        except:
            available_curves = ["None"]
    except Exception as e:
        st.error(f"Registry Link Offline: {e}")
        return

    # 2. NAVIGATION TABS
    tab_bulk, tab_logistics, tab_project, tab_ref_library, tab_scrub, tab_surgical = st.tabs([
        "✅ Bulk Approval", "📋 Node Logistics", "⚙️ Project Master", "📈 Ref Curve Library", "🧹 Maintenance", "🧨 Surgical"
    ])

    # --- TAB 2: NODE LOGISTICS ---
    with tab_logistics:
        reg_mode = st.radio("Logistics Mode", ["Search & Manage", "Bulk CSV Upload", "Global Status Audit"], horizontal=True)

        if reg_mode == "Search & Manage":
            search_id = st.text_input("🔍 Find Node (Enter NodeNum or Physical ID)")
            if search_id:
                # Filter records for this node
                matches = full_reg_df[
                    (full_reg_df['NodeNum'] == search_id) | 
                    (full_reg_df['PhysicalID'].astype(str) == search_id)
                ].sort_values('Start_Date', ascending=False)

                if not matches.empty:
                    options = matches.apply(lambda r: f"{r['Project']} | {r['Location']} (Start: {r['Start_Date']})", axis=1).tolist()
                    selection = st.selectbox("Select specific assignment to manage:", options)
                    row = matches.iloc[options.index(selection)]
                    
                    st.divider()
                    
                    # --- FORM: EDIT / RE-ASSIGN ---
                    with st.form("surgical_node_edit_form_v2"):
                        st.subheader("📝 Edit Assignment")
                        c1, c2 = st.columns(2)
                        u_proj = c1.text_input("Project", value=str(row['Project']))
                        u_loc = c2.text_input("Location", value=str(row['Location']))
                        u_bank = c1.text_input("Bank", value=str(row['Bank']) if pd.notnull(row['Bank']) else "")
                        u_depth = c2.number_input("Depth (ft)", value=float(row['Depth']) if pd.notnull(row['Depth']) else 0.0)
                        
                        d1, d2 = st.columns(2)
                        
                        # FIX: Handle NaT/Null dates to prevent ValueError
                        raw_start = pd.to_datetime(row['Start_Date'])
                        default_start = raw_start.date() if pd.notnull(raw_start) else datetime.now().date()
                        u_start = d1.date_input("Start Date", value=default_start)
                        
                        raw_end = pd.to_datetime(row['End_Date'])
                        is_retired = pd.notnull(raw_end)
                        default_end = raw_end.date() if is_retired else datetime.now().date()
                        u_end = d2.date_input("End Date", value=default_end)
                        apply_end = d2.checkbox("Apply/Active End Date", value=is_retired)

                        # FIX: Safe Status Indexing
                        status_list = ["Active", "Diagnostic", "Available", "Need Repair", "Dead"]
                        current_stat = str(row['SensorStatus']).strip()
                        default_idx = status_list.index(current_stat) if current_stat in status_list else 0
                        u_stat = st.selectbox("Status", status_list, index=default_idx)
                        
                        op_type = st.radio("Update Strategy", 
                            ["Correction (Overwrite this record)", "Re-assignment (Retire this, start new)"],
                            help="Correction: Use to fix typos. Re-assignment: Use when moving physical hardware.")

                        # FIX: Button is now inside the form block
                        submit_save = st.form_submit_button("💾 Save Registry Update", use_container_width=True)

                    if submit_save:
                        today = datetime.now().strftime('%Y-%m-%d')
                        end_val = f"'{u_end}'" if apply_end else "NULL"
                        
                        if "Correction" in op_type:
                            sql = f"""
                                UPDATE `{PROJECT_ID}.{DATASET_ID}.node_registry` 
                                SET Project='{u_proj}', Location='{u_loc}', Bank='{u_bank}', 
                                    Depth={u_depth}, SensorStatus='{u_stat}', 
                                    Start_Date='{u_start}', End_Date={end_val}
                                WHERE NodeNum='{row['NodeNum']}' AND Project='{row['Project']}' 
                                AND Start_Date='{row['Start_Date']}'
                            """
                        else:
                            sql = f"""
                                BEGIN TRANSACTION;
                                UPDATE `{PROJECT_ID}.{DATASET_ID}.node_registry` SET End_Date='{today}' 
                                WHERE NodeNum='{row['NodeNum']}' AND Project='{row['Project']}' AND End_Date IS NULL;
                                INSERT INTO `{PROJECT_ID}.{DATASET_ID}.node_registry` 
                                (NodeNum, PhysicalID, Project, Location, Bank, Depth, Start_Date, SensorStatus)
                                VALUES ('{row['NodeNum']}', {row['PhysicalID']}, '{u_proj}', '{u_loc}', '{u_bank}', {u_depth}, '{today}', '{u_stat}');
                                COMMIT;
                            """
                        client.query(sql).result()
                        st.success("Success! Registry updated.")
                        st.cache_data.clear()
                        st.rerun()

                    # --- DANGER ZONE (Outside Form) ---
                    st.divider()
                    with st.expander("🧨 Danger Zone: Delete Entry"):
                        confirm_delete = st.checkbox(f"Confirm permanent DELETE for {row['NodeNum']}")
                        if st.button("🗑️ Permanently Delete Record", type="primary", disabled=not confirm_delete):
                            delete_sql = f"""
                                DELETE FROM `{PROJECT_ID}.{DATASET_ID}.node_registry` 
                                WHERE NodeNum='{row['NodeNum']}' AND Project='{row['Project']}' 
                                AND Start_Date='{row['Start_Date']}'
                            """
                            client.query(delete_sql).result()
                            st.success("Record deleted.")
                            st.cache_data.clear()
                            st.rerun()
                else:
                    st.info("No records found for this ID.")

        elif reg_mode == "Bulk CSV Upload":
            st.write("Upload CSV with: `NodeNum`, `PhysicalID`, `Project`, `Location`, `Bank`, `Depth`")
            u_csv = st.file_uploader("Upload Node CSV", type="csv")
            if u_csv:
                up_df = pd.read_csv(u_csv)
                active_nodes = full_reg_df[full_reg_df['End_Date'].isna()]['NodeNum'].tolist()
                conflicts = up_df[up_df['NodeNum'].isin(active_nodes)]
                
                if not conflicts.empty:
                    st.warning(f"⚠️ {len(conflicts)} nodes in CSV are currently active. Uploading will set an End Date.")
                    st.dataframe(conflicts, hide_index=True)
                
                if st.button("🚀 Process Bulk Re-assignment"):
                    today = datetime.now().strftime('%Y-%m-%d')
                    for _, r in up_df.iterrows():
                        client.query(f"UPDATE `{PROJECT_ID}.{DATASET_ID}.node_registry` SET End_Date='{today}' WHERE NodeNum='{r['NodeNum']}' AND End_Date IS NULL").result()
                        ins_sql = f"""INSERT INTO `{PROJECT_ID}.{DATASET_ID}.node_registry` 
                                      (NodeNum, PhysicalID, Project, Location, Bank, Depth, Start_Date, SensorStatus)
                                      VALUES ('{r['NodeNum']}', {r['PhysicalID']}, '{r['Project']}', '{r['Location']}', '{r['Bank']}', {r['Depth']}, '{today}', 'Active')"""
                        client.query(ins_sql).result()
                    st.success("Bulk update processed.")
                    st.cache_data.clear()

        # --- Inside render_admin_page under Global Status Audit ---
        elif reg_mode == "Global Status Audit":
            st.subheader("📊 Hardware Inventory")
            
            # Ensure we handle nulls and whitespace in the Status column
            available_stats = [str(s).strip() for s in full_reg_df['SensorStatus'].unique() if pd.notnull(s)]
            
            f1, f2 = st.columns(2)
            
            # FIX: Only use defaults that actually exist in the available_stats list
            initial_defaults = [s for s in ["Active", "Diagnostic"] if s in available_stats]
            
            sel_stats = f1.multiselect(
                "Filter Status", 
                options=available_stats, 
                default=initial_defaults
            )
            
            active_only = f2.checkbox("Show Only Active Assignments", value=True)
            
            view_df = full_reg_df.copy()
            if sel_stats:
                # Use .str.strip() to match our cleaned list
                view_df = view_df[view_df['SensorStatus'].str.strip().isin(sel_stats)]
            if active_only:
                view_df = view_df[view_df['End_Date'].isna()]
            
            st.dataframe(
                view_df.sort_values(['Project', 'Location', 'Depth']), 
                use_container_width=True, 
                hide_index=True
            )

    # --- TAB 1: BULK APPROVAL ---
    with tab_bulk:
        st.subheader("✅ Range-Based Bulk Approval")
        active_locs = sorted(full_reg_df[full_reg_df['Project'] == selected_project]['Location'].unique())
        sel_loc = st.selectbox("Target Location", ["All Locations"] + active_locs)
        c1, c2 = st.columns(2)
        b_s = c1.date_input("Start Date Select", value=datetime.now() - timedelta(days=7))
        b_e = c2.date_input("End Date Select", value=datetime.now())
        
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
            st.success("Batch approval successful.")
            st.cache_data.clear()

    # --- TAB 3: PROJECT MASTER ---
    with tab_project:
        st.subheader("⚙️ Project Lifecycle")
        action = st.radio("Action", ["Overview", "New Project", "Update Existing"], horizontal=True)
        
        if action == "Update Existing":
            target = st.selectbox("Select Project to Edit", sorted(proj_reg_df['Project'].unique()))
            p_data = proj_reg_df[proj_reg_df['Project'] == target].iloc[0]
            
            with st.form("edit_p_form_final_v3"):
                # --- SAFE STATUS LOOKUP ---
                status_options = ["Initialized", "Pre-freeze", "Freezedown", "Maintenance", "Archived"]
                current_status = str(p_data.get('ProjectStatus', 'Initialized'))
                
                # If the status in the DB isn't in our list, default to index 0
                try:
                    status_index = status_options.index(current_status)
                except ValueError:
                    status_index = 0
                
                u_status = st.selectbox("Status", status_options, index=status_index)
                # --------------------------
                
                # Fetch available curves for the dropdown
                try:
                    curve_list_df = client.query(f"SELECT DISTINCT CurveID FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves`").to_dataframe()
                    available_curves = ["None"] + sorted(curve_list_df['CurveID'].tolist())
                except:
                    available_curves = ["None"]
                
                # SAFE SOIL LOOKUP
                current_soil = p_data.get('SoilType', 'None')
                try:
                    soil_index = available_curves.index(current_soil)
                except ValueError:
                    soil_index = 0

                u_soil = st.selectbox("Assigned Soil Reference", available_curves, index=soil_index)
                
                u_eng = st.text_area("Engineering Notes", value=p_data.get('EngNotes', ''))
                
                if st.form_submit_button("💾 Save Project Settings"):
                    # ... (rest of your save logic)
                    # Logic to set Day 0 automatically when status moves to Freezedown
                    date_sql = ", Date_Freezedown = CURRENT_DATE()" if u_status == "Freezedown" and pd.isnull(p_data.get('Date_Freezedown')) else ""
                    
                    sql = f"""
                        UPDATE `{PROJECT_ID}.{DATASET_ID}.project_registry` 
                        SET ProjectStatus='{u_status}', SoilType='{u_soil}', EngNotes='{u_eng}' {date_sql} 
                        WHERE Project='{target}'
                    """
                    client.query(sql).result()
                    st.success("Project updated.")
                    st.cache_data.clear()
                    st.rerun()

   # --- TAB: REFERENCE CURVE LIBRARY ---
    with tab_ref_library:
        st.subheader("📚 Theoretical Curve Library")
        st.write("Manage the target temperature curves used for visual goal-tracking on graphs.")
        
        # 1. MANAGEMENT & PURGE TOOLS
        # Using an expander to keep the UI clean and prevent accidental "Nuclear" purges.
        with st.expander("🗑️ Library Management (Delete/Purge)", expanded=False):
            st.warning("Action is permanent. Purging will remove curves from all graphs.")
            
            # A. SURGICAL DELETE (Specific Curve)
            try:
                # Fetch distinct CurveIDs for the dropdown
                lib_df = client.query(f"SELECT DISTINCT CurveID FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves`").to_dataframe()
                
                if not lib_df.empty:
                    to_delete = st.selectbox("Select Curve to Remove", sorted(lib_df['CurveID'].tolist()), key="delete_curve_picker")
                    if st.button(f"🗑️ Delete {to_delete}", type="secondary", key="delete_single_curve_btn"):
                        client.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` WHERE CurveID='{to_delete}'").result()
                        st.success(f"Removed {to_delete} from library.")
                        st.cache_data.clear()
                        time.sleep(0.5)
                        st.rerun()
                else:
                    st.info("No curves available to delete.")
            except Exception:
                st.info("Reference table is empty or not yet initialized.")

            st.divider()

            # B. NUCLEAR PURGE (Delete All)
            st.error("Danger: This wipes the entire reference database.")
            confirm_purge = st.checkbox("I confirm I want to DELETE ALL curves in the library.", key="confirm_purge_check")
            if st.button("🧨 PURGE ENTIRE LIBRARY", type="primary", disabled=not confirm_purge, key="nuclear_purge_btn"):
                try:
                    # TRUNCATE is the standard way to wipe a table in BigQuery
                    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.reference_curves`").result()
                    st.success("Library has been completely purged.")
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    st.error(f"Purge failed: {e}")

        st.divider()

        # 2. THE UPLOADER
        # This section handles the ingestion of CSV files (Skip 2 rows, Col 1: Day, Col 2: Temp)
        st.write("### 📤 Upload New Curves")
        st.caption("Expected Format: CSV files (e.g., `2527-TP1.csv`). Data should start on Row 3. Col 1: Day, Col 2: Temp.")
        
        u_files = st.file_uploader(
            "Select CSV Files", 
            type="csv", 
            accept_multiple_files=True, 
            key="ref_uploader_v6" 
        )
        
        if u_files:
            if st.button("💾 Commit Files to BigQuery", key="commit_ref_btn_final", use_container_width=True):
                progress_bar = st.progress(0)
                
                for idx, f in enumerate(u_files):
                    try:
                        # Extract CurveID from the filename (e.g., '2527-TP1.csv' -> '2527-TP1')
                        curve_id = f.name.replace(".csv", "")
                        
                        # Handle encoding variants (standard for many sensor exports)
                        try:
                            f.seek(0)
                            ref_df = pd.read_csv(f, skiprows=2, names=['Day', 'Temp'], encoding='utf-8')
                        except Exception:
                            f.seek(0)
                            ref_df = pd.read_csv(f, skiprows=2, names=['Day', 'Temp'], encoding='latin-1')

                        # Data Cleaning: Convert to numeric and drop invalid rows
                        ref_df['Day'] = pd.to_numeric(ref_df['Day'], errors='coerce')
                        ref_df['Temp'] = pd.to_numeric(ref_df['Temp'], errors='coerce')
                        ref_df = ref_df.dropna(subset=['Day', 'Temp'])
                        ref_df['CurveID'] = curve_id

                        if not ref_df.empty:
                            # 1. Clean out the old version of this specific curve to avoid data stacking
                            client.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` WHERE CurveID='{curve_id}'").result()
                            
                            # 2. Load the new cleaned data to BigQuery
                            table_ref = f"{PROJECT_ID}.{DATASET_ID}.reference_curves"
                            client.load_table_from_dataframe(ref_df, table_ref).result()
                            
                            st.toast(f"Success: {curve_id}", icon="✅")
                        else:
                            st.error(f"❌ {f.name} contained no valid numeric data after row 2.")
                            
                        progress_bar.progress((idx + 1) / len(u_files))
                        
                    except Exception as e:
                        st.error(f"❌ Error processing {f.name}: {e}")
                
                st.success("Library Processing Complete.")
                st.cache_data.clear()
                time.sleep(1)
                st.rerun()

        # 3. CURRENT INVENTORY VIEW
        st.divider()
        st.write("### 📂 Current Library Inventory")
        try:
            # Query the table to show a summary of what's stored
            inventory_df = client.query(
                f"SELECT CurveID, COUNT(*) as Data_Points, MIN(Day) as Start_Day, MAX(Day) as End_Day "
                f"FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` "
                f"GROUP BY CurveID ORDER BY CurveID"
            ).to_dataframe()
            
            if not inventory_df.empty:
                st.dataframe(inventory_df, use_container_width=True, hide_index=True)
            else:
                st.info("The library table is currently empty.")
        except Exception:
            st.warning("⚠️ Reference table (`reference_curves`) not found in BigQuery.")
        
    # --- TAB 4: MAINTENANCE ---
    with tab_scrub:
        st.subheader("🧹 Database Maintenance")
        target_tbl = st.radio("Target Source", ["SensorPush", "Lord"], horizontal=True)
        if st.button("🧨 Run Hourly Compression"):
            path = f"{PROJECT_ID}.{DATASET_ID}.raw_{target_tbl.lower()}"
            sql = f"CREATE OR REPLACE TABLE `{path}` AS SELECT TIMESTAMP_TRUNC(timestamp, HOUR) as timestamp, NodeNum, AVG(temperature) as temperature FROM `{path}` GROUP BY 1, 2"
            client.query(sql).result()
            st.success("Cleanup Complete.")
            st.cache_data.clear()

    # --- TAB 5: SURGICAL ---
    with tab_surgical:
        render_surgical_cleaner(selected_project, display_tz, unit_mode, unit_label)


######################################
# - 11. SURGICAL CLEANER FUNCTIONS - #
######################################

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
            SELECT 
                COALESCE(CAST(rej.approve AS STRING), 'PENDING') as status, 
                COUNT(*) as point_count
            FROM (
                SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` 
                UNION ALL 
                SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
            ) AS r
            INNER JOIN `{PROJECT_ID}.{DATASET_ID}.node_registry` AS n ON r.NodeNum = n.NodeNum
            LEFT JOIN `{OVERRIDE_TABLE}` AS rej 
                ON r.NodeNum = rej.NodeNum 
                AND TIMESTAMP_TRUNC(r.timestamp, HOUR) = rej.timestamp
            WHERE {where_clause} 
            AND r.timestamp BETWEEN '{s_str}' AND '{e_str}'
            {threshold_clause}
            GROUP BY 1
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
####################################
# - 11. SURGICAL CLEANER HELPERS - #
####################################

def update_records(pts, df, val, display_tz):
    """
    Writes status updates (TRUE, FALSE, MASKED) to the manual_rejections table.
    Ensures timezone alignment so clicked points match database timestamps.
    """
    import time # Required for the feedback pause
    
    # 1. INITIALIZE CLIENT
    client = get_bq_client()
    if client is None: 
        st.error("Database connection unavailable.")
        return

    recs = []
    for p in pts:
        try:
            # 2. CAPTURE & ALIGN TIMESTAMP
            # The click event returns a string 'x' representing the time on the graph
            ts_raw = pd.to_datetime(p['x'])
            
            # If the user is viewing in local time (Pacific/Eastern), 
            # we must convert it back to UTC for BigQuery.
            if ts_raw.tzinfo is None:
                # Graph was localized but the string lost the tzinfo
                ts = ts_raw.tz_localize(display_tz).tz_convert('UTC').floor('h')
            else:
                # String kept the tzinfo, just convert to UTC
                ts = ts_raw.tz_convert('UTC').floor('h')
            
            # 3. EXTRACT METADATA
            # Use the point index from the click to find the exact Node ID in the dataframe
            node = df.iloc[p['point_index']]['NodeNum']
            
            recs.append({
                "NodeNum": str(node), 
                "timestamp": ts, 
                "approve": val 
            })
        except Exception:
            # Skip points that don't match expected formats (e.g. clicking legend)
            continue
    
    if recs:
        # 4. PREPARE DATAFRAME
        # Remove duplicates to avoid writing the same point twice in one click
        status_df = pd.DataFrame(recs).drop_duplicates(subset=['NodeNum', 'timestamp'])
        
        try:
            # 5. EXECUTE BIGQUERY APPEND
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            job = client.load_table_from_dataframe(
                status_df, 
                OVERRIDE_TABLE, 
                job_config=job_config
            )
            job.result() # Wait for the upload to finish
            
            # 6. UI RESET & CACHE CLEAR
            # Clear the visual selection in Streamlit state
            if "locked_selection" in st.session_state:
                st.session_state.locked_selection = []
            
            # CRITICAL: Clear cache so the graphs instantly reflect the new status
            st.cache_data.clear() 
            
            st.success(f"✅ Successfully marked {len(status_df)} records as {val}")
            
            # Wait briefly so the user sees the success message before refresh
            time.sleep(0.6) 
            st.rerun()
            
        except Exception as e:
            st.error(f"❌ Failed to update override table: {e}")

#####################
# Dashboard Summary #
#####################
def render_summary_dashboard(unit_label, unit_mode, display_tz):
    """
    The main Dashboard. Shows active project health, 
    temperature trends, 24h extremes, and staleness alerts.
    """
    st.header("🌐 Global Project Summary")
    
    client = get_bq_client()
    if client is None: return

    # Optimized Query: Pulls 48h of data for all non-archived projects
    summary_q = f"""
        WITH active_projects AS (
            SELECT Project, ProjectName, ProjectStatus, Date_Freezedown
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
            p.Project, p.ProjectName, p.ProjectStatus, p.Date_Freezedown,
            ld.Bank, ld.Location, ld.Depth,
            AVG(CASE WHEN ld.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN ld.temperature END) as avg_now,
            AVG(CASE WHEN ld.timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN ld.temperature END) as avg_1h,
            AVG(CASE WHEN ld.timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR) THEN ld.temperature END) as avg_6h,
            AVG(CASE WHEN ld.timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 25 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN ld.temperature END) as avg_24h,
            
            -- EXTREMES (Restored)
            MIN(CASE WHEN ld.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN ld.temperature END) as min_24h,
            MAX(CASE WHEN ld.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN ld.temperature END) as max_24h,
            
            -- STALE FALLBACK
            ARRAY_AGG(ld.temperature ORDER BY ld.timestamp DESC LIMIT 1)[OFFSET(0)] as latest_temp,
            MAX(ld.timestamp) as latest_ts
        FROM active_projects p
        LEFT JOIN raw_data ld ON p.Project = ld.Project
        GROUP BY 1, 2, 3, 4, 5, 6, 7
    """
    
    try:
        df = client.query(summary_q).to_dataframe()
        df[['Bank', 'Location']] = df[['Bank', 'Location']].fillna('')
    except Exception as e:
        st.error(f"Dashboard Query Failed: {e}")
        return

    if df.empty:
        st.warning("No active projects found with data in the last 48 hours.")
        return

    now_utc = pd.Timestamp.now(tz='UTC')

    for project in sorted(df['Project'].unique()):
        p_df = df[df['Project'] == project]
        p_name = p_df['ProjectName'].iloc[0] or project
        
        f_date = p_df['Date_Freezedown'].iloc[0]
        day_text = ""
        if pd.notnull(f_date):
            days = (pd.Timestamp.now(tz=display_tz).date() - pd.to_datetime(f_date).date()).days
            day_text = f"🗓️ **Day {max(0, days)} of Freezedown**"
        
        with st.container(border=True):
            st.subheader(f"🏗️ {p_name}")
            if day_text: st.markdown(day_text)
            st.caption(f"Status: {p_df['ProjectStatus'].iloc[0]}")
            
            st.divider()
            cols = st.columns(4)
            
            # Classification Logic
            is_amb = p_df['Bank'].str.contains('Amb', case=False) | p_df['Location'].str.contains('Amb', case=False)
            is_s = (p_df['Bank'].str.startswith('S') | p_df['Location'].str.startswith('S')) & ~is_amb
            is_r = (p_df['Bank'].str.startswith('R') | p_df['Location'].str.startswith('R')) & ~is_amb
            is_tp = p_df['Depth'].notnull() & ~is_s & ~is_r & ~is_amb

            groups = [(cols[0], "📥 Supply", p_df[is_s]), (cols[1], "📤 Return", p_df[is_r]), 
                      (cols[2], "📏 TempPipes", p_df[is_tp]), (cols[3], "☁️ Ambient", p_df[is_amb])]
            
            for col, title, g_df in groups:
                with col:
                    st.markdown(f"#### {title}")
                    if g_df.empty:
                        st.caption("No recent data")
                        continue
                    
                    # 1. Logic for Current vs. Stale Fallback
                    avg_now = g_df['avg_now'].mean()
                    latest_val = g_df['latest_temp'].mean()
                    latest_time = g_df['latest_ts'].max()
                    
                    # Calculate Lag
                    ts_check = latest_time if latest_time.tzinfo else latest_time.tz_localize('UTC')
                    lag_hrs = (now_utc - ts_check).total_seconds() / 3600

                    is_stale = pd.isnull(avg_now)
                    val = latest_val if is_stale else avg_now
                    
                    # 24h Extremes
                    mn_24 = g_df['min_24h'].min()
                    mx_24 = g_df['max_24h'].max()
                    
                    # Unit Conversion
                    if unit_mode == "Celsius":
                        val = (val - 32) * 5/9 if pd.notnull(val) else None
                        mn_24 = (mn_24 - 32) * 5/9 if pd.notnull(mn_24) else None
                        mx_24 = (mx_24 - 32) * 5/9 if pd.notnull(mx_24) else None
                    
                    # 2. Rendering Metric
                    st.metric("Avg", f"{val:.1f}{unit_label}")
                    
                    if is_stale and pd.notnull(lag_hrs):
                        st.warning(f"🕒 {int(lag_hrs)}h ago")

                    if pd.notnull(mn_24) and pd.notnull(mx_24):
                        st.caption(f"Range: {mn_24:.1f} to {mx_24:.1f}{unit_label}")
                    
                    # 3. Trends
                    t_row = st.columns(3)
                    t_row[0].caption(f"1h\n{get_trend_arrow(val, g_df['avg_1h'].mean())}")
                    t_row[1].caption(f"6h\n{get_trend_arrow(val, g_df['avg_6h'].mean())}")
                    t_row[2].caption(f"24h\n{get_trend_arrow(val, g_df['avg_24h'].mean())}")

def get_trend_arrow(current, previous):
    """Helper to generate trend icons with updated blue downward arrow."""
    if pd.isnull(current) or pd.isnull(previous): return "N/A"
    delta = current - previous
    if delta > 0.1: return f"🔺 +{delta:.1f}"
    if delta < -0.1: return f"🔹 {delta:.1f}" # This renders as a blue diamond/square in many fonts, often used for blue down in Streamlit
    return "➡️ 0.0"


###################
# 12. MAIN ROUTER #
###################
# --- MAIN ROUTING LOGIC ---
# Initialize the DB Client one time for the main execution
client = get_bq_client() 

if page == "Summary":
    render_summary_dashboard(unit_label, unit_mode, display_tz)

elif page == "Time vs Temp":
    # Pass the metadata dictionary from session state
    render_global_overview(
        selected_project, 
        st.session_state.get('project_metadata'), 
        display_tz
    ) 

elif page == "Sensor Status":
    # Ensure this function exists in your script or is defined
    try:
        render_sensor_status(client, selected_project, unit_label, unit_mode, display_tz)
    except NameError:
        st.warning("Sensor Status module is currently being updated.")

elif page == "Depth Charts":
    render_depth_charts(selected_project, unit_label, display_tz)

elif page == "Node Diagnostics":
    render_node_diagnostics(selected_project, display_tz, unit_label)

elif page == "Client Portal":
    render_client_portal(
        selected_project, 
        st.session_state.get('project_metadata'), 
        display_tz, 
        unit_mode, 
        unit_label, 
        active_refs
    )

# --- PASSWORD PROTECTED SECTIONS ---
elif page in ["Data Intake Lab", "Admin Tools"]:
    # Check if user is already authenticated
    if st.session_state.get('authenticated', False):
        if page == "Data Intake Lab":
            render_data_intake_page(selected_project)
        else:
            render_admin_page(
                selected_project, 
                display_tz, 
                unit_mode, 
                unit_label, 
                active_refs
            )
    else:
        # Display the login gate
        st.divider()
        c1, c2, c3 = st.columns([1, 2, 1])
        with c2:
            st.subheader("🔐 Restricted Admin Access")
            pwd = st.text_input("Enter Admin Password", type="password")
            if st.button("Unlock Dashboard", use_container_width=True):
                if pwd == st.secrets["admin_password"]:
                    st.session_state['authenticated'] = True
                    st.rerun()
                else:
                    st.error("Invalid Password. Access Denied.")
