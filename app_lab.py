import streamlit as st
import pandas as pd
import time
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta, timezone, time as dt_time
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
        
        # Fallback for local development
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
        # Engineering sees ALL historical data
        visibility_sql = ""

    # Using a safer multi-line string with structured logic
    query = f"""
        SELECT m.* FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
        JOIN `{PROJECT_ID}.{DATASET_ID}.project_registry` p ON m.Project = p.Project
        WHERE m.Project = @project_id
        {visibility_sql}
        {filter_sql}
        ORDER BY m.Location ASC, m.timestamp ASC
    """
    
    # Secure Parameterized Query
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
        # Log the full error for debugging in the console
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
# Using session_state for page to allow for programmatic redirects if needed later
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
# We initialize these as None/Default to prevent undefined variable errors in the router
selected_project = "All Projects"
project_metadata = None  

sidebar_client = get_bq_client()

if sidebar_client is not None:
    try:
        # Fetching names from project_registry - we exclude Archived projects by default
        proj_q = f"""
            SELECT Project, ProjectName, Timezone, ProjectStatus, Date_Freezedown 
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
        
        # Keep global state in sync
        st.session_state['selected_project'] = selected_project
        
        if selected_project != "All Projects":
            # Extract metadata for the selected project
            meta_row = proj_df[proj_df['Project'] == selected_project]
            if not meta_row.empty:
                # Convert to dictionary for easier handling in functions
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

# Smart Default: If the project has a timezone set, use it. Otherwise, default to Pacific.
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

# Store as tuple (immutable) for caching stability
st.session_state["active_refs"] = tuple(active_refs)
# --- END OF SIDEBAR ---

#############
# - Graph - #
#############
def build_high_speed_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label, display_tz="UTC", mobile_mode=False):
    """
    Optimized Graphing Engine: Handles unit conversion, timezone alignment, 
    and status-based styling with high-performance vectorization.
    """
    if df.empty:
        return go.Figure().update_layout(title="No data available for the selected period.")

    plot_df = df.copy()
    
    # 1. TIMEZONE & UNIT CONVERSION
    # Ensure timestamps are timezone-aware UTC before converting to display timezone
    if plot_df['timestamp'].dt.tz is None:
        plot_df['timestamp'] = plot_df['timestamp'].dt.tz_localize('UTC')
    plot_df['timestamp'] = plot_df['timestamp'].dt.tz_convert(display_tz)
    
    # Helper to ensure view-bounds are timezone aligned
    def localize_bound(dt):
        if dt.tzinfo is None:
            return dt.tz_localize('UTC').tz_convert(display_tz)
        return dt.tz_convert(display_tz)

    start_local = localize_bound(start_view)
    end_local = localize_bound(end_view)
    now_local = pd.Timestamp.now(tz=display_tz)
    
    # Range Logic: Don't show empty space before the first actual data point
    actual_min_data = plot_df['timestamp'].min()
    range_start = max(start_local, actual_min_data) - pd.Timedelta(hours=12)
    range_end = end_local + pd.Timedelta(hours=12)
    
    if unit_mode == "Celsius":
        plot_df['temperature'] = (plot_df['temperature'] - 32) * 5/9
        y_range, dt_major, dt_minor = [-30, 30], 10, 5
    else:
        y_range, dt_major, dt_minor = [-20, 80], 10, 5

    # 2. VECTORIZED LABELING & SORTING (High Speed)
    # Avoids .apply() which is slow on large datasets
    plot_df['depth_label'] = "Node " + plot_df['NodeNum'].astype(str)
    plot_df['sort_val'] = 1000.0
    
    # Apply Depth labels if present
    depth_mask = plot_df['Depth'].notnull()
    plot_df.loc[depth_mask, 'depth_label'] = plot_df.loc[depth_mask, 'Depth'].astype(str) + "ft"
    plot_df.loc[depth_mask, 'sort_val'] = pd.to_numeric(plot_df.loc[depth_mask, 'Depth'], errors='coerce')
    
    # Apply Bank labels if present
    bank_mask = plot_df['Bank'].notnull() & (plot_df['Bank'].astype(str).str.strip() != "")
    plot_df.loc[bank_mask & ~depth_mask, 'depth_label'] = "Bank " + plot_df.loc[bank_mask, 'Bank'].astype(str)
    plot_df.loc[bank_mask & ~depth_mask, 'sort_val'] = 999.0
    
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
            
            # --- STATUS-BASED STYLING ---
            status = s_df['SensorStatus'].iloc[0] if 'SensorStatus' in s_df.columns else 'Active'
            line_dash = 'solid' if status == 'Active' else 'dot'
            opacity = 1.0 if status == 'Active' else 0.5
            
            # Gap Handling: break line if gap > 6 hours (Prevents misleading straight lines)
            if not is_surgical:
                s_df['gap'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
                if (s_df['gap'] > 6.0).any():
                    # Injecting None values to force line breaks in Plotly
                    gaps = s_df[s_df['gap'] > 6.0].copy()
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
                line=dict(color=color, width=1.8 if status == 'Active' else 1.0, dash=line_dash),
                marker=dict(size=4, opacity=opacity),
                hovertemplate=f"<b>{group_lbl} ({sn})</b><br>Status: {status}<br>Temp: %{{y:.1f}}{unit_label}<extra></extra>"
            ))

    # 4. REFERENCE LINES & MARKERS
    for val, ref_label in active_refs:
        c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
        fig.add_hline(y=c_val, line_dash="dash", line_color="RoyalBlue", 
                      annotation_text=ref_label, annotation_position="top right", layer="below")

    fig.add_vline(x=now_local, line_width=2, line_color="Red", layer='above', line_dash="dash")

    # 5. LAYOUT & GRID CONFIG
    l_cfg = dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5) if mobile_mode else \
            dict(orientation="v", x=1.02, y=1, xanchor="left", yanchor="top")
    
    m_cfg = dict(t=80, l=40, r=20, b=120) if mobile_mode else dict(t=80, l=50, r=180, b=50)

    fig.update_layout(
        title={'text': f"<b>{title}</b>", 'x': 0.02, 'y': 0.95},
        plot_bgcolor='white', hovermode="x unified", height=600,
        margin=m_cfg, legend=l_cfg,
        xaxis=dict(
            range=[range_start, range_end], showline=True, mirror=True, linecolor='black',
            showgrid=True, dtick="D1", gridcolor='DarkGray', gridwidth=0.5,
            minor=dict(dtick=6*60*60*1000, showgrid=True, gridcolor='Gainsboro', griddash='dash'),
            tickformat='%b %d\n%H:%M'
        ),
        yaxis=dict(
            title=f"Temperature ({unit_label})", range=y_range, dtick=dt_major, 
            gridcolor='DarkGray', showline=True, mirror=True, linecolor='black',
            minor=dict(dtick=dt_minor, showgrid=True, gridcolor='whitesmoke')
        )
    )
    
    # Weekly Markers (Mondays)
    mondays = pd.date_range(start=range_start, end=range_end, freq='W-MON', tz=display_tz)
    for mon in mondays:
        fig.add_vline(x=mon, line_width=1.5, line_color="gray", layer="below")

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
    Includes Dynamic Freezedown Day Tracking and optional Masked Data filtering.
    """
    # 1. HEADER & FREEZEDOWN TRACKER
    p_name = selected_project
    status = "Active"
    f_date_raw = None

    if project_metadata:
        p_name = project_metadata.get('ProjectName', selected_project)
        status = project_metadata.get('ProjectStatus', 'Active')
        f_date_raw = project_metadata.get('Date_Freezedown')

    st.header(f"📈 Time vs Temp: {p_name} [{status}]")
    
    if pd.notnull(f_date_raw):
        try:
            f_start = pd.to_datetime(f_date_raw).date()
            today = pd.Timestamp.now(tz=display_tz).date()
            days_since = (today - f_start).days
            st.markdown(f"### 🗓️ Day **{max(0, days_since)}** of Freezedown")
            st.caption(f"Freezedown began: {f_start.strftime('%B %d, %Y')}")
        except Exception:
            st.caption("⚠️ Error calculating freeze duration.")
    else:
        st.caption("ℹ️ Freeze start date not yet initialized.")

    # 2. UI STATE & PRE-FLIGHT CHECKS
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to view detailed engineering trends.")
        return

    # --- NEW: VISIBILITY SETTINGS ---
    st.sidebar.subheader("👁️ Visibility Settings")
    show_masked = st.sidebar.toggle(
        "Show Masked Points", 
        value=False, 
        help="When OFF, data points marked as 'MASKED' in Admin Tools are hidden."
    )

    mobile_mode = st.session_state.get("mobile_optimized_toggle", False)
    active_refs = st.session_state.get("active_refs", [])
    unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
    unit_label = st.session_state.get("unit_label", "°F")
    
    # 3. DATA FETCHING (Engineering Mode)
    with st.spinner(f"Syncing {p_name} telemetry..."):
        p_df = get_universal_portal_data(selected_project, view_mode="engineering")

    if p_df.empty:
        st.warning(f"No engineering data found for '{p_name}'.")
        return

    # --- 4. MASKING FILTER LOGIC ---
    if not show_masked and 'approve' in p_df.columns:
        # Identify points explicitly marked as MASKED
        masked_points = p_df[p_df['approve'] == 'MASKED']
        if not masked_points.empty:
            p_df = p_df[p_df['approve'] != 'MASKED'].copy()
            st.sidebar.info(f"🧹 Hidden {len(masked_points)} noise spikes.")
        else:
            st.sidebar.caption("✨ Clean Data: No masked points found.")

    # 5. FRESHNESS AUDIT
    last_reading = p_df['timestamp'].max()
    last_reading_utc = last_reading if last_reading.tzinfo else last_reading.tz_localize('UTC')
    now_utc = pd.Timestamp.now(tz='UTC')
    latency_hrs = (now_utc - last_reading_utc).total_seconds() / 3600
    
    if latency_hrs > 24:
        st.error(f"⚠️ **Stale Data Warning:** Last packet received {int(latency_hrs)} hours ago.")

    # 6. TIMELINE CONFIGURATION
    st.sidebar.subheader("📅 Timeline Controls")
    lookback = st.sidebar.slider("Lookback (Weeks)", 0, 52, 4, key="global_lookback_slider")
    
    now_local = pd.Timestamp.now(tz=display_tz)
    end_view = (now_local + pd.Timedelta(days=(7 - now_local.weekday()) % 7 or 7)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    
    if lookback == 0:
        start_view = p_df['timestamp'].min()
    else:
        start_view = end_view - timedelta(weeks=lookback)

    # 7. LOCATION-BASED PLOTTING
    locations = sorted([str(loc) for loc in p_df['Location'].dropna().unique()])
    
    for loc in locations:
        with st.expander(f"📍 Location: {loc}", expanded=True):
            loc_df = p_df[p_df['Location'] == loc].copy()
            
            fig = build_high_speed_graph(
                df=loc_df, 
                title=f"Thermal Trends: {loc}", 
                start_view=start_view, 
                end_view=end_view, 
                active_refs=active_refs, 
                unit_mode=unit_mode, 
                unit_label=unit_label, 
                display_tz=display_tz,
                mobile_mode=mobile_mode 
            )
            
            st.plotly_chart(
                fig, 
                use_container_width=True, 
                key=f"tvt_chart_{selected_project}_{loc}"
            )
        
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
    Visualizes the thermal gradient across soil depths over time.
    """
    st.header(f"📏 Depth Profile Analysis: {selected_project}")
    
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to view depth profiles.")
        return

    # 1. FETCH DATA
    # We use engineering mode to see baselines and bypass the Date_Freezedown mask
    with st.spinner("Fetching historical depth telemetry..."):
        p_df = get_universal_portal_data(selected_project, view_mode="engineering")

    if p_df.empty:
        st.warning("No data found for this project.")
        return

    # 2. PRE-PROCESS DEPTH DATA
    # Convert depth to numeric and drop rows missing critical mapping data
    p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
    depth_df = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
    
    if depth_df.empty:
        st.info("No sensors with valid 'Depth' values found in the Node Registry for this project.")
        return

    # 3. UI CONTROLS
    st.sidebar.subheader("📐 Profile Settings")
    # Number of historical snapshots to overlay
    lookback_weeks = st.sidebar.slider("Historical Snapshots (Weeks)", 1, 24, 8, key="depth_lookback")
    
    unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
    ref_val = 0.0 if unit_mode == "Celsius" else 32.0
    x_range = [-20, 40] if unit_mode == "Celsius" else [-10, 80]

    # 4. GENERATE WEEKLY SNAPSHOTS
    # We look at Monday mornings to provide a consistent 'stable' reading
    now_utc = pd.Timestamp.now(tz='UTC')
    mondays = pd.date_range(end=now_utc, periods=lookback_weeks, freq='W-MON')

    locations = sorted(depth_df['Location'].unique())
    
    for loc in locations:
        with st.expander(f"📍 Location: {loc}", expanded=True):
            loc_data = depth_df[depth_df['Location'] == loc].copy()
            fig = go.Figure()
            
            for m_date in mondays:
                # Target: 6:00 AM on the specific Monday
                target_ts = m_date.replace(hour=6, minute=0, second=0)
                
                # Define a 12-hour window around the target to find the closest reading
                window = loc_data[
                    (loc_data['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & 
                    (loc_data['timestamp'] <= target_ts + pd.Timedelta(hours=12))
                ]
                
                if not window.empty:
                    # Find the single reading closest to our target time for every unique node
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
                        line=dict(shape='spline', smoothing=0.3),
                        marker=dict(size=6),
                        hovertemplate="Depth: %{y}ft<br>Temp: %{x:.1f}" + unit_label
                    ))

            # Add Reference Lines
            fig.add_hline(y=0, line_width=2, line_color="black") # Represents ground level
            fig.add_vline(x=ref_val, line_dash="dash", line_color="RoyalBlue", 
                          annotation_text="Freezing", annotation_position="top right")

            # Determine Y-Axis scale (Depth goes down, so we reverse the range)
            max_depth = depth_df['Depth_Num'].max()
            y_limit = int(((max_depth // 10) + 1) * 10) if pd.notnull(max_depth) else 50

            fig.update_layout(
                title=f"Vertical Thermal Gradient - {loc}",
                plot_bgcolor='white', 
                height=700,
                xaxis=dict(
                    title=f"Temperature ({unit_label})", 
                    range=x_range, 
                    gridcolor='Gainsboro', 
                    showline=True, 
                    linecolor='black'
                ),
                yaxis=dict(
                    title="Depth (ft)", 
                    range=[y_limit, 0], # Reverses the axis so 0 is at the top
                    dtick=5, 
                    gridcolor='Silver', 
                    showline=True, 
                    linecolor='black'
                ),
                legend=dict(orientation="h", y=-0.15, xanchor="center", x=0.5)
            )
            
            st.plotly_chart(fig, use_container_width=True, key=f"depth_chart_{selected_project}_{loc}")


###########
# - 7. PAGE: CLIENT PORTAL - #
###########

def render_client_portal(selected_project, project_metadata, display_tz, unit_mode, unit_label, active_refs):
    """
    Client-facing portal with approved thermal trends and vertical profiles.
    Strictly filters data based on approval status and freeze-down dates.
    """
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to view client data.")
        return

    # 1. DYNAMIC HEADER SECTION
    # Ensure metadata is handled as a dictionary
    meta = project_metadata if isinstance(project_metadata, dict) else {}

    display_name = meta.get('ProjectName', selected_project)
    project_status = meta.get('ProjectStatus', 'Active')
    city = meta.get('City', 'Unknown Location')
    tz_info = meta.get('Timezone', 'UTC')
    
    registry_disclaimer = meta.get('ClientDisclaimer') 
    eng_notes = meta.get('EngNotes')
    asbuilt_filename = meta.get('AsBuiltFile')

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
            st.write(f"**Field Notes:** {eng_notes}")

    # Disclaimer logic
    if pd.notnull(registry_disclaimer) and str(registry_disclaimer).strip() != "":
        st.info(f"ℹ️ {registry_disclaimer}")
    else:
        st.info("ℹ️ Data is typically synchronized once per business day. Readings show approved trends only.")

    # 2. DATA FETCHING (APPROVED ONLY)
    with st.spinner("Synchronizing official records..."):
        p_df = get_universal_portal_data(selected_project, view_mode="client")
    
    if p_df.empty:
        st.warning(f"⚠️ No approved data records available for {display_name} yet.")
        return

    # 3. NAVIGATION TABS
    tab_time, tab_depth, tab_table, tab_built = st.tabs([
        "📈 Timeline Analysis", "📏 Depth Profile", "📋 Summary Table", "🗺️ As-Built Plan"
    ])

    # --- TAB 1: TIMELINE ANALYSIS ---
    with tab_time:
        st.sidebar.subheader("📅 Portal View Options")
        weeks_view = st.sidebar.slider("Timeline Span (Weeks)", 1, 12, 6, key="client_weeks_slider")
        now_utc = pd.Timestamp.now(tz='UTC')
        start_view = now_utc - timedelta(weeks=weeks_view)
        
        locations = sorted([str(loc) for loc in p_df['Location'].dropna().unique()])
        for loc in locations:
            with st.expander(f"📍 {loc}", expanded=(len(locations) == 1)):
                loc_data = p_df[p_df['Location'] == loc].copy()
                fig = build_high_speed_graph(
                    df=loc_data, 
                    title=f"{loc}: 6-Week Thermal Trend", 
                    start_view=start_view, 
                    end_view=now_utc, 
                    active_refs=active_refs, 
                    unit_mode=unit_mode, 
                    unit_label=unit_label, 
                    display_tz=display_tz 
                )
                st.plotly_chart(fig, use_container_width=True, key=f"portal_grid_{loc}")

    # --- TAB 2: DEPTH PROFILE ---
    with tab_depth:
        st.subheader("📏 Vertical Temperature Profile")
        ref_val = 0.0 if unit_mode == "Celsius" else 32.0
        x_range = [-20, 40] if unit_mode == "Celsius" else [-10, 80]

        p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
        depth_only = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
        
        if depth_only.empty:
            st.info("Vertical profile data is not available for this project's sensor configuration.")
        else:
            for loc in sorted(depth_only['Location'].unique()):
                with st.expander(f"📏 {loc} Weekly Profile", expanded=False):
                    loc_data = depth_only[depth_only['Location'] == loc].copy()
                    fig_d = go.Figure()
                    
                    # Last 6 Mondays Snapshots for clear week-over-week growth
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
                        legend=dict(orientation="h", y=-0.2, xanchor="center", x=0.5)
                    )
                    st.plotly_chart(fig_d, use_container_width=True, key=f"portal_depth_{loc}")

    # --- TAB 3: SUMMARY TABLE ---
    with tab_table:
        # Get only the absolute latest reading for each sensor
        latest = p_df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        
        def get_pos(r):
            if pd.notnull(r.get('Depth')): return f"{r['Depth']} ft"
            if pd.notnull(r.get('Bank')): return f"Bank {r['Bank']}"
            return "Surface"

        latest['Position'] = latest.apply(get_pos, axis=1)
        
        # Professional UI Table
        st.dataframe(
            latest[['Location', 'Position', 'temperature', 'timestamp']].sort_values(['Location', 'Position']), 
            use_container_width=True, hide_index=True,
            column_config={
                "temperature": st.column_config.NumberColumn(f"Current Temp ({unit_label})", format="%.1f"),
                "timestamp": st.column_config.DatetimeColumn("Last Sync", format="MM/DD/YY HH:mm")
            }
        )

    # --- TAB 4: AS-BUILT PLAN ---
    with tab_built:
        if pd.notnull(asbuilt_filename) and str(asbuilt_filename).strip() != "":
            # Search for local image file in assets directory
            st.image(f"assets/asbuilts/{asbuilt_filename}", caption=f"Engineering Layout: {display_name}")
        else:
            st.info("The as-built site plan for this project is currently being processed.")

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
            
            return cat, f"{round(diff_mins/60, 1)}h ago"

        df[['Latency_Cat', 'Time_Ago']] = df.apply(lambda x: pd.Series(get_latency_info(x)), axis=1)
        
        # 3. UNIT FORMATTING
        def fmt_temp(val):
            if pd.isnull(val): return "N/A"
            # Using global session state for unit preference
            unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
            c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
            return f"{round(c_val, 1)}{unit_label}"

        # 4. TABLE CONSTRUCTION
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

        # TROUBLESHOOTING SORT: Dead/Stale sensors first
        order = ["❌ Never", "🔴 > 24 Hours", "⏳ < 24 Hours", "🟡 15-60 Mins", "🟢 0-15 Mins"]
        display_df['Connectivity'] = pd.Categorical(display_df['Connectivity'], categories=order, ordered=True)
        display_df = display_df.sort_values(['Connectivity', 'Health', 'Location'])

        st.dataframe(
            display_df, 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "Health": st.column_config.TextColumn(help="Hardware state: Active, Diagnostic, Dead, etc."),
                "Connectivity": st.column_config.TextColumn(help="Time since last data packet received."),
                "Pings (1h)": st.column_config.NumberColumn(help="Target: SensorPush ~1, Lord ~60"),
                "Pings (6h)": st.column_config.NumberColumn(help="Target: SensorPush ~6, Lord ~360"),
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
                        # Extract Node ID from filename (e.g., "12345 (Garage).csv" -> "12345")
                        match = re.search(r'^([^ \(\.]+)', u_file.name)
                        df_processed['timestamp'] = pd.to_datetime(df_raw[t_match], format='mixed')
                        df_processed['temperature'] = pd.to_numeric(df_raw[v_match], errors='coerce')
                        df_processed['NodeNum'] = match.group(1) if match else "Unknown"

                    # 3. DB COMMIT
                    if not df_processed.empty:
                        df_processed = df_processed.dropna(subset=['timestamp', 'temperature'])
                        st.success(f"✅ Prepared {len(df_processed)} records for Node(s): {', '.join(df_processed['NodeNum'].unique())}")
                        
                        # Route to correct table based on ID naming convention
                        is_lord = "-" in str(df_processed['NodeNum'].iloc[0])
                        target_table = "raw_lord" if is_lord else "raw_sensorpush"
                        
                        if st.button(f"🚀 Upload to {target_table}"):
                            with st.spinner("Writing to BigQuery..."):
                                table_id = f"{PROJECT_ID}.{DATASET_ID}.{target_table}"
                                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                                client.load_table_from_dataframe(df_processed, table_id, job_config=job_config).result()
                                
                                st.success("Upload Complete!")
                                st.cache_data.clear() # Force immediate app refresh

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
                # Allow user to prune the export to specific areas
                all_locs = sorted(full_df['Location'].unique().tolist())
                selected_locs = st.multiselect("Filter by Location/Bank (Leave empty for ALL)", options=all_locs)

                # Filter and Pivot
                mask = (full_df['timestamp'].dt.date >= e_start) & (full_df['timestamp'].dt.date <= e_end)
                if selected_locs:
                    mask = mask & (full_df['Location'].isin(selected_locs))
                
                export_df = full_df.loc[mask].copy()
                
                if export_df.empty:
                    st.warning("No data found for the selected criteria.")
                else:
                    # Create clean headers: "Location (NodeID)"
                    export_df['Sensor'] = export_df['Location'] + " (" + export_df['NodeNum'].astype(str) + ")"
                    
                    # Pivot Long to Wide
                    wide_df = export_df.pivot_table(
                        index='timestamp', columns='Sensor', values='temperature', aggfunc='first'
                    ).reset_index()

                    # Excel-safe dates
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
    Bulk Staging, Project Management, and Data Scrubbing.
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
    except Exception as e:
        st.error(f"Registry Link Offline: {e}")
        return

    # 2. NAVIGATION TABS
    tab_bulk, tab_logistics, tab_project, tab_scrub, tab_surgical = st.tabs([
        "✅ Bulk Approval", "📋 Node Logistics", "⚙️ Project Master", "🧹 Maintenance", "🧨 Surgical"
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
        if action == "Overview":
            st.dataframe(proj_reg_df.sort_values('Date_Initialized', ascending=False), use_container_width=True, hide_index=True)
        elif action == "New Project":
            with st.form("new_p_form_final_v2"):
                c1, c2 = st.columns(2)
                n_id = c1.text_input("Project ID")
                n_name = c2.text_input("Project Name")
                n_tz = c2.selectbox("Site Timezone", ["US/Pacific", "US/Eastern", "UTC"])
                if st.form_submit_button("🚀 Create Project"):
                    sql = f"INSERT INTO `{PROJECT_ID}.{DATASET_ID}.project_registry` (Project, ProjectName, Timezone, ProjectStatus, Date_Initialized) VALUES ('{n_id}', '{n_name}', '{n_tz}', 'Initialized', CURRENT_DATE())"
                    client.query(sql).result()
                    st.success("Project created.")
                    st.cache_data.clear()
        elif action == "Update Existing":
            target = st.selectbox("Select Project to Edit", sorted(proj_reg_df['Project'].unique()))
            p_data = proj_reg_df[proj_reg_df['Project'] == target].iloc[0]
            with st.form("edit_p_form_final_v2"):
                u_status = st.selectbox("Status", ["Initialized", "Pre-freeze", "Freezedown", "Maintenance", "Archived"], 
                                      index=["Initialized", "Pre-freeze", "Freezedown", "Maintenance", "Archived"].index(p_data['ProjectStatus']))
                u_eng = st.text_area("Engineering Notes", value=p_data.get('EngNotes', ''))
                if st.form_submit_button("💾 Save Project Settings"):
                    date_sql = ", Date_Freezedown = CURRENT_DATE()" if u_status == "Freezedown" and pd.isnull(p_data.get('Date_Freezedown')) else ""
                    sql = f"UPDATE `{PROJECT_ID}.{DATASET_ID}.project_registry` SET ProjectStatus='{u_status}', EngNotes='{u_eng}' {date_sql} WHERE Project='{target}'"
                    client.query(sql).result()
                    st.success("Project updated.")
                    st.cache_data.clear()

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

# Initialize DB Client
client = get_bq_client() 

if page == "Summary":
    render_summary_dashboard(unit_label, unit_mode, display_tz)

elif page == "Time vs Temp":
    render_global_overview(selected_project, st.session_state.get('project_metadata'), display_tz) 

elif page == "Sensor Status":
    render_sensor_status(client, selected_project, unit_label, unit_mode, display_tz)

elif page == "Depth Charts":
    # Using the unique name we assigned in Section 8
    render_depth_charts(selected_project, unit_label, display_tz)

elif page == "Node Diagnostics":
    render_node_diagnostics(selected_project, display_tz, unit_label)

elif page == "Client Portal":
    render_client_portal(
        selected_project, 
        st.session_state.get('project_metadata'), 
        display_tz, unit_mode, unit_label, active_refs
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
        pwd = st.text_input("Enter Admin Password", type="password")
        if st.button("Unlock"):
            if pwd == st.secrets["admin_password"]:
                st.session_state['authenticated'] = True
                st.rerun()
            else:
                st.error("Invalid Password")
