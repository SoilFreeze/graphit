import streamlit as st
import pandas as pd
import time
import plotly.express as px
import plotly.graph_objects as go  # This defines 'go'
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
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
    try:
        SCOPES = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive"]
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
            return bigquery.Client(credentials=credentials, project=info["project_id"])
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"Authentication Failed: {e}")
        return None

client = get_bq_client()

############################
# - 2. DATA ENGINE LOGIC - #
############################

@st.cache_data(ttl=600)
def get_universal_portal_data(project_id, view_mode="engineering"):
    """
    Standardized Data Engine. 
    Joins Raw Data + Metadata + Manual Rejections[cite: 15].
    """
    cutoff = PROJECT_VISIBILITY_MASKS.get(project_id, "2000-01-01 00:00:00")
    
    # 1. Define the Query Filter based on View Mode 
    if view_mode == "client":
        # Must be explicitly Approved and NOT Masked 
        query_filter = f"""
            AND r.timestamp >= '{cutoff}'
            AND rej.approve = 'TRUE'
            AND NOT EXISTS (
                SELECT 1 FROM `{OVERRIDE_TABLE}` m 
                WHERE m.NodeNum = r.NodeNum 
                AND m.timestamp = TIMESTAMP_TRUNC(r.timestamp, HOUR)
                AND m.approve = 'MASKED'
            )
        """
    else:
        # Engineering sees everything except explicit deletions ('FALSE') 
        query_filter = "AND (rej.approve IS NULL OR rej.approve != 'FALSE')"

    # 2. Assign the 'query' variable explicitly
    query = f"""
        SELECT 
            r.NodeNum, r.timestamp, r.temperature,
            m.Location, m.Bank, m.Depth, m.Project,
            rej.approve as is_approved 
        FROM (
            SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
            UNION ALL
            SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
        ) AS r
        INNER JOIN `{PROJECT_ID}.{DATASET_ID}.metadata` AS m ON r.NodeNum = m.NodeNum
        LEFT JOIN `{OVERRIDE_TABLE}` AS rej 
            ON r.NodeNum = rej.NodeNum 
            AND TIMESTAMP_TRUNC(r.timestamp, HOUR) = rej.timestamp
        WHERE m.Project = '{project_id}'
        {query_filter}
        AND r.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 84 DAY)
        ORDER BY m.Location ASC, r.timestamp ASC
    """
    
    try:
        df = client.query(query).to_dataframe()
        
        # Ensure timestamp is UTC-aware immediately to avoid localization crashes
        if not df.empty and 'timestamp' in df.columns:
            if df['timestamp'].dt.tz is None:
                df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
                
        return df
    except Exception as e:
        st.error(f"BQ Data Engine Error: {e}")
        return pd.DataFrame()

def check_admin_access(service_name):
    """
    Restricts access to sensitive pages (Intake and Admin Tools).
    Requires a password defined in st.secrets["admin_password"].
    """
    # 1. Check if the user is already authenticated in this session
    if st.session_state.get("admin_authenticated"): 
        return True
    
    # 2. Display Lock Screen
    st.warning(f"🔒 Admin Access Required for {service_name}")
    pwd = st.text_input("Enter Admin Password", type="password", key=f"gate_{service_name}")
    
    if st.button("Unlock Access", key=f"btn_{service_name}"):
        # Check against streamlit secrets
        if pwd == st.secrets["admin_password"]:
            st.session_state["admin_authenticated"] = True
            st.success("Access Granted")
            st.rerun() # Refresh to show the restricted content
        else:
            st.error("Incorrect Password")
            
    return False
    
###########################
#- 3. SIDEBAR UI & STATE -#
###########################
st.sidebar.title("❄️ SoilFreeze Lab")

# --- 1. INITIALIZE FALLBACKS ---
service = "🏠 Executive Summary"
unit_mode = "Fahrenheit"
unit_label = "°F"
selected_project = "All Projects"
active_refs = [(32.0, "Freezing")]

# --- 2. TIMEZONE DEFAULT LOGIC ---
tz_lookup = {
    "UTC": "UTC", 
    "Local (US/Eastern)": "US/Eastern", 
    "Local (US/Pacific)": "US/Pacific"
}

# Force Pacific as the initial session state if nothing exists
if "tz_selection" not in st.session_state:
    st.session_state["tz_selection"] = "Local (US/Pacific)"

# --- 3. SIDEBAR WIDGETS ---
service = st.sidebar.selectbox(
    "📂 Page", 
    ["🏠 Executive Summary", "🌐 Global Overview", "📊 Client Portal", "📉 Node Diagnostics", "📤 Data Intake Lab", "🛠️ Admin Tools"]
)

unit_mode = st.sidebar.radio("Unit", ["Fahrenheit", "Celsius"])
unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"

# Timezone Display
tz_mode = st.sidebar.selectbox(
    "Timezone Display", 
    list(tz_lookup.keys()), 
    index=list(tz_lookup.keys()).index(st.session_state["tz_selection"])
)
# Update session state and set the active IANA string
st.session_state["tz_selection"] = tz_mode
display_tz = tz_lookup[tz_mode]

# Global Project Selection
if client is not None:
    try:
        proj_q = f"SELECT DISTINCT TRIM(Project) as Project FROM `{PROJECT_ID}.{DATASET_ID}.metadata` WHERE Project IS NOT NULL"
        proj_df = client.query(proj_q).to_dataframe()
        proj_list = sorted(proj_df['Project'].dropna().unique())
        options = ["All Projects"] + proj_list
        selected_project = st.sidebar.selectbox("🎯 Active Project", options, index=0, key="sidebar_proj_picker_global")
    except Exception as e:
        st.sidebar.error("Database connection lag. Defaulting to 'All Projects'.")
        selected_project = "All Projects"

# Reference Lines
st.sidebar.subheader("📏 Reference Lines")
active_refs = [] 
if st.sidebar.checkbox("Freezing (32°F)", value=True): 
    active_refs.append((32.0, "Freezing"))
if st.sidebar.checkbox("Type B (26.6°F)", value=False): 
    active_refs.append((26.6, "Type B"))
if st.sidebar.checkbox("Type A (10.2°F)", value=False): 
    active_refs.append((10.2, "Type A"))
########################
#- 4. GRAPHING ENGINE -#
########################

def build_high_speed_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label, display_tz="UTC"):
    """
    Stabilized Engine: Handles timezone synchronization for start/end endpoints 
    to prevent AssertionError in pd.date_range.
    """
    if df.empty:
        return go.Figure().update_layout(title="No data available for the selected period.")

    plot_df = df.copy()
    
    # 1. TIMEZONE & UNIT CONVERSION
    # Ensure the dataframe timestamp is UTC-aware before conversion
    if plot_df['timestamp'].dt.tz is None:
        plot_df['timestamp'] = plot_df['timestamp'].dt.tz_localize('UTC')
    
    # Convert dataframe to selected local timezone
    plot_df['timestamp'] = plot_df['timestamp'].dt.tz_convert(display_tz)
    
    # Convert endpoints to the SAME timezone as display_tz to prevent AssertionError
    start_local = start_view.tz_convert(display_tz) if start_view.tzinfo else start_view.tz_localize('UTC').tz_convert(display_tz)
    end_local = end_view.tz_convert(display_tz) if end_view.tzinfo else end_view.tz_localize('UTC').tz_convert(display_tz)
    now_local = pd.Timestamp.now(tz=display_tz)
    
    if unit_mode == "Celsius":
        plot_df['temperature'] = (plot_df['temperature'] - 32) * 5/9
        y_range, dt_major, dt_minor = [-30, 30], 10, 5
    else:
        y_range, dt_major, dt_minor = [-20, 80], 10, 5

    # 2. LABELING & SORTING
    def get_sort_info(r):
        b = str(r.get('Bank', '')).strip()
        d = str(r.get('Depth', '')).strip()
        if b and b.lower() not in ['nan', 'none', '']: 
            return f"Bank {b}", 0.0
        if d and d.lower() not in ['nan', 'none', '']:
            try:
                num = float(re.findall(r"[-+]?\d*\.\d+|\d+", d)[0])
                return f"{d}ft", num
            except: 
                return f"{d}ft", 999.0
        return f"Node {r['NodeNum']}", 1000.0

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
                line=dict(color=color, width=1.5),
                marker=dict(size=4, opacity=0.8),
                hovertemplate=f"<b>{group_lbl} ({sn})</b>: %{{y:.1f}}{unit_label}<extra></extra>"
            ))

    # 4. REFERENCE LINES & NOW MARKER
    for val, ref_label in active_refs:
        c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
        fig.add_hline(y=c_val, line_dash="dash", line_color="RoyalBlue", 
                      annotation_text=ref_label, annotation_position="top right")

    fig.add_vline(x=now_local, line_width=2, line_color="Red", layer='above', line_dash="dash")

    # 5. GRID HIERARCHY & LAYOUT
    fig.update_layout(
        title={'text': f"<b>{title}</b>", 'x': 0},
        plot_bgcolor='white', hovermode="x unified", height=600,
        margin=dict(t=80, l=50, r=180, b=50),
        xaxis=dict(
            range=[start_local, end_local], 
            showline=True, mirror=True, linecolor='black',
            showgrid=True, dtick="D1", gridcolor='DarkGray', gridwidth=1,
            minor=dict(dtick=6*60*60*1000, showgrid=True, gridcolor='Gainsboro', griddash='dash'),
            tickformat='%b %d\n%H:%M'
        ),
        yaxis=dict(
            title=f"Temperature ({unit_label})", range=y_range, dtick=dt_major, 
            gridcolor='DarkGray', showline=True, mirror=True, linecolor='black',
            minor=dict(dtick=dt_minor, showgrid=True, gridcolor='whitesmoke')
        ),
        legend=dict(title="Sensors", orientation="v", x=1.02, y=1)
    )
    
    # FIXED: Generate Monday lines using endpoints already converted to display_tz
    mondays = pd.date_range(start=start_local, end=end_local, freq='W-MON', tz=display_tz)
    for mon in mondays:
        fig.add_vline(x=mon, line_width=2, line_color="dimgray", layer="below")

    return fig
##################
# Page Functions #
##################

###########
# - 5. PAGE: GLOBAL OVERVIEW - #
###########

def render_global_overview(selected_project, display_tz):
    """
    Shows all pipes/banks for a selected project in one scrolling view.
    Engineering view: shows everything except 'FALSE'.
    """
    st.header("🌐 Global Project Overview")
    
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to begin.")
        return

    with st.spinner(f"Syncing {selected_project} (Engineering View)..."):
        # Fetch data using the updated engine
        p_df = get_universal_portal_data(selected_project, view_mode="engineering")

    if not p_df.empty:
        # 1. View Constraints
        lookback = st.sidebar.slider("Lookback (Weeks)", 1, 12, 4, key="global_lookback_slider")
        
        # Snap time window to the current Pacific (or selected) time
        now_local = pd.Timestamp.now(tz=display_tz)
        end_view = (now_local + pd.Timedelta(days=(7-now_local.weekday())%7 or 7)).replace(hour=0, minute=0, second=0, microsecond=0)
        start_view = end_view - timedelta(weeks=lookback)

        # 2. Render a graph for every physical location (Pipe/Bank) in the project [cite: 6]
        locations = sorted(p_df['Location'].dropna().unique())
        
        for loc in locations:
            with st.expander(f"📍 Location: {loc}", expanded=True):
                loc_df = p_df[p_df['Location'] == loc]
                fig = build_high_speed_graph(
                    df=loc_df, 
                    title=f"📈 {selected_project} - {loc}", 
                    start_view=start_view, 
                    end_view=end_view, 
                    active_refs=tuple(active_refs), 
                    unit_mode=unit_mode, 
                    unit_label=unit_label, 
                    display_tz=display_tz
                )
                st.plotly_chart(fig, use_container_width=True, key=f"ov_{selected_project}_{loc}")
    else:
        st.warning(f"No engineering data found for '{selected_project}' in the last 84 days.")
        st.info("Check if sensors are mapped to this project name in the metadata table[cite: 5].")
###########
# - 6. PAGE: EXECUTIVE SUMMARY - #
###########

def render_executive_summary(client, selected_project, unit_label, display_tz):
    st.header(f"🏠 Executive Summary: Health Monitor")
    
    # 1. Fuzzy Filter Logic
    proj_filter = ""
    if selected_project and selected_project != "All Projects":
        proj_filter = f"AND TRIM(Project) = '{selected_project.strip()}'"

    # SQL query tracking 24h activity, 6h activity, and lag endpoints
    query = f"""
        WITH MappedNodes AS (
            SELECT TRIM(Project) as Project, NodeNum, Location
            FROM `{PROJECT_ID}.{DATASET_ID}.metadata_snapshot`
            WHERE Project IS NOT NULL {proj_filter}
        ),
        BaseReporting AS (
            SELECT NodeNum, timestamp 
            FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
            UNION ALL
            SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
        ),
        RecentReporting24h AS (
            SELECT NodeNum, 1 as active_24 
            FROM BaseReporting 
            WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            GROUP BY NodeNum
        ),
        RecentReporting6h AS (
            SELECT NodeNum, 1 as active_6 
            FROM BaseReporting 
            WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
            GROUP BY NodeNum
        ),
        HistoricalPings AS (
            SELECT 
                NodeNum, 
                MAX(timestamp) as ever_ping_max
            FROM BaseReporting 
            GROUP BY NodeNum
        ),
        JoinedData AS (
            SELECT 
                m.Project, m.Location, m.NodeNum,
                COALESCE(r24.active_24, 0) as is_active_24,
                COALESCE(r6.active_6, 0) as is_active_6,
                h.ever_ping_max
            FROM MappedNodes m
            LEFT JOIN RecentReporting24h r24 ON m.NodeNum = r24.NodeNum
            LEFT JOIN RecentReporting6h r6 ON m.NodeNum = r6.NodeNum
            LEFT JOIN HistoricalPings h ON m.NodeNum = h.NodeNum
        ),
        LocationStats AS (
            SELECT 
                Project, Location, 
                COUNT(NodeNum) as total, 
                SUM(is_active_24) as active_24h, 
                SUM(is_active_6) as active_6h,
                MAX(ever_ping_max) as last_up,
                MIN(ever_ping_max) as oldest_node_ping 
            FROM JoinedData GROUP BY Project, Location
        ),
        ProjectTotals AS (
            SELECT 
                Project, 'PROJECT TOTAL' as Location, 
                COUNT(NodeNum) as total, 
                SUM(is_active_24) as active_24h, 
                SUM(is_active_6) as active_6h,
                MAX(ever_ping_max) as last_up,
                MIN(ever_ping_max) as oldest_node_ping
            FROM JoinedData GROUP BY Project
        )
        SELECT * FROM ProjectTotals
        UNION ALL
        SELECT * FROM LocationStats
        ORDER BY Project ASC, (Location = 'PROJECT TOTAL') DESC, Location ASC
    """
    
    try:
        with st.spinner("⚡ Auditing connectivity..."):
            df = client.query(query).to_dataframe()
        
        if df.empty:
            st.warning("⚠️ No data found. Check your Metadata and Project selection.")
            return

        now_local = pd.Timestamp.now(tz=display_tz)

        def process_health_row(row):
            last_ts = row['last_up']
            oldest_ts = row['oldest_node_ping']
            
            # Last Seen Calculation
            if pd.notnull(last_ts):
                if last_ts.tzinfo is None: last_ts = last_ts.tz_localize('UTC')
                last_ts_local = last_ts.tz_convert(display_tz)
                last_gap = round((now_local - last_ts_local).total_seconds() / 3600, 1)
                last_seen_str = f"{last_gap}h ago"
            else:
                last_seen_str = "Never"

            # Max Lag Calculation
            if pd.notnull(oldest_ts):
                if oldest_ts.tzinfo is None: oldest_ts = oldest_ts.tz_localize('UTC')
                oldest_ts_local = oldest_ts.tz_convert(display_tz)
                max_lag_hrs = round((now_local - oldest_ts_local).total_seconds() / 3600, 1)
                icon = "🔴" if max_lag_hrs > 24 else ("🟡" if max_lag_hrs > 6 else "🟢")
                lag_str = f"{max_lag_hrs}h {icon}"
            else:
                lag_str = "N/A"

            return pd.Series({
                "Project": row['Project'],
                "Location": row['Location'],
                "Nodes": row['total'],
                "Seen (24h)": row['active_24h'],
                "Seen (6h)": row['active_6h'],  # NEW COLUMN
                "Last Seen": last_seen_str,
                "Max Lag": lag_str
            })

        health_df = df.apply(process_health_row, axis=1)

        # --- STYLING LOGIC ---
        def style_project_rows(row):
            if row['Location'] == 'PROJECT TOTAL':
                return ['background-color: #f0f2f6; font-weight: bold'] * len(row)
            return [''] * len(row)

        styled_df = health_df.style.apply(style_project_rows, axis=1)

        st.dataframe(styled_df, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Executive Summary Error: {e}")

###########
# - 7. PAGE: CLIENT PORTAL - #
###########

def render_client_portal(selected_project, display_tz, unit_mode, unit_label, active_refs):
    st.header(f"📊 Project Status: {selected_project}")
    global client

    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar.")
        return
    
    with st.spinner("Loading approved data..."):
        # The portal specifically filters for manual_rejections.status = 'TRUE' [cite: 15, 16]
        p_df = get_universal_portal_data(selected_project, view_mode="client")
    
    # DEBUG: Help identify if data exists but is being filtered out later
    if not p_df.empty:
        st.caption(f"✅ Found {len(p_df)} approved records for {selected_project}.")
    else:
        st.warning(f"⚠️ No data marked as 'Approved' found for {selected_project}. Check the Admin Tools.")
        return

    tab_time, tab_depth, tab_table = st.tabs(["📈 Timeline Analysis", "📏 Depth Profile", "📋 Summary Table"])

    with tab_time:
        weeks_view = st.slider("Weeks to View", 1, 12, 6, key="client_weeks_slider")
        end_view = pd.Timestamp.now(tz='UTC')
        start_view = end_view - timedelta(weeks=weeks_view)
        
        # Performance: Pre-sort locations
        locations = sorted(p_df['Location'].dropna().unique())
        
        if not locations:
            st.error("Data loaded, but no 'Location' metadata was found to group the charts.")
        
        for loc in locations:
            with st.expander(f"📍 {loc}", expanded=(len(locations) == 1)):
                loc_data = p_df[p_df['Location'] == loc].copy()
                
                # Check if this specific location has data in the selected time window
                if loc_data.empty:
                    st.write("No data available for this specific location.")
                    continue

                fig = build_high_speed_graph(
                    df=loc_data, 
                    title=f"{loc} Approved Data", 
                    start_view=start_view, 
                    end_view=end_view, 
                    active_refs=tuple(active_refs), 
                    unit_mode=unit_mode, 
                    unit_label=unit_label, 
                    display_tz=display_tz 
                )
                st.plotly_chart(fig, use_container_width=True, key=f"portal_grid_{loc}")

    with tab_depth:
        st.subheader("📏 Vertical Temperature Profile")
        # Ensure Depth is numeric for proper Y-axis scaling [cite: 6, 9]
        p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
        depth_only = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
        
        for loc in sorted(depth_only['Location'].unique()):
            with st.expander(f"📏 {loc} Weekly Snapshots", expanded=False):
                loc_data = depth_only[depth_only['Location'] == loc].copy()
                fig_d = go.Figure()
                
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

                y_limit = int(((loc_data['Depth_Num'].max() // 10) + 1) * 10) if not loc_data.empty else 50
                fig_d.update_layout(
                    plot_bgcolor='white', height=600,
                    xaxis=dict(title=f"Temp ({unit_label})", gridcolor='Gainsboro'),
                    yaxis=dict(title="Depth (ft)", range=[y_limit, 0], dtick=10, gridcolor='Silver'),
                    legend=dict(orientation="h", y=-0.2)
                )
                st.plotly_chart(fig_d, use_container_width=True, key=f"d_graph_{loc}")

    with tab_table:
        # Latest Snapshot Table (Fastest way to group latest data)
        latest = p_df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        
        # Efficient vector conversion
        latest['Current Temp'] = latest['temperature'].apply(
            lambda x: f"{round((x - 32) * 5/9 if unit_mode == 'Celsius' else x, 1)}{unit_label}"
        )
        
        latest['Position'] = latest.apply(
            lambda r: f"Bank {r['Bank']}" if pd.notnull(r['Bank']) and str(r['Bank']).strip() != "" 
            else f"{r.get('Depth', '??')} ft", axis=1
        )
        
        st.dataframe(
            latest[['Location', 'Position', 'Current Temp', 'NodeNum']].sort_values(['Location', 'Position']), 
            use_container_width=True, 
            hide_index=True
        )
            
###########
# - 8. PAGE: NODE DIAGNOSTICS - #
###########

def render_node_diagnostics(selected_project, display_tz, unit_mode, unit_label, active_refs):
    """
    Engineering-level view. Shows everything (Pending, Masked, Approved).
    """
    st.header(f"📉 Node Diagnostics: {selected_project}")
    
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Select a specific project in the sidebar.")
        return

    with st.spinner("🔍 Syncing diagnostic streams..."):
        # Fetching data using the engineering view mode [cite: 16]
        all_data = get_universal_portal_data(selected_project, view_mode="engineering")
    
    if all_data.empty:
        st.warning(f"No diagnostic data found for project {selected_project}.")
        return

    # 1. Selection Controls
    loc_options = sorted(all_data['Location'].dropna().unique())
    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        sel_loc = st.selectbox("Select Pipe / Bank to Analyze", loc_options, key="diag_loc_select")
    with c2:
        weeks_view = st.sidebar.slider("Lookback (Weeks)", 1, 12, 2, key="diag_weeks_slider")
    with c3:
        show_profile = st.checkbox("Show Vertical Profile", value=True)
            
    # 2. Localize 'Now' and calculate window
    now_local = pd.Timestamp.now(tz=display_tz)
    start_view = now_local - timedelta(weeks=weeks_view)
    
    # Filter data for the selected Location
    # Ensure df_diag is defined before any sorting or grouping operations
    df_diag = all_data[all_data['Location'] == sel_loc].copy()
    
    if df_diag.empty:
        st.info(f"No data points found for location: {sel_loc}")
        return

    # --- 1. ENGINEERING TIMELINE ---
    st.subheader("🕒 Engineering Timeline")
    fig_time = build_high_speed_graph(
        df_diag, f"Diagnostic Stream: {sel_loc}", 
        start_view, now_local + timedelta(hours=2), 
        tuple(active_refs), unit_mode, unit_label, display_tz
    )
    st.plotly_chart(fig_time, use_container_width=True)

    # --- 2. VERTICAL PROFILE ---
    if show_profile:
        st.divider()
        st.subheader("📏 Vertical Temperature Profile")
        df_diag['Depth_Num'] = pd.to_numeric(df_diag['Depth'], errors='coerce')
        profile_df = df_diag.dropna(subset=['Depth_Num']).copy()

        if not profile_df.empty:
            latest_snap = profile_df.sort_values('timestamp').groupby('Depth_Num').last().reset_index()
            
            latest_snap['conv_temp'] = latest_snap['temperature'].apply(
                lambda x: (x - 32) * 5/9 if unit_mode == "Celsius" else x
            )

            fig_d = go.Figure()
            fig_d.add_trace(go.Scatter(
                x=latest_snap['conv_temp'], 
                y=latest_snap['Depth_Num'], 
                mode='lines+markers',
                name="Current State",
                line=dict(shape='spline', smoothing=0.5, width=3, color='RoyalBlue'),
                marker=dict(size=10, symbol='diamond')
            ))

            y_limit = int(((profile_df['Depth_Num'].max() // 10) + 1) * 10)
            fig_d.update_layout(
                plot_bgcolor='white', height=600,
                xaxis=dict(title=f"Temp ({unit_label})", gridcolor='Gainsboro'),
                yaxis=dict(title="Depth (ft)", range=[y_limit, 0], gridcolor='Silver')
            )
            st.plotly_chart(fig_d, use_container_width=True)

    # --- 3. COMMUNICATION HEALTH TABLE ---
    st.divider()
    st.subheader("📋 Sensor Communication Health")
    
    # Correctly identify latest nodes within the defined df_diag
    latest_nodes = df_diag.sort_values('timestamp').groupby('NodeNum').tail(1).copy()
    
    summary_rows = []
    for _, row in latest_nodes.iterrows():
        # Ensure timestamp is localized to Pacific/Selected TZ for math
        ts_local = row['timestamp'].tz_convert(display_tz) if row['timestamp'].tzinfo else row['timestamp'].tz_localize('UTC').tz_convert(display_tz)
        hrs_ago = int((now_local - ts_local).total_seconds() / 3600)
        
        status_icon = "🔴" if hrs_ago > 24 else ("🟢" if hrs_ago < 6 else "🟡")
        
        db_status = str(row.get('is_approved', 'PENDING')).upper()
        status_label = "✅ Approved" if db_status == "TRUE" else ("🚫 Masked" if db_status == "MASKED" else "⏳ Pending")

        conv_temp = (row['temperature'] - 32) * 5/9 if unit_mode == "Celsius" else row['temperature']

        summary_rows.append({
            "Node": row['NodeNum'],
            "Last Value": f"{round(conv_temp, 1)}{unit_label}",
            "Last Seen": f"{hrs_ago}h ago {status_icon}",
            "Admin Status": status_label
        })
    
    if summary_rows:
        st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, hide_index=True)
    else:
        st.info("No communication logs available for this selection.")
###########
# - 9. PAGE: DATA INTAKE LAB - #
###########

def render_data_intake_page(selected_project):
    st.header("📤 Data Ingestion Lab")
    tab_upload, tab_export = st.tabs(["📄 Upload", "📥 Export"])
    
    with tab_upload:
        st.subheader("📄 Manual File Ingestion")
        st.info("Standardized Rule: All Lord Node IDs will use '-' as a separator (e.g., 58014-ch1).")
        
        u_file = st.file_uploader("Upload Data File", type=['csv', 'xlsx'], key="manual_upload_main")
        
        if u_file is not None:
            try:
                # --- 1. DETECTION FOR SENSORCONNECT (WIDE) ---
                is_sensorconnect = False
                skip_rows = 0
                
                if u_file.name.endswith('.csv'):
                    u_file.seek(0)
                    for i, line in enumerate(u_file):
                        if b"DATA_START" in line:
                            is_sensorconnect = True
                            skip_rows = i + 1 
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
                    
                    # --- BRANCH A: SENSORCONNECT (Wide Format) ---
                    if is_sensorconnect:
                        time_col = [h for h in actual_headers if 'time' in h.lower()][0]
                        value_vars = [h for h in actual_headers if h != time_col]
                        
                        df_melted = df_raw.melt(
                            id_vars=[time_col], 
                            value_vars=value_vars, 
                            var_name='NodeNum', 
                            value_name='temperature'
                        )
                        
                        df_processed['timestamp'] = pd.to_datetime(df_melted[time_col], format='mixed')
                        # STANDARDIZATION: Swap ':' for '-'
                        df_processed['NodeNum'] = df_melted['NodeNum'].str.strip().str.replace(':', '-')
                        df_processed['temperature'] = pd.to_numeric(df_melted['temperature'], errors='coerce')

                    # --- BRANCH B: LORD (Long/Narrow Format) ---
                    elif any('channel' in h or 'node' in h for h in clean_headers) and any('time' in h for h in clean_headers):
                        st.info("Format Detected: Lord (Channel-based)")
                        time_idx = next(i for i, h in enumerate(clean_headers) if 'time' in h)
                        node_idx = next(i for i, h in enumerate(clean_headers) if 'channel' in h or 'node' in h)
                        
                        time_header = actual_headers[time_idx]
                        node_header = actual_headers[node_idx]
                        temp_match = [h for h in actual_headers if 'temp' in h.lower()]
                        
                        if temp_match:
                            df_processed['timestamp'] = pd.to_datetime(df_raw[time_header], format='mixed')
                            # STANDARDIZATION: Swap ':' for '-'
                            df_processed['NodeNum'] = df_raw[node_header].str.strip().str.replace(':', '-')
                            df_processed['temperature'] = pd.to_numeric(df_raw[temp_match[0]], errors='coerce')

                    # --- BRANCH C: SENSORPUSH ---
                    else:
                        st.info("Format Detected: SensorPush")
                        t_match = [h for h in actual_headers if 'timestamp' in h.lower()]
                        v_match = [h for h in actual_headers if 'temp' in h.lower()]
                        if t_match and v_match:
                            import re
                            match = re.search(r'^([^ \(\.]+)', u_file.name)
                            df_processed['timestamp'] = pd.to_datetime(df_raw[t_match[0]], format='mixed')
                            df_processed['temperature'] = pd.to_numeric(df_raw[v_match[0]], errors='coerce')
                            df_processed['NodeNum'] = match.group(1) if match else "Unknown"

                    # --- 3. PREVIEW & UPLOAD ---
                    if not df_processed.empty:
                        df_processed = df_processed.dropna(subset=['timestamp', 'temperature'])
                        
                        found_nodes = df_processed['NodeNum'].unique()
                        st.success(f"✅ Ready: Standardized Node IDs: {', '.join(found_nodes)}")
                        st.dataframe(df_processed.head(10))

                        target_table = "raw_lord" if (is_sensorconnect or 'channel' in clean_headers or 'node' in clean_headers) else "raw_sensorpush"
                        
                        if st.button("🚀 Push to BigQuery"):
                            with st.spinner("Uploading data..."):
                                table_id = f"{PROJECT_ID}.{DATASET_ID}.{target_table}"
                                config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                                client.load_table_from_dataframe(df_processed, table_id, job_config=config).result()
                                
                                st.success(f"Successfully uploaded {len(df_processed)} rows to {target_table}!")
                                st.cache_data.clear()
            except Exception as e:
                st.error(f"Error processing file: {e}")

    with tab_export:
        st.subheader("📥 Export Project Data")
        if not selected_project or selected_project == "All Projects":
            st.warning("⚠️ Please select a specific project in the sidebar to perform an export.")
        else:
            # 1. Date Selection
            c1, c2 = st.columns(2)
            with c1:
                e_start = st.date_input("Start Date", value=datetime.now() - timedelta(days=30), key="exp_start")
            with c2:
                e_end = st.date_input("End Date", value=datetime.now(), key="exp_end")
            
            # 2. Scope Selection (Whole Project vs Single Pipe)
            st.write("---")
            export_scope = st.radio("Export Scope", ["Whole Project", "Specific Pipe / Bank"], horizontal=True)
            
            # Fetch data once to populate location options and for filtering
            with st.spinner("Preparing export options..."):
                full_df = get_universal_portal_data(selected_project, view_mode="engineering")
            
            target_loc = None
            if export_scope == "Specific Pipe / Bank" and not full_df.empty:
                loc_list = sorted(full_df['Location'].dropna().unique())
                target_loc = st.selectbox("Select Pipe/Bank to Export", loc_list)

            # 3. Export Action
            if st.button("📦 Prepare Data for Download"):
                if full_df.empty:
                    st.error("No data found for this project in the engineering database.")
                else:
                    # Filter by Date
                    mask = (full_df['timestamp'].dt.date >= e_start) & (full_df['timestamp'].dt.date <= e_end)
                    export_df = full_df.loc[mask].copy()

                    # Filter by Scope
                    filename_suffix = "Whole_Project"
                    if export_scope == "Specific Pipe / Bank" and target_loc:
                        export_df = export_df[export_df['Location'] == target_loc]
                        filename_suffix = target_loc.replace(" ", "_")

                    if export_df.empty:
                        st.warning("No data found matching the combined date and scope filters.")
                    else:
                        # Success Message & Download
                        st.success(f"✅ Prepared {len(export_df)} rows for {filename_suffix}.")
                        
                        # Clean up timestamps for the CSV
                        export_df['timestamp'] = export_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                        
                        csv = export_df.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label=f"💾 Download {filename_suffix} CSV",
                            data=csv,
                            file_name=f"{selected_project}_{filename_suffix}_Export.csv",
                            mime="text/csv"
                        )
###########
# - 10. PAGE: ADMIN TOOLS - #
###########

def render_admin_page(selected_project, display_tz, unit_mode, unit_label, active_refs):
    st.header("🛠️ Admin Tools")
    
    # Define all administrative tabs
    tab_bulk, tab_mask, tab_scrub, tab_surgical = st.tabs([
        "✅ Bulk Approval", 
        "🚫 Mask Data", 
        "🧹 Scrub", 
        "🧨 Surgical"
    ])

    # --- TAB 1: BULK APPROVAL ---
    with tab_bulk:
        st.subheader("✅ Range-Based Bulk Approval")
        st.write("Approve all data within this specific window for the client portal.")
        
        c1, c2 = st.columns(2)
        with c1:
            b_start = st.date_input("Approval Start", value=datetime.now() - timedelta(days=7), key="bulk_start")
        with c2:
            b_end = st.date_input("Approval End", value=datetime.now(), key="bulk_end")

        if st.button(f"🚀 Approve {selected_project} Range", use_container_width=True):
            with st.spinner("Writing approvals to master override..."):
                bulk_sql = f"""
                    INSERT INTO `{OVERRIDE_TABLE}` (NodeNum, timestamp, approve)
                    SELECT DISTINCT r.NodeNum, TIMESTAMP_TRUNC(r.timestamp, HOUR), 'TRUE'
                    FROM (
                        SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` 
                        UNION ALL 
                        SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
                    ) AS r
                    INNER JOIN `{PROJECT_ID}.{DATASET_ID}.metadata` AS m ON r.NodeNum = m.NodeNum
                    WHERE m.Project = '{selected_project}'
                    AND r.timestamp >= '{b_start}' AND r.timestamp <= '{b_end}'
                    AND NOT EXISTS (
                        SELECT 1 FROM `{OVERRIDE_TABLE}` x 
                        WHERE x.NodeNum = r.NodeNum 
                        AND x.timestamp = TIMESTAMP_TRUNC(r.timestamp, HOUR)
                    )
                """
                try:
                    client.query(bulk_sql).result()
                    st.success(f"✅ Data for {selected_project} successfully approved.")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"Bulk Approval Error: {e}")

    # --- TAB 2: MASK DATA (Updated: Clear Masks Only) ---
    with tab_mask:
        st.subheader("🚫 Temporal Data Masking")
        
        if not selected_project or selected_project == "All Projects":
            st.warning("Please select a specific project in the sidebar.")
        else:
            # Mask Mode Toggle
            mask_mode = st.radio("Masking Mode", ["Specific Time Range", "All data before end date"], horizontal=True)
            
            m_col1, m_col2 = st.columns(2)
            with m_col1:
                m_start_date = st.date_input("Start Date", value=datetime.now() - timedelta(days=7), key="m_sd", disabled=(mask_mode == "All data before end date"))
                m_start_time = st.time_input("Start Time", value=datetime.time(datetime.now()), key="m_st", disabled=(mask_mode == "All data before end date"))
            with m_col2:
                m_end_date = st.date_input("End Date", value=datetime.now(), key="m_ed")
                m_end_time = st.time_input("End Time", value=datetime.time(datetime.now()), key="m_et")

            # Formatting logic
            end_dt = datetime.combine(m_end_date, m_end_time)
            if mask_mode == "All data before end date":
                start_dt_str = "2000-01-01 00:00:00" 
                action_desc = f"Hiding EVERYTHING before `{end_dt}`"
            else:
                start_dt = datetime.combine(m_start_date, m_start_time)
                start_dt_str = start_dt.strftime('%Y-%m-%d %H:%M:%S')
                action_desc = f"Hiding data from `{start_dt}` to `{end_dt}`"

            st.write(f"**Action:** {action_desc}")

            c1, c2 = st.columns(2)
            with c1:
                if st.button(f"🚫 Apply Mask", type="primary", use_container_width=True):
                    with st.spinner("Applying masks..."):
                        mask_sql = f"""
                            INSERT INTO `{OVERRIDE_TABLE}` (NodeNum, timestamp, approve)
                            SELECT DISTINCT r.NodeNum, TIMESTAMP_TRUNC(r.timestamp, HOUR), 'MASKED'
                            FROM (
                                SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` 
                                UNION ALL 
                                SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
                            ) AS r
                            INNER JOIN `{PROJECT_ID}.{DATASET_ID}.metadata` AS m ON r.NodeNum = m.NodeNum
                            WHERE m.Project = '{selected_project}'
                            AND r.timestamp >= '{start_dt_str}' 
                            AND r.timestamp <= '{end_dt.strftime('%Y-%m-%d %H:%M:%S')}'
                            AND NOT EXISTS (
                                SELECT 1 FROM `{OVERRIDE_TABLE}` x 
                                WHERE x.NodeNum = r.NodeNum 
                                AND x.timestamp = TIMESTAMP_TRUNC(r.timestamp, HOUR)
                            )
                        """
                        client.query(mask_sql).result()
                        st.success("✅ Mask applied successfully.")
                        st.cache_data.clear()
            
            with c2:
                # UPDATED: Now strictly deletes MASKED rows for this project
                if st.button(f"🗑️ Clear Project Masks", use_container_width=True):
                    with st.spinner("Clearing project masks..."):
                        clear_mask_sql = f"""
                            DELETE FROM `{OVERRIDE_TABLE}`
                            WHERE approve = 'MASKED'
                            AND NodeNum IN (
                                SELECT NodeNum FROM `{PROJECT_ID}.{DATASET_ID}.metadata` 
                                WHERE Project = '{selected_project}'
                            )
                        """
                        client.query(clear_mask_sql).result()
                        st.warning(f"🧹 All masks cleared for {selected_project}. Approved data remains.")
                        st.cache_data.clear()

    # --- TAB 3: DEEP DATA SCRUB ---
    with tab_scrub:
        st.subheader("🧹 Deep Data Scrub")
        st.warning("Averages raw data to 1-hour intervals. This is IRREVERSIBLE.")
        scrub_target = st.radio("Target Table", ["SensorPush", "Lord"], horizontal=True, key="admin_scrub_select")
        t_table = f"{PROJECT_ID}.{DATASET_ID}.raw_{scrub_target.lower()}"
        
        if st.button(f"🧨 Purge & Average {scrub_target}", use_container_width=True):
            with st.spinner(f"Reducing {scrub_target} to hourly means..."):
                scrub_sql = f"""
                    CREATE OR REPLACE TABLE `{t_table}` AS 
                    SELECT 
                        TIMESTAMP_TRUNC(TIMESTAMP_ADD(timestamp, INTERVAL 30 MINUTE), HOUR) as timestamp, 
                        NodeNum, 
                        AVG(temperature) as temperature
                    FROM `{t_table}`
                    WHERE temperature IS NOT NULL
                    GROUP BY 1, 2
                """
                client.query(scrub_sql).result()
                st.success(f"✅ {scrub_target} table successfully averaged.")
                st.cache_data.clear()

    # --- TAB 4: SURGICAL CLEANER ---
    with tab_surgical:
        st.subheader("🧨 Surgical Point Cleaner")
        if not selected_project or selected_project == "All Projects":
            st.warning("Please select a specific project in the sidebar.")
        else:
            render_surgical_cleaner(selected_project, display_tz, unit_mode, unit_label, active_refs)

###########
# - 11. SURGICAL CLEANER FUNCTIONS - #
###########

def render_surgical_cleaner(selected_project, display_tz, unit_mode, unit_label, active_refs):
    """
    Alternative Stable Cleaner using the 'streamlit-plotly-events' library.
    This component is highly reliable for capturing Lasso data without losing state.
    """
    st.subheader("🧨 Surgical Point Cleaner (Alt-Engine)")

    # 1. VIEW & ACTION TOGGLES
    c1, c2 = st.columns(2)
    view_toggle = c1.radio("Display Mode", ["Engineering", "Client"], horizontal=True)
    delete_method = c2.radio("Action Type", ["Soft Delete", "Hard Purge"], horizontal=True)
    v_mode = "engineering" if view_toggle == "Engineering" else "client"

    # 2. DATA PREP
    p_df = get_universal_portal_data(selected_project, view_mode=v_mode)
    if p_df.empty:
        st.info("No data available.")
        return

    sel_loc = st.selectbox("Select Pipe", sorted(p_df['Location'].unique()))
    scrub_df = p_df[p_df['Location'] == sel_loc].copy().reset_index(drop=True)

    # 3. BUILD THE GRAPH
    fig_scrub = build_high_speed_graph(
        scrub_df, f"Surgical Scrubbing: {sel_loc}", 
        pd.Timestamp.now(tz=display_tz) - timedelta(days=14), 
        pd.Timestamp.now(tz=display_tz) + timedelta(hours=6), 
        active_refs, unit_mode, unit_label, display_tz=display_tz
    )
    
    # Force the Lasso tool to be active
    fig_scrub.update_layout(dragmode='lasso')

    # 4. CAPTURE EVENTS (The Library Way)
    # This replaces st.plotly_chart and returns a list of dictionaries immediately
    selected_points = plotly_events(
        fig_scrub, 
        select_event=True, 
        key=f"alt_lasso_{sel_loc}_{v_mode}",
        override_height=600
    )

    # 5. ACTION BUTTONS
    if selected_points:
        st.success(f"📍 {len(selected_points)} points captured in memory.")
        
        b1, b2, b3, b4 = st.columns(4)
        with b1:
            if st.button("✅ Approve"):
                # The library returns 'pointNumber' which maps to our index
                update_records_alt(selected_points, scrub_df, "TRUE")
        with b2:
            if st.button("🚫 Mask"):
                update_records_alt(selected_points, scrub_df, "MASKED")
        with b3:
            label = "🔥 PURGE" if "Hard" in delete_method else "🗑️ Delete"
            if st.button(label, type="primary"):
                if "Hard" in delete_method:
                    hard_purge_points_alt(selected_points, scrub_df)
                else:
                    update_records_alt(selected_points, scrub_df, "FALSE")
        with b4:
            if st.button("Clear Selection"):
                st.rerun()
    else:
        st.info("💡 Draw a Lasso on the graph to capture points for action.")

def hard_purge_points(pts, df):
    """
    Permanently deletes lassoed points from the raw BigQuery tables.
    """
    with st.spinner("Executing permanent purge..."):
        for p in pts:
            try:
                node = df.iloc[p['point_index']]['NodeNum']
                ts = p['x'] 
                
                # Determine table and ID column [cite: 2, 3]
                table = "raw_lord" if "-" in str(node) else "raw_sensorpush"
                id_col = "NodeNum" if "lord" in table else "sensor_id"
                
                # Permanent DELETE query 
                delete_sql = f"""
                    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{table}` 
                    WHERE {id_col} = '{node}' 
                    AND timestamp = '{ts}'
                """
                client.query(delete_sql).result()
            except:
                continue
    
    st.session_state.locked_selection = []
    st.cache_data.clear() # 
    st.success("Points purged. Database sync complete.")
    st.rerun()


# - 11. SURGICAL CLEANER HELPERS - #
###########

def update_records(pts, df, val):
    """
    Writes status updates (TRUE, FALSE, MASKED) to the manual_rejections table.
    """
    recs = []
    for p in pts:
        try:
            # Snap timestamp to the hour for database alignment [cite: 14]
            ts = pd.to_datetime(p['x']).tz_convert('UTC').floor('h')
            node = df.iloc[p['point_index']]['NodeNum']
            
            recs.append({
                "NodeNum": str(node), 
                "timestamp": ts, 
                "approve": val 
            })
        except Exception:
            continue
    
    if recs:
        # Deduplicate to prevent multiple entries for the same hour [cite: 15]
        status_df = pd.DataFrame(recs).drop_duplicates(subset=['NodeNum', 'timestamp'])
        try:
            # Load updates into BigQuery [cite: 1, 11]
            job = client.load_table_from_dataframe(status_df, OVERRIDE_TABLE)
            job.result() 
            
            # Reset state and clear cache to reflect changes immediately [cite: 15]
            st.session_state.locked_selection = []
            st.cache_data.clear()
            st.success(f"Successfully marked {len(status_df)} records as {val}")
            time.sleep(1) 
            st.rerun()
        except Exception as e:
            st.error(f"Database Error: {e}")
###########
# - 12. MAIN ROUTER - #
###########

if service == "🌐 Global Overview":
    # Pass both project and timezone for rendering
    render_global_overview(selected_project, display_tz) 

elif service == "🏠 Executive Summary":
    # Ensure client and timezone are passed for health audit [cite: 6]
    render_executive_summary(client, selected_project, unit_label, display_tz) 

elif service == "📊 Client Portal":
    # Handles timeline and depth profile tabs [cite: 7]
    render_client_portal(selected_project, display_tz, unit_mode, unit_label, active_refs)

elif service == "📉 Node Diagnostics":
    # Engineering view for sensor communication health 
    render_node_diagnostics(selected_project, display_tz, unit_mode, unit_label, active_refs)

elif service == "📤 Data Intake Lab":
    # Gatekept by admin check 
    if check_admin_access(service):
        render_data_intake_page(selected_project)

elif service == "🛠️ Admin Tools":
    # Gatekept by admin check 
    if check_admin_access(service):
        render_admin_page(selected_project, display_tz, unit_mode, unit_label, active_refs)
