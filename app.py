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
    Updated Data Engine: Uses 'approve' column for visibility logic as per schema.
    """
    cutoff = PROJECT_VISIBILITY_MASKS.get(project_id, "2000-01-01 00:00:00")
    
    if view_mode == "client":
        # Logic: Must be marked 'TRUE' (Approved) AND must NOT be marked 'MASKED'
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
        return df
    except Exception as e:
        st.error(f"BQ Error: {e}")
        return pd.DataFrame()

def check_admin_access(service_name):
    if st.session_state.get("admin_authenticated"): return True
    st.warning("🔒 Admin Access Required")
    pwd = st.text_input("Password", type="password", key=f"gate_{service_name}")
    if st.button("Unlock", key=f"btn_{service_name}"):
        if pwd == st.secrets["admin_password"]:
            st.session_state["admin_authenticated"] = True
            st.rerun()
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
# UPDATED: Default set to Pacific [cite: 7, 10]
display_tz = "US/Pacific"
active_refs = [(32.0, "Freezing")]

# --- 2. SIDEBAR WIDGETS ---
service = st.sidebar.selectbox(
    "📂 Page", 
    ["🏠 Executive Summary", "🌐 Global Overview", "📊 Client Portal", "📉 Node Diagnostics", "📤 Data Intake Lab", "🛠️ Admin Tools"]
)

unit_mode = st.sidebar.radio("Unit", ["Fahrenheit", "Celsius"])
unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"

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

# --- TIMEZONE DISPLAY ---
# UPDATED: Set index=2 to make "Local (US/Pacific)" the default selection [cite: 7]
tz_options = ["UTC", "Local (US/Eastern)", "Local (US/Pacific)"]
tz_mode = st.sidebar.selectbox("Timezone Display", tz_options, index=2)

display_tz = {
    "UTC": "UTC", 
    "Local (US/Eastern)": "US/Eastern", 
    "Local (US/Pacific)": "US/Pacific"
}[tz_mode]
########################
#- 4. GRAPHING ENGINE -#
########################

def build_high_speed_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label, display_tz="UTC"):
    """
    Stabilized Engine: Uses standard Scatter for stability and proper legend grouping.
    """
    if df.empty:
        return go.Figure().update_layout(title="No data available for the selected period.")

    plot_df = df.copy()
    
    # 1. TIMEZONE & UNIT CONVERSION
    plot_df['timestamp'] = plot_df['timestamp'].dt.tz_convert(display_tz)
    now_local = pd.Timestamp.now(tz=display_tz)
    
    if unit_mode == "Celsius":
        plot_df['temperature'] = (plot_df['temperature'] - 32) * 5/9
        y_range, dt_major, dt_minor = [-30, 30], 10, 5
    else:
        y_range, dt_major, dt_minor = [-20, 80], 10, 5

    # 2. LABELING & SORTING [cite: 6, 9]
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
    
    # Identify unique depth/bank groups
    unique_groups = plot_df[['depth_label', 'sort_val']].drop_duplicates().sort_values('sort_val')
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']

    for i, (_, g_row) in enumerate(unique_groups.iterrows()):
        group_lbl = g_row['depth_label']
        group_data = plot_df[plot_df['depth_label'] == group_lbl]
        color = colors[i % len(colors)]
        sensors = group_data['NodeNum'].unique()
        
        for j, sn in enumerate(sensors):
            s_df = group_data[group_data['NodeNum'] == sn].sort_values('timestamp')
            
            # Gap Detection (6h threshold) [cite: 14]
            if not is_surgical:
                s_df['gap_hrs'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
                gap_mask = s_df['gap_hrs'] > 6.0
                if gap_mask.any():
                    gaps = s_df[gap_mask].copy()
                    gaps['temperature'] = None
                    gaps['timestamp'] = gaps['timestamp'] - pd.Timedelta(minutes=1)
                    s_df = pd.concat([s_df, gaps]).sort_values('timestamp')

            # Using standard Scatter (not gl) for better multi-chart stability
            fig.add_trace(go.Scatter(
                x=s_df['timestamp'], 
                y=s_df['temperature'], 
                name=f"{group_lbl} ({sn})", 
                legendgroup=group_lbl, 
                showlegend=True if j == 0 else False, # Show only the first sensor of a group in legend
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

    # RESTORED: Red Vertical 'Now' Line
    fig.add_vline(x=now_local, line_width=2, line_color="Red", layer='above', line_dash="dash")

    # 5. GRID HIERARCHY & LAYOUT
    fig.update_layout(
        title={'text': f"<b>{title}</b>", 'x': 0},
        plot_bgcolor='white',
        hovermode="x unified",
        height=600,
        margin=dict(t=80, l=50, r=180, b=50),
        xaxis=dict(
            range=[start_view, end_view], 
            showline=True, mirror=True, linecolor='black',
            showgrid=True, dtick="D1", gridcolor='DarkGray', gridwidth=1,
            minor=dict(
                dtick=6*60*60*1000, 
                showgrid=True, 
                gridcolor='Gainsboro', 
                griddash='dash' 
            ),
            tickformat='%b %d\n%H:%M'
        ),
        yaxis=dict(
            title=f"Temperature ({unit_label})", 
            range=y_range, dtick=dt_major, 
            gridcolor='DarkGray', showline=True, mirror=True, linecolor='black',
            minor=dict(dtick=dt_minor, showgrid=True, gridcolor='whitesmoke')
        ),
        legend=dict(title="Sensors", orientation="v", x=1.02, y=1)
    )
    
    # Monday markers
    mondays = pd.date_range(start=start_view, end=end_view, freq='W-MON', tz=display_tz)
    for mon in mondays:
        fig.add_vline(x=mon, line_width=2, line_color="dimgray", layer="below")

    return fig
##################
# Page Functions #
##################

###########
# - 5. PAGE: GLOBAL OVERVIEW - #
###########

def render_global_overview(selected_project):
    """
    Shows all pipes/banks for the project selected in the sidebar.
    Engineering view: shows everything except 'FALSE'[cite: 16].
    """
    st.header("🌐 Global Project Overview")
    
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to view the Global Overview.")
        return

    # Use the project selected from the sidebar
    target_project = selected_project

    with st.spinner(f"Syncing {target_project} (Engineering View)..."):
        # Engineering view shows all data not explicitly rejected ('FALSE') [cite: 16]
        p_df = get_universal_portal_data(target_project, view_mode="engineering")

    if not p_df.empty:
        # 2. View Constraints
        lookback = st.sidebar.slider("Lookback (Weeks)", 1, 12, 4, key="global_lookback_slider")
        now_utc = pd.Timestamp.now(tz='UTC')
        
        # Snap end view to the upcoming Monday for consistent weekly alignment
        end_view = (now_utc + pd.Timedelta(days=(7-now_utc.weekday())%7 or 7)).replace(hour=0, minute=0, second=0, microsecond=0)
        start_view = end_view - timedelta(weeks=lookback)

        # 3. Render a graph for every physical location (Pipe/Bank) in the project [cite: 6]
        for loc in sorted(p_df['Location'].unique()):
            with st.expander(f"📍 Location: {loc}", expanded=True):
                loc_df = p_df[p_df['Location'] == loc]
                fig = build_high_speed_graph(
                    loc_df, f"📈 {target_project} - {loc}", 
                    start_view, end_view, tuple(active_refs), 
                    unit_mode, unit_label, display_tz=display_tz
                )
                st.plotly_chart(fig, use_container_width=True, key=f"ov_{target_project}_{loc}")
    else:
        st.info(f"No engineering data found for {target_project} in the last 84 days.")
###########
# - 6. PAGE: EXECUTIVE SUMMARY - #
###########

def render_executive_summary(client, selected_project, unit_label):
    st.header(f"🏠 Executive Summary: Health Monitor")
    
    # 1. Fuzzy Filter Logic
    proj_filter = ""
    if selected_project and selected_project != "All Projects":
        proj_filter = f"AND TRIM(Project) = '{selected_project.strip()}'"

    summary_q = f"""
        WITH MappedNodes AS (
            SELECT TRIM(Project) as Project, NodeNum, Location
            FROM `{PROJECT_ID}.{DATASET_ID}.metadata_snapshot`
            WHERE Project IS NOT NULL {proj_filter}
        ),
        RecentReporting AS (
            SELECT r.NodeNum, MAX(r.timestamp) as last_ping
            FROM (
                SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
                UNION ALL
                SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
            ) AS r
            WHERE r.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            GROUP BY NodeNum
        ),
        HistoricalPings AS (
            SELECT NodeNum, MAX(timestamp) as ever_ping
            FROM (
                SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
                UNION ALL
                SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
            ) GROUP BY NodeNum
        ),
        JoinedData AS (
            SELECT 
                m.Project, m.Location, m.NodeNum,
                CASE WHEN r.NodeNum IS NOT NULL THEN 1 ELSE 0 END as is_active,
                h.ever_ping
            FROM MappedNodes m
            LEFT JOIN RecentReporting r ON m.NodeNum = r.NodeNum
            LEFT JOIN HistoricalPings h ON m.NodeNum = h.NodeNum
        ),
        LocationStats AS (
            SELECT Project, Location, COUNT(NodeNum) as total, SUM(is_active) as active, MAX(ever_ping) as last_up
            FROM JoinedData GROUP BY Project, Location
        ),
        ProjectTotals AS (
            SELECT Project, '--- PROJECT TOTAL ---' as Location, COUNT(NodeNum) as total, SUM(is_active) as active, MAX(ever_ping) as last_up
            FROM JoinedData GROUP BY Project
        )
        SELECT * FROM ProjectTotals
        UNION ALL
        SELECT * FROM LocationStats
        ORDER BY Project ASC, (Location = '--- PROJECT TOTAL ---') DESC, Location ASC
    """
    
    try:
        with st.spinner("⚡ Auditing connectivity for all projects..."):
            df = client.query(summary_q).to_dataframe()
        
        if df.empty:
            st.warning("⚠️ No data found. Check if your Metadata table is populated.")
            return

        now_utc = pd.Timestamp.now(tz=pytz.UTC)

        def process_health_row(row):
            is_total = row['Location'] == '--- PROJECT TOTAL ---'
            last_ts = row['last_up']
            
            if pd.notnull(last_ts):
                last_ts = last_ts.tz_convert(pytz.UTC)
                gap = round((now_utc - last_ts).total_seconds() / 3600, 1)
                icon = "🟢" if gap < 2 else ("🟡" if gap < 8 else "🔴")
                time_str = f"{gap}h ago {icon}"
            else:
                time_str = "Never Seen ⚪"

            return pd.Series({
                "Project": f"⭐ {row['Project']}" if is_total else row['Project'],
                "Location": row['Location'],
                "Mapped": row['total'],
                "Active": row['active'],
                "Ratio": f"{row['active']}/{row['total']}",
                "Status": "✅ Healthy" if row['total'] == row['active'] else f"⚠️ {row['total'] - row['active']} Offline",
                "Last Activity": time_str
            })

        health_df = df.apply(process_health_row, axis=1)

        # Metrics based on Project Totals rows only
        totals_df = df[df['Location'] == '--- PROJECT TOTAL ---']
        m1, m2, m3 = st.columns(3)
        m1.metric("System Nodes", f"{totals_df['total'].sum()}")
        m2.metric("System Active", f"{totals_df['active'].sum()}")
        m3.metric("Uptime", f"{round((totals_df['active'].sum()/totals_df['total'].sum())*100, 1) if totals_df['total'].sum() > 0 else 0}%")

        st.divider()
        st.dataframe(health_df, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Executive Summary Error: {traceback.format_exc()}")

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
    Restored: Time Series, Vertical Profile, and Communication Health Table.
    """
    st.header(f"📉 Node Diagnostics: {selected_project}")
    
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Select a specific project in the sidebar.")
        return

    with st.spinner("🔍 Syncing diagnostic streams..."):
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
        weeks_view = st.slider("Lookback (Weeks)", 1, 12, 2, key="diag_weeks_slider")
    with c3:
        show_profile = st.checkbox("Show Vertical Profile", value=True)
            
    # Filter data for the selected Location and Timeframe
    now_utc = pd.Timestamp.now(tz='UTC')
    start_view = now_utc - timedelta(weeks=weeks_view)
    df_diag = all_data[all_data['Location'] == sel_loc].copy()
    df_filtered = df_diag[(df_diag['timestamp'] >= start_view) & (df_diag['timestamp'] <= now_utc)]

    # --- 1. ENGINEERING TIMELINE ---
    st.subheader("🕒 Engineering Timeline")
    fig_time = build_high_speed_graph(
        df_filtered, f"Diagnostic Stream: {sel_loc}", 
        start_view, now_utc + timedelta(hours=2), 
        tuple(active_refs), unit_mode, unit_label, display_tz
    )
    st.plotly_chart(fig_time, use_container_width=True)

    # --- 2. VERTICAL PROFILE (Depth vs Temp) ---
    if show_profile:
        st.divider()
        st.subheader("📏 Vertical Temperature Profile")
        df_diag['Depth_Num'] = pd.to_numeric(df_diag['Depth'], errors='coerce')
        profile_df = df_diag.dropna(subset=['Depth_Num']).copy()

        if profile_df.empty:
            st.info("No numeric depth data found for this location.")
        else:
            latest_snap = profile_df.sort_values('timestamp').groupby('Depth_Num').last().reset_index()
            
            # Using Lambda to avoid convert_val NameError
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

    # --- 3. COMMUNICATION HEALTH TABLE (Restored) ---
    st.divider()
    st.subheader("📋 Sensor Communication Health")
    
    # We use the raw df_diag to check the latest overall reporting time
    latest_nodes = df_diag.sort_values('timestamp').groupby('NodeNum').tail(1).copy()
    
    summary_rows = []
    for _, row in latest_nodes.iterrows():
        # Handle timestamp localization safely
        ts = row['timestamp'].tz_localize('UTC') if row['timestamp'].tzinfo is None else row['timestamp']
        hrs_ago = int((now_utc - ts).total_seconds() / 3600)
        
        # Status Logic: Green < 6h, Yellow < 24h, Red > 24h
        status_icon = "🔴" if hrs_ago > 24 else ("🟢" if hrs_ago < 6 else "🟡")
        
        # Map database 'is_approved' column to human-readable status
        db_status = str(row['is_approved']).upper()
        status_label = "✅ Approved" if db_status == "TRUE" else ("🚫 Masked" if db_status == "MASKED" else "⏳ Pending")

        # Temperature Conversion Lambda for table display
        f_temp = row['temperature']
        conv_temp = (f_temp - 32) * 5/9 if unit_mode == "Celsius" else f_temp

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
    Handles the Plotly Lasso tool to Approve, Mask, or Delete specific data points.
    """
    # 1. Fetch Engineering-level data
    p_df = get_universal_portal_data(selected_project, view_mode="engineering")
    if p_df.empty:
        st.info("No data available to scrub.")
        return

    # 2. Location Selection
    loc_options = sorted(p_df['Location'].dropna().unique())
    sel_loc = st.selectbox("Select Pipe to Clean", loc_options, key="surgical_loc_select")
    
    # Reset index so 'point_index' from Plotly aligns with the dataframe rows
    scrub_df = p_df[p_df['Location'] == sel_loc].copy().reset_index(drop=True)

    # 3. Persistent State for Selection
    if "locked_selection" not in st.session_state:
        st.session_state.locked_selection = None

    # 4. Build Marker Graph for Selection
    fig_scrub = build_high_speed_graph(
        scrub_df, f"Surgical Scrubbing: {sel_loc}", 
        pd.Timestamp.now(tz='UTC') - timedelta(days=14), 
        pd.Timestamp.now(tz='UTC') + timedelta(hours=6), 
        tuple(active_refs), unit_mode, unit_label, display_tz=display_tz
    )

    # Highlight previously selected points
    if st.session_state.locked_selection:
        indices = [p['point_index'] for p in st.session_state.locked_selection]
        fig_scrub.update_traces(selectedpoints=indices, unselected=dict(marker=dict(opacity=0.2)))

    # 5. Render Plot and Capture Selection Event
    event_data = st.plotly_chart(fig_scrub, use_container_width=True, on_select="rerun", key=f"scrub_{sel_loc}")

    if event_data and "selection" in event_data:
        pts = event_data["selection"].get("points", [])
        if len(pts) > 0:
            st.session_state.locked_selection = pts

    # 6. Action Buttons
    if st.session_state.locked_selection:
        st.success(f"📍 {len(st.session_state.locked_selection)} points selected.")
        c1, c2, c3, c4 = st.columns(4)
        
        with c1:
            if st.button("✅ APPROVE (Client)", use_container_width=True):
                update_records(st.session_state.locked_selection, scrub_df, "TRUE")
        with c2:
            if st.button("🚫 MASK (Internal)", use_container_width=True):
                update_records(st.session_state.locked_selection, scrub_df, "MASKED")
        with c3:
            if st.button("🗑️ DELETE (Full Reject)", type="primary", use_container_width=True):
                update_records(st.session_state.locked_selection, scrub_df, "FALSE")
        with c4:
            if st.button("Clear Selection", use_container_width=True):
                st.session_state.locked_selection = None
                st.rerun()

###########
# - 11. SURGICAL CLEANER HELPERS - #
###########

def update_records(pts, df, val):
    """
    Updated: Writes status updates into the 'approve' column.
    Matches updated schema: NodeNum (STRING), timestamp (TIMESTAMP), approve (STRING).
    """
    recs = []
    for p in pts:
        # Snap timestamp to the hour to match master/raw intervals [cite: 15]
        ts = pd.to_datetime(p['x']).tz_convert('UTC').floor('h')
        node = df.iloc[p['point_index']]['NodeNum']
        
        recs.append({
            "NodeNum": str(node), 
            "timestamp": ts, 
            "approve": val  # Using 'approve' column for TRUE, FALSE, or MASKED [cite: 13]
        })
    
    if recs:
        status_df = pd.DataFrame(recs).drop_duplicates(subset=['NodeNum', 'timestamp'])
        try:
            job = client.load_table_from_dataframe(status_df, OVERRIDE_TABLE)
            job.result() 
            st.session_state.locked_selection = None
            st.cache_data.clear()
            st.success(f"Successfully marked {len(recs)} points as {val}")
            time.sleep(1) 
            st.rerun()
        except Exception as e:
            st.error(f"Failed to update records: {e}")

###########
# - 12. MAIN ROUTER - #
###########

if service == "🌐 Global Overview":
    render_global_overview(selected_project) # Now passing the sidebar variable

elif service == "🏠 Executive Summary":
    render_executive_summary(client, selected_project, unit_label) 

elif service == "📊 Client Portal":
    render_client_portal(selected_project, display_tz, unit_mode, unit_label, active_refs)

elif service == "📉 Node Diagnostics":
    render_node_diagnostics(selected_project, display_tz, unit_mode, unit_label, active_refs)

elif service == "📤 Data Intake Lab":
    if check_admin_access(service):
        render_data_intake_page(selected_project)

elif service == "🛠️ Admin Tools":
    if check_admin_access(service):
        render_admin_page(selected_project, display_tz, unit_mode, unit_label, active_refs)
