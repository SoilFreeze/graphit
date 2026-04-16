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
    "Main_Site": "2026-01-01 00:00:00"
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
    Fetches data with independent time-masking for clients.
    """
    # 1. Get the global cutoff for this project
    cutoff = PROJECT_VISIBILITY_MASKS.get(project_id, "2000-01-01 00:00:00")
    
    if view_mode == "client":
        # Client ONLY sees Approved (TRUE) AND data after the visibility cutoff
        query_filter = f"AND rej.reason = 'TRUE' AND r.timestamp >= '{cutoff}'"
    else:
        # Engineering sees everything except deleted (FALSE)
        query_filter = "AND (rej.reason IS NULL OR rej.reason != 'FALSE')"

    query = f"""
        SELECT 
            r.NodeNum, r.timestamp, r.temperature,
            m.Location, m.Bank, m.Depth, m.Project,
            rej.reason as is_approved 
        FROM (
            SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
            UNION ALL
            SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
        ) AS r
        INNER JOIN `{PROJECT_ID}.{DATASET_ID}.metadata` AS m ON r.NodeNum = m.NodeNum
        LEFT JOIN `{OVERRIDE_TABLE}` AS rej 
            ON r.NodeNum = rej.NodeNum 
            AND TIMESTAMP_TRUNC(TIMESTAMP_ADD(r.timestamp, INTERVAL 30 MINUTE), HOUR) = 
                TIMESTAMP_TRUNC(TIMESTAMP_ADD(rej.timestamp, INTERVAL 30 MINUTE), HOUR)
        WHERE m.Project = '{project_id}'
        {query_filter}
        AND r.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 84 DAY)
        ORDER BY m.Location ASC, r.timestamp ASC
    """
    try:
        df = client.query(query).to_dataframe()
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
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

# --- 1. INITIALIZE FALLBACKS (Prevents NameError) ---
# These ensure the variables exist even if a query fails
service = "🏠 Executive Summary"
unit_mode = "Fahrenheit"
unit_label = "°F"
selected_project = "All Projects"
display_tz = "UTC"
active_refs = [(32.0, "Freezing")]

# --- 2. SIDEBAR WIDGETS ---
service = st.sidebar.selectbox(
    "📂 Page", 
    ["🏠 Executive Summary", "🌐 Global Overview", "📊 Client Portal", "📉 Node Diagnostics", "📤 Data Intake Lab", "🛠️ Admin Tools"]
)

unit_mode = st.sidebar.radio("Unit", ["Fahrenheit", "Celsius"])
unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"

# Robust Project Selection
if client is not None:
    try:
        proj_q = f"SELECT DISTINCT TRIM(Project) as Project FROM `{PROJECT_ID}.{DATASET_ID}.metadata` WHERE Project IS NOT NULL"
        proj_df = client.query(proj_q).to_dataframe()
        proj_list = sorted(proj_df['Project'].dropna().unique())
        options = ["All Projects"] + proj_list
        selected_project = st.sidebar.selectbox("🎯 Active Project", options, index=0, key="sidebar_proj_picker_final")
    except Exception as e:
        st.sidebar.error("Database connection lag. Defaulting to 'All Projects'.")
        selected_project = "All Projects"

# Reference Lines
st.sidebar.subheader("📏 Reference Lines")
active_refs = [] # Reset and rebuild based on checkboxes
if st.sidebar.checkbox("Freezing (32°F)", value=True): 
    active_refs.append((32.0, "Freezing"))
if st.sidebar.checkbox("Type B (26.6°F)", value=False): 
    active_refs.append((26.6, "Type B"))
if st.sidebar.checkbox("Type A (10.2°F)", value=False): 
    active_refs.append((10.2, "Type A"))

# Timezone Display
tz_mode = st.sidebar.selectbox("Timezone Display", ["UTC", "Local (US/Eastern)", "Local (US/Pacific)"])
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
    Standard Plotly engine for the entire app.
    Restored with Grid Hierarchy (Mondays/Midnights) and localized axis framing.
    """
    if df.empty:
        return go.Figure().update_layout(title="No data available for the selected period.")

    plot_df = df.copy()
    
    # 1. TIMEZONE CONVERSION
    plot_df['timestamp'] = plot_df['timestamp'].dt.tz_convert(display_tz)
    
    # Adjust axes windows to match the localized zone for correct framing
    tz = pytz.timezone(display_tz)
    start_local = start_view.astimezone(tz) if hasattr(start_view, 'astimezone') else start_view
    end_local = end_view.astimezone(tz) if hasattr(end_view, 'astimezone') else end_view
    now_local = pd.Timestamp.now(tz=display_tz)

    # 2. UNIT CONVERSION
    if unit_mode == "Celsius":
        plot_df['temperature'] = (plot_df['temperature'] - 32) * 5/9
        y_range, dt_major = [-30, 30], 5
    else:
        y_range, dt_major = [-20, 80], 10

    # 3. LABELING LOGIC
    plot_df['label'] = plot_df.apply(
        lambda r: f"Bank {r['Bank']} ({r['NodeNum']})" if str(r.get('Bank')).strip().lower() not in ["", "none", "nan", "null"]
        else f"{r.get('Depth')}ft ({r.get('NodeNum')})", axis=1
    )
    
    # 4. PLOT MODE
    is_surgical = any(word in title for word in ["Scrubbing", "Surgical", "Diag"])
    plot_mode = 'markers' if is_surgical else 'lines'
    marker_size = 7 if is_surgical else 3

    fig = go.Figure()
    
    for lbl in sorted(plot_df['label'].unique()):
        s_df = plot_df[plot_df['label'] == lbl].sort_values('timestamp')
        hover_name = lbl.split('(')[0].strip()

        # 5. GAP DETECTION
        if not is_surgical:
            s_df['gap_hrs'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
            gap_mask = s_df['gap_hrs'] > 6.0
            if gap_mask.any():
                gaps = s_df[gap_mask].copy()
                gaps['temperature'] = None
                gaps['timestamp'] = gaps['timestamp'] - pd.Timedelta(minutes=1)
                s_df = pd.concat([s_df, gaps]).sort_values('timestamp')

        # 6. ADD TRACE
        fig.add_trace(go.Scattergl(
            x=s_df['timestamp'], 
            y=s_df['temperature'], 
            name=lbl, 
            mode=plot_mode,
            marker=dict(size=marker_size, opacity=0.8 if is_surgical else 1.0),
            connectgaps=False,
            customdata=[hover_name] * len(s_df),
            hovertemplate=f"<b>%{{customdata}}</b>: %{{y:.1f}}{unit_label}<extra></extra>"
        ))

    # 7. RESTORED GRID HIERARCHY
    grid_times = pd.date_range(start=start_local, end=end_local, freq='6h', tz=display_tz)
    for ts in grid_times:
        if ts.weekday() == 0 and ts.hour == 0:
            color, width = "Black", 1.2 # Mondays
        elif ts.hour == 0:
            color, width = "Gray", 0.8  # Midnights
        else:
            color, width = "LightGray", 0.3 # 6-hour marks
        fig.add_vline(x=ts, line_width=width, line_color=color, layer='below')

    # 8. REFERENCE LINES & "NOW" MARKER
    for val, ref_label in active_refs:
        c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
        fig.add_hline(y=c_val, line_dash="dash", line_color="RoyalBlue", 
                      annotation_text=ref_label, annotation_position="top right")

    fig.add_vline(x=now_local, line_width=2, line_color="Red", layer='above', line_dash="dash")

    # 9. FINAL LAYOUT
    fig.update_layout(
        title={'text': f"{title} ({display_tz})", 'x': 0},
        plot_bgcolor='white',
        hovermode="x unified",
        height=600,
        margin=dict(t=80, l=50, r=180, b=50),
        xaxis=dict(
            range=[start_local, end_local], 
            showline=True, 
            linecolor='black', 
            mirror=True, 
            tickformat='%b %d\n%H:%M',
            gridcolor='rgba(0,0,0,0)' # Hide default grid to use our custom hierarchy
        ),
        yaxis=dict(
            title=f"Temperature ({unit_label})", 
            range=y_range, 
            dtick=dt_major, 
            gridcolor='Gainsboro', 
            showline=True, 
            linecolor='black', 
            mirror=True
        ),
        legend=dict(title="Sensors", orientation="v", x=1.02, y=1, xanchor="left")
    )
    
    return fig
##################
# Page Functions #
##################

###########
# - 5. PAGE: GLOBAL OVERVIEW - #
###########

def render_global_overview():
    """
    Shows all pipes/banks for a selected project in one scrolling view.
    Engineering view: shows everything except 'FALSE'.
    """
    st.header("🌐 Global Project Overview")
    
    # 1. Project Selection from Metadata
    proj_list_q = f"SELECT DISTINCT Project FROM `{PROJECT_ID}.{DATASET_ID}.metadata` WHERE Project IS NOT NULL"
    try:
        available_projects = sorted(client.query(proj_list_q).to_dataframe()['Project'].tolist())
        target_project = st.selectbox("🏗️ Select a Project to Review", available_projects, key="global_proj_picker")
    except Exception as e:
        st.error(f"Metadata Error: {e}")
        return

    if target_project:
        with st.spinner(f"Syncing {target_project} (Engineering View)..."):
            # Engineering view shows all data not explicitly rejected ('FALSE') in 'reason' column
            p_df = get_universal_portal_data(target_project, view_mode="engineering")

        if not p_df.empty:
            # 2. View Constraints
            lookback = st.sidebar.slider("Lookback (Weeks)", 1, 12, 4, key="global_lookback_slider")
            now_utc = pd.Timestamp.now(tz='UTC')
            # Snap end view to the upcoming Monday for consistent weekly alignment
            end_view = (now_utc + pd.Timedelta(days=(7-now_utc.weekday())%7 or 7)).replace(hour=0, minute=0, second=0, microsecond=0)
            start_view = end_view - timedelta(weeks=lookback)

            # 3. Render a graph for every physical location (Pipe/Bank) in the project
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
            FROM `{PROJECT_ID}.{DATASET_ID}.metadata`
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
    """
    The strictly filtered view for clients. 
    Only shows data where manual_rejections.reason = 'TRUE'.
    """
    st.header(f"📊 Project Status: {selected_project}")
    
    # Use the global client defined at the top of the app
    global client

    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to view approved data.")
        return
    
    # 1. FETCH DATA (Client View)
    with st.spinner("Loading approved portal data..."):
        p_df = get_universal_portal_data(selected_project, view_mode="client")
    
    if p_df.empty:
        st.info(f"No approved data is currently available for {selected_project}. Data must be approved in Admin Tools.")
    else:
        tab_time, tab_depth, tab_table = st.tabs(["📈 Timeline Analysis", "📏 Depth Profile", "📋 Summary Table"])

        with tab_time:
            # Viewing window: Default 6 weeks
            weeks_view = st.slider("Weeks to View", 1, 12, 6, key="client_weeks_slider")
            end_view = pd.Timestamp.now(tz='UTC')
            start_view = end_view - timedelta(weeks=weeks_view)
            
            for loc in sorted(p_df['Location'].dropna().unique()):
                with st.expander(f"📈 {loc}", expanded=True):
                    loc_data = p_df[p_df['Location'] == loc]
                    fig = build_high_speed_graph(
                        loc_data, loc, start_view, end_view, 
                        tuple(active_refs), unit_mode, unit_label, display_tz
                    )
                    st.plotly_chart(fig, use_container_width=True, key=f"client_time_{loc}")

        with tab_depth:
            st.subheader("📏 Vertical Temperature Profile")
            p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
            depth_only = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
            
            for loc in sorted(depth_only['Location'].unique()):
                with st.expander(f"📏 {loc} Weekly Snapshots", expanded=True):
                    loc_data = depth_only[depth_only['Location'] == loc].copy()
                    fig_d = go.Figure()
                    
                    # Generate Monday 6AM snapshots for the last 6 weeks
                    mondays = pd.date_range(end=pd.Timestamp.now(tz='UTC'), periods=6, freq='W-MON')
                    
                    for m_date in mondays:
                        target_ts = m_date.replace(hour=6, minute=0, second=0)
                        # Look for data within +/- 12 hours of target
                        window = loc_data[(loc_data['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & 
                                          (loc_data['timestamp'] <= target_ts + pd.Timedelta(hours=12))]
                        
                        if not window.empty:
                            snap_list = []
                            for node in window['NodeNum'].unique():
                                node_data = window[window['NodeNum'] == node].copy()
                                node_data['diff'] = (node_data['timestamp'] - target_ts).abs()
                                snap_list.append(node_data.sort_values('diff').iloc[0])
                            
                            snap_df = pd.DataFrame(snap_list).sort_values('Depth_Num')
                            
                            # FIX: Using Lambda to prevent NameError
                            converted_temps = snap_df['temperature'].apply(
                                lambda x: (x - 32) * 5/9 if unit_mode == "Celsius" else x
                            )
                            
                            fig_d.add_trace(go.Scattergl(
                                x=converted_temps, 
                                y=snap_df['Depth_Num'], 
                                mode='lines+markers', 
                                name=target_ts.strftime('%m/%d/%y')
                            ))

                    y_limit = int(((loc_data['Depth_Num'].max() // 10) + 1) * 10) if not loc_data.empty else 50
                    fig_d.update_layout(
                        plot_bgcolor='white', height=700,
                        xaxis=dict(title=f"Temp ({unit_label})", gridcolor='Gainsboro', showline=True, linecolor='black', mirror=True),
                        yaxis=dict(title="Depth (ft)", range=[y_limit, 0], dtick=10, gridcolor='Silver', showline=True, linecolor='black', mirror=True),
                        legend=dict(title="Weekly Snapshots (6AM)", orientation="h", y=-0.15)
                    )
                    st.plotly_chart(fig_d, use_container_width=True, key=f"client_depth_{loc}")

        with tab_table:
            latest = p_df.sort_values('timestamp').groupby('NodeNum').tail(1).copy()
            # FIX: Using Lambda to prevent NameError
            latest['Current Temp'] = latest['temperature'].apply(
                lambda x: f"{round((x - 32) * 5/9 if unit_mode == 'Celsius' else x, 1)}{unit_label}"
            )
            latest['Position'] = latest.apply(lambda r: f"Bank {r['Bank']}" if pd.notnull(r['Bank']) and str(r['Bank']).strip() != "" else f"{r.get('Depth', '??')} ft", axis=1)
            st.dataframe(latest[['Location', 'Position', 'Current Temp', 'NodeNum']].sort_values(['Location', 'Position']), use_container_width=True, hide_index=True)
            
###########
# - 8. PAGE: NODE DIAGNOSTICS - #
###########

def render_node_diagnostics(selected_project, display_tz, unit_mode, unit_label, active_refs):
    """
    Engineering-level view. Shows everything (Pending, Masked, Approved).
    Restored Vertical Profile with fix for Scattergl ValueError.
    """
    st.header(f"📉 Node Diagnostics: {selected_project}")
    
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Select a specific project in the sidebar.")
        return

    with st.spinner("🔍 Syncing diagnostic streams..."):
        all_data = get_universal_portal_data(selected_project, view_mode="engineering")
    
    if all_data.empty:
        st.warning(f"No diagnostic data found for project {selected_project}.")
    else:
        loc_options = sorted(all_data['Location'].dropna().unique())
        c1, c2, c3 = st.columns([2, 1, 1])
        with c1:
            sel_loc = st.selectbox("Select Pipe / Bank to Analyze", loc_options, key="diag_loc_select")
        with c2:
            weeks_view = st.slider("Lookback (Weeks)", 1, 12, 2, key="diag_weeks_slider")
        with c3:
            show_profile = st.checkbox("Show Vertical Profile", value=True)
            
        df_diag = all_data[all_data['Location'] == sel_loc].copy()

        # 1. Engineering Timeline (Keep Scattergl for performance here)
        st.subheader("🕒 Engineering Timeline")
        fig_time = build_high_speed_graph(
            df_diag, f"Diagnostic Stream: {sel_loc}", 
            pd.Timestamp.now(tz='UTC') - timedelta(weeks=weeks_view), 
            pd.Timestamp.now(tz='UTC') + timedelta(hours=2), 
            tuple(active_refs), unit_mode, unit_label, display_tz
        )
        st.plotly_chart(fig_time, use_container_width=True)

        # 2. RESTORED Vertical Profile
        if show_profile:
            st.divider()
            st.subheader("📏 Vertical Temperature Profile")
            
            # Ensure Depth is numeric
            df_diag['Depth_Num'] = pd.to_numeric(df_diag['Depth'], errors='coerce')
            profile_df = df_diag.dropna(subset=['Depth_Num']).copy()

            if profile_df.empty:
                st.info("No numeric depth data found for this location.")
            else:
                # Get the absolute latest reading for each depth sensor
                latest_snap = profile_df.sort_values('timestamp').groupby('Depth_Num').last().reset_index()
                
                # Lambda fix for NameError: convert_val
                latest_snap['conv_temp'] = latest_snap['temperature'].apply(
                    lambda x: (x - 32) * 5/9 if unit_mode == "Celsius" else x
                )

                # Using standard go.Scatter to allow for smooth 'spline' curves
                fig_d = go.Figure()
                fig_d.add_trace(go.Scatter(
                    x=latest_snap['conv_temp'], 
                    y=latest_snap['Depth_Num'], 
                    mode='lines+markers',
                    name="Current Profile",
                    line=dict(shape='spline', smoothing=0.5, width=3, color='RoyalBlue'),
                    marker=dict(size=10, symbol='diamond')
                ))

                # Set Y-axis so 0 (surface) is at the top
                y_limit = int(((profile_df['Depth_Num'].max() // 10) + 1) * 10)
                fig_d.update_layout(
                    title=f"Latest Vertical Snapshot: {sel_loc}",
                    plot_bgcolor='white', 
                    height=700,
                    xaxis=dict(title=f"Temp ({unit_label})", gridcolor='Gainsboro', showline=True, linecolor='black'),
                    yaxis=dict(title="Depth (ft)", range=[y_limit, 0], dtick=10, gridcolor='Silver', showline=True, linecolor='black')
                )
                
                # Add reference lines to vertical profile
                for val, ref_label in active_refs:
                    # Convert ref line value if needed
                    c_ref = (val - 32) * 5/9 if unit_mode == "Celsius" else val
                    fig_d.add_vline(x=c_ref, line_dash="dash", line_color="Red", annotation_text=ref_label)

                st.plotly_chart(fig_d, use_container_width=True)
###########
# - 9. PAGE: DATA INTAKE LAB - #
###########

def render_data_intake_page(selected_project):
    st.header("📤 Data Ingestion Lab")
    tab_upload, tab_export = st.tabs(["📄 Upload", "📥 Export"])
    
    with tab_upload:
        # ... (keep your existing upload logic here) ...
        st.subheader("📄 Manual File Ingestion")
        st.info("Upload Lord SensorConnect (Wide), Lord Desktop (Narrow), or SensorPush (CSV/Excel).")
        
        u_file = st.file_uploader("Upload Data File", type=['csv', 'xlsx', 'xls'], key="manual_upload_main")
        
        if u_file is not None:
            # [Your existing parsing logic from the provided file]
            pass

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
    tab_bulk, tab_scrub, tab_surgical = st.tabs(["✅ Bulk Approval", "🧹 Scrub", "🧨 Surgical"])

    with tab_bulk:
        st.subheader("✅ Range-Based Bulk Approval")
        st.write("Approve all data within this specific window:")
        
        c1, c2 = st.columns(2)
        with c1:
            b_start = st.date_input("Approval Start", value=datetime.now() - timedelta(days=7), key="b_start")
        with c2:
            b_end = st.date_input("Approval End", value=datetime.now(), key="b_end")

        if st.button(f"🚀 Approve {selected_project} Range"):
            with st.spinner("Writing approvals..."):
                bulk_sql = f"""
                    INSERT INTO `{OVERRIDE_TABLE}` (NodeNum, timestamp, reason)
                    SELECT DISTINCT r.NodeNum, TIMESTAMP_TRUNC(r.timestamp, HOUR), 'TRUE'
                    FROM (SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` UNION ALL SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`) AS r
                    INNER JOIN `{PROJECT_ID}.{DATASET_ID}.metadata` AS m ON r.NodeNum = m.NodeNum
                    WHERE m.Project = '{selected_project}'
                    AND r.timestamp >= '{b_start}' AND r.timestamp <= '{b_end}'
                    AND NOT EXISTS (SELECT 1 FROM `{OVERRIDE_TABLE}` x WHERE x.NodeNum = r.NodeNum AND x.timestamp = TIMESTAMP_TRUNC(r.timestamp, HOUR))
                """
                client.query(bulk_sql).result()
                st.success("Range successfully approved.")
                st.cache_data.clear()

    with tab_scrub:
        st.subheader("🧹 Deep Data Scrub")
        st.warning("Averages raw data to 1-hour intervals. This is irreversible.")
        scrub_target = st.radio("Target Table", ["SensorPush", "Lord"], horizontal=True, key="admin_scrub_select")
        t_table = f"{PROJECT_ID}.{DATASET_ID}.raw_{scrub_target.lower()}"
        
        if st.button(f"🧨 Purge & Average {scrub_target}"):
            with st.spinner("Processing Mean Reduction..."):
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
                try:
                    client.query(scrub_sql).result()
                    st.success(f"✅ {scrub_target} table successfully averaged.")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"Scrub Error: {e}")

    with tab_surgical:
        if not selected_project:
            st.warning("Please select a project in the sidebar.")
        else:
            # Calls the Lasso function defined previously
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
    Final corrected version: Writes status updates into the 'reason' column.
    Matches schema: NodeNum (STRING), timestamp (TIMESTAMP), reason (STRING).
    """
    recs = []
    for p in pts:
        # Snap timestamp to the hour to match master/raw intervals
        ts = pd.to_datetime(p['x']).tz_convert('UTC').floor('h')
        node = df.iloc[p['point_index']]['NodeNum']
        
        recs.append({
            "NodeNum": str(node), 
            "timestamp": ts, 
            "reason": val
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
    render_global_overview()

elif service == "🏠 Executive Summary":
    # Pass 'client' into the function call here
    render_executive_summary(client, selected_project, unit_label) 

elif service == "📊 Client Portal":
    # Ensure there are exactly 5 variables here to match the 5 in the definition above
    render_client_portal(selected_project, display_tz, unit_mode, unit_label, active_refs)
elif service == "📉 Node Diagnostics":
    render_node_diagnostics(selected_project, display_tz, unit_mode, unit_label, active_refs)
elif service == "📤 Data Intake Lab":
    if check_admin_access(service):
        render_data_intake_page(selected_project)
elif service == "🛠️ Admin Tools":
    if check_admin_access(service):
        render_admin_page(selected_project, display_tz, unit_mode, unit_label, active_refs)
