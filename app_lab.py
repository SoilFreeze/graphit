import streamlit as st
import pandas as pd
import time
import plotly.express as px
import plotly.graph_objects as go
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
# manual_rejections is our override table for status flags
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"

@st.cache_resource
def get_bq_client():
    """Handles authentication with BigQuery."""
    try:
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(info)
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
    Fetches data and joins with manual_rejections for status flags.
    Matches the schema: NodeNum, timestamp, and 'reason' (as status).
    """
    # Filter based on the 'reason' column in manual_rejections
    if view_mode == "client":
        # Client view: ONLY shows points explicitly approved ('TRUE')
        approval_filter = "AND rej.reason = 'TRUE'"
    else:
        # Engineering view: Shows everything NOT explicitly deleted ('FALSE')
        approval_filter = "AND (rej.reason IS NULL OR rej.reason != 'FALSE')"

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
        {approval_filter}
        AND r.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 84 DAY)
        ORDER BY m.Location ASC, r.timestamp ASC
    """
    try:
        df = client.query(query).to_dataframe()
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
            if 'Bank' not in df.columns: df['Bank'] = ""
        return df
    except Exception as e:
        st.error(f"BigQuery Error: {e}")
        return pd.DataFrame()

def check_admin_access(current_service):
    """Security gate with unique keys to prevent DuplicateElementId errors."""
    if "admin_authenticated" not in st.session_state:
        st.session_state["admin_authenticated"] = False
    
    if st.session_state["admin_authenticated"]: 
        return True
    
    st.warning("🔒 Restricted Area: Engineering Admin Only.")
    # Unique ID based on service prevents widget collisions
    unique_id = current_service.replace(" ", "_").lower()
    pwd_input = st.text_input("Enter Admin Password", type="password", key=f"pwd_{unique_id}")
    
    if st.button("Unlock Tools", key=f"btn_{unique_id}"):
        if "admin_password" in st.secrets and pwd_input == st.secrets["admin_password"]:
            st.session_state["admin_authenticated"] = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    return False

###########################
#- 3. SIDEBAR UI & STATE -#
###########################
st.sidebar.title("❄️ SoilFreeze Lab")

# 1. Navigation
service = st.sidebar.selectbox("📂 Select Page", 
    ["🌐 Global Overview", "🏠 Executive Summary", "📊 Client Portal", "📉 Node Diagnostics", "📤 Data Intake Lab", "🛠️ Admin Tools"])

# 2. Units
unit_mode = st.sidebar.radio("Temperature Unit", ["Fahrenheit", "Celsius"])
unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"

def convert_val(f_val):
    """Converts Fahrenheit from DB to selected unit."""
    if f_val is None or pd.isna(f_val): return None
    return (f_val - 32) * 5/9 if unit_mode == "Celsius" else f_val

# 3. Project Selection (Sidebar)
selected_project = None
if service in ["📊 Client Portal", "📉 Node Diagnostics", "🛠️ Admin Tools", "🏠 Executive Summary"]:
    try:
        proj_q = f"SELECT DISTINCT Project FROM `{PROJECT_ID}.{DATASET_ID}.metadata` WHERE Project IS NOT NULL"
        proj_list = sorted(client.query(proj_q).to_dataframe()['Project'].dropna().unique())
        selected_project = st.sidebar.selectbox("🎯 Active Project", proj_list)
    except:
        st.sidebar.warning("Could not load project list.")

# 4. Reference Lines
st.sidebar.subheader("📏 Reference Lines")
active_refs = []
if st.sidebar.checkbox("Freezing (32°F)", value=True): active_refs.append((32.0, "Freezing"))
if st.sidebar.checkbox("Type B (26.6°F)", value=True): active_refs.append((26.6, "Type B"))

# 5. Timezone Display
tz_mode = st.sidebar.selectbox("Timezone Display", ["UTC", "Local (US/Eastern)", "Local (US/Pacific)"])
display_tz = {"UTC": "UTC", "Local (US/Eastern)": "US/Eastern", "Local (US/Pacific)": "US/Pacific"}[tz_mode]

########################
#- 4. GRAPHING ENGINE -#
########################

def build_high_speed_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label, display_tz="UTC"):
    """
    Standard Plotly engine for the entire app.
    Handles unit conversion, timezone shifting, and gap detection.
    """
    if df.empty:
        return go.Figure().update_layout(title="No data available for the selected period.")

    plot_df = df.copy()
    
    # 1. TIMEZONE CONVERSION
    # Shifting the UTC timestamps to the user's selected display zone
    plot_df['timestamp'] = plot_df['timestamp'].dt.tz_convert(display_tz)
    
    # Adjust axes windows to match the localized zone for correct framing
    start_local = start_view.astimezone(pytz.timezone(display_tz))
    end_local = end_view.astimezone(pytz.timezone(display_tz))
    now_local = pd.Timestamp.now(tz=display_tz)

    # 2. UNIT CONVERSION
    # BigQuery stores everything in Fahrenheit; we convert here if the user selected Celsius
    if unit_mode == "Celsius":
        plot_df['temperature'] = (plot_df['temperature'] - 32) * 5/9
        y_range, dt_major = [-30, 30], 5
    else:
        y_range, dt_major = [-20, 80], 10

    # 3. LABELING LOGIC
    # Priority: Bank Name (if available) -> Depth
    plot_df['label'] = plot_df.apply(
        lambda r: f"Bank {r['Bank']} ({r['NodeNum']})" if str(r.get('Bank')).strip().lower() not in ["", "none", "nan", "null"]
        else f"{r.get('Depth')}ft ({r.get('NodeNum')})", axis=1
    )
    
    # 4. PLOT MODE (Markers for Admin/Surgical for selection, Lines for standard viewing)
    is_surgical = any(word in title for word in ["Scrubbing", "Surgical", "Diag"])
    plot_mode = 'markers' if is_surgical else 'lines'
    marker_size = 7 if is_surgical else 3

    fig = go.Figure()
    
    for lbl in sorted(plot_df['label'].unique()):
        s_df = plot_df[plot_df['label'] == lbl].sort_values('timestamp')
        hover_name = lbl.split('(')[0].strip()

        # 5. GAP DETECTION
        # Prevents "stretching" lines across multi-hour data outages
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

    # 7. GRID HIERARCHY (Visual cues for time blocks)
    # Mondays are black, Midnights are gray, 6-hour intervals are light gray
    grid_times = pd.date_range(start=start_local, end=end_local, freq='6h', tz=display_tz)
    for ts in grid_times:
        if ts.weekday() == 0 and ts.hour == 0:
            color, width = "Black", 1.2 
        elif ts.hour == 0:
            color, width = "Gray", 0.8  
        else:
            color, width = "LightGray", 0.3
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
        xaxis=dict(range=[start_local, end_local], showline=True, linecolor='black', mirror=True, tickformat='%b %d\n%H:%M'),
        yaxis=dict(title=f"Temperature ({unit_label})", range=y_range, dtick=dt_major, gridcolor='Gainsboro', showline=True, linecolor='black', mirror=True),
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

def render_executive_summary(selected_project, unit_label):
    """
    Command Center view: Shows 24-hour health, min/max temps, and delta magnitude.
    """
    st.header(f"🏠 Executive Summary: {selected_project if selected_project else 'All Projects'}")
    
    st.write("### ↕️ Sorting & View Options")
    c1, c2 = st.columns([1, 1])
    with c1:
        sort_choice = st.selectbox("Sort By", ["None", "Hours Since Last Seen", "Delta Magnitude"])
    with c2:
        sort_order = st.radio("Order", ["Descending", "Ascending"], horizontal=True)
    
    # 1. Complex Summary Query
    # Calculates the delta (current vs 24h ago) and finds latest rank per node
    summary_q = f"""
        WITH RecentData AS (
            SELECT 
                r.NodeNum, r.timestamp, r.temperature, 
                m.Project, m.Location, m.Bank, m.Depth,
                FIRST_VALUE(r.temperature) OVER(PARTITION BY r.NodeNum ORDER BY r.timestamp ASC) as first_temp_24h,
                ROW_NUMBER() OVER(PARTITION BY r.NodeNum ORDER BY r.timestamp DESC) as latest_rank
            FROM (
                SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
                UNION ALL
                SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
            ) AS r
            INNER JOIN `{PROJECT_ID}.{DATASET_ID}.metadata` AS m ON r.NodeNum = m.NodeNum
            WHERE r.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            {"AND m.Project = '" + selected_project + "'" if selected_project else ""}
        )
        SELECT 
            NodeNum, Project, Location, Bank, Depth, timestamp, temperature,
            first_temp_24h,
            MIN(temperature) OVER(PARTITION BY NodeNum) as min_24h,
            MAX(temperature) OVER(PARTITION BY NodeNum) as max_24h
        FROM RecentData
        WHERE latest_rank = 1
    """
    
    try:
        with st.spinner("⚡ Fetching Command Center Snapshot..."):
            raw_summary_df = client.query(summary_q).to_dataframe()
        
        if raw_summary_df.empty:
            st.warning("📡 No active sensors seen in the last 24 hours.")
        else:
            now_utc = pd.Timestamp.now(tz=pytz.UTC)
            
            # 2. Process dataframe for display
            def process_summary_row(row):
                ts = row['timestamp'].tz_localize(pytz.UTC) if row['timestamp'].tzinfo is None else row['timestamp']
                hrs_ago = int((now_utc - ts).total_seconds() / 3600)
                status_icon = "🟢" if hrs_ago < 6 else ("🟡" if hrs_ago < 12 else "🟠" if hrs_ago < 24 else "🔴")
                
                # Delta is always calculated in Fahrenheit for magnitude consistency
                raw_delta = row['temperature'] - row['first_temp_24h']
                
                return pd.Series({
                    "Project": row['Project'],
                    "Node": row['NodeNum'],
                    "Location": row['Location'],
                    "Position": f"Bank {row['Bank']}" if str(row['Bank']).strip() not in ["", "None", "nan", "NaN"] else f"{row['Depth']} ft",
                    "Min": f"{round(convert_val(row['min_24h']), 1)}{unit_label}",
                    "Max": f"{round(convert_val(row['max_24h']), 1)}{unit_label}",
                    "Delta_Val": raw_delta, 
                    "Delta": f"{'+' if raw_delta > 0 else ''}{round(raw_delta, 1)}°F",
                    "Hours_Ago": hrs_ago,
                    "Last Seen": f"{ts.strftime('%m/%d %H:%M')} ({hrs_ago}h) {status_icon}"
                })

            summary_df = raw_summary_df.apply(process_summary_row, axis=1)

            # 3. Sorting Logic
            asc = (sort_order == "Ascending")
            if sort_choice == "Hours Since Last Seen":
                summary_df = summary_df.sort_values(by="Hours_Ago", ascending=asc)
            elif sort_choice == "Delta Magnitude":
                summary_df = summary_df.sort_values(by="Delta_Val", key=abs, ascending=asc)

            # 4. Display
            st.dataframe(
                summary_df[["Project", "Node", "Location", "Position", "Min", "Max", "Delta", "Last Seen"]],
                use_container_width=True, hide_index=True
            )
            
    except Exception as e:
        st.error(f"Executive Summary Error: {e}")

###########
# - 7. PAGE: CLIENT PORTAL - #
###########

def render_client_portal(selected_project, display_tz, unit_mode, unit_label, active_refs):
    """
    The strictly filtered view for clients. 
    Only shows data where manual_rejections.reason = 'TRUE'.
    """
    st.header(f"📊 Project Status: {selected_project}")
    
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
                        # Look for data within +/- 12 hours of the target Monday 6AM
                        window = loc_data[(loc_data['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & 
                                          (loc_data['timestamp'] <= target_ts + pd.Timedelta(hours=12))]
                        
                        if not window.empty:
                            snap_list = []
                            for node in window['NodeNum'].unique():
                                node_data = window[window['NodeNum'] == node].copy()
                                node_data['diff'] = (node_data['timestamp'] - target_ts).abs()
                                snap_list.append(node_data.sort_values('diff').iloc[0])
                            
                            snap_df = pd.DataFrame(snap_list).sort_values('Depth_Num')
                            fig_d.add_trace(go.Scattergl(
                                x=snap_df['temperature'].apply(convert_val), 
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
            # Latest Snapshot Table
            latest = p_df.sort_values('timestamp').groupby('NodeNum').tail(1).copy()
            latest['Current Temp'] = latest['temperature'].apply(lambda x: f"{round(convert_val(x), 1)}{unit_label}")
            latest['Position'] = latest.apply(lambda r: f"Bank {r['Bank']}" if pd.notnull(r['Bank']) and str(r['Bank']).strip() != "" else f"{r.get('Depth', '??')} ft", axis=1)
            st.dataframe(latest[['Location', 'Position', 'Current Temp', 'NodeNum']].sort_values(['Location', 'Position']), use_container_width=True, hide_index=True)
            
###########
# - 8. PAGE: NODE DIAGNOSTICS - #
###########

def render_node_diagnostics(selected_project, display_tz, unit_mode, unit_label, active_refs):
    """
    Engineering-level view. Shows everything (Pending, Masked, Approved).
    Used for troubleshooting sensor health and communication gaps.
    """
    st.header(f"📉 Node Diagnostics: {selected_project}")
    
    with st.spinner("🔍 Syncing diagnostic streams (Engineering View)..."):
        all_data = get_universal_portal_data(selected_project, view_mode="engineering")
    
    if all_data.empty:
        st.warning(f"No diagnostic data found for project {selected_project}.")
    else:
        loc_options = sorted(all_data['Location'].dropna().unique())
        c1, c2 = st.columns([2, 1])
        with c1:
            sel_loc = st.selectbox("Select Pipe / Bank to Analyze", loc_options, key="diag_loc_select")
        with c2:
            weeks_view = st.slider("Lookback (Weeks)", 1, 12, 2, key="diag_weeks_slider")
            
        df_diag = all_data[all_data['Location'] == sel_loc].copy()

        # 1. Engineering Timeline (Markers enabled for gap detection)
        st.subheader("📈 Engineering Timeline")
        fig_time = build_high_speed_graph(
            df_diag, f"Diagnostic Stream: {sel_loc}", 
            pd.Timestamp.now(tz='UTC') - timedelta(weeks=weeks_view), 
            pd.Timestamp.now(tz='UTC') + timedelta(hours=2), 
            tuple(active_refs), unit_mode, unit_label, display_tz
        )
        st.plotly_chart(fig_time, use_container_width=True, key=f"diag_chart_{sel_loc}")

        # 2. Communication Health Table
        st.subheader("📋 Sensor Communication Health")
        now_utc = pd.Timestamp.now(tz=pytz.UTC)
        latest_nodes = df_diag.sort_values('timestamp').groupby('NodeNum').tail(1).copy()
        
        summary_rows = []
        for _, row in latest_nodes.iterrows():
            # Calculate hours since last reporting
            ts = row['timestamp'].tz_localize(pytz.UTC) if row['timestamp'].tzinfo is None else row['timestamp']
            hrs_ago = int((now_utc - ts).total_seconds() / 3600)
            
            # Status Logic: Green < 6h, Yellow < 24h, Red > 24h
            status_icon = "🔴" if hrs_ago > 24 else ("🟢" if hrs_ago < 6 else "🟡")
            
            # Map database 'reason' column to human-readable status
            db_status = row['is_approved']
            status_label = "✅ Approved" if db_status == "TRUE" else ("🚫 Masked" if db_status == "MASKED" else "⏳ Pending")

            summary_rows.append({
                "Node": row['NodeNum'],
                "Last Value": f"{round(convert_val(row['temperature']), 1)}{unit_label}",
                "Last Seen": f"{hrs_ago}h ago {status_icon}",
                "Admin Status": status_label
            })
        
        st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, hide_index=True)


###########
# - 9. PAGE: DATA INTAKE LAB - #
###########

def render_data_intake_page(selected_project):
    """
    Handles ingestion of raw Lord and SensorPush files and data exports.
    """
    st.header("📤 Data Ingestion Lab")
    tab_upload, tab_export = st.tabs(["📄 Manual File Upload", "📥 Export Project Data"])
    
    with tab_upload:
        st.subheader("📄 Manual File Ingestion")
        st.info("Upload Lord SensorConnect (Wide), Lord Desktop (Narrow), or SensorPush (CSV/Excel).")
        
        u_file = st.file_uploader("Upload Data File", type=['csv', 'xlsx', 'xls'], key="manual_upload_main")
        
        if u_file is not None:
            filename = u_file.name.lower()
            is_excel = filename.endswith(('.xlsx', '.xls'))
            
            try:
                # 1. READ FILE
                if is_excel:
                    df_raw = pd.read_excel(u_file)
                else:
                    raw_bytes = u_file.getvalue().decode('utf-8', errors='ignore').splitlines()
                    header_idx = 0
                    for i, line in enumerate(raw_bytes[:100]):
                        if any(k in line for k in ["Timestamp", "Channel", "nodenumber", "SensorId", "Observed"]):
                            header_idx = i
                            break
                    df_raw = pd.read_csv(io.StringIO("\n".join(raw_bytes[header_idx:])))

                df_raw.columns = [str(c).strip() for c in df_raw.columns]
                cols_lower = [c.lower() for c in df_raw.columns]

                # 2. IDENTIFY FORMATS & PROCESS
                # Format: Lord Wide (SensorConnect)
                if not is_excel and any("DATA_START" in str(line) for line in raw_bytes[:100]):
                    start_idx = next(i for i, line in enumerate(raw_bytes) if "DATA_START" in line)
                    df_wide = pd.read_csv(io.StringIO("\n".join(raw_bytes[start_idx+1:])))
                    df_proc = df_wide.melt(id_vars=['Time'], var_name='NodeNum', value_name='temperature')
                    df_proc['NodeNum'] = df_proc['NodeNum'].str.replace(':', '-', regex=False)
                    df_proc['timestamp'] = pd.to_datetime(df_proc['Time'], format='mixed')
                    target_tbl = f"{PROJECT_ID}.{DATASET_ID}.raw_lord"
                    cols_to_keep = ['timestamp', 'NodeNum', 'temperature']

                # Format: SensorPush
                elif "sensorid" in cols_lower or "observed" in cols_lower:
                    id_col = next(c for c in df_raw.columns if "sensorid" in c.lower())
                    ts_col = next(c for c in df_raw.columns if any(k in c.lower() for k in ["observed", "sample time"]))
                    temp_col = next(c for c in df_raw.columns if "temp" in c.lower())
                    df_proc = pd.DataFrame({
                        'NodeNum': df_raw[id_col].astype(str).str.strip(),
                        'timestamp': pd.to_datetime(df_raw[ts_col], format='mixed'),
                        'temperature': pd.to_numeric(df_raw[temp_col], errors='coerce')
                    }).dropna()
                    target_tbl = f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush"
                    cols_to_keep = ['timestamp', 'NodeNum', 'temperature']

                # Format: Lord Narrow (Desktop)
                else:
                    mapping = {c: ("timestamp" if "timestamp" in c.lower() else "NodeNum" if any(k in c.lower() for k in ["channel", "node"]) else "temperature" if "temp" in c.lower() else c) for c in df_raw.columns}
                    df_proc = df_raw.rename(columns=mapping)
                    df_proc['NodeNum'] = df_proc['NodeNum'].astype(str).str.replace(':', '-', regex=False)
                    df_proc['timestamp'] = pd.to_datetime(df_proc['timestamp'], format='mixed')
                    target_tbl = f"{PROJECT_ID}.{DATASET_ID}.raw_lord"
                    cols_to_keep = ['timestamp', 'NodeNum', 'temperature']

                st.success(f"✅ Parsed {len(df_proc)} rows.")
                st.dataframe(df_proc.head())

                if st.button("🚀 Commit to BigQuery"):
                    final_upload = df_proc[cols_to_keep].dropna()
                    client.load_table_from_dataframe(final_upload, target_tbl).result()
                    st.success(f"Data successfully uploaded to {target_tbl}.")
                    st.cache_data.clear()

            except Exception:
                st.error(f"Ingestion Error: {traceback.format_exc()}")

    with tab_export:
        st.subheader("📥 Export Project Data")
        if not selected_project:
            st.warning("Please select a project in the sidebar.")
        else:
            with st.spinner("Fetching data for export..."):
                export_df = get_universal_portal_data(selected_project, view_mode="engineering")
            
            if export_df.empty:
                st.info("No data found for this project.")
            else:
                st.download_button(
                    "💾 Download Project CSV", 
                    export_df.to_csv(index=False).encode('utf-8'), 
                    f"{selected_project}_Full_Export.csv", 
                    "text/csv"
                )

###########
# - 10. PAGE: ADMIN TOOLS - #
###########

def render_admin_page(selected_project, display_tz, unit_mode, unit_label, active_refs):
    """
    Main UI for Admin tasks: Bulk Approval, Scrubbing, and Surgical Cleaning.
    """
    st.header("🛠️ Engineering Admin Tools")
    tab_bulk, tab_scrub, tab_surgical = st.tabs(["✅ Bulk Approval", "🧹 Deep Data Scrub", "🧨 Surgical Cleaner"])

    with tab_bulk:
        st.subheader("✅ Bulk Project Approval")
        st.info(f"Approving all pending data for **{selected_project}**.")
        
        if st.button(f"🚀 Bulk Approve {selected_project}"):
            with st.spinner("Executing Bulk Approval..."):
                # Explicitly uses r.NodeNum to avoid ambiguity error
                bulk_sql = f"""
                    INSERT INTO `{OVERRIDE_TABLE}` (NodeNum, timestamp, reason)
                    SELECT DISTINCT 
                        r.NodeNum, 
                        TIMESTAMP_TRUNC(r.timestamp, HOUR), 
                        'TRUE'
                    FROM (
                        SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
                        UNION ALL
                        SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
                    ) AS r
                    INNER JOIN `{PROJECT_ID}.{DATASET_ID}.metadata` AS m ON r.NodeNum = m.NodeNum
                    WHERE m.Project = '{selected_project}'
                    AND NOT EXISTS (
                        SELECT 1 FROM `{OVERRIDE_TABLE}` AS x 
                        WHERE x.NodeNum = r.NodeNum 
                        AND x.timestamp = TIMESTAMP_TRUNC(r.timestamp, HOUR)
                    )
                """
                try:
                    client.query(bulk_sql).result()
                    st.success(f"All data for {selected_project} is now live in the Client Portal.")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"Bulk Approval Error: {e}")

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

#######################################
# - 11.5 SURGICAL UPDATE HELPER - #
#######################################

def update_records(pts, df, val):
    """
    Final corrected version: Writes status updates into the 'reason' column.
    Matches schema: NodeNum (STRING), timestamp (TIMESTAMP), reason (STRING).
    """
    recs = []
    for p in pts:
        # Snap timestamp to the hour to match master/raw intervals
        # Ensure it is UTC before flooring to match BigQuery storage
        ts = pd.to_datetime(p['x']).tz_convert('UTC').floor('h')
        node = df.iloc[p['point_index']]['NodeNum']
        
        # We use 'reason' as the column name to store TRUE/FALSE/MASKED
        recs.append({
            "NodeNum": str(node), 
            "timestamp": ts, 
            "reason": val
        })
    
    if recs:
        # Deduplicate to prevent primary key bloat in BigQuery
        status_df = pd.DataFrame(recs).drop_duplicates(subset=['NodeNum', 'timestamp'])
        
        try:
            # Upload the dataframe to the override table
            job = client.load_table_from_dataframe(status_df, OVERRIDE_TABLE)
            job.result() # Wait for table upload to finish
            
            # Reset UI state
            st.session_state.locked_selection = None
            st.cache_data.clear()
            st.success(f"Successfully marked {len(recs)} points as {val}")
            time.sleep(1) # Brief pause so user sees success
            st.rerun()
        except Exception as e:
            st.error(f"Failed to update records: {e}")
            
###########
# - 12. MAIN ROUTER - #
###########

# Execute page based on sidebar 'service' selection
if service == "🌐 Global Overview":
    render_global_overview()

elif service == "🏠 Executive Summary":
    render_executive_summary(selected_project, unit_label)

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
