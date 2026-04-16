import streamlit as st
import pandas as pd
import time
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz
import requests
import json
import traceback
import re
import io
import openpyxl

##################################
# - 1. CONFIGURATION & STYLING - #
##################################
st.set_page_config(page_title="SoilFreeze Data Lab", layout="wide")

# Database Constants
DATASET_ID = "Temperature" 
PROJECT_ID = "sensorpush-export"
MASTER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.master_data"
# The Override table stores our Approve/Mask/Delete statuses
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"

@st.cache_resource
def get_bq_client():
    """Handles authentication with BigQuery and Drive scopes."""
    try:
        SCOPES = [
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/drive"
        ]
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
    Standardizes data fetching based on the audience.
    
    view_mode:
    - 'engineering': Sees Approved (TRUE), Pending (NULL), and Masked (MASKED). Hides FALSE.
    - 'client': Sees ONLY Approved (TRUE).
    """
    
    # Define the filter based on the view mode
    if view_mode == "client":
        # The client ONLY sees data explicitly marked as 'TRUE'
        approval_filter = "AND rej.status = 'TRUE'"
    else:
        # Engineering sees everything that hasn't been explicitly rejected ('FALSE')
        # This includes NULL (new/pending data) and 'MASKED' (pre-project data)
        approval_filter = "AND (rej.status IS NULL OR rej.status != 'FALSE')"

    query = f"""
        WITH UnifiedRaw AS (
            SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
            UNION ALL
            SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
        ),
        JoinedData AS (
            SELECT 
                r.NodeNum, r.timestamp, r.temperature,
                m.Location, m.Bank, m.Depth, m.Project,
                # Join with the override table to get current status
                rej.status as is_approved 
            FROM UnifiedRaw r
            INNER JOIN `{PROJECT_ID}.{DATASET_ID}.metadata` m ON r.NodeNum = m.NodeNum
            LEFT JOIN `{OVERRIDE_TABLE}` rej 
                ON r.NodeNum = rej.NodeNum 
                AND TIMESTAMP_TRUNC(TIMESTAMP_ADD(r.timestamp, INTERVAL 30 MINUTE), HOUR) = 
                    TIMESTAMP_TRUNC(TIMESTAMP_ADD(rej.timestamp, INTERVAL 30 MINUTE), HOUR)
        )
        SELECT * FROM JoinedData
        WHERE Project = '{project_id}'
        {approval_filter}
        AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 84 DAY)
        ORDER BY Location ASC, timestamp ASC
    """
    try:
        df = client.query(query).to_dataframe()
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
            if 'Bank' not in df.columns:
                df['Bank'] = ""
        return df
    except Exception as e:
        st.error(f"BigQuery Error in Data Engine: {e}")
        return pd.DataFrame()

def check_admin_access():
    """Security gate for Admin and Intake pages."""
    if "admin_authenticated" not in st.session_state:
        st.session_state["admin_authenticated"] = False

    if st.session_state["admin_authenticated"]:
        return True

    if "admin_password" not in st.secrets:
        st.error("System Error: 'admin_password' not defined in Streamlit Secrets.")
        return False

    st.warning("🔒 Restricted Area: Engineering Admin Only.")
    pwd_input = st.text_input("Enter Admin Password", type="password")
    
    if st.button("Unlock Tools"):
        if pwd_input == st.secrets["admin_password"]:
            st.session_state["admin_authenticated"] = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    return False

###########################
#- 3. SIDEBAR UI & STATE -#
###########################
# --- GLOBAL SIDEBAR ---
st.sidebar.title("❄️ SoilFreeze Lab")

# Page Navigation
service = st.sidebar.selectbox(
    "📂 Select Page", 
    ["🌐 Global Overview", "🏠 Executive Summary", "📊 Client Portal", "📉 Node Diagnostics", "📤 Data Intake Lab", "🛠️ Admin Tools"],
    index=0
)
st.sidebar.divider()

# Temperature Unit Handling
unit_mode = st.sidebar.radio("Temperature Unit", ["Fahrenheit", "Celsius"], index=0)
unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"

def convert_val(f_val):
    """Global helper to convert Fahrenheit from DB to user's display unit."""
    if f_val is None or pd.isna(f_val): return None
    return (f_val - 32) * 5/9 if unit_mode == "Celsius" else f_val

st.sidebar.divider()

# Global Project Selection (Used by all pages except Global Overview)
selected_project = None
if service in ["📊 Client Portal", "📉 Node Diagnostics", "🛠️ Admin Tools", "🏠 Executive Summary"]:
    try:
        # Fetching project list directly from metadata for the sidebar
        proj_q = f"SELECT DISTINCT Project FROM `{PROJECT_ID}.{DATASET_ID}.metadata` WHERE Project IS NOT NULL"
        proj_df = client.query(proj_q).to_dataframe()
        selected_project = st.sidebar.selectbox("🎯 Active Project", sorted(proj_df['Project'].dropna().unique()))
    except Exception:
        st.sidebar.warning("Could not load project list from BigQuery.")

st.sidebar.divider()

# Reference Line Settings
st.sidebar.subheader("📏 Reference Lines")
active_refs = []
if st.sidebar.checkbox("Freezing (32°F / 0°C)", value=True): active_refs.append((32.0, "Freezing"))
if st.sidebar.checkbox("Type B (26.6°F / -3°C)", value=True): active_refs.append((26.6, "Type B"))
if st.sidebar.checkbox("Type A (10.2°F / -12.1°C)", value=True): active_refs.append((10.2, "Type A"))

# Timezone Settings
st.sidebar.subheader("🕒 Display Settings")
tz_mode = st.sidebar.selectbox("Timezone Display", ["UTC", "Local (US/Eastern)", "Local (US/Pacific)"])
tz_lookup = {
    "UTC": "UTC",
    "Local (US/Eastern)": "US/Eastern",
    "Local (US/Pacific)": "US/Pacific"
}
display_tz = tz_lookup[tz_mode]

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
    # Ensure timestamps are localized to the user's preferred viewing zone
    plot_df['timestamp'] = plot_df['timestamp'].dt.tz_convert(display_tz)
    
    # Adjust axes windows to match local zone
    start_local = start_view.astimezone(pytz.timezone(display_tz))
    end_local = end_view.astimezone(pytz.timezone(display_tz))
    now_local = pd.Timestamp.now(tz=display_tz)

    # 2. UNIT CONVERSION
    if unit_mode == "Celsius":
        plot_df['temperature'] = (plot_df['temperature'] - 32) * 5/9
        y_range, dt_major = [-30, 30], 5
    else:
        y_range, dt_major = [-20, 80], 10

    # 3. LABELING LOGIC
    # Priority: Bank Name -> Depth
    plot_df['label'] = plot_df.apply(
        lambda r: f"Bank {r['Bank']} ({r['NodeNum']})" if str(r.get('Bank')).strip().lower() not in ["", "none", "nan", "null"]
        else f"{r.get('Depth')}ft ({r.get('NodeNum')})", axis=1
    )
    
    # 4. PLOT MODE (Markers for Admin/Surgical, Lines for Client/Global)
    is_surgical = any(word in title for word in ["Scrubbing", "Surgical", "Diag"])
    plot_mode = 'markers' if is_surgical else 'lines'
    marker_size = 7 if is_surgical else 3

    fig = go.Figure()
    
    for lbl in sorted(plot_df['label'].unique()):
        s_df = plot_df[plot_df['label'] == lbl].sort_values('timestamp')
        hover_name = lbl.split('(')[0].strip()

        # 5. GAP DETECTION (Prevents lines from jumping across large data outages)
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

    # 7. GRID HIERARCHY (Monday=Black, Midnight=Gray)
    grid_times = pd.date_range(start=start_local, end=end_local, freq='6h', tz=display_tz)
    for ts in grid_times:
        if ts.weekday() == 0 and ts.hour == 0:
            color, width = "Black", 1.2 
        elif ts.hour == 0:
            color, width = "Gray", 0.8  
        else:
            color, width = "LightGray", 0.3
        fig.add_vline(x=ts, line_width=width, line_color=color, layer='below')

    # 8. REFERENCE LINES & NOW LINE
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

###########
#- 5. GLOBAL OVERVIEW -
###########
if service == "🌐 Global Overview":
    st.header("🌐 Global Project Overview")
    
    # Using the cached project list to speed up the initial dropdown
    try:
        proj_list_q = f"SELECT DISTINCT Project FROM `{PROJECT_ID}.{DATASET_ID}.metadata` WHERE Project IS NOT NULL"
        available_projects = sorted(client.query(proj_list_q).to_dataframe()['Project'].tolist())
        target_project = st.selectbox("🏗️ Select a Project to Review", available_projects, key="global_proj_picker")
    except:
        st.error("Could not connect to project metadata.")
        target_project = None

    if target_project:
        with st.spinner(f"Syncing {target_project} timeline (Engineering View)..."):
            # view_mode="engineering" ensures you see the data BEFORE you approve it for the client
            p_df = get_universal_portal_data(target_project, view_mode="engineering")

        if not p_df.empty:
            # Lookback control for the overview
            lookback = st.sidebar.slider("Lookback (Weeks)", 1, 12, 4)
            
            # Snap to the following Monday at Midnight for a clean chart "Future" window
            now_utc = pd.Timestamp.now(tz='UTC')
            end_view = (now_utc + pd.Timedelta(days=(7-now_utc.weekday())%7 or 7)).replace(hour=0, minute=0, second=0)
            start_view = end_view - timedelta(weeks=lookback)

            # Generate a chart for every unique Location (Pipe/Bank) in the project
            for loc in sorted(p_df['Location'].unique()):
                with st.expander(f"📍 Location: {loc}", expanded=True):
                    loc_df = p_df[p_df['Location'] == loc]
                    fig = build_high_speed_graph(
                        loc_df, 
                        f"📈 {target_project} - {loc}", 
                        start_view, 
                        end_view, 
                        tuple(active_refs), 
                        unit_mode, 
                        unit_label, 
                        display_tz=display_tz
                    )
                    st.plotly_chart(fig, use_container_width=True, key=f"ov_{target_project}_{loc}")
        else:
            st.info(f"No engineering data found for {target_project} in the last 84 days.")

###########
#- 6. EXECUTIVE SUMMARY -
###########
elif service == "🏠 Executive Summary":
    st.header(f"🏠 Executive Summary: {selected_project if selected_project else 'All Projects'}")
    
    # 1. SORTING CONTROLS
    st.write("### ↕️ Sorting & View Options")
    c1, c2 = st.columns([1, 1])
    with c1:
        sort_choice = st.selectbox("Sort By", ["None", "Hours Since Last Seen", "Delta Magnitude"])
    with c2:
        sort_order = st.radio("Order", ["Descending", "Ascending"], horizontal=True)
    
    # 2. BATCH COMMAND CENTER QUERY
    # This query bypasses the approval filter to show you the current health of ALL sensors
    summary_q = f"""
        WITH RecentData AS (
            SELECT *,
                FIRST_VALUE(temperature) OVER(PARTITION BY NodeNum ORDER BY timestamp ASC) as first_temp_24h,
                ROW_NUMBER() OVER(PARTITION BY NodeNum ORDER BY timestamp DESC) as latest_rank
            FROM (
                SELECT NodeNum, timestamp, temperature, Project, Location, Bank, Depth FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
                UNION ALL
                SELECT NodeNum, timestamp, temperature, Project, Location, Bank, Depth FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
            )
            WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            {"AND Project = '" + selected_project + "'" if selected_project else ""}
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
        with st.spinner("⚡ Fetching 24-Hour Snapshot..."):
            raw_summary_df = client.query(summary_q).to_dataframe()
        
        if raw_summary_df.empty:
            st.warning("📡 No sensor activity detected in the last 24 hours.")
        else:
            now_utc = pd.Timestamp.now(tz=pytz.UTC)
            
            def process_summary_row(row):
                ts = row['timestamp'].tz_localize(pytz.UTC) if row['timestamp'].tzinfo is None else row['timestamp']
                hrs_ago = int((now_utc - ts).total_seconds() / 3600)
                
                # Health Icon
                status_icon = "🟢" if hrs_ago < 6 else ("🟡" if hrs_ago < 12 else "🟠" if hrs_ago < 24 else "🔴")
                
                raw_delta = row['temperature'] - row['first_temp_24h']
                pos_label = f"Bank {row['Bank']}" if str(row['Bank']).strip().lower() not in ["","none","nan","null"] else f"{row['Depth']} ft"

                return pd.Series({
                    "Project": row['Project'],
                    "Node": row['NodeNum'],
                    "Location": row['Location'],
                    "Position": pos_label,
                    "Min": f"{round(convert_val(row['min_24h']), 1)}{unit_label}",
                    "Max": f"{round(convert_val(row['max_24h']), 1)}{unit_label}",
                    "Delta_Val": raw_delta, 
                    "Delta": f"{round(raw_delta, 1)}°F",
                    "Hours_Ago": hrs_ago,
                    "Last Seen": f"{ts.strftime('%m/%d %H:%M')} ({hrs_ago}h) {status_icon}"
                })

            summary_df = raw_summary_df.apply(process_summary_row, axis=1)

            # 3. APPLY USER SORTING
            asc = (sort_order == "Ascending")
            if sort_choice == "Hours Since Last Seen":
                summary_df = summary_df.sort_values(by="Hours_Ago", ascending=asc)
            elif sort_choice == "Delta Magnitude":
                summary_df['abs_d'] = summary_df['Delta_Val'].abs().fillna(-1)
                summary_df = summary_df.sort_values(by="abs_d", ascending=asc).drop(columns=['abs_d'])

            # 4. DISPLAY COMMAND CENTER
            st.subheader(f"📡 Sensor Health ({len(summary_df)} Active)")
            
            st.dataframe(
                summary_df[["Project", "Node", "Location", "Position", "Min", "Max", "Delta", "Last Seen"]].style.apply(
                    lambda x: [style_delta(rv) for rv in summary_df['Delta_Val']], axis=0, subset=['Delta']
                ),
                use_container_width=True,
                hide_index=True,
                height=600
            )
            
    except Exception:
        st.error(f"Executive Summary Error: {traceback.format_exc()}")

###########
#- 7. CLIENT PORTAL -
###########
elif service == "📊 Client Portal":
    if not selected_project:
        st.sidebar.warning("Please select a project in the sidebar to view the portal.")
    else:
        st.header(f"📊 Project Status: {selected_project}")
        
        # 1. FETCH DATA (Strict Client View - ONLY Approved 'TRUE' data)
        with st.spinner("Loading approved client data..."):
            p_df = get_universal_portal_data(selected_project, view_mode="client")
        
        if p_df.empty:
            st.info(f"No approved data is currently available for {selected_project}. Check Admin Tools to approve pending points.")
        else:
            tab_time, tab_depth, tab_table = st.tabs(["📈 Timeline Analysis", "📏 Depth Profile", "📋 Project Data"])

            with tab_time:
                weeks_view = st.slider("Weeks to View", 1, 12, 6, key="cp_weeks")
                now = pd.Timestamp.now(tz=pytz.UTC)
                # Snap end view to Monday midnight for consistency
                end_view = (now + pd.Timedelta(days=(7 - now.weekday()) % 7 or 7)).replace(hour=0, minute=0, second=0, microsecond=0)
                start_view = end_view - timedelta(weeks=weeks_view)
                
                for loc in sorted(p_df['Location'].dropna().unique()):
                    with st.expander(f"📈 {loc}", expanded=True):
                        loc_data = p_df[(p_df['Location'] == loc) & (p_df['timestamp'] >= start_view)]
                        fig = build_high_speed_graph(loc_data, loc, start_view, end_view, tuple(active_refs), unit_mode, unit_label, display_tz=display_tz)
                        st.plotly_chart(fig, use_container_width=True, key=f"cht_{loc}", config={'displayModeBar': False})

            with tab_depth:
                # Standard Depth Profile Logic
                p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
                depth_only = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
                
                for loc in sorted(depth_only['Location'].unique()):
                    with st.expander(f"📏 {loc} Depth Profile", expanded=True):
                        loc_data = depth_only[depth_only['Location'] == loc].copy()
                        fig_d = go.Figure()
                        
                        # Generate Weekly Monday Snapshots (6AM)
                        mondays = pd.date_range(start=start_view, end=now, freq='W-MON')
                        for m_date in mondays:
                            target_ts = m_date.replace(hour=6, minute=0, second=0).tz_localize(pytz.UTC)
                            window = loc_data[(loc_data['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & (loc_data['timestamp'] <= target_ts + pd.Timedelta(hours=12))]
                            
                            if not window.empty:
                                snap_list = []
                                for node in window['NodeNum'].unique():
                                    node_data = window[window['NodeNum'] == node].copy()
                                    node_data['diff'] = (node_data['timestamp'] - target_ts).abs()
                                    snap_list.append(node_data.sort_values('diff').iloc[0])
                                
                                snap_df = pd.DataFrame(snap_list).sort_values('Depth_Num')
                                fig_d.add_trace(go.Scattergl(x=snap_df['temperature'], y=snap_df['Depth_Num'], mode='lines+markers', name=target_ts.strftime('%m/%d/%y')))

                        # Chart Layout
                        y_limit = int(((loc_data['Depth_Num'].max() // 5) + 1) * 5) if not loc_data.empty else 50
                        x_range = [-20, 80] if unit_mode == "Fahrenheit" else [(-20-32)*5/9, (80-32)*5/9]
                        
                        fig_d.update_layout(
                            plot_bgcolor='white', height=700,
                            xaxis=dict(title=f"Temp ({unit_label})", range=x_range, gridcolor='Gainsboro'),
                            yaxis=dict(title="Depth (ft)", range=[y_limit, 0], dtick=10, gridcolor='Silver'),
                            legend=dict(title="Weekly Snapshots (6AM)", orientation="h", y=-0.2)
                        )
                        st.plotly_chart(fig_d, use_container_width=True, key=f"dep_{loc}")

            with tab_table:
                # Latest Snapshot Table
                latest = p_df.sort_values('timestamp').groupby('NodeNum').tail(1).copy()
                latest['Current Temp'] = latest['temperature'].apply(lambda x: f"{round(convert_val(x), 1)}{unit_label}")
                latest['Position'] = latest.apply(lambda r: f"Bank {r['Bank']}" if pd.notnull(r['Bank']) and str(r['Bank']).strip() != "" else f"{r.get('Depth', '??')} ft", axis=1)
                st.dataframe(latest[['Location', 'Position', 'Current Temp', 'NodeNum']].sort_values(['Location', 'Position']), use_container_width=True, hide_index=True)

###########
#- 8. NODE DIAGNOSTICS -
###########
elif service == "📉 Node Diagnostics":
    st.header(f"📉 Node Diagnostics: {selected_project}")
    
    if not selected_project:
        st.warning("👈 Please select a project in the sidebar to begin analysis.")
    else:
        # 1. FETCH DATA (Engineering View - Shows Approved + Pending/Blank)
        with st.spinner("🔍 Syncing diagnostic streams (Engineering View)..."):
            all_data = get_universal_portal_data(selected_project, view_mode="engineering")
        
        if all_data.empty:
            st.warning(f"No data found for project {selected_project}.")
        else:
            loc_options = sorted(all_data['Location'].dropna().unique())
            c1, c2 = st.columns([2, 1])
            with c1: 
                sel_loc = st.selectbox("Select Pipe / Bank to Analyze", loc_options)
            with c2: 
                weeks_view = st.slider("Lookback (Weeks)", 1, 12, 4, key="diag_lookback")

            now_utc = pd.Timestamp.now(tz=pytz.UTC)
            end_view = (now_utc + pd.Timedelta(days=(7 - now_utc.weekday()) % 7 or 7)).replace(hour=0, minute=0, second=0, microsecond=0)
            start_view = end_view - timedelta(weeks=weeks_view)

            df_diag = all_data[all_data['Location'] == sel_loc].copy()

            # --- TIMELINE ANALYSIS ---
            st.subheader("📈 Engineering Timeline")
            st.caption("Viewing trends including unapproved data.")
            fig_time = build_high_speed_graph(df_diag, f"Diag: {sel_loc}", start_view, end_view, tuple(active_refs), unit_mode, unit_label, display_tz=display_tz)
            st.plotly_chart(fig_time, use_container_width=True)

            # --- STATUS SUMMARY TABLE ---
            st.subheader(f"📋 Node Health & Approval Status: {sel_loc}")
            latest_nodes = df_diag.sort_values('timestamp').groupby('NodeNum').tail(1).copy()
            
            summary_rows = []
            for _, row in latest_nodes.iterrows():
                hrs_ago = int((now_utc - row['timestamp']).total_seconds() / 3600)
                status_icon = "🔴" if hrs_ago > 24 else ("🟢" if hrs_ago < 6 else "🟡")
                
                # Show what the status actually is in the database
                db_status = row['is_approved']
                approval_display = "✅ Approved" if db_status == "TRUE" else ("🚫 Masked" if db_status == "MASKED" else "⏳ Pending Review")

                summary_rows.append({
                    "Node": row['NodeNum'],
                    "Depth/Bank": f"Bank {row['Bank']}" if str(row['Bank']).strip().lower() not in ["","none"] else f"{row['Depth']} ft",
                    "Last Reading": f"{round(convert_val(row['temperature']), 1)}{unit_label}",
                    "Last Seen": f"{hrs_ago}h ago {status_icon}",
                    "Current Status": approval_display
                })
            
            st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, hide_index=True)

###########
#- 9. DATA INTAKE LAB -
###########
elif service == "📤 Data Intake Lab":
    if check_admin_access():
        st.header("📤 Data Ingestion Lab")
        tab_upload, tab_export = st.tabs(["📄 Manual File Upload", "📥 Export Project Data"])
        
        with tab_upload:
            ###########
            # - Tab: Upload - #
            ###########
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

                    # 2. IDENTIFY FORMATS
                    is_lord_wide = not is_excel and any("DATA_START" in str(line) for line in raw_bytes[:100])
                    is_lord_narrow = "channel" in cols_lower or "nodenumber" in cols_lower
                    is_sensorpush = "sensorid" in cols_lower or "observed" in cols_lower

                    # --- PROCESSING ---
                    if is_lord_wide:
                        start_idx = next(i for i, line in enumerate(raw_bytes) if "DATA_START" in line)
                        df_wide = pd.read_csv(io.StringIO("\n".join(raw_bytes[start_idx+1:])))
                        df_long = df_wide.melt(id_vars=['Time'], var_name='NodeNum', value_name='temperature')
                        df_long['NodeNum'] = df_long['NodeNum'].str.replace(':', '-', regex=False)
                        df_long['timestamp'] = pd.to_datetime(df_long['Time'], format='mixed')
                        
                        st.success(f"✅ Lord Wide Parsed: {len(df_long)} rows")
                        if st.button("🚀 UPLOAD TO RAW_LORD"):
                            client.load_table_from_dataframe(df_long[['timestamp', 'NodeNum', 'temperature']], f"{PROJECT_ID}.{DATASET_ID}.raw_lord").result()
                            st.success("Uploaded to BigQuery.")
                            st.cache_data.clear()

                    elif is_lord_narrow:
                        mapping = {c: ("timestamp" if "timestamp" in c.lower() else "NodeNum" if any(k in c.lower() for k in ["channel", "node"]) else "temperature" if "temp" in c.lower() else c) for c in df_raw.columns}
                        df_ln = df_raw.rename(columns=mapping)
                        df_ln['timestamp'] = pd.to_datetime(df_ln['timestamp'], format='mixed')
                        df_ln['NodeNum'] = df_ln['NodeNum'].astype(str).str.replace(':', '-', regex=False)
                        
                        st.success(f"✅ Lord Narrow Parsed: {len(df_ln)} rows")
                        if st.button("🚀 UPLOAD TO RAW_LORD"):
                            client.load_table_from_dataframe(df_ln[['timestamp', 'NodeNum', 'temperature']], f"{PROJECT_ID}.{DATASET_ID}.raw_lord").result()
                            st.success("Uploaded to BigQuery.")
                            st.cache_data.clear()

                    elif is_sensorpush:
                        id_col = next((c for c in df_raw.columns if "sensorid" in c.lower()), None)
                        ts_col = next((c for c in df_raw.columns if any(k in c.lower() for k in ["observed", "sample time"])), None)
                        temp_col = next((c for c in df_raw.columns if "temp" in c.lower()), None)
                        
                        df_sp = pd.DataFrame({
                            'NodeNum': df_raw[id_col].astype(str).str.strip(),
                            'timestamp': pd.to_datetime(df_raw[ts_col], format='mixed'),
                            'temperature': pd.to_numeric(df_raw[temp_col], errors='coerce')
                        }).dropna()

                        st.success(f"✅ SensorPush Parsed: {len(df_sp)} rows")
                        if st.button("🚀 UPLOAD TO RAW_SENSORPUSH"):
                            client.load_table_from_dataframe(df_sp, f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush").result()
                            st.success("Uploaded to BigQuery.")
                            st.cache_data.clear()

                except Exception:
                    st.error(f"Ingestion Error: {traceback.format_exc()}")

        with tab_export:
            ###########
            # - Tab: Export - #
            ###########
            st.subheader("📥 Export Project Data (Wide Format)")
            if selected_project:
                with st.spinner("Preparing export..."):
                    export_df = get_universal_portal_data(selected_project, view_mode="engineering")
                
                if not export_df.empty:
                    pipes = sorted(export_df['Location'].dropna().unique().tolist())
                    sel_pipe = st.selectbox("Select Pipe / Location", pipes)
                    df_final = export_df[export_df['Location'] == sel_pipe].copy()
                    
                    if not df_final.empty:
                        # Pivot to Wide Format
                        df_final['Depth_Col'] = df_final['Depth'].astype(str) + "ft"
                        df_wide = df_final.pivot_table(index='timestamp', columns='Depth_Col', values='temperature', aggfunc='mean').reset_index()
                        st.download_button("💾 Download Wide CSV", df_wide.to_csv(index=False).encode('utf-8'), f"{selected_project}_{sel_pipe}_Export.csv", "text/csv")

###########
#- 10. ADMIN TOOLS -
###########
elif service == "🛠️ Admin Tools":
    if check_admin_access():
        st.header("🛠️ Engineering Admin Tools")
        tab_bulk, tab_scrub, tab_surgical = st.tabs(["✅ Bulk Approval", "🧹 Deep Data Scrub", "🧨 Surgical Cleaner"])

        with tab_bulk:
            ###########
            # - Tab: Bulk Approval - #
            ###########
            st.subheader("✅ Bulk Project Approval")
            st.info("Moves all currently 'Pending' data points to 'Approved' status for the client.")
            if st.button(f"🚀 Bulk Approve {selected_project}"):
                with st.spinner("Processing Bulk Approval..."):
                    # This SQL grabs all node/hour timestamps currently in raw that aren't in overrides
                    bulk_sql = f"""
                        INSERT INTO `{OVERRIDE_TABLE}` (NodeNum, timestamp, status, Project)
                        SELECT DISTINCT NodeNum, TIMESTAMP_TRUNC(timestamp, HOUR), 'TRUE', '{selected_project}'
                        FROM (
                            SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
                            UNION ALL
                            SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
                        )
                        WHERE NodeNum IN (SELECT NodeNum FROM `{PROJECT_ID}.{DATASET_ID}.metadata` WHERE Project = '{selected_project}')
                        AND NOT EXISTS (
                            SELECT 1 FROM `{OVERRIDE_TABLE}` x 
                            WHERE x.NodeNum = NodeNum AND x.timestamp = TIMESTAMP_TRUNC(timestamp, HOUR)
                        )
                    """
                    client.query(bulk_sql).result()
                    st.success(f"Project {selected_project} data is now live in Client Portal.")
                    st.cache_data.clear()

        with tab_scrub:
            ###########
            # - Tab: Deep Scrub - #
            ###########
            st.subheader("🧹 Deep Data Scrub & Averaging")
            st.warning("Permanently averages data in the RAW tables to 1-hour intervals.")
            scrub_target = st.radio("Target Table", ["SensorPush", "Lord"], horizontal=True)
            t_table = f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush" if scrub_target == "SensorPush" else f"{PROJECT_ID}.{DATASET_ID}.raw_lord"
            
            if st.button(f"🧨 Purge & Average {scrub_target}"):
                with st.spinner("Executing SQL Mean Reduction..."):
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
                    st.success(f"✅ {scrub_target} table successfully averaged and snapped.")
                    st.cache_data.clear()

        with tab_surgical:
            ###########
            # - Tab: Surgical - #
            ###########
            if not selected_project:
                st.warning("Please select a project in the sidebar.")
            else:
                render_surgical_cleaner(selected_project, display_tz, unit_mode, unit_label, active_refs)

###################################
# - SURGICAL CLEANER FUNCTIONS - #
###################################

def render_surgical_cleaner(selected_project, display_tz, unit_mode, unit_label, active_refs):
    p_df = get_universal_portal_data(selected_project, view_mode="engineering")
    if p_df.empty:
        st.info("No data available to scrub.")
        return

    loc_options = sorted(p_df['Location'].dropna().unique())
    sel_loc = st.selectbox("Select Pipe to Clean", loc_options, key="surgical_loc_select")
    scrub_df = p_df[p_df['Location'] == sel_loc].copy().reset_index(drop=True)

    if "locked_selection" not in st.session_state:
        st.session_state.locked_selection = None

    fig_scrub = build_high_speed_graph(scrub_df, f"Surgical Scrubbing: {sel_loc}", pd.Timestamp.now(tz='UTC') - timedelta(days=14), pd.Timestamp.now(tz='UTC') + timedelta(hours=6), tuple(active_refs), unit_mode, unit_label, display_tz=display_tz)

    if st.session_state.locked_selection:
        indices = [p['point_index'] for p in st.session_state.locked_selection]
        fig_scrub.update_traces(selectedpoints=indices, unselected=dict(marker=dict(opacity=0.2)))

    event_data = st.plotly_chart(fig_scrub, use_container_width=True, on_select="rerun", key=f"scrub_{sel_loc}")

    if event_data and "selection" in event_data:
        pts = event_data["selection"].get("points", [])
        if len(pts) > 0: st.session_state.locked_selection = pts

    if st.session_state.locked_selection:
        st.success(f"📍 {len(st.session_state.locked_selection)} points selected.")
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            if st.button("✅ APPROVE (Client)", use_container_width=True):
                process_status_update(st.session_state.locked_selection, scrub_df, "TRUE", selected_project)
        with c2:
            if st.button("🚫 MASK (Client)", use_container_width=True):
                process_status_update(st.session_state.locked_selection, scrub_df, "MASKED", selected_project)
        with c3:
            if st.button("🗑️ DELETE", type="primary", use_container_width=True):
                process_status_update(st.session_state.locked_selection, scrub_df, "FALSE", selected_project)
        with c4:
            if st.button("Clear Selection", use_container_width=True):
                st.session_state.locked_selection = None
                st.rerun()

def process_status_update(points, df, status_val, project_id):
    records = []
    for pt in points:
        # Floor to hour to ensure override join matches scrubbed data
        raw_ts = pd.to_datetime(pt['x'])
        scrub_ts = raw_ts.tz_convert('UTC').floor('h')
        node_id = df.iloc[pt['point_index']]['NodeNum']
        records.append({"NodeNum": str(node_id), "timestamp": scrub_ts, "status": status_val, "Project": project_id})
    
    if records:
        status_df = pd.DataFrame(records).drop_duplicates(subset=['NodeNum', 'timestamp'])
        client.load_table_from_dataframe(status_df, OVERRIDE_TABLE).result()
        st.session_state.locked_selection = None
        st.cache_data.clear()
        st.rerun()
