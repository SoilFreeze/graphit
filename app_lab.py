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

################################
# --- GET ALL PROJECT DATA --- #
################################
@st.cache_data(ttl=600)
def get_universal_portal_data(project_id, only_approved=True):
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
                # TRUNCATE BOTH TO HOUR: This is the magic that makes the scrub work
                CASE WHEN rej.NodeNum IS NULL THEN 'TRUE' ELSE 'FALSE' END as is_currently_approved
            FROM UnifiedRaw r
            INNER JOIN `{PROJECT_ID}.{DATASET_ID}.metadata` m ON r.NodeNum = m.NodeNum
            LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.manual_rejections` rej 
                ON r.NodeNum = rej.NodeNum 
                AND TIMESTAMP_TRUNC(r.timestamp, HOUR) = TIMESTAMP_TRUNC(rej.timestamp, HOUR)
        )
        SELECT * FROM JoinedData
        WHERE Project = '{project_id}'
        { "AND is_currently_approved = 'TRUE'" if only_approved else "" }
        AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 84 DAY)
        ORDER BY Location ASC, timestamp ASC
    """
    # ... rest of function (convert to pd.datetime with utc=True) ...
    try:
        df = client.query(query).to_dataframe()
        if not df.empty:
            # FORCE UTC to align NY and Pacific timezones
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
            if 'Bank' not in df.columns:
                df['Bank'] = ""
        return df
    except Exception as e:
        st.error(f"BigQuery Error: {e}")
        return pd.DataFrame()
##############################
# --- CHECK ADMIN ACCESS --- #
##############################
def check_admin_access():
    if "admin_authenticated" not in st.session_state:
        st.session_state["admin_authenticated"] = False

    if st.session_state["admin_authenticated"]:
        return True

    # Check if the secret even exists before trying to compare it
    if "admin_password" not in st.secrets:
        st.error("Developer Error: 'admin_password' is not defined in Streamlit Secrets.")
        return False

    st.warning("🔒 This area is restricted to Engineering Admins.")
    pwd_input = st.text_input("Enter Admin Password", type="password")
    
    if st.button("Unlock Tools"):
        if pwd_input == st.secrets["admin_password"]:
            st.session_state["admin_authenticated"] = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    return False
###################################
# --- GET CASHED PROJECT DATA --- #
###################################
@st.cache_data(ttl=600) # Cache data for 10 minutes
def get_cached_project_data(project_id, days=84):
    """
    Centralized data fetcher. 
    Returns all approved data for a project in one batch.
    """
    query = f"""
        SELECT timestamp, temperature, Depth, Location, Bank, NodeNum
        FROM `{MASTER_TABLE}`
        WHERE Project = '{project_id}' 
        AND (approve = 'TRUE' OR approve = 'true')
        AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
        ORDER BY timestamp ASC
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def get_project_list():
    """Caches the project list to speed up sidebar loading."""
    proj_q = f"SELECT DISTINCT Project FROM `{MASTER_TABLE}` WHERE Project IS NOT NULL"
    return client.query(proj_q).to_dataframe()['Project'].dropna().unique()

def style_delta(val):
    """Global styling for temperature deltas."""
    if val is None or pd.isna(val): return ""
    bg, color = "", "black"
    if val >= 5: bg, color = "#FF0000", "white"     # Critical Rise
    elif val >= 2: bg = "#FFA500"                   # Warning Rise
    elif val >= 0.5: bg = "#FFFF00"                 # Slight Rise
    elif -0.5 <= val <= 0.5: bg, color = "#008000", "white" # Stable
    elif -2 < val < -0.5: bg = "#ADD8E6"            # Slight Cooling
    elif -5 < val <= -2: bg, color = "#4169E1", "white" # Strong Cooling
    elif val <= -5: bg, color = "#00008B", "white"  # Deep Freeze
    return f'background-color: {bg}; color: {color}'

#########################
# --- CONFIGURATION --- #
#########################
# --- 1. CONFIGURATION & STYLING ---
st.set_page_config(page_title="SoilFreeze Data Lab", layout="wide")

# UPDATED: Pointing to the new 'Temperature' dataset
DATASET_ID = "Temperature" 
PROJECT_ID = "sensorpush-export"
# The full table name is now sensorpush-export.Temperature.master_data
MASTER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.master_data"
METADATA_TABLE = "metadata"

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

#########################
# --- REBUILD TABLE --- #
#########################
def rebuild_master_table(mode="preserve"):
    """
    Failsafe Rebuild: Strips all non-numeric characters to ensure 
    a match between CSV IDs and Google Sheet IDs.
    """
    table_id = f"{PROJECT_ID}.{DATASET_ID}.final_databoard_master"
    
    # Check if table exists to handle the 'ex' alias error
    exists = True
    try:
        client.get_table(table_id)
    except Exception:
        exists = False

    status_logic = "TRUE" if mode == "approve_all" else ("COALESCE(ex.is_approved, FALSE)" if exists else "FALSE")
    join_clause = f"LEFT JOIN `{table_id}` ex ON h.ts = ex.timestamp AND m.NodeNum = ex.sensor_id" if (exists and mode == "preserve") else ""

    scrub_sql = f"""
        CREATE OR REPLACE TABLE `{table_id}` AS 
        WITH RawUnified AS (
            SELECT CAST(timestamp AS TIMESTAMP) as ts, temperature as temp, 
                   -- Clean the ID: Remove colons, spaces, and non-digits
                   REGEXP_REPLACE(CAST(sensor_id AS STRING), r'[^0-9]', '') as clean_node 
            FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` WHERE temperature IS NOT NULL
            UNION ALL
            SELECT CAST(timestamp AS TIMESTAMP) as ts, value as temp, 
                   REGEXP_REPLACE(REPLACE(nodenumber, ':', '-'), r'[^0-9]', '') as clean_node 
            FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord` WHERE value IS NOT NULL
        ),
        HourlyDedupped AS (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY clean_node, TIMESTAMP_TRUNC(ts, HOUR) ORDER BY ts DESC) as rank 
            FROM RawUnified
        )
        SELECT 
            h.ts as timestamp, 
            h.temp as temperature, 
            m.NodeNum as sensor_id,
            m.NodeNum as sensor_name,
            m.Project as project, 
            m.Location as location, 
            m.Depth as depth, 
            {status_logic} as is_approved
        FROM HourlyDedupped h 
        INNER JOIN `{PROJECT_ID}.{DATASET_ID}.metadata` m 
            -- Match by stripping the Google Sheet PhysicalID of all non-digits too
            ON SUBSTR(h.clean_node, 1, 12) = SUBSTR(REGEXP_REPLACE(CAST(m.PhysicalID AS STRING), r'[^0-9]', ''), 1, 12)
        {join_clause}
        WHERE h.rank = 1
    """
    try:
        client.query(scrub_sql).result()
        return True
    except Exception as e:
        st.error(f"Rebuild Error: {e}")
        return False

############################
# --- FETCH SENSORPUSH --- #
############################
def fetch_sensorpush_data(start_dt, end_dt):
    """
    Handles API connection to SensorPush.
    Note: Requires 'sensorpush_creds' in st.secrets.
    """
    try:
        # 1. AUTHENTICATE
        auth_url = "https://api.sensorpush.com/v1/oauth/authorize"
        creds = st.secrets["sensorpush_creds"]
        auth_payload = {"email": creds["email"], "password": creds["password"]}
        
        auth_res = requests.post(auth_url, json=auth_payload).json()
        token = auth_res.get("accesstoken")
        
        if not token:
            st.error("API Auth Failed: Check credentials.")
            return pd.DataFrame()

        # 2. FETCH DATA
        data_url = "https://api.sensorpush.com/v1/samples"
        headers = {"accept": "application/json", "Authorization": token}
        # API expects ISO format strings
        payload = {
            "startTime": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "endTime": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        
        res = requests.post(data_url, headers=headers, json=payload).json()
        
        # 3. TRANSFORM TO BIGQUERY SCHEMA
        records = []
        for sensor_id, samples in res.get("sensors", {}).items():
            for s in samples:
                records.append({
                    "sensor_id": sensor_id,
                    "timestamp": s["observed"],
                    "temperature": s["temperature"]
                })
        
        return pd.DataFrame(records)
    except Exception as e:
        st.error(f"API Sync Error: {e}")
        return pd.DataFrame()


#################
# --- Graph --- #
#################
def build_high_speed_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label, display_tz="UTC"):
    if df.empty: return go.Figure()

    plot_df = df.copy()
    
    # --- NEW: LOCAL TIME CONVERSION ---
    # Convert data timestamps from UTC to the chosen display zone
    plot_df['timestamp'] = plot_df['timestamp'].dt.tz_convert(display_tz)
    
    # Adjust the view window (the 'camera') to match the display zone
    start_local = start_view.astimezone(pytz.timezone(display_tz))
    end_local = end_view.astimezone(pytz.timezone(display_tz))
    now_local = pd.Timestamp.now(tz=display_tz)

    # (Keep your existing Unit Conversion & Labeling logic here...)

    fig = go.Figure()
    
    for lbl in sorted(plot_df['label'].unique()):
        s_df = plot_df[plot_df['label'] == lbl].sort_values('timestamp')
        hover_name = lbl.split('(')[0].strip()

        # (Keep Gap Detection logic here...)

        fig.add_trace(go.Scattergl(
            x=s_df['timestamp'], y=s_df['temperature'], 
            name=lbl, mode='lines', connectgaps=False,
            customdata=[hover_name] * len(s_df),
            hovertemplate=f"<b>%{{customdata}}</b>: %{{y:.1f}}{unit_label}<br>%{{x|%b %d, %H:%M}}<extra></extra>"
        ))

    # --- UPDATED: LOCAL GRID HIERARCHY ---
    # We generate gridlines based on the local time (Midnight/Mondays)
    grid_times = pd.date_range(start=start_local, end=end_local, freq='6h', tz=display_tz)
    for ts in grid_times:
        if ts.weekday() == 0 and ts.hour == 0:
            color, width = "Black", 1.2 # Monday Midnight
        elif ts.hour == 0:
            color, width = "Gray", 0.8  # Other Midnights
        else:
            color, width = "LightGray", 0.4
        fig.add_vline(x=ts, line_width=width, line_color=color, layer='below')

    # --- UPDATED: RED "NOW" LINE (LOCAL) ---
    fig.add_vline(x=now_local, line_width=2, line_color="Red", layer='above', line_dash="dash")

    fig.update_layout(
        title={'text': f"{title} ({tz_mode})", 'x': 0},
        xaxis=dict(range=[start_local, end_local], showline=True, linecolor='black', mirror=True),
        # (Keep the rest of your layout settings...)
    )
    return fig
    
#######################
# --- SIDEBAR UI --- #
#######################
st.sidebar.title("❄️ SoilFreeze Lab")

service = st.sidebar.selectbox(
    "📂 Select Page", 
    ["🌐 Global Overview", "🏠 Executive Summary", "📊 Client Portal", "📉 Node Diagnostics", "📤 Data Intake Lab", "🛠️ Admin Tools"],
    index=0  # Sets Global Overview as the landing page
    )
st.sidebar.divider()

unit_mode = st.sidebar.radio("Temperature Unit", ["Fahrenheit", "Celsius"], index=0)
unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"

def convert_val(f_val):
    if f_val is None: return None
    return (f_val - 32) * 5/9 if unit_mode == "Celsius" else f_val

st.sidebar.divider()

# Project Selection
selected_project = None
if service in ["📊 Client Portal", "📉 Node Diagnostics", "🛠️ Admin Tools"]:
    try:
        proj_q = f"SELECT DISTINCT Project FROM `{MASTER_TABLE}` WHERE Project IS NOT NULL"
        proj_df = client.query(proj_q).to_dataframe()
        selected_project = st.sidebar.selectbox("🎯 Active Project", sorted(proj_df['Project'].dropna().unique()))
    except: st.sidebar.warning("No projects found.")

st.sidebar.divider()
st.sidebar.write("### 📏 Reference Lines")
active_refs = []
if st.sidebar.checkbox("Freezing (32°F / 0°C)", value=True): active_refs.append((32.0, "Freezing"))
if st.sidebar.checkbox("Type B (26.6°F / -3°C)", value=True): active_refs.append((26.6, "Type B"))
if st.sidebar.checkbox("Type A (10.2°F / -12.1°C)", value=True): active_refs.append((10.2, "Type A"))

# Add to your sidebar section
st.sidebar.subheader("🕒 Display Settings")
tz_mode = st.sidebar.selectbox("Timezone Display", ["UTC", "Local (US/Eastern)", "Local (US/Pacific)"])

# Map the selection to pytz strings
tz_lookup = {
    "UTC": "UTC",
    "Local (US/Eastern)": "US/Eastern",
    "Local (US/Pacific)": "US/Pacific"
}
display_tz = tz_lookup[tz_mode]
#################
# --- PAGES --- #
#################
###########################
# --- GLOBAL OVERVIEW --- #
###########################
if service == "🌐 Global Overview":
    st.header("🌐 Project Overview")
    
    # 1. FETCH ALL AVAILABLE PROJECTS FOR THE DROPDOWN
    # This query gets the list of unique projects from your metadata
    project_list_query = f"SELECT DISTINCT Project FROM `{PROJECT_ID}.{DATASET_ID}.metadata` WHERE Project IS NOT NULL"
    try:
        available_projects = client.query(project_list_query).to_dataframe()['Project'].tolist()
        available_projects = sorted([str(p) for p in available_projects])
    except Exception:
        available_projects = []

    # 2. PROJECT SELECTOR (Directly on the page)
    # If a project is already selected in the sidebar, we use it as the default
    default_ix = 0
    if selected_project in available_projects:
        default_ix = available_projects.index(selected_project)
    
    target_project = st.selectbox(
        "🏗️ Select a Project to View", 
        available_projects, 
        index=default_ix,
        key="global_overview_proj_picker"
    )

    if not target_project:
        st.info("👈 Please select a project from the dropdown above to view the timeline graphs.")
    else:
        # 3. FETCH DATA FOR THE CHOSEN PROJECT
        with st.spinner(f"Aligning NY & Pacific timezones for {target_project}..."):
            # We use our UTC-Hardened function here
            p_df = get_universal_portal_data(target_project, only_approved=True)

        if p_df.empty:
            st.warning(f"No approved data found for project {target_project}. Check 'Node Diagnostics' to see if data is being rejected.")
        else:
            # 4. TIMEFRAME CONTROLS (UTC STANDARDIZED)
            lookback = st.sidebar.slider("View Lookback (Weeks)", 1, 12, 4, key="ov_lookback")
            
            # This 'now_utc' is what fixes the New York offset issue
            now_utc = pd.Timestamp.now(tz='UTC')
            
            # Align window to the upcoming Monday at Midnight (UTC)
            end_view = (now_utc + pd.Timedelta(days=(7 - now_utc.weekday()) % 7 or 7)).replace(hour=0, minute=0, second=0, microsecond=0)
            start_view = end_view - timedelta(weeks=lookback)

            st.divider()
            st.subheader(f"Current Status: {target_project}")

            # 5. RENDER COLLAPSIBLE GRAPHS BY PIPE/LOCATION
            locations = sorted(p_df['Location'].dropna().unique())
            
            for loc in locations:
                # Wrap each graph in an expander so you can minimize them
                with st.expander(f"📍 Location: {loc}", expanded=True):
                    loc_df = p_df[p_df['Location'] == loc]
                    
                    # Create the title: Project - Location
                    graph_title = f"📈 {target_project} - {loc}"
                    
                    # Call the High-Speed Engine
                    fig = build_high_speed_graph(
                        loc_df, 
                        graph_title, 
                        start_view, 
                        end_view, 
                        tuple(active_refs), 
                        unit_mode, 
                        unit_label
                    )
                    
                    # Key must be unique to prevent Streamlit duplicate widget errors
                    st.plotly_chart(fig, use_container_width=True, key=f"ov_chart_{target_project}_{loc}")
#############################
# --- Executive Summary --- #
#############################
if service == "🏠 Executive Summary":
    st.header(f"🏠 Executive Summary: {selected_project if selected_project else 'All Projects'}")
    
    # 1. SORTING & CONTROLS
    st.write("### ↕️ Sorting & View Options")
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        sort_choice = st.selectbox("Sort By", ["None", "Hours Since Last Seen", "Delta Magnitude"])
    with c2:
        sort_order = st.radio("Order", ["Descending", "Ascending"], horizontal=True)
    
    # 2. BATCH DATA QUERY (Optimized to 1 Query instead of N queries)
    # Fetch all data for the last 24H for the entire project at once
    summary_q = f"""
        WITH RecentData AS (
            SELECT *,
                FIRST_VALUE(temperature) OVER(PARTITION BY NodeNum ORDER BY timestamp ASC) as first_temp_24h,
                ROW_NUMBER() OVER(PARTITION BY NodeNum ORDER BY timestamp DESC) as latest_rank
            FROM `{MASTER_TABLE}`
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
        with st.spinner("⚡ Syncing Command Center (Batch Processing)..."):
            raw_summary_df = client.query(summary_q).to_dataframe()
        
        if raw_summary_df.empty:
            st.warning("📡 No active sensors seen in the last 24 hours.")
        else:
            now = pd.Timestamp.now(tz=pytz.UTC)
            
            # 3. PROCESSING LOGIC (Pandas is faster than SQL for these calculations)
            def process_row(row):
                # Time handling
                ts = row['timestamp'].tz_localize(pytz.UTC) if row['timestamp'].tzinfo is None else row['timestamp']
                hrs_ago = int((now - ts).total_seconds() / 3600)
                
                # Delta Calculation
                raw_delta = row['temperature'] - row['first_temp_24h']
                
                # Status Icon Logic
                if hrs_ago > 24:
                    status_icon, delta_text, delta_val = "🔴", "-", None
                else:
                    status_icon = "🟢" if hrs_ago < 6 else ("🟡" if hrs_ago < 12 else "🟠")
                    delta_text = f"{round(raw_delta, 1)}°F"
                    delta_val = raw_delta

                # Position Labeling
                pos_label = f"Bank {row['Bank']}" if str(row['Bank']).strip().lower() not in ["","none","nan","null"] else f"{row['Depth']} ft"

                return pd.Series({
                    "Project": row['Project'],
                    "Node": row['NodeNum'],
                    "Pipe/Bank": row['Location'],
                    "Pos/Depth": pos_label,
                    "Min": f"{round(convert_val(row['min_24h']), 1)}{unit_label}",
                    "Max": f"{round(convert_val(row['max_24h']), 1)}{unit_label}",
                    "Delta_Val": delta_val, 
                    "Delta": delta_text,
                    "Hours_Ago": hrs_ago,
                    "Last Seen": f"{ts.strftime('%m/%d %H:%M')} ({hrs_ago}h) {status_icon}"
                })

            summary_df = raw_summary_df.apply(process_row, axis=1)

            # 4. APPLY SORTING
            asc = (sort_order == "Ascending")
            if sort_choice == "Hours Since Last Seen":
                summary_df = summary_df.sort_values(by="Hours_Ago", ascending=asc)
            elif sort_choice == "Delta Magnitude":
                summary_df['abs_d'] = summary_df['Delta_Val'].abs().fillna(-1)
                summary_df = summary_df.sort_values(by="abs_d", ascending=asc).drop(columns=['abs_d'])

            # 5. STYLING FUNCTION
            def style_delta(val):
                if val is None or pd.isna(val): return ""
                bg, color = "", "black"
                if val >= 5: bg, color = "#FF0000", "white"     # Critical Heat
                elif val >= 2: bg = "#FFA500"                   # Warning Heat
                elif val >= 0.5: bg = "#FFFF00"                 # Slight Rise
                elif -0.5 <= val <= 0.5: bg, color = "#008000", "white" # Stable
                elif -2 < val < -0.5: bg = "#ADD8E6"            # Slight Cooling
                elif -5 < val <= -2: bg, color = "#4169E1", "white" # Strong Cooling
                elif val <= -5: bg, color = "#00008B", "white"  # Deep Freeze
                return f'background-color: {bg}; color: {color}'

            # 6. DISPLAY
            st.subheader(f"📡 Engineering Command Center ({len(summary_df)} sensors)")
            
            st.dataframe(
                summary_df[["Project", "Node", "Pipe/Bank", "Pos/Depth", "Min", "Max", "Delta", "Last Seen"]].style.apply(
                    lambda x: [style_delta(rv) for rv in summary_df['Delta_Val']], axis=0, subset=['Delta']
                ),
                use_container_width=True,
                hide_index=True,
                height=600
            )
            
    except Exception as e: 
        st.error(f"Summary Error: {traceback.format_exc()}")
#################################
# --- END EXECUTIVE SUMMARY --- #
#################################
#########################
# --- CLIENT PORTAL --- #
#########################
elif service == "📊 Client Portal":
    if not selected_project:
        st.sidebar.warning("Please select a project.")
    else:
        st.header(f"📊 Project Status: {selected_project}")
        
        # 1. FETCH DATA (Uses cached function)
        p_df = get_universal_portal_data(selected_project)
        
        if p_df.empty:
            st.info(f"No approved data found for {selected_project}.")
        else:
            tab_time, tab_depth, tab_table = st.tabs(["📈 Timeline Analysis", "📏 Depth Profile", "📋 Project Data"])

            with tab_time:
                weeks_view = st.slider("Weeks to View", 1, 12, 6, key="cp_weeks")
                now = pd.Timestamp.now(tz=pytz.UTC)
                end_view = (now + pd.Timedelta(days=(7 - now.weekday()) % 7 or 7)).replace(hour=0, minute=0, second=0, microsecond=0)
                start_view = end_view - timedelta(weeks=weeks_view)
                
                for loc in sorted(p_df['Location'].dropna().unique()):
                    with st.expander(f"📈 {loc}", expanded=True):
                        loc_data = p_df[(p_df['Location'] == loc) & (p_df['timestamp'] >= start_view)]
                        # Uses the High-Speed Engine (ensure build_high_speed_graph is updated)
                        fig = build_high_speed_graph(loc_data, loc, start_view, end_view, tuple(active_refs), unit_mode, unit_label)
                        st.plotly_chart(fig, use_container_width=True, key=f"cht_{loc}", config={'displayModeBar': False})

            with tab_depth:
                p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
                depth_only = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
                
                for loc in sorted(depth_only['Location'].unique()):
                    with st.expander(f"📏 {loc} Depth Profile", expanded=True):
                        loc_data = depth_only[depth_only['Location'] == loc].copy()
                        fig_d = go.Figure()
                        
                        # Monday Snapshots logic
                        mondays = pd.date_range(start=start_view, end=now, freq='W-MON')
                        
                        for m_date in mondays:
                            target_ts = m_date.replace(hour=6, minute=0, second=0).tz_localize(pytz.UTC) if m_date.tzinfo is None else m_date.replace(hour=6)
                            window = loc_data[(loc_data['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & (loc_data['timestamp'] <= target_ts + pd.Timedelta(hours=12))]
                            
                            if not window.empty:
                                snap_list = []
                                for node in window['NodeNum'].unique():
                                    node_data = window[window['NodeNum'] == node].copy()
                                    node_data['diff'] = (node_data['timestamp'] - target_ts).abs()
                                    snap_list.append(node_data.sort_values('diff').iloc[0])
                                
                                snap_df = pd.DataFrame(snap_list).sort_values('Depth_Num')
                                fig_d.add_trace(go.Scattergl(x=snap_df['temperature'], y=snap_df['Depth_Num'], mode='lines+markers', name=target_ts.strftime('%m/%d/%y')))

                        # --- DYNAMIC GRID LOGIC ---
                        if unit_mode == "Celsius":
                            x_range = [( -20 - 32) * 5/9, (80 - 32) * 5/9]
                            x_major, x_minor = 10, 2
                        else:
                            x_range = [-20, 80]
                            x_major, x_minor = 20, 5

                        y_limit = int(((loc_data['Depth_Num'].max() // 5) + 1) * 5) if not loc_data.empty else 50
                        
                        fig_d.update_layout(
                            plot_bgcolor='white', height=700,
                            # X-AXIS: Configured for Minor 5° grid
                            xaxis=dict(
                                title=f"Temp ({unit_label})", 
                                range=x_range, 
                                dtick=x_minor,           # Set minor interval (5°)
                                showgrid=True,           # Explicitly show minor grid
                                gridcolor='Gainsboro',   # Light gray for minor lines
                                gridwidth=0.5,
                                showline=True, 
                                linecolor='black', 
                                mirror=True
                            ),
                            # Y-AXIS: 10ft grid
                            yaxis=dict(
                                title="Depth (ft)", 
                                range=[y_limit, 0], 
                                dtick=10, 
                                showgrid=True,
                                gridcolor='Silver',      # Slightly darker for depth lines
                                showline=True, 
                                linecolor='black', 
                                mirror=True
                            ),
                            legend=dict(title="Weekly Snapshots (6AM)", orientation="h", y=-0.2)
                        )

                        # ADD MAJOR TEMPERATURE LINES (Every 20°)
                        # We iterate through the range to add bold vertical markers
                        for x_v in range(-40, 101, x_major):
                            if x_range[0] <= x_v <= x_range[1]:
                                fig_d.add_vline(x=x_v, line_width=1.5, line_color="DimGray", layer='below')

                        # ADD REFERENCE THRESHOLDS (Freezing, Type A, Type B)
                        for val, label in active_refs:
                            c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
                            fig_d.add_vline(x=c_val, line_dash="dash", line_color="maroon" if "Type A" in label else "RoyalBlue", line_width=2.5, opacity=0.8)
                            
                        st.plotly_chart(fig_d, use_container_width=True, key=f"dep_{loc}", config={'displayModeBar': False})

            with tab_table:
                latest = p_df.sort_values('timestamp').groupby('NodeNum').tail(1).copy()
                latest['Current Temp'] = latest['temperature'].apply(lambda x: f"{round(convert_val(x), 1)}{unit_label}")
                latest['Position'] = latest.apply(lambda r: f"Bank {r['Bank']}" if pd.notnull(r['Bank']) and str(r['Bank']).strip() != "" else f"{r.get('Depth', '??')} ft", axis=1)
                st.dataframe(latest[['Location', 'Position', 'Current Temp', 'NodeNum']].sort_values(['Location', 'Position']), use_container_width=True, hide_index=True)
#############################
# --- END CLIENT PORTAL --- #
#############################  
###########################
# --- NODE DIAGNOSTIC --- #
###########################  
elif service == "📉 Node Diagnostics":
    st.header(f"📉 Node Diagnostics: {selected_project}")
    
    if not selected_project:
        st.warning("👈 Please select a Project in the sidebar to begin.")
    else:
        # 1. Fetch ALL data (including rejected points) for troubleshooting
        with st.spinner("Fetching diagnostic data..."):
            all_data = get_universal_portal_data(selected_project, only_approved=False)
        
        if all_data.empty:
            st.warning(f"No data found for project {selected_project}.")
        else:
            # 2. Timezone-Aware Now Line and Range
            now_utc = pd.Timestamp.now(tz='UTC')
            lookback_days = st.sidebar.slider("Diagnostic Window (Days)", 1, 14, 7)
            start_view = now_utc - timedelta(days=lookback_days)
            end_view = now_utc + timedelta(hours=12) # Buffer for future-dated NY points

            # 3. Location / Pipe Filter
            loc_options = sorted(all_data['Location'].dropna().unique())
            selected_loc = st.selectbox("Select Pipe/Location", loc_options)
            
            # Filter the dataframe for the selected location
            diag_df = all_data[all_data['Location'] == selected_loc]
            
            # 4. Diagnostic Metrics
            c1, c2, c3 = st.columns(3)
            with c1:
                total_nodes = diag_df['NodeNum'].nunique()
                st.metric("Active Nodes", total_nodes)
            with c2:
                rejected_pts = len(diag_df[diag_df['is_currently_approved'] == 'FALSE'])
                st.metric("Rejected Points", rejected_pts)
            with c3:
                last_seen = diag_df['timestamp'].max()
                st.write(f"**Last Sync (UTC):** \n{last_seen.strftime('%Y-%m-%d %H:%M') if not pd.isnull(last_seen) else 'N/A'}")

            # 5. Render the Diagnostic Graph
            st.subheader(f"Timeline: {selected_loc}")
            fig = build_high_speed_graph(
                diag_df, 
                f"Diagnostic View: {selected_loc}", 
                start_view, 
                end_view, 
                tuple(active_refs), 
                unit_mode, 
                unit_label
            )
            st.plotly_chart(fig, use_container_width=True, key=f"diag_{selected_project}_{selected_loc}")

            # 6. Detailed Node List Table
            st.subheader("Node Status Summary")
            summary_table = diag_df.groupby('NodeNum').agg({
                'timestamp': 'max',
                'temperature': 'mean',
                'is_currently_approved': lambda x: (x == 'TRUE').sum()
            }).rename(columns={
                'timestamp': 'Last Seen',
                'temperature': f'Avg Temp ({unit_label})',
                'is_currently_approved': 'Approved Count'
            })
            st.dataframe(summary_table, use_container_width=True)
            # Inside Project Overview or Node Diagnostics
fig = build_high_speed_graph(
    loc_df, 
    graph_title, 
    start_view, 
    end_view, 
    tuple(active_refs), 
    unit_mode, 
    unit_label,
    display_tz=display_tz # <--- Pass the sidebar selection here
)
###############################
# --- END NODE DIAGNOSTIC --- #
###############################
###############################
# --- DATA INTAKE LAB --- #
###############################
elif service == "📤 Data Intake Lab":
    if check_admin_access():
        st.header("📤 Data Ingestion & Recovery")
        
        # Removed Maintenance Tab as requested
        tab1, tab2, tab3 = st.tabs(["📄 Manual File Upload", "📡 API Data Recovery", "📥 Export Project Data"])

        with tab1:
            st.subheader("📄 Manual File Ingestion")
            st.info("Upload Lord SensorConnect (Wide), Lord Desktop Log (Narrow), or SensorPush CSVs.")
            u_file = st.file_uploader("Upload CSV", type=['csv'], key="manual_upload_unified_fixed")
            
            if u_file is not None:
                import io
                filename = u_file.name.lower()
                raw_content = u_file.getvalue().decode('utf-8').splitlines()
                
                # --- DETECT FILE TYPE ---
                is_lord_wide = any("DATA_START" in line for line in raw_content[:100])
                is_lord_narrow = "nodenumber" in raw_content[0].lower() and "temperature" in raw_content[0].lower()
                
                # --- CASE 1: LORD SENSORCONNECT (WIDE) ---
                if is_lord_wide:
                    try:
                        start_idx = next(i for i, line in enumerate(raw_content) if "DATA_START" in line)
                        df_wide = pd.read_csv(io.StringIO("\n".join(raw_content[start_idx+1:])))
                        # Rename 'Time' to 'timestamp' and melt columns into 'NodeNum'
                        df_long = df_wide.melt(id_vars=['Time'], var_name='NodeNum', value_name='temperature')
                        df_long['NodeNum'] = df_long['NodeNum'].str.replace(':', '-', regex=False)
                        df_long['timestamp'] = pd.to_datetime(df_long['Time'], format='mixed')
                        df_long = df_long.dropna(subset=['temperature'])
                        
                        st.success(f"✅ Lord Wide Format Parsed: {len(df_long)} readings.")
                        st.dataframe(df_long.head())
                        if st.button("🚀 UPLOAD LORD WIDE DATA"):
                            client.load_table_from_dataframe(df_long[['timestamp', 'NodeNum', 'temperature']], 
                                                             f"{PROJECT_ID}.{DATASET_ID}.raw_lord").result()
                            st.success("Uploaded successfully to raw_lord!")
                    except Exception as e: st.error(f"Lord Wide Error: {e}")
    
                # --- CASE 2: LORD DESKTOP LOG (NARROW) ---
                elif is_lord_narrow:
                    try:
                        df_ln = pd.read_csv(io.StringIO("\n".join(raw_content)))
                        # MAP TO BIGQUERY SCHEMA: Case-sensitive NodeNum and timestamp
                        df_ln = df_ln.rename(columns={
                            'Timestamp': 'timestamp', 
                            'nodenumber': 'NodeNum', 
                            'temperature': 'temperature'
                        })
                        df_ln['timestamp'] = pd.to_datetime(df_ln['timestamp'], format='mixed')
                        df_ln['NodeNum'] = df_ln['NodeNum'].str.replace(':', '-', regex=False)
                        
                        st.success(f"✅ Lord Narrow Format Parsed: {len(df_ln)} readings.")
                        st.dataframe(df_ln.head())
                        if st.button("🚀 UPLOAD LORD NARROW DATA"):
                            client.load_table_from_dataframe(df_ln[['timestamp', 'NodeNum', 'temperature']], 
                                                             f"{PROJECT_ID}.{DATASET_ID}.raw_lord").result()
                            st.success("Uploaded successfully to raw_lord!")
                    except Exception as e: st.error(f"Lord Narrow Error: {e}")

                # --- CASE 3: SENSORPUSH ---
                else:
                    try:
                        header_idx = -1
                        for i, line in enumerate(raw_content[:50]):
                            if "SensorId" in line or "Observed" in line:
                                header_idx = i; break
                        
                        if header_idx != -1:
                            df_sp = pd.read_csv(io.StringIO("\n".join(raw_content[header_idx:])), dtype=str)
                            ts_col = "Observed" if "Observed" in df_sp.columns else df_sp.columns[1]
                            
                            df_up = pd.DataFrame()
                            # Mapping to the raw_sensorpush schema
                            df_up['sensor_id'] = df_sp['SensorId'].astype(str).str.strip()
                            df_up['timestamp'] = pd.to_datetime(df_sp[ts_col], format='mixed')
                            t_cols = [c for c in df_sp.columns if "Temperature" in c or "Thermocouple" in c]
                            df_up['temperature'] = pd.to_numeric(df_sp[t_cols].bfill(axis=1).iloc[:, 0], errors='coerce')
                            df_up = df_up.dropna(subset=['timestamp', 'temperature'])
    
                            st.success(f"✅ SensorPush Parsed: {len(df_up)} readings.")
                            if st.button("🚀 UPLOAD SENSORPUSH"):
                                client.load_table_from_dataframe(df_up, f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush").result()
                                st.success("Uploaded successfully to raw_sensorpush!")
                        else:
                            st.error("Format not recognized. Check CSV headers.")
                    except Exception as e: st.error(f"SensorPush Error: {e}")

        with tab2:
            st.subheader("📡 Cloud-to-Cloud API Sync")
            c1, c2 = st.columns(2)
            start_date = c1.date_input("Start Date", datetime.now() - timedelta(days=1))
            end_date = c2.date_input("End Date", datetime.now())
            
            if st.button("🛰️ FETCH & SYNC"):
                # Level 3: Date Conversion
                start_dt = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=pytz.UTC)
                end_dt = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=pytz.UTC)
                
                with st.spinner("Fetching data..."):
                    # Level 4: Call the Function
                    df_api = fetch_sensorpush_data(start_dt, end_dt)
                    
                    if not df_api.empty:
                        # Level 5: Upload to BigQuery
                        table_path = f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush"
                        client.load_table_from_dataframe(df_api, table_path).result()
                        st.success(f"✅ Integrated {len(df_api)} points successfully!")
                    else:
                        # Level 5: Fallback
                        st.warning("No data found for this range.")
                        
        # Add this to your "📤 Data Intake Lab" tabs or a new section
        with tab3:
            st.subheader("📥 Export Project Data")
            
            # 1. PROJECT SELECTION (In case it's not set in the sidebar)
            # We fetch the list of unique projects from the metadata table
            all_projects_q = f"SELECT DISTINCT Project FROM `{PROJECT_ID}.{DATASET_ID}.metadata` WHERE Project IS NOT NULL"
            all_projs = client.query(all_projects_q).to_dataframe()['Project'].tolist()
            
            # If a project is selected in sidebar, use it as default; otherwise, let user pick
            default_ix = all_projs.index(selected_project) if selected_project in all_projs else 0
            target_project = st.selectbox("1️⃣ Select Project to Export", sorted(all_projs), index=default_ix)
        
            if target_project:
                # 2. FETCH DATA FOR FILTERS
                with st.spinner(f"Loading data for {target_project}..."):
                    # Fetch ALL data (unapproved included) for THIS project
                    export_df = get_universal_portal_data(target_project, only_approved=False)
        
                if export_df.empty:
                    st.warning(f"No data found for {target_project}.")
                else:
                    # 3. PIPE & DATE FILTERS
                    c1, c2 = st.columns(2)
                    
                    with c1:
                        # Pipe Selection
                        pipes = ["All Pipes"] + sorted(export_df['Location'].dropna().unique().tolist())
                        sel_pipe = st.selectbox("2️⃣ Select Pipe / Location", pipes)
                    
                    with c2:
                        # Date Range Selection
                        min_ts = export_df['timestamp'].min().date()
                        max_ts = export_df['timestamp'].max().date()
                        # We use a key to ensure this widget doesn't reset unexpectedly
                        export_range = st.date_input("3️⃣ Select Date Range", value=(min_ts, max_ts), key="export_range_picker")
        
                    # 4. APPLY FILTERS
                    df_final = export_df.copy()
                    
                    if sel_pipe != "All Pipes":
                        df_final = df_final[df_final['Location'] == sel_pipe]
                    
                    if isinstance(export_range, tuple) and len(export_range) == 2:
                        start, end = export_range
                        df_final = df_final[(df_final['timestamp'].dt.date >= start) & (df_final['timestamp'].dt.date <= end)]
        
                    # 5. DOWNLOAD BUTTON
                    st.divider()
                    st.write(f"📊 **Result:** Found {len(df_final)} rows for export.")
                    
                    if not df_final.empty:
                        # Standardize units if needed
                        if unit_mode == "Celsius":
                            df_final['temperature'] = (df_final['temperature'] - 32) * 5/9
                        
                        csv_bytes = df_final.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="💾 Download CSV",
                            data=csv_bytes,
                            file_name=f"Export_{target_project}_{sel_pipe}.csv",
                            mime='text/csv'
                        )
###############################
# --- END DATA INTAKE LAB --- #
###############################
#######################
# --- ADMIN TOOLS --- #
#######################             
elif service == "🛠️ Admin Tools":
    if check_admin_access():
        st.header("🛠️ Engineering Admin Tools")
    
        # 1. DEFINE TABS FIRST (Fixes the NameError)
        tab_scrub, tab_approve, tab_cleaner = st.tabs(["🧹 Deep Data Scrub", "✅ Bulk Approval", "🧨 Surgical Cleaner"])
    
        # 2. BULK APPROVAL (Only targets physical RAW tables)
        with tab_approve:
            st.subheader("✅ Bulk Approval")
            st.info("Sets records to 'TRUE' unless they were specifically marked 'FALSE' with the Lasso tool.")
            
            if st.button("🚀 Approve All Pending Data"):
                # We ONLY update raw tables; the MASTER_TABLE View will reflect changes automatically
                raw_tables = [f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush", 
                              f"{PROJECT_ID}.{DATASET_ID}.raw_lord"]
                
                with st.spinner("Processing approvals..."):
                    for table in raw_tables:
                        try:
                            approve_sql = f"""
                                UPDATE `{table}` 
                                SET approve = 'TRUE' 
                                WHERE approve IS NULL 
                                OR UPPER(CAST(approve AS STRING)) != 'FALSE'
                            """
                            client.query(approve_sql).result()
                        except Exception as e:
                            st.warning(f"Could not update {table}: {e}")
                st.success("Bulk approval complete.")
                st.cache_data.clear()
    
        # 3. DEEP DATA SCRUB (Physical Purge of RAW tables)
        with tab_scrub:
            st.subheader("🧹 Deep Data Scrub & Final Purge")
            st.info("Permanently deletes 'FALSE' points and reduces data to 1-hour intervals.")
            
            scrub_target = st.radio("Target Table", ["SensorPush", "Lord"], horizontal=True)
            target_table = f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush" if scrub_target == "SensorPush" else f"{PROJECT_ID}.{DATASET_ID}.raw_lord"
    
            if st.button(f"🧨 Permanently Purge & Dedup {scrub_target}"):
                with st.spinner("Executing hard delete and dedup..."):
                    scrub_sql = f"""
                    CREATE OR REPLACE TABLE `{target_table}` AS 
                    SELECT * EXCEPT(rn) FROM (
                        SELECT *, 
                               ROW_NUMBER() OVER(
                                   PARTITION BY NodeNum, TIMESTAMP_TRUNC(timestamp, HOUR) 
                                   ORDER BY timestamp DESC
                               ) as rn
                        FROM `{target_table}` 
                        WHERE (approve IS NULL OR UPPER(CAST(approve AS STRING)) != 'FALSE')
                        AND temperature IS NOT NULL
                    ) WHERE rn = 1
                    """
                    # Note: Purge for Master Table removed as it is a VIEW
                    try:
                        client.query(scrub_sql).result()
                        st.success(f"{scrub_target} purged of rejected points and deduped.")
                        st.cache_data.clear()
                    except Exception as e:
                        st.error(f"Scrub Error: {e}")
    
        # 4. SURGICAL CLEANER (Lasso Selection)
        with tab_cleaner:
            # --- SURGICAL DATA CLEANER (LASSO TOOL) ---
            st.subheader("✂️ Surgical Data Cleaner")
            st.info("Use the Lasso or Box Select on the graph above, then click the button below to scrub those hours.")
            
            # This captures the selection data from the Plotly chart
            # Ensure your plotly_chart call above uses: on_select="rerun"
            event_data = st.session_state.get("plotly_selection") # or however you've named your selection capture
            
            if event_data and "points" in event_data:
                points = event_data["points"]
                st.write(f"🎯 **{len(points)}** points selected.")
            
                if st.button("🚫 HIDE SELECTED DATA"):
                    with st.spinner("Processing scrub to the top of the hour..."):
                        try:
                            rejection_records = []
                            
                            for pt in points:
                                # 1. Capture the raw timestamp from the X-axis
                                raw_ts = pd.to_datetime(pt['x'])
                                
                                # 2. Floor to the TOP OF THE HOUR (e.g., 10:45 -> 10:00)
                                # This ensures the join catches all readings in that hour
                                scrubbed_ts = raw_ts.floor('h')
                                
                                # 3. Get the NodeNum from the underlying dataframe
                                # Using point_index to map back to the original data row
                                node_id = p_df.iloc[pt['point_index']]['NodeNum']
                                
                                rejection_records.append({
                                    "NodeNum": node_id,
                                    "timestamp": scrubbed_ts,
                                    "reason": "Top-of-Hour Scrub",
                                    "project": selected_project
                                })
            
                            if rejection_records:
                                # Convert to DataFrame
                                rej_df = pd.DataFrame(rejection_records)
                                
                                # Upload to BigQuery (Append mode)
                                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                                
                                job = client.load_table_from_dataframe(
                                    rej_df, 
                                    f"{PROJECT_ID}.{DATASET_ID}.manual_rejections",
                                    job_config=job_config
                                )
                                
                                # Wait for job to complete
                                job.result()
                                
                                st.success(f"✅ Successfully scrubbed {len(rejection_records)} hours for Project {selected_project}!")
                                
                                # 4. CLEAR CACHE: This forces the graphs to reload and hide the points immediately
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                st.warning("No valid points found in selection.")
            
                        except Exception as e:
                            # This 'except' block fixes the SyntaxError you saw
                            st.error(f"❌ Error during scrubbing: {str(e)}")
                            st.write("Ensure the 'manual_rejections' table schema matches: NodeNum (STRING), timestamp (TIMESTAMP), reason (STRING)")
            
            else:
                st.write("💡 *Select points on the graph above to enable the hide button.*")
###########################
# --- END ADMIN TOOLS --- #
###########################
