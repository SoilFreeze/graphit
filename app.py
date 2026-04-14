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

################################
# --- GET ALL PROJECT DATA --- #
################################
@st.cache_data(ttl=600)
def get_universal_portal_data(project_id, only_approved=True):
    """
    Standardizes data to UTC and handles rounded hourly scrubbing joins.
    """
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
                # Join logic: Snap both to nearest hour so the scrub/rejection works
                CASE WHEN rej.NodeNum IS NULL THEN 'TRUE' ELSE 'FALSE' END as is_currently_approved
            FROM UnifiedRaw r
            INNER JOIN `{PROJECT_ID}.{DATASET_ID}.metadata` m ON r.NodeNum = m.NodeNum
            LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.manual_rejections` rej 
                ON r.NodeNum = rej.NodeNum 
                AND TIMESTAMP_TRUNC(TIMESTAMP_ADD(r.timestamp, INTERVAL 30 MINUTE), HOUR) = 
                    TIMESTAMP_TRUNC(TIMESTAMP_ADD(rej.timestamp, INTERVAL 30 MINUTE), HOUR)
        )
        SELECT * FROM JoinedData
        WHERE Project = '{project_id}'
        { "AND is_currently_approved = 'TRUE'" if only_approved else "" }
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
    Failsafe Rebuild: Snaps all timestamps to the NEAREST hour to ensure
    one measurement per hour per sensor.
    """
    table_id = f"{PROJECT_ID}.{DATASET_ID}.final_databoard_master"
    
    exists = True
    try:
        client.get_table(table_id)
    except Exception:
        exists = False

    status_logic = "TRUE" if mode == "approve_all" else ("COALESCE(ex.is_approved, FALSE)" if exists else "FALSE")
    join_clause = f"LEFT JOIN `{table_id}` ex ON TIMESTAMP_TRUNC(TIMESTAMP_ADD(h.ts, INTERVAL 30 MINUTE), HOUR) = ex.timestamp AND m.NodeNum = ex.sensor_id" if (exists and mode == "preserve") else ""

    scrub_sql = f"""
        CREATE OR REPLACE TABLE `{table_id}` AS 
        WITH RawUnified AS (
            SELECT CAST(timestamp AS TIMESTAMP) as ts, temperature as temp, 
                   REGEXP_REPLACE(CAST(sensor_id AS STRING), r'[^0-9]', '') as clean_node 
            FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` WHERE temperature IS NOT NULL
            UNION ALL
            SELECT CAST(timestamp AS TIMESTAMP) as ts, temperature as temp, 
                   REGEXP_REPLACE(REPLACE(NodeNum, ':', '-'), r'[^0-9]', '') as clean_node 
            FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord` WHERE temperature IS NOT NULL
        ),
        HourlyDedupped AS (
            SELECT *, ROW_NUMBER() OVER(
                PARTITION BY clean_node, TIMESTAMP_TRUNC(TIMESTAMP_ADD(ts, INTERVAL 30 MINUTE), HOUR) 
                ORDER BY ts DESC
            ) as rank 
            FROM RawUnified
        )
        SELECT 
            TIMESTAMP_TRUNC(TIMESTAMP_ADD(h.ts, INTERVAL 30 MINUTE), HOUR) as timestamp, 
            h.temp as temperature, 
            m.NodeNum as sensor_id,
            m.NodeNum as sensor_name,
            m.Project as project, 
            m.Location as location, 
            m.Depth as depth, 
            {status_logic} as is_approved
        FROM HourlyDedupped h 
        INNER JOIN `{PROJECT_ID}.{DATASET_ID}.metadata` m 
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
        
#################
# --- Graph --- #
#################
def build_high_speed_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label, display_tz="UTC"):
    """
    High-speed Plotly engine with Local Time support and Clean Hover Tooltips.
    """
    if df.empty:
        return go.Figure()

    plot_df = df.copy()
    
    # 1. TIMEZONE CONVERSION
    # Standardize data to the user's preferred local display zone
    plot_df['timestamp'] = plot_df['timestamp'].dt.tz_convert(display_tz)
    
    # Adjust the 'Camera' window and the 'Red Now Line' to match the local zone
    start_local = start_view.astimezone(pytz.timezone(display_tz))
    end_local = end_view.astimezone(pytz.timezone(display_tz))
    now_local = pd.Timestamp.now(tz=display_tz)

    # 2. UNIT CONVERSION
    if unit_mode == "Celsius":
        plot_df['temperature'] = (plot_df['temperature'] - 32) * 5/9
        y_range, dt_minor = [-30, 30], 2
    else:
        y_range, dt_minor = [-20, 80], 5

    # 3. LABELING LOGIC
    plot_df['label'] = plot_df.apply(
        lambda r: f"Bank {r['Bank']} ({r['NodeNum']})" if str(r.get('Bank')).strip().lower() not in ["", "none", "nan", "null"]
        else f"{r.get('Depth')}ft ({r.get('NodeNum')})", axis=1
    )
    
    # 4. PLOT MODE (Lines vs Points)
    # Admin and Diag views use markers for precise Lasso selection
    is_admin = "Scrubbing" in title or "Diag" in title
    plot_mode = 'markers' if is_admin else 'lines'
    marker_size = 7 if is_admin else 3

    fig = go.Figure()
    
    for lbl in sorted(plot_df['label'].unique()):
        s_df = plot_df[plot_df['label'] == lbl].sort_values('timestamp')
        # This is the name shown in the hover box (e.g., 'Bank R1')
        hover_name = lbl.split('(')[0].strip()

        # 5. GAP DETECTION (Only for Line mode)
        if not is_admin:
            s_df['gap_hrs'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
            gap_mask = s_df['gap_hrs'] > 6.0
            if gap_mask.any():
                gaps = s_df[gap_mask].copy()
                gaps['temperature'] = None
                gaps['timestamp'] = gaps['timestamp'] - pd.Timedelta(minutes=1)
                s_df = pd.concat([s_df, gaps]).sort_values('timestamp')

        # 6. ADD TRACE WITH CLEAN HOVER
        fig.add_trace(go.Scattergl(
            x=s_df['timestamp'], 
            y=s_df['temperature'], 
            name=lbl, 
            mode=plot_mode,
            marker=dict(size=marker_size, opacity=0.8 if is_admin else 1.0),
            connectgaps=False,
            customdata=[hover_name] * len(s_df),
            # THE FIX: Removed time/date from individual lines. Only shows Name + Temp.
            # <extra></extra> removes the secondary trace name box.
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

    # 8. REFERENCE LINES
    for val, ref_label in active_refs:
        c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
        fig.add_hline(y=c_val, line_dash="dash", line_color="maroon" if "Type A" in ref_label else "RoyalBlue", 
                      annotation_text=ref_label, annotation_position="top right")

    # 9. RED "NOW" LINE
    fig.add_vline(x=now_local, line_width=2, line_color="Red", layer='above', line_dash="dash")

    # 10. FINAL LAYOUT
    fig.update_layout(
        title={'text': f"{title} ({display_tz})", 'x': 0},
        plot_bgcolor='white',
        # 'x unified' puts the time/date ONLY at the top of the box
        hovermode="x unified",
        height=600,
        margin=dict(t=80, l=50, r=180, b=50),
        xaxis=dict(
            range=[start_local, end_local],
            showline=True, 
            linecolor='black', 
            mirror=True,
            tickformat='%b %d\n%H:%M'
        ),
        yaxis=dict(
            title=f"Temperature ({unit_label})",
            range=y_range,
            dtick=dt_minor,
            gridcolor='Gainsboro',
            showline=True,
            linecolor='black',
            mirror=True
        ),
        legend=dict(
            title="Sensors",
            orientation="v",
            x=1.02,
            y=1,
            xanchor="left"
        )
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
    
    # 1. Project Selector
    project_list_query = f"SELECT DISTINCT Project FROM `{PROJECT_ID}.{DATASET_ID}.metadata` WHERE Project IS NOT NULL"
    available_projects = sorted(client.query(project_list_query).to_dataframe()['Project'].astype(str).tolist())
    
    target_project = st.selectbox("🏗️ Select a Project", available_projects, key="global_proj_picker")

    if target_project:
        with st.spinner("Loading site timeline..."):
            p_df = get_universal_portal_data(target_project, only_approved=True)

        if not p_df.empty:
            lookback = st.sidebar.slider("Lookback (Weeks)", 1, 12, 4)
            now_utc = pd.Timestamp.now(tz='UTC')
            end_view = (now_utc + pd.Timedelta(days=(7-now_utc.weekday())%7 or 7)).replace(hour=0, minute=0, second=0)
            start_view = end_view - timedelta(weeks=lookback)

            for loc in sorted(p_df['Location'].unique()):
                with st.expander(f"📍 {loc}", expanded=True):
                    loc_df = p_df[p_df['Location'] == loc]
                    fig = build_high_speed_graph(loc_df, f"📈 {target_project} - {loc}", start_view, end_view, tuple(active_refs), unit_mode, unit_label, display_tz=display_tz)
                    st.plotly_chart(fig, use_container_width=True, key=f"ov_{target_project}_{loc}")
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
        st.warning("👈 Please select a project in the sidebar to begin analysis.")
    else:
        try:
            # 1. DATA ACCESS & TIMEFRAME
            # Fetch all data (including unapproved) for full engineering visibility
            with st.spinner("🔍 Syncing diagnostic streams..."):
                all_data = get_universal_portal_data(selected_project, only_approved=False)
            
            if all_data.empty:
                st.warning(f"No data found for project {selected_project}.")
            else:
                # Layout for Top Controls
                loc_options = sorted(all_data['Location'].dropna().unique())
                c1, c2 = st.columns([2, 1])
                with c1: 
                    sel_loc = st.selectbox("Select Pipe / Bank to Analyze", loc_options)
                with c2: 
                    weeks_view = st.slider("Lookback (Weeks)", 1, 12, 4, key="diag_lookback")

                # Date Calculations (Standardized UTC)
                now_utc = pd.Timestamp.now(tz=pytz.UTC)
                days_until_monday = (7 - now_utc.weekday()) % 7 or 7
                end_view = (now_utc + pd.Timedelta(days=days_until_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
                start_view = end_view - timedelta(weeks=weeks_view)

                # Filter data for the specific location
                df_diag = all_data[all_data['Location'] == sel_loc].copy()

                # --- 2. THE THREE ANALYSIS SECTIONS ---
                
                # SECTION A: TIMELINE ANALYSIS
                st.subheader("📈 Timeline Analysis")
                st.caption(f"Viewing historical trends in **{tz_mode}**.")
                fig_time = build_high_speed_graph(
                    df_diag, sel_loc, start_view, end_view, 
                    tuple(active_refs), unit_mode, unit_label, 
                    display_tz=display_tz
                )
                st.plotly_chart(fig_time, use_container_width=True, config={'displayModeBar': True}, key=f"diag_time_{sel_loc}")

                st.divider()

                # SECTION B: DEPTH PROFILE ANALYSIS
                st.subheader("📏 Depth Profile Analysis")
                df_diag['Depth_Num'] = pd.to_numeric(df_diag['Depth'], errors='coerce')
                depth_only_df = df_diag.dropna(subset=['Depth_Num', 'NodeNum']).copy()
                
                if depth_only_df.empty:
                    st.info("No depth-based sensors (ground sticks/strings) found for this location.")
                else:
                    fig_depth = go.Figure()
                    # Generate snapshots for Monday mornings at 6:00 AM UTC
                    mondays = pd.date_range(start=start_view, end=end_view, freq='W-MON')
                    
                    for m_date in mondays:
                        target_ts = m_date.replace(hour=6, minute=0, second=0, tzinfo=pytz.UTC)
                        
                        # Grab closest points within a 12-hour window of the target
                        window = depth_only_df[
                            (depth_only_df['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & 
                            (depth_only_df['timestamp'] <= target_ts + pd.Timedelta(hours=12))
                        ]
                        
                        if not window.empty:
                            snap_list = []
                            for node in window['NodeNum'].unique():
                                node_data = window[window['NodeNum'] == node].copy()
                                node_data['diff'] = (node_data['timestamp'] - target_ts).abs()
                                snap_list.append(node_data.sort_values('diff').iloc[0])
                            
                            snap_df = pd.DataFrame(snap_list).sort_values('Depth_Num')
                            fig_depth.add_trace(go.Scattergl(
                                x=snap_df['temperature'], y=snap_df['Depth_Num'],
                                mode='lines+markers', name=target_ts.strftime('%m/%d/%y')
                            ))

                    # Formatting the Vertical Profile
                    y_limit = int(((depth_only_df['Depth_Num'].max() // 5) + 1) * 5)
                    x_range = [-20, 80] if unit_mode == "Fahrenheit" else [(-20-32)*5/9, (80-32)*5/9]
                    
                    fig_depth.update_layout(
                        plot_bgcolor='white', height=700,
                        xaxis=dict(title=f"Temp ({unit_label})", range=x_range, showgrid=True, gridcolor='Gainsboro'),
                        yaxis=dict(title="Depth (ft)", range=[y_limit, 0], dtick=10, showgrid=True, gridcolor='Gray'),
                        legend=dict(title="Weekly Snapshots (6AM UTC)", x=1.02, y=1)
                    )
                    st.plotly_chart(fig_depth, use_container_width=True)

                st.divider()

                # SECTION C: ENGINEERING STATUS TABLE
                st.subheader(f"📋 Engineering Summary: {sel_loc}")
                latest_nodes = df_diag.sort_values('timestamp').groupby('NodeNum').tail(1).copy()
                summary_rows = []
                
                for _, row in latest_nodes.iterrows():
                    node_id = row['NodeNum']
                    # Calculate how long since the last packet in UTC
                    hrs_ago = int((now_utc - row['timestamp']).total_seconds() / 3600)
                    status_icon = "🔴" if hrs_ago > 24 else ("🟢" if hrs_ago < 6 else "🟡")
                    
                    # Position Labeling
                    pos_display = f"Bank {row['Bank']}" if str(row['Bank']).strip().lower() not in ["","none","nan","null"] else f"{row['Depth']} ft"

                    summary_rows.append({
                        "Node": node_id,
                        "Position": pos_display,
                        "Last Reading": f"{round(row['temperature'], 1)}{unit_label}",
                        "Last Seen (UTC)": f"{row['timestamp'].strftime('%m/%d %H:%M')} ({hrs_ago}h ago)",
                        "Health": status_icon,
                        "Approval": "✅ Approved" if row['is_currently_approved'] == 'TRUE' else "❌ Rejected"
                    })
                
                st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, hide_index=True)

        except Exception as e:
            st.error(f"Diagnostics Rendering Error: {traceback.format_exc()}")
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
            st.info("Upload Lord SensorConnect (Wide), Lord Desktop Log (Narrow), or SensorPush (CSV/Excel).")
            
            # File uploader (Allows CSV and Excel)
            u_file = st.file_uploader("Upload CSV or Excel File", type=['csv', 'xlsx', 'xls'], key="manual_upload_unified_fixed")
            
            if u_file is not None:
                filename = u_file.name.lower()
                is_excel = filename.endswith(('.xlsx', '.xls'))
                
                try:
                    # 1. READ FILE INTO DATAFRAME
                    if is_excel:
                        try:
                            import openpyxl
                            df_raw = pd.read_excel(u_file)
                        except ImportError:
                            st.error("🚨 Missing `openpyxl`. Please install it or upload a CSV version.")
                            st.stop()
                    else:
                        # CSV Header Detection: Skip metadata rows if present (like in Lord Wide)
                        raw_bytes = u_file.getvalue().decode('utf-8').splitlines()
                        header_idx = 0
                        for i, line in enumerate(raw_bytes[:100]):
                            # Look for common header keywords
                            if any(k in line for k in ["Timestamp", "Channel", "nodenumber", "SensorId", "Observed"]):
                                header_idx = i
                                break
                        df_raw = pd.read_csv(io.StringIO("\n".join(raw_bytes[header_idx:])))

                    # Clean headers: Strip whitespace and identify columns case-insensitively
                    df_raw.columns = [str(c).strip() for c in df_raw.columns]
                    cols_lower = [c.lower() for c in df_raw.columns]

                    # 2. IDENTIFY FORMAT
                    # A. Lord Wide (SensorConnect) - Usually has "DATA_START" in the preamble
                    is_lord_wide = not is_excel and any("DATA_START" in line for line in raw_bytes[:100])
                    
                    # B. Lord Narrow (Your current file: Timestamp, Channel, Temperature)
                    is_lord_narrow = "channel" in cols_lower or "nodenumber" in cols_lower
                    
                    # C. SensorPush
                    is_sensorpush = "sensorid" in cols_lower or "observed" in cols_lower

                    # --- CASE 1: LORD WIDE ---
                    if is_lord_wide:
                        start_idx = next(i for i, line in enumerate(raw_bytes) if "DATA_START" in line)
                        df_wide = pd.read_csv(io.StringIO("\n".join(raw_bytes[start_idx+1:])))
                        df_long = df_wide.melt(id_vars=['Time'], var_name='NodeNum', value_name='temperature')
                        df_long['NodeNum'] = df_long['NodeNum'].str.replace(':', '-', regex=False)
                        df_long['timestamp'] = pd.to_datetime(df_long['Time'], format='mixed')
                        
                        st.success(f"✅ Lord Wide Format Parsed: {len(df_long)} readings.")
                        st.dataframe(df_long.head())
                        if st.button("🚀 UPLOAD LORD WIDE DATA"):
                            client.load_table_from_dataframe(df_long[['timestamp', 'NodeNum', 'temperature']], f"{PROJECT_ID}.{DATASET_ID}.raw_lord").result()
                            st.success("Uploaded to raw_lord!")
                            st.cache_data.clear()

                    # --- CASE 2: LORD NARROW (Matches 62260.xlsx) ---
                    elif is_lord_narrow:
                        # Map your specific columns to the standard schema
                        mapping = {}
                        for c in df_raw.columns:
                            cl = c.lower()
                            if "timestamp" in cl: mapping[c] = "timestamp"
                            elif "channel" in cl or "nodenumber" in cl: mapping[c] = "NodeNum"
                            elif "temperature" in cl: mapping[c] = "temperature"
                        
                        df_ln = df_raw.rename(columns=mapping)
                        df_ln['timestamp'] = pd.to_datetime(df_ln['timestamp'], format='mixed')
                        df_ln['NodeNum'] = df_ln['NodeNum'].astype(str).str.replace(':', '-', regex=False)
                        df_ln = df_ln.dropna(subset=['timestamp', 'temperature', 'NodeNum'])

                        st.success(f"✅ Lord Narrow Format Parsed: {len(df_ln)} readings.")
                        st.dataframe(df_ln[['timestamp', 'NodeNum', 'temperature']].head())
                        if st.button("🚀 UPLOAD LORD NARROW DATA"):
                            client.load_table_from_dataframe(df_ln[['timestamp', 'NodeNum', 'temperature']], f"{PROJECT_ID}.{DATASET_ID}.raw_lord").result()
                            st.success("Uploaded to raw_lord!")
                            st.cache_data.clear()

                    # --- CASE 3: SENSORPUSH ---
                    elif is_sensorpush:
                        id_col = next((c for c in df_raw.columns if "sensorid" in c.lower()), None)
                        ts_col = next((c for c in df_raw.columns if "observed" in c.lower()), None)
                        temp_col = next((c for c in df_raw.columns if any(k in c.lower() for k in ["temperature", "thermocouple"])), None)
                        
                        if id_col and ts_col and temp_col:
                            df_sp = pd.DataFrame()
                            df_sp['sensor_id'] = df_raw[id_col].astype(str).str.strip()
                            df_sp['timestamp'] = pd.to_datetime(df_raw[ts_col], format='mixed')
                            df_sp['temperature'] = pd.to_numeric(df_raw[temp_col], errors='coerce')
                            df_sp = df_sp.dropna(subset=['timestamp', 'temperature'])

                            st.success(f"✅ SensorPush Format Parsed: {len(df_sp)} readings.")
                            st.dataframe(df_sp.head())
                            if st.button("🚀 UPLOAD SENSORPUSH DATA"):
                                client.load_table_from_dataframe(df_sp, f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush").result()
                                st.success("Uploaded to raw_sensorpush!")
                                st.cache_data.clear()
                        else:
                            st.error("Required columns (SensorId/Observed) not found for SensorPush.")

                    else:
                        st.error("Unrecognized format. File must contain 'Channel' (Lord) or 'SensorId' (SensorPush).")
                        st.write("Columns found:", list(df_raw.columns))

                except Exception as e:
                    st.error(f"Upload Error: {e}")
                    st.exception(e)

       # 3. DEEP DATA SCRUB (Physical Purge) - SNAP TO HOUR FIX
        with tab2:
            st.subheader("🧹 Deep Data Scrub & Final Purge")
            st.info("This tool dedups data to 1-hour intervals and snaps all timestamps to the **Top of the Hour**.")
            st.error("⚠️ WARNING: This permanently modifies data in RAW tables.")
            
            scrub_target = st.radio("Target Table", ["SensorPush", "Lord"], horizontal=True, key="scrub_radio_fixed")
            
            # Use the exact column names from your ingestion functions
            if scrub_target == "SensorPush":
                target_table = f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush"
                node_col = "sensor_id"
                temp_col = "temperature"
            else:
                target_table = f"{PROJECT_ID}.{DATASET_ID}.raw_lord"
                node_col = "NodeNum" # Fixed from nodenumber
                temp_col = "temperature" # Fixed from value
    
            if st.button(f"🧨 Permanently Purge & Snap {scrub_target}"):
                with st.spinner("Executing hard delete and snap-to-hour..."):
                    # This snaps the timestamp to the top of the hour (:00:00)
                    scrub_sql = f"""
                    CREATE OR REPLACE TABLE `{target_table}` AS 
                    SELECT 
                        TIMESTAMP_TRUNC(TIMESTAMP_ADD(timestamp, INTERVAL 30 MINUTE), HOUR) as timestamp, 
                        * EXCEPT(timestamp, rn) 
                    FROM (
                        SELECT *, 
                               ROW_NUMBER() OVER(
                                   PARTITION BY {node_col}, TIMESTAMP_TRUNC(TIMESTAMP_ADD(timestamp, INTERVAL 30 MINUTE), HOUR) 
                                   ORDER BY timestamp DESC
                               ) as rn
                        FROM `{target_table}` 
                        WHERE (approve IS NULL OR UPPER(CAST(approve AS STRING)) != 'FALSE')
                        AND {temp_col} IS NOT NULL
                    ) WHERE rn = 1
"""
                    try:
                        client.query(scrub_sql).result()
                        st.success(f"✅ {scrub_target} purged and snapped to top-of-hour.")
                        st.cache_data.clear()
                    except Exception as e:
                        st.error(f"Scrub Error: {e}")
                        
        with tab3:
            st.subheader("📥 Export Project Data (SensorConnect Format)")
            
            # Project Selection
            all_projects_q = f"SELECT DISTINCT Project FROM `{PROJECT_ID}.{DATASET_ID}.metadata` WHERE Project IS NOT NULL"
            all_projs = client.query(all_projects_q).to_dataframe()['Project'].tolist()
            default_ix = all_projs.index(selected_project) if selected_project in all_projs else 0
            target_project = st.selectbox("1️⃣ Select Project to Export", sorted(all_projs), index=default_ix)
        
            if target_project:
                with st.spinner(f"Loading data for {target_project}..."):
                    export_df = get_universal_portal_data(target_project, only_approved=False)
        
                if not export_df.empty:
                    c1, c2 = st.columns(2)
                    with c1:
                        pipes = sorted(export_df['Location'].dropna().unique().tolist())
                        sel_pipe = st.selectbox("2️⃣ Select Pipe / Location", pipes)
                    with c2:
                        min_ts, max_ts = export_df['timestamp'].min().date(), export_df['timestamp'].max().date()
                        export_range = st.date_input("3️⃣ Select Date Range", value=(min_ts, max_ts))
        
                    # --- TRANSFORMATION TO WIDE FORMAT ---
                    df_final = export_df[export_df['Location'] == sel_pipe].copy()
                    
                    if isinstance(export_range, tuple) and len(export_range) == 2:
                        start, end = export_range
                        df_final = df_final[(df_final['timestamp'].dt.date >= start) & (df_final['timestamp'].dt.date <= end)]
        
                    if not df_final.empty:
                        # 1. Standardize Units
                        if unit_mode == "Celsius":
                            df_final['temperature'] = (df_final['temperature'] - 32) * 5/9
                        
                        # 2. Create Column Labels (e.g., "5ft")
                        df_final['Depth_Col'] = df_final['Depth'].astype(str) + "ft"
                        
                        # 3. PIVOT: Turns Depths into Columns
                        df_wide = df_final.pivot_table(
                            index='timestamp', 
                            columns='Depth_Col', 
                            values='temperature',
                            aggfunc='first' 
                        ).reset_index()

                        # 4. NATURAL SORT: Ensures 5ft < 10ft < 20ft
                        def depth_sort_key(col):
                            if col == 'timestamp': return -1
                            nums = re.findall(r'\d+', col)
                            return int(nums[0]) if nums else 999
                        
                        df_wide = df_wide.reindex(columns=sorted(df_wide.columns, key=depth_sort_key))

                        st.divider()
                        st.write(f"📊 **Preview:** {sel_pipe} ({len(df_wide)} rows)")
                        st.dataframe(df_wide.head(10), use_container_width=True)
                        
                        csv_bytes = df_wide.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label=f"💾 Download {sel_pipe} Wide CSV",
                            data=csv_bytes,
                            file_name=f"SensorConnect_{target_project}_{sel_pipe}.csv",
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
    
        # 1. DEFINE TABS
        tab1, tab2, tab3 = st.tabs(["✅ Bulk Approval", "🧹 Deep Data Scrub", "🧨 Surgical Cleaner"])
    
        with tab1:
            st.subheader("✅ Bulk Approval")
            if st.button("🚀 Approve All Pending Data"):
                raw_tables = [f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush", f"{PROJECT_ID}.{DATASET_ID}.raw_lord"]
                for table in raw_tables:
                    client.query(f"UPDATE `{table}` SET approve = 'TRUE' WHERE approve IS NULL OR UPPER(CAST(approve AS STRING)) != 'FALSE'").result()
                st.success("Bulk approval complete.")
                st.cache_data.clear()

        with tab2:
            st.subheader("🧹 Deep Data Scrub & Final Purge")
            st.error("⚠️ WARNING: This permanently deletes data from RAW tables.")
            
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
                    try:
                        client.query(scrub_sql).result()
                        st.success(f"{scrub_target} purged and deduped to 1-hour intervals.")
                        st.cache_data.clear()
                    except Exception as e:
                        st.error(f"Scrub Error: {e}")
            

        # 4. SURGICAL CLEANER (State-Locked Lasso)
        with tab3:
            st.subheader("🧨 Surgical Data Cleaner")
            if not selected_project:
                st.warning("👈 Please select a Project in the sidebar.")
            else:
                p_df = get_universal_portal_data(selected_project, only_approved=False)
        
                if not p_df.empty:
                    # Filter and RESET INDEX
                    loc_options = sorted(p_df['Location'].dropna().unique())
                    sel_loc = st.selectbox("Select Pipe", loc_options, key="admin_scrub_loc")
                    scrub_plot_df = p_df[p_df['Location'] == sel_loc].copy().reset_index(drop=True)

                    # --- THE STATE LOCK ---
                    # Use a 'confirmed' state that the widget cannot touch directly
                    if "locked_selection" not in st.session_state:
                        st.session_state.locked_selection = None

                    # 3. BUILD GRAPH
                    fig_scrub = build_high_speed_graph(
                        scrub_plot_df, f"Scrubbing: {sel_loc}", 
                        pd.Timestamp.now(tz='UTC') - timedelta(days=7), 
                        pd.Timestamp.now(tz='UTC') + timedelta(hours=6), 
                        tuple(active_refs), unit_mode, unit_label, display_tz=display_tz
                    )

                    # 4. FORCE DRAW THE LOCK
                    # If we have a locked selection, force Plotly to show it
                    if st.session_state.locked_selection:
                        selected_indices = [p['point_index'] for p in st.session_state.locked_selection]
                        fig_scrub.update_traces(
                            selectedpoints=selected_indices, 
                            unselected=dict(marker=dict(opacity=0.3))
                        )

                    # 5. RENDER CHART
                    # use_container_width=True is mandatory for your version
                    event_data = st.plotly_chart(
                        fig_scrub, 
                        use_container_width=True, 
                        on_select="rerun", 
                        key=f"scrub_chart_{sel_loc.replace(' ', '_')}"
                    )

                    # 6. LOCKING LOGIC (The One-Way Valve)
                    # We ONLY update the lock if there are actual points in the event.
                    # This prevents the 'reset' from wiping your selection.
                    if event_data and "selection" in event_data:
                        current_event_points = event_data["selection"].get("points", [])
                        if len(current_event_points) > 0:
                            st.session_state.locked_selection = current_event_points

                    # 7. UI FOR LOCKED SELECTION
                    if st.session_state.locked_selection:
                        points = st.session_state.locked_selection
                        st.success(f"📍 {len(points)} points locked in memory.")
                        
                        c1, c2 = st.columns(2)
                        if c1.button("🚨 HIDE SELECTED DATA", type="primary"):
                            rejection_records = []
                            for pt in points:
                                raw_ts = pd.to_datetime(pt['x'])
                                scrub_ts = raw_ts.tz_convert('UTC').floor('h')
                                node_id = scrub_plot_df.iloc[pt['point_index']]['NodeNum']
                                rejection_records.append({"NodeNum": str(node_id), "timestamp": scrub_ts, "reason": "Surgical Scrub", "Project": selected_project})
                            
                            if rejection_records:
                                rej_df = pd.DataFrame(rejection_records).drop_duplicates()
                                client.load_table_from_dataframe(rej_df, f"{PROJECT_ID}.{DATASET_ID}.manual_rejections").result()
                                st.session_state.locked_selection = None
                                st.cache_data.clear()
                                st.rerun()

                        if c2.button("🧹 Clear Selection"):
                            st.session_state.locked_selection = None
                            st.rerun()
                    else:
                        st.info("💡 Use the Lasso tool. Selection is now locked and will not disappear.")
                                
###########################
# --- END ADMIN TOOLS --- #
###########################
