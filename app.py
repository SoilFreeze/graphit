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

@st.cache_data(ttl=600)
def get_universal_portal_data(project_id):
    # Added: AND temperature != 0 to the WHERE clause
    query = f"""
        SELECT timestamp, temperature, Depth, Location, Bank, NodeNum, approve
        FROM `{MASTER_TABLE}`
        WHERE Project = '{project_id}' 
        AND (approve = 'TRUE' OR approve = 'true')
        AND temperature != 0
        AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 84 DAY)
        ORDER BY Location ASC, timestamp ASC
    """
    df = client.query(query).to_dataframe()
    # ... (rest of your existing timestamp processing)
    return df

def build_high_speed_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label):
    """
    High-Performance Graph Engine:
    - 3-tier Grid Hierarchy
    - 6-Hour Gap Detection (Breaks the line)
    - Red 'Now' Line
    - ZERO-VALUE FILTER: Prevents vertical lines dropping to 0
    """
    if df.empty: 
        return go.Figure()

    plot_df = df.copy()

    # --- NEW: VERTICAL LINE FIX ---
    # Convert exactly 0.0 to None so Plotly breaks the line instead of diving to 0
    plot_df.loc[plot_df['temperature'] == 0, 'temperature'] = None

    # Unit Conversion Logic
    if unit_mode == "Celsius":
        # Only convert if the value isn't None
        plot_df['temperature'] = plot_df['temperature'].apply(
            lambda x: (x - 32) * 5/9 if x is not None else None
        )
        y_range = [( -20 - 32) * 5/9, (80 - 32) * 5/9]
        dt_major, dt_minor = 10, 2 
    else:
        y_range = [-20, 80]
        dt_major, dt_minor = 20, 5

    # Labeling Logic
    plot_df['label'] = plot_df.apply(
        lambda r: f"Bank {r['Bank']} ({r['NodeNum']})" if str(r.get('Bank')).strip().lower() not in ["", "none", "nan", "null"]
        else f"{r.get('Depth')}ft ({r.get('NodeNum')})", axis=1
    )
    
    fig = go.Figure()
    
    # 1. CORE DATA PLOTTING WITH GAP HANDLING
    for lbl in sorted(plot_df['label'].unique()):
        s_df = plot_df[plot_df['label'] == lbl].sort_values('timestamp')
        
        # --- GAP DETECTOR ---
        # If time between points > 6 hours, insert a row with temperature=None to break the line
        s_df['gap_hrs'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
        gap_mask = s_df['gap_hrs'] > 6.0
        
        if gap_mask.any():
            gaps = s_df[gap_mask].copy()
            gaps['temperature'] = None
            # Move the 'gap' point slightly back so it sits between the real data points
            gaps['timestamp'] = gaps['timestamp'] - pd.Timedelta(minutes=1)
            s_df = pd.concat([s_df, gaps]).sort_values('timestamp')

        fig.add_trace(go.Scattergl(
            x=s_df['timestamp'], 
            y=s_df['temperature'], 
            name=lbl, 
            mode='lines', 
            connectgaps=False, # CRITICAL: Ensures Nones actually break the line
            line=dict(width=2)
        ))

    # 2. GRID HIERARCHY (Monday/Midnight markers)
    grid_times = pd.date_range(start=start_view, end=end_view, freq='6h')
    for ts in grid_times:
        if ts.weekday() == 0 and ts.hour == 0:
            color, width = "Black", 1.5
        elif ts.hour == 0:
            color, width = "Gray", 1.0
        else:
            color, width = "LightGray", 0.5
        fig.add_vline(x=ts, line_width=width, line_color=color, layer='below')

    # 3. RED "NOW" LINE
    now_marker = pd.Timestamp.now(tz=pytz.UTC)
    fig.add_vline(x=now_marker, line_width=2, line_color="Red", layer='above', line_dash="dash")

    # 4. STYLING & AXIS CONFIG
    fig.update_layout(
        title={'text': title, 'x': 0, 'font': dict(size=18)},
        plot_bgcolor='white', 
        hovermode="x unified", 
        height=600,
        margin=dict(t=80, l=50, r=180, b=50),
        legend=dict(title="Sensors", orientation="v", yanchor="top", y=1, xanchor="left", x=1.02),
        xaxis=dict(range=[start_view, end_view], showline=True, linecolor='black', mirror=True),
        yaxis=dict(title=f"Temp ({unit_label})", range=y_range, dtick=dt_minor, gridcolor='Gainsboro', showline=True, linecolor='black', mirror=True)
    )

    # Major Horizontal Gridlines
    for y_val in range(int(y_range[0]), int(y_range[1]) + 1, dt_major):
        fig.add_hline(y=y_val, line_width=1.2, line_color="DimGray", layer='below')

    # Reference Thresholds (Freezing, Type A, Type B)
    for val, label in active_refs:
        c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
        fig.add_hline(y=c_val, line_dash="dash", line_color="maroon" if "Type A" in label else "RoyalBlue", opacity=0.8)
    
    return fig
    
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
        INNER JOIN `{PROJECT_ID}.{DATASET_ID}.master_metadata` m 
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


############################
# --- Graph --- #
############################
def build_high_speed_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label):
    """
    High-Performance Graph Engine:
    - 3-tier Grid Hierarchy
    - 6-Hour Gap Detection (Breaks the line)
    - Red 'Now' Line
    """
    if df.empty: return go.Figure()

    plot_df = df.copy()
    # Unit Conversion
    if unit_mode == "Celsius":
        plot_df['temperature'] = (plot_df['temperature'] - 32) * 5/9
        y_range = [( -20 - 32) * 5/9, (80 - 32) * 5/9]
        dt_major, dt_minor = 10, 2 
    else:
        y_range = [-20, 80]
        dt_major, dt_minor = 20, 5

    # Labeling
    plot_df['label'] = plot_df.apply(
        lambda r: f"Bank {r['Bank']} ({r['NodeNum']})" if str(r.get('Bank')).strip().lower() not in ["", "none", "nan", "null"]
        else f"{r.get('Depth')}ft ({r.get('NodeNum')})", axis=1
    )
    
    fig = go.Figure()
    
    # 1. CORE DATA PLOTTING WITH GAP HANDLING
    for lbl in sorted(plot_df['label'].unique()):
        s_df = plot_df[plot_df['label'] == lbl].sort_values('timestamp')
        
        # --- GAP DETECTOR ---
        # If time between points > 6 hours, insert a None row to break the line
        s_df['gap_hrs'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
        gap_mask = s_df['gap_hrs'] > 6.0
        if gap_mask.any():
            gaps = s_df[gap_mask].copy()
            gaps['temperature'] = None
            gaps['timestamp'] = gaps['timestamp'] - pd.Timedelta(minutes=1)
            s_df = pd.concat([s_df, gaps]).sort_values('timestamp')

        fig.add_trace(go.Scattergl(
            x=s_df['timestamp'], y=s_df['temperature'], 
            name=lbl, mode='lines', connectgaps=False, line=dict(width=2)
        ))

    # 2. GRID HIERARCHY
    grid_times = pd.date_range(start=start_view, end=end_view, freq='6h')
    for ts in grid_times:
        if ts.weekday() == 0 and ts.hour == 0:
            color, width = "Black", 1.5
        elif ts.hour == 0:
            color, width = "Gray", 1.0
        else:
            color, width = "LightGray", 0.5
        fig.add_vline(x=ts, line_width=width, line_color=color, layer='below')

    # 3. RED "NOW" LINE
    now_marker = pd.Timestamp.now(tz=pytz.UTC)
    fig.add_vline(x=now_marker, line_width=2, line_color="Red", layer='above', line_dash="dash")

    # 4. STYLING
    fig.update_layout(
        title={'text': title, 'x': 0, 'font': dict(size=18)},
        plot_bgcolor='white', hovermode="x unified", height=600,
        margin=dict(t=80, l=50, r=180, b=50),
        legend=dict(title="Sensors", orientation="v", yanchor="top", y=1, xanchor="left", x=1.02),
        xaxis=dict(range=[start_view, end_view], showline=True, linecolor='black', mirror=True),
        yaxis=dict(title=f"Temp ({unit_label})", range=y_range, dtick=dt_minor, gridcolor='Gainsboro', showline=True, linecolor='black', mirror=True)
    )

    for y_val in range(int(y_range[0]), int(y_range[1]) + 1, dt_major):
        fig.add_hline(y=y_val, line_width=1.2, line_color="DimGray", layer='below')

    for val, label in active_refs:
        c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
        fig.add_hline(y=c_val, line_dash="dash", line_color="maroon" if "Type A" in label else "RoyalBlue", opacity=0.8)
    
    return fig


#######################
# --- SIDEBAR UI --- #
#######################
st.sidebar.title("❄️ SoilFreeze Lab")

service = st.sidebar.selectbox("📂 Select Page", ["🏠 Executive Summary", "📊 Client Portal", "📉 Node Diagnostics", "📤 Data Intake Lab", "🛠️ Admin Tools"])
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

####################
# --- SERVICES --- #
####################
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
        st.warning("Please select a project in the sidebar.")
    else:
        try:
            # 1. DATA ACCESS & CONTROLS
            all_data = get_universal_portal_data(selected_project)
            loc_options = sorted(all_data['Location'].dropna().unique())
            
            c1, c2 = st.columns([2, 1])
            with c1: 
                sel_loc = st.selectbox("Select Pipe / Bank to Analyze", loc_options)
            with c2: 
                weeks_view = st.slider("Lookback (Weeks)", 1, 12, 6, key="diag_lookback")

            # 2. DATE CALCULATIONS
            now = pd.Timestamp.now(tz=pytz.UTC)
            days_until_monday = (7 - now.weekday()) % 7
            if days_until_monday == 0: days_until_monday = 7
            end_view = (now + pd.Timedelta(days=days_until_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
            start_view = end_view - timedelta(weeks=weeks_view)

            df_diag = all_data[all_data['Location'] == sel_loc].copy()
            
            if df_diag.empty:
                st.warning(f"No data found for {sel_loc} in the selected timeframe.")
            else:
                # --- 3. TIME VS TEMPERATURE GRAPH ---
                st.subheader("📈 Timeline Analysis")
                fig_time = build_high_speed_graph(df_diag, sel_loc, start_view, end_view, tuple(active_refs), unit_mode, unit_label)
                st.plotly_chart(fig_time, use_container_width=True, config={'displayModeBar': False})

                st.divider()

                # --- 4. DEPTH VS TEMPERATURE GRAPH ---
                st.subheader("📏 Depth Profile Analysis")
                df_diag['Depth_Num'] = pd.to_numeric(df_diag['Depth'], errors='coerce')
                depth_only_df = df_diag.dropna(subset=['Depth_Num', 'NodeNum']).copy()
                
                if depth_only_df.empty:
                    st.info("No depth-based sensors found for this location.")
                else:
                    fig_depth = go.Figure()
                    mondays = pd.date_range(start=start_view, end=end_view, freq='W-MON')
                    
                    for m_date in mondays:
                        # --- FIXED TIMEZONE LOGIC ---
                        # If m_date has no timezone, localize it. If it has one, just set the hour.
                        target_ts = m_date.replace(hour=6, minute=0, second=0)
                        if target_ts.tzinfo is None:
                            target_ts = target_ts.tz_localize(pytz.UTC)
                        else:
                            target_ts = target_ts.tz_convert(pytz.UTC)
                        
                        window = depth_only_df[(depth_only_df['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & 
                                               (depth_only_df['timestamp'] <= target_ts + pd.Timedelta(hours=12))]
                        
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

                    # Grid Styling
                    x_range = [-20, 80] if unit_mode == "Fahrenheit" else [(-20-32)*5/9, (80-32)*5/9]
                    y_limit = int(((depth_only_df['Depth_Num'].max() // 5) + 1) * 5)

                    fig_depth.update_layout(
                        plot_bgcolor='white', height=700,
                        xaxis=dict(title=f"Temp ({unit_label})", range=x_range, dtick=5 if unit_mode=="Fahrenheit" else 2, 
                                   showgrid=True, gridcolor='Gainsboro'),
                        yaxis=dict(title="Depth (ft)", range=[y_limit, 0], dtick=10, 
                                   showgrid=True, gridcolor='Gray', gridwidth=0.7),
                        legend=dict(title="Weekly Snapshots (6AM)", orientation="v", x=1.02, y=1)
                    )
                    for x_v in range(int(x_range[0]), int(x_range[1]) + 1, 20 if unit_mode=="Fahrenheit" else 10):
                        fig_depth.add_vline(x=x_v, line_width=1.5, line_color="DimGray")
                    for val, label in active_refs:
                        c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
                        fig_depth.add_vline(x=c_val, line_dash="dash", line_color="maroon" if "Type A" in label else "RoyalBlue", line_width=2.5)
                    
                    st.plotly_chart(fig_depth, use_container_width=True, config={'displayModeBar': False})

                st.divider()

                # --- 5. ENGINEERING SUMMARY TABLE ---
                st.subheader(f"📋 Engineering Summary: {sel_loc}")
                latest_nodes = df_diag.sort_values('timestamp').groupby('NodeNum').tail(1).copy()
                summary_rows = []
                
                for _, row in latest_nodes.iterrows():
                    node_id = row['NodeNum']
                    day_data = df_diag[(df_diag['NodeNum'] == node_id) & (df_diag['timestamp'] >= now - pd.Timedelta(hours=24))]
                    
                    if not day_data.empty:
                        min_v, max_v = day_data['temperature'].min(), day_data['temperature'].max()
                        raw_delta = day_data['temperature'].iloc[-1] - day_data['temperature'].iloc[0]
                    else:
                        min_v, max_v, raw_delta = None, None, None

                    hrs_ago = int((now - row['timestamp']).total_seconds() / 3600)
                    status_icon = "🔴" if hrs_ago > 24 else ("🟢" if hrs_ago < 6 else "🟡")
                    pos_display = f"Bank {row['Bank']}" if str(row['Bank']).strip().lower() not in ["","none","nan","null"] else f"{row['Depth']} ft"

                    summary_rows.append({
                        "Node": node_id,
                        "Pos/Depth": pos_display,
                        "Min (24h)": f"{round(convert_val(min_v), 1)}{unit_label}" if min_v is not None else "N/A",
                        "Max (24h)": f"{round(convert_val(max_v), 1)}{unit_label}" if max_v is not None else "N/A",
                        "Delta (24h)": f"{round(raw_delta, 1)}°F" if raw_delta is not None else "0.0°F",
                        "Delta_Val": raw_delta,
                        "Last Seen": f"{row['timestamp'].strftime('%m/%d %H:%M')} ({hrs_ago}h) {status_icon}"
                    })
                
                summary_df = pd.DataFrame(summary_rows)
                st.dataframe(
                    summary_df[["Node", "Pos/Depth", "Min (24h)", "Max (24h)", "Delta (24h)", "Last Seen"]].style.apply(
                        lambda x: [style_delta(rv) for rv in summary_df['Delta_Val']], axis=0, subset=['Delta (24h)']
                    ),
                    use_container_width=True, hide_index=True
                )

        except Exception as e:
            st.error(f"Diagnostics Error: {traceback.format_exc()}")
###############################
# --- END NODE DIAGNOSTIC --- #
###############################
###############################
# --- DATA INTAKE LAB --- #
###############################
elif service == "📤 Data Intake Lab":
    st.header("📤 Data Ingestion & Recovery")
    
    tab1, tab2, tab3 = st.tabs(["📄 Manual File Upload", "📡 API Data Recovery", "🛠️ Maintenance"])

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
                    
    with tab3:
        st.subheader("🛠️ Metadata Management")
        u_meta = st.file_uploader("Upload Master_Log / Metadata CSV", type=['csv'])
        if u_meta:
            df_new_meta = pd.read_csv(u_meta)
            st.dataframe(df_new_meta.head())
            if st.button("Overwrite Master Metadata"):
                # This replaces the mapping table in BigQuery
                client.load_table_from_dataframe(df_new_meta, f"{PROJECT_ID}.{DATASET_ID}.master_metadata", 
                                                 job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")).result()
                st.success("Master Metadata Updated!")

from google.cloud import bigquery

def refresh_snapshot():
    client = bigquery.Client()
    
    sql = """
    CREATE OR REPLACE TABLE `your_project.your_dataset.metadata_snapshot` AS
    SELECT * FROM `your_project.your_dataset.metadata`
    """
    
    try:
        query_job = client.query(sql)
        query_job.result()  # Wait for the job to complete
        return "Success! Snapshot updated."
    except Exception as e:
        return f"Error: {e}"

# In your Streamlit UI
if st.button('Update App Data from Google Sheets'):
    result = refresh_snapshot()
    st.success(result)
###############################
# --- END DATA INTAKE LAB --- #
###############################
#######################
# --- ADMIN TOOLS --- #
#######################             
elif service == "🛠️ Admin Tools":
    st.header("🛠️ Engineering Admin Tools")
    
    tab_scrub, tab_approve, tab_cleaner = st.tabs(["🧹 Deep Data Scrub", "✅ Bulk Approval", "🧨 Surgical Cleaner"])

    ###########################
# --- ADMIN TOOLS REVISED --- #
###########################
with tab_approve:
    st.subheader("✅ Bulk Approval")
    if st.button("🚀 Approve All Pending Data"):
        # REMOVED MASTER_TABLE because it is a VIEW
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
        st.success("Bulk approval complete. View will update automatically.")
        st.cache_data.clear()

with tab_cleaner:
    st.subheader("🧨 Surgical Data Cleaner: Lasso Selection")
    p_df = pd.DataFrame() 

    # UI Setup
    meta_df = client.query(f"SELECT DISTINCT Project, Location, NodeNum FROM `{MASTER_TABLE}`").to_dataframe().fillna("N/A")
    c1, c2, c3 = st.columns(3)
    with c1: sel_proj = st.selectbox("Project", sorted(meta_df['Project'].unique()), key="lasso_proj")
    with c2: 
        pipes = ["ALL"] + sorted(meta_df[meta_df['Project'] == sel_proj]['Location'].unique())
        sel_pipe = st.selectbox("Pipe / Location", pipes, key="lasso_pipe")
    with c3:
        nodes = ["ALL"] + sorted(meta_df[meta_df['Location'] == sel_pipe]['NodeNum'].unique()) if sel_pipe != "ALL" else ["ALL"]
        sel_node = st.selectbox("Node ID", nodes, key="lasso_node")

    lookback_hrs = st.slider("Lookback Window (Hours)", 6, 168, 24, format="%d hours ago")

    # Preview Query (Selecting from the View is fine)
    preview_q = f"""
        SELECT timestamp, temperature, NodeNum, Location 
        FROM `{MASTER_TABLE}` 
        WHERE Project = '{sel_proj}' 
        AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_hrs} HOUR)
        ORDER BY timestamp ASC
    """
    p_df = client.query(preview_q).to_dataframe()

    if not p_df.empty:
        if sel_pipe != "ALL": p_df = p_df[p_df['Location'] == sel_pipe]
        if sel_node != "ALL": p_df = p_df[p_df['NodeNum'] == sel_node]

        if not p_df.empty:
            fig = px.scatter(p_df, x='timestamp', y='temperature', color='NodeNum', 
                             title=f"Lasso selection (Last {lookback_hrs}h)",
                             template="plotly_white", height=600)
            fig.update_layout(dragmode='lasso', hovermode='closest')
            fig.update_traces(marker=dict(size=10, opacity=0.7))

            selected_points = st.plotly_chart(fig, use_container_width=True, on_select="rerun")

            if selected_points and "selection" in selected_points and len(selected_points["selection"]["points"]) > 0:
                points = selected_points["selection"]["points"]
                st.warning(f"⚠️ {len(points)} points highlighted.")
                
                if st.button("🚫 HIDE SELECTED DATA"):
                    with st.spinner("Updating raw tables..."):
                        try:
                            # Build conditions for the RAW tables
                            conditions = [f"(NodeNum = '{p_df.iloc[pt['point_index']]['NodeNum']}' AND timestamp = CAST('{pt['x']}' AS TIMESTAMP))" for pt in points]
                            where_clause = " OR ".join(conditions)
                            
                            # ONLY update physical tables
                            raw_tables = [f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush", 
                                          f"{PROJECT_ID}.{DATASET_ID}.raw_lord"]
                                          
                            for table in raw_tables:
                                update_sql = f"UPDATE `{table}` SET approve = 'FALSE' WHERE {where_clause}"
                                client.query(update_sql).result()
                            
                            st.success("Points hidden in raw tables. Refreshing view...")
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Hide failed: {e}")
###########################
# --- END ADMIN TOOLS --- #
###########################
