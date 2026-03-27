import streamlit as st
import pandas as pd
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
#############################
# --- END CONFIGURATION --- #
#############################
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
#############################
# --- END REBUILD TABLE --- #
#############################
############################
# --- FETCH SENSORPUSH --- #
############################
        
# --- 2. ENGINE: FAST API FETCH ---

def fetch_sensorpush_data(start_dt, end_dt):
    """
    Fetches data for ALL sensors (HT.w and TC.x) in 12-hour chunks 
    to prevent timeouts.
    """
    ACCOUNTS = [
        {'email': 'tsteele@soilfreeze.com', 'password': 'Freeze123!!'},
        {'email': 'soilfreeze98072@gmail.com', 'password': 'Freeze123!!'}
    ]
    BASE_URL = "https://api.sensorpush.com/api/v1"
    all_records = []

    for acc in ACCOUNTS:
        try:
            # 1. Auth
            auth_resp = requests.post(f"{BASE_URL}/oauth/authorize", json=acc, timeout=15)
            if auth_resp.status_code != 200: continue
            token = requests.post(f"{BASE_URL}/oauth/accesstoken", 
                                   json={"authorization": auth_resp.json().get('authorization')}, timeout=15).json().get('accesstoken')
            headers = {"Authorization": token}

            # 2. Get All Sensors (HT.w and TC.x)
            dev_resp = requests.post(f"{BASE_URL}/devices/sensors", headers=headers, json={}, timeout=20)
            name_map = {}
            if dev_resp.status_code == 200:
                s_list = dev_resp.json().values() if isinstance(dev_resp.json(), dict) else dev_resp.json()
                for s in s_list:
                    s_type = str(s.get('type', ''))
                    # Updated Filter: Allow both HT.w and TC.x
                    if s_type in ['HT.w', 'TC.x']:
                        name_map[str(s.get('id'))] = s.get('name', str(s.get('id')))

            # 3. Chunked Fetching (12-hour blocks for stability)
            current_start = start_dt
            while current_start < end_dt:
                current_end = min(current_start + timedelta(hours=12), end_dt)
                payload = {
                    "limit": 10000, 
                    "startTime": current_start.strftime('%Y-%m-%dT%H:%M:%S+0000'), 
                    "sensors": list(name_map.keys())
                }
                # Increased timeout to 60s
                r = requests.post(f"{BASE_URL}/samples", headers=headers, json=payload, timeout=60)
                
                if r.status_code == 200:
                    data = r.json().get('sensors', {})
                    for s_id, samples in data.items():
                        for s in samples:
                            ts = pd.to_datetime(s.get('observed'))
                            if current_start <= ts <= current_end:
                                # Logic to handle temperature from different sensor types
                                temp = s.get('temp_f') or s.get('temperature') or s.get('thermocouple_temperature')
                                if temp is None and s.get('temp_c') is not None:
                                    temp = (float(s['temp_c']) * 1.8) + 32
                                
                                if temp is not None:
                                    all_records.append({
                                        'timestamp': ts,
                                        'sensor_id': s_id.replace(':', '-'),
                                        'sensor_name': name_map.get(s_id),
                                        'temperature': round(float(temp), 2)
                                    })
                current_start = current_end
        except Exception as e:
            st.error(f"Fetch Error ({acc['email']}): {e}")
            
    return pd.DataFrame(all_records)
################################
# --- END FETCH SENSORPUSH --- #
################################
########################
# --- GRAPH ENGINE --- #
########################
########################
# --- GRAPH ENGINE --- #
########################
def build_standard_sf_graph(df, title, start_view, end_view, active_refs):
    """
    SF Standard with Enhanced Gridlines:
    - Monday Midnight: DimGray (2.5)
    - Daily Midnight: DarkGray (1.5)
    - 6-Hour Marks: Gainsboro (0.5)
    - 10.2 Line: Burgundy
    """
    try:
        display_df = df.copy()
        
        # 1. Force strict types to prevent math/sorting crashes
        display_df['timestamp'] = pd.to_datetime(display_df['timestamp'])
        display_df['depth'] = display_df['depth'].fillna("Unknown").astype(str)
        display_df['sensor_name'] = display_df['sensor_name'].fillna("Unknown").astype(str)
        
        start_ts = pd.to_datetime(start_view)
        end_ts = pd.to_datetime(end_view)
        
        # 2. Labeling & Gap Handling
        display_df['label'] = display_df['depth'] + " (" + display_df['sensor_name'] + ")"
        
        processed_dfs = []
        for lbl in display_df['label'].unique():
            s_df = display_df[display_df['label'] == lbl].copy().sort_values('timestamp')
            s_df['gap_hrs'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
            gap_mask = s_df['gap_hrs'] > 6.0
            if gap_mask.any():
                gaps = s_df[gap_mask].copy()
                gaps['temperature'] = None
                gaps['timestamp'] = gaps['timestamp'] - pd.Timedelta(minutes=1)
                s_df = pd.concat([s_df, gaps]).sort_values('timestamp')
            processed_dfs.append(s_df)
        clean_df = pd.concat(processed_dfs) if processed_dfs else display_df
        
        fig = go.Figure()
        
        # 3. Traces (Numerical Depth Sorting)
        def natural_sort_key(s):
            nums = re.findall(r'\d+', s)
            return int(nums[0]) if nums else 0
        
        labels = sorted(clean_df['label'].unique(), key=natural_sort_key)
        for lbl in labels:
            sensor_df = clean_df[clean_df['label'] == lbl]
            fig.add_trace(go.Scatter(x=sensor_df['timestamp'], y=sensor_df['temperature'], 
                                     name=lbl, mode='lines', connectgaps=False))

        # 4. Layout
        fig.update_layout(
            title={'text': title, 'x': 0, 'xanchor': 'left'},
            plot_bgcolor='white', hovermode="x unified", margin=dict(t=50, l=50, r=150), height=750
        )
        
        fig.update_yaxes(title="Temp (°F)", range=[-20, 80], gridcolor='DimGray', gridwidth=1.5,
                         minor=dict(dtick=5, gridcolor='Silver', showgrid=True),
                         mirror=True, showline=True, linecolor='black', linewidth=2)

        fig.update_xaxes(range=[start_ts, end_ts], mirror=True, showline=True, linecolor='black',
                         linewidth=2, showgrid=False, tickformat="%a\n%m/%d",
                         minor=dict(showgrid=False)) # Disabled minor auto-grid to stop "bunching"

        # 5. CUSTOM GRIDLINES (Clean 6-Hour Intervals)
        grid_times = pd.date_range(start=start_ts, end=end_ts, freq='6H')
        for ts in grid_times:
            if ts.hour == 0:
                color, width = ("DimGray", 2.5) if ts.weekday() == 0 else ("DarkGray", 1.5)
            else:
                color, width = "Gainsboro", 0.5
            fig.add_vline(x=ts, line_width=width, line_color=color, layer='below')

        # 6. NOW MARKER (Separate annotation to prevent Plotly crash)
        now_marker = pd.Timestamp.now(tz=pytz.UTC)
        fig.add_vline(x=now_marker, line_width=2, line_color="Red", layer='above')
        fig.add_annotation(x=now_marker, y=1, yref="paper", text="NOW", 
                           showarrow=False, font=dict(color="Red", size=12), xanchor="left")

        # 7. HORIZONTAL REFERENCES (10.2 = Burgundy)
        for val, label in active_refs:
            line_color = "#800020" if str(val) == "10.2" else "blue"
            fig.add_hline(y=val, line_dash="dash", line_color=line_color)
            fig.add_annotation(x=1, xref="paper", y=val, text=f"{label} {val}°", 
                               showarrow=False, font=dict(color=line_color), xanchor="left")
        
        return fig
    except Exception as e:
        st.error(f"Graph Error: {e}")
        return go.Figure()
############################
# --- END GRAPH ENGINE --- #
############################
###################
# --- SIDEBAR --- #
###################
st.sidebar.title("❄️ SoilFreeze Lab")

# 1. PAGE SELECTION (Now First)
service = st.sidebar.selectbox("📂 Select Page", 
    ["🏠 Executive Summary", "📊 Client Portal", "📉 Node Diagnostics", "📤 Data Intake Lab", "🛠️ Admin Tools"])

st.sidebar.divider()

# 2. CONTEXTUAL PROJECT SELECTION
# Pages that NEED a project: Client Portal, Node Diagnostics, Admin Tools (for targeted scrub)
needs_project = service in ["📊 Client Portal", "📉 Node Diagnostics", "🛠️ Admin Tools"]

if needs_project:
    try:
        # Get all projects
        proj_list_q = f"SELECT DISTINCT Project FROM `{PROJECT_ID}.Temperature.master_data` WHERE Project IS NOT NULL"
        all_projs = sorted(client.query(proj_list_q).to_dataframe()['Project'].unique())
        selected_project = st.sidebar.selectbox("🎯 Active Project", all_projs)
    except:
        selected_project = None
        st.sidebar.error("Error loading projects.")
else:
    # Greyed out / Disabled state
    st.sidebar.selectbox("🎯 Active Project", ["(Not Required for this Page)"], disabled=True)
    selected_project = None

st.sidebar.divider()

# 3. GRAPH SETTINGS
st.sidebar.write("### Graph Settings")
show_32 = st.sidebar.checkbox("Freezing (32°F)", value=True)
show_26 = st.sidebar.checkbox("Type B (26.6°F)", value=True)
show_10 = st.sidebar.checkbox("Type A (10.2°F)", value=True)

active_refs = []
if show_32: active_refs.append((32, "Freezing"))
if show_26: active_refs.append((26.6, "Type B"))
if show_10: active_refs.append((10.2, "Type A"))
#######################
# --- END SIDEBAR --- #
#######################
####################
# --- SERVICES --- #
####################
#############################
# --- EXECUTIVE SUMMARY --- #
#############################
if service == "🏠 Executive Summary":
    st.header("🏠 Site Health Summary")

    try:
        # UPDATED: Using NodeNum and Project from the new master_data table
        proj_q = f"SELECT DISTINCT Project FROM `{MASTER_TABLE}` WHERE Project IS NOT NULL"
        meta_df = client.query(proj_q).to_dataframe()
        all_projs = sorted(meta_df['Project'].unique())
        sel_summary_proj = st.selectbox("Select Project Focus", all_projs, index=0)

        query = f"""
            WITH NodeLimits AS (
                SELECT NodeNum, MAX(timestamp) as max_ts FROM `{MASTER_TABLE}`
                WHERE Project = '{sel_summary_proj}' GROUP BY NodeNum
            )
            SELECT m.timestamp, m.temperature, m.Location, m.Depth, m.NodeNum
            FROM `{MASTER_TABLE}` m
            JOIN NodeLimits nl ON m.NodeNum = nl.NodeNum
            WHERE m.timestamp >= TIMESTAMP_SUB(nl.max_ts, INTERVAL 24 HOUR)
        """
        df_summary = client.query(query).to_dataframe()
        if not df_summary.empty:
            now_ts = datetime.now(pytz.UTC)
            summary_stats = []
            for node in df_summary['NodeNum'].unique():
                n_df = df_summary[df_summary['NodeNum'] == node].sort_values('timestamp')
                curr_t = n_df['temperature'].iloc[-1]
                chg = curr_t - n_df['temperature'].iloc[0]
                last_ts = n_df['timestamp'].iloc[-1]
                if last_ts.tzinfo is None: last_ts = last_ts.replace(tzinfo=pytz.UTC)
                hrs = (now_ts - last_ts).total_seconds() / 3600
                summary_stats.append({
                    "Location": n_df['Location'].iloc[0], "Depth": f"{n_df['Depth'].iloc[0]}", "Node ID": node,
                    "Status": f"{last_ts.strftime('%m/%d %H:%M')} ({int(round(hrs, 0))}h ago)",
                    "Change": round(float(chg), 1), "Current": round(float(curr_t), 1)
                })
            st.dataframe(pd.DataFrame(summary_stats), width='stretch', hide_index=True)
    except Exception as e: st.error(f"Summary Error: {e}")
#################################
# --- END EXECUTIVE SUMMARY --- #
#################################
#########################
# --- CLIENT PORTAL --- #
#########################
elif service == "📊 Client Portal":
    st.header("📊 Project Status Report")
    try:
        # UPDATED: Using new field names 'Project', 'Location', and 'approve'
        meta_q = f"SELECT DISTINCT Project, Location FROM `{MASTER_TABLE}` WHERE approve = 'TRUE'"
        meta_df = client.query(meta_q).to_dataframe()
        
        if meta_df.empty:
            st.warning("No approved data available in the Temperature.master_data table.")
        else:
            c1, c2, c3 = st.columns([1, 1, 1])
            with c1: sel_proj = st.selectbox("Project", sorted(meta_df['Project'].dropna().unique()))
            with c2: 
                locs = sorted(meta_df[meta_df['Project'] == sel_proj]['Location'].dropna().unique())
                sel_loc = st.selectbox("Pipe / Bank", locs)
            with c3: weeks_to_view = st.slider("Weeks to View", 1, 12, 6)
            
            # Pulling data using the new schema
            data_q = f"""
                SELECT timestamp, temperature, Depth, NodeNum as sensor_name 
                FROM `{MASTER_TABLE}` 
                WHERE Project = '{sel_proj}' AND Location = '{sel_loc}' AND approve = 'TRUE' 
                ORDER BY timestamp ASC
            """
            df_c = client.query(data_q).to_dataframe()
            df_c['timestamp'] = pd.to_datetime(df_c['timestamp'])

            max_approved_ts = df_c['timestamp'].max()
            current_monday = (max_approved_ts - timedelta(days=max_approved_ts.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
            start_view = current_monday - timedelta(weeks=weeks_to_view - 1)
            end_view = current_monday + timedelta(days=7)

            st.subheader("📈 Historical Trends")
            fig_timeline = build_standard_sf_graph(df_c, f"{weeks_to_view}-Week Trend: {sel_loc}", start_view, end_view, active_refs)
            st.plotly_chart(fig_timeline, use_container_width=True)
            
    except Exception as e: 
        st.error(f"Portal Error: {e}")
#############################
# --- END CLIENT PORTAL --- #
#############################  
###########################
# --- NODE DIAGNOSTIC --- #
###########################  
###########################
# --- NODE DIAGNOSTIC --- #
###########################  
elif service == "📉 Node Diagnostics":
    st.header(f"📉 Diagnostics: {selected_project}")
    try:
        # Get locations for the ALREADY selected project
        loc_q = f"SELECT DISTINCT Location FROM `{PROJECT_ID}.Temperature.master_data` WHERE Project = '{selected_project}'"
        loc_df = client.query(loc_q).to_dataframe()
        
        c1, c2 = st.columns([2, 1])
        with c1: 
            sel_loc = st.selectbox("Pipe / Bank", sorted(loc_df['Location'].dropna().unique()))
        with c2: 
            weeks = st.slider("Lookback (Weeks)", 1, 12, 6)

        # Date Math
        now = pd.Timestamp.now(tz=pytz.UTC)
        monday_this_week = (now - pd.offsets.Day(now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        start_view = monday_this_week - pd.offsets.Week(int(weeks)-1)
        end_view = monday_this_week + pd.offsets.Day(7)

        data_q = f"""
            SELECT timestamp, temperature, Depth as depth, NodeNum as sensor_name
            FROM `{PROJECT_ID}.Temperature.master_data` 
            WHERE Project = '{selected_project}' AND Location = '{sel_loc}' 
            AND timestamp >= '{start_view.strftime('%Y-%m-%d %H:%M:%S')}' 
            ORDER BY timestamp ASC
        """
        df_g = client.query(data_q).to_dataframe()
        
        if not df_g.empty:
            st.plotly_chart(build_standard_sf_graph(df_g, f"{selected_project} | {sel_loc}", start_view, end_view, active_refs), use_container_width=True)
        else:
            st.warning("No data found.")
    except Exception as e:
        st.error(f"Diagnostics Error: {e}")
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
        start_date = c1.date_input("Start Date", datetime.now() - timedelta(days=7))
        end_date = c2.date_input("End Date", datetime.now())
        
        if st.button("🛰️ FETCH & SYNC"):
            start_dt = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=pytz.UTC)
            end_dt = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=pytz.UTC)
            
            with st.spinner("Fetching from SensorPush API..."):
                df_api = fetch_sensorpush_data(start_dt, end_dt)
                if not df_api.empty:
                    df_api['sensor_id'] = df_api['sensor_id'].astype(str).str.replace(':', '-', regex=False)
                    # UPDATED: Pushing to the new dataset location
                    client.load_table_from_dataframe(df_api, f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush").result()
                    st.success(f"✅ API Sync Complete: {len(df_api)} points integrated.")
                else:
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
###############################
# --- END DATA INTAKE LAB --- #
###############################
#######################
# --- ADMIN TOOLS --- #
#######################             
elif service == "🛠️ Admin Tools":
    st.header("🛠️ Engineering Admin Tools")
    
    # Table Definitions
    MASTER_TABLE = f"{PROJECT_ID}.Temperature.master_data"
    RAW_SP = f"{PROJECT_ID}.Temperature.raw_sensorpush"
    RAW_LORD = f"{PROJECT_ID}.Temperature.raw_lord"
    METADATA = f"{PROJECT_ID}.Temperature.master_metadata"

    tab_approve, tab_scrub = st.tabs(["✅ Approval Manager", "🧹 Raw Data Scrubber"])
    
    with tab_approve:
        st.subheader("Global & Targeted Approval")
        if st.button("🔓 APPROVE ALL HISTORIC DATA"):
            with st.spinner("Updating records..."):
                client.query(f"UPDATE `{MASTER_TABLE}` SET approve = 'TRUE' WHERE approve IS NULL OR approve != 'TRUE'").result()
                st.success("Global approval complete.")
        
        st.divider()
        # Targeted approval for the sidebar-selected project
        if selected_project:
            st.write(f"**Targeted Approval for {selected_project}**")
            un_q = f"SELECT DISTINCT Location FROM `{MASTER_TABLE}` WHERE Project = '{selected_project}' AND (approve IS NULL OR approve != 'TRUE')"
            un_df = client.query(un_q).to_dataframe()
            
            if not un_df.empty:
                # FIX: Use .dropna() to remove None values before sorting
                valid_locations = sorted(un_df['Location'].dropna().unique())
                if valid_locations:
                    sel_app_pipe = st.selectbox("Select Pipe to Approve", valid_locations)
                    if st.button(f"🚀 Approve {sel_app_pipe}"):
                        client.query(f"UPDATE `{MASTER_TABLE}` SET approve = 'TRUE' WHERE Project = '{selected_project}' AND Location = '{sel_app_pipe}'").result()
                        st.success(f"Approved {sel_app_pipe}!")
                        st.rerun()
                else:
                    st.info("No valid locations found for approval.")
            else:
                st.info("No unapproved data for this project.")
        else:
            st.warning("Please select a project in the sidebar.")

    with tab_scrub:
        st.subheader("🧹 Raw Source Scrubber")
        st.warning("This deletes data from the source (Raw) tables.")
        
        if selected_project:
            col_s1, col_s2 = st.columns(2)
            with col_s1:
                source_type = st.radio("Source", ["SensorPush", "Lord"])
                target_t = RAW_SP if source_type == "SensorPush" else RAW_LORD
                id_f = "sensor_id" if source_type == "SensorPush" else "nodenumber"
            with col_s2:
                pipe_q = f"SELECT DISTINCT Location FROM `{METADATA}` WHERE Project = '{selected_project}'"
                # FIX: Use .dropna() to remove None values before sorting
                pipes_df = client.query(pipe_q).to_dataframe()
                valid_pipes = sorted(pipes_df['Location'].dropna().unique())
                
                if valid_pipes:
                    sel_scrub_pipe = st.selectbox("Pipe to Wipe", valid_pipes)
                else:
                    st.error("No pipes found in metadata for this project.")
                    sel_scrub_pipe = None

            if sel_scrub_pipe:
                confirm = st.text_input(f"Type 'DELETE' to wipe {sel_scrub_pipe}", key="scrub_conf")
                if st.button("🔥 PERMANENTLY DELETE"):
                    if confirm == "DELETE":
                        scrub_sql = f"DELETE FROM `{target_t}` WHERE {id_f} IN (SELECT CAST(NodeNum AS STRING) FROM `{METADATA}` WHERE Project = '{selected_project}' AND Location = '{sel_scrub_pipe}')"
                        client.query(scrub_sql).result()
                        st.success("Raw data scrubbed!")
                    else:
                        st.error("Please type 'DELETE' to confirm.")
        else:
            st.warning("Please select a project in the sidebar.")
###########################
# --- END ADMIN TOOLS --- #
########################### 
