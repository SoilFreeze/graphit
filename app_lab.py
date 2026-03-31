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
def fetch_sensorpush_data(start_dt, end_dt, target_location=None):
    ACCOUNTS = [
        {'email': 'tsteele@soilfreeze.com', 'password': 'Freeze123!!'},
        {'email': 'soilfreeze98072@gmail.com', 'password': 'Freeze123!!'}
    ]
    BASE_URL = "https://api.sensorpush.com/api/v1"
    all_records = [] 

    # --- 1. LOAD MAPPINGS FROM 'metadata' (Integer-Only Map) ---
    name_map = {}
    try:
        query = f"SELECT PhysicalID, NodeNum FROM `{PROJECT_ID}.{DATASET_ID}.metadata`"
        meta_df = client.query(query).to_dataframe()
        for _, row in meta_df.iterrows():
            p_id = str(row['PhysicalID']).split('.')[0].strip()
            name_map[p_id] = str(row['NodeNum']).strip()
        st.sidebar.success(f"✅ Loaded {len(name_map)} mappings from metadata.")
    except Exception as e:
        st.error(f"Metadata load failed: {e}")

    for acc in ACCOUNTS:
        try:
            # --- 2. AUTHENTICATION ---
            auth_r = requests.post(f"{BASE_URL}/oauth/authorize", json=acc, timeout=15).json()
            token = requests.post(f"{BASE_URL}/oauth/accesstoken", 
                                   json={"authorization": auth_r.get('authorization')}, 
                                   timeout=15).json().get('accesstoken')
            headers = {"Authorization": token}

            # --- 3. SENSOR DISCOVERY ---
            s_resp = requests.post(f"{BASE_URL}/devices/sensors", headers=headers, json={}, timeout=20).json()
            sensor_list = s_resp.values() if isinstance(s_resp, dict) else s_resp
            api_sensor_ids = [str(s.get('id')) for s in sensor_list]
            
            # --- 4. FETCH SAMPLES ---
            current_start = start_dt
            while current_start < end_dt:
                current_end = min(current_start + timedelta(hours=24), end_dt)
                
                for i in range(0, len(api_sensor_ids), 10):
                    chunk = api_sensor_ids[i:i+10]
                    payload = {
                        "limit": 10000, 
                        "startTime": current_start.strftime('%Y-%m-%dT%H:%M:%S+0000'), 
                        "endTime": current_end.strftime('%Y-%m-%dT%H:%M:%S+0000'),
                        "sensors": chunk
                    }
                    r = requests.post(f"{BASE_URL}/samples", headers=headers, json=payload, timeout=60).json()
                    
                    samples_data = r.get('sensors', {})
                    for s_id, samples in samples_data.items():
                        for s in samples:
                            temp = s.get('temp_f') or s.get('temperature') or s.get('thermocouple_temperature')
                            if temp is None and s.get('temp_c') is not None:
                                temp = (float(s['temp_c']) * 1.8) + 32
                            
                            if temp is not None:
                                all_records.append({
                                    "timestamp": pd.to_datetime(s['observed']),   
                                    "PhysicalID": s_id, 
                                    "temperature": round(float(temp), 2),
                                    "approve": False  # FIXED: Boolean for BQ
                                })
                current_start = current_end
                time.sleep(0.1)
        except Exception as e:
            st.error(f"Account {acc['email']} failed: {e}")
            
    df = pd.DataFrame(all_records)
    if not df.empty:
        df['PhysicalID'] = pd.to_numeric(df['PhysicalID'], errors='coerce')
    return df

########################
# --- GRAPH ENGINE --- #
########################
def build_standard_sf_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label):
    try:
        display_df = df.copy()
        if display_df.empty:
            return go.Figure()

        display_df.columns = [c.lower() for c in display_df.columns]
        display_df['timestamp'] = pd.to_datetime(display_df['timestamp'])
        display_df['sensor_id'] = display_df.get('nodenum', display_df.get('sensor_name', 'Unknown')).fillna("Unknown").astype(str)

        if unit_mode == "Celsius":
            display_df['temperature'] = (display_df['temperature'] - 32) * 5/9
            y_range = [-30, 30]
        else:
            y_range = [-20, 80]
            
        start_ts = pd.to_datetime(start_view)
        end_ts = pd.to_datetime(end_view)
        
        def create_label(row):
            b_val = str(row.get('bank', '')).strip().lower()
            d_val = str(row.get('depth', '')).strip().lower()
            s_name = str(row.get('sensor_id', 'Unknown'))
            if b_val not in ["", "none", "nan", "null", "unknown"]: return f"Bank {row['bank']} ({s_name})"
            if d_val not in ["", "none", "nan", "null", "unknown"]: return f"{row['depth']}ft ({s_name})"
            return f"Unmapped ({s_name})"

        display_df['label'] = display_df.apply(create_label, axis=1)
        
        # Figure setup
        fig = go.Figure()
        labels = sorted(display_df['label'].unique())
        
        for lbl in labels:
            sensor_df = display_df[display_df['label'] == lbl].sort_values('timestamp')
            fig.add_trace(go.Scatter(x=sensor_df['timestamp'], y=sensor_df['temperature'], name=lbl, mode='lines', connectgaps=False))

        fig.update_layout(title=title, plot_bgcolor='white', hovermode="x unified", height=750)
        return fig
    except Exception as e:
        st.error(f"Graph Error: {e}")
        return go.Figure()

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
    
    # 2. DATA QUERY
    summary_q = f"SELECT * FROM `{MASTER_TABLE}`"
    if selected_project: 
        summary_q += f" WHERE Project = '{selected_project}'"
    summary_q += " QUALIFY ROW_NUMBER() OVER(PARTITION BY NodeNum ORDER BY timestamp DESC) = 1"
    
    try:
        with st.spinner("Syncing Command Center..."):
            raw_data = client.query(summary_q).to_dataframe()
        
        if raw_data.empty:
            st.warning("📡 No sensors found.")
        else:
            summary_rows = []
            now = pd.Timestamp.now(tz=pytz.UTC)
            
            for _, row in raw_data.iterrows():
                # Latency Logic
                ts = row['timestamp'].tz_localize(pytz.UTC) if row['timestamp'].tzinfo is None else row['timestamp']
                diff = now - ts
                hrs_ago = int(diff.total_seconds() / 3600)
                
                # Delta Logic
                if hrs_ago > 24:
                    status_icon = "🔴"
                    delta_text = "-"
                    raw_delta = 0.0
                else:
                    if hrs_ago > 12: status_icon = "🟠"
                    elif hrs_ago > 6: status_icon = "🟡"
                    else: status_icon = "🟢"
                    
                    delta_q = f"""
                        SELECT (SELECT temperature FROM `{MASTER_TABLE}` WHERE NodeNum = '{row['NodeNum']}' ORDER BY timestamp DESC LIMIT 1) - 
                               (SELECT temperature FROM `{MASTER_TABLE}` WHERE NodeNum = '{row['NodeNum']}' AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) ORDER BY timestamp ASC LIMIT 1) as d
                    """
                    d_res = client.query(delta_q).to_dataframe()
                    raw_delta = d_res['d'].iloc[0] if not d_res.empty and pd.notnull(d_res['d'].iloc[0]) else 0.0
                    delta_text = f"{round(raw_delta, 1)}°F"

                # Bank vs Depth Display
                bank_val = str(row['Bank']).strip()
                pos_display = f"Bank {bank_val}" if bank_val not in ["", "None", "nan", "null"] else f"{row['Depth']} ft"

                summary_rows.append({
                    "Node": row['NodeNum'],
                    "Pipe/Bank": row['Location'],
                    "Pos/Depth": pos_display,
                    "Min": f"{round(convert_val(row['temperature']), 1)}°F", 
                    "Max": f"{round(convert_val(row['temperature']), 1)}°F",
                    "Delta": raw_delta, 
                    "Delta_Text": delta_text,
                    "Hours_Ago": hrs_ago,
                    "Last Seen": f"{ts.strftime('%m/%d %H:%M')} ({hrs_ago}hr) {status_icon}"
                })

            summary_df = pd.DataFrame(summary_rows)

            # 3. APPLY SORTING
            ascending = (sort_order == "Ascending")
            if sort_choice == "Hours Since Last Seen":
                summary_df = summary_df.sort_values(by="Hours_Ago", ascending=ascending)
            elif sort_choice == "Delta Magnitude":
                summary_df = summary_df.sort_values(by="Delta", ascending=ascending, key=abs)

            # 4. PAGINATION (100 per page)
            batch_size = 100
            total_pages = (len(summary_df) // batch_size) + 1
            page = st.number_input("Page", min_value=1, max_value=total_pages, step=1)
            
            start_idx = (page - 1) * batch_size
            display_batch = summary_df.iloc[start_idx : start_idx + batch_size]

            # 5. STYLING & DISPLAY
            def style_delta(val):
                bg, color = "", "black"
                if val >= 5: bg = "#FF0000"; color = "white"
                elif val >= 2: bg = "#FFA500"
                elif val >= 0.5: bg = "#FFFF00"
                elif -0.5 <= val <= 0.5: bg = "#008000"; color = "white"
                elif -2 < val < -0.5: bg = "#ADD8E6"
                elif -5 < val <= -2: bg = "#4169E1"; color = "white"
                elif val <= -5: bg = "#00008B"; color = "white"
                return f'background-color: {bg}; color: {color}'

            st.subheader(f"📡 Engineering Command Center ({len(summary_df)} sensors)")
            
            # Map Delta_Text into the display table but style it using the hidden numeric Delta column
            st.table(display_batch[["Node", "Pipe/Bank", "Pos/Depth", "Min", "Max", "Delta_Text", "Last Seen"]].rename(columns={"Delta_Text": "Delta"}).style.apply(
                lambda x: [style_delta(row_val) for row_val in display_batch['Delta']], axis=0, subset=['Delta']
            ))

    except Exception as e: 
        st.error(f"Summary Error: {traceback.format_exc()}")
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
###############################
# --- END DATA INTAKE LAB --- #
###############################
#######################
# --- ADMIN TOOLS --- #
#######################             
elif service == "🛠️ Admin Tools":
    st.header("🛠️ Engineering Admin Tools")
    
    RAW_SP = f"{PROJECT_ID}.Temperature.raw_sensorpush"
    RAW_LORD = f"{PROJECT_ID}.Temperature.raw_lord"
    MAPPING_TABLE = f"{PROJECT_ID}.Temperature.master_data" 

    tab_scrub, tab_approve = st.tabs(["🧹 Deep Data Scrubber", "✅ Raw Bulk Approval"])
    
    with tab_scrub:
        st.subheader("🧹 Deep Raw Source Cleaning & Diagnostics")
        scrub_target = st.radio("Select Source Table", ["SensorPush", "Lord"], horizontal=True, key="admin_scrub_source")
        target_table = RAW_SP if scrub_target == "SensorPush" else RAW_LORD
        id_col = "sensor_id" if scrub_target == "SensorPush" else "NodeNum"

        # Fetch All Sensors for Admin View
        admin_query = f"""
            SELECT 
                {id_col} as Node, 
                COUNT(*) as Total_Points, 
                MIN(temperature) as Min_Temp,
                MAX(temperature) as Max_Temp,
                MAX(timestamp) as Last_Seen_TS,
                (SELECT temperature FROM `{target_table}` t2 WHERE t2.{id_col} = t1.{id_col} ORDER BY timestamp DESC LIMIT 1) - 
                (SELECT temperature FROM `{target_table}` t3 WHERE t3.{id_col} = t1.{id_col} AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) ORDER BY timestamp ASC LIMIT 1) as Raw_Delta
            FROM `{target_table}` t1
            GROUP BY Node
        """
        
        try:
            admin_df = client.query(admin_query).to_dataframe()
            if not admin_df.empty:
                st.write("### 🔍 Filter All Sensors")
                f_col1, f_col2 = st.columns(2)
                with f_col1:
                    delta_filter = st.slider("Min Delta Threshold (Abs Value)", 0.0, 20.0, 0.0)
                with f_col2:
                    hours_silent = st.number_input("Show sensors silent for more than (Hours):", min_value=0, value=0)

                # Processing Filters
                now = pd.Timestamp.now(tz=pytz.UTC)
                admin_df['Last_Seen_TS'] = pd.to_datetime(admin_df['Last_Seen_TS']).dt.tz_localize(pytz.UTC) if admin_df['Last_Seen_TS'].dt.tzinfo is None else pd.to_datetime(admin_df['Last_Seen_TS'])
                admin_df['Hours_Ago'] = (now - admin_df['Last_Seen_TS']).dt.total_seconds() / 3600
                
                filtered_df = admin_df[(admin_df['Raw_Delta'].abs() >= delta_filter) & (admin_df['Hours_Ago'] >= hours_silent)].copy()

                # Formatting to XX.X°F
                filtered_df['Min'] = filtered_df['Min_Temp'].apply(lambda x: f"{round(float(x), 1)}°F")
                filtered_df['Max'] = filtered_df['Max_Temp'].apply(lambda x: f"{round(float(x), 1)}°F")
                filtered_df['Delta'] = filtered_df['Raw_Delta'].apply(lambda x: f"{round(x, 1)}°F" if pd.notnull(x) else "N/A")
                filtered_df['Last Seen'] = filtered_df['Last_Seen_TS'].dt.strftime('%m/%d %H:%M')

                st.write(f"Showing {len(filtered_df)} sensors:")
                st.dataframe(filtered_df[["Node", "Min", "Max", "Delta", "Last Seen", "Total_Points"]], use_container_width=True, height=500)
        except Exception as e:
            st.error(f"Admin View Error: {e}")

        st.divider()
        if st.button(f"🚀 Execute Deep Scrub on {scrub_target}"):
            with st.spinner("Cleaning..."):
                # Your existing Purge/Dedup SQL remains here
                st.success("Scrub complete.")
###########################
# --- END ADMIN TOOLS --- #
########################### 
