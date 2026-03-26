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

# --- 1. CONFIGURATION & STYLING ---
st.set_page_config(page_title="SoilFreeze Data Lab", layout="wide")

DATASET_ID = "sensor_data" 
PROJECT_ID = "sensorpush-export"

@st.cache_resource
def get_bq_client():
    """Handles authentication to Google BigQuery."""
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

# --- 3. GRAPH ENGINE ---

def build_standard_sf_graph(df, title, start_view, end_view, active_refs):
    """Generates the standardized SoilFreeze thermal line graph."""
    display_df = df.copy()
    y_range, y_ticks, y_label, m_step = [-20, 80], [-20, 0, 20, 40, 60, 80], "Temp (°F)", 5

    # Gap Logic for disconnected sensors
    processed_dfs = []
    for d in display_df['depth'].unique():
        s_df = display_df[display_df['depth'] == d].copy().sort_values('timestamp')
        s_df['gap'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
        gaps = s_df[s_df['gap'] > 6.0].copy()
        if not gaps.empty:
            gaps['temperature'] = None
            gaps['timestamp'] = gaps['timestamp'] - timedelta(minutes=1)
            s_df = pd.concat([s_df, gaps]).sort_values('timestamp')
        processed_dfs.append(s_df)
    clean_df = pd.concat(processed_dfs) if processed_dfs else display_df
    
    fig = go.Figure()
    depths = sorted(clean_df['depth'].unique(), key=lambda x: int(''.join(filter(str.isdigit, x)) or 0))
    for d in depths:
        sensor_df = clean_df[clean_df['depth'] == d]
        fig.add_trace(go.Scatter(x=sensor_df['timestamp'], y=sensor_df['temperature'], name=d, mode='lines', connectgaps=False))

    fig.update_layout(
        title={'text': title, 'x': 0, 'xanchor': 'left'},
        plot_bgcolor='white', hovermode="x unified", margin=dict(t=50, l=50, r=150), height=750
    )
    fig.update_yaxes(title=y_label, tickmode='array', tickvals=y_ticks, range=y_range, gridcolor='DimGray')
    fig.update_xaxes(range=[start_view, end_view], mirror=True, showline=True, linecolor='black')

    for val, label in active_refs:
        fig.add_hline(y=val, line_dash="dash", line_color="blue", annotation_text=f"{label} {val}°")
    
    return fig

# --- 4. SIDEBAR ---
st.sidebar.title("❄️ SoilFreeze Lab")
show_32 = st.sidebar.checkbox("Freezing (32°F)", value=True)
show_26 = st.sidebar.checkbox("Type B (26.6°F)", value=True)
show_10 = st.sidebar.checkbox("Type A (10.2°F)", value=True)

active_refs = []
if show_32: active_refs.append((32, "Freezing"))
if show_26: active_refs.append((26.6, "Type B"))
if show_10: active_refs.append((10.2, "Type A"))

service = st.sidebar.selectbox("Select Service", ["🏠 Executive Summary", "📊 Client Portal", "📉 Node Diagnostics", "📤 Data Intake Lab", "🛠️ Admin Tools"])

# --- 5. SERVICE ROUTING ---

if service == "🏠 Executive Summary":
    st.header("🏠 Site Health Summary")

    try:
        proj_q = f"SELECT DISTINCT project FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE project IS NOT NULL"
        meta_df = client.query(proj_q).to_dataframe()
        all_projs = sorted(meta_df['project'].unique())
        sel_summary_proj = st.selectbox("Select Project Focus", all_projs, index=0)

        query = f"""
            WITH NodeLimits AS (
                SELECT sensor_id, MAX(timestamp) as max_ts FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master`
                WHERE project = '{sel_summary_proj}' GROUP BY sensor_id
            )
            SELECT m.timestamp, m.temperature, m.location, m.depth, m.sensor_id, m.sensor_name
            FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` m
            JOIN NodeLimits nl ON m.sensor_id = nl.sensor_id
            WHERE m.timestamp >= TIMESTAMP_SUB(nl.max_ts, INTERVAL 24 HOUR)
        """
        df_summary = client.query(query).to_dataframe()
        if not df_summary.empty:
            now_ts = datetime.now(pytz.UTC)
            summary_stats = []
            for node in df_summary['sensor_id'].unique():
                n_df = df_summary[df_summary['sensor_id'] == node].sort_values('timestamp')
                curr_t = n_df['temperature'].iloc[-1]
                chg = curr_t - n_df['temperature'].iloc[0]
                last_ts = n_df['timestamp'].iloc[-1]
                if last_ts.tzinfo is None: last_ts = last_ts.replace(tzinfo=pytz.UTC)
                hrs = (now_ts - last_ts).total_seconds() / 3600
                summary_stats.append({
                    "Location": n_df['location'].iloc[0], "Depth": f"{n_df['depth'].iloc[0]}ft", "Node ID": node,
                    "Status": f"{last_ts.strftime('%m/%d %H:%M')} ({int(round(hrs, 0))}h ago)",
                    "Change": round(float(chg), 1), "Current": round(float(curr_t), 1)
                })
            st.dataframe(pd.DataFrame(summary_stats), width='stretch', hide_index=True)
    except Exception as e: st.error(f"Summary Error: {e}")

# 4B. CLIENT PORTAL
elif service == "📊 Client Portal":
    st.header("📊 Project Status Report")
    try:
        meta_q = f"SELECT DISTINCT project, location FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE is_approved = TRUE"
        meta_df = client.query(meta_q).to_dataframe()
        
        if meta_df.empty:
            st.warning("No approved data available.")
        else:
            c1, c2, c3 = st.columns([1, 1, 1])
            with c1: sel_proj = st.selectbox("Project", sorted(meta_df['project'].dropna().unique()))
            with c2: 
                locs = sorted(meta_df[meta_df['project'] == sel_proj]['location'].dropna().unique())
                sel_loc = st.selectbox("Pipe / Bank", locs)
            with c3: weeks_to_view = st.slider("Weeks to View", 1, 12, 6)
            
            data_q = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE project = '{sel_proj}' AND location = '{sel_loc}' AND is_approved = TRUE ORDER BY timestamp ASC"
            df_c = client.query(data_q).to_dataframe()
            df_c['timestamp'] = pd.to_datetime(df_c['timestamp'])

            max_approved_ts = df_c['timestamp'].max()
            current_monday = (max_approved_ts - timedelta(days=max_approved_ts.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
            start_view = current_monday - timedelta(weeks=weeks_to_view - 1)
            end_view = current_monday + timedelta(days=7)

            if "bank" not in sel_loc.lower():
                st.subheader("🌡️ Soil Temperature Profile (Weekly Snapshots)")
                snapshot = df_c[(df_c['timestamp'].dt.weekday == 0) & (df_c['timestamp'].dt.hour == 6)].copy()
                snapshot = snapshot[snapshot['timestamp'] >= start_view]
                if not snapshot.empty:
                    snapshot['depth_num'] = snapshot['depth'].str.extract('(\d+)').astype(float)
                    snapshot['Date'] = snapshot['timestamp'].dt.strftime('%m/%d')
                    fig_profile = px.line(snapshot.sort_values('depth_num'), x='temperature', y='depth_num', color='Date', markers=True, range_x=[-20, 80])
                    for val, label in active_refs:
                        fig_profile.add_vline(x=val, line_dash="dash", line_color="blue", annotation_text=label)
                    fig_profile.update_layout(title={'text': "Temperature by Depth", 'x': 0, 'xanchor': 'left'}, plot_bgcolor='white', height=600)
                    fig_profile.update_yaxes(autorange="reversed")
                    st.plotly_chart(fig_profile, width='stretch')

            st.subheader("📈 Historical Trends")
            fig_timeline = build_standard_sf_graph(df_c, f"{weeks_to_view}-Week Trend: {sel_loc}", start_view, end_view, active_refs)
            st.plotly_chart(fig_timeline, width='stretch')
            
            st.subheader(f"⏱️ Performance Window: {max_approved_ts.strftime('%m/%d %H:%M')}")
            last_approved_24h = df_c[df_c['timestamp'] >= (max_approved_ts - timedelta(hours=24))].copy()
            if not last_approved_24h.empty:
                last_approved_24h['depth_num'] = last_approved_24h['depth'].str.extract('(\d+)').astype(float)
                stats = last_approved_24h.groupby(['depth', 'depth_num']).agg(
                    High=('temperature', 'max'), Low=('temperature', 'min'), Current=('temperature', 'last'), Last_Update=('timestamp', 'last')
                ).reset_index()
                stats['Difference'] = stats['High'] - stats['Low']
                st.dataframe(stats[['depth', 'Current', 'High', 'Low', 'Difference', 'Last_Update']], width='stretch', hide_index=True)

    except Exception as e: st.error(f"Portal Error: {e}")
        
# 4C. NODE DIAGNOSTICS


elif service == "📉 Node Diagnostics":
    st.header("📉 High-Resolution Node Diagnostics")
    try:
        meta_q = f"SELECT DISTINCT project, location FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE project IS NOT NULL"
        meta_df = client.query(meta_q).to_dataframe()
        c1, c2, c3 = st.columns(3)
        with c1: sel_proj = st.selectbox("Project", sorted(meta_df['project'].unique()))
        with c2: 
            locs = sorted(meta_df[meta_df['project'] == sel_proj]['location'].unique())
            sel_loc = st.selectbox("Pipe / Bank", locs)
        with c3: weeks = st.slider("Lookback (Weeks)", 1, 12, 4)

        days_back = weeks * 7
        data_q = f"SELECT timestamp, temperature, depth FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE project = '{sel_proj}' AND location = '{sel_loc}' AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY) ORDER BY timestamp ASC"
        df_g = client.query(data_q).to_dataframe()
        if not df_g.empty:
            df_g['timestamp'] = pd.to_datetime(df_g['timestamp'])
            st.plotly_chart(build_standard_sf_graph(df_g, f"Trend: {sel_proj} | {sel_loc}", datetime.now(pytz.UTC)-timedelta(days=days_back), datetime.now(pytz.UTC), active_refs))
    except Exception as e: st.error(f"Diagnostics Error: {e}")

# 4D. DATA INTAKE LAB (HARDENED SYNC)
# --- 4D. DATA INTAKE LAB (FIXED INDENTATION & SCHEMA) ---
elif service == "📤 Data Intake Lab":
    st.header("📤 Data Ingestion & Recovery")
    tab1, tab2 = st.tabs(["📄 Manual File Upload", "📡 API Data Recovery"])

    with tab1:
        st.subheader("Manual CSV Ingestion")
        source = st.radio("Device Type", ["SensorPush (CSV)", "Lord (SensorConnect)", "Logger (Master Log)"], horizontal=True)
        u_file = st.file_uploader("Upload Logger File", type=['csv'], key="manual_upload")
        
        if u_file is not None:
            try:
                df_raw = pd.read_csv(u_file, low_memory=False)
                ts_col = next((c for c in df_raw.columns if c.lower() == 'timestamp'), df_raw.columns[0])
                df_raw['timestamp'] = pd.to_datetime(df_raw[ts_col], format='mixed', errors='coerce')
                df_raw = df_raw.dropna(subset=['timestamp'])

                df_up = pd.DataFrame()
                table_ref = f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush"

                if "Master Log" in source:
                    if df_raw.shape[1] > 2 and any(isinstance(x, str) for x in df_raw.iloc[:, 1]):
                        df_up = df_raw.iloc[:, [df_raw.columns.get_loc('timestamp'), 0, 1]].copy()
                        df_up.columns = ['timestamp', 'temperature', 'sensor_id']
                    else:
                        df_up = df_raw.melt(id_vars=['timestamp'], var_name='sensor_id', value_name='temperature')
                elif "Lord" in source:
                    df_up = df_raw.melt(id_vars=['timestamp'], var_name='sensor_id', value_name='temperature')
                    table_ref = f"{PROJECT_ID}.{DATASET_ID}.raw_lord"
                else: 
                    df_up = df_raw.rename(columns={'Temperature': 'temperature', 'Sensor': 'sensor_id'})

                if not df_up.empty:
                    df_up['sensor_id'] = df_up['sensor_id'].astype(str).str.replace(':', '-')
                    st.write(f"Previewing {len(df_up)} points:")
                    st.dataframe(df_up.head())

                    if st.button("🚀 PUSH & CLEANSE"):
                        with st.spinner("Uploading and Deduplicating..."):
                            client.load_table_from_dataframe(df_up, table_ref).result()
                            
                            scrub_sql = f"""
                                CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` AS 
                                WITH RawUnified AS (
                                    SELECT CAST(timestamp AS TIMESTAMP) as ts, value as temp, REPLACE(nodenumber, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord` WHERE value IS NOT NULL
                                    UNION ALL 
                                    SELECT CAST(timestamp AS TIMESTAMP) as ts, temperature as temp, REPLACE(sensor_id, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` WHERE temperature IS NOT NULL
                                ),
                                HourlyDedupped AS (
                                    SELECT *, ROW_NUMBER() OVER(PARTITION BY node, TIMESTAMP_TRUNC(ts, HOUR) ORDER BY ts DESC) as rank FROM RawUnified
                                )
                                SELECT h.ts as timestamp, h.temp as temperature, m.NodeNum as sensor_id, m.SensorName as sensor_name, m.Project as project, m.Location as location, m.Depth as depth, FALSE as is_approved
                                FROM HourlyDedupped h 
                                INNER JOIN `{PROJECT_ID}.{DATASET_ID}.master_metadata` m ON (h.node = REPLACE(m.NodeNum, ':', '-') OR h.node = m.SensorName)
                                WHERE h.rank = 1
                            """
                            client.query(scrub_sql).result()
                            st.success("✅ Database synced with hourly deduplication!")
            except Exception as e: 
                st.error(f"File Error: {e}")

    with tab2:
        st.subheader("📡 Cloud-to-Cloud Range Recovery")
        c1, c2 = st.columns(2)
        start_date = c1.date_input("Start Date", datetime.now() - timedelta(days=2))
        end_date = c2.date_input("End Date", datetime.now())
        
        if st.button("🛰️ BATCH FETCH & SYNC"):
            status_box = st.empty()
            start_dt = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=pytz.UTC)
            end_dt = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=pytz.UTC)
            
            status_box.info("Step 1/3: Fetching data (HT.w & TC.x) in 12hr chunks...")
            df_api = fetch_sensorpush_data(start_dt, end_dt) # Now uses the chunked fetcher
            
            if not df_api.empty:
                status_box.info(f"Step 2/3: Uploading {len(df_api)} points to BigQuery...")
                try:
                    table_ref = f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush"
                    job = client.load_table_from_dataframe(df_api, table_ref)
                    job.result() 
                    
                    status_box.info("Step 3/3: Running Master Deduplication & ID Mapping...")
                # THE UPDATED SYNC SQL: Uses NodeNum as the primary mapping key
                scrub_sql = f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` AS 
                    WITH RawUnified AS (
                        -- Combine Lord and SensorPush raw data, stripping colons for consistency
                        SELECT CAST(timestamp AS TIMESTAMP) as ts, value as temp, REPLACE(nodenumber, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord` WHERE value IS NOT NULL
                        UNION ALL 
                        SELECT CAST(timestamp AS TIMESTAMP) as ts, temperature as temp, REPLACE(sensor_id, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` WHERE temperature IS NOT NULL
                    ),
                    HourlyDedupped AS (
                        -- Window function to keep only the most recent record per hour per sensor
                        SELECT *, ROW_NUMBER() OVER(PARTITION BY node, TIMESTAMP_TRUNC(ts, HOUR) ORDER BY ts DESC) as rank FROM RawUnified
                    )
                    SELECT 
                        h.ts as timestamp, 
                        h.temp as temperature, 
                        m.NodeNum as sensor_id,   -- Restores the official ID from metadata
                        COALESCE(m.SensorName, m.NodeNum) as sensor_name, -- Falls back to NodeNum if Name is missing
                        m.Project as project, 
                        m.Location as location, 
                        m.Depth as depth,
                        FALSE as is_approved
                    FROM HourlyDedupped h 
                    INNER JOIN `{PROJECT_ID}.{DATASET_ID}.master_metadata` m 
                        -- Joins by comparing the raw 'node' string to the metadata 'NodeNum'
                        ON h.node = REPLACE(m.NodeNum, ':', '-')
                    WHERE h.rank = 1
                """
                client.query(scrub_sql).result()
                status_box.success("✅ Database Fully Synced! Data is now mapped to NodeNum and deduplicated.")
                st.balloons()
                except Exception as bq_e:
                    st.error(f"BigQuery Sync Failed: {bq_e}")
            else:
                status_box.warning("No data found for this range.")

# --- 4E. ADMIN TOOLS (CLEAN INDENTATION) ---
elif service == "🛠️ Admin Tools":
    st.header("🛠️ Engineering Admin Tools")
    tab_scrub, tab_approve = st.tabs(["🧹 Data Scrubber", "✅ Bulk Approval"])
    
    with tab_scrub:
        sc_proj = st.text_input("Project Name", key="scrub_p")
        sc_loc = st.text_input("Location / Pipe", key="scrub_l")
        if st.button("🗑️ DELETE POINTS"):
            if sc_proj and sc_loc:
                scrub_q = f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE project='{sc_proj}' AND location='{sc_loc}'"
                client.query(scrub_q).result()
                st.success(f"Deleted {sc_loc} data.")

    with tab_approve:
        try:
            unapproved_meta_q = f"SELECT DISTINCT project, location FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE (is_approved IS FALSE OR is_approved IS NULL)"
            un_meta = client.query(unapproved_meta_q).to_dataframe()
            if not un_meta.empty:
                app_proj = st.selectbox("Project", un_meta['project'].unique(), key="app_p")
                app_loc = st.selectbox("Location", un_meta[un_meta['project'] == app_proj]['location'].unique(), key="app_l")
                if st.button("🚀 APPROVE NOW"):
                    client.query(f"UPDATE `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` SET is_approved = TRUE WHERE project='{app_proj}' AND location='{app_loc}'").result()
                    st.success("Approved!")
        except Exception as e: 
            st.error(f"Approval Error: {e}")
