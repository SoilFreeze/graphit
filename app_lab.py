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

def fetch_sensorpush_data(start_dt, end_dt):
    """Fetches data from SensorPush API between two specific datetimes."""
    ACCOUNTS = [
        {'email': 'tsteele@soilfreeze.com', 'password': 'Freeze123!!'},
        {'email': 'soilfreeze98072@gmail.com', 'password': 'Freeze123!!'}
    ]
    BASE_URL = "https://api.sensorpush.com/api/v1"
    all_records = []
    
    # Create hourly slots between start and end
    delta = end_dt - start_dt
    hours_to_fetch = int(delta.total_seconds() / 3600)
    target_times = [start_dt + timedelta(hours=i) for i in range(hours_to_fetch + 1)]

    for acc in ACCOUNTS:
        # Step 1: Auth
        auth_resp = requests.post(f"{BASE_URL}/oauth/authorize", json=acc)
        if auth_resp.status_code != 200: continue
        token_resp = requests.post(f"{BASE_URL}/oauth/accesstoken", 
                                   json={"authorization": auth_resp.json().get('authorization')})
        token = token_resp.json().get('accesstoken')
        headers = {"Authorization": token}

        # Step 2: Get Sensors and Names
        dev_resp = requests.post(f"{BASE_URL}/devices/sensors", headers=headers, json={})
        sensor_name_map = {}
        valid_ids = []
        if dev_resp.status_code == 200:
            sensors_data = dev_resp.json()
            sensor_list = sensors_data.values() if isinstance(sensors_data, dict) else sensors_data
            for s in sensor_list:
                s_id = str(s.get('id'))
                sensor_name_map[s_id] = s.get('name', s_id) # Store the Name
                if str(s.get('type')) == 'HT.w':
                    valid_ids.append(s_id)

        if not valid_ids: continue

        # Step 3: Fetch Samples
        for target in target_times:
            api_time = target.strftime('%Y-%m-%dT%H:%M:%S+0000')
            payload = {"limit": 50, "startTime": api_time, "sensors": valid_ids}
            r = requests.post(f"{BASE_URL}/samples", headers=headers, json=payload)
            
            if r.status_code == 200:
                samples_dict = r.json().get('sensors', {})
                for s_id, samples in samples_dict.items():
                    if samples:
                        s = samples[0]
                        temp = s.get('temp_f') or s.get('temperature') or ((s.get('temp_c', 0) * 1.8) + 32)
                        all_records.append({
                            'timestamp': target,
                            'sensor_id': s_id.replace(':', '-'),
                            'sensor_name': sensor_name_map.get(s_id, s_id), # Added Sensor Name
                            'temperature': round(float(temp), 2)
                        })
    return pd.DataFrame(all_records)

# --- 2. GRAPH ENGINE ---
# --- 2. STANDARDIZED GRAPH ENGINE (RESTORED) ---
# --- 2. STANDARDIZED GRAPH ENGINE ---
def build_standard_sf_graph(df, title, start_view, end_view, active_refs):
    display_df = df.copy()
    y_range, y_ticks, y_label, m_step = [-20, 80], [-20, 0, 20, 40, 60, 80], "Temp (°F)", 5

    # 1. Gap Logic
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
    
    # 2. Trace Creation
    fig = go.Figure()
    depths = sorted(clean_df['depth'].unique(), key=lambda x: int(''.join(filter(str.isdigit, x)) or 0))
    for d in depths:
        sensor_df = clean_df[clean_df['depth'] == d]
        fig.add_trace(go.Scatter(x=sensor_df['timestamp'], y=sensor_df['temperature'], name=d, mode='lines', connectgaps=False))

    # 3. Framing & Left-Aligning Title
    fig.update_layout(
        title={'text': title, 'x': 0, 'xanchor': 'left'}, # Left Aligned
        plot_bgcolor='white', hovermode="x unified", margin=dict(t=50, l=50, r=150), height=750
    )
    
    fig.update_yaxes(title=y_label, tickmode='array', tickvals=y_ticks, range=y_range,
                     gridcolor='DimGray', gridwidth=1.5, minor=dict(dtick=m_step, gridcolor='Silver', showgrid=True),
                     mirror=True, showline=True, linecolor='black', linewidth=2)
    fig.update_xaxes(showgrid=False, range=[start_view, end_view], mirror=True, showline=True, linecolor='black', linewidth=2)

    # 4. Add Reference Lines & "Right Now"
    for val, label in active_refs:
        fig.add_hline(y=val, line_dash="dash", line_color="blue", annotation_text=f"{label} {val}°")
    
    now_ms = datetime.now(pytz.UTC).timestamp() * 1000
    fig.add_vline(x=now_ms, line_width=2, line_color="red", annotation_text="RIGHT NOW")

    return fig # Crucial: Ensure the figure is returned

# --- 3. SIDEBAR NAVIGATION ---
# --- 3. SIDEBAR NAVIGATION ---
# --- 3. SIDEBAR NAVIGATION ---
# --- 3. SIDEBAR NAVIGATION ---
# --- 3. SIDEBAR NAVIGATION ---
st.sidebar.title("❄️ SoilFreeze Lab")

st.sidebar.subheader("Graph Reference Lines")
# Individual Checkboxes for thermal limits
show_32 = st.sidebar.checkbox("Freezing (32°F)", value=True)
show_26 = st.sidebar.checkbox("Type B (26.6°F)", value=True)
show_10 = st.sidebar.checkbox("Type A (10.2°F)", value=True)

# Build the active_refs list based on checkbox states
active_refs = []
if show_32: active_refs.append((32, "Freezing"))
if show_26: active_refs.append((26.6, "Type B"))
if show_10: active_refs.append((10.2, "Type A"))

service = st.sidebar.selectbox("Select Service", [
    "🏠 Executive Summary", "📊 Client Portal", "📉 Node Diagnostics", "📤 Data Intake Lab", "🛠️ Admin Tools"
])

# --- 4. SERVICE ROUTING ---

# 4A. EXECUTIVE SUMMARY
if service == "🏠 Executive Summary":
    st.header("🏠 Site Health & Warming Alerts")
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
# --- 4B. CLIENT PORTAL ---

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

            # Date Math based on latest approved point
            max_approved_ts = df_c['timestamp'].max()
            current_monday = (max_approved_ts - timedelta(days=max_approved_ts.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
            start_view = current_monday - timedelta(weeks=weeks_to_view - 1)
            end_view = current_monday + timedelta(days=7)

            # 1. DEPTH PROFILE
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
                    fig_profile.update_xaxes(mirror=True, showline=True, linecolor='black', linewidth=2, gridcolor='DimGray', minor=dict(dtick=5, gridcolor='Silver', showgrid=True))
                    fig_profile.update_yaxes(autorange="reversed", mirror=True, showline=True, linecolor='black', linewidth=2, gridcolor='LightGray')
                    st.plotly_chart(fig_profile, width='stretch')

            # 2. TIMELINE
            st.subheader("📈 Historical Trends")
            fig_timeline = build_standard_sf_graph(df_c, f"{weeks_to_view}-Week Trend: {sel_loc}", start_view, end_view, active_refs)
            st.plotly_chart(fig_timeline, width='stretch')
            
            # 3. PERFORMANCE TABLE (Last Approved 24h Window)
            st.subheader(f"⏱️ Performance Window: {max_approved_ts.strftime('%m/%d %H:%M')}")
            last_approved_24h = df_c[df_c['timestamp'] >= (max_approved_ts - timedelta(hours=24))].copy()
            
            if not last_approved_24h.empty:
                last_approved_24h['depth_num'] = last_approved_24h['depth'].str.extract('(\d+)').astype(float)
                stats = last_approved_24h.groupby(['depth', 'depth_num']).agg(
                    High=('temperature', 'max'), Low=('temperature', 'min'), Current=('temperature', 'last'), Last_Update=('timestamp', 'last')
                ).reset_index()
                stats['Difference'] = stats['High'] - stats['Low']
                stats = stats.sort_values('depth_num') # Fixed Numerical Order
                
                st.dataframe(stats[['depth', 'Current', 'High', 'Low', 'Difference', 'Last_Update']].style.format({
                    'Current': '{:.1f}', 'High': '{:.1f}', 'Low': '{:.1f}', 'Difference': '{:.1f}', 'Last_Update': '{:%m/%d %H:%M}'
                }), width='stretch', hide_index=True)

            latest_note = df_c.sort_values('timestamp', ascending=False)['engineer_note'].dropna()
            if not latest_note.empty and latest_note.iloc[0]:
                st.info(f"**Field Engineer Note:** {latest_note.iloc[0]}")

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
        data_q = f"SELECT timestamp, temperature, depth, sensor_name FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE project = '{sel_proj}' AND location = '{sel_loc}' AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY) ORDER BY timestamp ASC"
        df_g = client.query(data_q).to_dataframe()
        
        if not df_g.empty:
            df_g['timestamp'] = pd.to_datetime(df_g['timestamp'])
            end_v = datetime.now(pytz.UTC)
            start_v = end_v - timedelta(days=days_back)
            st.plotly_chart(build_standard_sf_graph(df_g, f"Trend: {sel_proj} | {sel_loc}", start_v, end_v), width='stretch')
    except Exception as e: st.error(f"Diagnostics Error: {e}")

# 4D. DATA INTAKE
# --- 4D. DATA INTAKE (HARDENED FOR MIXED FORMATS) ---
# --- 4D. DATA INTAKE (INTEGRATED WITH API RECOVERY) ---
# --- 4D. DATA INTAKE ---
elif service == "📤 Data Intake Lab":
    st.header("📤 Data Ingestion & Recovery")
    tab1, tab2 = st.tabs(["📄 Manual File Upload", "📡 API Data Recovery"])

    with tab1:
        # [Existing Manual Upload Code remains the same as previous response]
        st.subheader("Manual CSV Ingestion")
        source = st.radio("Device Type", ["SensorPush (CSV)", "Lord (SensorConnect)", "Logger (Master Log)"], horizontal=True)
        u_file = st.file_uploader("Upload Logger File", type=['csv'], key="manual_upload")
        # ... (rest of tab1 logic)

    with tab2:
        st.subheader("📡 Cloud-to-Cloud Range Recovery")
        st.write("Select a specific time range to pull from the SensorPush API.")
        
        col_date1, col_date2 = st.columns(2)
        with col_date1:
            start_date = st.date_input("Start Date", datetime.now() - timedelta(days=1))
        with col_date2:
            end_date = st.date_input("End Date", datetime.now())
        
        # Combine date with default times (00:00 and 23:59)
        start_dt = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=pytz.UTC)
        end_dt = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=pytz.UTC)
        
        if st.button("🛰️ FETCH DATA RANGE"):
            with st.spinner(f"Requesting data from {start_date} to {end_date}..."):
                df_api = fetch_sensorpush_data(start_dt, end_dt) 
                
                if not df_api.empty:
                    st.write(f"Retrieved {len(df_api)} points across {df_api['sensor_id'].nunique()} sensors.")
                    st.dataframe(df_api.head()) # Now includes sensor_name!
                    
                    if st.button("Confirm Cloud Sync"):
                        with st.spinner("Syncing to BigQuery..."):
                            table_ref = f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush"
                            # We send to raw_sensorpush. Note: your master scrub joins this 
                            # against metadata to get final names/projects.
                            client.load_table_from_dataframe(df_api, table_ref).result()
                            
                            # Master Sync Logic
                            sync_sql = f"""
                                CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` AS 
                                WITH Unified AS (
                                    SELECT CAST(timestamp AS TIMESTAMP) as timestamp, value as temperature, REPLACE(nodenumber, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord` 
                                    UNION ALL 
                                    SELECT CAST(timestamp AS TIMESTAMP) as timestamp, temperature, REPLACE(sensor_id, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
                                ) 
                                SELECT u.*, u.node AS sensor_id, m.SensorName as sensor_name, m.Project as project, m.Location as location, m.Depth as depth 
                                FROM Unified u INNER JOIN `{PROJECT_ID}.{DATASET_ID}.master_metadata` m ON u.node = REPLACE(m.NodeNum, ':', '-')
                            """
                            client.query(sync_sql).result()
                            st.success("✅ Cloud database updated and names synced!")
                else:
                    st.warning("No data found for this range.")

# --- 4E. ADMIN TOOLS ---
elif service == "🛠️ Admin Tools":
    st.header("🛠️ Engineering Admin Tools")
    tab_scrub, tab_approve = st.tabs(["🧹 Data Scrubber", "✅ Bulk Approval"])
    # ... (Rest of Admin Tools code remains exactly as provided in the previous response)
