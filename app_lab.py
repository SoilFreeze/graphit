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

def fetch_sensorpush_data(hours_back):
    """Fetches data from SensorPush API based on hours back."""
    # Using the credentials from your catch-up script
    ACCOUNTS = [
        {'email': 'tsteele@soilfreeze.com', 'password': 'Freeze123!!'},
        {'email': 'soilfreeze98072@gmail.com', 'password': 'Freeze123!!'}
    ]
    BASE_URL = "https://api.sensorpush.com/api/v1"
    all_records = []
    
    now_utc = datetime.now(pytz.UTC)
    start_time = (now_utc - timedelta(hours=hours_back)).replace(minute=0, second=0, microsecond=0)
    
    # Create hourly slots
    target_times = [start_time + timedelta(hours=i) for i in range(hours_back + 1)]

    for acc in ACCOUNTS:
        # Step 1: Auth
        auth_resp = requests.post(f"{BASE_URL}/oauth/authorize", json=acc)
        if auth_resp.status_code != 200: continue
        
        token_resp = requests.post(f"{BASE_URL}/oauth/accesstoken", 
                                   json={"authorization": auth_resp.json().get('authorization')})
        token = token_resp.json().get('accesstoken')
        headers = {"Authorization": token}

        # Step 2: Get Sensors (to filter HT.w)
        dev_resp = requests.post(f"{BASE_URL}/devices/sensors", headers=headers, json={})
        valid_ids = []
        if dev_resp.status_code == 200:
            sensors_data = dev_resp.json()
            # Normalize to list if dict
            sensor_list = sensors_data.values() if isinstance(sensors_data, dict) else sensors_data
            valid_ids = [str(s.get('id')) for s in sensor_list if str(s.get('type')) == 'HT.w']

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
elif service == "📤 Data Intake Lab":
    st.header("📤 Data Ingestion & Recovery")
    tab1, tab2 = st.tabs(["📄 Manual File Upload", "📡 API Data Recovery"])

    with tab1:
        st.subheader("Manual CSV Ingestion")
        source = st.radio("Device Type", ["SensorPush (CSV)", "Lord (SensorConnect)", "Logger (Master Log)"], horizontal=True)
        u_file = st.file_uploader("Upload Logger File", type=['csv'], key="manual_upload")
        
        if u_file is not None:
            try:
                # 1. Load the raw data
                # We use low_memory=False to handle mixed types in columns
                df_raw = pd.read_csv(u_file, low_memory=False)
                
                # 2. Fix Timestamps (The 'mixed' format solves your error)
                # We find the timestamp column regardless of capitalization
                ts_col = next((c for c in df_raw.columns if c.lower() == 'timestamp'), df_raw.columns[0])
                df_raw['timestamp_parsed'] = pd.to_datetime(df_raw[ts_col], format='mixed', errors='coerce')
                df_raw = df_raw.dropna(subset=['timestamp_parsed']).drop(columns=[ts_col]).rename(columns={'timestamp_parsed': 'timestamp'})

                df_up = pd.DataFrame()
                table_ref = ""

                # 3. Process based on source
                if "Master Log" in source:
                    # Detect if narrow format: check if the 3rd column contains Sensor ID strings
                    # Master_Log.csv mostly uses: Timestamp, Temp, SensorID
                    if df_raw.iloc[:, 1].dtype == object or (df_raw.shape[1] > 2 and any(isinstance(x, str) for x in df_raw.iloc[:, 1])):
                        # Narrow logic: Col 0: TS, Col 1: Temp, Col 2: SensorID
                        df_up = df_raw.iloc[:, [df_raw.columns.get_loc('timestamp'), 0, 1]].copy()
                        df_up.columns = ['timestamp', 'temperature', 'sensor_id']
                        # Filter out rows that are actually wide (where sensor_id is numeric)
                        df_up = df_up[df_up['sensor_id'].apply(lambda x: not str(x).replace('.','',1).isdigit())]
                        table_ref = f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush"
                    else:
                        # Wide logic: Melt the columns
                        df_up = df_raw.melt(id_vars=['timestamp'], var_name='sensor_id', value_name='temperature')
                        table_ref = f"{PROJECT_ID}.{DATASET_ID}.raw_lord"

                elif "Lord" in source:
                    df_up = df_raw.melt(id_vars=['timestamp'], var_name='nodenumber', value_name='value')
                    table_ref = f"{PROJECT_ID}.{DATASET_ID}.raw_lord"

                else: # SensorPush
                    df_up = df_raw.rename(columns={'Temperature': 'temperature', 'Sensor': 'sensor_id'})
                    table_ref = f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush"

                # 4. Standardize and Upload
                if not df_up.empty:
                    # Ensure node IDs use hyphens instead of colons
                    id_col = 'sensor_id' if 'sensor_id' in df_up.columns else 'nodenumber'
                    df_up[id_col] = df_up[id_col].astype(str).str.replace(':', '-', regex=False)
                    
                    st.write(f"Previewing {len(df_up)} points for {table_ref}:")
                    st.dataframe(df_up.head(), width='stretch')

                    if st.button("🚀 PUSH TO BIGQUERY"):
                        with st.spinner("Uploading..."):
                            client.load_table_from_dataframe(df_up, table_ref).result()
                            
                            # Run the Master Scrub to sync everything
                            scrub_sql = f"""
                                CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` AS 
                                WITH Unified AS (
                                    SELECT CAST(timestamp AS TIMESTAMP) as timestamp, value as temperature, REPLACE(nodenumber, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord` 
                                    UNION ALL 
                                    SELECT CAST(timestamp AS TIMESTAMP) as timestamp, temperature, REPLACE(sensor_id, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
                                ) 
                                SELECT u.*, u.node AS sensor_id, m.SensorName as sensor_name, m.Project as project, m.Location as location, m.Depth as depth 
                                FROM Unified u INNER JOIN `{PROJECT_ID}.{DATASET_ID}.master_metadata` m ON u.node = REPLACE(m.NodeNum, ':', '-')
                            """
                            client.query(scrub_sql).result()
                            st.success("✅ Uploaded and Master Table Synced!")
                            st.balloons()
            except Exception as e:
                st.error(f"File Error: {e}")

with tab2:
        st.subheader("Cloud-to-Cloud Recovery")
        st.write("Pull missing data directly from SensorPush API into BigQuery.")
        
        recovery_hours = st.number_input("Hours of data to recover", min_value=1, max_value=168, value=24)
        
        if st.button("📡 START API RECOVERY"):
            with st.spinner(f"Contacting SensorPush for the last {recovery_hours} hours..."):
                df_recovered = fetch_sensorpush_data(recovery_hours)
                
                if not df_recovered.empty:
                    st.write(f"Found {len(df_recovered)} data points.")
                    st.dataframe(df_recovered.head())
                    
                    # Push to BigQuery
                    table_ref = f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush"
                    job = client.load_table_from_dataframe(df_recovered, table_ref)
                    job.result()
                    
                    # Trigger the Master Scrub (Same SQL logic you already have)
                    scrub_sql = f"""
                        CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` AS 
                        WITH Unified AS (
                            SELECT CAST(timestamp AS TIMESTAMP) as timestamp, value as temperature, REPLACE(nodenumber, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord` 
                            UNION ALL 
                            SELECT CAST(timestamp AS TIMESTAMP) as timestamp, temperature, REPLACE(sensor_id, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
                        ) 
                        SELECT u.*, u.node AS sensor_id, m.SensorName as sensor_name, m.Project as project, m.Location as location, m.Depth as depth 
                        FROM Unified u INNER JOIN `{PROJECT_ID}.{DATASET_ID}.master_metadata` m ON u.node = REPLACE(m.NodeNum, ':', '-')
                    """
                    client.query(scrub_sql).result()
                    st.success("✅ API Data Recovered and Database Synced!")
                    st.balloons()
                else:
                    st.warning("No data found for that time range.")

# 4E. ADMIN TOOLS
# --- 4E. ADMIN TOOLS (BULK APPROVAL & SCRUBBER) ---
# --- 4E. ADMIN TOOLS (BULK APPROVAL & SCRUBBER) ---
elif service == "🛠️ Admin Tools":
    st.header("🛠️ Engineering Admin Tools")
    tab_scrub, tab_approve = st.tabs(["🧹 Data Scrubber", "✅ Bulk Approval"])
    
    with tab_scrub:
        st.subheader("Delete Bad Data")
        sc_proj = st.text_input("Project Name", key="scrub_p")
        sc_loc = st.text_input("Location / Pipe", key="scrub_l")
        col1, col2 = st.columns(2)
        with col1:
            sc_start = st.text_input("Start (YYYY-MM-DD HH:MM:SS)", value="2026-01-01 00:00:00")
        with col2:
            sc_end = st.text_input("End (YYYY-MM-DD HH:MM:SS)", value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            
        if st.button("🗑️ PERMANENTLY DELETE POINTS"):
            if sc_proj and sc_loc:
                scrub_q = f"""
                    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` 
                    WHERE project='{sc_proj}' AND location='{sc_loc}' 
                    AND timestamp BETWEEN '{sc_start}' AND '{sc_end}'
                """
                client.query(scrub_q).result()
                st.success(f"✅ Deleted points for {sc_loc} in {sc_proj}")
            else:
                st.error("Please enter both Project and Location.")

    with tab_approve:
        st.subheader("Bulk Approve Data")
        st.write("Set `is_approved = TRUE` for a specific site and time range.")
        
        try:
            # 1. Get metadata, filtering out Nulls to prevent the '<' error
            unapproved_meta_q = f"""
                SELECT DISTINCT project, location 
                FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` 
                WHERE (is_approved IS FALSE OR is_approved IS NULL)
                AND project IS NOT NULL 
                AND location IS NOT NULL
            """
            un_meta = client.query(unapproved_meta_q).to_dataframe()
            
            if un_meta.empty:
                st.success("🎉 All data is currently approved!")
            else:
                # Use dropna and sorted to safely handle the dropdowns
                u_projs = sorted(un_meta['project'].dropna().unique())
                app_proj = st.selectbox("Select Project", u_projs)
                
                u_locs = sorted(un_meta[un_meta['project'] == app_proj]['location'].dropna().unique())
                app_loc = st.selectbox("Select Location/Pipe", u_locs)
                
                app_note = st.text_area("Engineer Note", placeholder="Verified data trends...")
                
                c1, c2 = st.columns(2)
                with c1:
                    t_start = st.text_input("Start Time", value="2026-01-01 00:00:00", key="app_start")
                with c2:
                    t_end = st.text_input("End Time", value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), key="app_end")

                # 2. Preview Count
                if st.button("🔍 PREVIEW COUNT"):
                    count_q = f"""
                        SELECT COUNT(*) as total FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master`
                        WHERE project='{app_proj}' AND location='{app_loc}'
                        AND timestamp BETWEEN '{t_start}' AND '{t_end}'
                        AND (is_approved IS FALSE OR is_approved IS NULL)
                    """
                    count_res = client.query(count_q).to_dataframe()
                    st.info(f"This will approve **{count_res['total'].iloc[0]}** data points.")

                # 3. Final Approval
                if st.button("🚀 BULK APPROVE NOW"):
                    bulk_q = f"""
                        UPDATE `{PROJECT_ID}.{DATASET_ID}.final_databoard_master`
                        SET is_approved = TRUE, engineer_note = '{app_note}'
                        WHERE project = '{app_proj}' AND location = '{app_loc}'
                        AND timestamp BETWEEN '{t_start}' AND '{t_end}'
                    """
                    client.query(bulk_q).result()
                    st.success(f"✅ Approved data for {app_loc}!")
                    st.balloons()
                    
        except Exception as e:
            st.error(f"Approval Error: {e}")
