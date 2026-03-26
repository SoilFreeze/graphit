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

# --- 2. HELPERS & API ENGINE ---

def fetch_sensorpush_data(start_dt, end_dt):
    """
    FAST BATCH FETCH: Pulls the entire time range in one API call per account.
    """
    ACCOUNTS = [
        {'email': 'tsteele@soilfreeze.com', 'password': 'Freeze123!!'},
        {'email': 'soilfreeze98072@gmail.com', 'password': 'Freeze123!!'}
    ]
    BASE_URL = "https://api.sensorpush.com/api/v1"
    all_records = []
    
    # Format times for API
    start_str = start_dt.strftime('%Y-%m-%dT%H:%M:%S+0000')

    for acc in ACCOUNTS:
        try:
            # 1. Auth
            auth_resp = requests.post(f"{BASE_URL}/oauth/authorize", json=acc, timeout=10)
            if auth_resp.status_code != 200: continue
            token = requests.post(f"{BASE_URL}/oauth/accesstoken", 
                                   json={"authorization": auth_resp.json().get('authorization')}, timeout=10).json().get('accesstoken')
            headers = {"Authorization": token}

            # 2. Get Names & ID List
            dev_resp = requests.post(f"{BASE_URL}/devices/sensors", headers=headers, json={}, timeout=10)
            name_map = {}
            if dev_resp.status_code == 200:
                sensor_list = dev_resp.json().values() if isinstance(dev_resp.json(), dict) else dev_resp.json()
                for s in sensor_list:
                    name_map[str(s.get('id'))] = s.get('name', str(s.get('id')))

            # 3. BATCH FETCH (No more hourly loops!)
            # We set a high limit (10,000) to get the whole range at once
            payload = {
                "limit": 10000, 
                "startTime": start_str, 
                "sensors": list(name_map.keys())
            }
            r = requests.post(f"{BASE_URL}/samples", headers=headers, json=payload, timeout=20)
            
            if r.status_code == 200:
                data = r.json().get('sensors', {})
                for s_id, samples in data.items():
                    for s in samples:
                        # Extract and Convert
                        ts = pd.to_datetime(s.get('observed'))
                        # Only keep data within our requested window
                        if start_dt <= ts <= end_dt:
                            temp = s.get('temp_f') or s.get('temperature') or ((s.get('temp_c', 0) * 1.8) + 32)
                            all_records.append({
                                'timestamp': ts,
                                'sensor_id': s_id.replace(':', '-'),
                                'sensor_name': name_map.get(s_id),
                                'temperature': round(float(temp), 2)
                            })
        except Exception as e:
            st.error(f"Error on {acc['email']}: {e}")
            
    return pd.DataFrame(all_records)

def build_standard_sf_graph(df, title, start_view, end_view, active_refs):
    """Generates the standardized SoilFreeze thermal line graph."""
    display_df = df.copy()
    y_range, y_ticks, y_label, m_step = [-20, 80], [-20, 0, 20, 40, 60, 80], "Temp (°F)", 5

    # Gap Logic
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
    
    fig.update_yaxes(title=y_label, tickmode='array', tickvals=y_ticks, range=y_range,
                     gridcolor='DimGray', gridwidth=1.5, minor=dict(dtick=m_step, gridcolor='Silver', showgrid=True),
                     mirror=True, showline=True, linecolor='black', linewidth=2)
    fig.update_xaxes(showgrid=False, range=[start_view, end_view], mirror=True, showline=True, linecolor='black', linewidth=2)

    for val, label in active_refs:
        fig.add_hline(y=val, line_dash="dash", line_color="blue", annotation_text=f"{label} {val}°")
    
    now_ms = datetime.now(pytz.UTC).timestamp() * 1000
    fig.add_vline(x=now_ms, line_width=2, line_color="red", annotation_text="RIGHT NOW")

    return fig

# --- 3. SIDEBAR NAVIGATION ---
st.sidebar.title("❄️ SoilFreeze Lab")

st.sidebar.subheader("Graph Reference Lines")
show_32 = st.sidebar.checkbox("Freezing (32°F)", value=True)
show_26 = st.sidebar.checkbox("Type B (26.6°F)", value=True)
show_10 = st.sidebar.checkbox("Type A (10.2°F)", value=True)

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
# --- 4D. DATA INTAKE LAB ---
elif service == "📤 Data Intake Lab":
    st.header("📤 Data Ingestion & Recovery")
    tab1, tab2 = st.tabs(["📄 Manual File Upload", "📡 API Data Recovery"])

    with tab1:
        # (Manual upload code stays the same)
        pass

    with tab2:
        st.subheader("📡 Cloud-to-Cloud Range Recovery")
        c1, c2 = st.columns(2)
        start_date = c1.date_input("Start Date", datetime.now() - timedelta(days=2))
        end_date = c2.date_input("End Date", datetime.now())
        
        if st.button("🛰️ BATCH FETCH & SYNC"):
            status_box = st.empty() # Placeholder for live updates
            
            start_dt = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=pytz.UTC)
            end_dt = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=pytz.UTC)
            
            # STEP 1: API FETCH
            status_box.info("Step 1: Contacting SensorPush API...")
            df_api = fetch_sensorpush_data(start_dt, end_dt)
            
            if not df_api.empty:
                st.write(f"✅ Found {len(df_api)} points.")
                st.dataframe(df_api.head())
                
                # STEP 2: BIGQUERY UPLOAD
                status_box.info("Step 2: Uploading Raw Data to BigQuery...")
                try:
                    table_ref = f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush"
                    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                    
                    # Force upload
                    job = client.load_table_from_dataframe(df_api, table_ref, job_config=job_config)
                    job.result() # Wait for upload to finish
                    st.success(f"✅ Successfully uploaded {len(df_api)} rows to `{table_ref}`")
                    
                    # STEP 3: DATABASE SCRUB
                    status_box.info("Step 3: Running Master Deduplication (1 point/hour)...")
                    scrub_sql = f"""
                        CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` AS 
                        WITH RawUnified AS (
                            SELECT CAST(timestamp AS TIMESTAMP) as timestamp, value as temperature, REPLACE(nodenumber, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord` WHERE value IS NOT NULL
                            UNION ALL 
                            SELECT CAST(timestamp AS TIMESTAMP) as timestamp, temperature, REPLACE(sensor_id, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` WHERE temperature IS NOT NULL
                        ),
                        HourlyDedupped AS (
                            SELECT *, ROW_NUMBER() OVER(PARTITION BY node, TIMESTAMP_TRUNC(timestamp, HOUR) ORDER BY timestamp DESC) as rank FROM RawUnified
                        )
                        SELECT h.timestamp, h.temperature, h.node AS sensor_id, m.SensorName as sensor_name, m.Project as project, m.Location as location, m.Depth as depth, FALSE as is_approved
                        FROM HourlyDedupped h 
                        INNER JOIN `{PROJECT_ID}.{DATASET_ID}.master_metadata` m ON h.node = REPLACE(m.NodeNum, ':', '-')
                        WHERE h.rank = 1
                    """
                    client.query(scrub_sql).result()
                    status_box.success("✅ FINAL STEP: Master Table Synced and Deduplicated!")
                    st.balloons()
                    
                except Exception as e:
                    status_box.error(f"❌ BigQuery Error: {e}")
                    st.code(traceback.format_exc())
            else:
                status_box.warning("No data found for this range. Check your SensorPush account.")

# 4E. ADMIN TOOLS
elif service == "🛠️ Admin Tools":
    st.header("🛠️ Engineering Admin Tools")
    tab_scrub, tab_approve = st.tabs(["🧹 Data Scrubber", "✅ Bulk Approval"])
    
    with tab_scrub:
        sc_proj = st.text_input("Project Name")
        sc_loc = st.text_input("Location / Pipe")
        if st.button("🗑️ DELETE POINTS"):
            scrub_q = f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE project='{sc_proj}' AND location='{sc_loc}'"
            client.query(scrub_q).result()
            st.success(f"Deleted {sc_loc} data.")

    with tab_approve:
        try:
            unapproved_meta_q = f"SELECT DISTINCT project, location FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE (is_approved IS FALSE OR is_approved IS NULL)"
            un_meta = client.query(unapproved_meta_q).to_dataframe()
            if not un_meta.empty:
                app_proj = st.selectbox("Project", un_meta['project'].unique())
                app_loc = st.selectbox("Location", un_meta[un_meta['project'] == app_proj]['location'].unique())
                if st.button("🚀 APPROVE NOW"):
                    client.query(f"UPDATE `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` SET is_approved = TRUE WHERE project='{app_proj}' AND location='{app_loc}'").result()
                    st.success("Approved!")
        except Exception as e: st.error(f"Approval Error: {e}")
