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

# --- 1. CREDENTIALS & INITIALIZATION ---
from google.oauth2 import service_account

if "gcp_service_account" in st.secrets:
    info = st.secrets["gcp_service_account"]
    # We explicitly define scopes here to bridge BigQuery and Google Drive
    scopes = [
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    client = bigquery.Client(credentials=creds, project=info["project_id"])
else:
    st.error("GCP Credentials not found in Streamlit Secrets.")

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
# --- 4. SERVICE ROUTING ---
# --- 4. SERVICE ROUTING ---
# --- 4. SERVICE ROUTING ---
# --- 4. SERVICE ROUTING ---
# --- 4. SERVICE ROUTING ---
# --- 4. SERVICE ROUTING ---

# Updated to match your specific BigQuery Hierarchy
DATASET_ID = "sensor_data" 

# 4A. EXECUTIVE SUMMARY
if service == "🏠 Executive Summary":
    st.header("🏠 Site Health & Warming Alerts")
    try:
        proj_q = f"SELECT DISTINCT project FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE project IS NOT NULL"
        meta_df = client.query(proj_q).to_dataframe()
        all_projs = sorted(meta_df['project'].dropna().unique())
        sel_summary_proj = st.selectbox("Select Project Focus", all_projs, index=0)

        query = f"""
            WITH NodeLimits AS (
                SELECT sensor_id, MAX(timestamp) as max_ts 
                FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master`
                WHERE project = '{sel_summary_proj}' 
                GROUP BY sensor_id
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
                    "Location": n_df['location'].iloc[0], 
                    "Depth": f"{n_df['depth'].iloc[0]}ft", 
                    "Node ID": node,
                    "Status": f"{last_ts.strftime('%m/%d %H:%M')} ({int(round(hrs, 0))}h ago)",
                    "Change": round(float(chg), 1), 
                    "Current": round(float(curr_t), 1)
                })
            st.dataframe(pd.DataFrame(summary_stats), width='stretch', hide_index=True)
    except Exception as e:
        st.error(f"Summary Error: {e}")

# 4B. CLIENT PORTAL
elif service == "📊 Client Portal":
    st.header("📊 Project Status Report")
    try:
        meta_q = f"SELECT DISTINCT project, location FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE is_approved = TRUE"
        meta_df = client.query(meta_q).to_dataframe()
        if meta_df.empty:
            st.warning("No approved data found. Check Admin Tools to approve data.")
        else:
            c1, c2, c3 = st.columns([1, 1, 1])
            with c1:
                sel_proj = st.selectbox("Project", sorted(meta_df['project'].dropna().unique()))
            with c2: 
                locs = sorted(meta_df[meta_df['project'] == sel_proj]['location'].dropna().unique())
                sel_loc = st.selectbox("Pipe / Bank", locs)
            with c3:
                weeks_to_view = st.slider("Weeks to View", 1, 12, 6)
            
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
                    # Raw string r'' fixed
                    snapshot['depth_num'] = snapshot['depth'].str.extract(r'(\d+)').astype(float)
                    snapshot['Date'] = snapshot['timestamp'].dt.strftime('%m/%d')
                    fig_profile = px.line(snapshot.sort_values('depth_num'), x='temperature', y='depth_num', color='Date', markers=True, range_x=[-20, 80])
                    for val, label in active_refs:
                        fig_profile.add_vline(x=val, line_dash="dash", line_color="blue", annotation_text=label)
                    fig_profile.update_layout(title={'text': "Temperature by Depth", 'x': 0, 'xanchor': 'left'}, plot_bgcolor='white', height=600)
                    fig_profile.update_yaxes(autorange="reversed", mirror=True, showline=True, linecolor='black', linewidth=2)
                    st.plotly_chart(fig_profile, width='stretch')

            st.subheader("📈 Historical Trends")
            fig_timeline = build_standard_sf_graph(df_c, f"{weeks_to_view}-Week Trend: {sel_loc}", start_view, end_view, active_refs)
            st.plotly_chart(fig_timeline, width='stretch')
    except Exception as e:
        st.error(f"Portal Error: {e}")

# 4D. DATA INTAKE
elif service == "📤 Data Intake Lab":
    st.header("📤 Data Ingestion & Recovery")
    tab1, tab2 = st.tabs(["📄 Manual File Upload", "📡 API Data Recovery"])
    
    with tab1:
        st.subheader("Manual CSV Ingestion")
        source = st.radio("Device Type", ["Master Log", "Lord (SensorConnect)", "SensorPush Export"], horizontal=True)
        u_file = st.file_uploader("Upload Logger File", type=['csv'], key="manual_upload")
        if u_file is not None:
            try:
                df_up, table_ref = pd.DataFrame(), ""
                if source == "Master Log":
                    df_up = pd.read_csv(u_file)
                    df_up.columns = [c.lower() for c in df_up.columns]
                    if 'nodenumber' in df_up.columns:
                        df_up = df_up.rename(columns={'nodenumber': 'sensor_id'})
                    table_ref = f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush"
                elif source == "Lord (SensorConnect)":
                    content = u_file.getvalue().decode("utf-8").splitlines()
                    start_idx = next((i for i, l in enumerate(content) if "DATA_START" in l), 0)
                    u_file.seek(0)
                    df_raw = pd.read_csv(u_file, skiprows=start_idx + 1)
                    df_up = df_raw.melt(id_vars=[df_raw.columns[0]], var_name='sensor_id', value_name='value').rename(columns={df_raw.columns[0]: 'timestamp'})
                    table_ref = f"{PROJECT_ID}.{DATASET_ID}.raw_lord"
                
                if not df_up.empty:
                    df_up['timestamp'] = pd.to_datetime(df_up['timestamp'], format='mixed', errors='coerce').dropna()
                    df_up['sensor_id'] = df_up['sensor_id'].astype(str).str.replace(':', '-', regex=False)
                    st.dataframe(df_up.head())
                    if st.button("🚀 PUSH TO BIGQUERY"):
                        with st.spinner("Uploading..."):
                            client.load_table_from_dataframe(df_up, table_ref).result()
                            st.success("✅ Raw Data Uploaded!")
            except Exception as e:
                st.error(f"Upload Error: {e}")

    with tab2:
        st.subheader("📡 SensorPush Multi-Account Recovery")
        c1, c2 = st.columns(2)
        with c1:
            sd = st.date_input("Recovery Start Date", datetime.now() - timedelta(days=2))
            st_time = st.time_input("Start Time (UTC)", datetime.strptime("00:00", "%H:%M").time())
        with c2:
            ed = st.date_input("Recovery End Date", datetime.now())
            et_time = st.time_input("End Time (UTC)", datetime.now().time())

        if st.button("🛰️ RUN ALL-ACCOUNT RECOVERY"):
            s_iso = datetime.combine(sd, st_time).strftime("%Y-%m-%dT%H:%M:%S+0000")
            e_iso = datetime.combine(ed, et_time).strftime("%Y-%m-%dT%H:%M:%S+0000")
            
            if "sensorpush_accounts" not in st.secrets:
                st.error("Missing accounts in Secrets.")
            else:
                accounts, all_api_recs = st.secrets["sensorpush_accounts"], []
                for acc_id, creds in accounts.items():
                    try:
                        with st.spinner(f"Auth {acc_id}..."):
                            auth_res = requests.post("https://api.sensorpush.com/api/v1/oauth/authorize", json=dict(creds))
                            if auth_res.status_code == 200:
                                auth_code = auth_res.json().get("authorization")
                                t_res = requests.post("https://api.sensorpush.com/api/v1/oauth/accesstoken", json={"authorization": auth_code})
                                if t_res.status_code == 200:
                                    token = t_res.json().get("accesstoken")
                                    headers = {"Authorization": token, "accept": "application/json"}
                                    sensor_res = requests.post("https://api.sensorpush.com/api/v1/devices/sensors", headers=headers, json={})
                                    if sensor_res.status_code == 200:
                                        s_ids = list(sensor_res.json().keys())
                                        payload = {"startTime": s_iso, "endTime": e_iso, "sensors": s_ids}
                                        samp_res = requests.post("https://api.sensorpush.com/api/v1/samples", headers=headers, json=payload)
                                        if samp_res.status_code == 200:
                                            for sid, samples in samp_res.json().get("sensors", {}).items():
                                                for s in samples:
                                                    val = s.get('value') or s.get('temperature')
                                                    if val is not None:
                                                        all_api_recs.append({"timestamp": s["observed"], "temperature": float(val), "sensor_id": sid.replace(':', '-')})
                    except Exception as e:
                        st.error(f"API Error: {e}")

                if all_api_recs:
                    df_api = pd.DataFrame(all_api_recs)
                    df_api['timestamp'] = pd.to_datetime(df_api['timestamp'], format='mixed')
                    
                    with st.spinner("Pushing to BigQuery & Syncing Master Table..."):
                        client.load_table_from_dataframe(df_api, f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush").result()
                        # Sync logic joining on m.nodenum
                        scrub_sql = f"""
                            CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` AS 
                            WITH Unified AS (
                                SELECT CAST(timestamp AS TIMESTAMP) as timestamp, value as temperature, REPLACE(CAST(nodenumber AS STRING), ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord` 
                                UNION ALL 
                                SELECT CAST(timestamp AS TIMESTAMP) as timestamp, temperature, REPLACE(CAST(sensor_id AS STRING), ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
                            ) 
                            SELECT u.timestamp, u.node AS sensor_id, u.temperature, m.nodenum as sensor_name, m.project, m.location, m.depth, CAST(FALSE AS BOOLEAN) as is_approved, CAST(NULL AS STRING) as engineer_note
                            FROM Unified u INNER JOIN `{PROJECT_ID}.{DATASET_ID}.master_metadata` m ON u.node = REPLACE(CAST(m.nodenum AS STRING), ':', '-')
                        """
                        client.query(scrub_sql).result()
                    st.success(f"✅ Sync Complete! {len(all_api_recs)} points added.")
                    st.balloons()

# 4E. ADMIN TOOLS
elif service == "🛠️ Admin Tools":
    st.header("🛠️ Admin Tools")
    st.subheader("1. Metadata Connection Test")
    if st.button("🔍 Test Metadata Access"):
        try:
            test_q = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.master_metadata` LIMIT 5"
            test_df = client.query(test_q).to_dataframe()
            st.success(f"✅ BigQuery can see the Metadata Google Sheet in dataset '{DATASET_ID}'!")
            st.dataframe(test_df)
        except Exception as e:
            st.error(f"❌ Diagnostic failed: {e}")

    st.subheader("2. Table Scrubber")
    sc_proj, sc_loc = st.text_input("Project Name"), st.text_input("Location")
    if st.button("🗑️ DELETE POINTS"):
        client.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE project='{sc_proj}' AND location='{sc_loc}'").result()
        st.success("Data Scrubbed.")
