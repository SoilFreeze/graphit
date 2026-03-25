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

# --- 2. THE MALTBY GRAPH ENGINE (FIXED) ---
def build_standard_sf_graph(df, title, start_view, end_view):
    display_df = df.copy()
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
        fig.add_trace(go.Scatter(
            x=sensor_df['timestamp'], y=sensor_df['temperature'].round(1),
            name=d, mode='lines', connectgaps=False, line=dict(width=2.5),
            hovertemplate='%{x}<br>Temp: %{y:.1f}°F'
        ))

    # Grid & Axis Styling
    fig.update_yaxes(gridcolor='DimGray', gridwidth=1, minor=dict(dtick=5, gridcolor='Silver', showgrid=True),
                     mirror=True, showline=True, linecolor='black', linewidth=2, title="Temperature (°F)")
    fig.update_xaxes(showgrid=False, range=[start_view, end_view], mirror=True, showline=True, linecolor='black', linewidth=2)

    # NOW Line
    now_ts = datetime.now(pytz.UTC)
    fig.add_vline(x=now_ts, line_width=2, line_color="red", annotation_text="RIGHT NOW")
    fig.add_hline(y=32, line_dash="dash", line_color="cyan", annotation_text="32°F")

    fig.update_layout(title={'text': title, 'x': 0.5, 'font': {'size': 24}}, plot_bgcolor='white',
                      hovermode="x unified", legend=dict(x=1.02, y=1, bordercolor="Black", borderwidth=1), 
                      margin=dict(r=150, t=80), height=800)
    return fig

# --- 3. SIDEBAR NAVIGATION ---
st.sidebar.title("❄️ SoilFreeze Lab")
service = st.sidebar.selectbox("Select Service", ["🏠 Executive Summary", "📉 Node Diagnostics", "📤 Data Intake Lab"])

# --- 4. SERVICE ROUTING ---

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
                hrs = (now_ts - n_df['timestamp'].iloc[-1]).total_seconds() / 3600
                summary_stats.append({
                    "Location": n_df['location'].iloc[0], "Depth": f"{n_df['depth'].iloc[0]}ft", "Node ID": node,
                    "Status": f"{n_df['timestamp'].iloc[-1].strftime('%m/%d %H:%M')} ({int(round(hrs, 0))}h ago)",
                    "Change": round(float(chg), 1), "Current": round(float(curr_t), 1)
                })
            st.dataframe(pd.DataFrame(summary_stats), use_container_width=True, hide_index=True)
    except Exception as e: st.error(f"Summary Error: {e}")

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
        data_q = f"""
            SELECT timestamp, temperature, depth, sensor_name FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master`
            WHERE project = '{sel_proj}' AND location = '{sel_loc}'
            AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
            ORDER BY timestamp ASC
        """
        df_g = client.query(data_q).to_dataframe()
        if not df_g.empty:
            df_g['timestamp'] = pd.to_datetime(df_g['timestamp'])
            end_v = datetime.now(pytz.UTC)
            start_v = end_v - timedelta(days=days_back)
            fig = build_standard_sf_graph(df_g, f"Trend: {sel_proj} | {sel_loc}", start_v, end_v)
            st.plotly_chart(fig, use_container_width=True)
            latest = df_g.sort_values('timestamp').groupby('depth').tail(1)
            st.dataframe(latest[['depth', 'sensor_name', 'temperature']].style.format({'temperature': '{:.1f}'}), use_container_width=True, hide_index=True)
    except Exception as e: st.error(f"Diagnostics Error: {e}")

elif service == "📤 Data Intake Lab":
    st.header("📤 Data Ingestion & Recovery")
    t1, t2 = st.tabs(["📄 Manual File", "📡 API Recovery"])
    
    with t1:
        u_file = st.file_uploader("Upload CSV", type=['csv'])
        if u_file and st.button("🚀 PUSH TO BIGQUERY"):
            try:
                df_up = pd.read_csv(u_file).rename(columns={'Timestamp':'timestamp','Temperature':'temperature','Sensor':'sensor_id'})
                df_up['sensor_id'] = df_up['sensor_id'].astype(str).str.replace(':', '-')
                df_up['timestamp'] = pd.to_datetime(df_up['timestamp'])
                client.load_table_from_dataframe(df_up, f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush").result()
                
                # Auto-Scrub
                scrub_sql = f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` AS 
                    WITH Unified AS (
                        SELECT CAST(timestamp AS TIMESTAMP) as timestamp, value as temperature, REPLACE(nodenumber, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord` 
                        UNION ALL 
                        SELECT CAST(timestamp AS TIMESTAMP) as timestamp, temperature, REPLACE(sensor_id, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
                    ) 
                    SELECT u.*, u.node as sensor_id, m.SensorName as sensor_name, m.Project as project, m.Location as location, m.Depth as depth 
                    FROM Unified u INNER JOIN `{PROJECT_ID}.{DATASET_ID}.master_metadata` m ON u.node = REPLACE(m.NodeNum, ':', '-')
                """
                client.query(scrub_sql).result()
                st.success("✅ Uploaded and Synced!")
            except Exception as e: st.error(f"Upload failed: {e}")

    with t2:
        st.subheader("SensorPush Cloud Recovery")
        if st.button("🛰️ RUN CLOUD RECOVERY"):
            st.info("Triggering API Fetch...")
            # [Full API logic goes here]
