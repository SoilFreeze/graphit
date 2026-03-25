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

# Constants for BigQuery
DATASET_ID = "sensor_data" 
PROJECT_ID = "sensorpush-export"

@st.cache_resource
def get_bq_client():
    try:
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(info)
            scoped_creds = credentials.with_scopes([
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/bigquery"
            ])
            return bigquery.Client(credentials=scoped_creds, project=info["project_id"])
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"Authentication Failed: {e}")
        return None

client = get_bq_client()

# --- 2. SIDEBAR NAVIGATION ---
st.sidebar.title("❄️ SoilFreeze Lab")
service = st.sidebar.selectbox("Select Service", [
    "🏠 Executive Summary", 
    "📉 Node Diagnostics", 
    "📤 Data Intake Lab"
])

# --- 3. SERVICE ROUTING ---

# 🏠 EXECUTIVE SUMMARY
if service == "🏠 Executive Summary":
    st.header("🏠 Site Health & Warming Alerts")
    try:
        meta_df = client.query(
            f"SELECT DISTINCT project FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE project IS NOT NULL"
        ).to_dataframe()
        
        all_projs = sorted(meta_df['project'].unique())
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

        if df_summary.empty:
            st.warning("No historical data found.")
        else:
            now_ts = datetime.now(pytz.UTC)
            summary_stats = []
            for node in df_summary['sensor_id'].unique():
                n_df = df_summary[df_summary['sensor_id'] == node].sort_values('timestamp')
                current_temp = n_df['temperature'].iloc[-1]
                net_change = current_temp - n_df['temperature'].iloc[0]
                last_seen_dt = n_df['timestamp'].iloc[-1]
                hours_ago = (now_ts - last_seen_dt).total_seconds() / 3600
                
                summary_stats.append({
                    "Location": n_df['location'].iloc[0],
                    "Depth": f"{n_df['depth'].iloc[0]}ft",
                    "Node ID": node,
                    "Status / Last Seen": f"{last_seen_dt.strftime('%m/%d %H:%M')} ({int(round(hours_ago, 0))}h ago)",
                    "hours_raw": hours_ago,
                    "Min (24h)": round(float(n_df['temperature'].min()), 1),
                    "Max (24h)": round(float(n_df['temperature'].max()), 1),
                    "24h Change": round(float(net_change), 1),
                    "Current": round(float(current_temp), 1)
                })

            df_full = pd.DataFrame(summary_stats).sort_values(by="24h Change", ascending=False)
            
            # Formatting & Display
            def apply_styles(row):
                styles = [''] * len(row)
                h, chg = row['hours_raw'], row['24h Change']
                s_idx, c_idx = row.index.get_loc("Status / Last Seen"), row.index.get_loc("24h Change")
                if h >= 24: styles[s_idx] = 'background-color: #ff4b4b; color: white'
                elif h >= 12: styles[s_idx] = 'background-color: #ffa500; color: black'
                elif h >= 6: styles[s_idx] = 'background-color: #ffff00; color: black'
                
                if chg >= 5.0: styles[c_idx] = 'background-color: #ff4b4b; color: white'
                elif chg >= 1.0: styles[c_idx] = 'background-color: #ffff00; color: black'
                elif chg <= -1.0: styles[c_idx] = 'background-color: #00008b; color: white'
                return styles

            st.dataframe(
                df_full.style.apply(apply_styles, axis=1),
                column_config={"hours_raw": None, "Current": st.column_config.NumberColumn(format="%.1f")},
                hide_index=True, width="stretch"
            )
    except Exception as e:
        st.error(f"Executive Summary Error: {e}")

# 📉 NODE DIAGNOSTICS
elif service == "📉 Node Diagnostics":
    st.header("📉 High-Resolution Node Diagnostics")
    try:
        meta_q = f"SELECT DISTINCT project, location FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE project IS NOT NULL"
        meta_df = client.query(meta_q).to_dataframe()

        if meta_df.empty:
            st.warning("No data found. Run 'Master Scrub' in Data Intake.")
        else:
            c1, c2, c3 = st.columns(3)
            with c1: sel_proj = st.selectbox("Project", sorted(meta_df['project'].unique()))
            with c2: 
                locs = sorted(meta_df[meta_df['project'] == sel_proj]['location'].unique())
                sel_loc = st.selectbox("Pipe / Bank", locs)
            with c3: weeks = st.slider("Lookback (Weeks)", 1, 12, 4)

            data_q = f"""
                SELECT timestamp, temperature, depth, sensor_name FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master`
                WHERE project = '{sel_proj}' AND location = '{sel_loc}'
                AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {weeks} WEEK)
                ORDER BY timestamp ASC
            """
            df_g = client.query(data_q).to_dataframe()

            if df_g.empty:
                st.info("No data found.")
            else:
                df_g['timestamp'] = pd.to_datetime(df_g['timestamp'])
                df_g['temperature'] = df_g['temperature'].astype(float).round(1)
                df_g['d_sort'] = df_g['depth'].str.extract(r'(\d+)').fillna(0).astype(float)
                df_g = df_g.sort_values(['d_sort', 'timestamp'])

                fig = px.line(df_g, x='timestamp', y='temperature', color='depth', 
                             hover_data={'temperature': ':.1f', 'timestamp': True})
                fig.update_layout(hovermode="x unified")
                st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Diagnostics Error: {e}")
        st.code(traceback.format_exc())

# 📤 DATA INTAKE LAB
elif service == "📤 Data Intake Lab":
    st.header("📤 Data Ingestion & Recovery")
    t1, t2 = st.tabs(["📄 Manual File", "📡 API Recovery"])
    
    with t1:
        u_file = st.file_uploader("Upload CSV", type=['csv'])
        if u_file and st.button("🚀 PUSH TO BIGQUERY"):
            df = pd.read_csv(u_file).rename(columns={'Timestamp':'timestamp','Temperature':'temperature','Sensor':'sensor_id'})
            client.load_table_from_dataframe(df, f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush").result()
            st.success("Uploaded!")

    with t2:
        if st.button("🛰️ RUN CLOUD RECOVERY"):
            # Simplified for brevity; uses your requests logic
            st.write("Fetching from SensorPush API...")
            # [Insert the Tab 2 API Recovery code here]
