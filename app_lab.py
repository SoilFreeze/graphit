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
from google.cloud import bigquery

if "gcp_service_account" in st.secrets:
    info = st.secrets["gcp_service_account"]
    
    # THIS IS THE LOOP BREAKER: Explicitly asking for Drive access
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
# --- 4. SERVICE ROUTING ---

# Confirmed Project and Dataset
PROJECT_ID = "sensorpush-export"
DATASET_ID = "sensor_data"

# 4A. EXECUTIVE SUMMARY
if service == "🏠 Executive Summary":
    st.header("🏠 Site Health & Warming Alerts")
    try:
        query = f"""
            WITH NodeLimits AS (
                SELECT sensor_id, MAX(timestamp) as max_ts 
                FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master`
                WHERE project IS NOT NULL GROUP BY sensor_id
            )
            SELECT m.timestamp, m.temperature, m.location, m.depth, m.sensor_id, m.sensor_name
            FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` m
            JOIN NodeLimits nl ON m.sensor_id = nl.sensor_id
            WHERE m.timestamp >= TIMESTAMP_SUB(nl.max_ts, INTERVAL 24 HOUR)
        """
        df_summary = client.query(query).to_dataframe()
        if not df_summary.empty:
            st.dataframe(df_summary, width='stretch', hide_index=True)
    except Exception as e:
        st.error(f"Summary Error: {e}")

# 4B. CLIENT PORTAL
elif service == "📊 Client Portal":
    st.header("📊 Project Status Report")
    try:
        meta_q = f"SELECT DISTINCT project, location FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE is_approved = TRUE"
        meta_df = client.query(meta_q).to_dataframe()
        
        if meta_df.empty:
            st.warning("No approved data found.")
        else:
            c1, c2 = st.columns(2)
            with c1: sel_p = st.selectbox("Project", sorted(meta_df['project'].unique()))
            with c2: sel_l = st.selectbox("Location", sorted(meta_df[meta_df['project']==sel_p]['location'].unique()))
            
            data_q = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE project='{sel_p}' AND location='{sel_l}' AND is_approved=TRUE"
            df_c = client.query(data_q).to_dataframe()
            
            if not df_c.empty:
                # Fixed regex with raw string r''
                df_c['depth_num'] = df_c['depth'].str.extract(r'(\d+)').astype(float)
                st.line_chart(df_c, x='timestamp', y='temperature')
    except Exception as e:
        st.error(f"Portal Error: {e}")

# 4D. DATA INTAKE
elif service == "📤 Data Intake Lab":
    st.header("📤 Data Ingestion & Recovery")
    tab1, tab2 = st.tabs(["📄 Manual File Upload", "📡 API Data Recovery"])
    
    with tab2:
        st.subheader("📡 SensorPush Multi-Account Recovery")
        if st.button("🛰️ RUN ALL-ACCOUNT RECOVERY"):
            try:
                # This SQL handles the long numeric IDs by casting to STRING
                sync_sql = f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` AS 
                    WITH Unified AS (
                        SELECT CAST(timestamp AS TIMESTAMP) as timestamp, CAST(value AS FLOAT64) as temperature, REPLACE(CAST(nodenumber AS STRING), ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord` 
                        UNION ALL 
                        SELECT CAST(timestamp AS TIMESTAMP) as timestamp, CAST(temperature AS FLOAT64) as temperature, REPLACE(CAST(sensor_id AS STRING), ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
                    ) 
                    SELECT u.timestamp, u.node AS sensor_id, u.temperature, m.nodenum as sensor_name, m.project, m.location, m.depth, CAST(FALSE AS BOOLEAN) as is_approved, CAST(NULL AS STRING) as engineer_note
                    FROM Unified u 
                    INNER JOIN `{PROJECT_ID}.{DATASET_ID}.master_metadata` m ON u.node = REPLACE(CAST(m.nodenum AS STRING), ':', '-')
                """
                client.query(sync_sql).result()
                st.success("✅ Sync Complete!")
                st.balloons()
            except Exception as e:
                st.error(f"Sync Error: {e}")

# 4E. ADMIN TOOLS
elif service == "🛠️ Admin Tools":
    st.header("🛠️ Admin Tools")
    if st.button("🔍 Test Metadata Access"):
        try:
            # If Step 1 (Scopes) is done, this will finally work
            test_q = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.master_metadata` LIMIT 5"
            test_df = client.query(test_q).to_dataframe()
            st.success("✅ Connection Successful! Metadata is visible.")
            st.dataframe(test_df)
        except Exception as e:
            st.error(f"❌ Diagnostic failed: {e}")
