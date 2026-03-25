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

# --- 2. THE MALTBY GRAPH ENGINE ---
def build_standard_sf_graph(df, title, start_view, end_view):
    """Restored: 6hr gaps, custom grid, precision locking, and 'Right Now' red line."""
    display_df = df.copy()
    
    # Gap Logic (Line breaks > 6hrs)
    processed_dfs = []
    for d in display_df['depth'].unique():
        s_df = display_df[display_df['depth'] == d].copy().sort_values('timestamp')
        s_df['gap'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
        gaps = s_df[s_df['gap'] > 6.0].copy()
        if not gaps.empty:
            gaps['temperature'] = None  # Force a break in the line
            gaps['timestamp'] = gaps['timestamp'] - timedelta(minutes=1)
            s_df = pd.concat([s_df, gaps]).sort_values('timestamp')
        processed_dfs.append(s_df)
    
    clean_df = pd.concat(processed_dfs) if processed_dfs else display_df
    
    fig = go.Figure()
    # Sort depths numerically for the legend
    depths = sorted(clean_df['depth'].unique(), key=lambda x: int(''.join(filter(str.isdigit, x)) or 0))
    
    for d in depths:
        sensor_df = clean_df[clean_df['depth'] == d]
        fig.add_trace(go.Scatter(
            x=sensor_df['timestamp'], 
            y=sensor_df['temperature'].round(1),
            name=d, 
            mode='lines', 
            connectgaps=False, 
            line=dict(width=2.5),
            hovertemplate='%{x}<br>Temp: %{y:.1f}°F'
        ))

    # Grid & Axis Styling
    fig.update_yaxes(
        gridcolor='DimGray', gridwidth=1, 
        minor=dict(dtick=5, gridcolor='Silver', showgrid=True),
        mirror=True, showline=True, linecolor='black', linewidth=2,
        title="Temperature (°F)"
    )
    fig.update_xaxes(
        showgrid=False, range=[start_view, end_view], 
        mirror=True, showline=True, linecolor='black', linewidth=2,
        title="Time (UTC)"
    )

    # RIGHT NOW Line
    now_ts = datetime.now(pytz.UTC)
    fig.add_vline(x=now_ts, line_width=2, line_color="red", annotation_text="RIGHT NOW")
    
    # Freeze Reference
    fig.add_hline(y=32, line_dash="dash", line_color="cyan", annotation_text="32°F")

    fig.update_layout(
        title={'text': title, 'x': 0.5, 'font': {'size': 24}}, 
        plot_bgcolor='white',
        hovermode="x unified",
        legend=dict(x=1.02, y=1, bordercolor="Black", borderwidth=1), 
        margin=dict(r=150, t=80), 
        height=800
    )
    return fig

# --- 3. SIDEBAR NAVIGATION ---
st.sidebar.title("❄️ SoilFreeze Lab")
service = st.sidebar.selectbox("Select Service", [
    "🏠 Executive Summary", 
    "📉 Node Diagnostics", 
    "📤 Data Intake Lab"
])

# --- 4. SERVICE ROUTING ---

if service == "🏠 Executive Summary":
    st.header("🏠 Site Health & Warming Alerts")
    try:
        # Fetch available projects
        proj_q = f"SELECT DISTINCT project FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE project IS NOT NULL"
        meta_df = client.query(proj_q).to_dataframe()
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

        if not df_summary.empty:
            now_ts = datetime.now(pytz.UTC)
            summary_stats = []
            for node in df_summary['sensor_id'].unique():
                n_df = df_summary[df_summary['sensor_id'] == node].sort_values('timestamp')
                curr_temp = n_df['temperature'].iloc[-1]
                change = curr_temp - n_df['temperature'].iloc[0]
                hours_ago = (now_ts - n_df['timestamp'].iloc[-1]).total_seconds() / 3600
                
                summary_stats.append({
                    "Location": n_df['location'].iloc[0],
                    "Depth": f"{n_df['depth'].iloc[0]}ft",
                    "Node ID": node,
                    "Status / Last Seen": f"{n_df['timestamp'].iloc[-1].strftime('%m/%d %H:%M')} ({int(round(hours_ago, 0))}h ago)",
                    "hours_raw": hours_ago,
                    "Min (24h)": round(float(n_df['temperature'].min()), 1),
                    "Max (24h)": round(float(n_df['temperature'].max()), 1),
                    "24h Change": round(float(change), 1),
                    "Current": round(float(curr_temp), 1)
                })
            
            df_full = pd.DataFrame(summary_stats).sort_values(by="24h Change", ascending=False)
            st.dataframe(df_full.drop(columns=['hours_raw']), use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Executive Summary Error: {e}")

elif service == "📉 Node Diagnostics":
    st.header("📉 High-Resolution Node Diagnostics")
    try:
        # Project/Location selector
        meta_q = f"SELECT DISTINCT project, location FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE project IS NOT NULL"
        meta_df = client.query(meta_q).to_dataframe()

        c1, c2, c3 = st.columns(3)
        with c1: sel_proj = st.selectbox("Project", sorted(meta_df['project'].unique()))
        with c2: 
            locs = sorted(meta_df[meta_df['project'] == sel_proj]['location'].unique())
            sel_loc = st.selectbox("Pipe / Bank", locs)
        with c3: weeks = st.slider("Lookback (Weeks)", 1, 12, 4)

        # Query Data (using fixed DAY interval)
        days_back = weeks * 7
        data_q = f"""
            SELECT timestamp, temperature, depth, sensor_name 
            FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master`
            WHERE project = '{sel_proj}' AND location = '{sel_loc}'
            AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
            ORDER BY timestamp ASC
        """
        df_g = client.query(data_q).to_dataframe()

        if not df_g.empty:
            df_g['timestamp'] = pd.to_datetime(df_g['timestamp'])
            end_view = datetime.now(pytz.UTC)
            start_view = end_view - timedelta(days=days_back)
            
            # CALLING THE ENGINE
            fig = build_standard_sf_graph(df_g, f"Thermal Trend: {sel_proj} | {sel_loc}", start_view, end_view)
            st.plotly_chart(fig, use_container_width=True)
            
            st.subheader("Latest Readings")
            latest = df_g.sort_values('timestamp').groupby('depth').tail(1)
            st.dataframe(latest[['depth', 'sensor_name', 'temperature']].style.format({'temperature': '{:.1f}'}), use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Diagnostics Error: {e}")

elif service == "📤 Data Intake Lab":
    st.header("📤 Data Ingestion & Recovery")
    st.info("Manual Upload and API Recovery tools are available here.")
    # [Insert Data Intake logic here as needed]
