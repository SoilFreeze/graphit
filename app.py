import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz
import math
import os

# =================================================================
# 1. PAGE SETUP (Must be the very first Streamlit command)
# =================================================================
st.set_page_config(layout="wide", page_title="SF Technician Dashboard")

# =================================================================
# 2. AUTHENTICATION & API CONNECTIONS
# =================================================================
if "gcp_service_account" in st.secrets:
    info = st.secrets["gcp_service_account"]
    # We define 'creds' globally so it is available for BQ and Drive
    creds = service_account.Credentials.from_service_account_info(
        info, 
        scopes=["https://www.googleapis.com/auth/drive.readonly", 
                "https://www.googleapis.com/auth/bigquery"]
    )
    client = bigquery.Client(credentials=creds, project=info["project_id"])
else:
    st.error("Credential Error: 'gcp_service_account' not found in Streamlit Secrets.")
    st.stop()

# =================================================================
# 3. THEME LOADER (Google Drive JSON)
# =================================================================
@st.cache_data(ttl=3600)
def load_remote_theme(_credentials):
    """Downloads the style JSON from Google Drive using the File ID."""
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    import io, json
    
    try:
        service = build('drive', 'v3', credentials=_credentials)
        # Verified File ID from your link
        file_id = '18_DQ72HQ1HGaRGjkTUI7PDoIvzp2CqDy' 
        
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        return json.load(fh)
    except Exception as e:
        # BACKUP THEME: If Drive fails, the app stays responsive
        return {
            "table_theme": {
                "thresholds": {"critical_warming": 5.0, "warning_warming": 2.5, "slight_warming": 1.0, "cooling": -1.0},
                "status_colors": {"offline_red": "#ff4b4b", "warning_orange": "#ffa500", "standby_yellow": "#ffff00", "healthy_green": "#90ee90"}
            }
        }

SF_THEME = load_remote_theme(creds)

# =================================================================
# 4. SHARED UTILITIES (Logic from sf_utils.py)
# =================================================================
# This ensures that math and styles are identical across all 3 SF apps
try:
    from sf_utils import get_standard_24h_summary, apply_standard_chart_style
except ImportError:
    st.error("CRITICAL ERROR: 'sf_utils.py' missing from GitHub repository.")
    st.stop()

# =================================================================
# 5. DATA FETCHING (BigQuery)
# =================================================================
@st.cache_data(ttl=300)
def fetch_tech_data():
    """Pulls raw sensor data joined with master metadata."""
    query = """
    SELECT d.timestamp, d.value, d.nodenumber, m.Project, m.Location, m.Depth
    FROM `sensorpush-export.sensor_data.final_databoard_data` as d
    INNER JOIN `sensorpush-export.sensor_data.master_metadata` as m ON d.nodenumber = m.NodeNum
    ORDER BY d.timestamp ASC
    """
    # Note: Requires 'db-dtypes' in requirements.txt
    df = client.query(query).to_dataframe()
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df

# Create the main dataframe variable
full_df = fetch_tech_data()

# =================================================================
# 6. SIDEBAR FILTERS
# =================================================================
st.sidebar.title("🛠️ Tech Operations")

unit = st.sidebar.radio("Temp Unit", ["Fahrenheit (°F)", "Celsius (°C)"])
is_celsius = unit == "Celsius (°C)"

all_projs = sorted(full_df['Project'].unique().tolist())
sel_proj = st.sidebar.selectbox("1. Select Project", all_projs)
df_proj = full_df[full_df['Project'] == sel_proj].copy()

# Apply Celsius conversion globally if selected
if is_celsius:
    df_proj['value'] = (df_proj['value'] - 32) * 5/9

num_weeks = st.sidebar.slider("Weeks of History", 1, 12, 4)

# Global Date Logic (Monday-to-Monday Windows)
now_utc = datetime.now(tz=pytz.UTC)
days_to_mon = (7 - now_utc.weekday()) % 7
if days_to_mon == 0: days_to_mon = 7
graph_end = (now_utc + timedelta(days=days_to_mon)).replace(hour=0, minute=0, second=0)
graph_start = graph_end - timedelta(weeks=num_weeks)

# =================================================================
# 7. UI TABS (The Main Display)
# =================================================================
tab_health, tab_depth, tab_time = st.tabs(["📡 System Health", "📏 Depth Profiles", "📈 Time History"])

# --- TAB 1: SYSTEM HEALTH ---
with tab_health:
    st.subheader(f"📋 24-Hour Performance: {sel_proj}")
    
    # Generate the standardized table from sf_utils
    perf_table = get_standard_24h_summary(df_proj, SF_THEME)
    
    if perf_table is not None:
        # STABILITY FIX: Use dataframe instead of table for better scrolling/responsiveness
        st.dataframe(perf_table, use_container_width=True, hide_index=True)
    else:
        st.info("No active data found in the last 24 hours.")

    st.divider()
    
    st.subheader("⚠️ Connectivity (Last 24h)")
    cutoff_24 = now_utc - timedelta(hours=24)
    active_nodes = df_proj[df_proj['timestamp'] >= cutoff_24]['nodenumber'].unique()
    expected_nodes = df_proj[['Location', 'Depth', 'nodenumber']].drop_duplicates()
    
    # Compare expected vs actual to find missing sensors
    offline = expected_nodes[~expected_nodes['nodenumber'].isin(active_nodes)]
    
    if not offline.empty:
        st.warning(f"{len(offline)} Sensors currently Offline.")
        st.dataframe(offline[['Location', 'Depth', 'nodenumber']], use_container_width=True, hide_index=True)
    else:
        st.success("All sensors for this project are reporting data.")

# --- TAB 2: DEPTH PROFILES ---
with tab_depth:
    # Exclude horizontal "Banks" from the vertical profile view
    pipe_locs = [l for l in sorted(df_proj['Location'].unique()) if "bank" not in l.lower()]
    
    if pipe_locs:
        sel_pipe = st.selectbox("Select Pipe for Profile", pipe_locs)
        pipe_df = df_proj[df_proj['Location'] == sel_pipe].copy()
        
        # Snapshot Logic: Pull Monday morning at 6:00 AM
        mondays = [graph_end - timedelta(weeks=i) for i in range(num_weeks)]
        all_snaps = []
        for m in mondays:
            t_time = m.replace(hour=6, minute=0)
            # Find the closest data point within a 6-hour window
            snap_data = pipe_df[(pipe_df['timestamp'] >= t_time - timedelta(hours=3)) & 
                                (pipe_df['timestamp'] <= t_time + timedelta(hours=3))].copy()
            if not snap_data.empty:
                snap_data['diff'] = (snap_data['timestamp'] - t_time).abs()
                best_ts = snap_data.sort_values('diff')['timestamp'].iloc[0]
                snap = snap_data[snap_data['timestamp'] == best_ts].copy()
                snap['Date'] = t_time.strftime('%b %d')
                all_snaps.append(snap)

        if all_snaps:
            plot_df = pd.concat(all_snaps)
            plot_df['Depth'] = pd.to_numeric(plot_df['Depth'], errors='coerce')
            plot_df = plot_df.dropna(subset=['Depth']).sort_values('Depth')
            
            # Auto-calculate the scale of the Y-axis based on project depth
            max_d = plot_df['Depth'].max()
            rounded_max = int(math.ceil(max_d / 10.0) * 10) + 10

            fig_prof = px.line(plot_df, x='value', y='Depth', color='Date', markers=True)
            
            # Apply the Standardized Chart Style from sf_utils
            fig_prof = apply_standard_chart_style(fig_prof, SF_THEME, is_profile=True)
            
            # Force Ground Surface at 0
            fig_prof.update_yaxes(
                range=[rounded_max, 0],
                tickvals=list(range(0, rounded_max + 1, 10)),
                ticktext=["Ground Surface"] + [str(i) for i in range(10, rounded_max + 1, 10)]
            )
            st.plotly_chart(fig_prof, use_container_width=True)
    else:
        st.info("No 'Pipe' locations found for this project.")

# --- TAB 3: TIME HISTORY ---
with tab_time:
    all_locs = sorted(df_proj['Location'].unique())
    if all_locs:
        sel_loc_time = st.selectbox("Select Location (Pipes or Banks)", all_locs)
        time_df = df_proj[df_proj['Location'] == sel_loc_time].copy()
        
        fig_time = px.line(time_df, x='timestamp', y='value', color='Depth')
        
        # Apply the Standardized Chart Style from sf_utils
        fig_time = apply_standard_chart_style(fig_time, SF_THEME, is_profile=False)
        fig_time.update_xaxes(range=[graph_start, graph_end])
        
        st.plotly_chart(fig_time, use_container_width=True)
