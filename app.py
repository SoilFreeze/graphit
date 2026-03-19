import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz
import math
import json
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# --- 1. AUTHENTICATION & THEME LOADING ---
# Ensure these match your secrets.toml or Streamlit Cloud Secrets
if "gcp_service_account" in st.secrets:
    info = st.secrets["gcp_service_account"]
    creds = service_account.Credentials.from_service_account_info(
        info, 
        scopes=["https://www.googleapis.com/auth/drive.readonly", 
                "https://www.googleapis.com/auth/bigquery"]
    )
    client = bigquery.Client(credentials=creds, project=info["project_id"])
else:
    st.error("Credential Error: Check Streamlit Secrets.")
    st.stop()

# --- 2. THEME LOADER ---
@st.cache_data(ttl=3600)
def load_remote_theme(credentials):
    from googleapiclient.discovery import build
    import io, json
    from googleapiclient.http import MediaIoBaseDownload
    
    try:
        service = build('drive', 'v3', credentials=credentials)
        # REPLACE WITH YOUR ACTUAL FILE ID FROM GOOGLE DRIVE
        file_id = 'YOUR_SF_STYLE_CONFIG_JSON_ID' 
        
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        return json.load(fh)
    except Exception as e:
        st.sidebar.warning(f"Theme Load Failed: Using Defaults. Error: {e}")
        return None # Utilities will handle the fallback

# Load the central theme
SF_THEME = load_remote_theme(creds)

# --- 3. IMPORT SHARED LOGIC ---
# This pulls the tools from the sf_utils.py file you just created
from sf_utils import get_standard_24h_summary, apply_standard_chart_style
@st.cache_data(ttl=3600) # Check for style updates once per hour
def load_remote_theme(credentials):
    try:
        service = build('drive', 'v3', credentials=credentials)
        # You'll need the File ID of your theme_config.json from Google Drive
        file_id = 'theme_config.json' 
        
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        
        fh.seek(0)
        return json.load(fh)
    except Exception as e:
        # Fallback styles if the file can't be reached
        return {
            "frame_color": "black", "frame_width": 2, 
            "major_grid_color": "black", "chart_height": 850
        }

# --- 1. SETUP ---
st.set_page_config(page_title="SoilFreeze Tech Ops", layout="wide", page_icon="🛠️")

# --- 2. DATA FETCHING ---
@st.cache_data(ttl=300)
def get_tech_data():
    info = st.secrets["gcp_service_account"]
    scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform"]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    client = bigquery.Client(credentials=creds, project=info["project_id"])
    
    # Technician sees everything (no WHERE pid filter here)
    query = """
    SELECT d.timestamp, d.value, d.nodenumber, m.Project, m.Location, m.Depth
    FROM `sensorpush-export.sensor_data.final_databoard_data` as d
    INNER JOIN `sensorpush-export.sensor_data.master_metadata` as m ON d.nodenumber = m.NodeNum
    WHERE d.is_approved = TRUE
    ORDER BY d.timestamp ASC
    """
    df = client.query(query).to_dataframe()
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df

df_all = get_tech_data()

# --- 3. SIDEBAR CONTROLS ---
st.sidebar.title("🛠️ Tech Operations")
unit = st.sidebar.radio("Unit", ["Fahrenheit (°F)", "Celsius (°C)"])
is_celsius = unit == "Celsius (°C)"
u_sym = "°C" if is_celsius else "°F"

# Project Selection
all_projects = sorted(df_all['Project'].unique())
selected_project = st.sidebar.selectbox("Active Project", all_projects)
df_proj = df_all[df_all['Project'] == selected_project].copy()

# Global Ref Points (Converted if necessary)
ref_32 = 0.0 if is_celsius else 32.0
ref_26 = (26.6 - 32) * 5/9 if is_celsius else 26.6
ref_10 = (10.2 - 32) * 5/9 if is_celsius else 10.2

if is_celsius:
    df_proj['value'] = (df_proj['value'] - 32) * 5/9

num_weeks = st.sidebar.slider("History (Weeks)", 1, 12, 4)

# Monday-to-Monday Logic
now_utc = datetime.now(tz=pytz.UTC)
days_to_mon = (7 - now_utc.weekday()) % 7
if days_to_mon == 0: days_to_mon = 7
end_v = (now_utc + timedelta(days=days_to_mon)).replace(hour=0, minute=0, second=0)
start_v = end_v - timedelta(weeks=num_weeks)

# --- 4. TABS ---
tab1, tab2, tab3 = st.tabs(["📡 Offline Alerts", "📏 Pipe Profiles", "📈 Time History"])

# --- TAB 1: SYSTEM HEALTH ---
with tab1:
    st.subheader("⚠️ Offline Sensor Report (Last 24h)")
    cutoff_24 = now_utc - timedelta(hours=24)
    active_now = df_proj[df_proj['timestamp'] >= cutoff_24]
    
    # Compare project sensors to active sensors
    all_sensors = df_proj[['Location', 'Depth', 'nodenumber']].drop_duplicates()
    active_nodes = active_now['nodenumber'].unique()
    offline = all_sensors[~all_sensors['nodenumber'].isin(active_nodes)]
    
    if not offline.empty:
        st.warning(f"Found {len(offline)} sensors not reporting.")
        st.table(offline[['Location', 'Depth']].sort_values(['Location', 'Depth']))
    else:
        st.success("All sensors for this project are online.")

# --- TAB 2: PIPE PROFILES (NON-BANK ONLY) ---
with tab2:
    # Filter out locations containing "Bank"
    pipe_locs = [l for l in sorted(df_proj['Location'].unique()) if "bank" not in l.lower()]
    sel_pipe = st.selectbox("Select Pipe", pipe_locs)
    
    pipe_df = df_proj[df_proj['Location'] == sel_pipe].copy()
    
    # Failsafe Snapshot Logic for History
    mondays = [end_v - timedelta(weeks=i) for i in range(num_weeks)]
    all_snaps = []
    for m in mondays:
        t_time = m.replace(hour=6, minute=0)
        day_data = pipe_df[(pipe_df['timestamp'] >= t_time - timedelta(hours=3)) & 
                           (pipe_df['timestamp'] <= t_time + timedelta(hours=3))].copy()
        if not day_data.empty:
            day_data['diff'] = (day_data['timestamp'] - t_time).abs()
            best_t = day_data.sort_values('diff')['timestamp'].iloc[0]
            snap = day_data[day_data['timestamp'] == best_t].copy()
            snap['Date'] = t_time.strftime('%b %d')
            all_snaps.append(snap)

    if all_snaps:
        plot_df = pd.concat(all_snaps)
        plot_df['Depth'] = pd.to_numeric(plot_df['Depth'], errors='coerce')
        plot_df = plot_df.dropna(subset=['Depth']).sort_values('Depth')
        
        # Max Depth Calculation
        m_depth = plot_df['Depth'].max()
        r_max = int(math.ceil(m_depth / 10.0) * 10)
        if r_max == m_depth: r_max += 10
        
        fig_prof = px.line(plot_df, x='value', y='Depth', color='Date', markers=True, height=800)
        
        # Grid & Frame
        fig_prof.update_yaxes(range=[r_max, 0], tickvals=list(range(0, r_max+1, 10)), 
                              ticktext=["Ground Surface"] + [str(i) for i in range(10, r_max+1, 10)],
                              mirror=True, showline=True, linecolor='black', gridcolor='black')
        
        fig_prof.update_xaxes(range=[-20, 80] if not is_celsius else [-30, 30],
                              tickvals=[-20, 0, 20, 40, 60, 80] if not is_celsius else [-30, -20, -10, 0, 10, 20, 30],
                              mirror=True, showline=True, linecolor='black', gridcolor='black')
        
        # Baseline References
        for val, col, name in [(ref_32, 'blue', '32°F'), (ref_26, 'blue', '26.6°F'), (ref_10, 'red', '10.2°F')]:
            fig_prof.add_vline(x=val, line_dash="dash", line_color=col, annotation_text=name)

        fig_prof.update_layout(plot_bgcolor='white', title=f"Thermal Profile: {sel_pipe}", hovermode="y unified")
        st.plotly_chart(fig_prof, width='stretch', key="tech_profile")

# --- TAB 3: TIME HISTORY (INCLUDES BANKS) ---
with tab3:
    all_locs = sorted(df_proj['Location'].unique())
    sel_loc_time = st.selectbox("Select Location (Pipes or Banks)", all_locs)
    
    time_df = df_proj[df_proj['Location'] == sel_loc_time].copy()
    
    fig_time = px.line(time_df, x='timestamp', y='value', color='Depth', height=600)
    
    # Reference lines for Time History
    for val, col, name in [(ref_32, 'blue', '32°F'), (ref_26, 'blue', '26.6°F'), (ref_10, 'red', '10.2°F')]:
        fig_time.add_hline(y=val, line_dash="dash", line_color=col, annotation_text=name)

    fig_time.update_layout(plot_bgcolor='white', hovermode="x unified", title=f"History: {sel_loc_time}")
    fig_time.update_xaxes(range=[start_v, end_v], mirror=True, showline=True, linecolor='black')
    fig_time.update_yaxes(mirror=True, showline=True, linecolor='black', gridcolor='black', gridwidth=0.5)
    
    st.plotly_chart(fig_time, width='stretch', key="tech_time")
