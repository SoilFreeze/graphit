import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz
import math

# --- 1. CONFIG (Forced Sidebar) ---
st.set_page_config(
    layout="wide", 
    page_title="SF TEST ENVIRONMENT",
    initial_sidebar_state="expanded"
)

# --- 2. AUTHENTICATION ---
if "gcp_service_account" in st.secrets:
    info = st.secrets["gcp_service_account"]
    scopes = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive.readonly"]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    client = bigquery.Client(credentials=creds, project=info["project_id"])
else:
    st.error("Secrets missing in Test App.")
    st.stop()

# --- 3. THEME & UTILS (Simplified for Debugging) ---
@st.cache_data(ttl=60)
def load_sf_theme(_credentials):
    from googleapiclient.discovery import build
    import io, json
    from googleapiclient.http import MediaIoBaseDownload
    try:
        service = build('drive', 'v3', credentials=_credentials)
        file_id = '18_DQ72HQ1HGaRGjkTUI7PDoIvzp2CqDy'
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done: _, done = downloader.next_chunk()
        fh.seek(0)
        return json.load(fh)
    except Exception as e:
        st.sidebar.warning(f"Theme Load Failed: {e}")
        return None

SF_THEME = load_sf_theme(creds)

# Import utils - Ensure sf_utils.py is also on the 'dev' branch!
try:
    from sf_utils import get_standard_24h_summary, apply_standard_chart_style
except ImportError:
    st.error("Missing sf_utils.py on dev branch.")
    st.stop()

# --- 4. DATA FETCH ---
@st.cache_data(ttl=300)
def fetch_data():
    query = """
    SELECT d.timestamp, d.value, d.nodenumber, m.Project, m.Location, m.Depth
    FROM `sensorpush-export.sensor_data.final_databoard_data` as d
    INNER JOIN `sensorpush-export.sensor_data.master_metadata` as m ON d.nodenumber = m.NodeNum
    ORDER BY d.timestamp ASC
    """
    return client.query(query).to_dataframe()

df_raw = fetch_data()
df_raw['timestamp'] = pd.to_datetime(df_raw['timestamp'], utc=True)

# --- 5. SIDEBAR CONTROLS ---
st.sidebar.title("🧪 Test Controls")
unit = st.sidebar.radio("Temp Unit", ["Fahrenheit (°F)", "Celsius (°C)"])
is_celsius = unit == "Celsius (°C)"

all_projs = sorted(df_raw['Project'].unique().tolist())
sel_proj = st.sidebar.selectbox("Select Project", all_projs)
df_proj = df_raw[df_raw['Project'] == sel_proj].copy()

if is_celsius:
    df_proj['value'] = (df_proj['value'] - 32) * 5/9

num_weeks = st.sidebar.slider("Weeks of History", 1, 12, 4)

# --- 6. MAIN CONTENT ---
st.title("🧪 SF Test Environment")
st.success(f"Connected: {len(df_proj)} rows loaded for Project {sel_proj}")

tab_health, tab_history, tab_depth = st.tabs(["📡 System Health", "📈 Time History", "📏 Depth Profiles"])

with tab_health:
    st.subheader("24-Hour Performance")
    perf_table = get_standard_24h_summary(df_proj, SF_THEME)
    if perf_table is not None:
        st.dataframe(perf_table, use_container_width=True, hide_index=True)

with tab_history:
    locs = sorted(df_proj['Location'].unique())
    sel_loc = st.selectbox("Select Location", locs, key="hist_loc")
    # Downsample by 10 to keep the test app fast
    time_df = df_proj[df_proj['Location'] == sel_loc].iloc[::10]
    
    fig_time = px.line(time_df, x='timestamp', y='value', color='Depth')
    if SF_THEME:
        fig_time = apply_standard_chart_style(fig_time, SF_THEME, is_profile=False)
    st.plotly_chart(fig_time, use_container_width=True)

with tab_depth:
    pipes = [l for l in sorted(df_proj['Location'].unique()) if "bank" not in l.lower()]
    if pipes:
        sel_pipe = st.selectbox("Select Pipe", pipes, key="depth_loc")
        pipe_df = df_proj[df_proj['Location'] == sel_pipe].copy()
        
        # Simple Snapshot Logic for Testing
        now = datetime.now(tz=pytz.UTC)
        mondays = [(now - timedelta(days=now.weekday()) - timedelta(weeks=i)).replace(hour=6, minute=0) for i in range(num_weeks)]
        
        snaps = []
        for m in mondays:
            s = pipe_df[(pipe_df['timestamp'] >= m - timedelta(hours=6)) & (pipe_df['timestamp'] <= m + timedelta(hours=6))].copy()
            if not s.empty:
                s['Date'] = m.strftime('%b %d')
                snaps.append(s.sort_values('timestamp').groupby('Depth').head(1))
        
        if snaps:
            plot_df = pd.concat(snaps)
            fig_prof = px.line(plot_df.sort_values('Depth'), x='value', y='Depth', color='Date', markers=True)
            fig_prof.update_yaxes(autorange="reversed")
            st.plotly_chart(fig_prof, use_container_width=True)
