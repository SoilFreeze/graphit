import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz
import math

st.set_page_config(layout="wide", page_title="SF Technician Dashboard")

# =================================================================
# 1. AUTHENTICATION (Creates 'creds' for everyone else)
# =================================================================
if "gcp_service_account" in st.secrets:
    info = st.secrets["gcp_service_account"]
    creds = service_account.Credentials.from_service_account_info(
        info, 
        scopes=["https://www.googleapis.com/auth/drive.readonly", 
                "https://www.googleapis.com/auth/bigquery"]
    )
    client = bigquery.Client(credentials=creds, project=info["project_id"])
else:
    st.error("Credential Error: Please check Streamlit Secrets.")
    st.stop()

# =================================================================
# 2. THEME LOADER (Pulls your JSON from Google Drive)
# =================================================================
@st.cache_data(ttl=3600)
def load_remote_theme(_credentials):
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    import io, json
    try:
        service = build('drive', 'v3', credentials=_credentials)
        # Your verified File ID
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
        st.sidebar.warning(f"Theme Load Failed: Using Defaults. Error: {e}")
        return None

SF_THEME = load_remote_theme(creds)

# =================================================================
# 3. IMPORT SHARED UTILS (Requires sf_utils.py in GitHub repo)
# =================================================================
try:
    from sf_utils import get_standard_24h_summary, apply_standard_chart_style
except ImportError:
    st.error("Error: 'sf_utils.py' not found in repository.")
    st.stop()

# =================================================================
# 4. DATA FETCHING (This defines 'full_df')
# =================================================================
# --- SECTION 4: DATA FETCHING ---
@st.cache_data(ttl=300)
def fetch_tech_data():
    query = """
    SELECT d.timestamp, d.value, d.nodenumber, m.Project, m.Location, m.Depth
    FROM `sensorpush-export.sensor_data.final_databoard_data` as d
    INNER JOIN `sensorpush-export.sensor_data.master_metadata` as m ON d.nodenumber = m.NodeNum
    ORDER BY d.timestamp ASC
    """
    df = client.query(query).to_dataframe()
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df

# 💡 ADD THIS LINE HERE (Flush to the left margin)
full_df = fetch_tech_data()

# =================================================================
# 5. PAGE SETUP & SIDEBAR
# =================================================================
st.set_page_config(layout="wide", page_title="SF Technician Dashboard")
st.sidebar.title("🛠️ Tech Operations")

unit = st.sidebar.radio("Temp Unit", ["Fahrenheit (°F)", "Celsius (°C)"])
is_celsius = unit == "Celsius (°C)"

# Now full_df is defined, so this line won't crash:
all_projs = sorted(full_df['Project'].unique())
sel_proj = st.sidebar.selectbox("1. Select Project", all_projs)
df_proj = full_df[full_df['Project'] == sel_proj].copy()

if is_celsius:
    df_proj['value'] = (df_proj['value'] - 32) * 5/9

num_weeks = st.sidebar.slider("Weeks of History", 1, 12, 4)

# Date Logic
now_utc = datetime.now(tz=pytz.UTC)
days_to_mon = (7 - now_utc.weekday()) % 7
if days_to_mon == 0: days_to_mon = 7
graph_end = (now_utc + timedelta(days=days_to_mon)).replace(hour=0, minute=0, second=0)
graph_start = graph_end - timedelta(weeks=num_weeks)

# =================================================================
# 6. UI TABS
# =================================================================
tab_health, tab_depth, tab_time = st.tabs(["📡 System Health", "📏 Depth Profiles", "📈 Time History"])

# --- TAB 1: SYSTEM HEALTH ---
with tab_health:
    st.subheader(f"📋 24-Hour Performance: {sel_proj}")
    perf_table = get_standard_24h_summary(df_proj, SF_THEME)
    if perf_table is not None:
        st.table(perf_table)
    else:
        st.info("No data recorded in the last 24 hours.")

    st.divider()
    st.subheader("⚠️ Connectivity (Last 24h)")
    cutoff_24 = now_utc - timedelta(hours=24)
    active_nodes = df_proj[df_proj['timestamp'] >= cutoff_24]['nodenumber'].unique()
    expected = df_proj[['Location', 'Depth', 'nodenumber']].drop_duplicates()
    offline = expected[~expected['nodenumber'].isin(active_nodes)]
    
    if not offline.empty:
        st.warning(f"{len(offline)} Sensors Offline")
        st.dataframe(offline[['Location', 'Depth', 'nodenumber']], hide_index=True)
    else:
        st.success("All project sensors are reporting.")

# --- TAB 2: DEPTH PROFILES ---
with tab_depth:
    pipe_locs = [l for l in sorted(df_proj['Location'].unique()) if "bank" not in l.lower()]
    if pipe_locs:
        sel_pipe = st.selectbox("Select Pipe for Profile", pipe_locs)
        pipe_df = df_proj[df_proj['Location'] == sel_pipe].copy()
        
        # History Logic
        mondays = [graph_end - timedelta(weeks=i) for i in range(num_weeks)]
        all_snaps = []
        for m in mondays:
            t_time = m.replace(hour=6, minute=0)
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
            
            # Dynamic Depth Rounding
            max_d = plot_df['Depth'].max()
            rounded_max = int(math.ceil(max_d / 10.0) * 10)
            if rounded_max == max_d: rounded_max += 10

            fig_prof = px.line(plot_df, x='value', y='Depth', color='Date', markers=True)
            fig_prof = apply_standard_chart_style(fig_prof, SF_THEME, is_profile=True)
            
            # Y-Axis Customization (Ground Surface)
            fig_prof.update_yaxes(
                range=[rounded_max, 0],
                tickvals=list(range(0, rounded_max + 1, 10)),
                ticktext=[SF_THEME['chart_theme']['labels']['y_axis_zero']] + [str(i) for i in range(10, rounded_max + 1, 10)]
            )
            st.plotly_chart(fig_prof, width='stretch')
    else:
        st.info("No 'Pipe' locations found (Banks are excluded from Profiles).")

# --- TAB 3: TIME HISTORY ---
with tab_time:
    all_locs = sorted(df_proj['Location'].unique())
    if all_locs:
        sel_loc_time = st.selectbox("Select Location (Pipes or Banks)", all_locs)
        time_df = df_proj[df_proj['Location'] == sel_loc_time].copy()
        fig_time = px.line(time_df, x='timestamp', y='value', color='Depth')
        
        fig_time = apply_standard_chart_style(fig_time, SF_THEME, is_profile=False)
        fig_time.update_xaxes(range=[graph_start, graph_end])
        
        st.plotly_chart(fig_time, width='stretch')
