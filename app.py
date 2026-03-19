import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz

# 1. PAGE CONFIG (Must be first)
st.set_page_config(layout="wide", page_title="SF Technician Dashboard")

# 2. CSS TO UNLOCK SCROLLING
st.markdown("""<style>.main .block-container {overflow-y: auto !important; height: auto !important;}</style>""", unsafe_allow_html=True)

# 3. AUTHENTICATION
if "gcp_service_account" in st.secrets:
    info = st.secrets["gcp_service_account"]
    creds = service_account.Credentials.from_service_account_info(info)
    client = bigquery.Client(credentials=creds, project=info["project_id"])
else:
    st.stop()

# 4. CACHED THEME LOAD (Prevents Rerun Loops)
@st.cache_data(ttl=3600)
def load_sf_theme(_creds):
    # If this causes a freeze, it will only try once per hour
    return None # Start with None to see if the page unfreezes

SF_THEME = load_sf_theme(creds)

# 5. CACHED DATA FETCH
@st.cache_data(ttl=300)
def get_data():
    query = "SELECT * FROM `sensorpush-export.sensor_data.final_databoard_data` LIMIT 1000"
    return client.query(query).to_dataframe()

full_df = get_data()

# 6. SIMPLE UI (To test responsiveness)
st.title("🛠️ Tech Operations")
st.write("If you can see this and scroll, the freeze is fixed.")
st.dataframe(full_df, use_container_width=True, height=400)
# =================================================================
# 2. AUTHENTICATION (The "Engine Start")
# =================================================================
if "gcp_service_account" in st.secrets:
    info = st.secrets["gcp_service_account"]
    creds = service_account.Credentials.from_service_account_info(
        info, 
        scopes=["https://www.googleapis.com/auth/drive.readonly", "https://www.googleapis.com/auth/bigquery"]
    )
    client = bigquery.Client(credentials=creds, project=info["project_id"])
else:
    st.error("Credential Error: Please check Streamlit Secrets.")
    st.stop()

# =================================================================
# 3. THEME & UTILS (The "Standardized Brain")
# =================================================================
@st.cache_data(ttl=3600)
def load_remote_theme(_credentials):
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
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        return json.load(fh)
    except:
        return None

SF_THEME = load_remote_theme(creds)

# Ensure sf_utils.py is in your GitHub repo!
from sf_utils import get_standard_24h_summary, apply_standard_chart_style

# =================================================================
# 4. DATA FETCHING
# =================================================================
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

full_df = fetch_tech_data()

# =================================================================
# 5. SIDEBAR & DATE LOGIC
# =================================================================
st.sidebar.title("🛠️ Tech Operations")
unit = st.sidebar.radio("Temp Unit", ["Fahrenheit (°F)", "Celsius (°C)"])
is_celsius = unit == "Celsius (°C)"

all_projs = sorted(full_df['Project'].unique().tolist())
sel_proj = st.sidebar.selectbox("1. Select Project", all_projs)
df_proj = full_df[full_df['Project'] == sel_proj].copy()

if is_celsius:
    df_proj['value'] = (df_proj['value'] - 32) * 5/9

num_weeks = st.sidebar.slider("Weeks of History", 1, 12, 4)

# Monday-to-Monday Snapshot Logic
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
    # This function colors the table based on the JSON theme
    perf_table = get_standard_24h_summary(df_proj, SF_THEME)
    if perf_table is not None:
        st.dataframe(perf_table, use_container_width=True, hide_index=True, height=450)

    st.divider()
    st.subheader("⚠️ Connectivity (Last 24h)")
    cutoff_24 = now_utc - timedelta(hours=24)
    active = df_proj[df_proj['timestamp'] >= cutoff_24]['nodenumber'].unique()
    offline = df_proj[['Location', 'Depth', 'nodenumber']].drop_duplicates()
    offline = offline[~offline['nodenumber'].isin(active)]
    
    if not offline.empty:
        st.warning(f"{len(offline)} Sensors Offline")
        st.dataframe(offline[['Location', 'Depth', 'nodenumber']], hide_index=True)
    else:
        st.success("All sensors reporting.")

# --- TAB 2: DEPTH PROFILES ---
with tab_depth:
    pipe_locs = [l for l in sorted(df_proj['Location'].unique()) if "bank" not in l.lower()]
    if pipe_locs:
        sel_pipe = st.selectbox("Select Pipe", pipe_locs)
        pipe_df = df_proj[df_proj['Location'] == sel_pipe].copy()
        
        # Snapshot Logic for Weekly Lines
        mondays = [graph_end - timedelta(weeks=i) for i in range(num_weeks)]
        all_snaps = []
        for m in mondays:
            t_target = m.replace(hour=6, minute=0)
            snap = pipe_df[(pipe_df['timestamp'] >= t_target - timedelta(hours=3)) & 
                           (pipe_df['timestamp'] <= t_target + timedelta(hours=3))].copy()
            if not snap.empty:
                snap['diff'] = (snap['timestamp'] - t_target).abs()
                best = snap.sort_values('diff').head(len(snap['Depth'].unique()))
                best['Date'] = t_target.strftime('%b %d')
                all_snaps.append(best)

        if all_snaps:
            plot_df = pd.concat(all_snaps)
            plot_df['Depth'] = pd.to_numeric(plot_df['Depth'], errors='coerce')
            fig_prof = px.line(plot_df.sort_values('Depth'), x='value', y='Depth', color='Date', markers=True)
            
            # Apply Brand Styling
            fig_prof = apply_standard_chart_style(fig_prof, SF_THEME, is_profile=True)
            st.plotly_chart(fig_prof, use_container_width=True)

# --- TAB 3: TIME HISTORY ---
with tab_time:
    locs = sorted(df_proj['Location'].unique())
    sel_loc = st.selectbox("Select Location", locs)
    # Downsample slightly (every 3rd point) to keep the browser fast
    time_df = df_proj[df_proj['Location'] == sel_loc].iloc[::3]
    
    fig_time = px.line(time_df, x='timestamp', y='value', color='Depth')
    fig_time = apply_standard_chart_style(fig_time, SF_THEME, is_profile=False)
    fig_time.update_xaxes(range=[graph_start, graph_end])
    st.plotly_chart(fig_time, use_container_width=True)
