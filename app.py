import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz

# =================================================================
# 1. AUTHENTICATION FIRST (This creates 'creds')
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
# 2. DEFINE THE THEME LOADER FUNCTION
# =================================================================
@st.cache_data(ttl=3600)
def load_remote_theme(_credentials):
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    import io, json
    try:
        service = build('drive', 'v3', credentials=_credentials)
        # YOUR ACTUAL FILE ID FROM GOOGLE DRIVE
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
        st.sidebar.warning(f"Using default style. Error: {e}")
        return None

# =================================================================
# 3. NOW CALL THE THEME (Now 'creds' is defined!)
# =================================================================
SF_THEME = load_remote_theme(creds)

# =================================================================
# 4. IMPORT UTILS AND SETUP PAGE
# =================================================================
from sf_utils import get_standard_24h_summary, apply_standard_chart_style

st.set_page_config(layout="wide", page_title="SF Project Dashboard")
# =================================================================
# SECTION 2: AUTHENTICATION & CORE HEADER
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

# --- THEME LOADER (With the _credentials fix) ---
@st.cache_data(ttl=3600)
def load_remote_theme(_credentials):
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    import io, json
    try:
        service = build('drive', 'v3', credentials=_credentials)
        request = service.files().get_media(fileId=GOOGLE_DRIVE_THEME_ID)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        return json.load(fh)
    except Exception as e:
        st.sidebar.warning(f"Using default style. Error: {e}")
        return None

SF_THEME = load_remote_theme(creds)

# --- IMPORT SHARED UTILS (Ensure sf_utils.py is in your repo) ---
from sf_utils import get_standard_24h_summary, apply_standard_chart_style

# =================================================================
# SECTION 3: DATA FETCHING
# =================================================================
@st.cache_data(ttl=300)
def fetch_tech_data():
    # Pulls everything for the tech view
    query = f"""
    SELECT d.timestamp, d.value, d.nodenumber, m.Project, m.Location, m.Depth
    FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.final_databoard_data` as d
    INNER JOIN `{BQ_PROJECT_ID}.{BQ_DATASET}.master_metadata` as m ON d.nodenumber = m.NodeNum
    ORDER BY d.timestamp ASC
    """
    df = client.query(query).to_dataframe()
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df

df_all = fetch_tech_data()

# =================================================================
# SECTION 4: SIDEBAR & GLOBAL FILTERS
# =================================================================
st.sidebar.title("🛠️ Tech Operations")
unit = st.sidebar.radio("Display Unit", ["Fahrenheit (°F)", "Celsius (°C)"])
is_celsius = unit == "Celsius (°C)"

all_projects = sorted(df_all['Project'].unique())
selected_project = st.sidebar.selectbox("Select Project", all_projects)
df_proj = df_all[df_all['Project'] == selected_project].copy()

if is_celsius:
    df_proj['value'] = (df_proj['value'] - 32) * 5/9

num_weeks = st.sidebar.slider("History (Weeks)", 1, 12, 4)

# Standard Monday-to-Monday logic
now_utc = datetime.now(tz=pytz.UTC)
days_to_mon = (7 - now_utc.weekday()) % 7
if days_to_mon == 0: days_to_mon = 7
end_v = (now_utc + timedelta(days=days_to_mon)).replace(hour=0, minute=0, second=0)
start_v = end_v - timedelta(weeks=num_weeks)

# =================================================================
# SECTION 5: UI TABS
# =================================================================
tab1, tab2, tab3 = st.tabs(["📡 Offline Alerts", "📏 Pipe Profiles", "📈 Time History"])

# --- TAB 1: SYSTEM HEALTH ---
with tab1:
    st.subheader("📋 24-Hour Performance Table")
    # Using the standard logic from sf_utils
    perf_table = get_standard_24h_summary(df_proj, SF_THEME)
    if perf_table:
        st.table(perf_table)
    else:
        st.info("No data in last 24h.")

    st.divider()
    st.subheader("⚠️ Connectivity Issues")
    cutoff_24 = now_utc - timedelta(hours=24)
    active_now = df_proj[df_proj['timestamp'] >= cutoff_24]['nodenumber'].unique()
    all_assigned = df_proj[['Location', 'Depth', 'nodenumber']].drop_duplicates()
    offline = all_assigned[~all_assigned['nodenumber'].isin(active_now)]
    
    if not offline.empty:
        st.warning(f"{len(offline)} sensors offline.")
        st.dataframe(offline[['Location', 'Depth', 'nodenumber']], hide_index=True)
    else:
        st.success("All sensors online.")

# --- TAB 2: PIPE PROFILES ---
with tab2:
    pipe_locs = [l for l in sorted(df_proj['Location'].unique()) if "bank" not in l.lower()]
    sel_pipe = st.selectbox("Select Pipe", pipe_locs)
    
    # ... (Insert Profile logic here, then apply style) ...
    # fig_prof = px.line(...)
    # fig_prof = apply_standard_chart_style(fig_prof, SF_THEME, is_profile=True)
    st.info("Select a pipe to view the thermal profile.")

# --- TAB 3: TIME HISTORY ---
with tab3:
    sel_loc_time = st.selectbox("Select Location (Pipes or Banks)", sorted(df_proj['Location'].unique()))
    time_df = df_proj[df_proj['Location'] == sel_loc_time].copy()
    
    fig_time = px.line(time_df, x='timestamp', y='value', color='Depth')
    # Apply standard brand style
    fig_time = apply_standard_chart_style(fig_time, SF_THEME, is_profile=False)
    
    st.plotly_chart(fig_time, width='stretch')
