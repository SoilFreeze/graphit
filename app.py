import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz

# 1. THE FOUNDATION (Must be Line 1)
st.set_page_config(layout="wide", page_title="SF Technician Dashboard")

# Force CSS scroll so the "Freeze" can't happen
st.markdown("""<style>.main .block-container {overflow-y: auto !important; height: auto !important;}</style>""", unsafe_allow_html=True)

# 2. AUTHENTICATION
# --- SECTION 2: AUTHENTICATION ---
if "gcp_service_account" in st.secrets:
    info = st.secrets["gcp_service_account"]
    
    # 💡 THIS IS THE CRITICAL PART: 
    # You must have both strings in this list!
    scopes = [
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/drive.readonly"
    ]
    
    creds = service_account.Credentials.from_service_account_info(
        info, 
        scopes=scopes
    )
    client = bigquery.Client(credentials=creds, project=info["project_id"])
else:
    st.error("GCP Secrets not found.")
    st.stop()

# 3. THE UNIVERSAL THEME (Safe Load)
@st.cache_data(ttl=3600)
def load_sf_theme(_credentials):
    from googleapiclient.discovery import build
    import io, json
    from googleapiclient.http import MediaIoBaseDownload
    try:
        service = build('drive', 'v3', credentials=_credentials)
        file_id = '18_DQ72HQ1HGaRGjkTUI7PDoIvzp2CqDy' # Your Shared JSON ID
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        return json.load(fh)
    except:
        # Fallback so the app NEVER freezes if Google Drive is down
        return {"table_theme": {"thresholds": {"critical_warming": 5.0}}}

SF_THEME = load_sf_theme(creds)

# 4. IMPORT UTILS (The Brains)
try:
    from sf_utils import get_standard_24h_summary, apply_standard_chart_style
except ImportError:
    st.error("Please ensure 'sf_utils.py' is uploaded to GitHub.")
    st.stop()

# 5. DATA FETCH (Your original working query)
@st.cache_data(ttl=300)
def fetch_data():
    query = """
    SELECT d.timestamp, d.value, d.nodenumber, m.Project, m.Location, m.Depth
    FROM `sensorpush-export.sensor_data.final_databoard_data` as d
    INNER JOIN `sensorpush-export.sensor_data.master_metadata` as m ON d.nodenumber = m.NodeNum
    ORDER BY d.timestamp ASC
    """
    df = client.query(query).to_dataframe()
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df

full_df = fetch_data()

# 6. SIDEBAR FILTERS
st.sidebar.title("🛠️ Tech Operations")
all_projs = sorted(full_df['Project'].unique().tolist())
sel_proj = st.sidebar.selectbox("Select Project", all_projs)
df_proj = full_df[full_df['Project'] == sel_proj].copy()

# 7. UI TABS
tab1, tab2, tab3 = st.tabs(["📡 Health", "📏 Profiles", "📈 History"])

with tab1:
    st.subheader(f"📋 24-Hour Summary: {sel_proj}")
    # Feature 1: The Universal Colored Table
    perf_table = get_standard_24h_summary(df_proj, SF_THEME)
    if perf_table is not None:
        st.dataframe(perf_table, use_container_width=True, hide_index=True, height=400)

with tab2:
    # Feature 2: Depth Profiles (Pipe filtering)
    pipes = [l for l in sorted(df_proj['Location'].unique()) if "bank" not in l.lower()]
    if pipes:
        sel_p = st.selectbox("Select Pipe", pipes)
        # (Add your snapshot logic here - keep it light!)
        st.write("Profile data loading...")

with tab3:
    # Feature 3: Time History (Hourly Downsampling)
    locs = sorted(df_proj['Location'].unique())
    sel_l = st.selectbox("Select Location", locs)
    # 💡 One point per hour fix:
    time_df = df_proj[df_proj['Location'] == sel_l].copy()
    time_df = time_df.set_index('timestamp').groupby('nodenumber').resample('1H').first().reset_index()
    
    fig = px.line(time_df, x='timestamp', y='value', color='Depth', height=600)
    # Feature 4: The Universal Chart Style
    fig = apply_standard_chart_style(fig, SF_THEME, is_profile=False)
    st.plotly_chart(fig, use_container_width=True)
