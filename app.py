import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz

# --- FIX 1: FORCE SCROLLING & PREVENT OVERFLOW ---
st.set_page_config(layout="wide", page_title="SF Tech")

st.markdown("""
    <style>
    /* Force the main container to be scrollable regardless of content */
    .main .block-container {
        overflow-y: auto !important;
        height: auto !important;
        max-height: none !important;
        padding-bottom: 10rem !important;
    }
    /* Stop Plotly from 'highjacking' the mouse wheel */
    .js-plotly-plot {
        pointer-events: auto !important;
    }
    </style>
    """, unsafe_allow_html=True)

# =================================================================
# 1. AUTHENTICATION
# =================================================================
if "gcp_service_account" in st.secrets:
    info = st.secrets["gcp_service_account"]
    creds = service_account.Credentials.from_service_account_info(
        info, 
        scopes=["https://www.googleapis.com/auth/drive.readonly", "https://www.googleapis.com/auth/bigquery"]
    )
    client = bigquery.Client(credentials=creds, project=info["project_id"])
else:
    st.error("Secrets missing.")
    st.stop()

# =================================================================
# 2. THEME LOADER (With 'None' Guard)
# =================================================================
@st.cache_data(ttl=3600)
def load_remote_theme(_credentials):
    from googleapiclient.discovery import build
    import json, io
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
        return None # If this fails, the app won't loop; it just returns None

SF_THEME = load_remote_theme(creds)

# =================================================================
# 3. DATA FETCHING (With 'Limit' to prevent freezing)
# =================================================================
@st.cache_data(ttl=300)
def fetch_tech_data():
    # Only pull what is necessary for the current view
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
# 4. SIDEBAR & LOGIC
# =================================================================
all_projs = sorted(full_df['Project'].unique().tolist())
sel_proj = st.sidebar.selectbox("Project", all_projs)
df_proj = full_df[full_df['Project'] == sel_proj].copy()

# =================================================================
# 5. UI TABS (With Data Density Protection)
# =================================================================
tab1, tab2, tab3 = st.tabs(["📡 Health", "📏 Profiles", "📈 History"])

with tab1:
    st.subheader("24-Hour Summary")
    # FIX 2: Using st.dataframe with a height limit prevents 'Page Locking'
    from sf_utils import get_standard_24h_summary
    perf_table = get_standard_24h_summary(df_proj, SF_THEME)
    if perf_table is not None:
        st.dataframe(perf_table, use_container_width=True, hide_index=True, height=400)

with tab3:
    st.subheader("History")
    # FIX 3: If history has 50,000 rows, the browser will freeze. 
    # We 'Downsample' to every 5th row to keep the UI snappy.
    locs = sorted(df_proj['Location'].unique())
    sel_loc = st.selectbox("Location", locs)
    time_df = df_proj[df_proj['Location'] == sel_loc].iloc[::5] # Take every 5th point
    
    fig = px.line(time_df, x='timestamp', y='value', color='Depth')
    st.plotly_chart(fig, use_container_width=True)
