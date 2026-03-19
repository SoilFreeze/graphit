import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz

# =================================================================
# 1. AUTHENTICATION FIRST (This creates 'creds')
# =================================================================
if "gcp_service_account" in st.secrets:
    info = st.secrets["gcp_service_account"]
    # We define 'creds' here so it exists for the rest of the script
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
def load_remote_theme(_credentials): # Use the underscore to avoid hashing errors
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    import io, json
    try:
        service = build('drive', 'v3', credentials=_credentials)
        # REPLACE THIS with your actual ID from Google Drive
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
        return None

# =================================================================
# 3. NOW CALL THE THEME (Now 'creds' is safely defined!)
# =================================================================
SF_THEME = load_remote_theme(creds)

# =================================================================
# 4. IMPORT UTILS AND SETUP PAGE
# =================================================================
# Ensure sf_utils.py is uploaded to your GitHub repo
from sf_utils import get_standard_24h_summary, apply_standard_chart_style

st.set_page_config(layout="wide", page_title="SF Project Dashboard")


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
