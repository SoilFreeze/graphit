import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery, secretmanager
from google.oauth2 import service_account
from datetime import datetime, timedelta, time, date
import pytz
import json

# --- 0. PAGE CONFIG & SOILFREEZE PALETTE ---
st.set_page_config(layout="wide", page_title="SoilFreeze Engineering Hub")

def apply_sf_style():
    st.markdown("""
        <style>
            .stApp { background-color: #FFFFFF; }
            .stSidebar { background-color: #F8F9FA; border-right: 1px solid #E0E0E0; }
            h1, h2, h3 { color: #003366 !important; font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; }
            .stButton>button { background-color: #003366; color: white; border-radius: 4px; border: none; width: 100%; }
            .stMetric { background-color: #F8F9FA; padding: 10px; border-radius: 5px; border: 1px solid #E0E0E0; }
        </style>
    """, unsafe_allow_html=True)

apply_sf_style()

# --- 1. AUTHENTICATION (SECRET MANAGER) ---
@st.cache_resource
def get_bq_client():
    # Attempt to get from Secret Manager first (The "One Source of Truth" Plan)
    try:
        sm_client = secretmanager.SecretManagerServiceClient()
        # Ensure 'sensorpush-export' matches your GCP Project ID
        name = "projects/sensorpush-export/secrets/BIGQUERY_SERVICE_ACCOUNT_JSON/versions/latest"
        response = sm_client.access_secret_version(request={"name": name})
        info = json.loads(response.payload.data.decode("UTF-8"))
        credentials = service_account.Credentials.from_service_account_info(info)
        scoped_creds = credentials.with_scopes([
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery"
        ])
        return bigquery.Client(credentials=scoped_creds, project=info["project_id"])
    except Exception:
        # Fallback to st.secrets for local/legacy setup
        info = st.secrets["gcp_service_account"]
        credentials = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(credentials=credentials, project=info["project_id"])

client = get_bq_client()

# --- 2. DATA FETCHING (Using the Cleaned Master Table) ---
@st.cache_data(ttl=600)
def fetch_engineering_data():
    # We now pull from the 'final_databoard_master' which is already scrubbed and joined
    query = """
    SELECT timestamp, value, nodenumber, Project, Location, Depth, is_approved, engineer_note
    FROM `sensorpush-export.sensor_data.final_databoard_master`
    """
    df = client.query(query).to_dataframe()
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df

full_df = pd.DataFrame()
try:
    full_df = fetch_engineering_data()
except Exception as e:
    st.error(f"⚠️ Master Table Missing or Error: {e}. Run 'Database Maintenance' to build it.")

# --- 3. SIDEBAR & HEALTH CHECK ---
st.sidebar.title("❄️ SoilFreeze Lab")

if not full_df.empty:
    last_ts = full_df['timestamp'].max()
    st.sidebar.metric("Last Data Sync", last_ts.strftime('%m/%d %H:%M'))
    if (datetime.now(pytz.UTC) - last_ts).total_seconds() > 7200:
        st.sidebar.error("🚨 ALERT: Data is > 2 hours old!")

service = st.sidebar.selectbox(
    "Select Service", 
    ["🏠 Executive Summary", "🔍 Node Diagnostics", "📋 Data Approval Portal", "📥 Data Export Lab", "🧹 Database Maintenance"]
)

# --- SERVICE: EXECUTIVE SUMMARY ---
if service == "🏠 Executive Summary" and not full_df.empty:
    st.header("🏠 Site Health & Warming Alerts")
    
    c1, c2 = st.columns(2)
    with c1:
        all_projs = sorted([p for p in full_df['Project'].unique() if p is not None])
        sel_summary_proj = st.selectbox("1. Select Project", all_projs)
    
    proj_df = full_df[full_df['Project'] == sel_summary_proj].copy()
    
    with c2:
        all_locs = sorted([l for l in proj_df['Location'].unique() if l is not None])
        sel_summary_loc = st.selectbox("2. Select Pipe / Bank", all_locs)

    # 24-Hour Performance Table logic...
    now_ts = datetime.now(tz=pytz.UTC)
    loc_recent = proj_df[(proj_df['Location'] == sel_summary_loc) & (proj_df['timestamp'] >= (now_ts - timedelta(hours=24)))].copy()

    if not loc_recent.empty:
        node_analysis = []
        for node in loc_recent['nodenumber'].unique():
            n_df = loc_recent[loc_recent['nodenumber'] == node].sort_values('timestamp')
            if len(n_df) > 1:
                node_analysis.append({
                    "Depth": n_df['Depth'].iloc[0], "Node ID": node,
                    "Min": n_df['value'].min(), "Max": n_df['value'].max(),
                    "Current": n_df['value'].iloc[-1], "24h Change": n_df['value'].iloc[-1] - n_df['value'].iloc[0]
                })
        st.table(pd.DataFrame(node_analysis).sort_values('Depth'))

# --- SERVICE: DATABASE MAINTENANCE (THE FIX-IT TAB) ---
elif service == "🧹 Database Maintenance":
    st.header("🧹 Master Data Scrubbing & Consolidation")
    st.info("This tool applies the 90°F limit and the 5°F hourly averaging across all Lord and SensorPush data.")
    
    if st.button("🚀 EXECUTE FULL MASTER SCRUB", type="primary"):
        with st.spinner("Rebuilding Master Dashboard..."):
            scrub_sql = """
            CREATE OR REPLACE TABLE `sensorpush-export.sensor_data.final_databoard_master` AS
            WITH UnifiedRaw AS (
                SELECT CAST(timestamp AS TIMESTAMP) as ts, value, nodenumber, is_approved, engineer_note FROM `sensorpush-export.sensor_data.raw_lord` WHERE value <= 90
                UNION ALL
                SELECT CAST(timestamp AS TIMESTAMP) as ts, temperature AS value, sensor_name AS nodenumber, is_approved, engineer_note FROM `sensorpush-export.sensor_data.raw_sensorpush` WHERE temperature <= 90
            ),
            HourlyAgg AS (
                SELECT TIMESTAMP_TRUNC(ts, HOUR) as timestamp, nodenumber, AVG(value) as value, (MAX(value) - MIN(value)) as spread, LOGICAL_OR(is_approved) as is_approved, ANY_VALUE(engineer_note) as engineer_note
                FROM UnifiedRaw GROUP BY 1, 2 HAVING spread <= 5.0
            )
            SELECT d.*, m.Project, m.Location, m.Depth
            FROM HourlyAgg d
            INNER JOIN `sensorpush-export.sensor_data.master_metadata` m ON d.nodenumber = m.NodeNum
            """
            client.query(scrub_sql).result()
            st.cache_data.clear()
            st.success("✨ Master Dashboard Rebuilt! Weekend gaps have been averaged/scrubbed.")
            st.balloons()

# (Other services like Diagnostics and Approvals follow same logic...)
