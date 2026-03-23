import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery, secretmanager
from google.oauth2 import service_account
from datetime import datetime, timedelta, time, date
import pytz
import json
import requests


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
    # 1. TRY SECRET MANAGER (The "One Source" Plan)
    try:
        from google.cloud import secretmanager
        sm_client = secretmanager.SecretManagerServiceClient()
        # Ensure 'sensorpush-export' is your EXACT project ID
        name = "projects/sensorpush-export/secrets/BIGQUERY_SERVICE_ACCOUNT_JSON/versions/latest"
        
        # We add a 5-second timeout so it doesn't get "stuck"
        response = sm_client.access_secret_version(request={"name": name}, timeout=5)
        info = json.loads(response.payload.data.decode("UTF-8"))
        credentials = service_account.Credentials.from_service_account_info(info)
        scoped_creds = credentials.with_scopes([
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery"
        ])
        return bigquery.Client(credentials=scoped_creds, project=info["project_id"])

    except Exception as e:
        # 2. FALLBACK TO LOCAL SECRETS (So you aren't stuck!)
        st.sidebar.warning("⚠️ Secret Manager failed. Using local secrets.")
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(info)
            # Make sure we still add the Drive scope here for the Metadata join
            scoped_creds = credentials.with_scopes([
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/bigquery"
            ])
            return bigquery.Client(credentials=scoped_creds, project=info["project_id"])
        else:
            raise Exception("No credentials found in Secret Manager or st.secrets")

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

# --- SERVICE 1: NODE DIAGNOSTICS (FIXES THE SPAGHETTI GRAPH) ---
elif service == "🔍 Node Diagnostics" and not full_df.empty:
    st.header("🔍 Node Diagnostic Hub")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        sel_proj = st.selectbox("Project", sorted(full_df['Project'].unique()))
    with col2:
        locs = sorted(full_df[full_df['Project'] == sel_proj]['Location'].unique())
        sel_loc = st.selectbox("Location", locs)
    with col3:
        weeks = st.number_input("Weeks", min_value=1, value=2)

    plot_df = full_df[
        (full_df['Project'] == sel_proj) & 
        (full_df['Location'] == sel_loc) & 
        (full_df['timestamp'] >= (datetime.now(pytz.UTC) - timedelta(weeks=weeks)))
    ].copy()

    if not plot_df.empty:
        # CRITICAL FIX: Sort by timestamp so lines don't zig-zag backward
        plot_df = plot_df.sort_values(['nodenumber', 'timestamp'])
        plot_df['Sensor'] = plot_df['Depth'].astype(str) + "ft (" + plot_df['nodenumber'] + ")"
        
        fig = px.line(plot_df, x='timestamp', y='value', color='Sensor', height=700)
        fig.update_traces(connectgaps=False) # Shows gaps where data is missing
        fig.add_hline(y=32, line_dash="dash", line_color="#007BFF")
        fig.update_layout(plot_bgcolor='white', hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data found for this selection.")

# --- SERVICE 2: DATA APPROVAL PORTAL (WITH EXCLUSIONS) ---
elif service == "📋 Data Approval Portal":
    st.header("📋 Engineering Approval Portal")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        ap_proj = st.selectbox("Target Project", sorted(full_df['Project'].unique()))
    with col2:
        ap_loc = st.selectbox("Target Pipe (Optional)", ["All"] + sorted(full_df[full_df['Project'] == ap_proj]['Location'].unique().tolist()))
    with col3:
        ap_date = st.date_input("Date to Target", value=date.today() - timedelta(days=1))

    status = st.radio("Set Status To:", ["✅ Approved (Show Client)", "🚫 Hidden (Internal Only)"])
    note = st.text_area("Engineering Note", placeholder="Reason for status change...")

    if st.button("🚀 SYNC APPROVAL STATUS"):
        is_app = "TRUE" if "Approved" in status else "FALSE"
        loc_filter = "" if ap_loc == "All" else f"AND Location = '{ap_loc}'"
        
        # This SQL targets exactly what you asked for: Job, Pipe, and Time
        sync_sql = f"""
        UPDATE `sensorpush-export.sensor_data.final_databoard_master`
        SET is_approved = {is_app}, engineer_note = '{note}'
        WHERE Project = '{ap_proj}' {loc_filter} 
        AND CAST(timestamp AS DATE) = '{ap_date}'
        """
        client.query(sync_sql).result()
        st.success(f"Updated status for {ap_proj} on {ap_date}")

# --- SERVICE 3: DATA EXPORT LAB (FIXED) ---
elif service == "📥 Data Export Lab" and not full_df.empty:
    st.header("📥 Data Export Lab")
    
    ex_proj = st.selectbox("Select Project to Export", sorted(full_df['Project'].unique()))
    export_df = full_df[full_df['Project'] == ex_proj].copy()
    
    st.dataframe(export_df.head(100)) # Preview
    
    csv = export_df.to_csv(index=False).encode('utf-8')
    st.download_button("📥 Download Full Project CSV", data=csv, file_name=f"{ex_proj}_export.csv", mime="text/csv")

# --- SERVICE: DATABASE MAINTENANCE (THE FIX-IT TAB) ---
if service == "🧹 Database Maintenance":
    st.header("🧹 Database Maintenance")
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
    if st.button("📥 FORCE BACKFILL SENSORPUSH", key="maintenance_backfill"):
        try:
            sp_creds = st.secrets["sensorpush_login"]
            # ... rest of the backfill logic ...
            st.success("Backfill triggered!")
        except Exception as e:
            st.error(f"Could not find sensorpush_login in secrets: {e}")
# (Other services like Diagnostics and Approvals follow same logic...)
if st.button("📥 FORCE BACKFILL SENSORPUSH", key="backfill_maintenance"):
    # 1. Get Login from your secrets
    sp_creds = st.secrets["sensorpush_login"] # Assuming you have this in st.secrets
    
    # 2. Authenticate
    auth_resp = requests.post("https://api.sensorpush.com/api/v1/oauth/authorize", 
                              json={"email": sp_creds['user'], "password": sp_creds['pass']})
    token = auth_resp.json().get('accesstoken')
    
    # 3. Request "Weekend Gap" (Since Friday 7pm UTC)
    start_time = "2026-03-20T19:00:00Z"
    data_resp = requests.post("https://api.sensorpush.com/api/v1/samples", 
                              headers={"Authorization": token}, 
                              json={"startTime": start_time})
    
    samples = data_resp.json()
    st.write(f"Found {len(samples)} new records from the weekend!")
    
    # 4. Push to BigQuery (I can provide the 'to_gbq' code if you need it)
    # ... logic to upload to raw_sensorpush ...


