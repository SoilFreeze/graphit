import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
import io
import json

# --- 1. CONFIGURATION & STYLING ---
st.set_page_config(page_title="SoilFreeze Data Lab", layout="wide")

# Set these variables to toggle between your "Working" and "Dev" environments
DATASET_ID = "sensor_data"  # Change to "sensor_data_dev" for your dev app
PROJECT_ID = "sensorpush-export"

# --- 2. AUTHENTICATION ENGINE ---
@st.cache_resource
def get_bq_client():
    """
    Retrieves credentials from Streamlit Secrets to prevent TransportErrors.
    This mimics the 'Plumbing' fix used in your main tech script.
    """
    try:
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(info)
            # Scopes are required to join BigQuery data with Google Sheet Metadata
            scoped_creds = credentials.with_scopes([
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/bigquery"
            ])
            return bigquery.Client(credentials=scoped_creds, project=info["project_id"])
        else:
            # Fallback for local development using environment default
            return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"Authentication Failed: {e}")
        return None

client = get_bq_client()

# --- 3. SIDEBAR NAVIGATION ---
st.sidebar.title("❄️ SoilFreeze Lab")
service = st.sidebar.selectbox("Select Service", [
    "🏠 Executive Summary", 
    "📈 Node Diagnostics", 
    "📤 Data Intake Lab", 
    "⚙️ Database Maintenance"
])

# --- SERVICE 1: EXECUTIVE SUMMARY ---
if service == "🏠 Executive Summary":
    st.header("🏠 Executive Summary")
    
    # Logic: Status at a glance with thermal color-coding
    query = f"""
        SELECT 
            nodenumber, Project, Location, Depth,
            MAX(timestamp) as last_seen,
            AVG(value) as current_temp
        FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master`
        GROUP BY 1, 2, 3, 4
    """
    try:
        df_ex = client.query(query).to_dataframe()
        
        if df_ex.empty:
            st.warning("Master Table is empty. Run 'Database Maintenance' first.")
        else:
            # Metrics: Project Average & Active Count
            avg_t = df_ex['current_temp'].mean()
            c1, c2 = st.columns(2)
            c1.metric("Project Avg Temp", f"{avg_t:.1f}°F")
            c2.metric("Active Sensors", len(df_ex))

            # Apply Maltby Engineering Color Logic
            def thermal_style(v):
                if v > 32: return 'background-color: #ff4b4b; color: white' # Red: Active Thaw
                if 28 <= v <= 32: return 'background-color: #ffa500'        # Orange: Transition
                return 'background-color: #28a745; color: white'           # Green: Target achieved
            
            st.dataframe(df_ex.style.applymap(thermal_style, subset=['current_temp']), use_container_width=True)
    except Exception as e:
        st.error(f"Error loading summary: {e}")

# --- SERVICE 2: NODE DIAGNOSTICS ---
elif service == "📈 Node Diagnostics":
    st.header("📈 Node Diagnostics")
    # Multi-project and multi-pipe filtering layout
    c1, c2, c3 = st.columns(3)
    
    # Pull metadata for filters
    meta_df = client.query(f"SELECT DISTINCT Project, Location FROM `{PROJECT_ID}.{DATASET_ID}.master_metadata`").to_dataframe()
    
    with c1: sel_projs = st.multiselect("Projects", meta_df['Project'].unique())
    with c2: sel_locs = st.multiselect("Pipes/Banks", meta_df[meta_df['Project'].isin(sel_projs)]['Location'].unique())
    with c3: weeks = st.slider("Trend Duration (Weeks)", 1, 12, 6) # Wide mode supports long trends

    if sel_projs and sel_locs:
        days = weeks * 7
        graph_q = f"""
            SELECT timestamp, value, Location, Depth
            FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master`
            WHERE Project IN UNNEST({list(sel_projs)})
            AND Location IN UNNEST({list(sel_locs)})
            AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
            ORDER BY timestamp ASC
        """
        df_g = client.query(graph_q).to_dataframe()
        # Legend labeling: Location (Depth) for instant ID
        df_g['Sensor'] = df_g['Location'] + " (" + df_g['Depth'] + "ft)"
        
        fig = px.line(df_g, x='timestamp', y='value', color='Sensor')
        # Reference Lines at 32°F and 28°F
        fig.add_hline(y=32, line_dash="dash", line_color="red", annotation_text="32°F Warning")
        fig.add_hline(y=28, line_dash="dot", line_color="green", annotation_text="28°F Target")
        st.plotly_chart(fig, use_container_width=True)

# --- SERVICE 3: DATA INTAKE LAB ---
elif service == "📤 Data Intake Lab":
    st.header("📤 Manual Data Ingestion")
    source = st.radio("Device Type", ["SensorPush (CSV)", "Lord (SensorConnect)"])
    u_file = st.file_uploader("Upload Logger File", type=['csv'])

    if u_file:
        # Specialized Lord Parser
        if "Lord" in source:
            lines = u_file.getvalue().decode("utf-8").splitlines()
            # Find DATA_START marker
            start_idx = next((i for i, l in enumerate(lines) if "DATA_START" in l), 0)
            u_file.seek(0)
            df_raw = pd.read_csv(u_file, skiprows=start_idx + 1)
            # Melt wide columns into long format
            df_up = df_raw.melt(id_vars=[df_raw.columns[0]], var_name='nodenumber', value_name='value')
            df_up = df_up.rename(columns={df_raw.columns[0]: 'timestamp'})
            # Standardize: Colons to Hyphens
            df_up['nodenumber'] = df_up['nodenumber'].str.replace(':', '-', regex=False)
            table_ref = f"{PROJECT_ID}.{DATASET_ID}.raw_lord"
        
        if st.button("🚀 PUSH TO BIGQUERY"):
            client.load_table_from_dataframe(df_up, table_ref).result()
            st.success("Data ingested successfully.")

# --- SERVICE 4: DATABASE MAINTENANCE ---
elif service == "⚙️ Database Maintenance":
    st.header("⚙️ Database Maintenance (The Engine Room)")
    if st.button("🔄 EXECUTE MASTER SCRUB"):
        with st.spinner("Rebuilding Master Table..."):
            # The Master Scrub: Merges manual and online data
            scrub_q = f"""
            CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` AS
            WITH Unified AS (
                SELECT timestamp, value, REPLACE(nodenumber, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
                UNION ALL
                SELECT timestamp, temperature as value, REPLACE(sensor_name, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
            )
            SELECT u.*, m.Project, m.Location, m.Depth
            FROM Unified u
            INNER JOIN `{PROJECT_ID}.{DATASET_ID}.master_metadata` m ON u.node = REPLACE(m.NodeNum, ':', '-')
            """
            client.query(scrub_q).result()
            st.success("Master Table Rebuilt & Standardized!")
