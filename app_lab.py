import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
import io

# --- 1. CONFIGURATION & STYLING ---
st.set_page_config(page_title="SoilFreeze Data Lab", layout="wide")

# Change to "sensor_data_dev" for your experimental dev app
DATASET_ID = "sensor_data" 
PROJECT_ID = "sensorpush-export"

# --- 2. AUTHENTICATION ENGINE (The "Plumbing") ---
@st.cache_resource
def get_bq_client():
    """Retrieves credentials from Secrets to prevent TransportErrors."""
    try:
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(info)
            # Scopes allow BigQuery to read Metadata from Google Sheets
            scoped_creds = credentials.with_scopes([
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/bigquery"
            ])
            return bigquery.Client(credentials=scoped_creds, project=info["project_id"])
        else:
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
            avg_t = df_ex['current_temp'].mean()
            c1, c2 = st.columns(2)
            c1.metric("Project Avg Temp", f"{avg_t:.1f}°F")
            c2.metric("Active Sensors", len(df_ex))

            # Maltby Engineering Requirements: Red (>32), Orange (28-32), Green (<28)
            def thermal_style(v):
                if v > 32: return 'background-color: #ff4b4b; color: white' 
                if 28 <= v <= 32: return 'background-color: #ffa500'        
                return 'background-color: #28a745; color: white'           
            
            st.dataframe(df_ex.style.applymap(thermal_style, subset=['current_temp']), use_container_width=True)
    except Exception as e:
        st.error(f"Error loading summary: {e}")

# --- SERVICE 3: DATA INTAKE LAB (LORD PARSER) ---
elif service == "📤 Data Intake Lab":
    st.header("📤 Manual Data Ingestion")
    source = st.radio("Device Type", ["SensorPush (CSV)", "Lord (SensorConnect)"])
    u_file = st.file_uploader("Upload Logger File", type=['csv'])

    if u_file:
        if "Lord" in source:
            # specialized parser to find the DATA_START marker
            lines = u_file.getvalue().decode("utf-8").splitlines()
            start_idx = next((i for i, l in enumerate(lines) if "DATA_START" in l), 0)
            u_file.seek(0)
            df_raw = pd.read_csv(u_file, skiprows=start_idx + 1)
            
            # Melt wide columns (Sensors) into long rows
            df_up = df_raw.melt(id_vars=[df_raw.columns[0]], var_name='nodenumber', value_name='value')
            df_up = df_up.rename(columns={df_raw.columns[0]: 'timestamp'})
            
            # Standardization: Ensure IDs use Hyphens for BigQuery Joins
            df_up['nodenumber'] = df_up['nodenumber'].str.replace(':', '-', regex=False)
            table_ref = f"{PROJECT_ID}.{DATASET_ID}.raw_lord"
            
            if st.button("🚀 PUSH TO BIGQUERY"):
                client.load_table_from_dataframe(df_up, table_ref).result()
                st.success("Lord Data Ingested!")

# --- SERVICE 4: DATABASE MAINTENANCE (THE ENGINE ROOM) ---
elif service == "⚙️ Database Maintenance":
    st.header("⚙️ Database Maintenance")
    st.info("This merges manual logs and online data into a single clean table.")
    
    if st.button("🔄 EXECUTE MASTER SCRUB"):
        with st.spinner("Rebuilding Master Table..."):
            # This query joins Raw data with Metadata based on standardized IDs
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
