import streamlit as st
import pandas as pd
from google.cloud import bigquery
import plotly.express as px
from datetime import datetime, timedelta

# --- 1. SETUP ---
st.set_page_config(page_title="SoilFreeze Data Lab | INTERNAL", layout="wide")

# We assume credentials are set via Environment Variables or Streamlit Secrets
client = bigquery.Client(project="sensorpush-export")

# --- 2. THE MASTER SCRUB LOGIC (The "Engine") ---
def run_master_scrub():
    """
    Standardizes SensorPush and Lord data into one master table.
    Ensures all IDs use Hyphens and joins with Metadata.
    """
    scrub_q = """
    CREATE OR REPLACE TABLE `sensorpush-export.sensor_data.final_databoard_master` AS
    WITH UnifiedRaw AS (
        -- Standardize Lord Data (Manual Uploads)
        SELECT 
            CAST(timestamp AS TIMESTAMP) as ts, 
            value, 
            REPLACE(nodenumber, ':', '-') as sensor_id
        FROM `sensorpush-export.sensor_data.raw_lord` 
        WHERE value BETWEEN -50 AND 120
        
        UNION ALL
        
        -- Standardize SensorPush Data (API/Online)
        SELECT 
            CAST(timestamp AS TIMESTAMP) as ts, 
            temperature AS value, 
            REPLACE(sensor_name, ':', '-') as sensor_id
        FROM `sensorpush-export.sensor_data.raw_sensorpush` 
        WHERE temperature BETWEEN -50 AND 120
    ),
    HourlyAgg AS (
        SELECT 
            TIMESTAMP_TRUNC(ts, HOUR) as timestamp, 
            sensor_id, 
            AVG(value) as value
        FROM UnifiedRaw 
        GROUP BY 1, 2
    )
    -- Join with Metadata to get Project, Location (Pipe), and Depth
    SELECT 
        d.*, 
        m.Project, 
        m.Location, 
        m.Depth,
        CAST(NULL AS STRING) as engineer_note, 
        CAST(FALSE AS BOOL) as is_approved
    FROM HourlyAgg d
    INNER JOIN `sensorpush-export.sensor_data.master_metadata` m 
        ON d.sensor_id = REPLACE(m.NodeNum, ':', '-')
    """
    query_job = client.query(scrub_q)
    query_job.result() # Wait for completion

# --- 3. UI ROUTING ---
st.sidebar.title("❄️ SoilFreeze Lab")
service = st.sidebar.selectbox("Select Service", [
    "🏠 Executive Summary", 
    "📈 Node Diagnostics", 
    "📤 Data Intake Lab", 
    "⚙️ Database Maintenance"
])

if service == "⚙️ Database Maintenance":
    st.header("⚙️ Database Maintenance")
    st.info("This process merges raw manual logs and online data into the Master Table.")
    if st.button("🔄 EXECUTE MASTER SCRUB"):
        with st.spinner("Scrubbing and Rebuilding..."):
            try:
                run_master_scrub()
                st.success("✅ Master Table Rebuilt! IDs standardized to Hyphens.")
                st.balloons()
            except Exception as e:
                st.error(f"Scrub failed: {e}")

# ... (Include your existing Data Intake Lab and Diagnostics logic below)
