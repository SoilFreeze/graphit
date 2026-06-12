import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import time
import re
import requests
import numpy as np

# ===============================================================
# 1. CONFIGURATION, GLOBAL CONSTANTS & SESSION STATE
# ===============================================================
def initialize_app():
    """Sets up page config and global session state variables."""
    st.set_page_config(page_title="SF Engineering Admin", page_icon="🛠️", layout="wide")
    
    if 'unit_mode' not in st.session_state:
        st.session_state['unit_mode'] = "Fahrenheit"
    
    # Unified core data paths to prevent variable drift inside main()
    return "Temperature", "sensorpush-export"

DATASET_ID, PROJECT_ID = initialize_app()
display_tz = "America/Los_Angeles"

# Global target references for API tools to prevent variable drift
TABLE_ID = "raw_sensorpush"
INVENTORY_TABLE = "hardware_inventory"
BASE_URL = "https://api.sensorpush.com/api/v1"

# ===============================================================
# 2. DATABASE CLIENT
# ===============================================================
@st.cache_resource
def get_bq_client():
    """Initializes and returns the BigQuery client."""
    try:
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(info)
            return bigquery.Client(credentials=credentials, project=info["project_id"])
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"❌ Database Link Offline: {e}")
        return None

client = get_bq_client()
# ===============================================================
# 3. UTILITY FUNCTIONS
# ===============================================================

def get_fleet_telemetry(auth_headers):
    """Placeholder: Pings API to fetch fleet registry. Replace with actual API endpoints."""
    # Example logic:
    # response = requests.get(f"{BASE_URL}/gateways", headers=auth_headers)
    # return pd.DataFrame(response.json())
    return pd.DataFrame(columns=['NodeNum', 'PhysicalID', 'LastCheckIn'])

# ===============================================================
# 4. MAIN APP ENGINE
# ===============================================================

def main():
    st.sidebar.title("🛠️ Engineering Admin")
    page = st.sidebar.radio("Navigation", ["📄 Ingestion", "📡 API Sync & Audit", "🔍 Unmapped Nodes"])

    # TAB 1: MANUAL INGESTION
    if page == "📄 Ingestion":
        st.subheader("Manual File Ingestion")
        u_files = st.file_uploader("Select CSV/Excel files", type=['csv', 'xlsx'], accept_multiple_files=True)
        
        if u_files:
            all_processed_dfs = []
            for f in u_files:
                # ... [Insert your existing Processing Logic here] ...
                st.write(f"✅ Prepared: {f.name}")
            
            if st.button("🚀 Commit Batch to BigQuery"):
                combined_df = pd.concat(all_processed_dfs)
                # ... [Insert your load_table_from_dataframe logic here] ...
                st.success("Upload Complete")

    # TAB 2: API SYNC
    elif page == "📡 API Sync & Audit":
        st.subheader("Fleet Synchronization")
        if st.button("🔄 Sync API to Registry"):
            with st.spinner("Reconciling..."):
                # Call get_fleet_telemetry and perform SQL UPDATE
                st.success("Registry Synced with API")

    # TAB 3: UNMAPPED NODE AUDIT
    elif page == "🔍 Unmapped Nodes":
        st.subheader("Unmapped Node Audit")
        query = f"""
            SELECT NodeNum, COUNT(*) as total_points, MIN(timestamp) as first, MAX(timestamp) as last
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view`
            WHERE LOWER(NodeNum) LIKE '%unmapped%'
            GROUP BY NodeNum
            ORDER BY total_points DESC
        """
        if st.button("Run Audit"):
            df_unmapped = client.query(query).to_dataframe()
            if not df_unmapped.empty:
                st.dataframe(df_unmapped)
            else:
                st.success("✅ No unmapped nodes detected.")

if __name__ == "__main__":
    main()
