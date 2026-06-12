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
import streamlit as st
import pandas as pd
import requests

# Constants
BASE_URL = "https://api.sensorpush.com/api/v1"

def get_sensorpush_data():
    """Authenticates against all accounts in secrets and returns a consolidated fleet list."""
    all_devices = []
    
    # Access accounts from secrets
    accounts = st.secrets.get("sensorpush_accounts", {})
    
    for account_name, creds in accounts.items():
        try:
            # 1. Authorize
            auth_resp = requests.post(f"{BASE_URL}/oauth/authorize", json={
                "email": creds["email"],
                "password": creds["password"]
            })
            
            if auth_resp.status_code != 200:
                st.error(f"Auth failed for {account_name}")
                continue
                
            token = auth_resp.json().get("authorization")
            headers = {"Authorization": token}
            
            # 2. Fetch Devices
            dev_resp = requests.get(f"{BASE_URL}/devices", headers=headers)
            devices = dev_resp.json()
            
            # 3. Parse Metadata
            for dev in devices:
                all_devices.append({
                    "Account": account_name,
                    "NodeNum": dev.get("name"),
                    "PhysicalID": dev.get("id"),
                    "LastSeen": dev.get("last_seen")
                })
        except Exception as e:
            st.error(f"Error fetching {account_name}: {e}")
            
    return pd.DataFrame(all_devices)

def main():
    st.set_page_config(page_title="SF Fleet Audit", layout="wide")
    st.title("📡 SensorPush Fleet Audit Tool")
    
    if st.button("🔄 Pull Live Fleet Metadata"):
        with st.spinner("Pinging SensorPush API..."):
            df = get_sensorpush_data()
            
            if not df.empty:
                # Format timestamp for better readability
                df['LastSeen'] = pd.to_datetime(df['LastSeen']).dt.tz_convert('America/Los_Angeles')
                st.dataframe(df, use_container_width=True)
                
                # Optional: Download as CSV
                st.download_button("Download Fleet Report", df.to_csv(index=False), "fleet_audit.csv")
            else:
                st.warning("No data retrieved.")

if __name__ == "__main__":
    main()
