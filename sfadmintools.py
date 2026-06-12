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

BASE_URL = "https://api.sensorpush.com/api/v1"

def get_sensorpush_data():
    """Consolidates fleet list using hard-coded credentials."""
    all_devices = []
    
    # Hard-coded account list
    accounts = [
        {"name": "Account 1", "email": "tsteele@soilfreeze.com", "password": "Freeze123!!"},
        {"name": "Account 1", "email": "ldunham@soilfreeze.com", "password": "Freeze123!!"},
        {"name": "Account 2", "email": "soilfreeze98072@gmail.com", "password": "Freeze123!!"}
    ]
    
    for acc in accounts:
        try:
            # 1. Authorize
            auth_resp = requests.post(f"{BASE_URL}/oauth/authorize", json={
                "email": acc["email"],
                "password": acc["password"]
            })
            
            if auth_resp.status_code != 200:
                st.error(f"Auth failed for {acc['name']}")
                continue
                
            token = auth_resp.json().get("authorization")
            headers = {"Authorization": token}
            
            # 2. Fetch Devices
            dev_resp = requests.get(f"{BASE_URL}/devices", headers=headers)
            devices = dev_resp.json()
            
            # 3. Parse Metadata
            for dev in devices:
                all_devices.append({
                    "Account": acc["name"],
                    "NodeNum": dev.get("name"),
                    "PhysicalID": dev.get("id"),
                    "LastSeen": dev.get("last_seen")
                })
        except Exception as e:
            st.error(f"Error fetching {acc['name']}: {e}")
            
    return pd.DataFrame(all_devices)

# In your main() function, simply call:
# df = get_sensorpush_data()
