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
def get_sensorpush_fleet_status():
    """Fetches device list directly from API."""
    # Authenticate (Ensure you have your API credentials in secrets)
    email = st.secrets["sensorpush_accounts"]["account1"]["email"]
    password = st.secrets["sensorpush_accounts"]["account1"]["password"]
    
    # 1. Get Auth Token
    auth_resp = requests.post(f"{BASE_URL}/oauth/authorize", json={
        "email": email,
        "password": password
    })
    token = auth_resp.json().get("authorization")
    
    # 2. Get Devices
    headers = {"Authorization": token}
    devices_resp = requests.get(f"{BASE_URL}/devices", headers=headers)
    
    # 3. Format into a clean table
    data = []
    for d in devices_resp.json():
        data.append({
            "NodeName": d.get("name"),
            "PhysicalID": d.get("id"),
            "LastSeen": d.get("last_seen")
        })
    return pd.DataFrame(data)
