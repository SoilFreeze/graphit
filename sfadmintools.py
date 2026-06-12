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


import streamlit as st
import pandas as pd
import requests
from google.cloud import bigquery
from google.oauth2 import service_account

# ===============================================================
# 1. CONFIG & CLIENT INITIALIZATION
# ===============================================================
def get_bq_client():
    """Initializes BigQuery client using secrets."""
    try:
        info = st.secrets["gcp_service_account"]
        credentials = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(credentials=credentials, project=info["project_id"])
    except Exception as e:
        st.error(f"❌ BigQuery Auth Error: {e}")
        return None

client = get_bq_client()
BASE_URL = "https://api.sensorpush.com/api/v1"

# ===============================================================
# 2. SENSORPUSH API LOGIC (Hard-coded Accounts)
# ===============================================================
def get_sensorpush_data():
    """Consolidates fleet list using explicit, hard-coded dictionary access."""
    all_devices = []
    
    # Using a list of dictionaries
    accounts = [
        {"name": "Account 1", "email": "tsteele@soilfreeze.com", "password": "Freeze123!!"},
        {"name": "Account 2", "email": "soilfreeze98072@gmail.com", "password": "Freeze123!!"}
    ]
    
    for acc in accounts:
        try:
            # 1. Authorize - Force explicit string types
            payload = {"email": str(acc["email"]), "password": str(acc["password"])}
            auth_resp = requests.post(f"{BASE_URL}/oauth/authorize", json=payload)
            
            if auth_resp.status_code != 200:
                st.error(f"Auth failed for {acc['name']}: {auth_resp.text}")
                continue
                
            token = auth_resp.json()["authorization"] # Direct access
            headers = {"Authorization": token}
            
            # 2. Fetch Devices
            dev_resp = requests.get(f"{BASE_URL}/devices", headers=headers)
            devices = dev_resp.json()
            
            # 3. Parse Metadata using standard bracket notation
            for dev in devices:
                all_devices.append({
                    "Account": acc["name"],
                    "NodeNum": dev["name"],       # Direct access
                    "PhysicalID": dev["id"],      # Direct access
                    "LastSeen": dev["last_seen"]  # Direct access
                })
        except Exception as e:
            st.error(f"Error fetching {acc['name']}: {e}")
            
    return pd.DataFrame(all_devices)
# ===============================================================
# 3. MAIN UI
# ===============================================================
def main():
    st.set_page_config(page_title="SF Engineering Admin", layout="wide")
    st.title("📡 SensorPush Fleet Audit Tool")
    
    # This button now triggers the API pull
    if st.button("🔄 Pull Live Fleet Metadata"):
        with st.spinner("Pinging SensorPush API..."):
            df = get_sensorpush_data()
            
            if not df.empty:
                # Format timestamp
                df['LastSeen'] = pd.to_datetime(df['LastSeen']).dt.tz_convert('America/Los_Angeles')
                st.dataframe(df, use_container_width=True)
                
                # Optional: Add a button to save these to BigQuery if needed
                st.download_button("Download Report", df.to_csv(index=False), "fleet_audit.csv")
            else:
                st.warning("No data retrieved.")

if __name__ == "__main__":
    main()
