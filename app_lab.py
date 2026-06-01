import streamlit as st
import pandas as pd
import requests
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. PAGE LAYOUT CONFIGURATION
st.set_page_config(
    page_title="SensorPush API Sandbox",
    page_icon="📡",
    layout="wide"
)

# Global Database Constants (Mirrored from your production config)
DATASET_ID = "Temperature" 
PROJECT_ID = "sensorpush-export"

@st.cache_resource
def get_bq_client():
    """
    Initializes and caches the BigQuery connection.
    Prioritizes Service Account info from st.secrets if running on Streamlit Cloud.
    """
    try:
        SCOPES = [
            "https://www.googleapis.com/auth/bigquery", 
            "https://www.googleapis.com/auth/drive" 
        ]
        
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(
                info, 
                scopes=SCOPES
            )
            return bigquery.Client(credentials=credentials, project=info["project_id"])
        
        return bigquery.Client(project=PROJECT_ID)

    except Exception as e:
        st.sidebar.error(f"❌ BigQuery Authentication Failed: {e}")
        return None

def main():
    st.title("📡 SensorPush API & Hardware Link Auditor")
    st.markdown(
        "Use this standalone sandbox tool to query the SensorPush Cloud API directly and cross-reference "
        "hardware connection diagnostics with your active BigQuery Node Registry."
    )
    st.divider()

    # 2. HARDCODED CREDENTIAL ARRAYS
    BASE_URL = "https://api.sensorpush.com/api/v1"
    ACCOUNTS = [
        {'email': 'tsteele@soilfreeze.com', 'password': 'Freeze123!!'},
        {'email': 'ldunham@soilfreeze.com', 'password': 'Freeze123!!'},
        {'email': 'soilfreeze98072@gmail.com', 'password': 'Freeze123!!'}
    ]

    # 3. LIVE BIGQUERY REGISTRY LOOKUP
    client = get_bq_client()
    registered_nodes = set()
    
    if client is not None:
        try:
            # Pull active node configurations to cross-verify streaming data pipelines
            reg_q = f"SELECT DISTINCT NodeNum FROM `{PROJECT_ID}.{DATASET_ID}.node_registry` WHERE End_Date IS NULL"
            reg_df = client.query(reg_q).to_dataframe()
            registered_nodes = set(reg_df['NodeNum'].dropna().astype(str).tolist())
            st.success(f"🔗 Successfully indexed {len(registered_nodes)} active nodes from the database registry.")
        except Exception as e:
            st.warning(f"⚠️ Registry cross-reference offline (using API-only fallback diagnostics): {e}")
    else:
        st.info("💡 Running in local API fallback mode. Database connections will not be cross-referenced.")

    # 4. EXECUTION TRIGGER GATEWAY
    if st.button("🚀 Execute Multi-Account Cloud API Audit", use_container_width=True):
        headers = {"accept": "application/json", "Content-Type": "application/json"}
        all_sensor_records = []
        
        progress_bar = st.progress(0)
        status_text = st.empty()

        for idx, account in enumerate(ACCOUNTS):
            email = account['email']
            password = account['password']
            status_text.markdown(f"🔒 Requesting cloud authorization token for: **{email}**...")
            
            session = requests.Session()
            try:
                # Step A: OAuth Authorization
                auth_res = session.post(f"{BASE_URL}/oauth/authorize", json={"email": email, "password": password}, headers=headers)
                if auth_res.status_code != 200:
                    st.error(f"⚠️ Cloud Auth failed for {email}: {auth_res.text}")
                    continue
                auth_code = auth_res.json().get("authorization")

                # Step B: Access Token Exchange
                token_res = session.post(f"{BASE_URL}/oauth/accesstoken", json={"authorization": auth_code}, headers=headers)
                access_token = token_res.json().get("accesstoken")
                session.headers.update({"Authorization": access_token})
                
                # Step C: Extract Gateway Sensor Fleet Index
                sensor_res = session.post(f"{BASE_URL}/devices/sensors", json={})
                sensors_dict = sensor_res.json()
                
                # Step D: Pull Latest Sample Telemetry for Hardware Metrics
                sample_res = session.post(f"{BASE_URL}/samples", json={"limit": 1})
                samples_dict = sample_res.json().get("sensors", {})
