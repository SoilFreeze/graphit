import streamlit as st
import requests
import datetime
import time
from google.cloud import bigquery
from google.oauth2 import service_account

st.title("⚡ Direct Sandbox Telemetry Backfill Ingestion")
st.info("Targeting verified operational nodes using direct hardware inventory asset tables.")

# Configuration matches your exact database layout
PROJECT_ID = "sensorpush-export" 
DATASET_ID = "Temperature"      
TABLE_ID = "raw_sensorpush"
INVENTORY_TABLE = "hardware_inventory"

ACCOUNTS = [
    {'email': 'tsteele@soilfreeze.com', 'password': 'Freeze123!!'},
    {'email': 'ldunham@soilfreeze.com', 'password': 'Freeze123!!'},
    {'email': 'soilfreeze98072@gmail.com', 'password': 'Freeze123!!'}
]
BASE_URL = "https://api.sensorpush.com/api/v1"

# The specific operational nodes you requested to test
test_nodes = ["TP-0373", "TP-0259", "TP-0260"]

if st.button("🚀 Run Targeted Inventory Pass", type="primary"):
    all_rows = []
    reverse_map = {}
    
    with st.status("Executing Inventory Pipeline pass...", expanded=True) as status:
        st.write("🔍 Initializing BigQuery Client & Loading Inventory Mappings...")
        try:
            if "gcp_service_account" in st.secrets:
                info = st.secrets["gcp_service_account"]
                credentials = service_account.Credentials.from_service_account_info(info)
                client = bigquery.Client(credentials=credentials, project=info["project_id"])
            else:
                client = bigquery.Client(project=PROJECT_ID)
            
            # Using your clean verified layout columns: RawID and NodeNum
            query = f"""
                SELECT DISTINCT RawID, NodeNum 
                FROM `{PROJECT_ID}.{DATASET_ID}.{INVENTORY_TABLE}` 
                WHERE RawID IS NOT NULL
            """
            for row in client.query(query):
                # Clean up any trailing decimals or registration artifacts
                r_id = str(row.RawID).split('.')[0].strip()
                n_num = str(row.NodeNum).strip()
                
                if n_num in test_nodes:
                    reverse_map[r_id] = n_num
        except Exception as e:
            st.error(f"Database client initialization failed: {e}")
            st.stop()

        if not reverse_map:
            st.error(f"❌ Could not find matching RawID fields for nodes {test_nodes} inside your `{INVENTORY_TABLE}` table.")
            st.stop()

        st.info(f"🔗 Mappings Found! Mapped Node Names to Raw hardware tokens: {reverse_map}")

        # Target range: Backfill May 14 to May 28, 2026
        start_time_str = "2026-05-14T00:00:00+0000"
        api_limit = 5000 
        target_hardware_ids = list(reverse_map.keys())

        for acc in ACCOUNTS:
            st.write(f"🔐 Authenticating account: `{acc['email']}`...")
            try:
                auth_r = requests.post(f"{BASE_URL}/oauth/authorize", json=acc, timeout=15).json()
                token = requests.post(f"{BASE_URL}/oauth/accesstoken", json={"authorization": auth_r['authorization']}, timeout=15).json().get('accesstoken')

                # Fetch active sensor profiles for RSSI metrics
                s_resp = requests.post(f"{BASE_URL}/devices/sensors", headers={"Authorization": token}, json={}, timeout=20).json()
                
                device_rssi_map = {}
                if isinstance(s_resp, dict):
                    for s_id, s_meta in s_resp.items():
                        if isinstance(s_meta, dict) and 'rssi' in s_meta:
                            device_rssi_map[str(s_id)] = s_meta.get('rssi')
                else:
                    for s in s_resp:
                        if isinstance(s, dict) and 'id' in s and 'rssi' in s:
                            device_rssi_map[str(s['id'])] = s.get('rssi')
                
                st.write(f"📡 Querying missing timeline telemetry for targets: {list(reverse_map.values())}...")
                payload = {"limit": api_limit, "startTime": start_time_str, "sensors": target_hardware_ids}
                r_samples = requests.post(f"{BASE_URL}/samples", headers={"Authorization": token}, json=payload, timeout=45).json()
                
                sensors_data = r_samples.get('sensors', {})
                for s_id, samples in sensors_data.items():
                    clean_id = str(s_id).split('.')[0]
                    if clean_id in reverse_map:
                        friendly_name = reverse_map[clean_id]
                        current_device_rssi = device_rssi_map.get(str(s_id))
                        
                        for s in samples:
                            temp = s.get('temp_f') or s.get('temperature') or s.get('thermocouple_temperature')
                            if temp is not None:
                                all_rows.append({
                                    "timestamp": s['observed'],   
                                    "NodeNum": str(friendly_name),
                                    "temperature": float(temp),
                                    "rssi": int(current_device_rssi) if current_device_rssi is not None else None
                                })
                time.sleep(0.5)
            except Exception as e:
                st.warning(f"Account account processing notice: {e}")

        # Step 4: Stream Collected Data straight into BigQuery
        total_collected = len(all_rows)
        if total_collected == 0:
            st.error(f"❌ Completed pass, but 0 data records were found in the cloud for nodes {test_nodes}.")
            status.update(label="No Data Extracted", state="error")
        else:
            st.write(f"📥 Streaming {total_collected} records straight into `{TABLE_ID}`...")
            real_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
            errors = client.insert_rows_json(real_table_ref, all_rows)
            
            if not errors:
                st.success(f"🎉 Successfully backfilled {total_collected} records!")
                status.update(label="Ingestion Successful!", state="complete")
                st.balloons()
            else:
                st.error(f"Database insertion errors: {errors[:3]}")
                status.update(label="Database Rejection", state="error")
