import streamlit as st
import requests
import datetime
import time
from google.cloud import bigquery
from google.oauth2 import service_account

st.title("⚡ Direct Sandbox Telemetry Backfill Ingestion")
st.info("Targeting verified operational nodes using exact ISO time window formatting.")

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

test_nodes = ["TP-0373", "TP-0259", "TP-0260"]

if st.button("🚀 Run Time-Window API Pass", type="primary"):
    all_rows = []
    reverse_map = {}
    
    with st.status("Executing Adjusted Time Pipeline...", expanded=True) as status:
        st.write("🔍 Loading Inventory Mappings...")
        try:
            if "gcp_service_account" in st.secrets:
                info = st.secrets["gcp_service_account"]
                credentials = service_account.Credentials.from_service_account_info(info)
                client = bigquery.Client(credentials=credentials, project=info["project_id"])
            else:
                client = bigquery.Client(project=PROJECT_ID)
            
            query = f"SELECT DISTINCT RawID, NodeNum FROM `{PROJECT_ID}.{DATASET_ID}.{INVENTORY_TABLE}` WHERE RawID IS NOT NULL"
            for row in client.query(query):
                r_id = str(row.RawID).split('.')[0].strip()
                n_num = str(row.NodeNum).strip()
                if n_num in test_nodes:
                    reverse_map[r_id] = n_num
        except Exception as e:
            st.error(f"Database client initialization failed: {e}")
            st.stop()

        if not reverse_map:
            st.error(f"❌ Could not find matching RawID fields for nodes {test_nodes} inside your inventory table.")
            st.stop()

        # Shifting to rigid, standardized ISO-8601 UTC 'Z' parameters
        start_time_iso = "2026-05-14T00:00:00Z"
        end_time_iso = "2026-05-28T23:59:59Z"
        target_hardware_ids = list(reverse_map.keys())

        for acc in ACCOUNTS:
            st.write(f"🔐 Authenticating account: `{acc['email']}`...")
            try:
                auth_r = requests.post(f"{BASE_URL}/oauth/authorize", json=acc, timeout=15).json()
                token = requests.post(f"{BASE_URL}/oauth/accesstoken", json={"authorization": auth_r['authorization']}, timeout=15).json().get('accesstoken')

                # Fetch active sensor profiles for RSSI
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
                
                st.write(f"📡 Querying explicit date-bounded window...")
                # Providing both startTime and endTime to force a perfect boundary window slice
                payload = {
                    "startTime": start_time_iso,
                    "endTime": end_time_iso,
                    "sensors": target_hardware_ids
                }
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
            st.error(f"❌ Time window rejected or 0 data entries returned for targets {test_nodes}.")
            status.update(label="No Data Extracted", state="error")
        else:
            st.write(f"📥 Streaming {total_collected} records straight into `{TABLE_ID}`...")
            real_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
            errors = client.insert_rows_json(real_table_ref, all_rows)
            
            if not errors:
                st.success(f"🎉 Successfully pulled and committed {total_collected} historical entries!")
                status.update(label="Ingestion Successful!", state="complete")
                st.balloons()
            else:
                st.error(f"Database insertion errors: {errors[:3]}")
                status.update(label="Database Rejection", state="error")
