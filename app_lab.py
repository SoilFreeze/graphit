import streamlit as st
import requests
import datetime
import time
from google.cloud import bigquery
from google.oauth2 import service_account

st.title("⚡ Master Fleet Multi-Account Ingestion Pass")
st.info("Extracts historical telemetry across tsteele@ and soilfreeze98072@ profiles to catch the remaining SP sensors.")

PROJECT_ID = "sensorpush-export" 
DATASET_ID = "Temperature"      
TABLE_ID = "raw_sensorpush"
INVENTORY_TABLE = "hardware_inventory"

# Target the remaining accounts holding the missing hardware footprints
REMAINING_ACCOUNTS = [
    {'email': 'tsteele@soilfreeze.com', 'password': 'Freeze123!!'},
    {'email': 'soilfreeze98072@gmail.com', 'password': 'Freeze123!!'}
]
BASE_URL = "https://api.sensorpush.com/api/v1"

if st.button("🚀 Run Remaining Fleet Backfill", type="primary"):
    all_rows = []
    hardware_map = {}
    
    with st.status("Streaming Remaining Footprints...", expanded=True) as status:
        st.write("🔍 Loading Inventory Cross-References...")
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
                hardware_map[r_id] = str(row.NodeNum).strip()
        except Exception as e:
            st.error(f"Database setup failed: {e}")
            st.stop()

        # Broad window targeting the entire month
        start_time_iso = "2026-05-01T00:00:00Z"
        end_time_iso = "2026-05-28T23:59:59Z"

        for acc in REMAINING_ACCOUNTS:
            st.write(f"🔐 Extracting data stream for: `{acc['email']}`...")
            try:
                auth_r = requests.post(f"{BASE_URL}/oauth/authorize", json=acc, timeout=15).json()
                token = requests.post(f"{BASE_URL}/oauth/accesstoken", json={"authorization": auth_r['authorization']}, timeout=15).json().get('accesstoken')

                s_resp = requests.post(f"{BASE_URL}/devices/sensors", headers={"Authorization": token}, json={}, timeout=20).json()
                device_rssi_map = {}
                if isinstance(s_resp, dict):
                    for s_id, s_meta in s_resp.items():
                        if isinstance(s_meta, dict) and 'rssi' in s_meta:
                            device_rssi_map[str(s_id)] = s_meta.get('rssi')

                payload = {"startTime": start_time_iso, "endTime": end_time_iso}
                r_samples = requests.post(f"{BASE_URL}/samples", headers={"Authorization": token}, json=payload, timeout=60).json()
                
                sensors_data = r_samples.get('sensors', {})
                for s_id, samples in sensors_data.items():
                    # Clean the incoming sensor ID string to handle decimal artifacts automatically
                    clean_id = str(s_id).split('.')[0].strip()
                    friendly_name = hardware_map.get(clean_id, f"UNMAPPED-{clean_id}")
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
                st.warning(f"Notice processing account {acc['email']}: {e}")

        total_collected = len(all_rows)
        if total_collected == 0:
            st.error("❌ No additional data found in remaining profiles.")
            status.update(state="error")
        else:
            st.write(f"📥 Streaming {total_collected} records straight into `{TABLE_ID}`...")
            real_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
            errors = client.insert_rows_json(real_table_ref, all_rows)
            
            if not errors:
                st.success(f"🎉 Successfully backfilled {total_collected} rows across your remaining sensor profiles!")
                status.update(state="complete")
                st.balloons()
