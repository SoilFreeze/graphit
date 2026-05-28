import streamlit as st
import requests
import datetime
import time
from google.cloud import bigquery
from google.oauth2 import service_account

st.title("⚡ Master Production Ingestion Engine")
st.info("Extracts unfiltered historical streams directly from your primary namespace and updates clean engineering name references.")

PROJECT_ID = "sensorpush-export" 
DATASET_ID = "Temperature"      
TABLE_ID = "raw_sensorpush"
INVENTORY_TABLE = "hardware_inventory"

TARGET_ACCOUNT = {'email': 'ldunham@soilfreeze.com', 'password': 'Freeze123!!'}
BASE_URL = "https://api.sensorpush.com/api/v1"

if st.button("🚀 Run Master Production Backfill", type="primary"):
    all_rows = []
    hardware_map = {}
    
    with st.status("Streaming Production Backlog...", expanded=True) as status:
        st.write("🔍 Loading Clean Mappings from Asset Inventory...")
        try:
            if "gcp_service_account" in st.secrets:
                info = st.secrets["gcp_service_account"]
                credentials = service_account.Credentials.from_service_account_info(info)
                client = bigquery.Client(credentials=credentials, project=info["project_id"])
            else:
                client = bigquery.Client(project=PROJECT_ID)
            
            # Read clean pairs straight from your hardware inventory asset list
            query = f"SELECT DISTINCT RawID, NodeNum FROM `{PROJECT_ID}.{DATASET_ID}.{INVENTORY_TABLE}` WHERE RawID IS NOT NULL"
            for row in client.query(query):
                r_id = str(row.RawID).split('.')[0].strip()
                hardware_map[r_id] = str(row.NodeNum).strip()
        except Exception as e:
            st.error(f"Database lookup initialization failed: {e}")
            st.stop()

        # Expanding the date boundary to cover the full month window cleanly
        start_time_iso = "2026-05-01T00:00:00Z"
        end_time_iso = "2026-05-28T23:59:59Z"

        st.write("🔐 Authenticating session token...")
        try:
            auth_r = requests.post(f"{BASE_URL}/oauth/authorize", json=TARGET_ACCOUNT, timeout=15).json()
            token = requests.post(f"{BASE_URL}/oauth/accesstoken", json={"authorization": auth_r['authorization']}, timeout=15).json().get('accesstoken')

            # Fetch active sensor profiles for RSSI metrics
            s_resp = requests.post(f"{BASE_URL}/devices/sensors", headers={"Authorization": token}, json={}, timeout=20).json()
            device_rssi_map = {}
            if isinstance(s_resp, dict):
                for s_id, s_meta in s_resp.items():
                    if isinstance(s_meta, dict) and 'rssi' in s_meta:
                        device_rssi_map[str(s_id)] = s_meta.get('rssi')

            st.write("📡 Extracting unfiltered telemetry payload from Cloud Gateway...")
            payload = {
                "startTime": start_time_iso,
                "endTime": end_time_iso
            }
            r_samples = requests.post(f"{BASE_URL}/samples", headers={"Authorization": token}, json=payload, timeout=60).json()
            
            sensors_data = r_samples.get('sensors', {})
            if not sensors_data:
                st.error("❌ Cloud Gateway returned no historical rows for this window frame.")
                st.stop()
                
            for s_id, samples in sensors_data.items():
                clean_id = str(s_id).split('.')[0]
                
                # Cross-references against your sheet mappings dynamically!
                friendly_name = hardware_map.get(clean_id, s_id)
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
        except Exception as api_err:
            st.error(f"API data collection pipeline failed: {api_err}")
            st.stop()

        total_collected = len(all_rows)
        if total_collected == 0:
            st.warning("⚠️ No valid temperature samples parsed.")
            status.update(label="No Data Extracted", state="error")
        else:
            st.write(f"📥 Injecting {total_collected} records into your production table...")
            real_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
            errors = client.insert_rows_json(real_table_ref, all_rows)
            
            if not errors:
                st.success(f"🎉 Successfully committed {total_collected} data rows across your active fleet names!")
                status.update(label="Backfill Complete!", state="complete")
                st.balloons()
            else:
                st.error(f"Database insertion errors encountered: {errors[:3]}")
                status.update(label="Database Error", state="error")
