import streamlit as st
import requests
import datetime
import time
from google.cloud import bigquery
from google.oauth2 import service_account

st.title("⚡ Total Account Namespace Extraction")
st.info("Bypasses sensor ID filters to pull all available historical data from your profile.")

PROJECT_ID = "sensorpush-export" 
DATASET_ID = "Temperature"      
TABLE_ID = "raw_sensorpush"
INVENTORY_TABLE = "hardware_inventory"

TARGET_ACCOUNT = {'email': 'ldunham@soilfreeze.com', 'password': 'Freeze123!!'}
BASE_URL = "https://api.sensorpush.com/api/v1"

if st.button("🚀 Pull All Account Telemetry", type="primary"):
    all_rows = []
    hardware_map = {}
    
    with st.status("Extracting Complete Namespace...", expanded=True) as status:
        st.write("🔍 Loading Inventory Cross-Reference Table...")
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

        # Target range: May 14 to May 28, 2026
        start_time_iso = "2026-05-14T00:00:00Z"
        end_time_iso = "2026-05-28T23:59:59Z"

        st.write(f"🔐 Generating API tokens for `{TARGET_ACCOUNT['email']}`...")
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
            
            st.write("📡 Requesting total unfiltered data backlog from Cloud Gateway...")
            # CRITICAL SHIFT: Removing the "sensors" filter completely to pull all active data streams
            payload = {
                "startTime": start_time_iso,
                "endTime": end_time_iso
            }
            r_samples = requests.post(f"{BASE_URL}/samples", headers={"Authorization": token}, json=payload, timeout=60).json()
            
            sensors_data = r_samples.get('sensors', {})
            
            if not sensors_data:
                st.error("❌ The API returned a blank data block even without filters. Verify that data logging is enabled for this API key.")
                status.update(state="error")
                st.stop()
                
            st.info(f"📋 Detected active historical streams for {len(sensors_data.keys())} distinct hardware IDs!")
            
            # Map and parse whatever data came back
            for s_id, samples in sensors_data.items():
                clean_id = str(s_id).split('.')[0]
                # If it's in our inventory table, use the clean name; otherwise, track by its raw ID
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
                            
        except Exception as e:
            st.error(f"API Extraction Error: {e}")
            st.stop()

        # Step 4: Stream Collected Data straight into BigQuery
        total_collected = len(all_rows)
        if total_collected == 0:
            st.error("❌ No data records parsed.")
            status.update(label="No Data Extracted", state="error")
        else:
            st.write(f"📥 Injecting {total_collected} records into `{TABLE_ID}`...")
            real_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
            errors = client.insert_rows_json(real_table_ref, all_rows)
            
            if not errors:
                st.success(f"🎉 Backfill complete! Injected {total_collected} history records across your active fleet.")
                status.update(label="Ingestion Successful!", state="complete")
                st.balloons()
            else:
                st.error(f"Database insertion errors: {errors[:3]}")
                status.update(label="Database Rejection", state="error")
