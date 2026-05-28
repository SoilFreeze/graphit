import streamlit as st
import requests
import datetime
import time
from google.cloud import bigquery
from google.oauth2 import service_account

st.title("⚡ Telemetry Ingestion Engine + RSSI Tracking")
st.info("Extracts historical temperatures and cross-references live gateway RSSI signal strength.")

PROJECT_ID = "sensorpush-export" 
DATASET_ID = "Temperature"      
TABLE_ID = "raw_sensorpush"
INVENTORY_TABLE = "hardware_inventory"

TARGET_ACCOUNT = {'email': 'ldunham@soilfreeze.com', 'password': 'Freeze123!!'}
BASE_URL = "https://api.sensorpush.com/api/v1"

if st.button("🚀 Run Ingestion with Live RSSI", type="primary"):
    all_rows = []
    hardware_map = {}
    
    with st.status("Executing RSSI-Enabled Pipeline Pass...", expanded=True) as status:
        st.write("🔍 Loading Exact Registration Keys from Database...")
        try:
            if "gcp_service_account" in st.secrets:
                info = st.secrets["gcp_service_account"]
                credentials = service_account.Credentials.from_service_account_info(info)
                client = bigquery.Client(credentials=credentials, project=info["project_id"])
            else:
                client = bigquery.Client(project=PROJECT_ID)
            
            # Read long literal strings from your hardware inventory table
            query = f"SELECT RawID, NodeNum FROM `{PROJECT_ID}.{DATASET_ID}.{INVENTORY_TABLE}` WHERE RawID IS NOT NULL"
            for row in client.query(query):
                r_id = str(row.RawID).strip()
                hardware_map[r_id] = str(row.NodeNum).strip()
        except Exception as e:
            st.error(f"Database lookup initialization failed: {e}")
            st.stop()

        # Bounding to today's date window to process active transmissions
        start_time_iso = "2026-05-28T00:00:00Z"
        end_time_iso = "2026-05-28T23:59:59Z"

        st.write("🔐 Authenticating session token...")
        try:
            auth_r = requests.post(f"{BASE_URL}/oauth/authorize", json=TARGET_ACCOUNT, timeout=15).json()
            token = requests.post(f"{BASE_URL}/oauth/accesstoken", json={"authorization": auth_r['authorization']}, timeout=15).json().get('accesstoken')

            # CRITICAL STEP: Query active sensor profiles to capture real-time connection telemetry (RSSI)
            st.write("📡 Mapping live antenna signal strengths from your gateway...")
            s_resp = requests.post(f"{BASE_URL}/devices/sensors", headers={"Authorization": token}, json={}, timeout=20).json()
            
            device_rssi_map = {}
            if isinstance(s_resp, dict):
                for s_id, s_meta in s_resp.items():
                    if isinstance(s_meta, dict) and 'rssi' in s_meta:
                        # Keep the raw dictionary key exactly as it arrives
                        device_rssi_map[str(s_id).strip()] = s_meta.get('rssi')

            st.write("📥 Downloading historical data streams...")
            payload = {"startTime": start_time_iso, "endTime": end_time_iso}
            r_samples = requests.post(f"{BASE_URL}/samples", headers={"Authorization": token}, json=payload, timeout=60).json()
            
            sensors_data = r_samples.get('sensors', {})
            if not sensors_data:
                st.error("❌ Cloud Gateway returned no active rows for this time window.")
                st.stop()
                
            for s_id, samples in sensors_data.items():
                raw_api_key = str(s_id).strip()
                
                # Check 1: Find matching clean node name using literal string maps
                friendly_name = hardware_map.get(raw_api_key)
                
                # Check 2: Fallback to substring loop if formatting scales differently
                if not friendly_name:
                    for db_raw, db_node in hardware_map.items():
                        if db_raw in raw_api_key or raw_api_key in db_raw:
                            friendly_name = db_node
                            break
                
                if not friendly_name:
                    friendly_name = f"UNMAPPED-{raw_api_key.split('.')[0]}"
                
                # Extract the corresponding RSSI value for this specific key
                current_device_rssi = device_rssi_map.get(raw_api_key)
                
                # Substring match logic for RSSI mapping fallback
                if current_device_rssi is None:
                    for rssi_key, rssi_val in device_rssi_map.items():
                        if rssi_key in raw_api_key or raw_api_key in rssi_key:
                            current_device_rssi = rssi_val
                            break

                for s in samples:
                    temp = s.get('temp_f') or s.get('temperature') or s.get('thermocouple_temperature')
                    if temp is not None:
                        all_rows.append({
                            "timestamp": s['observed'],   
                            "NodeNum": str(friendly_name),
                            "temperature": float(temp),
                            # Safely cast signal strength metrics straight to BigQuery integers
                            "rssi": int(current_device_rssi) if current_device_rssi is not None else None
                        })
        except Exception as api_err:
            st.error(f"API data collection pipeline failed: {api_err}")
            st.stop()

        total_collected = len(all_rows)
        if total_collected == 0:
            st.warning("⚠️ No valid data samples matched.")
            status.update(state="error")
        else:
            st.write(f"📥 Injecting {total_collected} rows containing temperature and RSSI into BigQuery...")
            real_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
            errors = client.insert_rows_json(real_table_ref, all_rows)
            
            if not errors:
                st.success(f"🎉 Success! Injected {total_collected} records with active RSSI signal tracking values!")
                status.update(label="Ingestion Complete!", state="complete")
                st.balloons()
            else:
                st.error(f"Database insertion errors encountered: {errors[:3]}")
                status.update(state="error")
