import streamlit as st
import requests
import datetime
import time
from google.cloud import bigquery
from google.oauth2 import service_account

st.title("⚡ Direct Sandbox Telemetry Backfill Ingestion")
st.info("Uses your production multi-account and RSSI logic to directly backfill the missing data.")

# Configuration matches your working script exactly
PROJECT_ID = "sensorpush-export" 
DATASET_ID = "Temperature"      
TABLE_ID = "raw_sensorpush"
METADATA_TABLE = "metadata_snapshot" 

ACCOUNTS = [
    {'email': 'tsteele@soilfreeze.com', 'password': 'Freeze123!!'},
    {'email': 'ldunham@soilfreeze.com', 'password': 'Freeze123!!'},
    {'email': 'soilfreeze98072@gmail.com', 'password': 'Freeze123!!'}
]
BASE_URL = "https://api.sensorpush.com/api/v1"

if st.button("🚀 Run Direct History Injection Pass", type="primary"):
    all_rows = []
    name_map = {}
    
    with st.status("Executing Backfill Pipeline...", expanded=True) as status:
        # Step 1: Initialize BigQuery Client using app credentials fallback
        st.write("🔍 Booting BigQuery Client & Loading Mappings...")
        try:
            if "gcp_service_account" in st.secrets:
                info = st.secrets["gcp_service_account"]
                credentials = service_account.Credentials.from_service_account_info(info)
                client = bigquery.Client(credentials=credentials, project=info["project_id"])
            else:
                # Fallback to local authentication configuration if using local terminal environment
                client = bigquery.Client(project=PROJECT_ID)
            
            # Load your current clean metadata labels
            query = f"SELECT PhysicalID, NodeNum FROM `{PROJECT_ID}.{DATASET_ID}.{METADATA_TABLE}` WHERE PhysicalID IS NOT NULL"
            for row in client.query(query):
                p_id = str(row.PhysicalID).split('.')[0].strip()
                name_map[p_id] = str(row.NodeNum).strip()
        except Exception as e:
            st.error(f"Database client initialization failed: {e}")
            status.update(label="Failed Initialization", state="error")
            st.stop()

        # Step 2: Set Backfill Time Range (May 14 to May 28)
        start_time_str = "2026-05-14T00:00:00+0000"
        api_limit = 5000 

        # Step 3: Run the Account Processing Loop
        for acc in ACCOUNTS:
            st.write(f"🔐 Authenticating account: `{acc['email']}`...")
            try:
                auth_r = requests.post(f"{BASE_URL}/oauth/authorize", json=acc, timeout=15).json()
                token = requests.post(f"{BASE_URL}/oauth/accesstoken", json={"authorization": auth_r['authorization']}, timeout=15).json().get('accesstoken')

                # Fetch active sensor profiles for RSSI values
                s_resp = requests.post(f"{BASE_URL}/devices/sensors", headers={"Authorization": token}, json={}, timeout=20).json()
                
                device_rssi_map = {}
                if isinstance(s_resp, dict):
                    sensor_ids = list(s_resp.keys())
                    for s_id, s_meta in s_resp.items():
                        if isinstance(s_meta, dict) and 'rssi' in s_meta:
                            device_rssi_map[str(s_id)] = s_meta.get('rssi')
                else:
                    sensor_ids = [s['id'] for s in s_resp]
                    for s in s_resp:
                        if isinstance(s, dict) and 'id' in s and 'rssi' in s:
                            device_rssi_map[str(s['id'])] = s.get('rssi')
                
                st.write(f"📥 Extracting historical sample arrays for `{len(sensor_ids)}` sensors...")
                
                # Chunk through sensors 10 at a time
                for i in range(0, len(sensor_ids), 10):
                    chunk = sensor_ids[i:i+10]
                    payload = {"limit": api_limit, "startTime": start_time_str, "sensors": chunk}
                    r_samples = requests.post(f"{BASE_URL}/samples", headers={"Authorization": token}, json=payload, timeout=45).json()
                    
                    sensors_data = r_samples.get('sensors', {})
                    for s_id, samples in sensors_data.items():
                        clean_id = str(s_id).split('.')[0]
                        friendly_name = name_map.get(clean_id, s_id)
                        
                        current_device_rssi = device_rssi_map.get(str(s_id))
                        
                        # Only target your missing T21 nodes to keep processing fast and efficient
                        if "TP-032" in str(friendly_name):
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
                st.warning(f"Error processing account {acc['email']}: {e}")

        # Step 4: Stream Collected Data straight into BigQuery
        total_collected = len(all_rows)
        if total_collected == 0:
            st.error("❌ Process finished, but 0 historical records were found matching 'TP-032' prefix mappings.")
            status.update(label="No Data Found", state="error")
        else:
            st.write(f"📥 Streaming {total_collected} records into your database table...")
            
            # Using the exact configuration string variable to target the raw_sensorpush table cleanly
            real_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
            errors = client.insert_rows_json(real_table_ref, all_rows)
            
            if not errors:
                st.success(f"🎉 Successfully backfilled {total_collected} history records directly into your table!")
                status.update(label="Backfill Ingestion Successful!", state="complete")
                st.balloons()
            else:
                st.error(f"BigQuery Entry Rejections: {errors[:3]}")
                status.update(label="Database Rejection", state="error")
