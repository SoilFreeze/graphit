import streamlit as st
import requests
import datetime
import time
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

st.title("⚡ Direct Sandbox Telemetry Backfill Ingestion")
st.info("Using literal exact string matching with live node-by-node update tallies.")

PROJECT_ID = "sensorpush-export" 
DATASET_ID = "Temperature"      
TABLE_ID = "raw_sensorpush"
INVENTORY_TABLE = "hardware_inventory"

TARGET_ACCOUNT = {'email': 'ldunham@soilfreeze.com', 'password': 'Freeze123!!'}
BASE_URL = "https://api.sensorpush.com/api/v1"

if st.button("🚀 Run Exact-String Production Ingestion", type="primary"):
    all_rows = []
    hardware_map = {}
    node_counts = {}  # Dictionary to track updates per node
    
    with st.status("Executing Exact-String Pass...", expanded=True) as status:
        st.write("🔍 Loading Exact Registration Keys from Database...")
        try:
            if "gcp_service_account" in st.secrets:
                info = st.secrets["gcp_service_account"]
                credentials = service_account.Credentials.from_service_account_info(info)
                client = bigquery.Client(credentials=credentials, project=info["project_id"])
            else:
                client = bigquery.Client(project=PROJECT_ID)
            
            query = f"SELECT RawID, NodeNum FROM `{PROJECT_ID}.{DATASET_ID}.{INVENTORY_TABLE}` WHERE RawID IS NOT NULL"
            for row in client.query(query):
                r_id = str(row.RawID).strip()
                hardware_map[r_id] = str(row.NodeNum).strip()
        except Exception as e:
            st.error(f"Database lookup initialization failed: {e}")
            st.stop()

        # Bounding window (Adjust dates here if you need to pull further back)
        start_time_iso = "2026-05-28T00:00:00Z"
        end_time_iso = "2026-05-28T23:59:59Z"

        st.write("🔐 Authenticating session token...")
        try:
            auth_r = requests.post(f"{BASE_URL}/oauth/authorize", json=TARGET_ACCOUNT, timeout=15).json()
            token = requests.post(f"{BASE_URL}/oauth/accesstoken", json={"authorization": auth_r['authorization']}, timeout=15).json().get('accesstoken')

            st.write("📡 Pulling unfiltered live payload matrix...")
            payload = {"startTime": start_time_iso, "endTime": end_time_iso}
            r_samples = requests.post(f"{BASE_URL}/samples", headers={"Authorization": token}, json=payload, timeout=60).json()
            
            sensors_data = r_samples.get('sensors', {})
            if not sensors_data:
                st.error("❌ Cloud Gateway returned no rows for this frame.")
                st.stop()
                
            for s_id, samples in sensors_data.items():
                raw_api_key = str(s_id).strip()
                
                friendly_name = hardware_map.get(raw_api_key)
                if not friendly_name:
                    for db_raw, db_node in hardware_map.items():
                        if db_raw in raw_api_key or raw_api_key in db_raw:
                            friendly_name = db_node
                            break
                
                if not friendly_name:
                    friendly_name = f"UNMAPPED-{raw_api_key.split('.')[0]}"
                
                for s in samples:
                    temp = s.get('temp_f') or s.get('temperature') or s.get('thermocouple_temperature')
                    if temp is not None:
                        all_rows.append({
                            "timestamp": s['observed'],   
                            "NodeNum": str(friendly_name),
                            "temperature": float(temp),
                            "rssi": None
                        })
                        # Tally the point to this specific node name
                        node_counts[friendly_name] = node_counts.get(friendly_name, 0) + 1

        except Exception as api_err:
            st.error(f"API pipeline failed: {api_err}")
            st.stop()

        total_collected = len(all_rows)
        if total_collected == 0:
            st.warning("⚠️ No valid temperature samples matched.")
            status.update(state="error")
        else:
            st.write(f"📥 Injecting {total_collected} live records straight into `{TABLE_ID}`...")
            real_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
            errors = client.insert_rows_json(real_table_ref, all_rows)
            
            if not errors:
                st.success(f"🎉 Backfill Complete! Successfully committed {total_collected} records.")
                status.update(label="Backfill Complete!", state="complete")
                
                # --- NEW FEATURE: RENDER DISTRIBUTION TALLY ---
                st.write("### 📊 Ingestion Breakdown Per Node:")
                # Convert the counter dictionary into a clean, sortable Streamlit dataframe
                summary_df = pd.DataFrame(list(node_counts.items()), columns=["Node Number", "Points Updated"]).sort_values(by="Node Number")
                st.dataframe(summary_df, use_container_width=True, hide_index=True)
                
                st.balloons()
            else:
                st.error(f"Database insertion errors: {errors[:3]}")
                status.update(state="error")
