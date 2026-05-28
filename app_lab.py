import streamlit as st
import requests
import datetime
import time
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

st.title("⚡ Smart Delta Ingestion Engine")
st.info("Cross-references BigQuery live with enhanced root-ID cleaning to eliminate UNMAPPED tags.")

PROJECT_ID = "sensorpush-export" 
DATASET_ID = "Temperature"      
TABLE_ID = "raw_sensorpush"
INVENTORY_TABLE = "hardware_inventory"

TARGET_ACCOUNT = {'email': 'ldunham@soilfreeze.com', 'password': 'Freeze123!!'}
BASE_URL = "https://api.sensorpush.com/api/v1"

if st.button("🚀 Run Smart Delta Ingestion", type="primary"):
    all_rows = []
    hardware_map = {}
    db_max_timestamps = {}  # Tracks the latest date BigQuery knows about per node
    node_stats = {}         # Tracks total vs new counts per node
    
    with st.status("Executing Enhanced Delta Pass...", expanded=True) as status:
        try:
            if "gcp_service_account" in st.secrets:
                info = st.secrets["gcp_service_account"]
                credentials = service_account.Credentials.from_service_account_info(info)
                client = bigquery.Client(credentials=credentials, project=info["project_id"])
            else:
                client = bigquery.Client(project=PROJECT_ID)
            
            # --- STEP 1: MAP SENSORS USING CLEAN ROOT KEY ---
            st.write("🔍 Loading Translation Maps...")
            query = f"SELECT RawID, NodeNum FROM `{PROJECT_ID}.{DATASET_ID}.{INVENTORY_TABLE}` WHERE RawID IS NOT NULL"
            for row in client.query(query):
                # Extract the 8-digit base serial number from your inventory table
                clean_db_id = str(row.RawID).split('.')[0].strip()
                hardware_map[clean_db_id] = str(row.NodeNum).strip()
                
            # --- STEP 2: LOOKUP LATEST DATABASE TIMESTAMPS ---
            st.write("📅 Querying current database bookmarks...")
            time_query = f"""
                SELECT NodeNum, MAX(timestamp) as max_time 
                FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` 
                GROUP BY NodeNum
            """
            for row in client.query(time_query):
                if row.max_time:
                    db_max_timestamps[str(row.NodeNum)] = row.max_time.isoformat()

        except Exception as e:
            st.error(f"Database sync check failed: {e}")
            st.stop()

        # Bounding window setup (Current Date)
        start_time_iso = "2026-05-28T00:00:00Z"
        end_time_iso = "2026-05-28T23:59:59Z"

        # --- STEP 3: FETCH FROM CLOUD ---
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
                # Extract the 8-digit base root number from the live API broadcast string
                api_root_id = str(s_id).split('.')[0].strip()
                
                # Match them together at the root level!
                friendly_name = hardware_map.get(api_root_id)
                
                if not friendly_name:
                    friendly_name = f"UNMAPPED-{api_root_id}"
                
                # Initialize node tracker if not seen yet
                if friendly_name not in node_stats:
                    node_stats[friendly_name] = {"Downloaded": 0, "New Unique Appends": 0}

                # Get latest timestamp this node has inside BigQuery
                latest_db_time = db_max_timestamps.get(friendly_name, "")

                for s in samples:
                    observed_time = s['observed']
                    temp = s.get('temp_f') or s.get('temperature') or s.get('thermocouple_temperature')
                    
                    if temp is not None:
                        node_stats[friendly_name]["Downloaded"] += 1
                        
                        # DELTA CHECK: Is this specific data packet newer than what BigQuery has?
                        clean_observed = observed_time.replace('Z', '+00:00')
                        
                        if not latest_db_time or clean_observed > latest_db_time:
                            node_stats[friendly_name]["New Unique Appends"] += 1
                            all_rows.append({
                                "timestamp": observed_time,   
                                "NodeNum": str(friendly_name),
                                "temperature": float(temp),
                                "rssi": None
                            })

        except Exception as api_err:
            st.error(f"API pipeline failed: {api_err}")
            st.stop()

        # --- STEP 4: COMMIT ONLY TRUE NEW DATA ---
        total_new_rows = len(all_rows)
        if total_new_rows == 0:
            st.info("🔒 Safe! BigQuery is completely caught up. 0 duplicate rows written.")
            status.update(label="Database Already Current", state="complete")
        else:
            st.write(f"📥 Injecting {total_new_rows} genuinely new records straight into `{TABLE_ID}`...")
            real_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
            errors = client.insert_rows_json(real_table_ref, all_rows)
            
            if not errors:
                st.success(f"🎉 Delta Recovery Complete! Committed {total_new_rows} brand new rows.")
                status.update(label="Ingestion Successful!", state="complete")
            else:
                st.error(f"Database insertion errors: {errors[:3]}")
                status.update(state="error")

        # --- STEP 5: DISPLAY DETAILED DF SUMMARY MANIFEST ---
        if node_stats:
            st.write("### 📊 Smart Data Delta Tally:")
            summary_records = []
            for node, counts in node_stats.items():
                summary_records.append({
                    "Node Number": node,
                    "Total Points in Cloud Window": counts["Downloaded"],
                    "Genuinely New Points Appended": counts["New Unique Appends"]
                })
            
            summary_df = pd.DataFrame(summary_records).sort_values(by="Node Number")
            st.dataframe(summary_df, use_container_width=True, hide_index=True)
            if total_new_rows > 0:
                st.balloons()
