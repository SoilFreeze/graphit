import streamlit as st
import requests
from datetime import datetime, timedelta

st.title("🚀 Targeted History Backfill Gateway")
st.info("Bypasses standard sync routines to directly force a heavy historical telemetry pull.")

# Target your specific Cloud Run deployment
base_url = "https://sensorpush-hourly-sync-seattle-1013288934882.us-west1.run.app"

st.write("### 📅 Select Recovery Parameters")
c1, c2 = st.columns(2)
with c1:
    start_dt = st.date_input("Start Date", value=datetime(2026, 5, 14))
with c2:
    end_dt = st.date_input("End Date", value=datetime(2026, 5, 28))

if st.button("⚡ Fire Deep Historical Recovery Pipeline", type="primary"):
    # Target the dedicated recovery route built into your container schema
    recovery_endpoint = f"{base_url.rstrip('/')}/recovery"
    
    # We pass the clean names directly because your container knows how to look them up!
    target_nodes = [
        "TP-0320", "TP-0321", "TP-0322", "TP-0323", "TP-0324",
        "TP-0325", "TP-0326", "TP-0327", "TP-0328", "TP-0329"
    ]
    
    payload = {
        "nodes": target_nodes,
        "startTime": f"{start_dt}T00:00:00Z",
        "endTime": f"{end_dt}T23:59:59Z",
        "async_processing": True  # Tells the container to return immediate success while processing in the background
    }
    
    try:
        with st.spinner("Submitting historical request array to container gateway..."):
            response = requests.post(recovery_endpoint, json=payload, timeout=15)
            
            if response.status_code == 200:
                st.success("✅ Backfill Accepted by Cloud Run!")
                st.write("The container has initialized a background processing thread. Data will stream into BigQuery over the next few minutes.")
                st.json(response.json())
            else:
                st.error(f"Endpoint returned status {response.status_code}: {response.text}")
                st.info("Retrying with fallback alternative payload layout...")
                
                # Alternate key matching if your container expects 'sensors' instead of 'nodes'
                payload_alt = {
                    "sensors": target_nodes,
                    "startTime": f"{start_dt}T00:00:00Z",
                    "endTime": f"{end_dt}T23:59:59Z"
                }
                alt_res = requests.post(base_url.rstrip('/'), json=payload_alt, timeout=15)
                st.write("Fallback Execution Response:")
                st.write(alt_res.text)
                
    except Exception as e:
        st.error(f"Connection failed: {e}")
