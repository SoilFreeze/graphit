import streamlit as st
import requests
from datetime import datetime

st.title("📦 Historical Data Backfill Gateway")
st.info("Routes heavy multi-day telemetry requests directly to your dedicated historical Cloud Run service instance.")

# Paste your historical Cloud Run URL endpoint here
historical_url = st.text_input(
    "Historical Cloud Run URL", 
    value="https://sensorpush-historical-pull-seattle-1013288934882.us-west1.run.app"  # Update with your actual historical endpoint string
)

st.divider()

if st.button("🚀 Fire Deep History Backfill", type="primary"):
    if "historical" not in historical_url and "pull" not in historical_url:
        st.warning("⚠️ Double check that you've pasted your historical Cloud Run URL above, not the hourly one!")
        
    # Build the historical payload using your clean engineering tags
    target_nodes = [
        "TP-0320", "TP-0321", "TP-0322", "TP-0323", "TP-0324",
        "TP-0325", "TP-0326", "TP-0327", "TP-0328", "TP-0329"
    ]
    
    payload = {
        "sensors": target_nodes,
        "startTime": "2026-05-14T00:00:00Z",
        "endTime": "2026-05-28T23:59:59Z"
    }
    
    try:
        with st.spinner("Streaming data block to historical container... This may take up to a minute."):
            # We give this a long 120-second local read timeout to allow the historical function to finish its work
            response = requests.post(historical_url.strip(), json=payload, timeout=120)
            
            if response.status_code == 200:
                st.success("🎉 Historical backfill completed successfully!")
                st.write("### 📦 Server Execution Response Details:")
                st.write(response.text)
            else:
                st.error(f"❌ Historical Container returned status {response.status_code}: {response.text}")
                
    except Exception as e:
        st.error(f"Failed to connect to historical service container: {e}")
