import streamlit as st
import requests

st.title("📦 Historical Data Backfill Gateway")
st.info("Routes heavy multi-day telemetry requests using URL query-string string interpolation tags.")

# Paste your historical Cloud Run URL endpoint here
historical_url = st.text_input(
    "Historical Cloud Run URL", 
    value="https://sensorpush-historical-pull-seattle-1013288934882.us-west1.run.app"
)

st.divider()

if st.button("🚀 Fire Deep History Backfill", type="primary"):
    # 1. INTERPOLATION LAYER: Append the specific query syntax the container requested
    base_endpoint = historical_url.strip().split('?')[0] # strip any accidental old params
    parameterized_url = f"{base_endpoint}?start=2026-05-14&end=2026-05-28"
    
    st.write(f"🔗 Targeted Request URL: `{parameterized_url}`")
    
    # 2. Build the target nodes array using your clean engineering tags
    target_nodes = [
        "TP-0320", "TP-0321", "TP-0322", "TP-0323", "TP-0324",
        "TP-0325", "TP-0326", "TP-0327", "TP-0328", "TP-0329"
    ]
    
    payload = {
        "sensors": target_nodes
    }
    
    try:
        with st.spinner("Streaming structured data block to historical container... Please wait."):
            # Giving it a 120-second threshold to cleanly finish processing
            response = requests.post(parameterized_url, json=payload, timeout=120)
            
            if response.status_code == 200:
                st.success("🎉 Telemetry ingestion pass completed successfully!")
                st.write("### 📦 Server Execution Response Details:")
                st.write(response.text)
            else:
                st.error(f"❌ Historical Container returned status {response.status_code}: {response.text}")
                
    except Exception as e:
        st.error(f"Failed to connect to historical service container: {e}")
