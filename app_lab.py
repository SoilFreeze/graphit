import streamlit as st
import requests

st.title("📦 Historical Data Backfill Gateway (Hardware Mapping)")
st.info("Routes multi-day historical telemetry pulls using raw hardware tokens and URL parameters.")

historical_url = st.text_input(
    "Historical Cloud Run URL", 
    value="https://sensorpush-historical-recovery-1013288934882.us-west1.run.app"
)

st.divider()

if st.button("🚀 Execute History Injection", type="primary"):
    # 1. Format the target URL exactly as the container requested it
    base_endpoint = historical_url.strip().split('?')[0]
    parameterized_url = f"{base_endpoint}?start=2026-05-14&end=2026-05-28"
    
    st.write(f"🔗 Target: `{parameterized_url}`")
    
    # 2. Hardcoding the raw numeric keys we found in your API log to guarantee a match
    raw_hardware_tokens = [
        "17050089", "17050116", "17049872", "17049943", "17049918", 
        "17050051", "17050090", "17049836", "17049889", "17049841"
    ]
    
    payload = {
        "sensors": raw_hardware_tokens
    }
    
    try:
        with st.spinner("Streaming data block to historical container... Please wait."):
            # Giving it a 120-second threshold to cleanly finish processing
            response = requests.post(parameterized_url, json=payload, timeout=120)
            
            if response.status_code == 200:
                st.success("🎉 Historical container processed the hardware tokens successfully!")
                st.write("### 📦 Server Execution Response Details:")
                st.write(response.text)
            else:
                st.error(f"❌ Historical Container returned status {response.status_code}: {response.text}")
                
    except Exception as e:
        st.error(f"Failed to connect to historical service container: {e}")
