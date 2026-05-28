import requests
import streamlit as st

def fetch_sensor_api_sample():
    st.subheader("🔍 SensorPush API Live Sample")
    
    # Authenticate and pull from your active account credentials
    auth_url = "https://api.sensorpush.com/api/v1/oauth/authorize"
    gate_url = "https://api.sensorpush.com/api/v1/sensors"
    
    # Pulling directly from your secure Streamlit secrets configuration
    creds = st.secrets["sensorpush_api"] 
    
    try:
        with st.spinner("Connecting to SensorPush Cloud Gateway..."):
            # Step 1: Secure access token
            auth_payload = {"email": creds["email"], "password": creds["password"]}
            auth_res = requests.post(auth_url, json=auth_payload, timeout=15)
            token = auth_res.json().get("authorization")
            
            # Step 2: Request live hardware mapping samples
            headers = {"accept": "application/json", "Authorization": token}
            sample_res = requests.post(gate_url, headers=headers, json={}, timeout=15)
            sensor_data = sample_res.json()
            
            # Step 3: Isolate and display a few rows from your new T21 / Phase 2 fleet
            st.write("### Raw API Output Sample:")
            sample_count = 0
            for sensor_id, details in sensor_data.items():
                # Let's peek at a couple of entries to see their exact naming format
                if sample_count < 3:
                    st.json({sensor_id: details})
                    sample_count += 1
                    
    except Exception as e:
        st.error(f"Failed to fetch sample payload from API gateway: {e}")

# Run the inspector tool
fetch_sensor_api_sample()
