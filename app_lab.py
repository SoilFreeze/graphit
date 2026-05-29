import streamlit as st
import requests
import pandas as pd

st.title("🧪 SensorPush API Sandbox Extractor")
st.write("Click the button below to pull a raw mapping straight from the SensorPush API endpoints.")

ACCOUNTS = [
    {'email': 'tsteele@soilfreeze.com', 'password': 'Freeze123!!'},
    {'email': 'ldunham@soilfreeze.com', 'password': 'Freeze123!!'},
    {'email': 'soilfreeze98072@gmail.com', 'password': 'Freeze123!!'}
]
BASE_URL = "https://api.sensorpush.com/api/v1"

if st.button("🚀 Fetch Live API Map"):
    api_records = []
    
    with st.spinner("Pinging API Gateways..."):
        for acc in ACCOUNTS:
            try:
                auth_r = requests.post(f"{BASE_URL}/oauth/authorize", json=acc, timeout=15).json()
                token = requests.post(f"{BASE_URL}/oauth/accesstoken", json={"authorization": auth_r['authorization']}, timeout=15).json().get('accesstoken')
                
                # Pull raw active sensor data
                s_resp = requests.post(f"{BASE_URL}/devices/sensors", headers={"Authorization": token}, json={}, timeout=20).json()
                
                if isinstance(s_resp, dict):
                    for s_id, s_meta in s_resp.items():
                        clean_id = str(s_id).strip().split('.')[0]
                        app_name = s_meta.get('name', 'Unknown') if isinstance(s_meta, dict) else 'Unknown'
                        api_records.append({
                            "Account Owner": acc['email'], 
                            "Sensor ID (RawID)": clean_id, 
                            "App Name (NodeNum)": app_name
                        })
                else:
                    for s in s_resp:
                        if isinstance(s, dict) and 'id' in s:
                            clean_id = str(s['id']).strip().split('.')[0]
                            app_name = s.get('name', 'Unknown')
                            api_records.append({
                                "Account Owner": acc['email'], 
                                "Sensor ID (RawID)": clean_id, 
                                "App Name (NodeNum)": app_name
                            })
            except Exception as e:
                st.error(f"Error accessing profiles for {acc['email']}: {e}")

    if api_records:
        st.success(f"Successfully retrieved {len(api_records)} sensor profiles!")
        
        # Turn it into a clean dataframe for Streamlit to display
        df = pd.DataFrame(api_records)
        st.dataframe(df, use_container_width=True)
    else:
        st.warning("No data returned from the API.")
