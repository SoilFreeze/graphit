import streamlit as st
import requests
import pandas as pd
import json

st.title("⚙️ SensorPush API Gateway Alignment")
st.info("Testing corrected authorization header syntax to bypass AWS Gateway blocks.")

c_api1, c_api2 = st.columns(2)
with c_api1:
    email = st.text_input("Account Email", value="soilfreeze98072@gmail.com")
with c_api2:
    password = st.text_input("Account Password", type="password")

st.divider()

if st.button("📡 Execute Signed API Request", type="primary"):
    if not email or not password:
        st.error("Please enter credentials.")
    else:
        auth_url = "https://api.sensorpush.com/api/v1/oauth/authorize"
        
        # We are going to hit the specific GATEWAY samples endpoint
        sensors_url = "https://api.sensorpush.com/api/v1/sensors"
        
        try:
            with st.status("Executing Pipeline test...", expanded=True) as status:
                # Step 1: Request OAuth Token
                st.write("🔒 Requesting gateway access token...")
                auth_payload = {"email": email, "password": password}
                auth_res = requests.post(auth_url, json=auth_payload, timeout=15)
                auth_json = auth_res.json()
                
                token = auth_json.get("authorization")
                
                if not token:
                    st.error(f"OAuth failed: {auth_json}")
                    status.update(label="OAuth Failure", state="error")
                    return
                
                st.write("🔑 Token retrieved successfully.")
                
                # Step 2: Build EXACT request headers required by SensorPush API Specification
                # Often, SensorPush requires headers to accept application/json with explicit token casing
                headers = {
                    "accept": "application/json",
                    "Content-Type": "application/json",
                    "Authorization": token  # Verify if your system architecture maps this as a string literal
                }
                
                st.write("📡 Submitting payload to /v1/sensors...")
                # Note: The SensorPush API requires an empty JSON object '{}' as the body for a blank query
                res = requests.post(sensors_url, headers=headers, json={}, timeout=15)
                res_data = res.json()
                
                # Check if we got hit with the same AWS gateway error message
                if isinstance(res_data, dict) and "statusCode" in res_data:
                    st.error(f"❌ Gateway Rejected Request: {res_data.get('message')}")
                    
                    st.write("### 🛠️ Alternative Attempt: Retrying with Bearer format...")
                    headers["Authorization"] = f"Bearer {token}"
                    res = requests.post(sensors_url, headers=headers, json={}, timeout=15)
                    res_data = res.json()
                
                st.write("### 📦 Response Received from SensorPush:")
                st.json(res_data)
                
                if isinstance(res_data, dict) and "statusCode" not in res_data:
                    status.update(label="Success! Bypassed Gateway Error.", state="complete")
                    
                    # Output data to dataframe
                    rows = []
                    for s_id, details in res_data.items():
                        if isinstance(details, dict):
                            rows.append({
                                "Sensor_ID": s_id,
                                "Name_On_SensorPush_Cloud": details.get("name", "N/A"),
                                "Active": details.get("active", "N/A")
                            })
                    st.dataframe(pd.DataFrame(rows), use_container_width=True)
                    
        except Exception as e:
            st.error(f"Pipeline crashed: {e}")
