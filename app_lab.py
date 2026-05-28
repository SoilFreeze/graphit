import streamlit as st
import requests
import pandas as pd
import json

st.title("🔍 SensorPush API Inspector")
st.info("Querying the live API to see exactly what names are stored on the SensorPush cloud.")

c_api1, c_api2 = st.columns(2)
with c_api1:
    email = st.text_input("SensorPush Account Email", value="soilfreeze98072@gmail.com")
with c_api2:
    password = st.text_input("SensorPush Account Password", type="password")

st.divider()

if st.button("📡 Fetch Live Sensor Registry From API", type="primary"):
    if not email or not password:
        st.error("Please enter both your SensorPush email and password to connect.")
    else:
        auth_url = "https://api.sensorpush.com/api/v1/oauth/authorize"
        gate_url = "https://api.sensorpush.com/api/v1/sensors"
        
        try:
            with st.status("Connecting to SensorPush API...", expanded=True) as status:
                st.write("1. Requesting security token from SensorPush cloud...")
                auth_res = requests.post(auth_url, json={"email": email, "password": password}, timeout=15)
                auth_data = auth_res.json()
                
                if "authorization" not in auth_data:
                    st.error(f"Authentication failed: {auth_data}")
                    status.update(label="Authentication Failed", state="error")
                else:
                    token = auth_data["authorization"]
                    headers = {"accept": "application/json", "Authorization": token}
                    
                    st.write("2. Pulling live cloud hardware configuration array...")
                    sample_res = requests.post(gate_url, headers=headers, json={}, timeout=15)
                    sensor_data = sample_res.json()
                    
                    status.update(label=f"Data retrieved successfully!", state="complete")
                    
                    st.write("### 📜 Raw Structural Type Received")
                    st.write(f"Data is type: `{type(sensor_data)}` with length: `{len(sensor_data)}`")
                    
                    rows = []
                    
                    # SYSTEM A: If the API returned a dictionary mapping ID -> Detail Dict
                    if isinstance(sensor_data, dict):
                        for k, v in sensor_data.items():
                            if isinstance(v, dict):
                                rows.append({
                                    "Extracted_ID": k,
                                    "Stored_Name": v.get("name", "N/A"),
                                    "Raw_Payload_Object": str(v)
                                })
                            else:
                                rows.append({
                                    "Extracted_ID": k,
                                    "Stored_Name": str(v),
                                    "Raw_Payload_Object": "Plain String Element"
                                })
                                
                    # SYSTEM B: If the API returned a flat list of items/strings
                    elif isinstance(sensor_data, list):
                        for idx, item in enumerate(sensor_data):
                            rows.append({
                                "Extracted_ID": f"Index_{idx}",
                                "Stored_Name": str(item),
                                "Raw_Payload_Object": "Flat List Element"
                            })
                    
                    df = pd.DataFrame(rows)
                    
                    st.write("### 📊 Decoded API Names Table")
                    st.dataframe(df, use_container_width=True)
                    
                    # Show the absolute raw payload text so we can read it directly
                    st.write("### 📦 Exact JSON Payload String From Server:")
                    st.json(sensor_data)
                        
        except Exception as e:
            st.error(f"API Network Connection error: {e}")
