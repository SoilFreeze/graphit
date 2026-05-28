import streamlit as st
import requests
import pandas as pd  # Explicitly imported for the sandbox dataframe
import json

st.title("🔍 SensorPush API Inspector")
st.info("Querying the live API to see exactly what names are stored on the SensorPush cloud.")

# Manual input boxes so you don't have to rely on hidden secret files
c_api1, c_api2 = st.columns(2)
with c_api1:
    email = st.text_input("SensorPush Account Email", value="soilfreeze98072@gmail.com")
with c_api2:
    password = st.text_input("SensorPush Account Password", type="password")

st.divider()

# The button will now draw on the screen no matter what
if st.button("📡 Fetch Live Sensor Registry From API", type="primary"):
    if not email or not password:
        st.error("Please enter both your SensorPush email and password to connect.")
    else:
        auth_url = "https://api.sensorpush.com/api/v1/oauth/authorize"
        gate_url = "https://api.sensorpush.com/api/v1/sensors"
        
        try:
            with st.status("Connecting to SensorPush API...", expanded=True) as status:
                # Step A: Get an authorization token
                st.write("1. Requesting security token from SensorPush cloud...")
                auth_res = requests.post(auth_url, json={"email": email, "password": password}, timeout=15)
                auth_data = auth_res.json()
                
                if "authorization" not in auth_data:
                    st.error(f"Authentication failed: {auth_data}")
                    status.update(label="Authentication Failed", state="error")
                else:
                    token = auth_data["authorization"]
                    headers = {"accept": "application/json", "Authorization": token}
                    
                    # Step B: Request all live sensor metadata fields
                    st.write("2. Pulling live cloud hardware configuration array...")
                    sample_res = requests.post(gate_url, headers=headers, json={}, timeout=15)
                    sensor_data = sample_res.json()
                    
                    status.update(label=f"Successfully fetched {len(sensor_data)} sensors!", state="complete")
                    
                    # Step C: Render the results into a clean search table
                    rows = []
                    for sensor_id, details in sensor_data.items():
                        rows.append({
                            "API_Hardware_ID": sensor_id,
                            "API_Sensor_Name": details.get("name", "N/A"),
                            "Active": details.get("active", True),
                            "Type": details.get("type", "Unknown")
                        })
                    
                    df = pd.DataFrame(rows)
                    
                    st.write("### 📊 Live API Registry Table")
                    st.write("You can click the columns to sort or use the search icon on the top right of this table to find specific IDs!")
                    st.dataframe(df, use_container_width=True)
                    
                    # Step D: Isolate your new fleet context
                    st.write("### 🎯 Deep Target Matches (Looking for T21 or 0321 series strings)")
                    test_match = df[df['API_Sensor_Name'].str.contains('032|T21|321', na=False, case=False)]
                    if not test_match.empty:
                        st.dataframe(test_match, use_container_width=True)
                    else:
                        st.warning("No sensors containing '032' or 'T21' found in the API name text fields.")
                        st.write("Showing the first 5 raw rows from the API for structural comparison:")
                        st.json(rows[:5])
                        
        except Exception as e:
            st.error(f"API Network Connection error: {e}")
