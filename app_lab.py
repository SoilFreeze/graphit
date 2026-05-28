import streamlit as st
import requests
import json

st.title("🔍 SensorPush API Inspector")
st.info("Querying the live API to see exactly what names are stored on the SensorPush cloud.")

# 1. Grab your existing credentials straight from your secure secrets file
try:
    if "sensorpush_api" in st.secrets:
        email = st.secrets["sensorpush_api"]["email"]
        password = st.secrets["sensorpush_api"]["password"]
    elif "gcp_service_account" in st.secrets:
        # Fallback helper to peek if credentials are under a different key
        email = "soilfreeze98072@gmail.com"
        password = st.secrets.get("sensorpush_password", "")
    else:
        st.warning("Could not find 'sensorpush_api' credentials in st.secrets. Please type them below:")
        email = st.text_input("SensorPush Email", value="soilfreeze98072@gmail.com")
        password = st.text_input("SensorPush Password", type="password")
except Exception as e:
    st.error(f"Secrets read error: {e}")

if email and password:
    if st.button("📡 Fetch Live Sensor Samples", type="primary"):
        auth_url = "https://api.sensorpush.com/api/v1/oauth/authorize"
        gate_url = "https://api.sensorpush.com/api/v1/sensors"
        
        try:
            # Step A: Get an authorization token
            st.text("1. Authenticating with SensorPush Cloud...")
            auth_res = requests.post(auth_url, json={"email": email, "password": password}, timeout=15)
            auth_data = auth_res.json()
            
            if "authorization" not in auth_data:
                st.error(f"Authentication failed: {auth_data}")
            else:
                token = auth_data["authorization"]
                headers = {"accept": "application/json", "Authorization": token}
                
                # Step B: Request all live sensor metadata fields
                st.text("2. Pulling cloud sensor configurations...")
                sample_res = requests.post(gate_url, headers=headers, json={}, timeout=15)
                sensor_data = sample_res.json()
                
                # Step C: Render the results clearly on your screen
                st.success(f"Successfully retrieved {len(sensor_data)} sensors from the API!")
                
                # Turn the dict into a dataframe so we can filter and search it live
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
                st.dataframe(df, use_container_width=True)
                
                # Highlight what's happening with the new T21 series
                st.write("### 🎯 Quick Filter Search (Looking for T21 or 0321 series)")
                test_match = df[df['API_Sensor_Name'].str.contains('032|T21', na=False, case=False)]
                if not test_match.empty:
                    st.json(test_match.to_dict(orient='records'))
                else:
                    st.warning("No sensors containing '032' or 'T21' found in the API 'name' text field.")
                    st.write("First 5 raw rows from API for context:")
                    st.json(rows[:5])
                    
        except Exception as e:
            st.error(f"API Connection error: {e}")
