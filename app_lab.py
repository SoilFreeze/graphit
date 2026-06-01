import streamlit as st
import pandas as pd
import requests

# Set page layout
st.set_page_config(
    page_title="SensorPush Fetcher",
    page_icon="❄️",
    layout="wide"
)

st.title("📋 SensorPush Account Master Inventory")
st.markdown("Queries the SensorPush Cloud API directly to pull a complete list of hardware sensors registered under each account profile.")

# Hardcoded Credential Array
BASE_URL = "https://api.sensorpush.com/api/v1"
ACCOUNTS = [
    {'email': 'tsteele@soilfreeze.com', 'password': 'Freeze123!!'},
    {'email': 'ldunham@soilfreeze.com', 'password': 'Freeze123!!'},
    {'email': 'soilfreeze98072@gmail.com', 'password': 'Freeze123!!'}
]

# Action Button
if st.button("🔍 Fetch Sensors From All Accounts", use_container_width=True):
    headers = {"accept": "application/json", "Content-Type": "application/json"}
    master_sensor_list = []
    
    # Progress indicators to keep the UI responsive and avoid blank screens
    progress_bar = st.progress(0)
    status_msg = st.empty()

    for idx, account in enumerate(ACCOUNTS):
        email = account['email']
        password = account['password']
        
        status_msg.markdown(f"🔐 Authenticating and downloading fleet list for: **{email}**...")
        session = requests.Session()
        
        try:
            # 1. Authorize (with explicit 10-second safety timeout)
            auth_res = session.post(
                f"{BASE_URL}/oauth/authorize", 
                json={"email": email, "password": password}, 
                headers=headers, 
                timeout=10
            )
            if auth_res.status_code != 200:
                st.error(f"⚠️ Authentication failed for {email}: {auth_res.text}")
                continue
            auth_code = auth_res.json().get("authorization")

            # 2. Get Access Token
            token_res = session.post(
                f"{BASE_URL}/oauth/accesstoken", 
                json={"authorization": auth_code}, 
                headers=headers, 
                timeout=10
            )
            access_token = token_res.json().get("accesstoken")
            session.headers.update({"Authorization": access_token})
            
            # 3. Pull Master Sensor Registry
            sensor_res = session.post(
                f"{BASE_URL}/devices/sensors", 
                json={}, 
                headers=headers, 
                timeout=10
            )
            sensors_dict = sensor_res.json()
            
            # 4. Pull Latest Samples to parse current hardware link status (RSSI / Last Seen)
            sample_res = session.post(
                f"{BASE_URL}/samples", 
                json={"limit": 1}, 
                headers=headers, 
                timeout=10
            )
            samples_dict = sample_res.json().get("sensors", {})
            
            # 5. Parse and append out raw properties
            for s_id, s_meta in sensors_dict.items():
                latest_samples = samples_dict.get(s_id, [])
                
                last_ping = "Never Seen"
                rssi_val = None
                
                if latest_samples:
                    last_ping = latest_samples[0].get("observed")
                    rssi_val = latest_samples[0].get("rssi")

                master_sensor_list.append({
                    "SensorPush Account": email,
                    "Sensor ID (NodeNum)": s_id,
                    "App Display Name": s_meta.get("name", "Unnamed Probes"),
                    "Type": s_meta.get("type", "Unknown"),
                    "Is Profile Active": s_meta.get("active", True),
                    "Last Gateway Check-In (UTC)": last_ping,
                    "Signal Strength (RSSI)": rssi_val
                })
                
        except requests.exceptions.Timeout:
            st.error(f"❌ Connection timeout trying to reach SensorPush servers for account: {email}")
        except Exception as e:
            st.error(f"❌ Unexpected error processing profile {email}: {e}")
            
        # Tick the progress bar layout
        progress_bar.progress((idx + 1) / len(ACCOUNTS))
        
    # Clear visual progress trackers once execution completes
    status_msg.empty()
    progress_bar.empty()

    # --- Render Final Results ---
    if master_sensor_list:
        df = pd.DataFrame(master_sensor_list)
        
        # Summary metrics banner
        st.success(f"🎉 Successfully gathered {len(df)} total sensors across your accounts.")
        
        # Group summary breakdown for clean verification
        st.write("### 📊 Breakdown by Account Profile")
        summary_counts = df["SensorPush Account"].value_counts().reset_index()
        summary_counts.columns = ["Account Profile", "Total Registered Sensors Found"]
        st.dataframe(summary_counts, use_container_width=True, hide_index=True)
        
        st.divider()
        
        # Interactive master inventory log
        st.write("### 📋 Master Sensor Inventory Log")
        st.dataframe(
            df.sort_values(by=["SensorPush Account", "App Display Name"]),
            use_container_width=True,
            hide_index=True,
            column_config={
                "Signal Strength (RSSI)": st.column_config.NumberColumn(
                    help="Gateway connection status strength. Values lower than -90dBm indicate severe transmission issues.",
                    format="%d dBm"
                )
            }
        )
    else:
        st.error("No hardware assets could be returned from any of the configured account profiles.")
