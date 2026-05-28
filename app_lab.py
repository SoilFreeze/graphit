import streamlit as st
import requests

st.title("🎯 Raw API Payload Mirror")
st.info("Bypasses all internal database logic to display the exact strings the SensorPush API is sending right now.")

TARGET_ACCOUNT = {'email': 'ldunham@soilfreeze.com', 'password': 'Freeze123!!'}
BASE_URL = "https://api.sensorpush.com/api/v1"

if st.button("🔍 Inspect Raw Cloud Stream", type="primary"):
    with st.status("Fetching Raw JSON Payload...", expanded=True) as status:
        try:
            # 1. Authenticate
            auth_r = requests.post(f"{BASE_URL}/oauth/authorize", json=TARGET_ACCOUNT, timeout=15).json()
            token = requests.post(f"{BASE_URL}/oauth/accesstoken", json={"authorization": auth_r['authorization']}, timeout=15).json().get('accesstoken')

            # 2. Grab today's raw samples window
            start_time_iso = "2026-05-28T00:00:00Z"
            payload = {"startTime": start_time_iso}
            
            st.write("📡 Polling `/samples` endpoint...")
            r_samples = requests.post(f"{BASE_URL}/samples", headers={"Authorization": token}, json=payload, timeout=45).json()
            
            sensors_dict = r_samples.get('sensors', {})
            
            if not sensors_dict:
                st.error("❌ The cloud server literally returned an empty sensors dictionary for today's date.")
                status.update(state="error")
            else:
                st.success(f"📊 Connected! Found {len(sensors_dict.keys())} raw sensor keys transmitting today.")
                
                # Build a display layout of the literal keys and sample counts
                raw_manifest = []
                for sample_key, sample_list in sensors_dict.items():
                    raw_manifest.append({
                        "Literal API Sensor ID Key": str(sample_key),
                        "Samples Recorded Today": len(sample_list),
                        "First Sample Raw Data": sample_list[0] if len(sample_list) > 0 else "None"
                    })
                
                st.write("### 📦 Literal API Response Manifest:")
                st.dataframe(raw_manifest)
                status.update(label="Inspection Complete", state="complete")
                
        except Exception as e:
            st.error(f"API Connection Failed: {e}")
            status.update(state="error")
