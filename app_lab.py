import requests
import json
import pandas as pd
from datetime import datetime

# API Configuration
API_BASE_URL = "https://api.sensorpush.com/api/v1"
EMAIL = "your_sensorpush_email@example.com"
PASSWORD = "your_sensorpush_password"

def audit_sensorpush_hardware():
    session = requests.Session()
    headers = {"accept": "application/json", "Content-Type": "application/json"}
    
    # 1. Authenticate & Retrieve Authorization Token
    print("🔐 Authenticating with SensorPush Cloud...")
    auth_payload = {"email": EMAIL, "password": PASSWORD}
    auth_res = session.post(f"{API_BASE_URL}/oauth/authorize", json=auth_payload, headers=headers)
    if auth_res.status_code != 200:
        print(f"❌ Auth Failed: {auth_res.text}")
        return
    auth_code = auth_res.json().get("authorization")

    # 2. Exchange for Access Token
    token_payload = {"authorization": auth_code}
    token_res = session.post(f"{API_BASE_URL}/oauth/accesstoken", json=token_payload, headers=headers)
    access_token = token_res.json().get("accesstoken")
    
    # Update session headers to utilize the new bearer authorization
    session.headers.update({"Authorization": access_token})
    
    # 3. Pull Complete Sensor Inventory
    print("📡 Pulling Master Sensor List...")
    sensor_res = session.post(f"{API_BASE_URL}/devices/sensors", json={})
    sensors_dict = sensor_res.json() # Keyed by string sensor IDs
    
    # 4. Pull Latest Samples to parse RSSI and Last Seen Time
    print("📊 Pulling latest telemetry blocks...")
    # Limiting payload to the single latest sample per sensor to verify link status
    sample_payload = {"limit": 1} 
    sample_res = session.post(f"{API_BASE_URL}/samples", json=sample_payload)
    samples_dict = sample_res.json().get("sensors", {})

    # 5. Compile Hardware Audit Matrix
    audit_records = []
    for s_id, s_meta in sensors_dict.items():
        # SensorPush objects package specific sample info inside the global samples block
        latest_samples = samples_dict.get(s_id, [])
        
        last_seen = "N/A"
        rssi = "N/A"
        
        if latest_samples:
            latest_point = latest_samples[0]
            last_seen = latest_point.get("observed") # ISO Timestamp format
            rssi = latest_point.get("rssi", "N/A")

        audit_records.append({
            "Sensor ID (NodeNum)": s_id,
            "Name": s_meta.get("name", "Unnamed Sensor"),
            "Active Profile": s_meta.get("active", True),
            "Last Cloud Ping": last_seen,
            "Signal Strength (RSSI)": rssi
        })
        
    df = pd.DataFrame(audit_records)
    print("\n📦 --- SENSORPUSH HARDWARE METRIC AUDIT ---")
    print(df.to_string(index=False))
    return df

if __name__ == "__main__":
    audit_sensorpush_hardware()
