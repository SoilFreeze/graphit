import streamlit as st
import pandas as pd
import requests

st.set_page_config(page_title="API Debugger", layout="wide")

st.title("📡 SensorPush Link Test Sandbox")

BASE_URL = "https://api.sensorpush.com/api/v1"
ACCOUNTS = [
    {'email': 'tsteele@soilfreeze.com', 'password': 'Freeze123!!'},
    {'email': 'ldunham@soilfreeze.com', 'password': 'Freeze123!!'},
    {'email': 'soilfreeze98072@gmail.com', 'password': 'Freeze123!!'}
]

# Simple execution trigger
if st.button("🚀 Test Cloud Connections", use_container_width=True):
    all_records = []
    
    for account in ACCOUNTS:
        email = account['email']
        st.write(f"Connecting to account: `{email}`...")
        
        try:
            # Added explicit 10-second timeout gates to prevent blank screen freezes
            res = requests.post(
                f"{BASE_URL}/oauth/authorize", 
                json={"email": email, "password": account['password']}, 
                timeout=10
            )
            
            if res.status_code == 200:
                st.success(f"🟢 Connected to {email} successfully!")
                # Balance out records matrix
                all_records.append({"Account": email, "API Status": "Connected"})
            else:
                st.error(f"🔴 Auth Denied for {email}: {res.text}")
                
        except requests.exceptions.Timeout:
            st.error(f"❌ Connection Timed Out for {email}. The cloud network is unreachable.")
        except Exception as e:
            st.error(f"❌ Failed to reach API for {email}: {e}")
            
    if all_records:
        st.dataframe(pd.DataFrame(all_records), use_container_width=True)
