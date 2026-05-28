import streamlit as st
import requests
import time
from datetime import datetime, timedelta

st.title("🧩 Automated Chronological History Backfill")
st.info("Streams historical telemetry day-by-day to guarantee successful container ingestion without timeouts.")

# Target your historical Cloud Run endpoint
historical_url = st.text_input(
    "Historical Cloud Run URL", 
    value="https://sensorpush-historical-recovery-1013288934882.us-west1.run.app"
)

st.divider()

if st.button("⚡ Start Day-by-Day Processing Loop", type="primary"):
    base_endpoint = historical_url.strip().split('?')[0]
    
    # Define your missing timeframe range precisely
    start_date = datetime(2026, 5, 14)
    total_days = 15  # Spans from May 14th to May 28th
    
    # Explicit raw hardware IDs to match your API profiles perfectly
    raw_hardware_tokens = [
        "17050089", "17050116", "17049872", "17049943", "17049918", 
        "17050051", "17050090", "17049836", "17049889", "17049841"
    ]
    
    payload = {
        "sensors": raw_hardware_tokens
    }
    
    # Progress visualization for your sandbox UI
    progress_bar = st.progress(0)
    
    for i in range(total_days):
        current_day = start_date + timedelta(days=i)
        day_str = current_day.strftime("%Y-%m-%d")
        
        # Structure the target endpoint for ONE specific day loop
        parameterized_url = f"{base_endpoint}?start={day_str}&end={day_str}"
        
        st.write(f"📡 Processing date: `{day_str}`...")
        
        try:
            # Short 15-second timeout since single days process instantly
            response = requests.post(parameterized_url, json=payload, timeout=15)
            
            if response.status_code == 200:
                st.success(f"✅ Date {day_str} successfully processed and committed!")
            else:
                st.warning(f"⚠️ Date {day_str} returned code {response.status_code}: {response.text}")
                
        except Exception as e:
            st.error(f"❌ Connection dropped on date {day_str}: {e}")
            
        # Update progress tracking
        progress_bar.progress((i + 1) / total_days)
        
        # Small breathing room gap to keep API connections smooth
        time.sleep(1)
        
    st.success("🎉 Chronological backfill loop completed entirely!")
