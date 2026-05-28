import streamlit as st
import pandas as pd
import requests
import datetime
from google.cloud import bigquery

# =============================================================================
# 1. CORE LAYOUT & CONFIGURATION
# =============================================================================
st.set_page_config(page_title="RSSI Real-Time Tester", page_icon="🧪", layout="wide")

PROJECT_ID = "sensorpush-export" 
DATASET_ID = "Temperature"      
METADATA_TABLE = "metadata_snapshot" 

BASE_URL = "https://api.sensorpush.com/api/v1"
ACCOUNTS = [
    {'email': 'tsteele@soilfreeze.com', 'password': 'Freeze123!!'},
    {'email': 'ldunham@soilfreeze.com', 'password': 'Freeze123!!'},
    {'email': 'soilfreeze98072@gmail.com', 'password': 'Freeze123!!'}
]

st.title("🧪 Real-Time Temperature & RSSI Fetch Verification")
st.markdown(
    """
    This sandbox app automatically pulls fresh data **directly from the SensorPush API** on demand.
    It bypasses the raw data storage tables entirely, allowing you to verify that data and signal mapping work before saving.
    """
)

# =============================================================================
# 2. CLIENT & REGISTRY LOADER
# =============================================================================
@st.cache_resource
def get_verification_bq_client():
    return bigquery.Client(project=PROJECT_ID)

client = get_verification_bq_client()

@st.cache_data(ttl=300)
def load_hardware_mappings():
    """Loads current asset mappings from the active snapshot table."""
    name_map = {}
    try:
        query = f"SELECT PhysicalID, NodeNum FROM `{PROJECT_ID}.{DATASET_ID}.{METADATA_TABLE}` WHERE PhysicalID IS NOT NULL"
        for row in client.query(query):
            p_id = str(row.PhysicalID).split('.')[0].strip()
            name_map[p_id] = str(row.NodeNum).strip()
        return name_map
    except Exception as e:
        st.error(f"Failed loading registry mapping rules from BigQuery: {e}")
        return {}

name_map = load_hardware_mappings()

# =============================================================================
# 3. INTERACTIVE CONTROL WORKSPACE
# =============================================================================
st.sidebar.header("⚙️ Query Parameters")
lookback_hours = st.sidebar.slider("Lookback Window (Hours)", min_value=1, max_value=12, value=2)
unit_mode = st.sidebar.radio("Temperature Scale Display", ["Fahrenheit", "Celsius"])

if st.button("⚡ Query Live Temperatures & Signal Strengths", type="primary", use_container_width=True):
    all_live_records = []
    
    # Establish UTC time ranges matching API specification formats
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    start_time_str = (now_utc - datetime.timedelta(hours=lookback_hours)).strftime('%Y-%m-%dT%H:%M:%S+0000')
    
    progress_bar = st.progress(0)
    
    # Loop over accounts to compile across all 70 sensors
    for index, acc in enumerate(ACCOUNTS):
        st.caption(f"Processing gateway pipe for: `{acc['email']}`...")
        try:
            # Step A: Authentication Handshake
            auth_r = requests.post(f"{BASE_URL}/oauth/authorize", json=acc, timeout=15).json()
            token = requests.post(f"{BASE_URL}/oauth/accesstoken", json={"authorization": auth_r['authorization']}, timeout=15).json().get('accesstoken')

            # Step B: Capture Device Metadata Root (Contains the real-time RSSI integers)
            s_resp = requests.post(f"{BASE_URL}/devices/sensors", headers={"Authorization": token}, json={}, timeout=20).json()
            
            device_rssi_map = {}
            if isinstance(s_resp, dict):
                sensor_ids = list(s_resp.keys())
                for s_id, s_meta in s_resp.items():
                    if isinstance(s_meta, dict) and 'rssi' in s_meta:
                        device_rssi_map[str(s_id)] = s_meta.get('rssi')
            else:
                sensor_ids = [s['id'] for s in s_resp]
                for s in s_resp:
                    if isinstance(s, dict) and 'id' in s and 'rssi' in s:
                        device_rssi_map[str(s['id'])] = s.get('rssi')

            # Step C: Fetch Samples for Current Array
            for i in range(0, len(sensor_ids), 10):
                chunk = sensor_ids[i:i+10]
                payload = {"limit": 100, "startTime": start_time_str, "sensors": chunk}
                r_samples = requests.post(f"{BASE_URL}/samples", headers={"Authorization": token}, json=payload, timeout=45).json()
                
                sensors_data = r_samples.get('sensors', {})
                for s_id, samples in sensors_data.items():
                    clean_id = str(s_id).split('.')[0]
                    friendly_name = name_map.get(clean_id, s_id)
                    current_device_rssi = device_rssi_map.get(str(s_id))
                    
                    # Sort historical samples to find the absolute newest record packet
                    if samples:
                        sorted_samples = sorted(samples, key=lambda k: k['observed'], reverse=True)
                        newest_sample = sorted_samples[0]
                        
                        raw_temp = newest_sample.get('temp_f') or newest_sample.get('temperature') or newest_sample.get('thermocouple_temperature')
                        
                        if raw_temp is not None:
                            # Apply scale conversion based on your display preferences
                            if unit_mode == "Celsius":
                                display_temp = (float(raw_temp) - 32) * 5/9
                                label = f"{display_temp:.1f}°C"
                            else:
                                label = f"{float(raw_temp):.1f}°F"
                                
                            all_live_records.append({
                                "Node ID": friendly_name,
                                "Physical Hash ID": s_id,
                                "Last Ping Time": newest_sample.get('observed'),
                                "Temperature Reading": label,
                                "Signal Strength (RSSI)": f"{int(current_device_rssi)} dBm" if current_device_rssi is not None else "N/A",
                                "Raw RSSI Int": current_device_rssi if current_device_rssi is not None else -999
                            })
        except Exception as api_err:
            st.sidebar.error(f"Error on account {acc['email']}: {api_err}")
            
        progress_bar.progress((index + 1) / len(ACCOUNTS))

    # =============================================================================
    # 4. RENDER PREVIEW GRIDS
    # =============================================================================
    st.divider()
    if all_live_records:
        df_display = pd.DataFrame(all_live_records)
        
        # Sort chronologically by signal robustness (strongest connections up top)
        df_display = df_display.sort_values(by="Raw RSSI Int", ascending=False).drop(columns=["Raw RSSI Int"])
        
        st.subheader(f"📊 Live Data Stream Preview ({len(df_display)} Sensors Responding)")
        st.success("✅ Verification Complete! The API data structure is intact and ready for production integration.")
        
        st.dataframe(
            df_display,
            use_container_width=True,
            hide_index=True
        )
    else:
        st.warning("No fresh temperature points discovered inside this lookback time frame. Try increasing your lookback slider context.")
