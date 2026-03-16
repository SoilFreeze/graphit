import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. Setup
st.set_page_config(page_title="Geotechnical Dashboard", layout="wide")

scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform"]
creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
client = bigquery.Client(credentials=creds, project="sensorpush-export")

# 2. Sidebar - Project Selection
st.sidebar.title("📁 Project Select")

@st.cache_data(ttl=300)
def get_full_dataset():
    query = "SELECT * FROM `sensorpush-export.sensor_data.monday_morning_depth_profile` ORDER BY timestamp ASC"
    return client.query(query).to_dataframe()

df_raw = get_full_dataset()
df_raw['timestamp'] = pd.to_datetime(df_raw['timestamp'])

available_projects = sorted(df_raw['Project'].unique())
selected_project = st.sidebar.selectbox("Choose Project", available_projects)

df_proj = df_raw[df_raw['Project'] == selected_project].copy()

# 3. Sidebar - Time Filter
num_weeks = st.sidebar.slider("Weeks of History", 1, 12, 2)
cutoff_date = pd.Timestamp.now(tz='UTC') - pd.Timedelta(weeks=num_weeks)
df_filtered = df_proj[df_proj['timestamp'] >= cutoff_date]

st.title(f"Project: {selected_project}")
tab_depth, tab_time = st.tabs(["📊 Depth Profiles", "📈 Temperature Trends"])

available_locations = sorted(df_filtered['Location'].unique())

# --- TAB: DEPTH PROFILES (Daily Snapshots to avoid "Scribble") ---
with tab_depth:
    st.subheader("Vertical Temperature Distribution (Daily Snapshots)")
    for loc in available_locations:
        with st.expander(f"Location: {loc}", expanded=True):
            df_loc = df_filtered[df_filtered['Location'] == loc].copy()
            
            # Keep only data from 8:00 AM each day to keep chart readable
            df_loc['hour'] = df_loc['timestamp'].dt.hour
            df_daily_snap = df_loc[df_loc['hour'] == 8] 
            
            if not df_daily_snap.empty:
                fig1, ax1 = plt.subplots(figsize=(8, 6))
                for ts in sorted(df_daily_snap['timestamp'].unique()):
                    subset = df_daily_snap[df_daily_snap['timestamp'] == ts]
                    label_date = pd.to_datetime(ts).strftime('%m/%d')
                    ax1.plot(subset['temperature'], subset['Depth'], marker='o', label=label_date)
                
                ax1.invert_yaxis()
                ax1.axvline(x=32, color='red', linestyle='--', alpha=0.7)
                ax1.set_xlabel("Temp (°F)")
                ax1.set_ylabel("Depth (ft)")
                ax1.legend(title="8:00 AM Daily", bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='x-small')
                st.pyplot(fig1)
            else:
                st.info("No 8:00 AM snapshots found. Try increasing the 'Weeks of History'.")

# --- TAB: TEMPERATURE TRENDS (Hourly Data) ---
with tab_time:
    st.subheader("Hourly Temperature Trends")
    for loc in available_locations:
        # Added expander here as requested
        with st.expander(f"Trends: {loc}", expanded=True):
            df_loc_time = df_filtered[df_filtered['Location'] == loc].sort_values('timestamp')
            
            if not df_loc_time.empty:
                fig2, ax2 = plt.subplots(figsize=(12, 5))
                for d in sorted(df_loc_time['Depth'].unique()):
                    subset = df_loc_time[df_loc_time['Depth'] == d]
                    # We use markersize=2 to see the actual hourly data points
                    ax2.plot(subset['timestamp'], subset['temperature'], label=f"{d}ft", linewidth=1, marker='o', markersize=2)
                    
                ax2.axhline(y=32, color='red', linestyle='--', alpha=0.8)
                ax2.set_ylabel("Temp (°F)")
                ax2.grid(True, alpha=0.3)
                ax2.legend(loc='upper left', bbox_to_anchor=(1, 1), fontsize='x-small')
                plt.xticks(rotation=45)
                st.pyplot(fig2)
