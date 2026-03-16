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
    # We pull everything, then filter in Pandas to keep the app snappy
    query = "SELECT * FROM `sensorpush-export.sensor_data.monday_morning_depth_profile` ORDER BY timestamp DESC"
    return client.query(query).to_dataframe()

df_raw = get_full_dataset()
df_raw['timestamp'] = pd.to_datetime(df_raw['timestamp'])

available_projects = sorted(df_raw['Project'].unique())
selected_project = st.sidebar.selectbox("Choose Project", available_projects)

# Filter by Project
df_proj = df_raw[df_raw['Project'] == selected_project].copy()

# 3. Sidebar - Time Filter (Hourly)
num_weeks = st.sidebar.slider("Weeks of History", 1, 12, 2)
cutoff_date = pd.Timestamp.now(tz='UTC') - pd.Timedelta(weeks=num_weeks)
df_filtered = df_proj[df_proj['timestamp'] >= cutoff_date].sort_values('timestamp')

# 4. Main UI Layout
st.title(f"Project: {selected_project}")
tab_depth, tab_time = st.tabs(["📊 Depth Profiles", "📈 Temperature Trends"])

available_locations = sorted(df_filtered['Location'].unique())

# --- TAB: DEPTH PROFILES ---
with tab_depth:
    st.subheader("Vertical Temperature Distribution")
    # If multiple locations, let user toggle or show all
    for loc in available_locations:
        with st.expander(f"Location: {loc}", expanded=True):
            df_loc = df_filtered[df_filtered['Location'] == loc]
            fig1, ax1 = plt.subplots(figsize=(8, 6))
            
            # To keep it hourly/clean, we plot unique snapshots
            for ts in df_loc['timestamp'].unique():
                subset = df_loc[df_loc['timestamp'] == ts]
                # Format legend to show Date and Hour
                label_time = pd.to_datetime(ts).strftime('%m/%d %H:00')
                ax1.plot(subset['temperature'], subset['Depth'], marker='o', label=label_time)
            
            ax1.invert_yaxis()
            ax1.axvline(x=32, color='red', linestyle='--', alpha=0.7)
            ax1.set_xlabel("Temp (°F)")
            ax1.set_ylabel("Depth (ft)")
            ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='x-small')
            st.pyplot(fig1)

# --- TAB: TEMPERATURE TRENDS (Hourly) ---
with tab_time:
    st.subheader("Hourly Temperature Trends by Depth")
    for loc in available_locations:
        st.write(f"### Location: {loc}")
        df_loc_time = df_filtered[df_filtered['Location'] == loc]
        
        fig2, ax2 = plt.subplots(figsize=(12, 5))
        
        # Plot a line for every unique depth in this pipe
        for d in sorted(df_loc_time['Depth'].unique()):
            subset = df_loc_time[df_loc_time['Depth'] == d].sort_values('timestamp')
            ax2.plot(subset['timestamp'], subset['temperature'], label=f"Depth: {d}ft", linewidth=1.5)
            
        ax2.axhline(y=32, color='red', linestyle='--', alpha=0.8)
        ax2.set_ylabel("Temperature (°F)")
        ax2.set_xlabel("Time (Hourly Data)")
        ax2.grid(True, which='both', linestyle=':', alpha=0.5)
        ax2.legend(loc='upper left', bbox_to_anchor=(1, 1))
        plt.xticks(rotation=45)
        st.pyplot(fig2)
