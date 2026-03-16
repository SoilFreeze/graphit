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
num_weeks = st.sidebar.slider("Weeks of History", 1, 24, 8) # Increased range for weekly views
cutoff_date = pd.Timestamp.now(tz='UTC') - pd.Timedelta(weeks=num_weeks)
df_filtered = df_proj[df_proj['timestamp'] >= cutoff_date]

st.title(f"Project: {selected_project}")
tab_depth, tab_time = st.tabs(["📊 Weekly Depth Profiles", "📈 Hourly Trends"])

available_locations = sorted(df_filtered['Location'].unique())

# --- TAB: DEPTH PROFILES (Mondays at 6:00 AM) ---
with tab_depth:
    st.subheader("Vertical Temperature: Mondays at 6:00 AM")
    for loc in available_locations:
        with st.expander(f"Location: {loc}", expanded=True):
            df_loc = df_filtered[df_filtered['Location'] == loc].copy()
            
            # Filter for Monday (weekday 0) at Hour 6
            # We use a 1-hour window (6:00 to 6:59) to be safe
            df_monday = df_loc[(df_loc['timestamp'].dt.weekday == 0) & (df_loc['timestamp'].dt.hour == 6)]
            
            if not df_monday.empty:
                fig1, ax1 = plt.subplots(figsize=(8, 6))
                # Sort unique timestamps so the legend is in order
                unique_snaps = sorted(df_monday['timestamp'].unique())
                
                for ts in unique_snaps:
                    subset = df_monday[df_monday['timestamp'] == ts]
                    label_date = pd.to_datetime(ts).strftime('%Y-%m-%d')
                    ax1.plot(subset['temperature'], subset['Depth'], marker='o', label=label_date)
                
                ax1.invert_yaxis()
                ax1.axvline(x=32, color='red', linestyle='--', alpha=0.7, label='Freezing')
                ax1.set_xlabel("Temp (°F)")
                ax1.set_ylabel("Depth (ft)")
                ax1.legend(title="Monday Snapshots", bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='x-small')
                ax1.grid(True, alpha=0.2)
                st.pyplot(fig1)
            else:
                st.info("No data found for Mondays at 6:00 AM. Check if your sensors were active then.")

# --- TAB: TEMPERATURE TRENDS (Hourly) ---
with tab_time:
    st.subheader("Continuous Temperature Trends")
    for loc in available_locations:
        with st.expander(f"Trends: {loc}", expanded=False): # Set to False so it's minimized by default
            df_loc_time = df_filtered[df_filtered['Location'] == loc].sort_values('timestamp')
            
            if not df_loc_time.empty:
                fig2, ax2 = plt.subplots(figsize=(12, 5))
                for d in sorted(df_loc_time['Depth'].unique()):
                    subset = df_loc_time[df_loc_time['Depth'] == d]
                    # 'marker' shows the actual data points so you can see the gaps
                    ax2.plot(subset['timestamp'], subset['temperature'], label=f"{d}ft", linewidth=1, marker='.', markersize=4)
                    
                ax2.axhline(y=32, color='red', linestyle='--', alpha=0.8)
                ax2.set_ylabel("Temp (°F)")
                ax2.grid(True, which='both', linestyle=':', alpha=0.4)
                ax2.legend(loc='upper left', bbox_to_anchor=(1, 1), fontsize='x-small')
                plt.xticks(rotation=45)
                st.pyplot(fig2)
