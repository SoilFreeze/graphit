import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. Setup Page and Auth
st.set_page_config(page_title="Geotechnical Temp Dashboard", layout="wide")

scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform"]

try:
    creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    client = bigquery.Client(credentials=creds, project="sensorpush-export")
except Exception as e:
    st.error(f"Authentication Error: {e}")
    st.stop()

# 2. Sidebar - Project Selection
st.sidebar.title("📁 Project Management")

# Pull all data to identify available projects
@st.cache_data(ttl=600)
def get_full_dataset():
    query = "SELECT * FROM `sensorpush-export.sensor_data.monday_morning_depth_profile` ORDER BY timestamp DESC"
    return client.query(query).to_dataframe()

df_raw = get_full_dataset()

# Get unique list of Projects from your 'Project' column
available_projects = sorted(df_raw['Project'].unique())
selected_project = st.sidebar.selectbox("Select Project", available_projects)

# Filter global data frame to ONLY the selected project
df_proj = df_raw[df_raw['Project'] == selected_project]

# --- 3. Sidebar - Time Filter ---
num_weeks = st.sidebar.slider("Weeks of History", 1, 12, 4)

# Force the timestamp column to datetime objects
df_proj['timestamp'] = pd.to_datetime(df_proj['timestamp'])

# Create a timezone-aware cutoff (UTC)
cutoff_date = pd.Timestamp.now(tz='UTC') - pd.Timedelta(weeks=num_weeks)

# Filter the data
df_filtered = df_proj[df_proj['timestamp'] >= cutoff_date]

# --- THE FIX: Match your actual column names ---
# Using 'Depth' (Capital D) and 'Location' (Capital L) 
# to match your shared header map.
try:
    df_filtered = df_filtered.sort_values(['Location', 'Depth', 'timestamp'])
except KeyError:
    # If BigQuery made them lowercase, this fallback will catch it
    df_filtered = df_filtered.sort_values(['location', 'depth', 'timestamp'])

st.title(f"Project: {selected_project}")

# --- 4. Create Tabs based on your Locations ---
# Update 'Location' here too
available_pipes = sorted(df_filtered['Location'].unique()) 
tabs = st.tabs(available_pipes + ["📈 Overall Trends"])

for i, pipe_name in enumerate(available_pipes):
    with tabs[i]:
        st.header(f"Depth Profile: {pipe_name}")
        df_pipe = df_filtered[df_filtered['Location'] == pipe_name]
        
        if not df_pipe.empty:
            fig, ax = plt.subplots(figsize=(6, 8))
            for ts in df_pipe['timestamp'].unique():
                subset = df_pipe[df_pipe['timestamp'] == ts]
                # Update 'Depth' and 'temperature' (check if T is capital!)
                ax.plot(subset['temperature'], subset['Depth'], marker='o', label=pd.to_datetime(ts).strftime('%m/%d %H:%M'))
            
            ax.invert_yaxis()
            ax.axvline(x=32, color='red', linestyle='--', alpha=0.5)
            ax.set_xlabel('Temperature (°F)')
            ax.set_ylabel('Depth (ft)')
            ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='small')
            st.pyplot(fig)
