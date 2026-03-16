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

# 3. Sidebar - Time Filter
num_weeks = st.sidebar.slider("Weeks of History", 1, 12, 4)

# Ensure the timestamp column is actually datetime objects
df_proj['timestamp'] = pd.to_datetime(df_proj['timestamp'])

# Make the cutoff date timezone-aware (UTC) to match BigQuery
cutoff_date = pd.Timestamp.now(tz='UTC') - pd.Timedelta(weeks=num_weeks)

# Filter the data
df_filtered = df_proj[df_proj['timestamp'] >= cutoff_date]

# Sort data so the lines connect correctly on the graph
df_filtered = df_filtered.sort_values(['Location', 'depth', 'timestamp'])

# 4. Create Tabs based on your Locations (Pipes) within that project
available_pipes = sorted(df_filtered['Location'].unique())
tabs = st.tabs(available_pipes + ["📈 Overall Trends"])

# Loop through each pipe and create a Profile Plot
for i, pipe_name in enumerate(available_pipes):
    with tabs[i]:
        st.header(f"Depth Profile: {pipe_name}")
        df_pipe = df_filtered[df_filtered['Location'] == pipe_name]
        
        if not df_pipe.empty:
            fig, ax = plt.subplots(figsize=(6, 8))
            # Plot each unique timestamp as a separate line
            for ts in df_pipe['timestamp'].unique():
                subset = df_pipe[df_pipe['timestamp'] == ts]
                ax.plot(subset['temperature'], subset['depth'], marker='o', label=pd.to_datetime(ts).strftime('%m/%d %H:%M'))
            
            ax.invert_yaxis()
            ax.axvline(x=32, color='red', linestyle='--', alpha=0.5, label='Freezing')
            ax.set_xlabel('Temperature (°F)')
            ax.set_ylabel('Depth (ft)')
            ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='small')
            ax.grid(True, alpha=0.2)
            st.pyplot(fig)
        else:
            st.info("No data for this timeframe.")

# --- Final Tab: Trends ---
with tabs[-1]:
    st.header(f"{selected_project} - Historical Trends")
    if not df_filtered.empty:
        fig_trend, ax_trend = plt.subplots(figsize=(10, 5))
        # Group by Location and Depth to show how specific points change over time
        for pipe in available_pipes:
            df_trend_pipe = df_filtered[df_filtered['Location'] == pipe]
            for d in sorted(df_trend_pipe['depth'].unique()):
                subset = df_trend_pipe[df_trend_pipe['depth'] == d].sort_values('timestamp')
                ax_trend.plot(subset['timestamp'], subset['temperature'], label=f"{pipe} @ {d}ft")
        
        ax_trend.axhline(y=32, color='red', linestyle='--')
        ax_trend.set_ylabel("Temp (°F)")
        ax_trend.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='x-small')
        plt.xticks(rotation=45)
        st.pyplot(fig_trend)
