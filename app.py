import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
from google.oauth2 import service_account

# Set page title
st.set_page_config(page_title="Soil Temperature Profile", layout="wide")
st.title("🧪 Ground Temperature Depth Profile")

# 1. Setup Authentication
# Update your credentials setup to include Drive and BigQuery scopes
scopes = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform"
]

try:
    creds = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], 
        scopes=scopes  # THIS IS THE KEY ADDITION
    )
    client = bigquery.Client(credentials=creds, project="sensorpush-export")
except Exception as e:
    st.error(f"Authentication Error: {e}")
    st.stop()

# 2. Sidebar Filters
st.sidebar.header("Filter Data")
num_weeks = st.sidebar.slider("Number of weeks to show", 1, 12, 4)

# 3. Pull Data
df = pd.DataFrame() 

try:
    # We convert weeks to days to keep BigQuery happy
    num_days = num_weeks * 7
    
    query = f"""
    SELECT timestamp, depth, temperature 
    FROM `sensorpush-export.sensor_data.monday_morning_depth_profile`
    WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {num_days} DAY)
    ORDER BY timestamp DESC, depth ASC
    """
    df = client.query(query).to_dataframe()
except Exception as e:
    st.error(f"Query Error: {e}")
    st.stop()

# 4. Create the Chart
if not df.empty:
    fig, ax = plt.subplots(figsize=(8, 10))
    
    for date in df['timestamp'].dt.date.unique():
        subset = df[df['timestamp'].dt.date == date]
        ax.plot(subset['temperature'], subset['depth'], marker='o', label=str(date))

    # Reverse axis so 0 is surface, 4 is deep
    ax.invert_yaxis()
    ax.axvline(x=32, color='red', linestyle='--', label='Freezing (32°F)')
    ax.set_xlabel('Temperature (°F)')
    ax.set_ylabel('Depth (ft)')
    ax.grid(True, linestyle=':', alpha=0.6)
    ax.legend(title="Snapshots", bbox_to_anchor=(1.05, 1), loc='upper left')

    st.pyplot(fig)
else:
    st.info("The query ran successfully, but no data was returned. Check your date filters.")
