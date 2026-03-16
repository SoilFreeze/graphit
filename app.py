import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
from google.oauth2 import service_account

# Set page title
st.set_page_config(page_title="Soil Temperature Profile", layout="wide")
st.title("🧪 Ground Temperature Depth Profile")

# 1. Setup Authentication (For the cloud, we use a 'Secrets' file)
# In your local test, you can use your json key file path
# client = bigquery.Client.from_service_account_json('path_to_your_key.json')

# For the live app, we use Streamlit's secret manager:
creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
client = bigquery.Client(credentials=creds, project=creds.project_id)

# 2. Sidebar Filters
st.sidebar.header("Filter Data")
num_weeks = st.sidebar.slider("Number of weeks to show", 1, 12, 4)

# 3. Pull Data
query = f"""
SELECT timestamp, depth, temperature 
FROM `your-project.your_dataset.monday_morning_depth_profile`
WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {num_weeks} WEEK)
ORDER BY timestamp DESC, depth ASC
"""
df = client.query(query).to_dataframe()

# 4. Create the Chart
if not df.empty:
    fig, ax = plt.subplots(figsize=(8, 10))
    
    # Plot each date as a unique line
    for date in df['timestamp'].dt.date.unique():
        subset = df[df['timestamp'].dt.date == date]
        ax.plot(subset['temperature'], subset['depth'], marker='o', label=str(date))

    # Formatting
    ax.invert_yaxis()
    ax.axvline(x=32, color='red', linestyle='--', label='Freezing (32°F)')
    ax.set_xlabel('Temperature (°F)')
    ax.set_ylabel('Depth (ft)')
    ax.grid(True, linestyle=':', alpha=0.6)
    ax.legend(title="Snapshots", bbox_to_anchor=(1.05, 1), loc='upper left')

    st.pyplot(fig)
else:
    st.warning("No data found for the selected range.")
