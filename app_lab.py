import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import date
from google.oauth2 import service_account

# --- SHARED AUTHENTICATION LOGIC ---
# (Keep the BigQuery client setup we just fixed here so it works for all pages)

st.sidebar.title("🛠 Engineering Services")
service = st.sidebar.selectbox(
    "Select Service",
    ["📥 Data Export Lab", "🔍 Node Diagnostics", "🧹 Data Cleaning Tool"]
)

# --- SERVICE 1: DATA EXPORT LAB ---
if service == "📥 Data Export Lab":
    st.header("Data Export Lab")
    # Paste the code for the date range filter and CSV download button here.

# --- SERVICE 2: NODE DIAGNOSTICS ---
elif service == "🔍 Node Diagnostics":
    st.header("Node Diagnostics")
    # Paste your code here that looks at individual node health, 
    # battery levels, and last-seen timestamps.

# --- SERVICE 3: DATA CLEANING TOOL ---
elif service == "🧹 Data Cleaning Tool":
    st.header("Data Cleaning Tool")
    st.write("Current Filter: Removing 'NaN' and Outliers (>100°C or <-50°C)")
    # We can add a slider here to let engineers define what 'erroneous' means.

# This replaces your single 'from_service_account_json' line
if "gcp_service_account" in st.secrets:
    # CLOUD: Use the secrets you pasted into the Streamlit dashboard
    info = st.secrets["gcp_service_account"]
    credentials = service_account.Credentials.from_service_account_info(info)
    client = bigquery.Client(credentials=credentials, project=info["project_id"])
else:
    # LOCAL: Use the file on your computer
    client = bigquery.Client.from_service_account_json("service_account.json")
    
# 2. PULL THE DATA FIRST
query = "SELECT timestamp, value, nodenumber FROM `sensorpush-export.sensor_data.raw_lord`"
full_df = client.query(query).to_dataframe()

# Ensure timestamp is actually a datetime object
full_df['timestamp'] = pd.to_datetime(full_df['timestamp'])

# 3. SIDEBAR CONTROLS
st.sidebar.header("Data Export Tools")
start_date = st.sidebar.date_input("Start Date", value=date.today() - pd.Timedelta(days=7))
end_date = st.sidebar.date_input("End Date", value=date.today())

# 4. FILTERING FUNCTION
def get_filtered_data(df, start, end):
    mask = (df['timestamp'].dt.date >= start) & (df['timestamp'].dt.date <= end)
    return df.loc[mask]

# Now 'full_df' exists, so this won't throw a NameError
filtered_df = get_filtered_data(full_df, start_date, end_date)

# 5. DISPLAY & DOWNLOAD
st.subheader("📋 Project Data Preview")
st.dataframe(filtered_df, use_container_width=True)

csv = filtered_df.to_csv(index=False).encode('utf-8')
st.download_button("📥 Download CSV", data=csv, file_name="SoilFreeze_Data.csv", mime="text/csv")
