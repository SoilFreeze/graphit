import streamlit as st
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
from scipy.stats import linregress
import matplotlib.pyplot as plt

# 1. SETUP
st.set_page_config(page_title="Engineering Data Lab", layout="wide")

# 2. DATA LOADING
# (Use the same credentials logic as your other apps)
creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
client = bigquery.Client(credentials=creds, project="sensorpush-export")

@st.cache_data(ttl=60)
def load_lab_data(project):
    query = f"SELECT * FROM `sensor_data.final_dashboard_data` WHERE project = '{project}'"
    return client.query(query).to_dataframe()

# 3. SIDEBAR: SELECTOR & CONTROLS
st.sidebar.title("🛠 Engineering Lab")
proj_list = ["2329", "SF2329", "North Dam"] # Or pull unique from DB
selected_proj = st.sidebar.selectbox("Select Project", proj_list)

raw_df = load_lab_data(selected_proj)
raw_df['timestamp'] = pd.to_datetime(raw_df['timestamp'])

# Filter by Specific Node
locations = sorted(raw_df['location'].unique())
loc = st.sidebar.selectbox("Select Location/Pipe", locations)
depths = sorted(raw_df[raw_df['location'] == loc]['depth'].unique())
depth = st.sidebar.selectbox("Select Depth/Node", depths)

# Filter the working dataset
working_df = raw_df[(raw_df['location'] == loc) & (raw_df['depth'] == depth)].copy()
working_df = working_df.sort_values('timestamp')

# 4. DATA CLEANING (Delete Errors)
st.header(f"🗂 Data Cleaning: {loc} at {depth}")
st.info("Edit the 'value' column below. To 'delete' a point, remove the number or set to NaN.")

# Use st.data_editor to allow live editing
edited_df = st.data_editor(
    working_df[['timestamp', 'value']], 
    num_rows="dynamic",
    use_container_width=True,
    key="data_editor"
)

# 5. TREND ANALYSIS
st.header("📈 Trend Analysis & Forecasting")
col1, col2 = st.columns([1, 3])

with col1:
    st.subheader("Analysis Parameters")
    days_to_predict = st.slider("Forecast Days", 7, 90, 30)
    show_trend = st.checkbox("Calculate Trend Line", value=True)
    
    # Calculate Linear Regression
    clean_df = edited_df.dropna(subset=['value'])
    if not clean_df.empty and show_trend:
        # Convert time to numeric for regression
        x = mdates.date2num(clean_df['timestamp'])
        y = clean_df['value']
        slope, intercept, r_value, p_value, std_err = linregress(x, y)
        
        st.metric("Daily Change", f"{slope:.4f} units/day")
        st.write(f"Confidence (R²): {r_value**2:.2f}")

with col2:
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.scatter(clean_df['timestamp'], clean_df['value'], color='black', s=10, label="Observed Data")
    
    if show_trend:
        # Create forecast range
        last_date = clean_df['timestamp'].max()
        future_dates = pd.date_range(start=clean_df['timestamp'].min(), 
                                     periods=len(clean_df) + days_to_predict)
        x_future = mdates.date2num(future_dates)
        y_trend = slope * x_future + intercept
        
        ax.plot(future_dates, y_trend, color='red', linestyle='--', label="Calculated Trend")
    
    plt.xticks(rotation=45)
    ax.legend()
    st.pyplot(fig)

# 6. SAVE CHANGES
if st.button("💾 Push Cleaned Data to BigQuery"):
    st.warning("Note: This requires 'BigQuery Data Editor' permissions for the service account.")
    # Logic to overwrite or update BigQuery rows would go here
