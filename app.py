import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from google.cloud import bigquery
from google.oauth2 import service_account
from scipy.stats import linregress

# 1. PAGE SETUP
st.set_page_config(page_title="Engineer Data Lab", layout="wide")

# 2. ESTABLISH BIGQUERY CONNECTION
# This must be defined before the load function is called
try:
    scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform"]
    creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    client = bigquery.Client(credentials=creds, project="sensorpush-export")
except Exception as e:
    st.error(f"Failed to connect to Google Cloud: {e}")
    st.stop()

# 3. DATA LOADING FUNCTION
@st.cache_data(ttl=60)
def load_lab_data():
    # Pulling the full dataset for exploration
    query = "SELECT * FROM `sensorpush-export.sensor_data.final_dashboard_data` ORDER BY timestamp ASC"
    return client.query(query).to_dataframe()

# 4. DATA PROCESSING & CLEANING
df_raw = load_lab_data()

# Clean headers and timestamps
df_raw.columns = [str(c).strip().lower() for c in df_raw.columns]
df_raw['timestamp'] = pd.to_datetime(df_raw['timestamp'])

# --- DATA REPAIR LOGIC ---
# Standardize Node IDs: Replace hyphens with underscores so they group together
df_raw['depth'] = df_raw['depth'].astype(str).str.replace('-', '_', regex=False).str.strip()

# REMOVE NULL ROWS: Drop rows where temperature or depth is missing
df_clean = df_raw.dropna(subset=['value', 'depth', 'location']).copy()

# TYPE FIXER: Force all Depths/Nodes to be strings (fixes the "missing node" issue)
df_clean['depth'] = df_clean['depth'].astype(str).str.strip()

# 5. SIDEBAR: SELECTION CONTROLS
st.sidebar.title("🛠 Engineering Lab")

# Project Selection
projects = sorted(df_clean['project'].unique())
selected_proj = st.sidebar.selectbox("Select Project", projects)
df_p = df_clean[df_clean['project'] == selected_proj].copy()

# Location & Node Selection
locs = sorted(df_p['location'].unique())
selected_loc = st.sidebar.selectbox("Location (Pipe)", locs)

nodes = sorted(df_p[df_p['location'] == selected_loc]['depth'].unique())
selected_node = st.sidebar.selectbox("Node / Depth ID", nodes)

# Sidebar Divider & Debug info
st.sidebar.divider()
st.sidebar.write(f"Unique Nodes found: {len(nodes)}")

# Filter to specific sensor
working_df = df_p[(df_p['location'] == selected_loc) & (df_p['depth'] == selected_node)].sort_values('timestamp')

# 6. MAIN INTERFACE
st.header(f"Analysis: {selected_proj} | {selected_loc} | Node {selected_node}")

if not working_df.empty:
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("Data Cleaning")
        # Slider to filter out spikes or sensor errors
        min_v = float(working_df['value'].min())
        max_v = float(working_df['value'].max())
        valid_range = st.slider("Valid Temp Range", -50.0, 150.0, (min_v, max_v))
        
        # Apply filter
        filtered_df = working_df[
            (working_df['value'] >= valid_range[0]) & 
            (working_df['value'] <= valid_range[1])
        ].copy()
        
        st.write(f"Points Analyzed: {len(filtered_df)} of {len(working_df)}")
        
        # Goal Forecasting
        st.subheader("Forecast Parameters")
        target_temp = st.number_input("Target Goal Temp", value=32.0)
        forecast_days = st.slider("Days to Forecast", 7, 180, 30)

    with col2:
        if len(filtered_df) > 2:
            # Linear Regression Math
            x_num = mdates.date2num(filtered_df['timestamp'])
            y_vals = filtered_df['value']
            slope, intercept, r_val, p_val, std_err = linregress(x_num, y_vals)
            
            # Create Plot
            fig, ax = plt.subplots(figsize=(10, 5))
            # Plot original data as light grey to show what was removed
            ax.scatter(working_df['timestamp'], working_df['value'], color='lightgrey', alpha=0.3, label="Excluded Points")
            # Plot cleaned data in black
            ax.scatter(filtered_df['timestamp'], filtered_df['value'], color='black', s=12, label="Clean Data")
            
            # Draw Trend & Forecast
            x_future = np.array([x_num.min(), x_num.max() + forecast_days])
            y_future = slope * x_future + intercept
            ax.plot(mdates.num2date(x_future), y_future, color='red', linestyle='--', linewidth=2, label="Trend Line")
            
            # Calculate Intersection with Goal
            if slope != 0:
                intersect_num = (target_temp - intercept) / slope
                intersect_date = mdates.num2date(intersect_num)
                ax.axhline(y=target_temp, color='blue', linestyle=':', label=f"Goal ({target_temp})")
            
            ax.legend()
            plt.xticks(rotation=45)
            st.pyplot(fig)
            
            # Results
            st.success(f"**Current Slope:** {slope:.4f} units/day | **R² Fit:** {r_val**2:.3f}")
            if slope != 0:
                st.info(f"Projected to hit {target_temp} on: **{intersect_date.strftime('%Y-%m-%d')}**")
        else:
            st.warning("Insufficient clean data to calculate trend. Adjust the Valid Temp Range.")

    # 7. THE DATA EDITOR
    st.divider()
    st.subheader("Manual Data Review")
    st.write("Edit values in the table below to see how they impact the trend line above.")
    st.data_editor(filtered_df[['timestamp', 'value']], use_container_width=True, hide_index=True)

else:
    st.error("No data found for the selected criteria. Check your Node ID or Project name.")
