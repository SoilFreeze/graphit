import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from google.cloud import bigquery
from google.oauth2 import service_account
from scipy.stats import linregress

# 1. SETUP
st.set_page_config(page_title="Engineer Data Lab", layout="wide")

# 2. DATA LOADING
scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform"]
creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
client = bigquery.Client(credentials=creds, project="sensorpush-export")

@st.cache_data(ttl=60)
def load_lab_data():
    # Adjusted to ensure we get the full dataset for exploration
    query = "SELECT * FROM `sensorpush-export.sensor_data.final_dashboard_data` ORDER BY timestamp ASC"
    return client.query(query).to_dataframe()

df = load_lab_data()
df.columns = [str(c).strip().lower() for c in df_raw.columns]
df['timestamp'] = pd.to_datetime(df['timestamp'])

# 3. SIDEBAR: SELECTION
st.sidebar.title("🛠 Engineering Lab")
project = st.sidebar.selectbox("Select Project", sorted(df['project'].unique()))
df_p = df[df['project'] == project].copy()

loc = st.sidebar.selectbox("Location/Pipe", sorted(df_p['location'].unique()))
depth = st.sidebar.selectbox("Node/Depth", sorted(df_p[df_p['location'] == loc]['depth'].unique()))

node_data = df_p[(df_p['location'] == loc) & (df_p['depth'] == depth)].sort_values('timestamp').copy()

# 4. TREND ANALYSIS & GOAL SETTING
st.header(f"Project {project}: Analysis for {loc} ({depth})")

col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("Analysis Parameters")
    # Slider to clean out sensor spikes instantly
    min_t, max_t = st.slider("Valid Temp Range", -40.0, 140.0, (node_data['value'].min(), node_data['value'].max()))
    clean_data = node_data[(node_data['value'] >= min_t) & (node_data['value'] <= max_t)].copy()
    
    st.write(f"Points Analyzed: {len(clean_data)}")
    
    # Target Temperature Prediction
    target_temp = st.number_input("Target Temp Goal (e.g. 32.0)", value=32.0)
    forecast_days = st.slider("Forecast Visibility (Days)", 7, 180, 30)

with col2:
    if len(clean_data) > 2:
        # Math: Linear Regression
        x_num = mdates.date2num(clean_data['timestamp'])
        y_vals = clean_data['value']
        slope, intercept, r_val, p_val, std_err = linregress(x_num, y_vals)
        
        # Plotting
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.scatter(node_data['timestamp'], node_data['value'], color='lightgrey', alpha=0.3, label="Excluded/Errors")
        ax.scatter(clean_data['timestamp'], clean_data['value'], color='black', s=12, label="Clean Data")
        
        # Calculate Trend & Prediction
        x_future = np.array([x_num.min(), x_num.max() + forecast_days])
        y_future = slope * x_future + intercept
        ax.plot(mdates.num2date(x_future), y_future, color='red', linestyle='--', linewidth=2, label="Trend Line")
        
        # Calculate Intersection with Goal
        if slope != 0:
            intersect_num = (target_temp - intercept) / slope
            intersect_date = mdates.num2date(intersect_num)
            ax.axhline(y=target_temp, color='blue', linestyle=':', label=f"Goal ({target_temp})")
            
        ax.set_ylabel("Temperature")
        ax.legend()
        st.pyplot(fig)
        
        # Metrics
        st.info(f"**Slope:** {slope:.4f}/day | **R²:** {r_val**2:.3f}")
        if slope != 0:
            st.success(f"Projected to hit {target_temp} on: **{intersect_date.strftime('%Y-%m-%d')}**")
    else:
        st.warning("Please adjust the 'Valid Temp Range' to include at least 3 data points.")

# 5. THE DATA EDITOR (Manual Deletion)
st.subheader("Manual Data Review")
st.write("Edit values below. Changes will reflect in the graph above upon refresh.")
st.data_editor(clean_data[['timestamp', 'value']], use_container_width=True, hide_index=True)
