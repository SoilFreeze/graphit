import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. Setup Page and Auth
st.set_page_config(page_title="Soil Temperature Dashboard", layout="wide")
st.title("🌡️ Ground Temperature Monitoring")

scopes = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform"
]

try:
    creds = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], 
        scopes=scopes
    )
    client = bigquery.Client(credentials=creds, project="sensorpush-export")
except Exception as e:
    st.error(f"Authentication Error: {e}")
    st.stop()

# 2. Sidebar Filters
st.sidebar.header("Global Settings")
num_weeks = st.sidebar.slider("Number of weeks to show", 1, 12, 4)
num_days = num_weeks * 7

# 3. Pull Data (All locations at once to save on query costs)
@st.cache_data(ttl=600) # Refresh every 10 minutes
def get_data(days):
    query = f"""
    SELECT timestamp, depth, temperature, Location 
    FROM `sensorpush-export.sensor_data.monday_morning_depth_profile`
    WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
    ORDER BY timestamp DESC, depth ASC
    """
    return client.query(query).to_dataframe()

df_all = get_data(num_days)

# 4. Create Navigation Tabs
tab1, tab2, tab3 = st.tabs(["OfficeTP1 Profile", "Second Pipe", "Trends Over Time"])

# --- TAB 1: OfficeTP1 ---
with tab1:
    st.header("Vertical Depth Profile - OfficeTP1")
    df_tp1 = df_all[df_all['Location'] == 'OfficeTP1']
    
    if not df_tp1.empty:
        fig1, ax1 = plt.subplots(figsize=(7, 9))
        for date in df_tp1['timestamp'].dt.date.unique():
            subset = df_tp1[df_tp1['timestamp'].dt.date == date]
            ax1.plot(subset['temperature'], subset['depth'], marker='o', label=str(date))
        
        ax1.invert_yaxis()
        ax1.axvline(x=32, color='red', linestyle='--', label='Freezing (32°F)')
        ax1.set_xlabel('Temperature (°F)')
        ax1.set_ylabel('Depth (ft)')
        ax1.legend(title="Date", bbox_to_anchor=(1.05, 1), loc='upper left')
        ax1.grid(True, alpha=0.3)
        st.pyplot(fig1)
    else:
        st.info("No data found for OfficeTP1.")

# --- TAB 2: SECOND PIPE ---
with tab2:
    # Find other locations in your data automatically
    other_locations = [loc for loc in df_all['Location'].unique() if loc != 'OfficeTP1']
    
    if other_locations:
        selected_loc = st.selectbox("Select a second pipe to view:", other_locations)
        df_tp2 = df_all[df_all['Location'] == selected_loc]
        
        fig2, ax2 = plt.subplots(figsize=(7, 9))
        for date in df_tp2['timestamp'].dt.date.unique():
            subset = df_tp2[df_tp2['timestamp'].dt.date == date]
            ax2.plot(subset['temperature'], subset['depth'], marker='o', label=str(date))
        ax2.invert_yaxis()
        ax2.axvline(x=32, color='red', linestyle='--', label='Freezing (32°F)')
        st.pyplot(fig2)
    else:
        st.info("Only one location (OfficeTP1) found in the current dataset.")

# --- TAB 3: TRENDS OVER TIME ---
with tab3:
    st.header("Temperature Trends")
    # Let user choose which pipe to see trends for
    trend_loc = st.selectbox("Show trends for:", df_all['Location'].unique(), key="trend_sel")
    df_trend = df_all[df_all['Location'] == trend_loc].sort_values('timestamp')

    if not df_trend.empty:
        fig3, ax3 = plt.subplots(figsize=(10, 5))
        for d in sorted(df_trend['depth'].unique()):
            subset = df_trend[df_trend['depth'] == d]
            ax3.plot(subset['timestamp'], subset['temperature'], label=f"{d} ft")
        
        ax3.axhline(y=32, color='red', linestyle='--', label='Freezing')
        ax3.set_ylabel("Temp (°F)")
        ax3.legend(loc='upper left', bbox_to_anchor=(1, 1))
        plt.xticks(rotation=45)
        st.pyplot(fig3)
