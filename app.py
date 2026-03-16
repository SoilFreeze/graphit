import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
from google.oauth2 import service_account

# ... (Keep your existing Auth/Scopes block here) ...

st.title("🌡️ Comprehensive Soil Climate Dashboard")

# 1. Sidebar Filters
num_weeks = st.sidebar.slider("Number of weeks to show", 1, 12, 4)

# 2. Create Tabs
tab1, tab2, tab3 = st.tabs(["Pipe A (East)", "Pipe B (West)", "Temperature Trends"])

# --- TAB 1: PIPE A PROFILE ---
with tab1:
    st.header("Vertical Depth Profile - Pipe A")
    # Add a filter to your SQL: WHERE pipe_id = 'Pipe A'
    # Use your existing plotting code here
    # st.pyplot(fig_pipe_a)

# --- TAB 2: PIPE B PROFILE ---
with tab2:
    st.header("Vertical Depth Profile - Pipe B")
    # st.pyplot(fig_pipe_b)

# --- TAB 3: TEMPERATURE VS TIME ---
with tab3:
    st.header("Historical Temperature Trends")
    
    # New Query for Time Series
    query_time = f"""
    SELECT timestamp, temperature, depth 
    FROM `sensorpush-export.sensor_data.monday_morning_depth_profile`
    WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {num_weeks*7} DAY)
    ORDER BY timestamp ASC
    """
    df_time = client.query(query_time).to_dataframe()

    if not df_time.empty:
        fig2, ax2 = plt.subplots(figsize=(10, 5))
        
        # Plot a line for each depth
        for d in df_time['depth'].unique():
            subset = df_time[df_time['depth'] == d]
            ax2.plot(subset['timestamp'], subset['temperature'], label=f"{d} ft")
        
        ax2.axhline(y=32, color='red', linestyle='--', label='Freezing')
        ax2.set_ylabel("Temperature (°F)")
        ax2.set_xlabel("Date")
        ax2.legend(title="Sensor Depth")
        st.pyplot(fig2)
