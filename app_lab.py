import streamlit as st
import pandas as pd
from datetime import datetime, date

# 1. DATE RANGE SELECTION
st.sidebar.header("Data Export Tools")

# Set default range to 'Last 7 Days'
start_date = st.sidebar.date_input("Start Date", value=date.today() - pd.Timedelta(days=7))
end_date = st.sidebar.date_input("End Date", value=date.today())

# 2. FILTERING THE DATA
# We use the 'timestamp' column from BigQuery to filter the local dataframe
def get_filtered_data(df, start, end):
    # Convert dates to datetime for comparison
    mask = (df['timestamp'].dt.date >= start) & (df['timestamp'].dt.date <= end)
    return df.loc[mask]

filtered_df = get_filtered_data(full_df, start_date, end_date)

# 3. DOWNLOAD BUTTON
st.subheader(f"📊 Project Data: {start_date} to {end_date}")

if not filtered_df.empty:
    # Convert dataframe to CSV in memory
    csv = filtered_df.to_csv(index=False).encode('utf-8')
    
    st.download_button(
        label="📥 Download Filtered Data as CSV",
        data=csv,
        file_name=f"SoilFreeze_Export_{start_date}_{end_date}.csv",
        mime='text/csv',
    )
    
    # Display a preview of what's being downloaded
    st.dataframe(filtered_df.head(100), use_container_width=True)
else:
    st.warning("No data found for the selected date range.")

# 4. CHART (NO TREND LINE)
# Using a clean Plotly line chart without the OLS/Trendline overlay
import plotly.express as px

fig = px.line(
    filtered_df, 
    x="timestamp", 
    y="value", 
    color="nodenumber",
    title="Sensor Readings (Raw Data)"
)
st.plotly_chart(fig, use_container_width=True)
