import streamlit as st
import pandas as pd
from google.cloud import bigquery

# --- SETTINGS ---
TARGET_PROJECT = "2538" 
PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature"

client = bigquery.Client(project=PROJECT_ID)

@st.cache_data(ttl=60) # Short TTL to clear the error quickly
def get_final_data():
    # Now that we've fixed the view, we can safely use master_data again!
    query = f"""
        SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.master_data`
        WHERE Project LIKE '{TARGET_PROJECT}%'
        AND approve = 'TRUE'
        AND NOT EXISTS (
            SELECT 1 FROM `{PROJECT_ID}.{DATASET_ID}.manual_rejections` m2
            WHERE m2.NodeNum = `{PROJECT_ID}.{DATASET_ID}.master_data`.NodeNum
            AND m2.timestamp = TIMESTAMP_TRUNC(`{PROJECT_ID}.{DATASET_ID}.master_data`.timestamp, HOUR)
            AND m2.approve = 'MASKED'
        )
    """
    return client.query(query).to_dataframe()

st.title(f"📊 Pump 16 Upgrade: {TARGET_PROJECT}")

# CRITICAL: Clear cache button to force-remove the old 400 error message
if st.button("🔄 Refresh & Clear Sync Error"):
    st.cache_data.clear()
    st.rerun()

df = get_final_data()

if df.empty:
    st.warning("Database fixed, but no rows are marked 'TRUE' for this project yet.")
else:
    st.success(f"Sync Successful. Found {len(df)} approved records.")
    st.dataframe(df.head())
