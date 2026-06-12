import streamlit as st
import pandas as pd
import re
from google.cloud import bigquery
import plotly.graph_objects as go
import plotly.express as px

# 1. SETUP
st.set_page_config(layout="wide", page_title="SoilFreeze Engineering Dashboard")
client = bigquery.Client()
PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature"

# 2. HELPER FUNCTIONS
def get_universal_portal_data(selected_project):
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` WHERE Project = '{selected_project}'"
    return client.query(query).to_dataframe()

def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', str(s))]

# 3. APP UI LAYOUT
st.title("🏗️ SoilFreeze Engineering Portal")
tabs = st.tabs(["📊 Overview", "📄 Data Ingestion", "🛠️ Database Maintenance"])

# TAB 1: OVERVIEW
with tabs[0]:
    selected_project = st.selectbox("Select Project", ["Project-2541-Blackjack", "Project-2538-Ferndale"])
    if selected_project:
        df = get_universal_portal_data(selected_project)
        st.dataframe(df.head())

# TAB 2: UPLOAD LOGIC
with tabs[1]:
    st.subheader("Manual File Ingestion")
    u_files = st.file_uploader("Upload CSV/Excel", accept_multiple_files=True)
    
    if u_files:
        all_dfs = []
        for f in u_files:
            try:
                df = pd.read_csv(f) if f.name.endswith('.csv') else pd.read_excel(f)
                all_dfs.append(df)
                st.write(f"✅ Prepared {f.name}")
            except Exception as e:
                st.error(f"Error {f.name}: {e}")
        
        if all_dfs and st.button("🚀 Commit Batch to BigQuery"):
            combined = pd.concat(all_dfs)
            # Add your load_table_from_dataframe logic here
            st.success("Batch Upload Successful")

# TAB 3: MAINTENANCE (API SYNC)
with tabs[2]:
    st.subheader("Fleet Synchronization")
    if st.button("🔄 Sync API to Registry"):
        with st.spinner("Pinging API..."):
            # Insert your Fleet API call and SQL Update logic here
            st.success("Fleet Synchronized")
            
    # Unmapped node summary
    if st.button("🔍 Scan for Unmapped Nodes"):
        q = f"""
            SELECT NodeNum, COUNT(*) as total_points, MIN(timestamp) as first, MAX(timestamp) as last
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view`
            WHERE LOWER(NodeNum) LIKE '%unmapped%'
            GROUP BY NodeNum
        """
        st.dataframe(client.query(q).to_dataframe())
