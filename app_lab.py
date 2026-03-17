import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date

# --- 1. AUTHENTICATION (Secrets vs Local) ---
if "gcp_service_account" in st.secrets:
    info = st.secrets["gcp_service_account"]
    credentials = service_account.Credentials.from_service_account_info(info)
    client = bigquery.Client(credentials=credentials, project=info["project_id"])
else:
    client = bigquery.Client.from_service_account_json("service_account.json")

# --- 2. DATA PULL ---
@st.cache_data(ttl=600)
def fetch_data():
    # We build the master data manually to bypass the 'Forbidden' View error
    query = """
    WITH unified AS (
        SELECT timestamp, value, nodenumber 
        FROM `sensorpush-export.sensor_data.raw_lord`
        UNION ALL
        SELECT timestamp, temperature AS value, sensor_name AS nodenumber
        FROM `sensorpush-export.sensor_data.raw_sensorpush`
    )
    SELECT 
        u.timestamp, 
        u.value, 
        u.nodenumber, 
        m.job_site AS project, 
        m.location, 
        m.depth 
    FROM unified AS u
    LEFT JOIN `sensorpush-export.sensor_data.SensorMapping` AS m
      ON u.nodenumber = m.nodenumber
    """
    df = client.query(query).to_dataframe()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Fill in blanks so the dropdown menus work correctly
    df['project'] = df['project'].fillna('Unmapped').astype(str)
    df['location'] = df['location'].fillna('Unmapped').astype(str)
    
    return df

full_df = fetch_data()

# --- 3. SIDEBAR NAVIGATION ---
st.sidebar.title("🛠 Engineering Services")
service = st.sidebar.selectbox(
    "Select Service",
    ["🔍 Node Diagnostics", "📥 Data Export Lab", "🧹 Data Cleaning Tool"]
)

# --- SERVICE: NODE DIAGNOSTICS (Restored Features) ---
if service == "🔍 Node Diagnostics":
    st.header("🔍 Individual Node Diagnostics")

    # Filter Row: Project -> Location -> Node
    col1, col2, col3 = st.columns(3)
    
    with col1:
        projects = sorted(full_df['job_site'].dropna().unique())
        selected_project = st.selectbox("Select Project ID", projects)
        proj_df = full_df[full_df['job_site'] == selected_project]

    with col2:
        locations = sorted(proj_df['location'].dropna().unique())
        selected_loc = st.selectbox("Select Location", locations)
        loc_df = proj_df[proj_df['location'] == selected_loc]

    with col3:
        nodes = sorted(loc_df['nodenumber'].unique())
        selected_node = st.selectbox("Select Node", nodes)
        final_df = loc_df[loc_df['nodenumber'] == selected_node]

    # The Graph (Clean Raw Data)
    if not final_df.empty:
        fig = px.line(final_df, x='timestamp', y='value', 
                      title=f"Node {selected_node} - {selected_loc}",
                      labels={'value': 'Temperature (°C)', 'timestamp': 'Date/Time'})
        st.plotly_chart(fig, use_container_width=True)
        
        # Diagnostic Stats
        st.write(f"**Total Samples:** {len(final_df)}")
        st.write(f"**Max Temp:** {final_df['value'].max()}°C")
        st.write(f"**Min Temp:** {final_df['value'].min()}°C")
    else:
        st.warning("No data found for the selected combination.")

# --- SERVICE: DATA EXPORT LAB (Existing Feature) ---
elif service == "📥 Data Export Lab":
    st.header("📥 Data Export Lab")
    # (Insert your date range and CSV download logic here)

# --- SERVICE: DATA CLEANING TOOL (New Feature) ---
elif service == "🧹 Data Cleaning Tool":
    st.header("🧹 Data Cleaning Tool")
    # (Insert cleaning logic here)
