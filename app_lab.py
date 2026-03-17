import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date

# --- 1. AUTHENTICATION ---
if "gcp_service_account" in st.secrets:
    info = st.secrets["gcp_service_account"]
    credentials = service_account.Credentials.from_service_account_info(info)
    client = bigquery.Client(credentials=credentials, project=info["project_id"])
else:
    client = bigquery.Client.from_service_account_json("service_account.json")

# --- 2. DATA PULL ---
@st.cache_data(ttl=600)
def fetch_engineering_data():
    query = """
    WITH raw_combined AS (
        -- Convert Lord DATETIME to TIMESTAMP
        SELECT CAST(timestamp AS TIMESTAMP) as timestamp, value, nodenumber 
        FROM `sensorpush-export.sensor_data.raw_lord`
        
        UNION ALL
        
        -- SensorPush is already TIMESTAMP
        SELECT timestamp, temperature AS value, sensor_name AS nodenumber 
        FROM `sensorpush-export.sensor_data.raw_sensorpush`
    )
    SELECT 
        r.timestamp, 
        r.value, 
        r.nodenumber, 
        m.Project AS project, 
        m.TempPipe AS location, 
        m.Depth AS depth
    FROM raw_combined AS r
    LEFT JOIN `sensorpush-export.sensor_data.master_metadata` AS m
      ON r.nodenumber = m.NodeNum
    """
    df = client.query(query).to_dataframe()
    # BigQuery timestamps are usually UTC; this ensures Pandas handles them correctly
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df

try:
    full_df = fetch_engineering_data()
except Exception as e:
    st.error(f"Error fetching data: {e}")
    full_df = pd.DataFrame()

# --- 3. SIDEBAR NAVIGATION ---
st.sidebar.title("🛠 Engineering Hub")
service = st.sidebar.selectbox(
    "Select Service",
    ["🔍 Node Diagnostics", "📥 Data Export Lab", "🧹 Data Cleaning Tool"]
)

# --- SERVICE: NODE DIAGNOSTICS ---
if service == "🔍 Node Diagnostics" and not full_df.empty:
    st.header("🔍 Node Diagnostics")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        projs = sorted(full_df['project'].dropna().unique())
        sel_proj = st.selectbox("Project", projs)
    with col2:
        locs = sorted(full_df[full_df['project']==sel_proj]['location'].dropna().unique())
        sel_loc = st.selectbox("Location", locs)
    with col3:
        nodes = sorted(full_df[(full_df['project']==sel_proj) & (full_df['location']==sel_loc)]['nodenumber'].unique())
        sel_node = st.selectbox("Node", nodes)

    plot_df = full_df[full_df['nodenumber'] == sel_node].sort_values('timestamp')
    
    if not plot_df.empty:
        fig = px.line(plot_df, x='timestamp', y='value', title=f"Sensor: {sel_node} ({sel_loc})")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data found for this node.")

# --- SERVICE: DATA EXPORT LAB ---
elif service == "📥 Data Export Lab" and not full_df.empty:
    st.header("📥 Data Export Lab")
    start_d = st.sidebar.date_input("Start Date", value=date.today() - pd.Timedelta(days=7))
    end_d = st.sidebar.date_input("End Date", value=date.today())
    
    # Filter and Download
    export_df = full_df[(full_df['timestamp'].dt.date >= start_d) & (full_df['timestamp'].dt.date <= end_d)]
    st.dataframe(export_df.head(100))
    
    csv = export_df.to_csv(index=False).encode('utf-8')
    st.download_button("Download CSV", data=csv, file_name=f"SoilFreeze_Export_{start_d}.csv")

# --- SERVICE: DATA CLEANING TOOL ---
elif service == "🧹 Data Cleaning Tool" and not full_df.empty:
    st.header("🧹 Data Cleaning Tool")
    st.write("Remove outliers based on temperature bounds.")
    
    min_t, max_t = st.slider("Valid Temperature Range (°C)", -60, 100, (-40, 60))
    
    clean_df = full_df[(full_df['value'] >= min_t) & (full_df['value'] <= max_t)]
    st.success(f"Cleaned Data: {len(clean_df)} rows (Removed {len(full_df)-len(clean_df)} outliers)")
    st.dataframe(clean_df.head(100))
