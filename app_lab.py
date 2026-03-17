import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date

# --- 1. AUTHENTICATION ---
# --- 1. AUTHENTICATION (Updated for Google Sheets Access) ---
# We need to add 'drive' to the scopes so BigQuery can read the metadata sheet
SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform"
]

if "gcp_service_account" in st.secrets:
    info = st.secrets["gcp_service_account"]
    # We add 'scopes=SCOPES' here
    credentials = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    client = bigquery.Client(credentials=credentials, project=info["project_id"])
else:
    # For local testing, ensure your .json key also has Drive access
    client = bigquery.Client.from_service_account_json("service_account.json", scopes=SCOPES)
# --- 2. DATA PULL (Standardized Columns) ---
@st.cache_data(ttl=600)
def fetch_engineering_data():
    # This query uses the exact columns from your Master Metadata screenshot
    query = """
    WITH raw_combined AS (
        SELECT CAST(timestamp AS TIMESTAMP) as timestamp, value, nodenumber 
        FROM `sensorpush-export.sensor_data.raw_lord`
        UNION ALL
        SELECT timestamp, temperature AS value, sensor_name AS nodenumber 
        FROM `sensorpush-export.sensor_data.raw_sensorpush`
    )
    SELECT 
        r.timestamp, 
        r.value, 
        r.nodenumber, 
        m.Project, 
        m.Location, 
        m.Depth
    FROM raw_combined AS r
    LEFT JOIN `sensorpush-export.sensor_data.master_metadata` AS m
      ON r.nodenumber = m.NodeNum
    """
    df = client.query(query).to_dataframe()
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df

try:
    full_df = fetch_engineering_data()
except Exception as e:
    st.sidebar.error(f"Error fetching data: {e}")
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
        projs = sorted(full_df['Project'].dropna().unique())
        sel_proj = st.selectbox("Select Project", projs)
    with col2:
        locs = sorted(full_df[full_df['Project']==sel_proj]['Location'].dropna().unique())
        sel_loc = st.selectbox("Select Location", locs)
    with col3:
        nodes = sorted(full_df[(full_df['Project']==sel_proj) & (full_df['Location']==sel_loc)]['nodenumber'].unique())
        sel_node = st.selectbox("Select Node (Serial)", nodes)

    plot_df = full_df[full_df['nodenumber'] == sel_node].sort_values('timestamp')
    
    if not plot_df.empty:
        fig = px.line(plot_df, x='timestamp', y='value', title=f"Sensor: {sel_node} | Depth: {plot_df['Depth'].iloc[0]}")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data points found for this selection.")

# --- SERVICE: DATA EXPORT LAB ---
elif service == "📥 Data Export Lab" and not full_df.empty:
    st.header("📥 Data Export Lab")
    start_d = st.sidebar.date_input("Start Date", value=date.today() - pd.Timedelta(days=14))
    end_d = st.sidebar.date_input("End Date", value=date.today())
    
    # Filter by date and provide download
    export_df = full_df[(full_df['timestamp'].dt.date >= start_d) & (full_df['timestamp'].dt.date <= end_d)]
    st.write(f"Showing {len(export_df)} records.")
    st.dataframe(export_df.head(500))
    
    csv = export_df.to_csv(index=False).encode('utf-8')
    st.download_button("📥 Download Filtered CSV", data=csv, file_name=f"SoilFreeze_Lab_Export.csv")

# --- SERVICE: DATA CLEANING TOOL ---
elif service == "🧹 Data Cleaning Tool" and not full_df.empty:
    st.header("🧹 Data Cleaning Tool")
    st.markdown("Use this to filter out outliers (like sensor open-circuit errors) from your view.")
    
    min_t, max_t = st.slider("Keep values between (°C)", -60, 100, (-40, 50))
    
    clean_df = full_df[(full_df['value'] >= min_t) & (full_df['value'] <= max_t)]
    st.success(f"Original: {len(full_df)} | Cleaned: {len(clean_df)} (Removed {len(full_df)-len(clean_df)} points)")
    
    # Preview cleaned data
    st.dataframe(clean_df.head(100))
