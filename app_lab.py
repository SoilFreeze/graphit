import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date

# --- 1. AUTHENTICATION (Standardized) ---
SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform"
]

if "gcp_service_account" in st.secrets:
    info = st.secrets["gcp_service_account"]
    credentials = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    client = bigquery.Client(credentials=credentials, project=info["project_id"])
else:
    client = bigquery.Client.from_service_account_json("service_account.json", scopes=SCOPES)

# --- 2. DATA FETCHING ---
@st.cache_data(ttl=600)
def fetch_engineering_data():
    query = """
    WITH raw_combined AS (
        SELECT CAST(timestamp AS TIMESTAMP) as timestamp, value, nodenumber 
        FROM `sensorpush-export.sensor_data.raw_lord`
        UNION ALL
        SELECT timestamp, temperature AS value, sensor_name AS nodenumber 
        FROM `sensorpush-export.sensor_data.raw_sensorpush`
    )
    SELECT 
        r.timestamp, r.value, r.nodenumber, 
        m.Project, m.Location, m.Depth
    FROM raw_combined AS r
    LEFT JOIN `sensorpush-export.sensor_data.master_metadata` AS m
      ON r.nodenumber = m.NodeNum
    """
    df = client.query(query).to_dataframe()
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df

# Initialize Data
full_df = pd.DataFrame()
try:
    full_df = fetch_engineering_data()
except Exception as e:
    st.sidebar.error(f"Database Error: {e}")

# --- 3. MAIN INTERFACE ---
st.title("📥 SoilFreeze Engineering Lab")

if not full_df.empty:
    st.subheader("Data Export Controls")
    
    # Filter Layout
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=date.today() - pd.Timedelta(days=14))
        ex_projs = sorted(full_df['Project'].dropna().unique())
        sel_proj = st.selectbox("Select Project", ex_projs)
        
    with col2:
        end_date = st.date_input("End Date", value=date.today())
        ex_locs = ["All Locations"] + sorted(full_df[full_df['Project']==sel_proj]['Location'].dropna().unique().tolist())
        sel_loc = st.selectbox("Select Location", ex_locs)

    # Filtering Logic
    export_df = full_df[
        (full_df['Project'] == sel_proj) & 
        (full_df['timestamp'].dt.date >= start_date) & 
        (full_df['timestamp'].dt.date <= end_date)
    ]
    
    if sel_loc != "All Locations":
        export_df = export_df[export_df['Location'] == sel_loc]

    # Results & Download
    st.markdown("---")
    st.write(f"📊 **Rows Found:** {len(export_df)}")
    st.dataframe(export_df.head(500), use_container_width=True)

    if not export_df.empty:
        csv = export_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download CSV Export",
            data=csv,
            file_name=f"SoilFreeze_{sel_proj}_{sel_loc}.csv",
            mime='text/csv'
        )
else:
    st.info("Loading data from BigQuery...")
