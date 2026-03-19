import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account

# --- 1. STABLE CONFIG ---
st.set_page_config(layout="wide", page_title="SF Technician Dashboard")

# --- 2. AUTHENTICATION (Includes Drive Scopes for the Sheet) ---
if "gcp_service_account" in st.secrets:
    info = st.secrets["gcp_service_account"]
    scopes = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive.readonly"]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    client = bigquery.Client(credentials=creds, project=info["project_id"])
else:
    st.error("Secrets missing.")
    st.stop()

# --- 3. STABLE DATA FETCH ---
@st.cache_data(ttl=300)
def fetch_stable_data():
    query = """
    SELECT d.timestamp, d.value, d.nodenumber, m.Project, m.Location, m.Depth
    FROM `sensorpush-export.sensor_data.final_databoard_data` as d
    INNER JOIN `sensorpush-export.sensor_data.master_metadata` as m ON d.nodenumber = m.NodeNum
    ORDER BY d.timestamp ASC
    """
    df = client.query(query).to_dataframe()
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df

full_df = fetch_stable_data()

# --- 4. SIDEBAR & UI ---
st.sidebar.title("🛠️ Tech Operations")
all_projs = sorted(full_df['Project'].unique().tolist())
sel_proj = st.sidebar.selectbox("Select Project", all_projs)
df_proj = full_df[full_df['Project'] == sel_proj].copy()

tab1, tab2 = st.tabs(["📡 System Health", "📈 History"])

with tab1:
    st.subheader(f"Project: {sel_proj}")
    # Using st.dataframe instead of st.table to prevent freezing
    st.dataframe(df_proj.tail(20), use_container_width=True)

with tab2:
    locs = sorted(df_proj['Location'].unique())
    sel_loc = st.selectbox("Location", locs)
    time_df = df_proj[df_proj['Location'] == sel_loc].copy()
    
    # SIMPLE PLOT: No complex resampling yet
    fig = px.line(time_df, x='timestamp', y='value', color='Depth')
    st.plotly_chart(fig, use_container_width=True)
