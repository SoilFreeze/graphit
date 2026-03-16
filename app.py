import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. SETUP
st.set_page_config(page_title="Master Geotechnical Dashboard", layout="wide")

# 2. DATA LOADING
scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform"]
creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
client = bigquery.Client(credentials=creds, project="sensorpush-export")

@st.cache_data(ttl=300)
def get_full_dataset():
    query = "SELECT * FROM `sensorpush-export.sensor_data.final_dashboard_data` ORDER BY timestamp ASC"
    return client.query(query).to_dataframe()

df_raw = get_full_dataset()
df_raw.columns = [str(c).strip().lower() for c in df_raw.columns]
df_raw['timestamp'] = pd.to_datetime(df_raw['timestamp'])

# 3. SIDEBAR - MASTER CONTROLS
st.sidebar.title("📁 Master Controls")
available_projects = sorted(df_raw['project'].dropna().unique())
selected_project = st.sidebar.selectbox("Choose Project", available_projects)
df_proj = df_raw[df_raw['project'] == selected_project].copy()

# ... [Include the rest of the logic: Units, Tabs, Tables, and Graphs] ...
