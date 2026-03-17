import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. SETUP
st.set_page_config(page_title="SoilFreeze Production Dashboard", layout="wide")

# 2. BIGQUERY CONNECTION
try:
    scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform"]
    creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    client = bigquery.Client(credentials=creds, project="sensorpush-export")
except Exception as e:
    st.error(f"Credentials Error: {e}")
    st.stop()

# 3. DATA LOADING
@st.cache_data(ttl=60)
def get_full_dataset():
    query = "SELECT * FROM `sensorpush-export.sensor_data.final_dashboard_data` ORDER BY timestamp ASC"
    return client.query(query).to_dataframe()

df_raw = get_full_dataset()
df_raw.columns = [str(c).strip().lower() for c in df_raw.columns]
df_raw['timestamp'] = pd.to_datetime(df_raw['timestamp'])

# --- DATA REPAIR LOGIC ---
# Remove null rows to keep the UI clean
df_raw = df_raw.dropna(subset=['value', 'depth', 'location'])

# THE HYPHEN FIX: Standardize Node IDs
# This converts '54018-ch1' to '54018_ch1' so they appear as ONE sensor
df_raw['depth'] = (
    df_raw['depth']
    .astype(str)
    .str.replace('-', '_', regex=False)
    .str.replace(r'\.0$', '', regex=True)
    .str.strip()
)

# 4. SIDEBAR
st.sidebar.title("📁 Main Dashboard")

unit = st.sidebar.radio("Temperature Unit", ["Fahrenheit (°F)", "Celsius (°C)"])
is_celsius = unit == "Celsius (°C)"
u_symbol = "°C" if is_celsius else "°F"
alert_threshold = 1.0 if is_celsius else 1.8

# Select Project
available_projects = sorted(df_raw['project'].unique())
selected_project = st.sidebar.selectbox("Choose Project", available_projects)

df_proj = df_raw[df_raw['project'] == selected_project].copy()
if is_celsius:
    df_proj['value'] = (df_proj['value'] - 32) * 5/9

st.sidebar.subheader("Reference Lines")
show_red_ref = st.sidebar.checkbox("Show 10.2 Line (Red)", value=True)
show_blue_ref = st.sidebar.checkbox("Show 26.6 Line (Blue)", value=True)

num_weeks = st.sidebar.slider("Weeks of History", 1, 24, 8)
cutoff_date = pd.Timestamp.now(tz='UTC') - pd.Timedelta(weeks=num_weeks)
df_filtered = df_proj[df_proj['timestamp'] >= cutoff_date].copy()

# 5. UI TABS
tab_summary, tab_depth, tab_time = st.tabs(["📊 24-Hour Insights", "📏 Temp vs Depth", "📈 Temp vs Time"])

# --- TAB 1: 24-HOUR INSIGHTS ---
with tab_summary:
    now = pd.Timestamp.now(tz='UTC')
    last_24 = df_proj[df_proj['timestamp'] >= (now - pd.Timedelta(hours=24))].copy()
    
    if not last_24.empty:
        node_stats = last_24.groupby(['location', 'depth'])['value'].agg(['min', 'max']).reset_index()
        node_stats['delta'] = node_stats['max'] - node_stats['min']
        
        p_rows = []
        for loc in sorted(last_24['location'].unique()):
            p_data = node_stats[node_stats['location'] == loc]
            if p_data.empty: continue
            p_min, p_max = p_data['min'].min(), p_data['max'].max()
            top_node = p_data.loc[p_data['delta'].idxmax()]
            
            row_disp = {
                "Pipe": loc,
                "Min Temp": f"{p_min:.1f}{u_symbol}",
                "Max Temp": f"{p_max:.1f}{u_symbol}",
                "Max Change at": f"{top_node['depth']}",
                "24h Change": f"{top_node['delta']:.1f}{u_symbol}"
            }
            color = 'color: red' if top_node['delta'] >= alert_threshold else ''
            p_rows.append((row_disp, color))

        st.subheader("Sensor Activity (Last 24h)")
        if p_rows:
            df_disp = pd.DataFrame([r[0] for r in p_rows])
            colors = [r[1] for r in p_rows]
            st.table(df_disp.style.apply(lambda x: [colors[i] for i in range(len(x))], axis=0))
    else:
        st.info("No sensor activity in the last 24 hours.")

# --- TAB 3: TEMP VS TIME (Simplified for visibility) ---
with tab_time:
    st.subheader(f"Temperature vs Time ({u_symbol})")
    for loc in sorted(df_filtered['location'].unique()):
        with st.expander(f"Location: {loc}", expanded=True):
            df_lt = df_filtered[df_filtered['location'] == loc].sort_values('timestamp')
            if not df_lt.empty:
                fig, ax = plt.subplots(figsize=(10, 4))
                for d in sorted(df_lt['depth'].unique()):
                    sub = df_lt[df_lt['depth'] == d]
                    ax.plot(sub['timestamp'], sub['value'], label=f"Node {d}")
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
                plt.xticks(rotation=45)
                ax.legend(loc='upper left', bbox_to_anchor=(1, 1))
                st.pyplot(fig)
