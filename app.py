import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. Page Config & CSS (Force clean tables)
st.set_page_config(page_title="Geotechnical Temp Dashboard", layout="wide")

# 2. Sidebar Controls
st.sidebar.title("📁 Project Controls")
unit = st.sidebar.radio("Temperature Unit", ["Fahrenheit (°F)", "Celsius (°C)"])
is_celsius = unit == "Celsius (°C)"
u_symbol = "°C" if is_celsius else "°F"
alert_threshold = 1.0 if is_celsius else 1.8

st.sidebar.subheader("Reference Lines")
show_red_ref = st.sidebar.checkbox("Show 10.2 Line (Red)", value=True)
show_blue_ref = st.sidebar.checkbox("Show 26.6 Line (Blue)", value=True)
show_freezing_ref = st.sidebar.checkbox("Show 32.0 Line (Blue)", value=True)

# 3. Data Loading
scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform"]
creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
client = bigquery.Client(credentials=creds, project="sensorpush-export")

@st.cache_data(ttl=300)
def get_full_dataset():
    query = "SELECT * FROM `sensorpush-export.sensor_data.final_dashboard_data` ORDER BY timestamp ASC"
    return client.query(query).to_dataframe()

# LOAD DATA FIRST
df_raw = get_full_dataset()
df_raw.columns = [str(c).strip().lower() for c in df_raw.columns]
df_raw['timestamp'] = pd.to_datetime(df_raw['timestamp'])

if is_celsius:
    df_raw['value'] = (df_raw['value'] - 32) * 5/9

available_projects = sorted(df_raw['project'].dropna().unique())
selected_project = st.sidebar.selectbox("Choose Project", available_projects)
df_proj = df_raw[df_raw['project'] == selected_project].copy()

# 4. DEFINE TABS HERE (Prevents NameError)
tab_summary, tab_depth, tab_time = st.tabs(["📊 24-Hour Insights", "📏 Temp vs Depth", "📈 Temp vs Time"])

# 5. Define Helpers
def add_ref_lines(ax, is_vertical=True):
    refs = []
    if show_red_ref:
        v_f = 10.2
        v = round((v_f - 32) * 5/9, 1) if is_celsius else v_f
        refs.append({'val': v, 'color': 'red', 'label': f"{v}{u_symbol}"})
    if show_blue_ref:
        v_f = 26.6
        v = round((v_f - 32) * 5/9, 1) if is_celsius else v_f
        refs.append({'val': v, 'color': 'blue', 'label': f"{v}{u_symbol}"})
    if show_freezing_ref:
        v = 0.0 if is_celsius else 32.0
        refs.append({'val': v, 'color': 'blue', 'label': f"{v}{u_symbol}"})
    for r in refs:
        if is_vertical: ax.axvline(x=r['val'], color=r['color'], linestyle='--', linewidth=1.5, label=r['label'])
        else: ax.axhline(y=r['val'], color=r['color'], linestyle='--', linewidth=1.5, label=r['label'])

# --- TAB: 24-HOUR INSIGHTS ---
with tab_summary:
    col1, col2 = st.columns([2, 1])
    now = pd.Timestamp.now(tz='UTC')
    last_24 = df_proj[df_proj['timestamp'] >= (now - pd.Timedelta(hours=24))].copy()
    
    with col1:
        if not last_24.empty:
            node_stats = last_24.groupby(['location', 'depth'])['value'].agg(['min', 'max']).reset_index()
            node_stats['delta'] = node_stats['max'] - node_stats['min']
            
            p_rows, b_rows = [], []
            for loc in sorted(last_24['location'].unique()):
                p_data = node_stats[node_stats['location'] == loc]
                p_min, p_max = p_data['min'].min(), p_data['max'].max()
                top_node = p_data.loc[p_data['delta'].idxmax()]
                
                row = {
                    "Pipe": loc,
                    "Min Temp": f"{p_min:.1f}{u_symbol}",
                    "Max Temp": f"{p_max:.1f}{u_symbol}",
                    "Max Change at": f"{float(top_node['depth']):.1f}ft" if "bank" not in loc.lower() else top_node['depth'],
                    "24h Change": f"{top_node['delta']:.1f}{u_symbol}",
                    "_raw_delta": top_node['delta'] # Leading underscore is a convention for hidden math
                }
                
                if "bank" in loc.lower(): b_rows.append(row)
                else: p_rows.append(row)

            def style_and_clean(data_list):
                if not data_list: return
                df = pd.DataFrame(data_list)
                # Apply styling based on the hidden raw column
                styler = df.style.apply(lambda x: ['color: red' if x['_raw_delta'] >= alert_threshold else '' for _ in x], axis=1)
                # REMOVE the raw delta column entirely before rendering
                styler = styler.hide(axis='columns', subset=['_raw_delta'])
                st.table(styler)

            st.subheader("Standard Pipes: 24h Activity")
            style_and_clean(p_rows)
            st.subheader("Bank Temperatures: 24h Activity")
            style_and_clean(b_rows)
        else:
            st.info("No data found for the last 24 hours.")

    with col2:
        st.subheader("⚠️ Offline Sensors")
        # [Offline logic remains here...]
