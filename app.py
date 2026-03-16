import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. Setup
st.set_page_config(page_title="Geotechnical Temp Dashboard", layout="wide")

scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform"]
creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
client = bigquery.Client(credentials=creds, project="sensorpush-export")

# 2. Sidebar & Data Loading
st.sidebar.title("📁 Project Controls")

unit = st.sidebar.radio("Temperature Unit", ["Fahrenheit (°F)", "Celsius (°C)"])
is_celsius = unit == "Celsius (°C)"
u_symbol = "°C" if is_celsius else "°F"
alert_threshold = 1.0 if is_celsius else 1.8

st.sidebar.subheader("Reference Lines")
show_red_ref = st.sidebar.checkbox("Show 10.2 Line (Red)", value=True)
show_blue_ref = st.sidebar.checkbox("Show 26.6 Line (Blue)", value=True)
show_freezing_ref = st.sidebar.checkbox("Show 32.0 Line (Blue)", value=True)

@st.cache_data(ttl=300)
def get_full_dataset():
    query = "SELECT * FROM `sensorpush-export.sensor_data.final_dashboard_data` ORDER BY timestamp ASC"
    return client.query(query).to_dataframe()

df_raw = get_full_dataset()
df_raw.columns = [str(c).strip().lower() for c in df_raw.columns]
df_raw['timestamp'] = pd.to_datetime(df_raw['timestamp'])
df_raw['project'] = df_raw['project'].fillna('Unnamed').astype(str)

if is_celsius:
    df_raw['value'] = (df_raw['value'] - 32) * 5/9

available_projects = sorted(df_raw['project'].unique())
selected_project = st.sidebar.selectbox("Choose Project", available_projects)
df_proj = df_raw[df_raw['project'] == selected_project].copy()

num_weeks = st.sidebar.slider("Weeks of History", 1, 24, 8)
cutoff_date = pd.Timestamp.now(tz='UTC') - pd.Timedelta(weeks=num_weeks)
df_filtered = df_proj[df_proj['timestamp'] >= cutoff_date].copy()

# 3. Helpers
def add_ref_lines(ax, is_vertical=True):
    refs = []
    if show_red_ref:
        val_f = 10.2
        disp_val = round((val_f - 32) * 5/9, 1) if is_celsius else val_f
        refs.append({'val': disp_val, 'color': 'red', 'label': f"{disp_val}{u_symbol}"})
    if show_blue_ref:
        val_f = 26.6
        disp_val = round((val_f - 32) * 5/9, 1) if is_celsius else val_f
        refs.append({'val': disp_val, 'color': 'blue', 'label': f"{disp_val}{u_symbol}"})
    if show_freezing_ref:
        val_f = 32.0
        disp_val = 0.0 if is_celsius else 32.0
        refs.append({'val': disp_val, 'color': 'blue', 'label': f"{disp_val}{u_symbol}"})
    
    for r in refs:
        if is_vertical:
            ax.axvline(x=r['val'], color=r['color'], linestyle='--', linewidth=1.5, label=r['label'])
        else:
            ax.axhline(y=r['val'], color=r['color'], linestyle='--', linewidth=1.5, label=r['label'])

# 4. Tabs
tab_summary, tab_depth, tab_time = st.tabs(["📊 24-Hour Insights", "📏 Temp vs Depth", "📈 Temp vs Time"])

# --- TAB: 24-HOUR INSIGHTS ---
with tab_summary:
    col1, col2 = st.columns([2, 1])
    now = pd.Timestamp.now(tz='UTC')
    last_24 = df_proj[df_proj['timestamp'] >= (now - pd.Timedelta(hours=24))].copy()
    
    with col1:
        if not last_24.empty:
            node_stats = last_24.groupby(['location', 'depth'])['value'].agg(['min', 'max']).reset_index()
            node_stats['delta'] = node_stats['max'] - node_stats['min']
            
            pipe_rows = []
            bank_rows = []
            
            for loc in sorted(last_24['location'].unique()):
                pipe_data = node_stats[node_stats['location'] == loc]
                p_min, p_max = pipe_data['min'].min(), pipe_data['max'].max()
                top_node_row = pipe_data.loc[pipe_data['delta'].idxmax()]
                
                # Format numeric strings with units
                row = {
                    "Pipe": loc,
                    "Min Temp": f"{p_min:.1f}{u_symbol}",
                    "Max Temp": f"{p_max:.1f}{u_symbol}",
                    "Max Change at": top_node_row['depth'],
                    "24h Change": round(top_node_row['delta'], 1), # Keep float for styling logic
                    "24h Change Display": f"{top_node_row['delta']:.1f}{u_symbol}"
                }
                
                if "bank" in loc.lower():
                    bank_rows.append(row)
                else:
                    row["Max Change at"] = f"{float(row['Max Change at']):.1f}ft"
                    pipe_rows.append(row)

            def style_alert(row):
                color = 'red' if row['24h Change'] >= alert_threshold else None
                return [f'color: {color}' if color else '' for _ in row]

            # Display Standard Pipes
            st.subheader("Standard Pipes: 24h Activity")
            if pipe_rows:
                df_p = pd.DataFrame(pipe_rows).drop(columns=['24h Change']).rename(columns={"24h Change Display": "24h Change"})
                # Re-map the delta for styling since we dropped the float column
                st.table(pd.DataFrame(pipe_rows).style.apply(style_alert, axis=1)
                         .format({"24h Change": "{:.1f}" + u_symbol})
                         .hide(axis='columns', subset=['24h Change'])
                         .rename(columns={"24h Change Display": "24h Change"}))
            
            # Display Banks
            st.subheader("Bank Temperatures: 24h Activity")
            if bank_rows:
                st.table(pd.DataFrame(bank_rows).style.apply(style_alert, axis=1)
                         .format({"24h Change": "{:.1f}" + u_symbol})
                         .hide(axis='columns', subset=['24h Change'])
                         .rename(columns={"24h Change Display": "24h Change"}))
        else:
            st.info("No active data in the last 24 hours.")

    with col2:
        st.subheader("⚠️ Offline Sensors")
        all_sensors = df_proj[['location', 'depth']].drop_duplicates()
        active_sensors = last_24[['location', 'depth']].drop_duplicates()
        offline = all_sensors.merge(active_sensors, on=['location', 'depth'], how='left', indicator=True)
        offline = offline[offline['_merge'] == 'left_only'][['location', 'depth']]
        if not offline.empty:
            st.warning(f"{len(offline)} nodes offline (24h+)")
            st.dataframe(offline.rename(columns={'location': 'Pipe', 'depth': 'Node'}), hide_index=True)
        else:
            st.success("All sensors online.")

# ... [Rest of plotting code remains the same as previous version]
