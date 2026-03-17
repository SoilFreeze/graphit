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
df_raw = df_raw.dropna(subset=['value', 'depth', 'location'])

# STANDARD: Force all node names to use DASHES to match your mapping
df_raw['depth'] = (
    df_raw['depth']
    .astype(str)
    .str.replace('_', '-', regex=False)
    .str.replace(r'\.0$', '', regex=True)
    .str.strip()
)

# 4. SIDEBAR
st.sidebar.title("📁 Main Dashboard")
unit = st.sidebar.radio("Temperature Unit", ["Fahrenheit (°F)", "Celsius (°C)"])
is_celsius = unit == "Celsius (°C)"
u_symbol = "°C" if is_celsius else "°F"
alert_threshold = 1.0 if is_celsius else 1.8

available_projects = sorted(df_raw['project'].unique())
selected_project = st.sidebar.selectbox("Choose Project", available_projects)

df_proj = df_raw[df_raw['project'] == selected_project].copy()
if is_celsius:
    df_proj['value'] = (df_proj['value'] - 32) * 5/9

st.sidebar.subheader("Reference Lines")
show_red_ref = st.sidebar.checkbox("Show 10.2 Line (Red)", value=True)
show_blue_ref = st.sidebar.checkbox("Show 26.6 Line (Blue)", value=True)

num_weeks = st.sidebar.slider("Weeks of History", 1, 24, 8)

# 5. UI TABS
tab_summary, tab_depth, tab_time = st.tabs(["📊 24-Hour Insights", "📏 Temp vs Depth", "📈 Temp vs Time"])

# --- HELPERS ---
def add_ref_lines(ax, is_vertical=True):
    refs = []
    if show_red_ref:
        v = round((10.2 - 32) * 5/9, 1) if is_celsius else 10.2
        refs.append({'val': v, 'color': 'red', 'label': f"{v}{u_symbol}"})
    if show_blue_ref:
        v = round((26.6 - 32) * 5/9, 1) if is_celsius else 26.6
        refs.append({'val': v, 'color': 'blue', 'label': f"{v}{u_symbol}"})
    for r in refs:
        if is_vertical: ax.axvline(x=r['val'], color=r['color'], linestyle='--', linewidth=1.5)
        else: ax.axhline(y=r['val'], color=r['color'], linestyle='--', linewidth=1.5)

# --- TAB 1: 24-HOUR INSIGHTS (RESTORED) ---
with tab_summary:
    col_tables, col_offline = st.columns([2, 1])
    now = pd.Timestamp.now(tz='UTC')
    last_24 = df_proj[df_proj['timestamp'] >= (now - pd.Timedelta(hours=24))].copy()
    
    with col_tables:
        if not last_24.empty:
            node_stats = last_24.groupby(['location', 'depth'])['value'].agg(['min', 'max']).reset_index()
            node_stats['delta'] = node_stats['max'] - node_stats['min']
            
            p_rows, b_rows = [], []
            for loc in sorted(last_24['location'].unique()):
                p_data = node_stats[node_stats['location'] == loc]
                p_min, p_max = p_data['min'].min(), p_data['max'].max()
                top_node = p_data.loc[p_data['delta'].idxmax()]
                
                row_disp = {"Pipe": loc, "Min": f"{p_min:.1f}", "Max": f"{p_max:.1f}", "Change": f"{top_node['delta']:.1f}", "at Node": top_node['depth']}
                color = 'color: red' if top_node['delta'] >= alert_threshold else ''
                if "bank" in loc.lower(): b_rows.append((row_disp, color))
                else: p_rows.append((row_disp, color))

            st.subheader("Standard Pipes: 24h Activity")
            if p_rows: st.table(pd.DataFrame([r[0] for r in p_rows]).style.apply(lambda x: [p_rows[i][1] for i in range(len(x))], axis=0))
            
            st.subheader("Bank Temperatures: 24h Activity")
            if b_rows: st.table(pd.DataFrame([r[0] for r in b_rows]).style.apply(lambda x: [b_rows[i][1] for i in range(len(x))], axis=0))
        else:
            st.info("No sensor activity in the last 24 hours.")

    with col_offline:
        st.subheader("⚠️ Offline Sensors")
        all_sensors = df_proj[['location', 'depth']].drop_duplicates()
        active_sensors = last_24[['location', 'depth']].drop_duplicates()
        offline = all_sensors.merge(active_sensors, on=['location', 'depth'], how='left', indicator=True)
        offline = offline[offline['_merge'] == 'left_only']
        if not offline.empty:
            st.warning(f"{len(offline)} nodes offline")
            st.dataframe(offline[['location', 'depth']], hide_index=True)
        else:
            st.success("All sensors online.")

# --- TAB 3: TEMP VS TIME (IMPROVED GRAPHICS) ---
with tab_time:
    # 1. GRAPH CALCULATIONS: Start on Monday
    today = pd.Timestamp.now(tz='UTC').normalize()
    graph_monday = today - pd.Timedelta(days=today.weekday()) - pd.Timedelta(weeks=num_weeks-1)
    graph_end = pd.Timestamp.now(tz='UTC')

    for loc in sorted(df_proj['location'].unique()):
        with st.expander(f"Location: {loc}", expanded=True):
            df_lt = df_proj[df_proj['location'] == loc].sort_values('timestamp')
            if not df_lt.empty:
                fig, ax = plt.subplots(figsize=(12, 5))
                
                # Plot data
                for d in sorted(df_lt['depth'].unique()):
                    sub = df_lt[df_lt['depth'] == d]
                    ax.plot(sub['timestamp'], sub['value'], label=f"Node {d}", linewidth=1.5)
                
                # 2. GRID LINES: Monday Midnight (Dark) vs Daily (Light)
                all_days = pd.date_range(start=graph_monday, end=graph_end, freq='D')
                for day in all_days:
                    if day.weekday() == 0: ax.axvline(day, color='#333333', linewidth=1.2, alpha=0.7)
                    else: ax.axvline(day, color='#CCCCCC', linewidth=0.5, alpha=0.4)
                
                # 3. NO BUFFERS: Edge-to-Edge
                ax.set_xlim(graph_monday, graph_end)
                ax.margins(x=0)
                
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
                add_ref_lines(ax, is_vertical=False)
                ax.legend(loc='upper left', bbox_to_anchor=(1, 1), fontsize='x-small')
                st.pyplot(fig)
