import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz

# --- 1. SETUP & AUTH ---
st.set_page_config(page_title="SoilFreeze Tech Dashboard", layout="wide", page_icon="🛠️")

try:
    scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform"]
    # Using the same secrets pattern as your client app
    creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    client = bigquery.Client(credentials=creds, project=st.secrets["gcp_service_account"]["project_id"])
except Exception as e:
    st.error(f"Credentials Error: {e}")
    st.stop()

# --- 2. DATA LOADING ---
@st.cache_data(ttl=300)
def get_full_dataset():
    # Technician view doesn't filter by PID in SQL so all jobs can be seen
    query = "SELECT * FROM `sensorpush-export.sensor_data.final_databoard_data` ORDER BY timestamp ASC"
    df = client.query(query).to_dataframe()
    # Metadata Join if needed for Project Names
    meta_query = "SELECT NodeNum, Project, Location, Depth FROM `sensorpush-export.sensor_data.master_metadata`"
    meta = client.query(meta_query).to_dataframe()
    return df, meta

df_raw, df_meta = get_full_dataset()
df_raw['timestamp'] = pd.to_datetime(df_raw['timestamp'], utc=True)

# --- 3. SIDEBAR & GLOBAL FILTERS ---
st.sidebar.title("🛠️ Tech Operations")
unit = st.sidebar.radio("Display Unit", ["Fahrenheit (°F)", "Celsius (°C)"])
is_celsius = unit == "Celsius (°C)"
u_symbol = "°C" if is_celsius else "°F"

# Technician can see ALL projects
all_projects = sorted(df_meta['Project'].unique())
selected_project = st.sidebar.selectbox("Select Active Project", all_projects)

num_weeks = st.sidebar.slider("Weeks of History", 1, 12, 4)

# Monday-to-Monday Calculation
now_utc = datetime.now(tz=pytz.UTC)
days_until_monday = (7 - now_utc.weekday()) % 7
if days_until_monday == 0: days_until_monday = 7
graph_end = (now_utc + timedelta(days=days_until_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
graph_start = graph_end - timedelta(weeks=num_weeks)

# Filter Data
proj_nodes = df_meta[df_meta['Project'] == selected_project]['NodeNum'].unique()
df_proj = df_raw[df_raw['nodenumber'].isin(proj_nodes)].merge(df_meta, left_on='nodenumber', right_on='NodeNum')

if is_celsius:
    df_proj['value'] = (df_proj['value'] - 32) * 5/9

# --- 4. TABS ---
tab_health, tab_depth, tab_time = st.tabs(["📡 System Health", "📏 Depth Profiles", "📈 Time History"])

# --- TAB 1: SYSTEM HEALTH (OFFLINE DETECTION) ---
with tab_health:
    st.subheader(f"Project Health: {selected_project}")
    col1, col2 = st.columns([2, 1])
    
    # 24h Activity
    cutoff_24h = now_utc - timedelta(hours=24)
    last_24 = df_proj[df_proj['timestamp'] >= cutoff_24h]
    
    with col2:
        st.subheader("⚠️ Connectivity Issues")
        # Expected sensors (from Meta) vs Active sensors (last 24h)
        expected = df_meta[df_meta['Project'] == selected_project][['Location', 'Depth', 'NodeNum']]
        active_nodes = last_24['nodenumber'].unique()
        offline = expected[~expected['NodeNum'].isin(active_nodes)]
        
        if not offline.empty:
            st.error(f"{len(offline)} Sensors Offline (No data < 24h)")
            st.dataframe(offline[['Location', 'Depth']], hide_index=True, width=400)
        else:
            st.success("All project sensors are reporting.")

    with col1:
        st.subheader("Recent Activity Summary")
        if not last_24.empty:
            summary = last_24.groupby(['Location', 'Depth'])['value'].agg(['min', 'max', 'last']).reset_index()
            st.dataframe(summary.style.format({"min": "{:.1f}", "max": "{:.1f}", "last": "{:.1f}"}), width=800)
        else:
            st.warning("No data found for this project in the last 24 hours.")

# --- TAB 2: DEPTH PROFILES ---
with tab_depth:
    st.subheader("Monday 6:00 AM Snapshots")
    locs = sorted(df_proj['Location'].unique())
    selected_loc = st.selectbox("Select Pipe for Profile", locs)
    
    loc_data = df_proj[df_proj['Location'] == selected_loc].copy()
    
    # Snapshot / Failsafe Logic
    mondays = [graph_end - timedelta(weeks=i) for i in range(num_weeks)]
    all_snaps = []
    for m in mondays:
        t_time = m.replace(hour=6, minute=0)
        window = loc_data[(loc_data['timestamp'] >= t_time - timedelta(hours=3)) & 
                          (loc_data['timestamp'] <= t_time + timedelta(hours=3))]
        if not window.empty:
            # Get closest to 6am
            window['diff'] = (window['timestamp'] - t_time).abs()
            best_ts = window.sort_values('diff')['timestamp'].iloc[0]
            snap = window[window['timestamp'] == best_ts].copy()
            snap['Date'] = t_time.strftime('%b %d')
            all_snaps.append(snap)
    
    if all_snaps:
        plot_df = pd.concat(all_snaps).sort_values('Depth')
        fig, ax = plt.subplots(figsize=(8, 10))
        for date, group in plot_df.groupby('Date'):
            ax.plot(group['value'], group['Depth'], marker='o', label=date)
        
        ax.set_title(f"Thermal Profile: {selected_loc}")
        ax.invert_yaxis()
        ax.set_xlabel(f"Temp ({u_symbol})")
        ax.set_ylabel("Depth (ft)")
        ax.grid(True, which='both', linestyle=':', alpha=0.6)
        ax.legend()
        # Framing and standard lines
        ax.axvline(0 if is_celsius else 32, color='blue', linestyle='--')
        st.pyplot(fig)

# --- TAB 3: TIME HISTORY ---
with tab_time:
    st.subheader("Historical Trends")
    # Same plot logic as client view but using Matplotlib for the Technician's requested style
    fig_time, ax_time = plt.subplots(figsize=(14, 6))
    for node, group in loc_data.groupby('Depth'):
        ax_time.plot(group['timestamp'], group['value'], label=f"{node} ft")
    
    # Custom Gridlines (Dark Monday, Light Day)
    for day in pd.date_range(graph_start, graph_end):
        lw = 1.5 if day.weekday() == 0 else 0.5
        color = '#333333' if day.weekday() == 0 else '#CCCCCC'
        ax_time.axvline(day, color=color, linewidth=lw, alpha=0.5)
        
    ax_time.set_xlim(graph_start, graph_end)
    ax_time.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax_time.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    st.pyplot(fig_time)
