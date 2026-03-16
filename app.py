import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. Setup Page and Auth
st.set_page_config(page_title="Geotechnical Temp Dashboard", layout="wide")

scopes = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform"
]

try:
    creds = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], 
        scopes=scopes
    )
    client = bigquery.Client(credentials=creds, project="sensorpush-export")
except Exception as e:
    st.error(f"Authentication Error: {e}")
    st.stop()

# 2. Sidebar - Project & Thresholds
st.sidebar.title("📁 Project Controls")

@st.cache_data(ttl=300)
def get_full_dataset():
    # Pulling from the master hourly dataset
    query = "SELECT * FROM `sensorpush-export.sensor_data.final_dashboard_data` ORDER BY timestamp ASC"
    return client.query(query).to_dataframe()

df_raw = get_full_dataset()

# --- DATA CLEANING ---
df_raw['timestamp'] = pd.to_datetime(df_raw['timestamp'])
# Fix for TypeError: fill empty projects and force to string
df_raw['Project'] = df_raw['Project'].fillna('Unnamed').astype(str)

available_projects = sorted(df_raw['Project'].unique())
selected_project = st.sidebar.selectbox("Choose Project", available_projects)

# Filter global data to only the selected project
df_proj = df_raw[df_raw['Project'] == selected_project].copy()

# Sidebar: Reference Marks
st.sidebar.subheader("Reference Marks")
show_freezing = st.sidebar.checkbox("Show Freezing Line (32°F)", value=True)
custom_marks_input = st.sidebar.text_input("Custom Reference Temps (comma separated)", "25, 40")

# Sidebar: Time Filter
num_weeks = st.sidebar.slider("Weeks of History", 1, 24, 8)
cutoff_date = pd.Timestamp.now(tz='UTC') - pd.Timedelta(weeks=num_weeks)
df_filtered = df_proj[df_proj['timestamp'] >= cutoff_date].copy()

st.title(f"Project: {selected_project}")
tab_depth, tab_time = st.tabs(["📊 Weekly Depth Profiles", "📈 Hourly Trends"])

available_locations = sorted(df_filtered['Location'].unique())

# --- Helper function for Reference Lines ---
def add_ref_lines(ax, is_vertical=True):
    if show_freezing:
        if is_vertical: 
            ax.axvline(x=32, color='blue', linestyle='--', linewidth=2, label='32°F Freezing')
        else: 
            ax.axhline(y=32, color='blue', linestyle='--', linewidth=2, label='32°F Freezing')
    
    if custom_marks_input:
        try:
            marks = [float(x.strip()) for x in custom_marks_input.split(',') if x.strip()]
            colors = ['green', 'orange', 'purple', 'brown']
            for idx, m in enumerate(marks):
                c = colors[idx % len(colors)]
                if is_vertical: 
                    ax.axvline(x=m, color=c, linestyle=':', label=f'Ref: {m}°F')
                else: 
                    ax.axhline(y=m, color=c, linestyle=':', label=f'Ref: {m}°F')
        except ValueError:
            pass # Ignore malformed input

# --- TAB 1: WEEKLY DEPTH PROFILES (Mondays @ 6 AM) ---
with tab_depth:
    st.subheader("Vertical Temperature: Mondays at 6:00 AM")
    for loc in available_locations:
        with st.expander(f"Location: {loc}", expanded=True):
            df_loc = df_filtered[df_filtered['Location'] == loc].copy()
            # Strict filter for Monday at 6 AM
            df_monday = df_loc[(df_loc['timestamp'].dt.weekday == 0) & (df_loc['timestamp'].dt.hour == 6)]
            
            if not df_monday.empty:
                fig1, ax1 = plt.subplots(figsize=(8, 6))
                for ts in sorted(df_monday['timestamp'].unique()):
                    snapshot = df_monday[df_monday['timestamp'] == ts].sort_values('Depth')
                    label_date = pd.to_datetime(ts).strftime('%Y-%m-%d')
                    ax1.plot(snapshot['temperature'], snapshot['Depth'], marker='o', label=label_date)
                
                ax1.invert_yaxis()
                add_ref_lines(ax1, is_vertical=True)
                ax1.set_xlabel("Temp (°F)")
                ax1.set_ylabel("Depth (ft)")
                ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='x-small')
                ax1.grid(True, alpha=0.2)
                st.pyplot(fig1)
            else:
                st.info(f"No Monday 6:00 AM data found for {loc} in this timeframe.")

# --- TAB 2: HOURLY TRENDS (The Granular "Wiggles") ---
with tab_time:
    st.subheader("Continuous Hourly Trends")
    for loc in available_locations:
        with st.expander(f"Trends: {loc}", expanded=True):
            df_loc_time = df_filtered[df_filtered['Location'] == loc].sort_values('timestamp')
            
            if not df_loc_time.empty:
                fig2, ax2 = plt.subplots(figsize=(12, 5))
                for d in sorted(df_loc_time['Depth'].unique()):
                    subset_depth = df_loc_time[df_loc_time['Depth'] == d]
                    # marker='.' ensures we see a dot for every hour recorded
                    ax2.plot(subset_depth['timestamp'], subset_depth['temperature'], 
                             label=f"{d}ft", linewidth=1, marker='.', markersize=3, alpha=0.7)
                
                add_ref_lines(ax2, is_vertical=False)
                ax2.set_ylabel("Temp (°F)")
                ax2.set_xlabel("Time")
                ax2.grid(True, which='both', linestyle=':', alpha=0.4)
                ax2.legend(loc='upper left', bbox_to_anchor=(1, 1), fontsize='x-small')
                plt.xticks(rotation=45)
                st.pyplot(fig2)
            else:
                st.info(f"No hourly data found for {loc}.")
