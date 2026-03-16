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

# 2. Sidebar
st.sidebar.title("📁 Project Controls")

@st.cache_data(ttl=300)
def get_full_dataset():
    query = "SELECT * FROM `sensorpush-export.sensor_data.final_dashboard_data` ORDER BY timestamp ASC"
    return client.query(query).to_dataframe()

df_raw = get_full_dataset()
df_raw.columns = [str(c).strip().lower() for c in df_raw.columns]
df_raw['timestamp'] = pd.to_datetime(df_raw['timestamp'])
df_raw['project'] = df_raw['project'].fillna('Unnamed').astype(str)

available_projects = sorted(df_raw['project'].unique())
selected_project = st.sidebar.selectbox("Choose Project", available_projects)
df_proj = df_raw[df_raw['project'] == selected_project].copy()

# Thresholds
st.sidebar.subheader("Reference Marks")
show_freezing = st.sidebar.checkbox("Show Freezing Line (32°F)", value=True)
custom_marks_input = st.sidebar.text_input("Custom Reference Temps (comma separated)", "25, 40")

num_weeks = st.sidebar.slider("Weeks of History", 1, 24, 8)
cutoff_date = pd.Timestamp.now(tz='UTC') - pd.Timedelta(weeks=num_weeks)
df_filtered = df_proj[df_proj['timestamp'] >= cutoff_date].copy()

st.title(f"Project: {selected_project}")
tab_depth, tab_time = st.tabs(["📊 Temperature vs Depth", "📈 Temperature vs Time"])

available_locations = sorted(df_filtered['location'].unique())

def add_ref_lines(ax, is_vertical=True):
    if show_freezing:
        if is_vertical: ax.axvline(x=32, color='blue', linestyle='--', linewidth=2, label='32°F Freezing')
        else: ax.axhline(y=32, color='blue', linestyle='--', linewidth=2, label='32°F Freezing')
    if custom_marks_input:
        try:
            marks = [float(x.strip()) for x in custom_marks_input.split(',') if x.strip()]
            for m in marks:
                if is_vertical: ax.axvline(x=m, color='green', linestyle=':', label=f'Ref: {m}°F')
                else: ax.axhline(y=m, color='green', linestyle=':')
        except: pass

# --- TAB 1: TEMPERATURE VS DEPTH ---
with tab_depth:
    st.subheader("Temperature vs Depth (Mondays at 6:00 AM)")
    for loc in available_locations:
        with st.expander(f"Location: {loc}", expanded=True):
            df_loc = df_filtered[df_filtered['location'] == loc].copy()
            df_loc['timestamp_round'] = df_loc['timestamp'].dt.round('1h')
            df_monday = df_loc[(df_loc['timestamp_round'].dt.weekday == 0) & (df_loc['timestamp_round'].dt.hour == 6)].copy()
            
            if not df_monday.empty:
                fig1, ax1 = plt.subplots(figsize=(8, 6))
                for ts, group in df_monday.groupby('timestamp_round'):
                    snapshot = group.sort_values('depth')
                    label_date = ts.strftime('%Y-%m-%d')
                    ax1.plot(snapshot['value'], snapshot['depth'], marker='o', label=label_date)
                
                ax1.invert_yaxis()
                add_ref_lines(ax1, is_vertical=True)
                ax1.set_xlabel("Temp (°F)")
                ax1.set_ylabel("Depth (ft)")
                ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='x-small')
                ax1.grid(True, alpha=0.2)
                st.pyplot(fig1)

# --- TAB 2: TEMPERATURE VS TIME ---
with tab_time:
    st.subheader("Temperature vs Time")
    for loc in available_locations:
        with st.expander(f"Trends: {loc}", expanded=True):
            df_loc_time = df_filtered[df_filtered['location'] == loc].sort_values('timestamp')
            
            if not df_loc_time.empty:
                fig2, ax2 = plt.subplots(figsize=(12, 5))
                for d in sorted(df_loc_time['depth'].unique()):
                    subset = df_loc_time[df_loc_time['depth'] == d].copy()
                    subset = subset.drop_duplicates('timestamp').sort_values('timestamp')
                    
                    # 6hr gap break logic
                    diff = subset['timestamp'].diff() > pd.Timedelta(hours=6)
                    new_rows = []
                    for i, has_gap in enumerate(diff):
                        if has_gap:
                            new_rows.append({'timestamp': subset.iloc[i-1]['timestamp'] + pd.Timedelta(seconds=1), 'value': np.nan})
                    if new_rows:
                        subset = pd.concat([subset, pd.DataFrame(new_rows)]).sort_values('timestamp')
                    
                    ax2.plot(subset['timestamp'], subset['value'], label=f"{d}ft", linewidth=1.2, marker='.', markersize=2, alpha=0.8)
                
                # --- FIXED LOCATOR LOGIC ---
                # Major Locator: Monday at Midnight (Note the list [0])
                ax2.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.MONDAY, byhour=[0]))
                # Minor Locator: Every Day at Midnight
                ax2.xaxis.set_minor_locator(mdates.DayLocator(interval=1))
                
                # Date Formatting based on time range
                if num_weeks > 3:
                    date_fmt = mdates.DateFormatter('%b %d')
                else:
                    date_fmt = mdates.DateFormatter('%a %m/%d')
                
                ax2.xaxis.set_major_formatter(date_fmt)

                # Grid Styling
                ax2.grid(which='major', color='#444444', linestyle='-', alpha=0.6, linewidth=1) 
                ax2.grid(which='minor', color='#999999', linestyle=':', alpha=0.4) 
                
                add_ref_lines(ax2, is_vertical=False)
                ax2.set_ylabel("Temp (°F)")
                ax2.legend(loc='upper left', bbox_to_anchor=(1, 1), fontsize='x-small')
                plt.setp(ax2.get_xticklabels(), rotation=45, ha='right')
                st.pyplot(fig2)
