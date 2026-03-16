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

# Sidebar: Controls
st.sidebar.subheader("Reference Marks")
show_freezing = st.sidebar.checkbox("Show Freezing Line (32°F)", value=True)
custom_marks_input = st.sidebar.text_input("Custom Reference Temps", "25, 40")
num_weeks = st.sidebar.slider("Weeks of History", 1, 24, 8)

cutoff_date = pd.Timestamp.now(tz='UTC') - pd.Timedelta(weeks=num_weeks)
df_filtered = df_proj[df_proj['timestamp'] >= cutoff_date].copy()

# 3. Helpers
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

# 4. Tabs
tab_summary, tab_depth, tab_time = st.tabs(["📊 24-Hour Insights", "📏 Temperature vs Depth", "📈 Temperature vs Time"])

# --- TAB: 24-HOUR INSIGHTS ---
with tab_summary:
    st.subheader("Last 24 Hours: Summary Stats")
    last_24 = df_proj[df_proj['timestamp'] >= (pd.Timestamp.now(tz='UTC') - pd.Timedelta(hours=24))]
    
    if not last_24.empty:
        # Calculate Delta, Min, Max per Pipe (Location) and Depth
        stats = last_24.groupby(['location', 'depth'])['value'].agg(['min', 'max', lambda x: x.max() - x.min()]).reset_index()
        stats.columns = ['Pipe', 'Node/Depth', 'Min Temp', 'Max Temp', '24h Change']
        
        # Highlight Greatest Change
        max_change_row = stats.loc[stats['24h Change'].idxmax()]
        st.metric(label=f"🔥 Greatest Change: {max_change_row['Pipe']} (Node {max_change_row['Node/Depth']})", 
                  value=f"{max_change_row['24h Change']:.2f}°F")
        
        st.dataframe(stats.sort_values('24h Change', ascending=False), use_container_width=True)
    else:
        st.info("No data found in the last 24 hours.")

# --- TAB: TEMPERATURE VS DEPTH ---
with tab_depth:
    st.subheader("Temperature vs Depth (Mondays @ 6 AM)")
    # Filter out "Bank" pipes as they don't have numeric depths for vertical profiles
    depth_locations = [loc for loc in df_filtered['location'].unique() if "bank" not in loc.lower()]
    
    for loc in depth_locations:
        with st.expander(f"Location: {loc}", expanded=True):
            df_loc = df_filtered[df_filtered['location'] == loc].copy()
            df_loc['timestamp_round'] = df_loc['timestamp'].dt.round('1h')
            df_monday = df_loc[(df_loc['timestamp_round'].dt.weekday == 0) & (df_loc['timestamp_round'].dt.hour == 6)].copy()
            
            if not df_monday.empty:
                fig1, ax1 = plt.subplots(figsize=(8, 6))
                for ts, group in df_monday.groupby('timestamp_round'):
                    snapshot = group.sort_values('depth')
                    ax1.plot(snapshot['value'], snapshot['depth'], marker='o', label=ts.strftime('%Y-%m-%d'))
                
                ax1.invert_yaxis()
                add_ref_lines(ax1, is_vertical=True)
                ax1.set_title(f"Temperature vs Depth for {loc}") # PRINT TITLE
                ax1.set_xlabel("Temp (°F)")
                ax1.set_ylabel("Depth (ft)")
                ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='x-small')
                st.pyplot(fig1)

# --- TAB: TEMPERATURE VS TIME ---
with tab_time:
    st.subheader("Temperature vs Time")
    all_locations = sorted(df_filtered['location'].unique())
    
    for loc in all_locations:
        with st.expander(f"Location: {loc}", expanded=True):
            df_loc_time = df_filtered[df_filtered['location'] == loc].sort_values('timestamp')
            
            if not df_loc_time.empty:
                fig2, ax2 = plt.subplots(figsize=(12, 5))
                for d in sorted(df_loc_time['depth'].unique()):
                    subset = df_loc_time[df_loc_time['depth'] == d].copy()
                    subset = subset.drop_duplicates('timestamp').sort_values('timestamp')
                    
                    # 6hr gap break
                    diff = subset['timestamp'].diff() > pd.Timedelta(hours=6)
                    new_rows = [{'timestamp': subset.iloc[i-1]['timestamp'] + pd.Timedelta(seconds=1), 'value': np.nan} 
                                for i, has_gap in enumerate(diff) if has_gap]
                    if new_rows:
                        subset = pd.concat([subset, pd.DataFrame(new_rows)]).sort_values('timestamp')
                    
                    ax2.plot(subset['timestamp'], subset['value'], label=f"Node {d}", linewidth=1.2, marker='.', markersize=2, alpha=0.8)
                
                # Grid & Locators
                ax2.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0))
                ax2.xaxis.set_minor_locator(mdates.DayLocator())
                fmt = '%b %d' if num_weeks > 3 else '%a %m/%d'
                ax2.xaxis.set_major_formatter(mdates.DateFormatter(fmt))
                ax2.grid(which='major', color='#444444', linestyle='-', alpha=0.7)
                ax2.grid(which='minor', color='#CCCCCC', linestyle=':', alpha=0.4)
                
                ax2.set_title(f"Temperature vs Time for {loc}") # PRINT TITLE
                add_ref_lines(ax2, is_vertical=False)
                ax2.set_ylabel("Temp (°F)")
                ax2.legend(loc='upper left', bbox_to_anchor=(1, 1), fontsize='x-small')
                plt.setp(ax2.get_xticklabels(), rotation=45, ha='right')
                st.pyplot(fig2)
