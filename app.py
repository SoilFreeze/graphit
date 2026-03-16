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
u_label = "°C" if is_celsius else "°F"
alert_threshold = 1.0 if is_celsius else 1.8

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

# Sidebar: Controls
num_weeks = st.sidebar.slider("Weeks of History", 1, 24, 8)
cutoff_date = pd.Timestamp.now(tz='UTC') - pd.Timedelta(weeks=num_weeks)
df_filtered = df_proj[df_proj['timestamp'] >= cutoff_date].copy()

# 3. Helpers
def add_ref_lines(ax, is_vertical=True):
    # Reference Line Values (Fahrenheit)
    refs = [
        {'val': 10.2, 'color': 'red', 'label': '10.2'},
        {'val': 26.6, 'color': 'blue', 'label': '26.6'}
    ]
    
    for r in refs:
        v = r['val']
        if is_celsius:
            v = (v - 32) * 5/9
        
        if is_vertical:
            ax.axvline(x=v, color=r['color'], linestyle='--', linewidth=1.5, label=f"{r['label']}{u_label}")
        else:
            ax.axhline(y=v, color=r['color'], linestyle='--', linewidth=1.5)

# 4. Tabs
tab_summary, tab_depth, tab_time = st.tabs(["📊 24-Hour Insights", "📏 Temp vs Depth", "📈 Temp vs Time"])

# --- TAB: 24-HOUR INSIGHTS ---
with tab_summary:
    col1, col2 = st.columns([2, 1])
    now = pd.Timestamp.now(tz='UTC')
    last_24 = df_proj[df_proj['timestamp'] >= (now - pd.Timedelta(hours=24))].copy()
    
    with col1:
        st.subheader(f"All Pipes: 24h Thermal Activity")
        if not last_24.empty:
            node_stats = last_24.groupby(['location', 'depth'])['value'].agg(['min', 'max']).reset_index()
            node_stats['delta'] = node_stats['max'] - node_stats['min']
            
            summary_rows = []
            for loc in sorted(last_24['location'].unique()):
                pipe_data = node_stats[node_stats['location'] == loc]
                p_min, p_max = pipe_data['min'].min(), pipe_data['max'].max()
                top_node_row = pipe_data.loc[pipe_data['delta'].idxmax()]
                
                summary_rows.append({
                    "Pipe": loc,
                    f"Min ({u_label})": round(p_min, 1),
                    f"Max ({u_label})": round(p_max, 1),
                    "Max Delta Node": top_node_row['depth'],
                    "24h Change": round(top_node_row['delta'], 1)
                })
            
            res_df = pd.DataFrame(summary_rows)
            def highlight_delta(row):
                color = 'red' if row['24h Change'] >= alert_threshold else None
                return [f'color: {color}' if color else '' for _ in row]

            st.table(res_df.style.apply(highlight_delta, axis=1).format({
                f"Min ({u_label})": "{:.1f}", f"Max ({u_label})": "{:.1f}", "24h Change": "{:.1f}"
            }))
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

# --- TAB: TEMPERATURE VS DEPTH ---
with tab_depth:
    st.subheader(f"Temperature vs Depth ({u_label})")
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
                ax1.set_title(f"Temperature vs Depth for {loc}")
                ax1.set_xlabel(f"Temp ({u_label})")
                ax1.set_ylabel("Depth (ft)")
                ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='x-small')
                st.pyplot(fig1)

# --- TAB: TEMPERATURE VS TIME ---
with tab_time:
    st.subheader(f"Temperature vs Time ({u_label})")
    all_locations = sorted(df_filtered['location'].unique())
    for loc in all_locations:
        with st.expander(f"Location: {loc}", expanded=True):
            df_loc_time = df_filtered[df_filtered['location'] == loc].sort_values('timestamp')
            if not df_loc_time.empty:
                fig2, ax2 = plt.subplots(figsize=(12, 5))
                for d in sorted(df_loc_time['depth'].unique()):
                    subset = df_loc_time[df_loc_time['depth'] == d].copy()
                    subset = subset.drop_duplicates('timestamp').sort_values('timestamp')
                    diff = subset['timestamp'].diff() > pd.Timedelta(hours=6)
                    new_rows = [{'timestamp': subset.iloc[i-1]['timestamp'] + pd.Timedelta(seconds=1), 'value': np.nan} for i, has_gap in enumerate(diff) if has_gap]
                    if new_rows: subset = pd.concat([subset, pd.DataFrame(new_rows)]).sort_values('timestamp')
                    label_name = f"Node {d}" if "bank" in loc.lower() else f"{round(float(d), 1) if str(d).replace('.','').isdigit() else d}ft"
                    ax2.plot(subset['timestamp'], subset['value'], label=label_name, linewidth=1.5, marker='.', markersize=3, alpha=0.8)
                
                ax2.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0))
                ax2.xaxis.set_minor_locator(mdates.DayLocator())
                ax2.xaxis.set_major_formatter(mdates.DateFormatter('%b %d' if num_weeks > 3 else '%a %m/%d'))
                ax2.grid(which='major', color='#444444', linestyle='-', alpha=0.7)
                ax2.grid(which='minor', color='#CCCCCC', linestyle=':', alpha=0.4)
                ax2.set_title(f"Temperature vs Time for {loc}")
                add_ref_lines(ax2, is_vertical=False)
                ax2.set_ylabel(f"Temp ({u_label})")
                ax2.legend(loc='upper left', bbox_to_anchor=(1, 1), fontsize='x-small')
                plt.setp(ax2.get_xticklabels(), rotation=45, ha='right')
                st.pyplot(fig2)
