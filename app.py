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

# Merge '54018-ch1' and '54018_ch1' and strip spaces
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
        if is_vertical: ax.axvline(x=r['val'], color=r['color'], linestyle='--', linewidth=1.5, label=r['label'])
        else: ax.axhline(y=r['val'], color=r['color'], linestyle='--', linewidth=1.5, label=r['label'])

# --- TAB 1: 24-HOUR INSIGHTS (WITH OFFLINE CHECK) ---
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
                
                row_disp = {
                    "Pipe": loc,
                    "Min Temp": f"{p_min:.1f}{u_symbol}",
                    "Max Temp": f"{p_max:.1f}{u_symbol}",
                    "Max Change at": f"{top_node['depth']}",
                    "24h Change": f"{top_node['delta']:.1f}{u_symbol}"
                }
                color = 'color: red' if top_node['delta'] >= alert_threshold else ''
                if "bank" in loc.lower(): b_rows.append((row_disp, color))
                else: p_rows.append((row_disp, color))

            def draw_table(rows, title):
                st.subheader(title)
                if rows:
                    df_disp = pd.DataFrame([r[0] for r in rows])
                    colors = [r[1] for r in rows]
                    st.table(df_disp.style.apply(lambda x: [colors[i] for i in range(len(x))], axis=0))

            draw_table(p_rows, "Standard Pipes: 24h Activity")
            draw_table(b_rows, "Bank Temperatures: 24h Activity")
        else:
            st.info("No sensor activity in the last 24 hours.")

    with col_offline:
        st.subheader("⚠️ Offline Sensors")
        # Find every sensor that has EVER been seen in this project
        all_sensors = df_proj[['location', 'depth']].drop_duplicates()
        # Find sensors seen in the last 24 hours
        active_sensors = last_24[['location', 'depth']].drop_duplicates()
        
        # Merge to find the difference
        offline = all_sensors.merge(active_sensors, on=['location', 'depth'], how='left', indicator=True)
        offline = offline[offline['_merge'] == 'left_only']
        
        if not offline.empty:
            st.warning(f"{len(offline)} nodes have not reported in 24h")
            st.dataframe(offline[['location', 'depth']].rename(columns={'location':'Pipe','depth':'Node'}), hide_index=True)
        else:
            st.success("All sensors are currently online.")

# --- TAB 2: TEMP VS DEPTH (RESTORED) ---
with tab_depth:
    st.subheader(f"Temperature vs Depth ({u_symbol})")
    depth_locs = [l for l in df_filtered['location'].unique() if "bank" not in l.lower()]
    for loc in depth_locs:
        with st.expander(f"Location: {loc}", expanded=True):
            df_loc = df_filtered[df_filtered['location'] == loc].copy()
            df_loc['depth_num'] = pd.to_numeric(df_loc['depth'], errors='coerce')
            df_loc = df_loc.dropna(subset=['depth_num'])
            df_loc['ts_round'] = df_loc['timestamp'].dt.round('1h')
            df_snap = df_loc[(df_loc['ts_round'].dt.weekday == 0) & (df_loc['ts_round'].dt.hour == 6)].copy()
            if not df_snap.empty:
                fig1, ax1 = plt.subplots(figsize=(8, 6))
                for ts, gp in df_snap.groupby('ts_round'):
                    snap = gp.sort_values('depth_num')
                    ax1.plot(snap['value'], snap['depth_num'], marker='o', label=ts.strftime('%Y-%m-%d'))
                ax1.invert_yaxis()
                add_ref_lines(ax1, is_vertical=True)
                ax1.set_xlabel(f"Temp ({u_symbol})")
                ax1.set_ylabel("Depth (ft)")
                ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='x-small')
                st.pyplot(fig1)

# --- TAB 3: TEMP VS TIME (Improved Visuals) ---
with tab_time:
    st.subheader(f"Temperature vs Time ({u_symbol})")
    
    # Calculate the strict start of the current week (Monday at 00:00:00)
    today = pd.Timestamp.now(tz='UTC').normalize()
    days_since_monday = today.weekday()
    current_monday = today - pd.Timedelta(days=days_since_monday)
    
    # Calculate the exact graph start based on the 'num_weeks' slider
    # This ensures the graph always starts on a Monday midnight
    graph_start = current_monday - pd.Timedelta(weeks=num_weeks-1)
    graph_end = pd.Timestamp.now(tz='UTC')

    for loc in sorted(df_filtered['location'].unique()):
        with st.expander(f"Location: {loc}", expanded=True):
            df_lt = df_filtered[df_filtered['location'] == loc].sort_values('timestamp')
            
            if not df_lt.empty:
                fig2, ax2 = plt.subplots(figsize=(12, 5))
                
                # Plot each node
                for d in sorted(df_lt['depth'].unique()):
                    sub = df_lt[df_lt['depth'] == d].copy()
                    ax2.plot(sub['timestamp'], sub['value'], label=f"Node {d}", alpha=0.9, linewidth=1.5)
                
                # 1. & 2. GRID LINES: Monday (Dark) vs Daily (Light)
                # Create a range of every single day in the view
                all_days = pd.date_range(start=graph_start, end=graph_end, freq='D')
                
                for day in all_days:
                    if day.weekday() == 0: # Monday
                        ax2.axvline(day, color='#333333', linewidth=1.2, alpha=0.8, zorder=1) # Dark line
                    else: # Other days
                        ax2.axvline(day, color='#CCCCCC', linewidth=0.6, alpha=0.5, zorder=1) # Light line
                
                # 3. NO BUFFERS: Force data from edge to edge
                ax2.set_xlim(graph_start, graph_end)
                ax2.margins(x=0) # Remove the internal padding
                
                # Formatting
                ax2.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
                add_ref_lines(ax2, is_vertical=False)
                ax2.set_ylabel(f"Temp ({u_symbol})")
                ax2.grid(True, axis='y', alpha=0.3, linestyle=':')
                ax2.legend(loc='upper left', bbox_to_anchor=(1, 1), fontsize='x-small')
                
                st.pyplot(fig2)
