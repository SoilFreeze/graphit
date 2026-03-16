import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. SETUP
st.set_page_config(page_title="Master Geotechnical Dashboard", layout="wide")

# 2. DATA LOADING (Must happen before UI logic)
scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform"]
creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
client = bigquery.Client(credentials=creds, project="sensorpush-export")

@st.cache_data(ttl=300)
def get_full_dataset():
    query = "SELECT * FROM `sensorpush-export.sensor_data.final_dashboard_data` ORDER BY timestamp ASC"
    return client.query(query).to_dataframe()

df_raw = get_full_dataset()
df_raw.columns = [str(c).strip().lower() for c in df_raw.columns]
df_raw['timestamp'] = pd.to_datetime(df_raw['timestamp'])

# 3. MASTER SIDEBAR CONTROLS
st.sidebar.title("📁 Master Controls")

unit = st.sidebar.radio("Temperature Unit", ["Fahrenheit (°F)", "Celsius (°C)"])
is_celsius = unit == "Celsius (°C)"
u_symbol = "°C" if is_celsius else "°F"
alert_threshold = 1.0 if is_celsius else 1.8

# Project Selection (Master Feature)
available_projects = sorted(df_raw['project'].dropna().unique())
selected_project = st.sidebar.selectbox("Choose Project", available_projects)

# Standardize values for chosen project
df_proj = df_raw[df_raw['project'] == selected_project].copy()
if is_celsius:
    df_proj['value'] = (df_proj['value'] - 32) * 5/9

st.sidebar.subheader("Reference Lines")
show_red_ref = st.sidebar.checkbox("Show 10.2 Line (Red)", value=True)
show_blue_ref = st.sidebar.checkbox("Show 26.6 Line (Blue)", value=True)
show_freezing_ref = st.sidebar.checkbox("Show 32.0 Line (Blue)", value=True)

num_weeks = st.sidebar.slider("Weeks of History", 1, 24, 8)
cutoff_date = pd.Timestamp.now(tz='UTC') - pd.Timedelta(weeks=num_weeks)
df_filtered = df_proj[df_proj['timestamp'] >= cutoff_date].copy()

# 4. DEFINE TABS (Avoids NameError)
tab_summary, tab_depth, tab_time = st.tabs(["📊 24-Hour Insights", "📏 Temp vs Depth", "📈 Temp vs Time"])

# 5. HELPERS
def add_ref_lines(ax, is_vertical=True):
    refs = []
    if show_red_ref:
        v = round((10.2 - 32) * 5/9, 1) if is_celsius else 10.2
        refs.append({'val': v, 'color': 'red', 'label': f"{v}{u_symbol}"})
    if show_blue_ref:
        v = round((26.6 - 32) * 5/9, 1) if is_celsius else 26.6
        refs.append({'val': v, 'color': 'blue', 'label': f"{v}{u_symbol}"})
    if show_freezing_ref:
        v = 0.0 if is_celsius else 32.0
        refs.append({'val': v, 'color': 'blue', 'label': f"{v}{u_symbol}"})
    for r in refs:
        if is_vertical: ax.axvline(x=r['val'], color=r['color'], linestyle='--', linewidth=1.5, label=r['label'])
        else: ax.axhline(y=r['val'], color=r['color'], linestyle='--', linewidth=1.5, label=r['label'])

# --- TAB 1: 24-HOUR INSIGHTS ---
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
                
                # We do NOT include the raw delta in this dictionary
                row_disp = {
                    "Pipe": loc,
                    "Min Temp": f"{p_min:.1f}{u_symbol}",
                    "Max Temp": f"{p_max:.1f}{u_symbol}",
                    "Max Change at": f"{float(top_node['depth']):.1f}ft" if "bank" not in loc.lower() else top_node['depth'],
                    "24h Change": f"{top_node['delta']:.1f}{u_symbol}"
                }
                color = 'color: red' if top_node['delta'] >= alert_threshold else ''
                if "bank" in loc.lower(): b_rows.append((row_disp, color))
                else: p_rows.append((row_disp, color))

            def draw_table(rows, title):
                st.subheader(title)
                if rows:
                    df = pd.DataFrame([r[0] for r in rows])
                    colors = [r[1] for r in rows]
                    # This styling applies to the display DF, which has no hidden columns
                    styler = df.style.apply(lambda x: [colors[i] for i in range(len(x))], axis=0)
                    st.table(styler)

            draw_table(p_rows, "Standard Pipes: 24h Activity")
            draw_table(b_rows, "Bank Temperatures: 24h Activity")
        else:
            st.info("No sensor activity in the last 24 hours.")

    with col2:
        st.subheader("⚠️ Offline Sensors")
        all_s = df_proj[['location', 'depth']].drop_duplicates()
        act_s = last_24[['location', 'depth']].drop_duplicates()
        offline = all_s.merge(act_s, on=['location', 'depth'], how='left', indicator=True)
        offline = offline[offline['_merge'] == 'left_only']
        if not offline.empty:
            st.warning(f"{len(offline)} nodes offline")
            st.dataframe(offline[['location', 'depth']].rename(columns={'location':'Pipe','depth':'Node'}), hide_index=True)
        else:
            st.success("All sensors online.")

# --- TAB 2: TEMP VS DEPTH ---
with tab_depth:
    st.subheader(f"Temperature vs Depth ({u_symbol})")
    depth_locs = [l for l in df_filtered['location'].unique() if "bank" not in l.lower()]
    for loc in depth_locs:
        with st.expander(f"Location: {loc}", expanded=True):
            df_loc = df_filtered[df_filtered['location'] == loc].copy()
            df_loc['ts_round'] = df_loc['timestamp'].dt.round('1h')
            df_snap = df_loc[(df_loc['ts_round'].dt.weekday == 0) & (df_loc['ts_round'].dt.hour == 6)].copy()
            if not df_snap.empty:
                fig1, ax1 = plt.subplots(figsize=(8, 6))
                for ts, gp in df_snap.groupby('ts_round'):
                    snap = gp.sort_values('depth')
                    ax1.plot(snap['value'], snap['depth'], marker='o', label=ts.strftime('%Y-%m-%d'))
                ax1.invert_yaxis()
                add_ref_lines(ax1, is_vertical=True)
                ax1.set_xlabel(f"Temp ({u_symbol})"); ax1.set_ylabel("Depth (ft)")
                ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='x-small')
                st.pyplot(fig1)

# --- TAB 3: TEMP VS TIME ---
with tab_time:
    st.subheader(f"Temperature vs Time ({u_symbol})")
    for loc in sorted(df_filtered['location'].unique()):
        with st.expander(f"Location: {loc}", expanded=True):
            df_lt = df_filtered[df_filtered['location'] == loc].sort_values('timestamp')
            if not df_lt.empty:
                fig2, ax2 = plt.subplots(figsize=(12, 5))
                for d in sorted(df_lt['depth'].unique()):
                    sub = df_lt[df_lt['depth'] == d].copy()
                    diff = sub['timestamp'].diff() > pd.Timedelta(hours=6)
                    new_rows = [{'timestamp': sub.iloc[i-1]['timestamp'] + pd.Timedelta(seconds=1), 'value': np.nan} for i, has_gap in enumerate(diff) if has_gap]
                    if new_rows: sub = pd.concat([sub, pd.DataFrame(new_rows)]).sort_values('timestamp')
                    lbl = f"Node {d}" if "bank" in loc.lower() else f"{float(d):.1f}ft"
                    ax2.plot(sub['timestamp'], sub['value'], label=lbl, alpha=0.8)
                ax2.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
                add_ref_lines(ax2, is_vertical=False)
                ax2.set_ylabel(f"Temp ({u_symbol})")
                ax2.legend(loc='upper left', bbox_to_anchor=(1, 1), fontsize='x-small')
                st.pyplot(fig2)
