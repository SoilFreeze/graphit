import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. Setup & Data Loading
st.set_page_config(page_title="Geotechnical Temp Dashboard", layout="wide")

# ... [Keep your existing BigQuery client/auth setup here] ...

# 2. Sidebar & Data Processing
st.sidebar.title("📁 Project Controls")
unit = st.sidebar.radio("Temperature Unit", ["Fahrenheit (°F)", "Celsius (°C)"])
is_celsius = unit == "Celsius (°C)"
u_symbol = "°C" if is_celsius else "°F"
alert_threshold = 1.0 if is_celsius else 1.8

# Reference Line Toggles
st.sidebar.subheader("Reference Lines")
show_red_ref = st.sidebar.checkbox("Show 10.2 Line (Red)", value=True)
show_blue_ref = st.sidebar.checkbox("Show 26.6 Line (Blue)", value=True)
show_freezing_ref = st.sidebar.checkbox("Show 32.0 Line (Blue)", value=True)

# ... [Keep your get_full_dataset function and df_proj filtering here] ...

# 3. Define Tabs FIRST to avoid NameError
tab_summary, tab_depth, tab_time = st.tabs(["📊 24-Hour Insights", "📏 Temp vs Depth", "📈 Temp vs Time"])

# 4. Helpers (must be defined before being used in tabs)
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
                
                row = {
                    "Pipe": loc,
                    "Min Temp": f"{p_min:.1f}{u_symbol}",
                    "Max Temp": f"{p_max:.1f}{u_symbol}",
                    "Max Change at": top_node_row['depth'],
                    "Raw Delta": top_node_row['delta'], 
                    "24h Change": f"{top_node_row['delta']:.1f}{u_symbol}"
                }
                
                if "bank" in loc.lower():
                    bank_rows.append(row)
                else:
                    row["Max Change at"] = f"{float(row['Max Change at']):.1f}ft"
                    pipe_rows.append(row)

            def style_alert(row):
                # Use the 'Raw Delta' column for logic, then it will be hidden
                color = 'red' if row['Raw Delta'] >= alert_threshold else None
                return [f'color: {color}' if color else '' for _ in row]

            st.subheader("Standard Pipes: 24h Activity")
            if pipe_rows:
                df_p = pd.DataFrame(pipe_rows)
                st.table(df_p.style.apply(style_alert, axis=1).hide(axis='columns', subset=['Raw Delta']))
            
            st.subheader("Bank Temperatures: 24h Activity")
            if bank_rows:
                df_b = pd.DataFrame(bank_rows)
                st.table(df_b.style.apply(style_alert, axis=1).hide(axis='columns', subset=['Raw Delta']))
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

# ... [Continue with tab_depth and tab_time] ...
