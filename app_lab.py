import streamlit as st
import pandas as pd
import time
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz
import requests
import json
import traceback
import re
import io

#########################
# --- CONFIGURATION --- #
#########################
st.set_page_config(page_title="SoilFreeze Data Lab", layout="wide")

DATASET_ID = "Temperature" 
PROJECT_ID = "sensorpush-export"
MASTER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.master_data"
METADATA_TABLE = "metadata"

@st.cache_resource
def get_bq_client():
    """Handles authentication with BigQuery and Drive scopes."""
    try:
        SCOPES = [
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/drive"
        ]
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
            return bigquery.Client(credentials=credentials, project=info["project_id"])
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"Authentication Failed: {e}")
        return None

client = get_bq_client()

########################
# --- GRAPH ENGINE --- #
########################
def build_standard_sf_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label):
    """
    The 'Pulse Monitor': Historical Trend Graph (Time-Series Analysis).
    Shows how every individual sensor is behaving over time.
    """
    try:
        display_df = df.copy()
        if display_df.empty:
            return go.Figure()

        display_df.columns = [c.lower() for c in display_df.columns]
        display_df['timestamp'] = pd.to_datetime(display_df['timestamp'])
        
        if display_df['timestamp'].dt.tz is None:
            display_df['timestamp'] = display_df['timestamp'].dt.tz_localize(pytz.UTC)
        else:
            display_df['timestamp'] = display_df['timestamp'].dt.tz_convert(pytz.UTC)

        # 1. UNIT CONVERSION
        if unit_mode == "Celsius":
            display_df['temperature'] = (display_df['temperature'] - 32) * 5/9
            y_range = [-30, 30]
        else:
            y_range = [-20, 80]
            
        # 2. SMART LABELING
        def create_label(row):
            b_val = str(row.get('bank', '')).strip().lower()
            d_val = str(row.get('depth', '')).strip().lower()
            s_name = str(row.get('nodenum', row.get('sensor_name', 'Unknown')))
            if b_val not in ["", "none", "nan", "null"]:
                return f"Bank {row['bank']} ({s_name})"
            if d_val not in ["", "none", "nan", "null"]:
                return f"{row['depth']}ft ({s_name})"
            return f"Unmapped ({s_name})"

        display_df['label'] = display_df.apply(create_label, axis=1)
        
        # 3. GAP HANDLING
        processed_dfs = []
        for lbl in sorted(display_df['label'].unique()):
            s_df = display_df[display_df['label'] == lbl].copy().sort_values('timestamp')
            s_df['gap_hrs'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
            gap_mask = s_df['gap_hrs'] > 6.0
            if gap_mask.any():
                gaps = s_df[gap_mask].copy()
                gaps['temperature'] = None
                gaps['timestamp'] = gaps['timestamp'] - pd.Timedelta(seconds=1)
                s_df = pd.concat([s_df, gaps]).sort_values('timestamp')
            processed_dfs.append(s_df)
            
        clean_df = pd.concat(processed_dfs) if processed_dfs else display_df
        
        # 4. FIGURE SETUP
        fig = go.Figure()
        for lbl in sorted(clean_df['label'].unique()):
            sensor_df = clean_df[clean_df['label'] == lbl]
            fig.add_trace(go.Scatter(
                x=sensor_df['timestamp'], y=sensor_df['temperature'], 
                name=lbl, mode='lines', connectgaps=False, line=dict(width=2)
            ))

        # 5. STYLING & GRIDLINES
        fig.update_layout(
            title={'text': title, 'x': 0, 'xanchor': 'left', 'font': dict(size=18)},
            plot_bgcolor='white', hovermode="x unified", height=600,
            margin=dict(t=80, l=50, r=180, b=50),
            legend=dict(title="Nodes (Sensors)", orientation="v", yanchor="top", y=1, xanchor="left", x=1.02)
        )
        
        # Vertical gridlines (6h)
        grid_times = pd.date_range(start=start_view, end=end_view, freq='6h')
        for ts in grid_times:
            color, width = ("DimGray", 1.5) if ts.hour == 0 else ("GhostWhite", 0.5)
            fig.add_vline(x=ts, line_width=width, line_color=color, layer='below')

        # "NOW" MARKER
        now_marker = pd.Timestamp.now(tz=pytz.UTC)
        fig.add_vline(x=now_marker, line_width=2, line_color="Red", layer='above', line_dash="dot")

        fig.update_yaxes(title=f"Temp ({unit_label})", range=y_range, gridcolor='Gainsboro', mirror=True, showline=True, linecolor='black')
        fig.update_xaxes(range=[start_view, end_view], mirror=True, showline=True, linecolor='black')

        # GOAL LINE (32°F / 0°C)
        for val, label in active_refs:
            c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
            thickness = 3 if val == 32.0 else 1.5
            fig.add_hline(y=c_val, line_dash="dash", line_color="RoyalBlue", opacity=0.8, line_width=thickness, 
                          annotation_text=f"{label} Goal", annotation_position="top right")
        
        return fig
    except Exception as e:
        st.error(f"Pulse Monitor Error: {e}")
        return go.Figure()

def build_vertical_profile_graph(df, title, unit_mode, unit_label):
    """
    The 'Freeze Wall': Soil Temperature Profile (Vertical Analysis).
    Shows Weekly Snapshots on an inverted Y-axis.
    """
    try:
        v_df = df.copy()
        v_df['depth_num'] = pd.to_numeric(v_df['depth'], errors='coerce')
        v_df = v_df.dropna(subset=['depth_num', 'temperature'])
        
        # Unit Conversion
        if unit_mode == "Celsius":
            v_df['temperature'] = (v_df['temperature'] - 32) * 5/9
            freeze_line = 0
        else:
            freeze_line = 32

        # Filter for Weekly Snapshots (Mondays at 06:00)
        v_df['timestamp'] = pd.to_datetime(v_df['timestamp'])
        # Find all Mondays at 6AM in the data
        v_df['is_snapshot'] = (v_df['timestamp'].dt.weekday == 0) & (v_df['timestamp'].dt.hour == 6)
        snapshots_df = v_df[v_df['is_snapshot']].copy()
        
        # Add the absolute latest reading as a "Current" snapshot
        latest_ts = v_df['timestamp'].max()
        latest_df = v_df[v_df['timestamp'] == latest_ts].copy()
        latest_df['snapshot_label'] = "CURRENT STATUS"
        
        snapshots_df['snapshot_label'] = snapshots_df['timestamp'].dt.strftime('%Y-%m-%d')
        plot_df = pd.concat([snapshots_df, latest_df]).sort_values(['snapshot_label', 'depth_num'])

        fig = go.Figure()

        # Plot each weekly line
        labels = sorted(plot_df['snapshot_label'].unique())
        for i, lbl in enumerate(labels):
            snap = plot_df[plot_df['snapshot_label'] == lbl]
            is_latest = (lbl == "CURRENT STATUS")
            fig.add_trace(go.Scatter(
                x=snap['temperature'], y=snap['depth_num'],
                name=lbl, mode='lines+markers',
                line=dict(width=4 if is_latest else 2, color='Red' if is_latest else None),
                marker=dict(size=8 if is_latest else 6)
            ))

        # Vertical Goal Line (32F)
        fig.add_vline(x=freeze_line, line_width=3, line_dash="dash", line_color="RoyalBlue")
        fig.add_annotation(x=freeze_line, y=0, text="FREEZE LINE", showarrow=False, font=dict(color="RoyalBlue", size=12))

        fig.update_layout(
            title=title, plot_bgcolor='white', height=700,
            xaxis_title=f"Temperature ({unit_label})",
            yaxis_title="Depth (ft) - Surface at Top",
            legend_title="Weekly Snapshots",
            hovermode="y unified"
        )
        
        # INVERT Y-AXIS: 0 at top
        fig.update_yaxes(autorange="reversed", gridcolor='Gainsboro', showline=True, linecolor='black', mirror=True)
        fig.update_xaxes(gridcolor='Gainsboro', showline=True, linecolor='black', mirror=True)

        return fig
    except Exception as e:
        st.error(f"Vertical Profile Error: {e}")
        return go.Figure()

#######################
# --- SIDEBAR UI --- #
#######################
st.sidebar.title("❄️ SoilFreeze Lab")

service = st.sidebar.selectbox("📂 Select Page", ["🏠 Executive Summary", "📊 Client Portal", "📉 Node Diagnostics", "📤 Data Intake Lab", "🛠️ Admin Tools"])
st.sidebar.divider()

unit_mode = st.sidebar.radio("Temperature Unit", ["Fahrenheit", "Celsius"], index=0)
unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"

def convert_val(f_val):
    if f_val is None: return None
    return (f_val - 32) * 5/9 if unit_mode == "Celsius" else f_val

st.sidebar.divider()

selected_project = None
if service in ["📊 Client Portal", "📉 Node Diagnostics", "🛠️ Admin Tools"]:
    try:
        proj_q = f"SELECT DISTINCT Project FROM `{MASTER_TABLE}` WHERE Project IS NOT NULL"
        proj_df = client.query(proj_q).to_dataframe()
        selected_project = st.sidebar.selectbox("🎯 Active Project", sorted(proj_df['Project'].dropna().unique()))
    except: st.sidebar.warning("No projects found.")

st.sidebar.divider()
st.sidebar.write("### 📏 Reference Lines")
active_refs = []
if st.sidebar.checkbox("Freezing (32°F / 0°C)", value=True): active_refs.append((32.0, "Freezing"))
if st.sidebar.checkbox("Type B (26.6°F / -3°C)", value=True): active_refs.append((26.6, "Type B"))
if st.sidebar.checkbox("Type A (10.2°F / -12.1°C)", value=True): active_refs.append((10.2, "Type A"))

####################
# --- SERVICES --- #
####################

if service == "🏠 Executive Summary":
    st.header(f"🏠 Executive Summary: {selected_project if selected_project else 'All Projects'}")
    
    st.write("### ↕️ Sorting & View Options")
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        sort_choice = st.selectbox("Sort By", ["None", "Hours Since Last Seen", "Delta Magnitude"])
    with c2:
        sort_order = st.radio("Order", ["Descending", "Ascending"], horizontal=True)
    
    summary_q = f"SELECT * FROM `{MASTER_TABLE}`"
    if selected_project: 
        summary_q += f" WHERE Project = '{selected_project}'"
    summary_q += " QUALIFY ROW_NUMBER() OVER(PARTITION BY NodeNum ORDER BY timestamp DESC) = 1"
    
    try:
        with st.spinner("Syncing Command Center..."):
            raw_data = client.query(summary_q).to_dataframe()
        
        if raw_data.empty:
            st.warning("📡 No sensors found.")
        else:
            summary_rows = []
            now = pd.Timestamp.now(tz=pytz.UTC)
            
            for _, row in raw_data.iterrows():
                node_id = row['NodeNum']
                ts = row['timestamp'].tz_localize(pytz.UTC) if row['timestamp'].tzinfo is None else row['timestamp']
                hrs_ago = int((now - ts).total_seconds() / 3600)
                
                metrics_q = f"""
                    SELECT 
                        MIN(temperature) as min_24, 
                        MAX(temperature) as max_24,
                        (SELECT temperature FROM `{MASTER_TABLE}` WHERE NodeNum = '{node_id}' ORDER BY timestamp DESC LIMIT 1) - 
                        (SELECT temperature FROM `{MASTER_TABLE}` WHERE NodeNum = '{node_id}' AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) ORDER BY timestamp ASC LIMIT 1) as delta_24
                    FROM `{MASTER_TABLE}` 
                    WHERE NodeNum = '{node_id}' 
                    AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
                """
                m_res = client.query(metrics_q).to_dataframe()
                min_val = m_res['min_24'].iloc[0] if not m_res.empty else None
                max_val = m_res['max_24'].iloc[0] if not m_res.empty else None
                raw_delta = m_res['delta_24'].iloc[0] if not m_res.empty else None

                if hrs_ago > 24:
                    status_icon, delta_text, delta_style = "🔴", "-", None
                else:
                    status_icon = "🟠" if hrs_ago > 12 else ("🟡" if hrs_ago > 6 else "🟢")
                    delta_text = f"{round(raw_delta, 1)}°F" if pd.notnull(raw_delta) else "0.0°F"
                    delta_style = raw_delta

                pos_display = f"Bank {row['Bank']}" if str(row['Bank']).strip().lower() not in ["","none","nan","null"] else f"{row['Depth']} ft"

                summary_rows.append({
                    "Project": row['Project'],
                    "Node": node_id,
                    "Pipe/Bank": row['Location'],
                    "Pos/Depth": pos_display,
                    "Min": f"{round(convert_val(min_val), 1)}{unit_label}" if pd.notnull(min_val) else "N/A",
                    "Max": f"{round(convert_val(max_val), 1)}{unit_label}" if pd.notnull(max_val) else "N/A",
                    "Delta_Val": delta_style, 
                    "Delta": delta_text,
                    "Hours_Ago": hrs_ago,
                    "Last Seen": f"{ts.strftime('%m/%d %H:%M')} ({hrs_ago}hr) {status_icon}"
                })

            summary_df = pd.DataFrame(summary_rows)

            asc = (sort_order == "Ascending")
            if sort_choice == "Hours Since Last Seen":
                summary_df = summary_df.sort_values(by="Hours_Ago", ascending=asc)
            elif sort_choice == "Delta Magnitude":
                summary_df['abs_d'] = summary_df['Delta_Val'].abs().fillna(-1)
                summary_df = summary_df.sort_values(by="abs_d", ascending=asc).drop(columns=['abs_d'])

            batch_size = 100
            total_pages = max((len(summary_df) // batch_size) + 1, 1)
            page = st.number_input("Page", 1, total_pages, 1)
            display_batch = summary_df.iloc[(page-1)*batch_size : page*batch_size]

            def style_delta(val):
                if val is None: return ""
                bg, color = "", "black"
                if val >= 5: bg, color = "#FF0000", "white"
                elif val >= 2: bg = "#FFA500"
                elif val >= 0.5: bg = "#FFFF00"
                elif -0.5 <= val <= 0.5: bg, color = "#008000", "white"
                elif -2 < val < -0.5: bg = "#ADD8E6"
                elif -5 < val <= -2: bg, color = "#4169E1", "white"
                elif val <= -5: bg, color = "#00008B", "white"
                return f'background-color: {bg}; color: {color}'

            st.subheader(f"📡 Engineering Command Center ({len(summary_df)} sensors)")
            st.dataframe(
                display_batch[["Project", "Node", "Pipe/Bank", "Pos/Depth", "Min", "Max", "Delta", "Last Seen"]].style.apply(
                    lambda x: [style_delta(rv) for rv in display_batch['Delta_Val']], axis=0, subset=['Delta']
                ),
                use_container_width=True, hide_index=True, height=600
            )
    except Exception as e: 
        st.error(f"Summary Error: {traceback.format_exc()}")

elif service == "📊 Client Portal":
    target_proj = selected_project 
    if not target_proj:
        st.warning("Please select a project in the sidebar.")
    else:
        st.header(f"📊 Project Status: {target_proj}")
        tab_time, tab_depth, tab_table = st.tabs(["📉 Historical Pulse (Time-Series)", "📏 Freeze Wall (Vertical Profile)", "📋 Project Data"])

        portal_q = f"""
            SELECT timestamp, temperature, Depth, Location, Bank, NodeNum
            FROM `{MASTER_TABLE}`
            WHERE CAST(Project AS STRING) = '{target_proj}' 
            AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 84 DAY)
            ORDER BY Location ASC, timestamp ASC
        """
        try:
            p_df = client.query(portal_q).to_dataframe()
            if p_df.empty:
                st.info(f"No data found for Project {target_proj} in the last 12 weeks.")
            else:
                p_df['timestamp'] = pd.to_datetime(p_df['timestamp'])
                if p_df['timestamp'].dt.tz is None: p_df['timestamp'] = p_df['timestamp'].dt.tz_localize(pytz.UTC)
                else: p_df['timestamp'] = p_df['timestamp'].dt.tz_convert(pytz.UTC)

                with tab_time:
                    st.subheader("The Pulse Monitor")
                    st.info("Tracking rate of change and individual node health over time.")
                    weeks_view = st.slider("Weeks to View", 1, 12, 6, key=f"wk_{target_proj}")
                    end_view = pd.Timestamp.now(tz=pytz.UTC)
                    start_view = end_view - timedelta(weeks=weeks_view)
                    
                    time_filtered_df = p_df[p_df['timestamp'] >= start_view]
                    locations = sorted(time_filtered_df['Location'].unique())
                    
                    t_page = st.number_input("Timeline Page", 1, max((len(locations)//10)+1, 1), 1, key=f"pg_{target_proj}")
                    for loc in locations[(t_page-1)*10 : t_page*10]:
                        with st.expander(f"📉 {loc} Timeline", expanded=True):
                            loc_data = time_filtered_df[time_filtered_df['Location'] == loc]
                            fig = build_standard_sf_graph(loc_data, f"{loc} - Pulse Monitor", start_view, end_view, active_refs, unit_mode, unit_label)
                            st.plotly_chart(fig, use_container_width=True)

                with tab_depth:
                    st.subheader("The Freeze Wall")
                    st.info("Cross-section of the earth. Lines moving LEFT show the cooling front pushing deeper.")
                    d_locs = sorted(p_df['Location'].unique())
                    d_page = st.number_input("Profile Page", 1, max((len(d_locs)//10)+1, 1), 1, key=f"dp_{target_proj}")
                    for loc in d_locs[(d_page-1)*10 : d_page*10]:
                        with st.expander(f"📏 {loc} Vertical Analysis", expanded=True):
                            loc_depth_data = p_df[p_df['Location'] == loc]
                            fig_v = build_vertical_profile_graph(loc_depth_data, f"{loc} - Weekly Cooling Front", unit_mode, unit_label)
                            st.plotly_chart(fig_v, use_container_width=True)

                with tab_table:
                    latest_q = f"SELECT Location, Depth, Bank, temperature, NodeNum FROM `{MASTER_TABLE}` WHERE CAST(Project AS STRING) = '{target_proj}' QUALIFY ROW_NUMBER() OVER(PARTITION BY NodeNum ORDER BY timestamp DESC) = 1 ORDER BY Location ASC"
                    l_df = client.query(latest_q).to_dataframe()
                    if not l_df.empty:
                        l_df['Pos'] = l_df.apply(lambda r: f"Bank {r['Bank']}" if str(r['Bank']).strip().lower() not in ["","none","nan","null"] else f"{r['Depth']} ft", axis=1)
                        l_df['Current Temp'] = l_df['temperature'].apply(lambda x: f"{round(convert_val(x), 1)}{unit_label}")
                        st.table(l_df[["Location", "Pos", "Current Temp"]])

        except Exception as e:
            st.error(f"Portal Error: {e}")

elif service == "📉 Node Diagnostics":
    st.header(f"📉 Diagnostics: {selected_project}")
    try:
        loc_q = f"SELECT DISTINCT Location FROM `{MASTER_TABLE}` WHERE Project = '{selected_project}'"
        loc_df = client.query(loc_q).to_dataframe()
        c1, c2 = st.columns([2, 1])
        with c1: 
            sel_loc = st.selectbox("Pipe / Bank", sorted(loc_df['Location'].dropna().unique()))
        with c2: 
            weeks = st.slider("Lookback (Weeks)", 1, 12, 6)

        now = pd.Timestamp.now(tz=pytz.UTC)
        start_view = now - pd.offsets.Week(int(weeks))
        end_view = now

        data_q = f"SELECT * FROM `{MASTER_TABLE}` WHERE Project = '{selected_project}' AND Location = '{sel_loc}' AND timestamp >= '{start_view.strftime('%Y-%m-%d %H:%M:%S')}' ORDER BY timestamp ASC"
        df_g = client.query(data_q).to_dataframe()
        
        if not df_g.empty:
            st.plotly_chart(build_standard_sf_graph(df_g, f"Diagnostic: {sel_loc}", start_view, end_view, active_refs, unit_mode, unit_label), use_container_width=True)
        else:
            st.warning("No data found.")
    except Exception as e:
        st.error(f"Diagnostics Error: {e}")

elif service == "📤 Data Intake Lab":
    st.header("📤 Data Ingestion & Recovery")
    tab1, tab2, tab3 = st.tabs(["📄 Manual File Upload", "📡 API Data Recovery", "🛠️ Maintenance"])

    with tab1:
        st.subheader("📄 Manual File Ingestion")
        u_file = st.file_uploader("Upload CSV", type=['csv'], key="manual_upload_unified_fixed")
        if u_file is not None:
            filename = u_file.name.lower()
            raw_content = u_file.getvalue().decode('utf-8').splitlines()
            is_lord_wide = any("DATA_START" in line for line in raw_content[:100])
            is_lord_narrow = "nodenumber" in raw_content[0].lower() and "temperature" in raw_content[0].lower()
            
            if is_lord_wide:
                try:
                    start_idx = next(i for i, line in enumerate(raw_content) if "DATA_START" in line)
                    df_wide = pd.read_csv(io.StringIO("\n".join(raw_content[start_idx+1:])))
                    df_long = df_wide.melt(id_vars=['Time'], var_name='NodeNum', value_name='temperature')
                    df_long['NodeNum'] = df_long['NodeNum'].str.replace(':', '-', regex=False)
                    df_long['timestamp'] = pd.to_datetime(df_long['Time'], format='mixed')
                    df_long = df_long.dropna(subset=['temperature'])
                    st.success(f"✅ Lord Wide Parsed: {len(df_long)} readings.")
                    if st.button("🚀 UPLOAD LORD WIDE DATA"):
                        client.load_table_from_dataframe(df_long[['timestamp', 'NodeNum', 'temperature']], f"{PROJECT_ID}.{DATASET_ID}.raw_lord").result()
                        st.success("Uploaded!")
                except Exception as e: st.error(f"Wide Error: {e}")
            elif is_lord_narrow:
                try:
                    df_ln = pd.read_csv(io.StringIO("\n".join(raw_content)))
                    df_ln = df_ln.rename(columns={'Timestamp': 'timestamp', 'nodenumber': 'NodeNum', 'temperature': 'temperature'})
                    df_ln['timestamp'] = pd.to_datetime(df_ln['timestamp'], format='mixed')
                    df_ln['NodeNum'] = df_ln['NodeNum'].str.replace(':', '-', regex=False)
                    st.success(f"✅ Lord Narrow Parsed: {len(df_ln)} readings.")
                    if st.button("🚀 UPLOAD LORD NARROW DATA"):
                        client.load_table_from_dataframe(df_ln[['timestamp', 'NodeNum', 'temperature']], f"{PROJECT_ID}.{DATASET_ID}.raw_lord").result()
                        st.success("Uploaded!")
                except Exception as e: st.error(f"Narrow Error: {e}")
            else:
                try:
                    header_idx = -1
                    for i, line in enumerate(raw_content[:50]):
                        if "SensorId" in line or "Observed" in line:
                            header_idx = i; break
                    if header_idx != -1:
                        df_sp = pd.read_csv(io.StringIO("\n".join(raw_content[header_idx:])), dtype=str)
                        ts_col = "Observed" if "Observed" in df_sp.columns else df_sp.columns[1]
                        df_up = pd.DataFrame()
                        df_up['sensor_id'] = df_sp['SensorId'].astype(str).str.strip()
                        df_up['timestamp'] = pd.to_datetime(df_sp[ts_col], format='mixed')
                        t_cols = [c for c in df_sp.columns if "Temperature" in c or "Thermocouple" in c]
                        df_up['temperature'] = pd.to_numeric(df_sp[t_cols].bfill(axis=1).iloc[:, 0], errors='coerce')
                        df_up = df_up.dropna(subset=['timestamp', 'temperature'])
                        st.success(f"✅ SensorPush Parsed: {len(df_up)} readings.")
                        if st.button("🚀 UPLOAD SENSORPUSH"):
                            client.load_table_from_dataframe(df_up, f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush").result()
                            st.success("Uploaded!")
                except Exception as e: st.error(f"SensorPush Error: {e}")

    with tab3:
        st.subheader("🛠️ Metadata Management")
        u_meta = st.file_uploader("Upload Master Metadata CSV", type=['csv'])
        if u_meta and st.button("Overwrite Master Metadata"):
            df_new_meta = pd.read_csv(u_meta)
            client.load_table_from_dataframe(df_new_meta, f"{PROJECT_ID}.{DATASET_ID}.master_metadata", 
                                             job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")).result()
            st.success("Metadata Updated!")

elif service == "🛠️ Admin Tools":
    st.header("🛠️ Engineering Admin Tools")
    RAW_SP = f"{PROJECT_ID}.Temperature.raw_sensorpush"
    RAW_LORD = f"{PROJECT_ID}.Temperature.raw_lord"
    tab_scrub, tab_approve = st.tabs(["🧹 Source Cleaning", "✅ Bulk Approval"])
    
    with tab_scrub:
        scrub_target = st.radio("Source", ["SensorPush", "Lord"], horizontal=True)
        target_table = RAW_SP if scrub_target == "SensorPush" else RAW_LORD
        id_col = "sensor_id" if scrub_target == "SensorPush" else "NodeNum"
        admin_query = f"SELECT {id_col} as Node, COUNT(*) as Points, MAX(timestamp) as Last_Seen FROM `{target_table}` GROUP BY Node"
        try:
            admin_df = client.query(admin_query).to_dataframe()
            st.dataframe(admin_df, use_container_width=True)
        except Exception as e: st.error(f"Admin Error: {e}")
