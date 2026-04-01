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
# --- 1. CONFIGURATION & STYLING ---
st.set_page_config(page_title="SoilFreeze Data Lab", layout="wide")

# UPDATED: Pointing to the new 'Temperature' dataset
DATASET_ID = "Temperature" 
PROJECT_ID = "sensorpush-export"
# The full table name is now sensorpush-export.Temperature.master_data
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

#########################
# --- REBUILD TABLE --- #
#########################
def rebuild_master_table(mode="preserve"):
    """
    Failsafe Rebuild: Strips all non-numeric characters to ensure 
    a match between CSV IDs and Google Sheet IDs.
    """
    table_id = f"{PROJECT_ID}.{DATASET_ID}.final_databoard_master"
    
    # Check if table exists to handle the 'ex' alias error
    exists = True
    try:
        client.get_table(table_id)
    except Exception:
        exists = False

    status_logic = "TRUE" if mode == "approve_all" else ("COALESCE(ex.is_approved, FALSE)" if exists else "FALSE")
    join_clause = f"LEFT JOIN `{table_id}` ex ON h.ts = ex.timestamp AND m.NodeNum = ex.sensor_id" if (exists and mode == "preserve") else ""

    scrub_sql = f"""
        CREATE OR REPLACE TABLE `{table_id}` AS 
        WITH RawUnified AS (
            SELECT CAST(timestamp AS TIMESTAMP) as ts, temperature as temp, 
                   -- Clean the ID: Remove colons, spaces, and non-digits
                   REGEXP_REPLACE(CAST(sensor_id AS STRING), r'[^0-9]', '') as clean_node 
            FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` WHERE temperature IS NOT NULL
            UNION ALL
            SELECT CAST(timestamp AS TIMESTAMP) as ts, value as temp, 
                   REGEXP_REPLACE(REPLACE(nodenumber, ':', '-'), r'[^0-9]', '') as clean_node 
            FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord` WHERE value IS NOT NULL
        ),
        HourlyDedupped AS (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY clean_node, TIMESTAMP_TRUNC(ts, HOUR) ORDER BY ts DESC) as rank 
            FROM RawUnified
        )
        SELECT 
            h.ts as timestamp, 
            h.temp as temperature, 
            m.NodeNum as sensor_id,
            m.NodeNum as sensor_name,
            m.Project as project, 
            m.Location as location, 
            m.Depth as depth, 
            {status_logic} as is_approved
        FROM HourlyDedupped h 
        INNER JOIN `{PROJECT_ID}.{DATASET_ID}.master_metadata` m 
            -- Match by stripping the Google Sheet PhysicalID of all non-digits too
            ON SUBSTR(h.clean_node, 1, 12) = SUBSTR(REGEXP_REPLACE(CAST(m.PhysicalID AS STRING), r'[^0-9]', ''), 1, 12)
        {join_clause}
        WHERE h.rank = 1
    """
    try:
        client.query(scrub_sql).result()
        return True
    except Exception as e:
        st.error(f"Rebuild Error: {e}")
        return False

############################
# --- FETCH SENSORPUSH --- #
############################
def build_standard_sf_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label):
    """
    Updated Time vs Temp Graph: 
    - Y-axis: Medium gray at 20, light gray at 5. Range 80 to -20.
    - X-axis: Gray at Monday midnight, medium gray at midnight, light gray at 6h.
    - Reference: Burgundy dash for Type A, RoyalBlue for others.
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

        # 1. UNIT CONVERSION & RANGE
        if unit_mode == "Celsius":
            display_df['temperature'] = (display_df['temperature'] - 32) * 5/9
            y_range = [( -20 - 32) * 5/9, (80 - 32) * 5/9]
            dt_major, dt_minor = 10, 2 # Rough equivalents for C
        else:
            y_range = [-20, 80]
            dt_major, dt_minor = 20, 5

        # 2. SMART LABELING
        def create_label(row):
            b_val = str(row.get('bank', '')).strip().lower()
            d_val = str(row.get('depth', '')).strip().lower()
            s_name = str(row.get('nodenum', row.get('sensor_name', 'Unknown')))
            # If in Client Portal (simplified title logic can be added here)
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
            title={'text': f"{title}: Time vs Temperature", 'x': 0, 'xanchor': 'left', 'font': dict(size=18)},
            plot_bgcolor='white', hovermode="x unified", height=600,
            margin=dict(t=80, l=50, r=180, b=50),
            legend=dict(title="Sensors", orientation="v", yanchor="top", y=1, xanchor="left", x=1.02)
        )
        
        # X-AXIS CUSTOM GRIDLINES
        # Generate range covering full weeks (Monday to Monday)
        grid_6h = pd.date_range(start=start_view, end=end_view, freq='6h')
        for ts in grid_6h:
            if ts.weekday() == 0 and ts.hour == 0: # Monday Midnight
                color, width = "Gray", 2
            elif ts.hour == 0: # Daily Midnight
                color, width = "DimGray", 1
            else: # 6-hour blocks
                color, width = "LightGray", 0.5
            fig.add_vline(x=ts, line_width=width, line_color=color, layer='below')

        # "NOW" MARKER (Red Dashed)
        now_marker = pd.Timestamp.now(tz=pytz.UTC)
        fig.add_vline(x=now_marker, line_width=2, line_color="Red", layer='above', line_dash="dash")

        # Y-AXIS GRID (20 Major, 5 Minor)
        fig.update_yaxes(
            title=f"Temp ({unit_label})", range=y_range,
            gridcolor='LightGray', gridwidth=0.5, # Minor
            dtick=dt_minor,
            mirror=True, showline=True, linecolor='black'
        )
        # Overlay Major Gridlines (Every 20)
        for y_val in range(y_range[0], y_range[1] + 1, dt_major):
            fig.add_hline(y=y_val, line_width=1, line_color="Gray", layer='below')

        fig.update_xaxes(range=[start_view, end_view], mirror=True, showline=True, linecolor='black')

        # REFERENCE LINES
        for val, label in active_refs:
            c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
            line_color = "maroon" if label == "Type A" else "RoyalBlue"
            fig.add_hline(y=c_val, line_dash="dash", line_color=line_color, opacity=0.8, 
                         annotation_text=label, annotation_position="top right")
        
        return fig
    except Exception as e:
        st.error(f"Critical Graph Error: {e}")
        return go.Figure()

########################
# --- GRAPH ENGINE --- #
########################
def build_standard_sf_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label):
    """
    Standard Graph: Temp (y) vs Time (x)
    - X-Axis: Dark line Monday midnight, medium midnight, light 6-hour.
    - Y-Axis: Major lines at 20, minor at 5. Range 80 to -20.
    - Legend: 'Bank' with location and nodenum in parentheses.
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

        # 1. UNIT CONVERSION & RANGE
        if unit_mode == "Celsius":
            display_df['temperature'] = (display_df['temperature'] - 32) * 5/9
            y_range = [( -20 - 32) * 5/9, (80 - 32) * 5/9]
            dt_major, dt_minor = 10, 2 
        else:
            y_range = [-20, 80]
            dt_major, dt_minor = 20, 5

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
            title={'text': f"{title} Time vs Temperature", 'x': 0, 'xanchor': 'left', 'font': dict(size=18)},
            plot_bgcolor='white', hovermode="x unified", height=600,
            margin=dict(t=80, l=50, r=180, b=50),
            legend=dict(title="Sensors", orientation="v", yanchor="top", y=1, xanchor="left", x=1.02)
        )
        
        # X-AXIS VERTICAL GRIDLINES (HIERARCHY)
        grid_6h = pd.date_range(start=start_view, end=end_view, freq='6h')
        for ts in grid_6h:
            if ts.weekday() == 0 and ts.hour == 0:
                # Monday Midnight - Darkest/Thickest
                color, width = "Black", 2
            elif ts.hour == 0:
                # Other Midnights - Medium
                color, width = "Gray", 1
            else:
                # 6-Hour Intervals - Lightest
                color, width = "LightGray", 0.5
            fig.add_vline(x=ts, line_width=width, line_color=color, layer='below')

        # "NOW" MARKER (Red Dashed)
        now_marker = pd.Timestamp.now(tz=pytz.UTC)
        fig.add_vline(x=now_marker, line_width=2, line_color="Red", layer='above', line_dash="dash")

        # Y-AXIS GRID (20 Major, 5 Minor)
        fig.update_yaxes(
            title=f"Temp ({unit_label})", range=y_range,
            gridcolor='Gainsboro', gridwidth=0.5, # Minor grid color
            dtick=dt_minor,
            mirror=True, showline=True, linecolor='black'
        )
        # Major Y-Gridlines (Every 20)
        for y_val in range(int(y_range[0]), int(y_range[1]) + 1, dt_major):
            fig.add_hline(y=y_val, line_width=1.2, line_color="DimGray", layer='below')

        fig.update_xaxes(range=[start_view, end_view], mirror=True, showline=True, linecolor='black')

        # REFERENCE LINES
        for val, label in active_refs:
            c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
            # Type A: Burgundy (Maroon) dashed; Others: Blue dashed
            l_color = "maroon" if "Type A" in label else "RoyalBlue"
            fig.add_hline(y=c_val, line_dash="dash", line_color=l_color, opacity=0.8)
        
        return fig
    except Exception as e:
        st.error(f"Critical Graph Error: {e}")
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

# Project Selection
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

###########################
# --- GLOBAL DATA LOAD --- #
###########################

if "master_df" not in st.session_state:
    st.session_state.master_df = pd.DataFrame()
    st.session_state.current_project = None

if selected_project and st.session_state.current_project != selected_project:
    with st.spinner(f"⚡ Syncing {selected_project}..."):
        # We fetch 'Depth' here to ensure the profile can be built
        query = f"""
            SELECT timestamp, temperature, Depth, Location, Bank, NodeNum, approve
            FROM `{MASTER_TABLE}`
            WHERE Project = '{selected_project}'
            AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
            ORDER BY timestamp ASC
        """
        try:
            df = client.query(query).to_dataframe()
            
            if not df.empty:
                # 1. Fix Timestamps
                df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_convert(pytz.UTC) if df['timestamp'].dt.tz else pd.to_datetime(df['timestamp']).dt.tz_localize(pytz.UTC)
                
                # 2. FORCE Numeric Depth (Crucial for Depth Profile)
                df['Depth_Num'] = pd.to_numeric(df['Depth'], errors='coerce')
                
                # 3. Standardize Approval
                df['is_approved'] = df['approve'].astype(str).str.upper().str.strip() == 'TRUE'
                
                st.session_state.master_df = df
                st.session_state.current_project = selected_project
            else:
                st.session_state.master_df = pd.DataFrame()
        except Exception as e:
            st.error(f"Sync Error: {e}")

# Map local references
master_df = st.session_state.master_df
approved_df = master_df[master_df['is_approved'] == True] if not master_df.empty else pd.DataFrame()

####################
# --- SERVICES --- #
####################
#############################
# --- Executive Summary --- #
#############################
if service == "🏠 Executive Summary":
    st.header(f"🏠 Executive Summary: {selected_project if selected_project else 'All Projects'}")
    
    # 1. SORTING & CONTROLS
    st.write("### ↕️ Sorting & View Options")
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        sort_choice = st.selectbox("Sort By", ["None", "Hours Since Last Seen", "Delta Magnitude"])
    with c2:
        sort_order = st.radio("Order", ["Descending", "Ascending"], horizontal=True)
    
    # 2. DATA QUERY
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
                
                # Fetch 24H Metrics
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

                # Status and Delta logic (No color if >24h)
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
                    "Min": f"{round(convert_val(min_val), 1)}°F" if pd.notnull(min_val) else "N/A",
                    "Max": f"{round(convert_val(max_val), 1)}°F" if pd.notnull(max_val) else "N/A",
                    "Delta_Val": delta_style, 
                    "Delta": delta_text,
                    "Hours_Ago": hrs_ago,
                    "Last Seen": f"{ts.strftime('%m/%d %H:%M')} ({hrs_ago}hr) {status_icon}"
                })

            summary_df = pd.DataFrame(summary_rows)

            # 3. APPLY SORTING
            asc = (sort_order == "Ascending")
            if sort_choice == "Hours Since Last Seen":
                summary_df = summary_df.sort_values(by="Hours_Ago", ascending=asc)
            elif sort_choice == "Delta Magnitude":
                summary_df['abs_d'] = summary_df['Delta_Val'].abs().fillna(-1)
                summary_df = summary_df.sort_values(by="abs_d", ascending=asc).drop(columns=['abs_d'])

            # 4. PAGINATION (100 per page)
            batch_size = 100
            total_pages = max((len(summary_df) // batch_size) + 1, 1)
            page = st.number_input("Page", 1, total_pages, 1)
            display_batch = summary_df.iloc[(page-1)*batch_size : page*batch_size]

            # 5. STYLING
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
            
            # Use st.dataframe with hide_index=True to remove the left column
            st.dataframe(
            display_batch[["Project", "Node", "Pipe/Bank", "Pos/Depth", "Min", "Max", "Delta", "Last Seen"]].style.apply(
                lambda x: [style_delta(rv) for rv in display_batch['Delta_Val']], axis=0, subset=['Delta']
            ),
            use_container_width=True,
            hide_index=True,
            height=600
        )
    except Exception as e: 
        st.error(f"Summary Error: {traceback.format_exc()}")
#################################
# --- END EXECUTIVE SUMMARY --- #
#################################
#########################
# --- CLIENT PORTAL --- #
#########################
elif service == "📊 Client Portal":
    if not selected_project:
        st.warning("Please select a project in the sidebar.")
    elif approved_df.empty:
        st.info(f"No approved data found for {selected_project}. Vett data in Admin Tools.")
    else:
        st.header(f"📊 Project Status: {selected_project}")
        tab_time, tab_depth, tab_table = st.tabs(["📈 Timeline Analysis", "📏 Depth Profile", "📋 Project Data"])

        # Shared time calculations
        weeks_view = st.sidebar.slider("Weeks to View", 1, 12, 6, key="portal_slider") # Moved to sidebar for global control or keep here
        now = pd.Timestamp.now(tz=pytz.UTC)
        end_view = (now + pd.Timedelta(days=(7 - now.weekday()) % 7 or 7)).replace(hour=0, minute=0, second=0, microsecond=0)
        start_view = end_view - timedelta(weeks=weeks_view)

        with tab_time:
            locs = sorted(approved_df['Location'].dropna().unique())
            for loc in locs:
                with st.expander(f"📈 {loc}", expanded=True):
                    # Instant Filtering from Memory
                    loc_data = approved_df[(approved_df['Location'] == loc) & (approved_df['timestamp'] >= start_view)]
                    st.plotly_chart(build_standard_sf_graph(loc_data, loc, start_view, end_view, active_refs, unit_mode, unit_label), use_container_width=True, key=f"p_time_{loc}")

       with tab_depth:
            # 1. Filter for numeric depth and approved data
            depth_plot_df = approved_df.dropna(subset=['Depth_Num', 'NodeNum', 'Location'])
            
            if depth_plot_df.empty:
                st.warning("No approved data with valid Depth values found.")
            else:
                for loc in sorted(depth_plot_df['Location'].unique()):
                    with st.expander(f"📏 {loc} Depth Profile", expanded=True):
                        loc_data = depth_plot_df[depth_plot_df['Location'] == loc].copy()
                        fig_d = go.Figure()
                        
                        # Generate Monday Snapshots
                        mondays = pd.date_range(start=start_view, end=now, freq='W-MON')
                        for target_ts in [m.replace(hour=6, minute=0, second=0) for m in mondays]:
                            # Filter window for this specific Monday
                            window = loc_data[(loc_data['timestamp'] >= target_ts - pd.Timedelta(days=1)) & 
                                              (loc_data['timestamp'] <= target_ts + pd.Timedelta(days=1))]
                            
                            if not window.empty:
                                # Find closest reading for each node
                                snaps = []
                                for node in window['NodeNum'].unique():
                                    ndf = window[window['NodeNum'] == node].copy()
                                    ndf['diff'] = (ndf['timestamp'] - target_ts).abs()
                                    snaps.append(ndf.sort_values('diff').iloc[0])
                                
                                snap_df = pd.DataFrame(snaps).sort_values('Depth_Num')
                                fig_d.add_trace(go.Scatter(
                                    x=snap_df['temperature'], 
                                    y=snap_df['Depth_Num'], 
                                    mode='lines+markers', 
                                    name=target_ts.strftime('%m/%d/%Y')
                                ))

                        # Formatting
                        y_limit = int(((loc_data['Depth_Num'].max() // 5) + 1) * 5)
                        fig_d.update_xaxes(title=f"Temp ({unit_label})", range=[-20, 80], dtick=5, showgrid=True, gridcolor='LightGray')
                        for x_v in range(-20, 81, 20): fig_d.add_vline(x=x_v, line_width=2.0, line_color="Black")
                        fig_d.update_yaxes(title="Depth (ft)", range=[y_limit, 0], dtick=10, showgrid=True, gridcolor='LightGray')
                        fig_d.update_layout(plot_bgcolor='white', height=700)
                        
                        st.plotly_chart(fig_d, use_container_width=True, key=f"p_depth_{loc}")

        with tab_table:
            # Table generation from memory
            latest = approved_df.sort_values('timestamp').groupby('NodeNum').tail(1).copy()
            latest['Current Temp'] = latest['temperature'].apply(lambda x: f"{round(convert_val(x), 1)}{unit_label}")
            latest['Position'] = latest.apply(lambda r: f"Bank {r['Bank']}" if pd.notnull(r['Bank']) else f"{r['Depth']} ft", axis=1)
            st.dataframe(latest[['Location', 'Position', 'Current Temp', 'NodeNum']], use_container_width=True, hide_index=True)
#############################
# --- END CLIENT PORTAL --- #
#############################  
###########################
# --- NODE DIAGNOSTIC --- #
###########################  
elif service == "📉 Node Diagnostics":
    st.header(f"📉 Node Diagnostics: {selected_project}")
    
    if not selected_project:
        st.warning("Please select a project.")
    elif master_df.empty:
        st.warning("No data found.")
    else:
        # Instant Location Picker from memory
        locs = sorted(master_df['Location'].dropna().unique())
        c1, c2 = st.columns([2, 1])
        sel_loc = c1.selectbox("Select Pipe / Bank", locs)
        weeks_view = c2.slider("Lookback (Weeks)", 1, 12, 6, key="diag_slider")

        now = pd.Timestamp.now(tz=pytz.UTC)
        end_view = (now + pd.Timedelta(days=(7 - now.weekday()) % 7 or 7)).replace(hour=0, minute=0, second=0, microsecond=0)
        start_view = end_view - timedelta(weeks=weeks_view)

        # Instant local filter
        diag_data = master_df[(master_df['Location'] == sel_loc) & (master_df['timestamp'] >= start_view)]
        
        st.subheader("📈 Timeline Analysis")
        st.plotly_chart(build_standard_sf_graph(diag_data, sel_loc, start_view, end_view, active_refs, unit_mode, unit_label), use_container_width=True)
        try:
            # 1. ANALYTICS CONTROLS
            # Fetch locations for the selected project
            loc_q = f"SELECT DISTINCT Location FROM `{MASTER_TABLE}` WHERE Project = '{selected_project}'"
            loc_df = client.query(loc_q).to_dataframe()
            
            c1, c2 = st.columns([2, 1])
            with c1: 
                sel_loc = st.selectbox("Select Pipe / Bank to Analyze", sorted(loc_df['Location'].dropna().unique()))
            with c2: 
                weeks_view = st.slider("Lookback (Weeks)", 1, 12, 6)

            # 2. DATE CALCULATIONS
            # Ensures we show full weeks ending at the next Monday midnight
            now = pd.Timestamp.now(tz=pytz.UTC)
            days_until_monday = (7 - now.weekday()) % 7
            if days_until_monday == 0: days_until_monday = 7
            end_view = (now + pd.Timedelta(days=days_until_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
            start_view = end_view - timedelta(weeks=weeks_view)

            # 3. DATA FETCHING
            diag_q = f"""
                SELECT timestamp, temperature, Depth, Location, Bank, NodeNum
                FROM `{MASTER_TABLE}`
                WHERE Project = '{selected_project}' AND Location = '{sel_loc}'
                AND timestamp >= '{start_view.strftime('%Y-%m-%d %H:%M:%S')}'
                ORDER BY timestamp ASC
            """
            with st.spinner("Fetching diagnostic data..."):
                df_diag = client.query(diag_q).to_dataframe()
            
            if df_diag.empty:
                st.warning(f"No data found for {sel_loc} in the selected timeframe.")
            else:
                df_diag['timestamp'] = pd.to_datetime(df_diag['timestamp'])
                if df_diag['timestamp'].dt.tz is None:
                    df_diag['timestamp'] = df_diag['timestamp'].dt.tz_localize(pytz.UTC)
                else:
                    df_diag['timestamp'] = df_diag['timestamp'].dt.tz_convert(pytz.UTC)

                # --- 4. TIME VS TEMPERATURE GRAPH (TOP) ---
                st.subheader("📈 Timeline Analysis")
                fig_time = build_standard_sf_graph(df_diag, sel_loc, start_view, end_view, active_refs, unit_mode, unit_label)
                st.plotly_chart(fig_time, use_container_width=True)

                st.divider()

                # --- 5. DEPTH VS TEMPERATURE GRAPH (MIDDLE) ---
                st.subheader("📏 Depth Profile Analysis")
                df_diag['Depth_Num'] = pd.to_numeric(df_diag['Depth'], errors='coerce')
                depth_only_df = df_diag.dropna(subset=['Depth_Num', 'NodeNum']).copy()
                
                if depth_only_df.empty:
                    st.info("No depth-based sensors found for this location.")
                else:
                    # Generate Monday 6AM Snapshots
                    all_mondays = pd.date_range(start=start_view, end=end_view, freq='W-MON')
                    target_times = [m.replace(hour=6, minute=0, second=0, microsecond=0) for m in all_mondays]
                    
                    fig_depth = go.Figure()
                    for target_ts in target_times:
                        # 24-hour search window
                        window_df = depth_only_df[(depth_only_df['timestamp'] >= target_ts - pd.Timedelta(days=1)) & 
                                                  (depth_only_df['timestamp'] <= target_ts + pd.Timedelta(days=1))]
                        if window_df.empty: continue
                        
                        snapshot_points = []
                        for node in window_df['NodeNum'].unique():
                            node_df = window_df[window_df['NodeNum'] == node].copy()
                            node_df['diff'] = (node_df['timestamp'] - target_ts).abs()
                            closest = node_df.sort_values('diff').iloc[0]
                            if closest['diff'] <= pd.Timedelta(days=1):
                                snapshot_points.append(closest)
                        
                        if snapshot_points:
                            snap_df = pd.DataFrame(snapshot_points).sort_values('Depth_Num')
                            fig_depth.add_trace(go.Scatter(
                                x=snap_df['temperature'], y=snap_df['Depth_Num'],
                                mode='lines+markers', 
                                name=target_ts.strftime('%m/%d/%Y'), # Legend shows date only
                                hovertemplate="Depth: %{y}ft<br>Temp: %{x}°"
                            ))

                    # Depth Axis Logic: Rounded to nearest 5 or 10
                    max_d = depth_only_df['Depth_Num'].max()
                    y_limit = int(((max_d // 5) + 1) * 5)
                    y_major = 20 if y_limit > 60 else 10
                    y_minor = 10 if y_limit > 60 else 5

                    fig_depth.update_xaxes(
                        title=f"Temp ({unit_label})", range=[-20, 80] if unit_mode == "Fahrenheit" else [-30, 30],
                        dtick=5, gridcolor='LightGray', mirror=True, showline=True, linecolor='black'
                    )
                    # Add major vertical lines at 20 degree intervals
                    for x_v in range(-20, 81, 20):
                        fig_depth.add_vline(x=x_v, line_width=1, line_color="Gray")

                    fig_depth.update_yaxes(
                        title="Depth (ft) - Surface at 0", range=[y_limit, 0], 
                        dtick=y_major, gridcolor='Gray', mirror=True, showline=True, linecolor='black'
                    )
                    # Add minor horizontal lines
                    for d_v in range(0, y_limit + 1, y_minor):
                        fig_depth.add_hline(y=d_v, line_width=0.5, line_color="LightGray")

                    fig_depth.update_layout(
                        title=f"{sel_loc}: Depth vs Temperature", 
                        plot_bgcolor='white', height=700,
                        legend=dict(title="Weekly Snapshots (6AM)", orientation="v", x=1.02, y=1)
                    )
                    
                    # Add Freezing Reference Line
                    freeze_val = 32 if unit_mode == "Fahrenheit" else 0
                    fig_depth.add_vline(x=freeze_val, line_dash="dash", line_color="RoyalBlue", opacity=0.5)
                    
                    st.plotly_chart(fig_depth, use_container_width=True)

                st.divider()

                # --- 6. SENSOR SUMMARY TABLE (BOTTOM) ---
                st.subheader(f"📋 Engineering Summary: {sel_loc}")
                
                # Query latest data for all nodes in this pipe
                summary_q = f"""
                    SELECT * FROM `{MASTER_TABLE}` 
                    WHERE Project = '{selected_project}' AND Location = '{sel_loc}'
                    QUALIFY ROW_NUMBER() OVER(PARTITION BY NodeNum ORDER BY timestamp DESC) = 1
                """
                raw_summary = client.query(summary_q).to_dataframe()
                
                if not raw_summary.empty:
                    summary_rows = []
                    for _, row in raw_summary.iterrows():
                        node_id = row['NodeNum']
                        ts = row['timestamp'].tz_localize(pytz.UTC) if row['timestamp'].tzinfo is None else row['timestamp']
                        hrs_ago = int((now - ts).total_seconds() / 3600)
                        
                        # 24H Metrics for Min, Max, and Delta
                        metrics_q = f"""
                            SELECT 
                                MIN(temperature) as min_24, 
                                MAX(temperature) as max_24,
                                (SELECT temperature FROM `{MASTER_TABLE}` WHERE NodeNum = '{node_id}' ORDER BY timestamp DESC LIMIT 1) - 
                                (SELECT temperature FROM `{MASTER_TABLE}` WHERE NodeNum = '{node_id}' AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) ORDER BY timestamp ASC LIMIT 1) as delta_24
                            FROM `{MASTER_TABLE}` 
                            WHERE NodeNum = '{node_id}' AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
                        """
                        m_res = client.query(metrics_q).to_dataframe()
                        min_v = m_res['min_24'].iloc[0] if not m_res.empty else None
                        max_v = m_res['max_24'].iloc[0] if not m_res.empty else None
                        raw_delta = m_res['delta_24'].iloc[0] if not m_res.empty else None

                        # Status indicators
                        status_icon = "🔴" if hrs_ago > 24 else ("🟢" if hrs_ago < 6 else "🟡")
                        pos_display = f"Bank {row['Bank']}" if str(row['Bank']).strip().lower() not in ["","none","nan","null"] else f"{row['Depth']} ft"

                        summary_rows.append({
                            "Node": node_id,
                            "Pos/Depth": pos_display,
                            "Min (24h)": f"{round(convert_val(min_v), 1)}{unit_label}" if pd.notnull(min_v) else "N/A",
                            "Max (24h)": f"{round(convert_val(max_v), 1)}{unit_label}" if pd.notnull(max_v) else "N/A",
                            "Delta (24h)": f"{round(raw_delta, 1)}°F" if pd.notnull(raw_delta) else "0.0°F",
                            "Delta_Val": raw_delta,
                            "Last Seen": f"{ts.strftime('%m/%d %H:%M')} ({hrs_ago}h) {status_icon}"
                        })
                    
                    summary_df = pd.DataFrame(summary_rows)
                    
                    # Styling logic for Delta column
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

                    st.dataframe(
                        summary_df[["Node", "Pos/Depth", "Min (24h)", "Max (24h)", "Delta (24h)", "Last Seen"]].style.apply(
                            lambda x: [style_delta(rv) for rv in summary_df['Delta_Val']], axis=0, subset=['Delta (24h)']
                        ),
                        use_container_width=True,
                        hide_index=True
                    )

        except Exception as e:
            st.error(f"Diagnostics Error: {e}")
###############################
# --- END NODE DIAGNOSTIC --- #
###############################
###############################
# --- DATA INTAKE LAB --- #
###############################
elif service == "📤 Data Intake Lab":
    st.header("📤 Data Ingestion & Recovery")
    
    tab1, tab2, tab3 = st.tabs(["📄 Manual File Upload", "📡 API Data Recovery", "🛠️ Maintenance"])

    with tab1:
        st.subheader("📄 Manual File Ingestion")
        st.info("Upload Lord SensorConnect (Wide), Lord Desktop Log (Narrow), or SensorPush CSVs.")
        u_file = st.file_uploader("Upload CSV", type=['csv'], key="manual_upload_unified_fixed")
        
        if u_file is not None:
            import io
            filename = u_file.name.lower()
            raw_content = u_file.getvalue().decode('utf-8').splitlines()
            
            # --- DETECT FILE TYPE ---
            is_lord_wide = any("DATA_START" in line for line in raw_content[:100])
            is_lord_narrow = "nodenumber" in raw_content[0].lower() and "temperature" in raw_content[0].lower()
            
            # --- CASE 1: LORD SENSORCONNECT (WIDE) ---
            if is_lord_wide:
                try:
                    start_idx = next(i for i, line in enumerate(raw_content) if "DATA_START" in line)
                    df_wide = pd.read_csv(io.StringIO("\n".join(raw_content[start_idx+1:])))
                    # Rename 'Time' to 'timestamp' and melt columns into 'NodeNum'
                    df_long = df_wide.melt(id_vars=['Time'], var_name='NodeNum', value_name='temperature')
                    df_long['NodeNum'] = df_long['NodeNum'].str.replace(':', '-', regex=False)
                    df_long['timestamp'] = pd.to_datetime(df_long['Time'], format='mixed')
                    df_long = df_long.dropna(subset=['temperature'])
                    
                    st.success(f"✅ Lord Wide Format Parsed: {len(df_long)} readings.")
                    st.dataframe(df_long.head())
                    if st.button("🚀 UPLOAD LORD WIDE DATA"):
                        client.load_table_from_dataframe(df_long[['timestamp', 'NodeNum', 'temperature']], 
                                                         f"{PROJECT_ID}.{DATASET_ID}.raw_lord").result()
                        st.success("Uploaded successfully to raw_lord!")
                except Exception as e: st.error(f"Lord Wide Error: {e}")

            # --- CASE 2: LORD DESKTOP LOG (NARROW) ---
            elif is_lord_narrow:
                try:
                    df_ln = pd.read_csv(io.StringIO("\n".join(raw_content)))
                    # MAP TO BIGQUERY SCHEMA: Case-sensitive NodeNum and timestamp
                    df_ln = df_ln.rename(columns={
                        'Timestamp': 'timestamp', 
                        'nodenumber': 'NodeNum', 
                        'temperature': 'temperature'
                    })
                    df_ln['timestamp'] = pd.to_datetime(df_ln['timestamp'], format='mixed')
                    df_ln['NodeNum'] = df_ln['NodeNum'].str.replace(':', '-', regex=False)
                    
                    st.success(f"✅ Lord Narrow Format Parsed: {len(df_ln)} readings.")
                    st.dataframe(df_ln.head())
                    if st.button("🚀 UPLOAD LORD NARROW DATA"):
                        client.load_table_from_dataframe(df_ln[['timestamp', 'NodeNum', 'temperature']], 
                                                         f"{PROJECT_ID}.{DATASET_ID}.raw_lord").result()
                        st.success("Uploaded successfully to raw_lord!")
                except Exception as e: st.error(f"Lord Narrow Error: {e}")

            # --- CASE 3: SENSORPUSH ---
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
                        # Mapping to the raw_sensorpush schema
                        df_up['sensor_id'] = df_sp['SensorId'].astype(str).str.strip()
                        df_up['timestamp'] = pd.to_datetime(df_sp[ts_col], format='mixed')
                        t_cols = [c for c in df_sp.columns if "Temperature" in c or "Thermocouple" in c]
                        df_up['temperature'] = pd.to_numeric(df_sp[t_cols].bfill(axis=1).iloc[:, 0], errors='coerce')
                        df_up = df_up.dropna(subset=['timestamp', 'temperature'])

                        st.success(f"✅ SensorPush Parsed: {len(df_up)} readings.")
                        if st.button("🚀 UPLOAD SENSORPUSH"):
                            client.load_table_from_dataframe(df_up, f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush").result()
                            st.success("Uploaded successfully to raw_sensorpush!")
                    else:
                        st.error("Format not recognized. Check CSV headers.")
                except Exception as e: st.error(f"SensorPush Error: {e}")

    with tab2:
        st.subheader("📡 Cloud-to-Cloud API Sync")
        c1, c2 = st.columns(2)
        start_date = c1.date_input("Start Date", datetime.now() - timedelta(days=1))
        end_date = c2.date_input("End Date", datetime.now())
        
        if st.button("🛰️ FETCH & SYNC"):
            # Level 3: Date Conversion
            start_dt = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=pytz.UTC)
            end_dt = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=pytz.UTC)
            
            with st.spinner("Fetching data..."):
                # Level 4: Call the Function
                df_api = fetch_sensorpush_data(start_dt, end_dt)
                
                if not df_api.empty:
                    # Level 5: Upload to BigQuery
                    table_path = f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush"
                    client.load_table_from_dataframe(df_api, table_path).result()
                    st.success(f"✅ Integrated {len(df_api)} points successfully!")
                else:
                    # Level 5: Fallback
                    st.warning("No data found for this range.")
                    
    with tab3:
        st.subheader("🛠️ Metadata Management")
        u_meta = st.file_uploader("Upload Master_Log / Metadata CSV", type=['csv'])
        if u_meta:
            df_new_meta = pd.read_csv(u_meta)
            st.dataframe(df_new_meta.head())
            if st.button("Overwrite Master Metadata"):
                # This replaces the mapping table in BigQuery
                client.load_table_from_dataframe(df_new_meta, f"{PROJECT_ID}.{DATASET_ID}.master_metadata", 
                                                 job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")).result()
                st.success("Master Metadata Updated!")
###############################
# --- END DATA INTAKE LAB --- #
###############################
#######################
# --- ADMIN TOOLS --- #
#######################             
elif service == "🛠️ Admin Tools":
    st.header("🛠️ Engineering Admin Tools")
    
    # 1. TAB NAVIGATION
    tab_scrub, tab_approve, tab_cleaner = st.tabs(["🧹 Deep Data Scrub", "✅ Bulk Approval", "🧨 Surgical Cleaner"])

    # Physical Source Tables
    RAW_TABLES = [
        f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush",
        f"{PROJECT_ID}.{DATASET_ID}.raw_lord"
    ]

    with tab_scrub:
        st.subheader("🧹 Deep Data Scrub")
        scrub_target = st.radio("Select Source Table", ["SensorPush", "Lord"], horizontal=True)
        target_table = f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush" if scrub_target == "SensorPush" else f"{PROJECT_ID}.{DATASET_ID}.raw_lord"
        
        # Using NodeNum as confirmed by your schema
        id_col = "NodeNum" 

        if st.button(f"🚀 Execute Deep Scrub on {scrub_target}"):
            with st.spinner(f"Cleaning {scrub_target}..."):
                dedup_sql = f"""
                CREATE OR REPLACE TABLE `{target_table}` AS 
                SELECT * EXCEPT(rn) FROM (
                    SELECT *, 
                           ROW_NUMBER() OVER(
                               PARTITION BY {id_col}, TIMESTAMP_TRUNC(timestamp, HOUR) 
                               ORDER BY timestamp DESC
                           ) as rn
                    FROM `{target_table}` 
                    WHERE temperature IS NOT NULL
                ) WHERE rn = 1
                """
                try:
                    client.query(dedup_sql).result()
                    st.success(f"Success! {scrub_target} cleaned (1 reading per hour).")
                except Exception as e:
                    st.error(f"Scrub Error: {e}")

    with tab_approve:
        st.subheader("✅ Bulk Approval")
        st.info("Marking data as approved in both raw_sensorpush and raw_lord.")
        if st.button("Mark All Data as Approved"):
            success_count = 0
            for table in RAW_TABLES:
                try:
                    # Note: This assumes 'approve' or 'is_approved' column exists in raw tables
                    # Based on your SP schema, the column is named 'approve'
                    approve_sql = f"UPDATE `{table}` SET approve = 'TRUE' WHERE 1=1" 
                    job = client.query(approve_sql)
                    job.result()
                    success_count += 1
                except Exception as e:
                    st.warning(f"Could not update {table}: {e}")
            
            if success_count > 0:
                st.success("Approval command sent to available raw tables.")

    with tab_cleaner:
        st.subheader("🧨 Surgical Data Cleaner")
        st.write("Deletes bad data from both Raw Source tables.")
        
        # Timeframe selection
        col1, col2 = st.columns(2)
        start_del = col1.date_input("Start Date", datetime.now() - timedelta(days=1))
        end_del = col2.date_input("End Date", datetime.now())
        
        # Node selection
        node_to_clean = st.text_input("Enter NodeNum to clean (Optional - leave blank for all nodes)")

        if st.button("🔥 DELETE DATA FROM RAW SOURCES"):
            for table in RAW_TABLES:
                try:
                    # Constructing deletion for raw tables
                    del_clause = f"CAST(timestamp AS DATE) BETWEEN '{start_del}' AND '{end_del}'"
                    if node_to_clean:
                        del_clause += f" AND NodeNum = '{node_to_clean}'"
                    
                    delete_sql = f"DELETE FROM `{table}` WHERE {del_clause}"
                    
                    with st.spinner(f"Deleting from {table}..."):
                        del_job = client.query(delete_sql)
                        del_job.result()
                        st.write(f"✔️ {table}: Removed {del_job.num_dml_affected_rows} records.")
                except Exception as e:
                    st.error(f"Error on {table}: {e}")
            st.success("Surgical cleaning complete.")

###########################
# --- END ADMIN TOOLS --- #
###########################
