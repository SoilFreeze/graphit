import streamlit as st
import pandas as pd
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

#########################
# --- CONFIGURATION --- #
#########################
# --- 1. CONFIGURATION & STYLING ---
st.set_page_config(page_title="SoilFreeze Data Lab", layout="wide")

DATASET_ID = "sensor_data" 
PROJECT_ID = "sensorpush-export"

# --- 1. CONFIGURATION & AUTH ---
st.set_page_config(page_title="SoilFreeze Data Lab", layout="wide")

DATASET_ID = "sensor_data" 
PROJECT_ID = "sensorpush-export"

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

# CRITICAL: Initialize the global client variable here
client = get_bq_client()
#############################
# --- END CONFIGURATION --- #
#############################
#########################
# --- REBUILD TABLE --- #
#########################
def rebuild_master_table(mode="preserve"):
    """
    Rebuilds the master table. Handles the 'ex' error by checking 
    if the table exists before attempting to join.
    """
    # 1. Determine if we are force-approving everything
    status_logic = "TRUE" if mode == "approve_all" else "COALESCE(ex.is_approved, FALSE)"
    
    # 2. Check if the table actually exists to avoid the 'ex' error
    table_id = f"{PROJECT_ID}.{DATASET_ID}.final_databoard_master"
    exists = True
    try:
        client.get_table(table_id)
    except Exception:
        exists = False

    # 3. Build the SQL based on whether the table exists
    join_clause = ""
    if exists and mode == "preserve":
        join_clause = f"""
            LEFT JOIN `{table_id}` ex 
            ON h.ts = ex.timestamp AND m.NodeNum = ex.sensor_id
        """
    else:
        # If table doesn't exist, we can't join 'ex', so we set status to FALSE
        status_logic = "TRUE" if mode == "approve_all" else "FALSE"

    scrub_sql = f"""
        CREATE OR REPLACE TABLE `{table_id}` AS 
        WITH RawUnified AS (
            SELECT CAST(timestamp AS TIMESTAMP) as ts, value as temp, REPLACE(nodenumber, ':', '-') as node 
            FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord` WHERE value IS NOT NULL
            UNION ALL 
            SELECT CAST(timestamp AS TIMESTAMP) as ts, temperature as temp, REPLACE(sensor_id, ':', '-') as node 
            FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` WHERE temperature IS NOT NULL
        ),
        HourlyDedupped AS (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY node, TIMESTAMP_TRUNC(ts, HOUR) ORDER BY ts DESC) as rank 
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
            ON (SUBSTR(TRIM(h.node), 1, 12) = SUBSTR(TRIM(CAST(m.PhysicalID AS STRING)), 1, 12))
            OR (TRIM(h.node) = TRIM(CAST(m.NodeNum AS STRING)))
        {join_clause}
        WHERE h.rank = 1
    """
    
    try:
        client.query(scrub_sql).result()
        return True
    except Exception as e:
        st.error(f"Rebuild Error: {e}")
        return False
#############################
# --- END REBUILD TABLE --- #
#############################
############################
# --- FETCH SENSORPUSH --- #
############################
        
# --- 2. ENGINE: FAST API FETCH ---

def fetch_sensorpush_data(start_dt, end_dt):
    """
    Fetches data for ALL sensors (HT.w and TC.x) in 12-hour chunks 
    to prevent timeouts.
    """
    ACCOUNTS = [
        {'email': 'tsteele@soilfreeze.com', 'password': 'Freeze123!!'},
        {'email': 'soilfreeze98072@gmail.com', 'password': 'Freeze123!!'}
    ]
    BASE_URL = "https://api.sensorpush.com/api/v1"
    all_records = []

    for acc in ACCOUNTS:
        try:
            # 1. Auth
            auth_resp = requests.post(f"{BASE_URL}/oauth/authorize", json=acc, timeout=15)
            if auth_resp.status_code != 200: continue
            token = requests.post(f"{BASE_URL}/oauth/accesstoken", 
                                   json={"authorization": auth_resp.json().get('authorization')}, timeout=15).json().get('accesstoken')
            headers = {"Authorization": token}

            # 2. Get All Sensors (HT.w and TC.x)
            dev_resp = requests.post(f"{BASE_URL}/devices/sensors", headers=headers, json={}, timeout=20)
            name_map = {}
            if dev_resp.status_code == 200:
                s_list = dev_resp.json().values() if isinstance(dev_resp.json(), dict) else dev_resp.json()
                for s in s_list:
                    s_type = str(s.get('type', ''))
                    # Updated Filter: Allow both HT.w and TC.x
                    if s_type in ['HT.w', 'TC.x']:
                        name_map[str(s.get('id'))] = s.get('name', str(s.get('id')))

            # 3. Chunked Fetching (12-hour blocks for stability)
            current_start = start_dt
            while current_start < end_dt:
                current_end = min(current_start + timedelta(hours=12), end_dt)
                payload = {
                    "limit": 10000, 
                    "startTime": current_start.strftime('%Y-%m-%dT%H:%M:%S+0000'), 
                    "sensors": list(name_map.keys())
                }
                # Increased timeout to 60s
                r = requests.post(f"{BASE_URL}/samples", headers=headers, json=payload, timeout=60)
                
                if r.status_code == 200:
                    data = r.json().get('sensors', {})
                    for s_id, samples in data.items():
                        for s in samples:
                            ts = pd.to_datetime(s.get('observed'))
                            if current_start <= ts <= current_end:
                                # Logic to handle temperature from different sensor types
                                temp = s.get('temp_f') or s.get('temperature') or s.get('thermocouple_temperature')
                                if temp is None and s.get('temp_c') is not None:
                                    temp = (float(s['temp_c']) * 1.8) + 32
                                
                                if temp is not None:
                                    all_records.append({
                                        'timestamp': ts,
                                        'sensor_id': s_id.replace(':', '-'),
                                        'sensor_name': name_map.get(s_id),
                                        'temperature': round(float(temp), 2)
                                    })
                current_start = current_end
        except Exception as e:
            st.error(f"Fetch Error ({acc['email']}): {e}")
            
    return pd.DataFrame(all_records)
################################
# --- END FETCH SENSORPUSH --- #
################################
########################
# --- GRAPH ENGINE --- #
########################
def build_standard_sf_graph(df, title, start_view, end_view, active_refs):
    """
    SF Standard: 6hr, Midnight, and Monday Gridlines.
    Legend Format: Depth (SP-XXXX).
    """
    display_df = df.copy()
    y_range, y_ticks, y_label, m_step = [-20, 80], [-20, 0, 20, 40, 60, 80], "Temp (°F)", 5

    # 1. Labeling Logic: Use sensor_name (SP/TP) instead of the long ID
    display_df['label'] = display_df['depth'] + " (" + display_df['sensor_name'] + ")"
    
    processed_dfs = []
    for lbl in display_df['label'].unique():
        s_df = display_df[display_df['label'] == lbl].copy().sort_values('timestamp')
        s_df['gap'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
        gaps = s_df[s_df['gap'] > 6.0].copy()
        if not gaps.empty:
            gaps['temperature'] = None
            gaps['timestamp'] = gaps['timestamp'] - timedelta(minutes=1)
            s_df = pd.concat([s_df, gaps]).sort_values('timestamp')
        processed_dfs.append(s_df)
    clean_df = pd.concat(processed_dfs) if processed_dfs else display_df
    
    # 2. Trace Creation
    fig = go.Figure()
    # Sort by numerical depth using the 're' module
    labels = sorted(clean_df['label'].unique(), 
                    key=lambda x: int(next(iter(re.findall(r'\d+', x)), 0)))
    
    for lbl in labels:
        sensor_df = clean_df[clean_df['label'] == lbl]
        fig.add_trace(go.Scatter(x=sensor_df['timestamp'], y=sensor_df['temperature'], 
                                 name=lbl, mode='lines', connectgaps=False))

    # 3. Formatting & Granular Gridlines
    fig.update_layout(
        title={'text': title, 'x': 0, 'xanchor': 'left'},
        plot_bgcolor='white', hovermode="x unified", margin=dict(t=50, l=50, r=150), height=750
    )
    
    fig.update_yaxes(title=y_label, tickmode='array', tickvals=y_ticks, range=y_range,
                     gridcolor='DimGray', gridwidth=1.5, minor=dict(dtick=m_step, gridcolor='Silver', showgrid=True),
                     mirror=True, showline=True, linecolor='black', linewidth=2)

    # X-Axis Formatting
    fig.update_xaxes(
        range=[start_view, end_view],
        mirror=True, showline=True, linecolor='black', linewidth=2,
        gridcolor='DimGray', gridwidth=1,
        tick0=start_view, 
        dtick=86400000, # 24 hours
        tickformat="%a\n%m/%d",
        minor=dict(dtick=21600000, gridcolor='Silver', showgrid=True) # 6 hours
    )

    # 4. Vertical Monday Lines
    curr_ts = start_view
    while curr_ts <= end_view:
        if curr_ts.weekday() == 0: # 0 is Monday
            fig.add_vline(x=curr_ts.timestamp() * 1000, line_width=2, line_color="DimGray")
        curr_ts += timedelta(days=1)

    for val, label in active_refs:
        fig.add_hline(y=val, line_dash="dash", line_color="blue", annotation_text=f"{label} {val}°")
    
    return fig
############################
# --- END GRAPH ENGINE --- #
############################
###################
# --- SIDEBAR --- #
###################
# --- 4. SIDEBAR ---
st.sidebar.title("❄️ SoilFreeze Lab")
show_32 = st.sidebar.checkbox("Freezing (32°F)", value=True)
show_26 = st.sidebar.checkbox("Type B (26.6°F)", value=True)
show_10 = st.sidebar.checkbox("Type A (10.2°F)", value=True)

active_refs = []
if show_32: active_refs.append((32, "Freezing"))
if show_26: active_refs.append((26.6, "Type B"))
if show_10: active_refs.append((10.2, "Type A"))

service = st.sidebar.selectbox("Select Service", ["🏠 Executive Summary", "📊 Client Portal", "📉 Node Diagnostics", "📤 Data Intake Lab", "🛠️ Admin Tools"])
#######################
# --- END SIDEBAR --- #
#######################
####################
# --- SERVICES --- #
####################
#############################
# --- EXECUTIVE SUMMARY --- #
#############################
if service == "🏠 Executive Summary":
    st.header("🏠 Site Health Summary")

    try:
        proj_q = f"SELECT DISTINCT project FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE project IS NOT NULL"
        meta_df = client.query(proj_q).to_dataframe()
        all_projs = sorted(meta_df['project'].unique())
        sel_summary_proj = st.selectbox("Select Project Focus", all_projs, index=0)

        query = f"""
            WITH NodeLimits AS (
                SELECT sensor_id, MAX(timestamp) as max_ts FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master`
                WHERE project = '{sel_summary_proj}' GROUP BY sensor_id
            )
            SELECT m.timestamp, m.temperature, m.location, m.depth, m.sensor_id, m.sensor_name
            FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` m
            JOIN NodeLimits nl ON m.sensor_id = nl.sensor_id
            WHERE m.timestamp >= TIMESTAMP_SUB(nl.max_ts, INTERVAL 24 HOUR)
        """
        df_summary = client.query(query).to_dataframe()
        if not df_summary.empty:
            now_ts = datetime.now(pytz.UTC)
            summary_stats = []
            for node in df_summary['sensor_id'].unique():
                n_df = df_summary[df_summary['sensor_id'] == node].sort_values('timestamp')
                curr_t = n_df['temperature'].iloc[-1]
                chg = curr_t - n_df['temperature'].iloc[0]
                last_ts = n_df['timestamp'].iloc[-1]
                if last_ts.tzinfo is None: last_ts = last_ts.replace(tzinfo=pytz.UTC)
                hrs = (now_ts - last_ts).total_seconds() / 3600
                summary_stats.append({
                    "Location": n_df['location'].iloc[0], "Depth": f"{n_df['depth'].iloc[0]}ft", "Node ID": node,
                    "Status": f"{last_ts.strftime('%m/%d %H:%M')} ({int(round(hrs, 0))}h ago)",
                    "Change": round(float(chg), 1), "Current": round(float(curr_t), 1)
                })
            st.dataframe(pd.DataFrame(summary_stats), width='stretch', hide_index=True)
    except Exception as e: st.error(f"Summary Error: {e}")
#################################
# --- END EXECUTIVE SUMMARY --- #
#################################
#########################
# --- CLIENT PORTAL --- #
#########################
# 4B. CLIENT PORTAL
elif service == "📊 Client Portal":
    st.header("📊 Project Status Report")
    try:
        meta_q = f"SELECT DISTINCT project, location FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE is_approved = TRUE"
        meta_df = client.query(meta_q).to_dataframe()
        
        if meta_df.empty:
            st.warning("No approved data available.")
        else:
            c1, c2, c3 = st.columns([1, 1, 1])
            with c1: sel_proj = st.selectbox("Project", sorted(meta_df['project'].dropna().unique()))
            with c2: 
                locs = sorted(meta_df[meta_df['project'] == sel_proj]['location'].dropna().unique())
                sel_loc = st.selectbox("Pipe / Bank", locs)
            with c3: weeks_to_view = st.slider("Weeks to View", 1, 12, 6)
            
            data_q = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE project = '{sel_proj}' AND location = '{sel_loc}' AND is_approved = TRUE ORDER BY timestamp ASC"
            df_c = client.query(data_q).to_dataframe()
            df_c['timestamp'] = pd.to_datetime(df_c['timestamp'])

            max_approved_ts = df_c['timestamp'].max()
            current_monday = (max_approved_ts - timedelta(days=max_approved_ts.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
            start_view = current_monday - timedelta(weeks=weeks_to_view - 1)
            end_view = current_monday + timedelta(days=7)

            if "bank" not in sel_loc.lower():
                st.subheader("🌡️ Soil Temperature Profile (Weekly Snapshots)")
                snapshot = df_c[(df_c['timestamp'].dt.weekday == 0) & (df_c['timestamp'].dt.hour == 6)].copy()
                snapshot = snapshot[snapshot['timestamp'] >= start_view]
                if not snapshot.empty:
                    snapshot['depth_num'] = snapshot['depth'].str.extract('(\d+)').astype(float)
                    snapshot['Date'] = snapshot['timestamp'].dt.strftime('%m/%d')
                    fig_profile = px.line(snapshot.sort_values('depth_num'), x='temperature', y='depth_num', color='Date', markers=True, range_x=[-20, 80])
                    for val, label in active_refs:
                        fig_profile.add_vline(x=val, line_dash="dash", line_color="blue", annotation_text=label)
                    fig_profile.update_layout(title={'text': "Temperature by Depth", 'x': 0, 'xanchor': 'left'}, plot_bgcolor='white', height=600)
                    fig_profile.update_yaxes(autorange="reversed")
                    st.plotly_chart(fig_profile, width='stretch')

            st.subheader("📈 Historical Trends")
            fig_timeline = build_standard_sf_graph(df_c, f"{weeks_to_view}-Week Trend: {sel_loc}", start_view, end_view, active_refs)
            st.plotly_chart(fig_timeline, width='stretch')
            
            st.subheader(f"⏱️ Performance Window: {max_approved_ts.strftime('%m/%d %H:%M')}")
            last_approved_24h = df_c[df_c['timestamp'] >= (max_approved_ts - timedelta(hours=24))].copy()
            if not last_approved_24h.empty:
                last_approved_24h['depth_num'] = last_approved_24h['depth'].str.extract('(\d+)').astype(float)
                stats = last_approved_24h.groupby(['depth', 'depth_num']).agg(
                    High=('temperature', 'max'), Low=('temperature', 'min'), Current=('temperature', 'last'), Last_Update=('timestamp', 'last')
                ).reset_index()
                stats['Difference'] = stats['High'] - stats['Low']
                st.dataframe(stats[['depth', 'Current', 'High', 'Low', 'Difference', 'Last_Update']], width='stretch', hide_index=True)

    except Exception as e: st.error(f"Portal Error: {e}")
#############################
# --- END CLIENT PORTAL --- #
#############################  
###########################
# --- NODE DIAGNOSTIC --- #
###########################  
# 4C. NODE DIAGNOSTICS
# --- 4C. NODE DIAGNOSTICS ---
elif service == "📉 Node Diagnostics":
    st.header("📉 High-Resolution Node Diagnostics")
    try:
        # Get your metadata to fill the dropdowns
        meta_df = client.query(f"SELECT DISTINCT project, location FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master`").to_dataframe()
        
        c1, c2, c3 = st.columns(3)
        with c1: sel_proj = st.selectbox("Project", sorted(meta_df['project'].unique()))
        with c2: sel_loc = st.selectbox("Pipe / Bank", sorted(meta_df[meta_df['project'] == sel_proj]['location'].unique()))
        with c3: weeks = st.slider("Lookback (Weeks)", 1, 12, 1)

        now = datetime.now(pytz.UTC)
        monday_this_week = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        start_view = monday_this_week - timedelta(weeks=weeks-1)
        end_view = monday_this_week + timedelta(days=7)

        # Explicitly pull the newly created sensor_name (TP/SP Name)
        data_q = f"""
            SELECT timestamp, temperature, depth, sensor_name, sensor_id
            FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` 
            WHERE project = '{sel_proj}' AND location = '{sel_loc}' 
            AND timestamp >= '{start_view.strftime('%Y-%m-%d %H:%M:%S')}' 
            ORDER BY timestamp ASC
        """
        df_g = client.query(data_q).to_dataframe()
        
        if not df_g.empty:
            df_g['timestamp'] = pd.to_datetime(df_g['timestamp'])
            st.plotly_chart(build_standard_sf_graph(df_g, f"{sel_proj} | {sel_loc}", start_view, end_view, active_refs), use_container_width=True)
        else:
            st.warning("No data mapped. Check if the Physical IDs in your CSV match the IDs in your Metadata Google Sheet.")
            
    except Exception as e:
        st.error(f"Diagnostics Error: {e}")
###############################
# --- END NODE DIAGNOSTIC --- #
###############################
###############################
# --- DATA INTAKE LAB --- #
###############################
# --- 4D. DATA INTAKE LAB (RECOVERY & AUDIT) ---
elif service == "📤 Data Intake Lab":
    st.header("📤 Data Ingestion & Recovery")
    
    # 1. Create Tabs
    tab1, tab2, tab3 = st.tabs(["📄 Manual File Upload", "📡 API Data Recovery", "🛠️ Maintenance"])

    with tab1:
        st.subheader("Manual CSV Ingestion")
        st.info("Handles SensorPush CSVs. IDs are forced to String format to prevent rounding.")
        u_file = st.file_uploader("Upload SensorPush CSV", type=['csv'], key="manual_upload")
        
        if u_file is not None:
            try:
                import io
                # Read raw bytes and find the header row
                raw_bytes = u_file.getvalue().decode('utf-8').splitlines()
                header_idx = -1
                for i, line in enumerate(raw_bytes):
                    if "SensorId" in line or "Observed" in line:
                        header_idx = i
                        break
                
                if header_idx != -1:
                    # CRITICAL: dtype=str prevents the ID rounding you saw in BigQuery
                    df_raw = pd.read_csv(
                        io.StringIO("\n".join(raw_bytes[header_idx:])), 
                        low_memory=False, 
                        dtype=str 
                    )
                    df_raw = df_raw.dropna(how='all')

                    # Process Narrow Format (SensorId/Observed columns)
                    if "SensorId" in df_raw.columns:
                        ts_col = "Observed" if "Observed" in df_raw.columns else df_raw.columns[1]
                        
                        df_up = pd.DataFrame()
                        df_up['sensor_id'] = df_raw['SensorId'].astype(str).str.strip()
                        df_up['timestamp'] = pd.to_datetime(df_raw[ts_col], format='mixed', errors='coerce')
                        
                        # Handle Temperature or Thermocouple columns
                        t_cols = [c for c in df_raw.columns if "Temperature" in c or "Thermocouple" in c]
                        df_up['temperature'] = pd.to_numeric(df_raw[t_cols].bfill(axis=1).iloc[:, 0], errors='coerce')
                        
                        df_up = df_up.dropna(subset=['timestamp', 'temperature'])

                        st.write(f"✅ Parsed {len(df_up)} readings from {df_up['sensor_id'].nunique()} sensors.")
                        st.dataframe(df_up.head())

                        if st.button("🚀 UPLOAD TO RAW & REBUILD"):
                            with st.spinner("Pushing to BigQuery..."):
                                client.load_table_from_dataframe(df_up, f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush").result()
                                # Trigger rebuild using NodeNum matching
                                if rebuild_master_table(mode="preserve"):
                                    st.success("✅ Data integrated. Check Diagnostics for new graphs.")
                                    st.balloons()
                else:
                    st.error("Header row not found. Ensure 'SensorId' is present in your CSV.")
            except Exception as e: 
                st.error(f"Upload Error: {e}")

    with tab2:
        st.subheader("📡 Cloud-to-Cloud API Sync")
        c1, c2 = st.columns(2)
        start_date = c1.date_input("Start Date", datetime.now() - timedelta(days=7))
        end_date = c2.date_input("End Date", datetime.now())
        
        if st.button("🛰️ FETCH & FULL SYNC"):
            start_dt = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=pytz.UTC)
            end_dt = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=pytz.UTC)
            
            with st.spinner("Fetching from SensorPush API..."):
                df_api = fetch_sensorpush_data(start_dt, end_dt)
                if not df_api.empty:
                    df_api['sensor_id'] = df_api['sensor_id'].astype(str).str.replace(':', '-', regex=False)
                    client.load_table_from_dataframe(df_api, f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush").result()
                    if rebuild_master_table(mode="preserve"):
                        st.success(f"✅ API Sync Complete: {len(df_api)} points integrated.")
                else:
                    st.warning("No data found for this range.")

    with tab3:
        st.subheader("🛠️ Database Maintenance")
        
        # 1. Diagnostic Audit
        if st.button("🔍 RUN DAILY DATA AUDIT"):
            audit_sql = f"""
                WITH MasterCounts AS (
                    SELECT DATE(timestamp) as audit_date, project, COUNT(*) as master_points
                    FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master`
                    WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
                    GROUP BY audit_date, project
                ),
                RawPush AS (
                    SELECT DATE(r.timestamp) as audit_date, m.Project, COUNT(*) as raw_points
                    FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` r
                    INNER JOIN `{PROJECT_ID}.{DATASET_ID}.master_metadata` m 
                        ON (SUBSTR(TRIM(r.sensor_id), 1, 12) = SUBSTR(TRIM(CAST(m.PhysicalID AS STRING)), 1, 12))
                        OR (TRIM(r.sensor_id) = TRIM(CAST(m.NodeNum AS STRING)))
                    WHERE r.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
                    GROUP BY audit_date, m.Project
                )
                SELECT COALESCE(m.audit_date, r.audit_date) as Date, COALESCE(m.project, r.Project) as Project, 
                       COALESCE(r.raw_points, 0) as Raw_Points, COALESCE(m.master_points, 0) as Master_Points
                FROM MasterCounts m FULL OUTER JOIN RawPush r ON m.project = r.Project AND m.audit_date = r.audit_date
                ORDER BY Date DESC
            """
            try:
                df_audit = client.query(audit_sql).to_dataframe()
                st.dataframe(df_audit, use_container_width=True, hide_index=True)
            except Exception as e:
                st.error(f"Audit Error: {e}")

        st.divider()
        
        # 2. Rebuild Controls
        st.subheader("Master Table Controls")
        col_m1, col_m2 = st.columns(2)
        
        with col_m1:
            if st.button("🔄 FORCE MASTER REBUILD"):
                with st.spinner("Re-mapping Physical IDs and cleaning data..."):
                    if rebuild_master_table(mode="preserve"):
                        st.success("✅ Master Table Refreshed! Dropdowns and Graphs should now appear.")
                        st.balloons()
        
        with col_m2:
            if st.button("🚩 MARK ALL AS HISTORIC"):
                with st.spinner("Approving all data..."):
                    if rebuild_master_table(mode="approve_all"):
                        st.success("✅ All data marked as Approved.")
###############################
# --- END DATA INTAKE LAB --- #
###############################
#######################
# --- ADMIN TOOLS --- #
#######################             
# --- 4E. ADMIN TOOLS (CLEAN INDENTATION) ---
elif service == "🛠️ Admin Tools":
    st.header("🛠️ Engineering Admin Tools")
    tab_scrub, tab_approve = st.tabs(["🧹 Data Scrubber", "✅ Bulk Approval"])
    
    with tab_scrub:
        sc_proj = st.text_input("Project Name", key="scrub_p")
        sc_loc = st.text_input("Location / Pipe", key="scrub_l")
        if st.button("🗑️ DELETE POINTS"):
            if sc_proj and sc_loc:
                scrub_q = f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE project='{sc_proj}' AND location='{sc_loc}'"
                client.query(scrub_q).result()
                st.success(f"Deleted {sc_loc} data.")

    with tab_approve:
        try:
            unapproved_meta_q = f"SELECT DISTINCT project, location FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE (is_approved IS FALSE OR is_approved IS NULL)"
            un_meta = client.query(unapproved_meta_q).to_dataframe()
            if not un_meta.empty:
                app_proj = st.selectbox("Project", un_meta['project'].unique(), key="app_p")
                app_loc = st.selectbox("Location", un_meta[un_meta['project'] == app_proj]['location'].unique(), key="app_l")
                if st.button("🚀 APPROVE NOW"):
                    client.query(f"UPDATE `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` SET is_approved = TRUE WHERE project='{app_proj}' AND location='{app_loc}'").result()
                    st.success("Approved!")
        except Exception as e: 
            st.error(f"Approval Error: {e}")
###########################
# --- END ADMIN TOOLS --- #
########################### 
