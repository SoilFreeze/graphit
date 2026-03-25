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

# --- 1. CONFIGURATION & STYLING ---
st.set_page_config(page_title="SoilFreeze Data Lab", layout="wide")

DATASET_ID = "sensor_data" 
PROJECT_ID = "sensorpush-export"

@st.cache_resource
def get_bq_client():
    try:
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(info)
            return bigquery.Client(credentials=credentials, project=info["project_id"])
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"Authentication Failed: {e}")
        return None

client = get_bq_client()

# --- 1. RESTORED GRAPH ENGINE (STABLE) ---
# --- 1. CLEANED GRAPH ENGINE (STOPS THE OPERAND ERROR) ---
# --- 2. STANDARDIZED GRAPH ENGINE ---
def build_standard_sf_graph(df, title, start_view, end_view, unit="Fahrenheit", active_refs=None):
    """Restored: 6hr gaps, Custom Grid, and 'Right Now' line. Fixed for Timezone math."""
    if active_refs is None: active_refs = []
    display_df = df.copy()
    
    # 1. Unit Conversion
    if unit == "Celsius":
        display_df['temperature'] = (display_df['temperature'] - 32) * 5/9
        y_range, y_ticks, y_label, m_step = [-30, 30], [-30, -20, -10, 0, 10, 20, 30], "Temp (°C)", 2.5
    else:
        y_range, y_ticks, y_label, m_step = [-20, 80], [-20, 0, 20, 40, 60, 80], "Temp (°F)", 5

    # 2. Gap Logic (Line breaks > 6hrs)
    processed_dfs = []
    # Using 'depth' as the identifier
    for d in display_df['depth'].unique():
        s_df = display_df[display_df['depth'] == d].copy().sort_values('timestamp')
        s_df['gap'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
        gaps = s_df[s_df['gap'] > 6.0].copy()
        if not gaps.empty:
            gaps['temperature'] = None
            gaps['timestamp'] = gaps['timestamp'] - timedelta(minutes=1)
            s_df = pd.concat([s_df, gaps]).sort_values('timestamp')
        processed_dfs.append(s_df)
    
    clean_df = pd.concat(processed_dfs) if processed_dfs else display_df
    
    # 3. Trace Creation
    fig = go.Figure()
    depths = sorted(clean_df['depth'].unique(), key=lambda x: int(''.join(filter(str.isdigit, x)) or 0))
    for d in depths:
        sensor_df = clean_df[clean_df['depth'] == d]
        fig.add_trace(go.Scatter(
            x=sensor_df['timestamp'], y=sensor_df['temperature'],
            name=d, mode='lines', fill=None, connectgaps=False, line=dict(width=2)
        ))

    # 4. Grid & Axis Styling
    fig.update_yaxes(title=y_label, tickmode='array', tickvals=y_ticks, range=y_range,
                     gridcolor='DimGray', gridwidth=1.5, minor=dict(dtick=m_step, gridcolor='Silver', showgrid=True),
                     mirror=True, showline=True, linecolor='black', linewidth=2)
    fig.update_xaxes(showgrid=False, range=[start_view, end_view], mirror=True, showline=True, linecolor='black', linewidth=2)

    # 5. FIXED Custom Vertical Grid (Handles the datetime crash)
    shapes = []
    # Ensure curr is timezone aware to match start_view
    curr = start_view.replace(hour=0, minute=0, second=0, microsecond=0)
    
    while curr <= end_view:
        for h in [0, 6, 12, 18]:
            t = curr + timedelta(hours=h)
            if t < start_view or t > end_view: continue
            
            # Convert to milliseconds for Plotly shapes
            t_ms = t.timestamp() * 1000
            
            # Formatting for Monday Midnights vs Regular Midnights vs 6hr marks
            if t.weekday() == 0 and h == 0:
                c, w = "DimGray", 2  # Monday Midnight
            elif h == 0:
                c, w = "DarkGray", 1 # Regular Midnight
            else:
                c, w = "LightGray", 0.5 # 6hr increments
                
            shapes.append(dict(type="line", xref="x", yref="paper", x0=t_ms, y0=0, x1=t_ms, y1=1, 
                               line=dict(color=c, width=w), layer="below"))
        curr += timedelta(days=1)

    # 6. NOW Line & Reference Lines
    now_ms = datetime.now(pytz.UTC).timestamp() * 1000
    fig.add_vline(x=now_ms, line_width=2, line_color="red", annotation_text="RIGHT NOW")
    
    for ref_f, label in active_refs:
        val = (ref_f - 32) * 5/9 if unit == "Celsius" else ref_f
        fig.add_hline(y=val, line_dash="dash", line_color="blue", annotation_text=f"{label} {round(val,1)}°")

    fig.update_layout(title={'text': title, 'x': 0.5}, shapes=shapes, plot_bgcolor='white',
                      hovermode="x unified", legend=dict(x=1.02, y=1, bordercolor="Black", borderwidth=1), 
                      margin=dict(r=150), height=750)
    return fig

# --- 3. SIDEBAR NAVIGATION ---
st.sidebar.title("❄️ SoilFreeze Lab")
service = st.sidebar.selectbox("Select Service", ["🏠 Executive Summary", "📉 Node Diagnostics", "📤 Data Intake Lab"])

# --- 4. SERVICE ROUTING ---

if service == "🏠 Executive Summary":
    st.header("🏠 Site Health & Warming Alerts")
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
                hrs = (now_ts - n_df['timestamp'].iloc[-1]).total_seconds() / 3600
                summary_stats.append({
                    "Location": n_df['location'].iloc[0], "Depth": f"{n_df['depth'].iloc[0]}ft", "Node ID": node,
                    "Status": f"{n_df['timestamp'].iloc[-1].strftime('%m/%d %H:%M')} ({int(round(hrs, 0))}h ago)",
                    "Change": round(float(chg), 1), "Current": round(float(curr_t), 1)
                })
            st.dataframe(pd.DataFrame(summary_stats), use_container_width=True, hide_index=True)
    except Exception as e: st.error(f"Summary Error: {e}")

# --- 2. NODE DIAGNOSTICS SERVICE ---
# --- 2. NODE DIAGNOSTICS BLOCK ---
elif service == "📉 Node Diagnostics":
    st.header("📉 High-Resolution Node Diagnostics")
    try:
        # Load Filters
        meta_q = f"SELECT DISTINCT project, location FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE project IS NOT NULL"
        meta_df = client.query(meta_q).to_dataframe()
        
        c1, c2, c3 = st.columns(3)
        with c1: sel_proj = st.selectbox("Project", sorted(meta_df['project'].unique()))
        with c2: 
            locs = sorted(meta_df[meta_df['project'] == sel_proj]['location'].unique())
            sel_loc = st.selectbox("Pipe / Bank", locs)
        with c3: weeks = st.slider("Lookback (Weeks)", 1, 12, 4)

        # Pull Data
        days_back = weeks * 7
        data_q = f"""
            SELECT timestamp, temperature, depth, sensor_name FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master`
            WHERE project = '{sel_proj}' AND location = '{sel_loc}'
            AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
            ORDER BY timestamp ASC
        """
        df_g = client.query(data_q).to_dataframe()
        
        if not df_g.empty:
            df_g['timestamp'] = pd.to_datetime(df_g['timestamp'])
            end_v = datetime.now(pytz.UTC)
            start_v = end_v - timedelta(days=days_back)
            
            # Display Graph
            st.plotly_chart(build_standard_sf_graph(df_g, f"Trend: {sel_proj} | {sel_loc}", start_v, end_v), use_container_width=True)
            
            # 3. Restored COLOR-CODED TABLE
            st.subheader("Current Status & 24h Change")
            
            # Calculate 24h Change for the table
            latest = df_g.sort_values('timestamp').groupby('depth').tail(1).copy()
            earliest_24 = df_g[df_g['timestamp'] >= (end_v - timedelta(hours=24))].sort_values('timestamp').groupby('depth').head(1)
            
            # Merge to find change
            latest = latest.merge(earliest_24[['depth', 'temperature']], on='depth', suffixes=('', '_24h'))
            latest['24h_change'] = latest['temperature'] - latest['temperature_24h']
            latest['hrs_ago'] = (end_v - latest['timestamp']).dt.total_seconds() / 3600

            def apply_diag_styles(row):
                styles = [''] * len(row)
                # Status Color (Column 3)
                if row['hrs_ago'] >= 24: styles[3] = 'background-color: #ff4b4b; color: white'
                elif row['hrs_ago'] >= 12: styles[3] = 'background-color: #ffa500; color: black'
                
                # Change Color (Column 4)
                if row['24h_change'] >= 2.0: styles[4] = 'background-color: #ff4b4b; color: white'
                elif row['24h_change'] >= 0.5: styles[4] = 'background-color: #ffff00; color: black'
                elif row['24h_change'] <= -1.0: styles[4] = 'background-color: #00008b; color: white'
                return styles

            # Final Table Display
            st.dataframe(
                latest[['depth', 'sensor_name', 'temperature', 'timestamp', '24h_change', 'hrs_ago']].style.apply(apply_diag_styles, axis=1).format({
                    'temperature': '{:.1f}', 'timestamp': '{:%m/%d %H:%M}', '24h_change': '{:+.1f}', 'hrs_ago': '{:.0f}h ago'
                }),
                use_container_width=True, hide_index=True
            )
    except Exception as e:
        st.error(f"Diagnostics Error: {e}")

elif service == "📤 Data Intake Lab":
    st.header("📤 Data Ingestion & Recovery")
    
    tab1, tab2 = st.tabs(["📄 Manual File Upload", "📡 API Data Recovery"])

    # --- TAB 1: MANUAL FILE ---
    with tab1:
        st.subheader("Manual CSV Ingestion")
        source = st.radio("Device Type", ["SensorPush (CSV)", "Lord (SensorConnect)"], horizontal=True)
        u_file = st.file_uploader("Upload Logger File", type=['csv'], key="manual_upload")

        if u_file is not None:
            try:
                content = u_file.getvalue().decode("utf-8").splitlines()
                if "Lord" in source:
                    start_idx = next((i for i, l in enumerate(content) if "DATA_START" in l), 0)
                    u_file.seek(0)
                    df_raw = pd.read_csv(u_file, skiprows=start_idx + 1)
                    df_up = df_raw.melt(id_vars=[df_raw.columns[0]], var_name='nodenumber', value_name='temperature')
                    df_up = df_up.rename(columns={df_raw.columns[0]: 'timestamp'})
                    table_ref = f"{PROJECT_ID}.{DATASET_ID}.raw_lord"
                else:
                    df_up = pd.read_csv(u_file).rename(columns={'Timestamp':'timestamp', 'Temperature':'temperature', 'Sensor':'sensor_id'})
                    table_ref = f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush"
                
                # Standardize IDs
                id_col = 'sensor_id' if 'sensor_id' in df_up.columns else 'nodenumber'
                df_up[id_col] = df_up[id_col].astype(str).str.replace(':', '-', regex=False)
                df_up['timestamp'] = pd.to_datetime(df_up['timestamp'])

                if st.button("🚀 PUSH FILE TO BIGQUERY"):
                    client.load_table_from_dataframe(df_up, table_ref).result()
                    # Unified Scrub Logic
                    scrub_sql = f"""
                        CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` AS 
                        WITH Unified AS (
                            SELECT CAST(timestamp AS TIMESTAMP) as timestamp, value as temperature, REPLACE(nodenumber, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord` 
                            UNION ALL 
                            SELECT CAST(timestamp AS TIMESTAMP) as timestamp, temperature, REPLACE(sensor_id, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
                        ) 
                        SELECT u.*, u.node AS sensor_id, m.SensorName as sensor_name, m.Project as project, m.Location as location, m.Depth as depth 
                        FROM Unified u INNER JOIN `{PROJECT_ID}.{DATASET_ID}.master_metadata` m ON u.node = REPLACE(m.NodeNum, ':', '-')
                    """
                    client.query(scrub_sql).result()
                    st.success("✅ Uploaded and Master Table Synced!")
            except Exception as e:
                st.error(f"File Error: {e}")

    # --- TAB 2: API RECOVERY (FIXED 400 ERROR) ---
    with tab2:
        st.subheader("SensorPush Cloud Recovery")
        c1, c2 = st.columns(2)
        with c1:
            sd = st.date_input("Recovery Start", datetime.now() - timedelta(days=2))
            st_time = st.time_input("Start Time (UTC)", datetime.strptime("00:00", "%H:%M").time())
        with c2:
            ed = st.date_input("Recovery End", datetime.now())
            et_time = st.time_input("End Time (UTC)", datetime.now().time())

        # ISO 8601 Strings for API
        s_iso = datetime.combine(sd, st_time).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        e_iso = datetime.combine(ed, et_time).strftime("%Y-%m-%dT%H:%M:%S.000Z")

        if st.button("🛰️ RUN CLOUD RECOVERY"):
            if "sensorpush" not in st.secrets:
                st.error("Missing credentials.")
            else:
                try:
                    creds = st.secrets["sensorpush"]
                    # 1. Authorize
                    auth_res = requests.post("https://api.sensorpush.com/api/v1/oauth/authorize", 
                                             json={"email": creds["email"], "password": creds["password"]})
                    auth_res.raise_for_status()
                    token = auth_res.json().get("accesstoken")

                    # 2. Fetch Samples (Corrected Payload)
                    headers = {"accept": "application/json", "Authorization": token}
                    payload = {
                        "startTime": s_iso,
                        "endTime": e_iso,
                        "measures": ["temperature"] # Ensure lowercase
                    }
                    
                    with st.spinner("Requesting data from SensorPush..."):
                        sample_res = requests.post("https://api.sensorpush.com/api/v1/samples", headers=headers, json=payload)
                        # This captures why the 400 is happening if it fails again
                        if sample_res.status_code != 200:
                            st.error(f"API Error {sample_res.status_code}: {sample_res.text}")
                            st.stop()
                        
                        raw_json = sample_res.json()

                    # 3. Process and Push
                    api_recs = []
                    for sid, samples in raw_json.get("sensors", {}).items():
                        for s in samples:
                            api_recs.append({
                                "timestamp": s["observed"],
                                "temperature": s["value"],
                                "sensor_id": sid.replace(':', '-')
                            })
                    
                    if api_recs:
                        df_api = pd.DataFrame(api_recs)
                        df_api['timestamp'] = pd.to_datetime(df_api['timestamp'])
                        
                        # Load to BQ
                        client.load_table_from_dataframe(df_api, f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush").result()
                        
                        # Trigger Master Scrub
                        scrub_sql = f"""
                            CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` AS 
                            WITH Unified AS (
                                SELECT CAST(timestamp AS TIMESTAMP) as timestamp, value as temperature, REPLACE(nodenumber, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord` 
                                UNION ALL 
                                SELECT CAST(timestamp AS TIMESTAMP) as timestamp, temperature, REPLACE(sensor_id, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
                            ) 
                            SELECT u.*, u.node AS sensor_id, m.SensorName as sensor_name, m.Project as project, m.Location as location, m.Depth as depth 
                            FROM Unified u INNER JOIN `{PROJECT_ID}.{DATASET_ID}.master_metadata` m ON u.node = REPLACE(m.NodeNum, ':', '-')
                        """
                        client.query(scrub_sql).result()
                        st.success(f"✅ Success! Pulled {len(api_recs)} points.")
                        st.balloons()
                    else:
                        st.warning("No data found for this window. Check if the Gateway is online.")
                except Exception as e:
                    st.error(f"Recovery Failed: {e}")
