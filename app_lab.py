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

# --- 2. GRAPH ENGINE ---
# --- 2. STANDARDIZED GRAPH ENGINE (RESTORED) ---
def build_standard_sf_graph(df, title, start_view, end_view, unit="Fahrenheit", active_refs=None):
    """Standard SF look: 20° heavy grid, 5° minor grid, Monday/6hr vertical markers."""
    if active_refs is None: active_refs = []
    display_df = df.copy()
    
    # Unit & Grid Config
    if unit == "Celsius":
        display_df['temperature'] = (display_df['temperature'] - 32) * 5/9
        y_range, y_ticks, y_label, m_step = [-30, 30], [-30, -20, -10, 0, 10, 20, 30], "Temp (°C)", 2.5
    else:
        y_range, y_ticks, y_label, m_step = [-20, 80], [-20, 0, 20, 40, 60, 80], "Temp (°F)", 5

    # Gap Logic (Line breaks > 6hrs)
    processed_dfs = []
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
    
    fig = go.Figure()
    depths = sorted(clean_df['depth'].unique(), key=lambda x: int(''.join(filter(str.isdigit, x)) or 0))
    for d in depths:
        sensor_df = clean_df[clean_df['depth'] == d]
        fig.add_trace(go.Scatter(
            x=sensor_df['timestamp'], y=sensor_df['temperature'],
            name=d, mode='lines', fill=None, connectgaps=False, line=dict(width=2)
        ))

    # Grid & Axis Styling (The "Standard" Look)
    fig.update_yaxes(title=y_label, tickmode='array', tickvals=y_ticks, range=y_range,
                     gridcolor='DimGray', gridwidth=1.5, minor=dict(dtick=m_step, gridcolor='Silver', showgrid=True),
                     mirror=True, showline=True, linecolor='black', linewidth=2)
    fig.update_xaxes(showgrid=False, range=[start_view, end_view], mirror=True, showline=True, linecolor='black', linewidth=2)

    # Custom Vertical Grid (Monday marks)
    shapes = []
    curr = start_view.replace(hour=0, minute=0, second=0, microsecond=0)
    while curr <= end_view:
        for h in [0, 6, 12, 18]:
            t = curr + timedelta(hours=h)
            if t < start_view or t > end_view: continue
            t_ms = t.timestamp() * 1000
            if t.weekday() == 0 and h == 0: c, w = "DimGray", 2
            elif h == 0: c, w = "DarkGray", 1
            else: c, w = "LightGray", 0.5
            shapes.append(dict(type="line", xref="x", yref="paper", x0=t_ms, y0=0, x1=t_ms, y1=1, 
                               line=dict(color=c, width=w), layer="below"))
        curr += timedelta(days=1)

    now_ms = datetime.now(pytz.UTC).timestamp() * 1000
    fig.add_vline(x=now_ms, line_width=2, line_color="red", annotation_text="RIGHT NOW")
    
    fig.update_layout(title={'text': title, 'x': 0.5}, shapes=shapes, plot_bgcolor='white',
                      hovermode="x unified", legend=dict(x=1.02, y=1, bordercolor="Black", borderwidth=1), 
                      margin=dict(r=150), height=750)
    return fig

# --- 3. SIDEBAR NAVIGATION ---
st.sidebar.title("❄️ SoilFreeze Lab")
service = st.sidebar.selectbox("Select Service", [
    "🏠 Executive Summary", 
    "📊 Client Portal",
    "📉 Node Diagnostics", 
    "📤 Data Intake Lab",
    "🛠️ Admin Tools"
])

# --- 4. SERVICE ROUTING ---

# 4A. EXECUTIVE SUMMARY
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

# 4B. CLIENT PORTAL
# --- 4B. CLIENT PORTAL ---
# --- 4B. CLIENT PORTAL ---
elif service == "📊 Client Portal":
    st.header("📊 Project Status Report")
    try:
        meta_q = f"SELECT DISTINCT project, location FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE is_approved = TRUE"
        meta_df = client.query(meta_q).to_dataframe()
        
        if meta_df.empty:
            st.warning("No approved data available.")
        else:
            c1, c2 = st.columns(2)
            with c1: sel_proj = st.selectbox("Project", sorted(meta_df['project'].dropna().unique()))
            with c2: 
                locs = sorted(meta_df[meta_df['project'] == sel_proj]['location'].dropna().unique())
                sel_loc = st.selectbox("Pipe / Bank", locs)
            
            data_q = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE project = '{sel_proj}' AND location = '{sel_loc}' AND is_approved = TRUE ORDER BY timestamp ASC"
            df_c = client.query(data_q).to_dataframe()
            df_c['timestamp'] = pd.to_datetime(df_c['timestamp'])

            # --- NEW LOGIC: Only show Depth Profile if NOT a Bank ---
            if "bank" not in sel_loc.lower():
                st.subheader("🌡️ Soil Temperature Profile (Weekly Snapshots)")
                snapshot = df_c[(df_c['timestamp'].dt.weekday == 0) & (df_c['timestamp'].dt.hour == 6)].copy()
                
                if not snapshot.empty:
                    snapshot['depth_num'] = snapshot['depth'].str.extract('(\d+)').astype(float)
                    snapshot['Date'] = snapshot['timestamp'].dt.strftime('%Y-%m-%d')
                    
                    fig_profile = px.line(
                        snapshot.sort_values('depth_num'), 
                        x='temperature', 
                        y='depth_num', 
                        color='Date',
                        markers=True,
                        title="Temperature by Depth (Monday 6:00 AM)",
                        # Fixed X-Axis Range: -20 to 80
                        range_x=[-20, 80],
                        labels={'temperature': 'Temperature (°F)', 'depth_num': 'Depth (ft)'}
                    )
                    fig_profile.update_yaxes(autorange="reversed")
                    fig_profile.add_vline(x=32, line_dash="dash", line_color="blue", annotation_text="Freezing (32°F)")
                    st.plotly_chart(fig_profile, width='stretch')
                else:
                    st.info("No Monday 6:00 AM data points found.")
            else:
                st.info("ℹ️ Depth profiles are disabled for individual Bank sensor readings.")

            # --- HISTORICAL TREND (Already limited to -20 to 80 via build_standard_sf_graph) ---
            st.subheader("📈 Historical Trends")
            st.plotly_chart(build_standard_sf_graph(df_c, f"Timeline: {sel_loc}", df_c['timestamp'].min(), df_c['timestamp'].max()), width='stretch')
            
            # --- LATEST READINGS ---
            st.subheader("⏱️ Most Recent Readings")
            latest = df_c.sort_values('timestamp').groupby('depth').tail(1).copy()
            st.dataframe(latest[['depth', 'temperature', 'timestamp']].style.format({'temperature': '{:.1f}', 'timestamp': '{:%m/%d %H:%M}'}), width='stretch', hide_index=True)

            latest_note = df_c.sort_values('timestamp', ascending=False)['engineer_note'].dropna()
            if not latest_note.empty and latest_note.iloc[0]:
                st.info(f"**Field Engineer Note:** {latest_note.iloc[0]}")

    except Exception as e: st.error(f"Portal Error: {e}")

# 4C. NODE DIAGNOSTICS
elif service == "📉 Node Diagnostics":
    st.header("📉 High-Resolution Node Diagnostics")
    try:
        meta_q = f"SELECT DISTINCT project, location FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE project IS NOT NULL"
        meta_df = client.query(meta_q).to_dataframe()
        c1, c2, c3 = st.columns(3)
        with c1: sel_proj = st.selectbox("Project", sorted(meta_df['project'].unique()))
        with c2: 
            locs = sorted(meta_df[meta_df['project'] == sel_proj]['location'].unique())
            sel_loc = st.selectbox("Pipe / Bank", locs)
        with c3: weeks = st.slider("Lookback (Weeks)", 1, 12, 4)

        days_back = weeks * 7
        data_q = f"SELECT timestamp, temperature, depth, sensor_name FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE project = '{sel_proj}' AND location = '{sel_loc}' AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY) ORDER BY timestamp ASC"
        df_g = client.query(data_q).to_dataframe()
        
        if not df_g.empty:
            df_g['timestamp'] = pd.to_datetime(df_g['timestamp'])
            end_v = datetime.now(pytz.UTC)
            start_v = end_v - timedelta(days=days_back)
            st.plotly_chart(build_standard_sf_graph(df_g, f"Trend: {sel_proj} | {sel_loc}", start_v, end_v), width='stretch')
    except Exception as e: st.error(f"Diagnostics Error: {e}")

# 4D. DATA INTAKE
elif service == "📤 Data Intake Lab":
    st.header("📤 Data Ingestion & Recovery")
    tab1, tab2 = st.tabs(["📄 Manual File Upload", "📡 API Data Recovery"])

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
                
                df_up['timestamp'] = pd.to_datetime(df_up['timestamp'])
                if st.button("🚀 PUSH FILE TO BIGQUERY"):
                    client.load_table_from_dataframe(df_up, table_ref).result()
                    st.success("File uploaded to Raw storage.")
             except Exception as e: st.error(f"Upload error: {e}")

    with tab2:
        st.subheader("SensorPush Multi-Account Recovery")
        c1, c2 = st.columns(2)
        with c1:
            sd = st.date_input("Recovery Start", datetime.now() - timedelta(days=2))
            st_time = st.time_input("Start Time (UTC)", datetime.strptime("00:00", "%H:%M").time())
        with c2:
            ed = st.date_input("Recovery End", datetime.now())
            et_time = st.time_input("End Time (UTC)", datetime.now().time())

        if st.button("🛰️ RUN ALL-ACCOUNT RECOVERY"):
            s_iso = datetime.combine(sd, st_time).strftime("%Y-%m-%dT%H:%M:%S.000Z")
            e_iso = datetime.combine(ed, et_time).strftime("%Y-%m-%dT%H:%M:%S.000Z")
            if "sensorpush_accounts" not in st.secrets:
                st.error("Missing accounts in Secrets.")
            else:
                accounts = st.secrets["sensorpush_accounts"]
                all_api_recs = []
                for acc_id, creds in accounts.items():
                    try:
                        with st.spinner(f"Processing {acc_id}..."):
                            auth_res = requests.post("https://api.sensorpush.com/api/v1/oauth/authorize", json=dict(creds), headers={"accept": "application/json"})
                            if auth_res.status_code == 200:
                                token = auth_res.json().get("accesstoken")
                                h = {"accept": "application/json", "Authorization": token, "Content-Type": "application/json"}
                                p = {"startTime": s_iso, "endTime": e_iso, "measures": ["temperature"]}
                                sample_res = requests.post("https://api.sensorpush.com/api/v1/samples", headers=h, json=p)
                                if sample_res.status_code == 200:
                                    raw_json = sample_res.json()
                                    for sid, samples in raw_json.get("sensors", {}).items():
                                        for s in samples:
                                            all_api_recs.append({"timestamp": s["observed"], "temperature": s["value"], "sensor_id": sid.replace(':', '-')})
                    except Exception as e: st.error(f"Failure on {acc_id}: {e}")
                
                if all_api_recs:
                    df_api = pd.DataFrame(all_api_recs)
                    df_api['timestamp'] = pd.to_datetime(df_api['timestamp'])
                    client.load_table_from_dataframe(df_api, f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush").result()
                    st.success(f"✅ Pulled {len(all_api_recs)} points.")
                    st.balloons()

# 4E. ADMIN TOOLS
# --- 4E. ADMIN TOOLS (BULK APPROVAL & SCRUBBER) ---
# --- 4E. ADMIN TOOLS (BULK APPROVAL & SCRUBBER) ---
elif service == "🛠️ Admin Tools":
    st.header("🛠️ Engineering Admin Tools")
    tab_scrub, tab_approve = st.tabs(["🧹 Data Scrubber", "✅ Bulk Approval"])
    
    with tab_scrub:
        st.subheader("Delete Bad Data")
        sc_proj = st.text_input("Project Name", key="scrub_p")
        sc_loc = st.text_input("Location / Pipe", key="scrub_l")
        col1, col2 = st.columns(2)
        with col1:
            sc_start = st.text_input("Start (YYYY-MM-DD HH:MM:SS)", value="2026-01-01 00:00:00")
        with col2:
            sc_end = st.text_input("End (YYYY-MM-DD HH:MM:SS)", value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            
        if st.button("🗑️ PERMANENTLY DELETE POINTS"):
            if sc_proj and sc_loc:
                scrub_q = f"""
                    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` 
                    WHERE project='{sc_proj}' AND location='{sc_loc}' 
                    AND timestamp BETWEEN '{sc_start}' AND '{sc_end}'
                """
                client.query(scrub_q).result()
                st.success(f"✅ Deleted points for {sc_loc} in {sc_proj}")
            else:
                st.error("Please enter both Project and Location.")

    with tab_approve:
        st.subheader("Bulk Approve Data")
        st.write("Set `is_approved = TRUE` for a specific site and time range.")
        
        try:
            # 1. Get metadata, filtering out Nulls to prevent the '<' error
            unapproved_meta_q = f"""
                SELECT DISTINCT project, location 
                FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` 
                WHERE (is_approved IS FALSE OR is_approved IS NULL)
                AND project IS NOT NULL 
                AND location IS NOT NULL
            """
            un_meta = client.query(unapproved_meta_q).to_dataframe()
            
            if un_meta.empty:
                st.success("🎉 All data is currently approved!")
            else:
                # Use dropna and sorted to safely handle the dropdowns
                u_projs = sorted(un_meta['project'].dropna().unique())
                app_proj = st.selectbox("Select Project", u_projs)
                
                u_locs = sorted(un_meta[un_meta['project'] == app_proj]['location'].dropna().unique())
                app_loc = st.selectbox("Select Location/Pipe", u_locs)
                
                app_note = st.text_area("Engineer Note", placeholder="Verified data trends...")
                
                c1, c2 = st.columns(2)
                with c1:
                    t_start = st.text_input("Start Time", value="2026-01-01 00:00:00", key="app_start")
                with c2:
                    t_end = st.text_input("End Time", value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), key="app_end")

                # 2. Preview Count
                if st.button("🔍 PREVIEW COUNT"):
                    count_q = f"""
                        SELECT COUNT(*) as total FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master`
                        WHERE project='{app_proj}' AND location='{app_loc}'
                        AND timestamp BETWEEN '{t_start}' AND '{t_end}'
                        AND (is_approved IS FALSE OR is_approved IS NULL)
                    """
                    count_res = client.query(count_q).to_dataframe()
                    st.info(f"This will approve **{count_res['total'].iloc[0]}** data points.")

                # 3. Final Approval
                if st.button("🚀 BULK APPROVE NOW"):
                    bulk_q = f"""
                        UPDATE `{PROJECT_ID}.{DATASET_ID}.final_databoard_master`
                        SET is_approved = TRUE, engineer_note = '{app_note}'
                        WHERE project = '{app_proj}' AND location = '{app_loc}'
                        AND timestamp BETWEEN '{t_start}' AND '{t_end}'
                    """
                    client.query(bulk_q).result()
                    st.success(f"✅ Approved data for {app_loc}!")
                    st.balloons()
                    
        except Exception as e:
            st.error(f"Approval Error: {e}")
