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
#############################################
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
###############################
# --- 3. SIDEBAR NAVIGATION ---
# --- 3. SIDEBAR NAVIGATION ---
st.sidebar.title("❄️ SoilFreeze Lab")
# Added "📊 Client Portal" and "🛠️ Admin Tools" to the list
service = st.sidebar.selectbox("Select Service", [
    "🏠 Executive Summary", 
    "📊 Client Portal",
    "📉 Node Diagnostics", 
    "📤 Data Intake Lab",
    "🛠️ Admin Tools"
])

# --- 4. SERVICE ROUTING ---

# SERVICE 1: EXECUTIVE SUMMARY
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

# SERVICE 2: CLIENT PORTAL (NEW)
elif service == "📊 Client Portal":
    st.header("📊 Project Status Report")
    try:
        # Only show approved data for clients
        meta_q = f"SELECT DISTINCT project, location FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE is_approved = TRUE"
        meta_df = client.query(meta_q).to_dataframe()
        
        if meta_df.empty:
            st.warning("No approved data available for display yet.")
        else:
            c1, c2 = st.columns(2)
            with c1: sel_proj = st.selectbox("Project", sorted(meta_df['project'].unique()))
            with c2: 
                locs = sorted(meta_df[meta_df['project'] == sel_proj]['location'].unique())
                sel_loc = st.selectbox("Pipe / Bank", locs)
            
            # Pull Data
            data_q = f"""
                SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master`
                WHERE project = '{sel_proj}' AND location = '{sel_loc}' AND is_approved = TRUE
                ORDER BY timestamp ASC
            """
            df_c = client.query(data_q).to_dataframe()
            df_c['timestamp'] = pd.to_datetime(df_c['timestamp'])
            
            # A. Weekly Snapshot (Monday 6AM)
            st.subheader("🗓️ Weekly Monday 6:00 AM Snapshot")
            # Filter for Monday (weekday 0) at 6 AM
            snapshot = df_c[(df_c['timestamp'].dt.weekday == 0) & (df_c['timestamp'].dt.hour == 6)].copy()
            if not snapshot.empty:
                fig_snap = px.bar(snapshot, x='timestamp', y='temperature', color='depth', 
                                  title="Depth Temperatures over Time (Monday Snapshots)",
                                  barmode='group', labels={'temperature': 'Temp (°F)', 'timestamp': 'Week Starting'})
                st.plotly_chart(fig_snap, use_container_width=True)
            else:
                st.info("Not enough Monday 6:00 AM data points for a snapshot yet.")

            # B. Standard Trend Graph
            st.subheader("📈 Temperature Trends")
            st.plotly_chart(build_standard_sf_graph(df_c, f"Site Trend: {sel_loc}", df_c['timestamp'].min(), df_c['timestamp'].max()), use_container_width=True)
            
            # C. Engineer Note
            latest_note = df_c.sort_values('timestamp', ascending=False)['engineer_note'].dropna()
            if not latest_note.empty:
                st.info(f"**Engineer's Assessment:** {latest_note.iloc[0]}")

            # D. Current Status Table (Last 24h)
            st.subheader("🌡️ Current Conditions")
            latest = df_c.sort_values('timestamp').groupby('depth').tail(1).copy()
            st.dataframe(latest[['depth', 'temperature', 'timestamp']].format({'temperature': '{:.1f}', 'timestamp': '{:%m/%d %H:%M}'}), use_container_width=True, hide_index=True)

    except Exception as e: st.error(f"Portal Error: {e}")

# SERVICE 3: NODE DIAGNOSTICS
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
            st.plotly_chart(build_standard_sf_graph(df_g, f"Trend: {sel_proj} | {sel_loc}", start_v, end_v), use_container_width=True)
    except Exception as e: st.error(f"Diagnostics Error: {e}")

# SERVICE 4: DATA INTAKE (WITH MULTI-ACCOUNT API)
elif service == "📤 Data Intake Lab":
    st.header("📤 Data Ingestion & Recovery")
    tab1, tab2 = st.tabs(["📄 Manual File Upload", "📡 API Data Recovery"])

    with tab1:
        st.subheader("Manual CSV Ingestion")
        source = st.radio("Device Type", ["SensorPush (CSV)", "Lord (SensorConnect)"], horizontal=True)
        u_file = st.file_uploader("Upload Logger File", type=['csv'], key="manual_upload")
        if u_file is not None:
            try:
                # ... (Keep your existing manual upload logic here)
                st.info("Manual upload logic ready.")
            except Exception as e: st.error(f"File Error: {e}")

    with tab2:
        st.subheader("SensorPush Multi-Account Recovery")
        c1, c2 = st.columns(2)
        with c1:
            sd = st.date_input("Recovery Start", datetime.now() - timedelta(days=2))
            st_time = st.time_input("Start Time (UTC)", datetime.strptime("00:00", "%H:%M").time())
        with c2:
            ed = st.date_input("Recovery End", datetime.now())
            et_time = st.time_input("End Time (UTC)", datetime.now().time())

        s_iso = datetime.combine(sd, st_time).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        e_iso = datetime.combine(ed, et_time).strftime("%Y-%m-%dT%H:%M:%S.000Z")

        if st.button("🛰️ RUN ALL-ACCOUNT RECOVERY"):
            if "sensorpush_accounts" not in st.secrets:
                st.error("Missing 'sensorpush_accounts' in Streamlit Secrets.")
            else:
                accounts = st.secrets["sensorpush_accounts"]
                all_api_recs = []
                for acc_id, creds in accounts.items():
                    try:
                        with st.spinner(f"Fetching {acc_id}..."):
                            auth_res = requests.post("https://api.sensorpush.com/api/v1/oauth/authorize", json=creds)
                            token = auth_res.json().get("accesstoken")
                            headers = {"accept": "application/json", "Authorization": token}
                            payload = {"startTime": s_iso, "endTime": e_iso, "measures": ["temperature"]}
                            sample_res = requests.post("https://api.sensorpush.com/api/v1/samples", headers=headers, json=payload)
                            raw_json = sample_res.json()
                            for sid, samples in raw_json.get("sensors", {}).items():
                                for s in samples:
                                    all_api_recs.append({"timestamp": s["observed"], "temperature": s["value"], "sensor_id": sid.replace(':', '-')})
                    except Exception as e: st.warning(f"Failed {acc_id}: {e}")
                
                if all_api_recs:
                    df_api = pd.DataFrame(all_api_recs)
                    client.load_table_from_dataframe(df_api, f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush").result()
                    st.success(f"✅ Success! Pulled {len(all_api_recs)} points total.")

# SERVICE 5: ADMIN TOOLS (SCRUBBER & APPROVAL)
elif service == "🛠️ Admin Tools":
    st.header("🛠️ Engineering Admin Tools")
    tab_scrub, tab_approve = st.tabs(["🧹 Data Scrubber", "✅ Engineer Approval"])

    with tab_scrub:
        st.subheader("Delete Bad Data")
        sc_proj = st.text_input("Project Name (Exact)")
        sc_loc = st.text_input("Location (Exact)")
        sc_start = st.text_input("Start Time (YYYY-MM-DD HH:MM:SS)")
        sc_end = st.text_input("End Time (YYYY-MM-DD HH:MM:SS)")
        
        if st.button("🗑️ PERMANENTLY DELETE POINTS"):
            if sc_proj and sc_loc and sc_start and sc_end:
                scrub_q = f"""DELETE FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` 
                             WHERE project='{sc_proj}' AND location='{sc_loc}' 
                             AND timestamp BETWEEN '{sc_start}' AND '{sc_end}'"""
                client.query(scrub_q).result()
                st.success("Points successfully erased.")
            else: st.error("Please fill all fields.")

    with tab_approve:
        st.subheader("Approve Data & Add Notes")
        # Fetch the most recent 100 points that are not approved
        review_q = f"SELECT timestamp, sensor_name, project, location, temperature, is_approved, engineer_note FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE is_approved IS FALSE OR is_approved IS NULL ORDER BY timestamp DESC LIMIT 200"
        df_review = client.query(review_q).to_dataframe()
        
        if not df_review.empty:
            st.write("Edit the 'is_approved' checkbox and 'engineer_note' columns below:")
            edited_df = st.data_editor(df_review, use_container_width=True)
            
            if st.button("💾 SAVE CHANGES TO DATABASE"):
                # Note: Bulk updates in BigQuery are best done via a MERGE or by reloading the table.
                # For this lab, we will use a quick 'Set all to approved' for the selected project/location as an example.
                st.warning("Bulk saving requires a MERGE query. For now, use the Scrubber to remove bad data and verify the master sync logic.")
        else:
            st.success("All current data is approved!")
