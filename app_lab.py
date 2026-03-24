import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz
import io
import requests
import json


# --- 1. CONFIGURATION & STYLING ---
st.set_page_config(page_title="SoilFreeze Data Lab", layout="wide")

# Constants for BigQuery
DATASET_ID = "sensor_data" 
PROJECT_ID = "sensorpush-export"

@st.cache_resource
def get_bq_client():
    """Consistent auth engine to prevent TransportErrors."""
    try:
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(info)
            scoped_creds = credentials.with_scopes([
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/bigquery"
            ])
            return bigquery.Client(credentials=scoped_creds, project=info["project_id"])
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"Authentication Failed: {e}")
        return None

client = get_bq_client()

# --- 2. STANDARDIZED GRAPH ENGINE ---
def build_standard_sf_graph(df, title, start_view, end_view, unit="Fahrenheit", active_refs=None):
    """Handles 6hr gaps, C/F units, custom grid, and 'Right Now' red line."""
    if active_refs is None: active_refs = []
    
    # Unit Conversion Logic
    display_df = df.copy()
    if unit == "Celsius":
        display_df['value'] = (display_df['value'] - 32) * 5/9
        y_range, y_ticks, y_label, m_step = [-30, 30], [-30, -20, -10, 0, 10, 20, 30], "Temp (°C)", 2.5
    else:
        y_range, y_ticks, y_label, m_step = [-20, 80], [-20, 0, 20, 40, 60, 80], "Temp (°F)", 5

    # Gap Logic (Line breaks > 6hrs)
    processed_dfs = []
    for sensor in display_df['Sensor'].unique():
        s_df = display_df[display_df['Sensor'] == sensor].copy().sort_values('timestamp')
        s_df['gap'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
        gaps = s_df[s_df['gap'] > 6.0].copy()
        if not gaps.empty:
            gaps['value'] = None
            gaps['timestamp'] = gaps['timestamp'] - timedelta(minutes=1)
            s_df = pd.concat([s_df, gaps]).sort_values('timestamp')
        processed_dfs.append(s_df)
    
    clean_df = pd.concat(processed_dfs) if processed_dfs else display_df
    
    # Trace Creation: Explicitly No Fill to prevent 'Blobs'
    fig = go.Figure()
    for sensor in clean_df['Sensor'].unique():
        sensor_df = clean_df[clean_df['Sensor'] == sensor]
        fig.add_trace(go.Scatter(
            x=sensor_df['timestamp'], y=sensor_df['value'],
            name=sensor, mode='lines', fill=None, connectgaps=False, line=dict(width=2)
        ))

    # Grid & Axis Styling
    fig.update_yaxes(title=y_label, tickmode='array', tickvals=y_ticks, range=y_range,
                     gridcolor='DimGray', gridwidth=1.5, minor=dict(dtick=m_step, gridcolor='Silver', showgrid=True),
                     mirror=True, showline=True, linecolor='black', linewidth=2)
    fig.update_xaxes(showgrid=False, range=[start_view, end_view], mirror=True, showline=True, linecolor='black', linewidth=2)

    # Custom Vertical Grid (Mon/Mid/6hr)
    shapes = []
    curr = start_view.replace(hour=0, minute=0, second=0)
    while curr <= end_view:
        for h in [0, 6, 12, 18]:
            t = curr + timedelta(hours=h)
            if t < start_view or t > end_view: continue
            t_ms = t.timestamp() * 1000
            c, w = ("DimGray", 2) if (t.weekday() == 0 and h == 0) else (("DarkGray", 1) if h == 0 else ("LightGray", 0.5))
            shapes.append(dict(type="line", xref="x", yref="paper", x0=t_ms, y0=0, x1=t_ms, y1=1, line=dict(color=c, width=w), layer="below"))
        curr += timedelta(days=1)

    # NOW Line & Reference Lines
    now_ms = datetime.now(pytz.UTC).timestamp() * 1000
    fig.add_vline(x=now_ms, line_width=2, line_color="red", annotation_text="NOW")
    for ref_f, label in active_refs:
        val = (ref_f - 32) * 5/9 if unit == "Celsius" else ref_f
        fig.add_hline(y=val, line_dash="dash", line_color="blue", annotation_text=f"{label} {round(val,1)}°")

    fig.update_layout(title={'text': title, 'x': 0.5}, shapes=shapes, plot_bgcolor='white',
                      legend=dict(x=1.02, y=1, bordercolor="Black", borderwidth=1), margin=dict(r=150), height=750)
    return fig

# --- 3. SERVICE ROUTING ---
st.sidebar.title("❄️ SoilFreeze Lab")
service = st.sidebar.selectbox("Select Service", ["🏠 Executive Summary", "📈 Node Diagnostics", "📤 Data Intake Lab", "⚙️ Database Maintenance"])


#################################################################
if service == "🏠 Executive Summary":
    st.header("🏠 Site Health & Warming Alerts")

    # 1. Project Selection
    meta_df = client.query(
        f"SELECT DISTINCT Project FROM `{PROJECT_ID}.{DATASET_ID}.master_metadata`"
    ).to_dataframe(create_bqstorage_client=False)
    
    all_projs = sorted([p for p in meta_df['Project'].unique() if p is not None])
    default_idx = all_projs.index("Office") if "Office" in all_projs else 0
    sel_summary_proj = st.selectbox("Select Project Focus", all_projs, index=default_idx)

    # 2. SQL Query
    query = f"""
        WITH NodeLimits AS (
            SELECT nodenumber, MAX(timestamp) as max_ts
            FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master`
            WHERE Project = '{sel_summary_proj}'
            GROUP BY nodenumber
        )
        SELECT 
            m.timestamp, m.value, m.Location, m.Depth, m.nodenumber
        FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` m
        JOIN NodeLimits nl ON m.nodenumber = nl.nodenumber
        WHERE m.timestamp >= TIMESTAMP_SUB(nl.max_ts, INTERVAL 24 HOUR)
        ORDER BY m.timestamp DESC
    """
    
    try:
        df_summary = client.query(query).to_dataframe(create_bqstorage_client=False)

        if df_summary.empty:
            st.warning(f"No historical data found for project: {sel_summary_proj}")
        else:
            now_ts = datetime.now(pytz.UTC)
            summary_stats = []
            
            for node in df_summary['nodenumber'].unique():
                n_df = df_summary[df_summary['nodenumber'] == node].sort_values('timestamp')
                
                current_temp = n_df['value'].iloc[-1]
                net_change = current_temp - n_df['value'].iloc[0] # Heating (+) vs Cooling (-)
                
                last_seen_dt = n_df['timestamp'].iloc[-1]
                hours_ago = (now_ts - last_seen_dt).total_seconds() / 3600
                hours_int = int(round(hours_ago, 0))
                
                summary_stats.append({
                    "Location": n_df['Location'].iloc[0],
                    "Depth": f"{n_df['Depth'].iloc[0]}ft",
                    "Node ID": node,
                    "Status / Last Seen": f"{last_seen_dt.strftime('%m/%d %H:%M')} ({hours_int}h ago)",
                    "hours_raw": hours_ago,
                    "Min (24h)": round(float(n_df['value'].min()), 1),
                    "Max (24h)": round(float(n_df['value'].max()), 1),
                    "24h Change": round(float(net_change), 1),
                    "Current": round(float(current_temp), 1)
                })

            # --- 3. SORTING LOGIC: Warming Highest at Top ---
            df_full = pd.DataFrame(summary_stats)
            df_full = df_full.sort_values(by="24h Change", ascending=False)

            # --- 4. PAGINATION LOGIC: 20 Rows per Page ---
            rows_per_page = 20
            total_pages = (len(df_full) // rows_per_page) + (1 if len(df_full) % rows_per_page > 0 else 0)
            
            # Simple Navigation UI
            col_nav1, col_nav2 = st.columns([1, 4])
            with col_nav1:
                page_num = st.number_input(f"Page (1 of {total_pages})", min_value=1, max_value=total_pages, step=1)
            
            start_idx = (page_num - 1) * rows_per_page
            end_idx = start_idx + rows_per_page
            df_display = df_full.iloc[start_idx:end_idx]

            # 5. ADVANCED STYLING (Age of Data & Thermal Delta)
            def apply_row_styles(row):
                styles = [''] * len(row)
                
                # Age Coloring
                h = row['hours_raw']
                status_idx = row.index.get_loc("Status / Last Seen")
                if h >= 24: styles[status_idx] = 'background-color: #ff4b4b; color: white'
                elif h >= 12: styles[status_idx] = 'background-color: #ffa500; color: black'
                elif h >= 6: styles[status_idx] = 'background-color: #ffff00; color: black'
                
                # Thermal Delta Coloring
                change = row['24h Change']
                chg_idx = row.index.get_loc("24h Change")
                if change >= 5.0: styles[chg_idx] = 'background-color: #ff4b4b; color: white'
                elif change >= 2.0: styles[chg_idx] = 'background-color: #ffa500; color: black'
                elif change >= 1.0: styles[chg_idx] = 'background-color: #ffff00; color: black'
                elif change <= -1.0: styles[chg_idx] = 'background-color: #00008b; color: white'
                elif change <= -0.5: styles[chg_idx] = 'background-color: #0000ff; color: white'
                elif change <= -0.25: styles[chg_idx] = 'background-color: #add8e6; color: black'
                
                return styles

            # 6. Final Display (Height set for 20 rows to avoid internal scroll)
            st.dataframe(
                df_display.style.apply(apply_row_styles, axis=1),
                column_config={
                    "Min (24h)": st.column_config.NumberColumn(format="%.1f"),
                    "Max (24h)": st.column_config.NumberColumn(format="%.1f"),
                    "24h Change": st.column_config.NumberColumn(format="%.1f"),
                    "Current": st.column_config.NumberColumn(format="%.1f"),
                    "hours_raw": None
                },
                width='stretch',
                height=780, # Fits approx 20 rows comfortably
                hide_index=True
            )

    except Exception as e:
        st.error(f"Executive Summary Error: {e}")

##########################################
elif service == "📈 Node Diagnostics":
    st.header("📈 Node Diagnostics")
    
    # Sidebar Settings
    temp_unit = st.sidebar.radio("Unit", ["Fahrenheit", "Celsius"])
    ref_list = []
    if st.sidebar.checkbox("32°F (Frost)"): ref_list.append((32.0, "Frost"))
    if st.sidebar.checkbox("26.6°F (Brine)"): ref_list.append((26.6, "Brine"))
    if st.sidebar.checkbox("10.2°F (Deep)"): ref_list.append((10.2, "Deep"))

    # Single-Selection Filters
    meta_df = client.query(f"SELECT DISTINCT Project, Location FROM `{PROJECT_ID}.{DATASET_ID}.master_metadata`").to_dataframe(create_bqstorage_client=False)
    
    c1, c2, c3 = st.columns(3)
    with c1:
        all_projs = sorted([p for p in meta_df['Project'].unique() if p is not None])
        default_idx = all_projs.index("Office") if "Office" in all_projs else 0
        sel_proj = st.selectbox("Project", all_projs, index=default_idx)
    with c2:
        raw_locs = meta_df[meta_df['Project'] == sel_proj]['Location'].unique()
        avail_locs = sorted([l for l in raw_locs if l is not None])
        sel_loc = st.selectbox("Pipe / Bank", avail_locs)
    with c3:
        weeks = st.slider("Duration (Weeks)", 1, 12, 6)

    # Data & Plotting
    if sel_proj and sel_loc:
        now_utc = datetime.now(pytz.UTC)
        end_view = (now_utc + timedelta(days=(7 - now_utc.weekday()) % 7)).replace(hour=0, minute=0, second=0, microsecond=0)
        start_view = end_view - timedelta(weeks=weeks)
        
        q = f"SELECT timestamp, value, Location, Depth, nodenumber FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE Project = '{sel_proj}' AND Location = '{sel_loc}' AND timestamp >= '{start_view.strftime('%Y-%m-%d %H:%M:%S')}'"
        df_g = client.query(q).to_dataframe(create_bqstorage_client=False)
        
        if not df_g.empty:
            df_g['depth_num'] = pd.to_numeric(df_g['Depth'], errors='coerce').fillna(0)
            df_g['Sensor'] = df_g.apply(lambda x: f"{x['Depth']}ft ({x['nodenumber']})" if str(x['Depth']).replace('.','',1).isdigit() else f"{x['Depth']} ({x['nodenumber']})", axis=1)
            df_g = df_g.sort_values(by=['depth_num', 'timestamp'])
            
            fig = build_standard_sf_graph(df_g, f"Trend: {sel_proj} - {sel_loc}", start_view, end_view, unit=temp_unit, active_refs=ref_list)
            st.plotly_chart(fig, width='stretch')

################################################
elif service == "📤 Data Intake Lab":
    st.header("📤 Data Ingestion & Recovery")
    
    # 1. Tab Setup
    tab1, tab2 = st.tabs(["📄 Manual File Upload", "📡 API Data Recovery"])

    # --- TAB 1: MANUAL FILE UPLOAD ---
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
                    df_up = df_raw.melt(id_vars=[df_raw.columns[0]], var_name='nodenumber', value_name='value')
                    df_up = df_up.rename(columns={df_raw.columns[0]: 'timestamp'})
                    table_ref = f"{PROJECT_ID}.{DATASET_ID}.raw_lord"
                else:
                    df_up = pd.read_csv(u_file).rename(columns={'Timestamp':'timestamp', 'Temperature':'value', 'Sensor':'nodenumber'})
                    table_ref = f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush"
                
                df_up['nodenumber'] = df_up['nodenumber'].astype(str).str.replace(':', '-', regex=False)
                df_up['timestamp'] = pd.to_datetime(df_up['timestamp'])

                if st.button("🚀 PUSH FILE TO BIGQUERY"):
                    with st.spinner("Uploading..."):
                        client.load_table_from_dataframe(df_up, table_ref).result()
                        # Run Auto-Scrub
                        scrub_sql = f"CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` AS WITH Unified AS (SELECT CAST(timestamp AS TIMESTAMP) as timestamp, value, REPLACE(nodenumber, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord` UNION ALL SELECT CAST(timestamp AS TIMESTAMP) as timestamp, value, REPLACE(nodenumber, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`) SELECT u.*, m.Project, m.Location, m.Depth FROM Unified u INNER JOIN `{PROJECT_ID}.{DATASET_ID}.master_metadata` m ON u.node = REPLACE(m.NodeNum, ':', '-')"
                        client.query(scrub_sql).result()
                        st.success("✅ File Uploaded & Master Table Synced!")
                        st.balloons()
            except Exception as e:
                st.error(f"File Error: {e}")

    # --- TAB 2: API CLOUD RECOVERY ---
    with tab2:
        st.subheader("SensorPush Cloud Recovery")
        import requests # Imported here to prevent startup crash if missing
        
        c1, c2 = st.columns(2)
        with c1:
            s_d = st.date_input("Start Date", datetime.now() - timedelta(days=1))
            s_t = st.time_input("Start Time (UTC)", datetime.strptime("00:00", "%H:%M").time())
        with c2:
            e_d = st.date_input("End Date", datetime.now())
            e_t = st.time_input("End Time (UTC)", datetime.now().time())

        s_iso = datetime.combine(s_d, s_t).strftime("%Y-%m-%dT%H:%M:%SZ")
        e_iso = datetime.combine(e_d, e_t).strftime("%Y-%m-%dT%H:%M:%SZ")

        if st.button("🛰️ RUN CLOUD RECOVERY"):
            if "sensorpush" not in st.secrets:
                st.error("Missing 'sensorpush' credentials in Secrets.")
            else:
                try:
                    creds = st.secrets["sensorpush"]
                    # 1. Authenticate
                    auth_res = requests.post("https://api.sensorpush.com/api/v1/oauth/authorize", json={"email": creds["email"], "password": creds["password"]})
                    auth_res.raise_for_status()
                    token = auth_res.json().get("accesstoken")

                    # 2. Fetch
                    h = {"accept": "application/json", "Authorization": token}
                    p = {"startTime": s_iso, "endTime": e_iso, "measures": ["temperature"]}
                    data_res = requests.post("https://api.sensorpush.com/api/v1/samples", headers=h, json=p)
                    data_res.raise_for_status()
                    raw_json = data_res.json()

                    # 3. Process
                    recs = []
                    for sid, samples in raw_json.get("sensors", {}).items():
                        for s in samples:
                            recs.append({"timestamp": s["observed"], "value": s["value"], "nodenumber": sid.replace(':', '-')})
                    
                    if recs:
                        df_api = pd.DataFrame(recs)
                        df_api['timestamp'] = pd.to_datetime(df_api['timestamp'])
                        # Push Raw
                        client.load_table_from_dataframe(df_api, f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush").result()
                        # Run Master Scrub
                        scrub_sql = f"CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` AS WITH Unified AS (SELECT CAST(timestamp AS TIMESTAMP) as timestamp, value, REPLACE(nodenumber, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord` UNION ALL SELECT CAST(timestamp AS TIMESTAMP) as timestamp, value, REPLACE(nodenumber, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`) SELECT u.*, m.Project, m.Location, m.Depth FROM Unified u INNER JOIN `{PROJECT_ID}.{DATASET_ID}.master_metadata` m ON u.node = REPLACE(m.NodeNum, ':', '-')"
                        client.query(scrub_sql).result()
                        st.success(f"✅ Recovery Successful! Pulled {len(recs)} records.")
                        st.balloons()
                    else:
                        st.warning("No data found for this window.")
                except Exception as e:
                    st.error(f"API Error: {e}")
