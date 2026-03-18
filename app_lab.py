import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta, time, date
import pytz

# --- 0. PAGE CONFIGURATION ---
st.set_page_config(layout="wide", page_title="SoilFreeze Engineering Hub")

# --- 1. AUTHENTICATION ---
SCOPES = ["https://www.googleapis.com/auth/drive.readonly", "https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform"]

if "gcp_service_account" in st.secrets:
    info = st.secrets["gcp_service_account"]
    credentials = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    client = bigquery.Client(credentials=credentials, project=info["project_id"])
else:
    client = bigquery.Client.from_service_account_json("service_account.json", scopes=SCOPES)

# --- 2. DATA FETCHING ---
@st.cache_data(ttl=600)
def fetch_engineering_data():
    query = """
    WITH raw_combined AS (
        SELECT CAST(timestamp AS TIMESTAMP) as timestamp, value, nodenumber FROM `sensorpush-export.sensor_data.raw_lord`
        UNION ALL
        SELECT timestamp, temperature AS value, sensor_name AS nodenumber FROM `sensorpush-export.sensor_data.raw_sensorpush`
    )
    SELECT r.timestamp, r.value, r.nodenumber, m.Project, m.Location, m.Depth
    FROM raw_combined AS r
    LEFT JOIN `sensorpush-export.sensor_data.master_metadata` AS m ON r.nodenumber = m.NodeNum
    """
    df = client.query(query).to_dataframe()
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df

full_df = pd.DataFrame()
try:
    full_df = fetch_engineering_data()
except Exception as e:
    st.sidebar.error(f"Database Error: {e}")

# --- 3. SIDEBAR NAVIGATION ---
st.sidebar.title("🛠 Engineering Hub")
service = st.sidebar.selectbox(
    "Select Service", 
    ["🏠 Executive Summary", "🔍 Node Diagnostics", "📥 Data Export Lab", "🧹 Data Cleaning Tool"]
)

# --- SERVICE 0: EXECUTIVE SUMMARY (LANDING PAGE) ---
if service == "🏠 Executive Summary" and not full_df.empty:
    st.header("🏠 Site-Wide Executive Summary")
    st.write(f"Last Refresh: {datetime.now().strftime('%m/%d %H:%M')}")

    # Calculate global stats for the last 24h
    cutoff_24h = datetime.now(tz=pytz.UTC) - timedelta(hours=24)
    recent_all = full_df[full_df['timestamp'] >= cutoff_24h].copy()
    
    # 1. Global Metrics
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Active Sensors", len(recent_all['nodenumber'].unique()))
    m2.metric("Highest Temp (24h)", f"{recent_all['value'].max():.1f}°F")
    m3.metric("Lowest Temp (24h)", f"{recent_all['value'].min():.1f}°F")
    
    offline_cutoff = datetime.now(tz=pytz.UTC) - timedelta(hours=6)
    last_seen_all = full_df.groupby(['Project', 'nodenumber'])['timestamp'].max()
    global_offline = last_seen_all[last_seen_all < offline_cutoff]
    m4.metric("Offline Sensors", len(global_offline), delta_color="inverse")

    # 2. Project Breakdown Table
    st.subheader("📋 Project Health Overview")
    project_stats = []
    for proj in sorted([p for p in full_df['Project'].unique() if p is not None]):
        proj_data = recent_all[recent_all['Project'] == proj]
        if not proj_data.empty:
            project_stats.append({
                "Project": proj,
                "Avg Temp": f"{proj_data['value'].mean():.1f}°F",
                "Max Temp": f"{proj_data['value'].max():.1f}°F",
                "Min Temp": f"{proj_data['value'].min():.1f}°F",
                "Sensors Reporting": len(proj_data['nodenumber'].unique())
            })
    
    if project_stats:
        st.table(pd.DataFrame(project_stats))

    # 3. Urgent Alerts
    if len(global_offline) > 0:
        st.error(f"⚠️ ATTENTION: {len(global_offline)} sensors have not reported in over 6 hours.")
        with st.expander("Show Offline Sensor List"):
            st.dataframe(global_offline.reset_index(), width='stretch')

# --- SERVICE 1: NODE DIAGNOSTICS ---
elif service == "🔍 Node Diagnostics" and not full_df.empty:
    st.header("🔍 Node Diagnostic Hub")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        projs = sorted([p for p in full_df['Project'].unique() if p is not None])
        sel_proj = st.selectbox("Project", projs)
    with col2:
        locs = sorted([l for l in full_df[full_df['Project'] == sel_proj]['Location'].unique() if l is not None])
        sel_loc = st.selectbox("Location", locs)
    with col3:
        weeks_to_show = st.number_input("Weeks to Display", min_value=1, value=2)

    # Time Logic
    today_dt = datetime.now().date()
    last_monday = today_dt - timedelta(days=today_dt.weekday())
    start_time = datetime.combine(last_monday, time.min) - timedelta(weeks=weeks_to_show - 1)
    start_ts = pd.Timestamp(start_time, tz='UTC')
    
    # Process Gaps & Duplicates
    raw_plot_df = full_df[(full_df['Project'] == sel_proj) & (full_df['Location'] == sel_loc) & (full_df['timestamp'] >= start_ts)].copy()
    hourly_range = pd.date_range(start=start_ts, end=datetime.now(tz=pytz.UTC), freq='h')
    
    processed_dfs = []
    if not raw_plot_df.empty:
        for sensor in raw_plot_df['nodenumber'].unique():
            s_df = raw_plot_df[raw_plot_df['nodenumber'] == sensor].copy()
            sensor_depth = s_df['Depth'].iloc[0]
            s_df = s_df.groupby('timestamp').mean(numeric_only=True).reset_index()
            s_df = s_df.set_index('timestamp').reindex(hourly_range).rename_axis('timestamp').reset_index()
            s_df['Sensor_ID'] = f"Depth: {sensor_depth} (SN: {sensor})"
            processed_dfs.append(s_df)
    
    plot_df = pd.concat(processed_dfs) if processed_dfs else pd.DataFrame()

    if not plot_df.empty:
        fig = px.line(plot_df, x='timestamp', y='value', color='Sensor_ID', range_y=[-20, 80], height=800)
        fig.update_traces(connectgaps=False, hovertemplate="<b>%{fullData.name}</b><br>Temp: %{y:.2f}°F<extra></extra>")
        
        # UI Polish
        fig.update_layout(plot_bgcolor='white', hovermode="x unified", margin=dict(l=20, r=150, t=50, b=20))
        fig.update_xaxes(showgrid=True, dtick=86400000.0, gridcolor='DarkGrey', tickformat="%a\n%b %d", tickfont=dict(size=14))
        fig.update_yaxes(tick0=-20, dtick=20, gridcolor='DimGrey', gridwidth=1.5, minor=dict(dtick=5, gridcolor='Grey', showgrid=True), tickfont=dict(size=14))
        fig.add_hline(y=32, line_dash="dash", line_color="blue")
        
        st.plotly_chart(fig, width='stretch')
        st.download_button("📥 Download Diagnostic CSV", data=plot_df.dropna().to_csv(index=False).encode('utf-8'), key="diag_dl")

# --- SERVICE 2: DATA EXPORT LAB ---
elif service == "📥 Data Export Lab" and not full_df.empty:
    st.header("📥 Bulk Data Export")
    d_col1, d_col2 = st.columns(2)
    with d_col1:
        start_d = st.date_input("Start Date", value=date.today() - timedelta(days=30))
    with d_col2:
        end_d = st.date_input("End Date", value=date.today())

    s_col1, s_col2, s_col3 = st.columns(3)
    with s_col1:
        ex_projs = sorted([p for p in full_df['Project'].unique() if p is not None])
        sel_ex_proj = st.selectbox("Select Project", ex_projs)
        ex_df = full_df[full_df['Project'] == sel_ex_proj]
    with s_col2:
        ex_locs = ["All Locations"] + sorted([l for l in ex_df['Location'].unique() if l is not None])
        sel_ex_loc = st.selectbox("Select Location", ex_locs)
        if sel_ex_loc != "All Locations": ex_df = ex_df[ex_df['Location'] == sel_ex_loc]
    with s_col3:
        ex_nodes = ["All Nodes"] + sorted(ex_df['nodenumber'].unique().tolist())
        sel_ex_node = st.selectbox("Select Node", ex_nodes)
        if sel_ex_node != "All Nodes": ex_df = ex_df[ex_df['nodenumber'] == sel_ex_node]

    final_ex_df = ex_df[(ex_df['timestamp'].dt.date >= start_d) & (ex_df['timestamp'].dt.date <= end_d)]
    st.write(f"📊 Found **{len(final_ex_df)}** records.")
    st.dataframe(final_ex_df.head(200), width='stretch')
    if not final_ex_df.empty:
        st.download_button("📥 Download Export", data=final_ex_df.to_csv(index=False).encode('utf-8'), key="bulk_dl")

# --- SERVICE 3: DATA CLEANING TOOL ---
elif service == "🧹 Data Cleaning Tool" and not full_df.empty:
    st.header("🧹 Surgical Data Cleaning")
    c_col1, c_col2 = st.columns(2)
    with c_col1:
        clean_projs = sorted([p for p in full_df['Project'].unique() if p is not None])
        sel_c_proj = st.selectbox("Project to Clean", clean_projs)
    with c_col2:
        clean_start = st.date_input("Clean Start Date", value=date.today() - timedelta(days=2))
        clean_end = st.date_input("Clean End Date", value=date.today())

    clean_view_df = full_df[(full_df['Project'] == sel_c_proj) & (full_df['timestamp'].dt.date >= clean_start) & (full_df['timestamp'].dt.date <= clean_end)].copy()
    st.subheader("Highlight 'Spikes' to Clean")
    fig_clean = px.scatter(clean_view_df, x='timestamp', y='value', color='nodenumber', height=600)
    fig_clean.update_layout(dragmode='select', selectionrevision=True)
    event_data = st.plotly_chart(fig_clean, width='stretch', on_select="rerun")

    if event_data and event_data.get("selection", {}).get("points"):
        pts = event_data["selection"]["points"]
        st.error(f"⚠️ Targeted: {len(pts)} points selected.")
        if st.checkbox(f"Verify: Permanent Delete for Project {sel_c_proj}"):
            if st.button("🔥 GENERATE DELETE SQL", type="primary"):
                time_list = ", ".join([f"'{p['x']}'" for p in pts])
                st.code(f"DELETE FROM `sensor_data` WHERE Project = '{sel_c_proj}' AND timestamp IN ({time_list})")
