import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta, time, date

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
service = st.sidebar.selectbox("Select Service", ["🔍 Node Diagnostics", "📥 Data Export Lab", "🧹 Data Cleaning Tool"])

# --- SERVICE 1: NODE DIAGNOSTICS (THE STARTING IF) ---
if service == "🔍 Node Diagnostics" and not full_df.empty:
    st.header("🔍 Node Diagnostics")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        projs = sorted([p for p in full_df['Project'].unique() if p is not None])
        sel_proj = st.selectbox("Project", projs)
    with col2:
        locs = sorted([l for l in full_df[full_df['Project'] == sel_proj]['Location'].unique() if l is not None])
        sel_loc = st.selectbox("Location", locs)
    with col3:
        weeks_to_show = st.number_input("Weeks to Display", min_value=1, value=2)

    today_dt = datetime.now().date()
    last_monday = today_dt - timedelta(days=today_dt.weekday())
    start_time = datetime.combine(last_monday, time.min) - timedelta(weeks=weeks_to_show - 1)
    
    plot_df = full_df[
        (full_df['Project'] == sel_proj) & 
        (full_df['Location'] == sel_loc) &
        (full_df['timestamp'] >= pd.Timestamp(start_time, tz='UTC'))
    ].copy()
    plot_df['Sensor_ID'] = plot_df['nodenumber'].astype(str) + " | Depth: " + plot_df['Depth'].astype(str)

    if not plot_df.empty:
        fig = px.line(plot_df, x='timestamp', y='value', color='Sensor_ID', range_y=[-20, 80])
        mondays = pd.date_range(start=start_time, end=datetime.now(), freq='W-MON')
        for mon in mondays:
            fig.add_vline(x=mon.timestamp() * 1000, line_width=2.5, line_color="black")
        
        fig.update_xaxes(showgrid=True, dtick=86400000.0, gridcolor='DarkGrey', tickformat="%a\n%b %d", range=[start_time, datetime.now()])
        fig.update_yaxes(tick0=-20, dtick=20, gridcolor='DimGrey', gridwidth=1.5, minor=dict(dtick=5, gridcolor='Grey', showgrid=True))
        fig.update_layout(plot_bgcolor='white', margin=dict(l=0, r=150, t=30, b=0), legend=dict(x=1.02), hovermode="x unified")
        
        st.plotly_chart(fig, width='stretch')
        st.download_button("📥 Download Current Graph Data (CSV)", data=plot_df.to_csv(index=False).encode('utf-8'), file_name="QuickView.csv")
    else:
        st.info("No data found.")

# --- SERVICE 2: DATA EXPORT LAB (ELIF) ---
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
        sel_ex_proj = st.selectbox("Project", ex_projs)
        ex_df = full_df[full_df['Project'] == sel_ex_proj]
    with s_col2:
        ex_locs = ["All Locations"] + sorted([l for l in ex_df['Location'].unique() if l is not None])
        sel_ex_loc = st.selectbox("Location", ex_locs)
        if sel_ex_loc != "All Locations": ex_df = ex_df[ex_df['Location'] == sel_ex_loc]
    with s_col3:
        ex_nodes = ["All Nodes"] + sorted(ex_df['nodenumber'].unique().tolist())
        sel_ex_node = st.selectbox("Node/Serial", ex_nodes)
        if sel_ex_node != "All Nodes": ex_df = ex_df[ex_df['nodenumber'] == sel_ex_node]

    final_ex_df = ex_df[(ex_df['timestamp'].dt.date >= start_d) & (ex_df['timestamp'].dt.date <= end_d)]
    st.write(f"📊 Found **{len(final_ex_df)}** records.")
    st.dataframe(final_ex_df.head(100), width='stretch')
    if not final_ex_df.empty:
        st.download_button("📥 Download Bulk Export (CSV)", data=final_ex_df.to_csv(index=False).encode('utf-8'), file_name="SoilFreeze_Bulk.csv")

# --- SERVICE 3: DATA CLEANING TOOL (ELIF) ---
elif service == "🧹 Data Cleaning Tool" and not full_df.empty:
    st.header("🧹 Surgical Data Cleaning")
    
    c_col1, c_col2 = st.columns(2)
    with c_col1:
        clean_projs = sorted([p for p in full_df['Project'].unique() if p is not None])
        sel_c_proj = st.selectbox("Project to Clean", clean_projs)
    with c_col2:
        c_locs = ["All Locations"] + sorted([l for l in full_df[full_df['Project']==sel_c_proj]['Location'].unique() if l is not None])
        sel_c_loc = st.selectbox("Location Filter", c_locs)

    r_col1, r_col2 = st.columns(2)
    with r_col1:
        clean_start = st.date_input("Start Date", value=date.today() - timedelta(days=1))
    with r_col2:
        clean_end = st.date_input("End Date", value=date.today())

    clean_view_df = full_df[
        (full_df['Project'] == sel_c_proj) & 
        (full_df['timestamp'].dt.date >= clean_start) & 
        (full_df['timestamp'].dt.date <= clean_end)
    ].copy()
    if sel_c_loc != "All Locations": clean_view_df = clean_view_df[clean_view_df['Location'] == sel_c_loc]

    st.subheader("Highlight 'Spikes' to Clean")
    fig_clean = px.scatter(clean_view_df, x='timestamp', y='value', color='nodenumber', range_y=[-40, 100])
    fig_clean.update_layout(dragmode='select', plot_bgcolor='white')
    selected_points = st.plotly_chart(fig_clean, width='stretch', on_select="rerun")

    if selected_points and "points" in selected_points and len(selected_points["points"]) > 0:
        pts = pd.DataFrame(selected_points["points"])
        st.error(f"⚠️ TARGETING {len(pts)} DATA POINTS")
        del_scope = st.radio("Targeting Scope:", ["Selected points only", "Selected timestamps for ALL nodes in project"])
        
        if st.button("🔥 PERMANENTLY DELETE SELECTED DATA"):
            target_times = pts['x'].unique().tolist()
            time_list = ", ".join([f"'{t}'" for t in target_times])
            sql = f"DELETE FROM `sensor_data` WHERE Project = '{sel_c_proj}' AND timestamp IN ({time_list})"
            st.code(sql, language="sql")
            st.warning("Copy SQL to BigQuery to execute.")
    else:
        st.info("👆 Use the 'Box Select' tool to highlight bad data.")
