import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta, time, date

# --- 0. PAGE CONFIGURATION (REQUIRED FOR WIDE GRAPHS) ---
# This MUST be the first Streamlit command in your script
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
service = st.sidebar.selectbox("Select Service", ["🔍 Node Diagnostics", "📥 Data Export Lab", "🧹 Data Cleaning Tool"])

# --- SERVICE 1: NODE DIAGNOSTICS ---
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
        # Height 800 + width='stretch' + wide layout = Large Graph
        fig = px.line(plot_df, x='timestamp', y='value', color='Sensor_ID', range_y=[-20, 80], height=800)
        
        mondays = pd.date_range(start=start_time, end=datetime.now(), freq='W-MON')
        for mon in mondays:
            fig.add_vline(x=mon.timestamp() * 1000, line_width=2.5, line_color="black")
        
        fig.update_xaxes(showgrid=True, dtick=86400000.0, gridcolor='DarkGrey', tickformat="%a\n%b %d", range=[start_time, datetime.now()])
        fig.update_yaxes(tick0=-20, dtick=20, gridcolor='DimGrey', gridwidth=1.5, minor=dict(dtick=5, gridcolor='Grey', showgrid=True))
        fig.update_layout(plot_bgcolor='white', margin=dict(l=20, r=150, t=30, b=20), legend=dict(x=1.02), hovermode="x unified")
        
        st.plotly_chart(fig, width='stretch')
        st.download_button("📥 Download Current Graph Data (CSV)", data=plot_df.to_csv(index=False).encode('utf-8'), file_name="QuickView.csv")
    else:
        st.info("No data found.")

# --- SERVICE 3: DATA CLEANING TOOL ---
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
        clean_start = st.date_input("Start Date", value=date.today() - timedelta(days=2))
    with r_col2:
        clean_end = st.date_input("End Date", value=date.today())

    clean_view_df = full_df[
        (full_df['Project'] == sel_c_proj) & 
        (full_df['timestamp'].dt.date >= clean_start) & 
        (full_df['timestamp'].dt.date <= clean_end)
    ].copy()
    if sel_c_loc != "All Locations": clean_view_df = clean_view_df[clean_view_df['Location'] == sel_c_loc]

    st.subheader("1. Highlight 'Spikes' on Graph")
    fig_clean = px.scatter(clean_view_df, x='timestamp', y='value', color='nodenumber', range_y=[-40, 100], height=600)
    fig_clean.update_layout(dragmode='select', selectionrevision=True)
    
    # capturing selection 
    event_data = st.plotly_chart(fig_clean, width='stretch', on_select="rerun")

    # This is the "Engine" that shows the delete button
    if event_data and event_data.get("selection", {}).get("points"):
        st.divider()
        st.subheader("2. Confirm Deletion")
        
        pts = event_data["selection"]["points"]
        st.warning(f"⚠️ Targeted: {len(pts)} points selected.")
        
        safety = st.checkbox(f"Verify: I am deleting data for Project {sel_c_proj}")
        
        if safety:
            if st.button("🔥 PERMANENTLY DELETE DATA", type="primary"):
                target_times = list(set([p['x'] for p in pts]))
                time_list = ", ".join([f"'{t}'" for t in target_times])
                
                sql = f"DELETE FROM `sensorpush-export.sensor_data.raw_combined` WHERE Project = '{sel_c_proj}' AND timestamp IN ({time_list})"
                st.code(sql, language="sql")
                st.success("SQL generated. Execute in BigQuery to finalize.")
    else:
        st.info("👆 Use the **Box Select** tool to highlight points. The delete button will appear here.")
