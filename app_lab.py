import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta, time, date
import pytz # Added to fix the NameError

# --- 0. PAGE CONFIGURATION (Wide Mode + Large Fonts) ---
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

    # 1. TIME LOGIC
    today_dt = datetime.now().date()
    last_monday = today_dt - timedelta(days=today_dt.weekday())
    start_time = datetime.combine(last_monday, time.min) - timedelta(weeks=weeks_to_show - 1)
    start_ts = pd.Timestamp(start_time, tz='UTC')
    
    # 2. DATA PREP & GAP HANDLING
    raw_plot_df = full_df[
        (full_df['Project'] == sel_proj) & (full_df['Location'] == sel_loc) &
        (full_df['timestamp'] >= start_ts)
    ].copy()
    
    # Use lowercase 'h' for frequency as requested by the FutureWarning
    hourly_range = pd.date_range(start=start_ts, end=datetime.now(tz=pytz.UTC), freq='h')
    
    processed_dfs = []
    if not raw_plot_df.empty:
        for sensor in raw_plot_df['nodenumber'].unique():
            # Get data for this specific sensor
            s_df = raw_plot_df[raw_plot_df['nodenumber'] == sensor].copy()
            
            # Save metadata so we don't lose it during reindexing
            sensor_depth = s_df['Depth'].iloc[0] if not s_df.empty else "Unknown"
            sensor_label = f"Depth: {sensor_depth}"
            
            # Collapse duplicates and reindex to create gaps
            s_df = s_df.groupby('timestamp').mean(numeric_only=True).reset_index()
            s_df = s_df.set_index('timestamp').reindex(hourly_range).rename_axis('timestamp').reset_index()
            
            # Restore the labels
            s_df['Sensor_ID'] = sensor_label
            processed_dfs.append(s_df)
    
    plot_df = pd.concat(processed_dfs) if processed_dfs else raw_plot_df

    if not plot_df.empty:
        # 3. CREATE CHART
        fig = px.line(
            plot_df, x='timestamp', y='value', color='Sensor_ID', 
            range_y=[-20, 80], height=800
        )

        # 4. HOVER & GAP CONFIG
        fig.update_traces(
            connectgaps=False,
            hovertemplate="<b>%{fullData.name}</b><br>Temp: %{y:.2f}°F<extra></extra>"
        )

        # 5. GRID & MONDAY MARKERS
        mondays = pd.date_range(start=start_time, end=datetime.now(), freq='W-MON')
        for mon in mondays:
            fig.add_vline(x=mon.timestamp() * 1000, line_width=2.5, line_color="black")
        
        fig.update_xaxes(
            showgrid=True, dtick=86400000.0, gridcolor='DarkGrey', 
            tickformat="%a\n%b %d", range=[start_time, datetime.now()],
            tickfont=dict(size=14)
        )
        fig.update_yaxes(
            tick0=-20, dtick=20, gridcolor='DimGrey', gridwidth=1.5, 
            minor=dict(dtick=5, gridcolor='Grey', showgrid=True),
            tickfont=dict(size=14)
        )

        # 6. LAYOUT
        fig.update_layout(
            plot_bgcolor='white', margin=dict(l=20, r=150, t=50, b=20),
            hovermode="x unified", legend=dict(x=1.02, font=dict(size=12)),
            title=dict(text=f"Project: {sel_proj} | {sel_loc}", font=dict(size=20))
        )
        
        fig.add_hline(y=32, line_dash="dash", line_color="blue", annotation_text="32°F")
        st.plotly_chart(fig, width='stretch')
        
        # --- 💡 FIX: Added unique KEY to stop Duplicate ID Error ---
        st.download_button(
            label="📥 Download Diagnostic View (CSV)", 
            data=plot_df.dropna(subset=['value']).to_csv(index=False).encode('utf-8'), 
            file_name=f"Diagnostic_{sel_proj}.csv",
            key="btn_diagnostic_view" # UNIQUE KEY
        )
    else:
        st.info("No data found for this selection.")

# --- SERVICE 2: DATA EXPORT LAB (RESTORED) ---
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
        sel_ex_loc = st.selectbox("Location Filter", ex_locs)
        if sel_ex_loc != "All Locations": ex_df = ex_df[ex_df['Location'] == sel_ex_loc]
    with s_col3:
        ex_nodes = ["All Nodes"] + sorted(ex_df['nodenumber'].unique().tolist())
        sel_ex_node = st.selectbox("Node/Serial Filter", ex_nodes)
        if sel_ex_node != "All Nodes": ex_df = ex_df[ex_df['nodenumber'] == sel_ex_node]

    final_ex_df = ex_df[(ex_df['timestamp'].dt.date >= start_d) & (ex_df['timestamp'].dt.date <= end_d)]
    st.write(f"📊 Found **{len(final_ex_df)}** records.")
    st.dataframe(final_ex_df.head(100), width='stretch')
    
    if not final_ex_df.empty:
        csv_bulk = final_ex_df.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Download Bulk Export (CSV)", data=csv_bulk, file_name=f"SoilFreeze_Export_{sel_ex_proj}.csv")

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
        clean_start = st.date_input("Clean Start Date", value=date.today() - timedelta(days=2))
    with r_col2:
        clean_end = st.date_input("Clean End Date", value=date.today())

    clean_view_df = full_df[(full_df['Project'] == sel_c_proj) & (full_df['timestamp'].dt.date >= clean_start) & (full_df['timestamp'].dt.date <= clean_end)].copy()
    if sel_c_loc != "All Locations": clean_view_df = clean_view_df[clean_view_df['Location'] == sel_c_loc]

    st.subheader("1. Highlight 'Spikes' on Graph")
    fig_clean = px.scatter(clean_view_df, x='timestamp', y='value', color='nodenumber', range_y=[-40, 100], height=600)
    fig_clean.update_layout(dragmode='select', selectionrevision=True)
    event_data = st.plotly_chart(fig_clean, width='stretch', on_select="rerun")

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
                sql = f"DELETE FROM `sensor_data` WHERE Project = '{sel_c_proj}' AND timestamp IN ({time_list})"
                st.code(sql, language="sql")
                st.success("SQL generated. Execute in BigQuery.")
