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
    st.header("🏠 Site Health & Warming Alerts")
    
    # 1. PROJECT SELECTOR
    all_projs = sorted([p for p in full_df['Project'].unique() if p is not None])
    sel_summary_proj = st.selectbox("Select Project to Audit", all_projs)
    
    # 2. DATA WINDOW (Last 24 Hours)
    cutoff_24h = datetime.now(tz=pytz.UTC) - timedelta(hours=24)
    proj_recent = full_df[
        (full_df['Project'] == sel_summary_proj) & 
        (full_df['timestamp'] >= cutoff_24h)
    ].copy()

    if not proj_recent.empty:
        # 3. OFFLINE SENSORS SECTION
        cutoff_6h = datetime.now(tz=pytz.UTC) - timedelta(hours=6)
        last_seen = proj_recent.groupby(['nodenumber', 'Depth'])['timestamp'].max().reset_index()
        offline = last_seen[last_seen['timestamp'] < cutoff_6h]
        
        if not offline.empty:
            st.error(f"⚠️ {len(offline)} SENSORS OFFLINE (No data in 6+ hours)")
            st.dataframe(offline.rename(columns={'timestamp': 'Last Seen'}), width='stretch')
        else:
            st.success("✅ All Sensors Online")

        st.subheader(f"📊 24-Hour Pipe Performance: Project {sel_summary_proj}")

        # 4. LOCATION (PIPE/BANK) ANALYSIS
        locations = sorted([l for l in proj_recent['Location'].unique() if l is not None])
        
        for loc in locations:
            st.markdown(f"### 📍 {loc}")
            loc_data = proj_recent[proj_recent['Location'] == loc].copy()
            
            # Calculate 24h Delta for each node
            # We compare the first reading of the 24h window to the last reading
            node_analysis = []
            for node in loc_data['nodenumber'].unique():
                n_df = loc_data[loc_data['nodenumber'] == node].sort_values('timestamp')
                if len(n_df) > 1:
                    depth = n_df['Depth'].iloc[0]
                    first_val = n_df['value'].iloc[0]
                    last_val = n_df['value'].iloc[-1]
                    change = last_val - first_val # Positive = Warming, Negative = Cooling
                    
                    node_analysis.append({
                        "Depth/Name": f"Depth: {depth} ({node})",
                        "Min Temp": f"{n_df['value'].min():.2f}°F",
                        "Max Temp": f"{n_df['value'].max():.1f}°F",
                        "Current": f"{last_val:.1f}°F",
                        "24h Change": change
                    })

            # Create Table
            summary_table = pd.DataFrame(node_analysis)

            # 5. CONDITIONAL FORMATTING LOGIC
            def style_warming(row):
                val = row['24h Change']
                if val >= 5.0:
                    return ['background-color: #ff4b4b; color: white'] * len(row) # RED
                elif val >= 2.5:
                    return ['background-color: #ffa500; color: black'] * len(row) # ORANGE
                elif val >= 1.0:
                    return ['background-color: #ffff00; color: black'] * len(row) # YELLOW
                elif val < -1.0:
                    return ['background-color: #90ee90; color: black'] * len(row) # GREEN (Cooling)
                else:
                    return [''] * len(row) # Normal

            if not summary_table.empty:
                # Apply styles and format the Change column for display
                styled_df = summary_table.style.apply(style_warming, axis=1).format({"24h Change": "{:+.2f}°F"})
                st.table(styled_df)
            else:
                st.info(f"Insufficient data for {loc} to calculate 24h change.")
            
            st.divider()

    else:
        st.info(f"No data reported for Project {sel_summary_proj} in the last 24 hours.")

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
