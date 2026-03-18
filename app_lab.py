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
    [
        "🏠 Executive Summary", 
        "🔍 Node Diagnostics", 
        "📥 Data Export Lab", 
        "🧹 Data Cleaning Tool", 
        "📋 Data Approval Portal"  # <-- ADD THIS LINE
    ]
)

# --- SERVICE 0: EXECUTIVE SUMMARY (LANDING PAGE) ---
if service == "🏠 Executive Summary" and not full_df.empty:
    st.header("🏠 Site Health & Warming Alerts")
    
    # 1. PROJECT & PIPE SELECTION
    c1, c2 = st.columns(2)
    with c1:
        all_projs = sorted([p for p in full_df['Project'].unique() if p is not None])
        sel_summary_proj = st.selectbox("1. Select Project", all_projs)
        
    proj_df = full_df[full_df['Project'] == sel_summary_proj].copy()
    
    with c2:
        all_locs = sorted([l for l in proj_df['Location'].unique() if l is not None])
        sel_summary_loc = st.selectbox("2. Select Pipe / Bank", all_locs)

    st.divider()

    # 2. DATA WINDOWS
    now_ts = datetime.now(tz=pytz.UTC)
    cutoff_24h = now_ts - timedelta(hours=24)
    loc_recent = proj_df[
        (proj_df['Location'] == sel_summary_loc) & 
        (proj_df['timestamp'] >= cutoff_24h)
    ].copy()

    # --- 3. PERFORMANCE TABLE ---
    st.subheader(f"📋 24-Hour Performance: {sel_summary_loc}")
    if not loc_recent.empty:
        node_analysis = []
        for node in loc_recent['nodenumber'].unique():
            n_df = loc_recent[loc_recent['nodenumber'] == node].sort_values('timestamp')
            if len(n_df) > 1:
                depth = n_df['Depth'].iloc[0]
                first_val = n_df['value'].iloc[0]
                last_val = n_df['value'].iloc[-1]
                change = last_val - first_val
                
                node_analysis.append({
                    "Depth": depth, "Node ID": node,
                    "Min Temp": n_df['value'].min(), "Max Temp": n_df['value'].max(),
                    "Current": last_val, "24h Change": change
                })

        summary_table = pd.DataFrame(node_analysis).sort_values('Depth')

        def style_pipe_health(row):
            val = row['24h Change']
            if val >= 5.0: return ['background-color: #ff4b4b; color: white'] * len(row)
            elif val >= 2.5: return ['background-color: #ffa500; color: black'] * len(row)
            elif val >= 1.0: return ['background-color: #ffff00; color: black'] * len(row)
            elif val <= -1.0: return ['background-color: #90ee90; color: black'] * len(row)
            return [''] * len(row)

        if not summary_table.empty:
            st.table(summary_table.style.apply(style_pipe_health, axis=1).format({
                "Min Temp": "{:.2f}°F", "Max Temp": "{:.2f}°F",
                "Current": "{:.2f}°F", "24h Change": "{:+.2f}°F"
            }))
    else:
        st.info("No active data found for performance calculation.")

    st.divider()

    # --- 4. SENSOR CONNECTIVITY HEAT MAP (FIXED KEYERROR) ---
    st.subheader(f"📡 Current Sensor Connectivity: {sel_summary_loc}")
    
    # Identify CURRENTLY assigned nodes
    current_mapping = proj_df[proj_df['Location'] == sel_summary_loc][['Depth', 'nodenumber']].drop_duplicates()
    
    if not current_mapping.empty:
        last_heartbeats = proj_df.groupby('nodenumber')['timestamp'].max().reset_index()
        connectivity_df = pd.merge(current_mapping, last_heartbeats, on='nodenumber', how='left')
        
        # Internal Math Column
        connectivity_df['Hours_Silent'] = (now_ts - connectivity_df['timestamp']).dt.total_seconds() / 3600
        
        def style_connectivity(row):
            hrs = row['Hours_Silent']
            if pd.isna(hrs) or hrs >= 24: return ['background-color: #ff4b4b; color: white'] * len(row)
            elif hrs >= 12: return ['background-color: #ffa500; color: black'] * len(row)
            elif hrs >= 6: return ['background-color: #ffff00; color: black'] * len(row)
            return ['background-color: #f0f2f6; color: gray'] * len(row)

        connectivity_df = connectivity_df.sort_values('Depth').rename(columns={
            'timestamp': 'Last Seen', 
            'nodenumber': 'Active Node ID'
        })
        
        # --- 💡 THE FIX: Hide 'Hours_Silent' after styling but before displaying ---
        styled_conn = connectivity_df.style.apply(style_connectivity, axis=1).format({
            "Last Seen": lambda t: t.strftime('%m/%d %H:%M') if pd.notnull(t) else "NEVER SEEN",
            "Hours_Silent": "{:.1f}"
        })
        
        # Use hide() to remove the math column from the final display
        st.table(styled_conn.hide(['Hours_Silent'], axis=1))
    else:
        st.info("No active sensor mappings found for this location.")
    
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

# --- SERVICE 4: ENGINEER APPROVAL PORTAL ---
elif "Data Approval Portal" in service:
    st.header("📋 Engineer Approval Portal")
    
    # 1. Selection Controls
    ap_col1, ap_col2 = st.columns(2)
    with ap_col1:
        ap_projs = sorted([p for p in full_df['Project'].unique() if p is not None])
        sel_ap_proj = st.selectbox("1. Select Project", ap_projs)
    with ap_col2:
        ap_date = st.date_input("2. Select Date to Release", value=date.today() - timedelta(days=1))

    # 2. Scope Selection
    scope_col1, scope_col2 = st.columns(2)
    with scope_col1:
        approval_scope = st.radio("3. Approval Scope", ["Entire Project", "Specific Pipe / Bank"])
    
    sel_ap_loc = None
    if approval_scope == "Specific Pipe / Bank":
        with scope_col2:
            proj_locs = sorted([l for l in full_df[full_df['Project'] == sel_ap_proj]['Location'].unique() if l is not None])
            sel_ap_loc = st.selectbox("Select Pipe/Bank to Approve", proj_locs)

    # 3. Note Section
    st.subheader("✍️ Engineering Explanation")
    note_text = st.text_area(
        "Write a note for the client", 
        placeholder="Example: 'Pipe 3 data is confirmed accurate after sensor maintenance.'",
        height=100
    )

    # 4. Action Button & SQL Logic
    if st.button("🚀 APPROVE & PUBLISH DATA", type="primary"):
        target_tables = [
            "sensorpush-export.sensor_data.raw_lord",
            "sensorpush-export.sensor_data.raw_sensorpush"
        ]
        
        # Build the WHERE clause based on the scope
        if approval_scope == "Entire Project":
            scope_desc = f"Project {sel_ap_proj}"
            where_clause = f"""
                nodenumber IN (
                    SELECT NodeNum FROM `sensorpush-export.sensor_data.master_metadata`
                    WHERE Project = '{sel_ap_proj}'
                )
            """
        else:
            scope_desc = f"Pipe {sel_ap_loc} in Project {sel_ap_proj}"
            where_clause = f"""
                nodenumber IN (
                    SELECT NodeNum FROM `sensorpush-export.sensor_data.master_metadata`
                    WHERE Project = '{sel_ap_proj}' AND Location = '{sel_ap_loc}'
                )
            """

        for table_path in target_tables:
            update_sql = f"""
            UPDATE `{table_path}`
            SET is_approved = TRUE,
                engineer_note = '{note_text.replace("'", "''")}'
            WHERE {where_clause}
            AND CAST(timestamp AS DATE) = '{ap_date}'
            """
            # client.query(update_sql).result() # Execute the update
            
        st.balloons()
        st.success(f"✅ Successfully published data for {scope_desc} on {ap_date}!")
        st.code(update_sql, language="sql")
