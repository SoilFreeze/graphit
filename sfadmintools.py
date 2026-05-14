import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import time
import re

# 1. CONFIGURATION
st.set_page_config(page_title="SF Engineering Admin", page_icon="🛠️", layout="wide")

DATASET_ID = "Temperature" 
PROJECT_ID = "sensorpush-export"

# 2. DATABASE CLIENT
@st.cache_resource
def get_bq_client():
    try:
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(info)
            return bigquery.Client(credentials=credentials, project=info["project_id"])
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"❌ Database Link Offline: {e}")
        return None

client = get_bq_client()

# 3. SIDEBAR NAVIGATION
st.sidebar.title("🛠️ Admin Command Center")

# Renamed labels for practical engineering use
admin_page = st.sidebar.radio("Management Tool", [
    "📡 Setup Node Tool", 
    "🔍 Sensor Status",
    "🔄 Sensor Replace",      
    "🩹 Sensor Switch",       
    "📝 Sensor Edit",         
    "📦 Bulk Registry Manager",
    "📡 Data Recovery",
    "⚙️ Project Master", 
    "📈 Ref Curve Library", 
    "🧨 Data Management"
])

is_dev = st.sidebar.toggle("🧪 Use Registry Playground", value=True)
TARGET_REGISTRY = f"{PROJECT_ID}.{DATASET_ID}.node_registry" + ("_dummy" if is_dev else "")

# Global Project Selection
proj_q = f"SELECT Project FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE ProjectStatus != 'Archived'"
proj_list = sorted(client.query(proj_q).to_dataframe()['Project'].tolist())
selected_project = st.sidebar.selectbox("🎯 Target Project Context", proj_list)

# 4. GLOBAL DATA LOAD (Registry)
try:
    reg_df = client.query(f"SELECT * FROM `{TARGET_REGISTRY}`").to_dataframe()
except:
    reg_df = pd.DataFrame()

# ===============================================================
# PAGE: SETUP NODE TOOL
# ===============================================================
if admin_page == "📡 Setup Node Tool":
    st.header(f"🏗️ Setup Node Tool: {selected_project}")

    # --- 1. LAB-GRADE DASHBOARD SUMMARY ---
    # Pulls 48h of data to calculate trends and handle stale fallback
    dashboard_q = f"""
        WITH raw_data AS (
            SELECT 
                n.NodeNum, n.Bank, n.Location, n.Depth, m.temperature, m.timestamp
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
            JOIN `{PROJECT_ID}.{DATASET_ID}.node_registry` n ON m.NodeNum = n.NodeNum
            WHERE n.Project = @proj_id 
              AND m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
        )
        SELECT 
            Bank, Location, Depth,
            AVG(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN temperature END) as avg_now,
            AVG(CASE WHEN timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN temperature END) as avg_1h,
            AVG(CASE WHEN timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR) THEN temperature END) as avg_6h,
            AVG(CASE WHEN timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 25 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature END) as avg_24h,
            MIN(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature END) as min_24h,
            MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature END) as max_24h,
            ARRAY_AGG(temperature ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] as latest_temp,
            MAX(timestamp) as latest_ts
        FROM raw_data
        GROUP BY 1, 2, 3
    """
    
    dash_df = client.query(dashboard_q, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)]
    )).to_dataframe()

    if not dash_df.empty:
        dash_df[['Bank', 'Location']] = dash_df[['Bank', 'Location']].fillna('')
        now_utc = pd.Timestamp.now(tz='UTC')

        # Classification Logic (from your company lab app)
        is_amb = dash_df['Bank'].str.contains('Amb', case=False) | dash_df['Location'].str.contains('Amb', case=False)
        is_s = (dash_df['Bank'].str.startswith('S') | dash_df['Location'].str.startswith('S')) & ~is_amb
        is_r = (dash_df['Bank'].str.startswith('R') | dash_df['Location'].str.startswith('R')) & ~is_amb
        is_tp = dash_df['Depth'].notnull() & ~is_s & ~is_r & ~is_amb

        st.subheader("📊 Project Status Summary")
        cols = st.columns(4)
        groups = [
            (cols[0], "📥 Supply", dash_df[is_s]), 
            (cols[1], "📤 Return", dash_df[is_r]), 
            (cols[2], "📏 TempPipes", dash_df[is_tp]), 
            (cols[3], "☁️ Ambient", dash_df[is_amb])
        ]

        for col, title, g_df in groups:
            with col:
                st.markdown(f"#### {title}")
                if g_df.empty:
                    st.error("No recent data (24h+)")
                else:
                    avg_now = g_df['avg_now'].mean()
                    latest_val = g_df['latest_temp'].mean()
                    latest_time = g_df['latest_ts'].max()
                    
                    # Lag calculation
                    ts_check = latest_time if latest_time.tzinfo else latest_time.tz_localize('UTC')
                    lag_hrs = (now_utc - ts_check).total_seconds() / 3600
                    
                    is_stale = pd.isnull(avg_now)
                    val = latest_val if is_stale else avg_now
                    
                    # Rendering
                    if is_stale and pd.notnull(lag_hrs):
                        st.subheader(f"⚠️ Offline {int(lag_hrs)}h")
                    else:
                        st.title(f"{val:.1f}°F")
                    
                    st.caption(f"Range: {g_df['min_24h'].min():.1f}° to {g_df['max_24h'].max():.1f}°")
                    
                    # Trends (using your helper arrow function)
                    t_row = st.columns(3)
                    t_row[0].caption(f"1h\n{get_trend_arrow(val, g_df['avg_1h'].mean())}")
                    t_row[1].caption(f"6h\n{get_trend_arrow(val, g_df['avg_6h'].mean())}")
                    t_row[2].caption(f"24h\n{get_trend_arrow(val, g_df['avg_24h'].mean())}")

    st.divider()

    # --- 2. HARDWARE INTEGRITY TABLE ---
st.subheader("📋 Hardware Integrity & Connectivity")

# Fetch data with COALESCE to prevent pd.NA errors
audit_q = f"""
    WITH RawData AS (
        SELECT NodeNum, timestamp, temperature,
        LAG(timestamp) OVER (PARTITION BY NodeNum ORDER BY timestamp ASC) as prev_ts
        FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view`
        WHERE Project = @proj_id AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
    ),
    Stats AS (
        SELECT 
            NodeNum, 
            COALESCE(MAX(TIMESTAMP_DIFF(timestamp, prev_ts, MINUTE)), 0) as max_gap_mins,
            COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)) as pings_6h,
            COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) as pings_24h
        FROM RawData GROUP BY NodeNum
    ),
    Latest AS (
        SELECT NodeNum, MAX(timestamp) as last_ping,
        ARRAY_AGG(temperature ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] as last_temp
        FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view`
        WHERE Project = @proj_id GROUP BY NodeNum
    )
    SELECT 
        n.NodeNum, n.Location, n.Bank, n.Depth, n.SensorStatus, 
        l.last_ping, l.last_temp,
        s.pings_6h, s.pings_24h, s.max_gap_mins
    FROM `{PROJECT_ID}.{DATASET_ID}.node_registry` n
    LEFT JOIN Latest l ON n.NodeNum = l.NodeNum
    LEFT JOIN Stats s ON n.NodeNum = s.NodeNum
    WHERE n.Project = @proj_id
"""

df = client.query(audit_q, job_config=bigquery.QueryJobConfig(
    query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)]
)).to_dataframe()

if not df.empty:
    now_utc = pd.Timestamp.now(tz='UTC')

    # Data Transformation Logic
    def apply_row_logic(row):
        # 1. Connectivity Status (Last Seen)
        ping = row['last_ping']
        if pd.isnull(ping):
            seen_txt, seen_color = "Not Seen", "background-color: #d3d3d3"
        else:
            diff = (now_utc - (ping if ping.tzinfo else ping.tz_localize('UTC'))).total_seconds() / 60
            if diff <= 65: 
                seen_txt, seen_color = f"{int(diff)}m ago", "background-color: #ccffcc; color: black"
            else: 
                seen_txt, seen_color = f"{round(diff/60, 1)}h ago", "background-color: #ffe4b5; color: black"
        
        # 2. Position Labeling
        pos_txt = f"{row['Depth']}ft" if pd.notnull(row['Depth']) else f"Bank {row['Bank']}"
        
        return pd.Series([pos_txt, seen_txt, seen_color])

    df[['Position', 'Last Seen', 'SeenStyle']] = df.apply(apply_row_logic, axis=1)

    # Prepare Display DataFrame
    display_df = df[[
        'NodeNum', 'Location', 'Position', 'Last Seen', 
        'pings_6h', 'pings_24h', 'max_gap_mins'
    ]].rename(columns={
        'NodeNum': 'Node ID',
        'pings_6h': '6h Coverage',
        'pings_24h': '24h Coverage',
        'max_gap_mins': 'Max Gap'
    })

    # --- THE STYLING ENGINE ---
    # This function handles the red cell for 'Diagnostic'
    def diagnostic_styler(data):
        # Create a blank dataframe of styles
        style_df = pd.DataFrame('', index=data.index, columns=data.columns)
        
        # Apply red background to 'Node ID' column where SensorStatus is Diagnostic
        # We use the original 'df' to check the status
        diagnostic_mask = df['SensorStatus'] == 'Diagnostic'
        style_df.loc[diagnostic_mask, 'Node ID'] = 'background-color: #ff4b4b; color: white; font-weight: bold;'
        
        # Apply the pre-calculated 'SeenStyle' to the 'Last Seen' column
        style_df['Last Seen'] = df['SeenStyle']
        
        return style_df

    # Final Render
    st.dataframe(
        display_df.style.apply(diagnostic_styler, axis=None), 
        use_container_width=True, 
        hide_index=True
    )
# ===============================================================
# PAGE: SENSOR STATUS (With Location Drill-Down)
# ===============================================================
elif admin_page == "🔍 Sensor Status":
    st.header("🔍 Sensor Status & Performance Overview")
    
    # Direct production target
    TARGET_REGISTRY = f"{PROJECT_ID}.{DATASET_ID}.node_registry"

    # --- 1. FLEET INVENTORY METRICS ---
    if not reg_df.empty:
        reg_df['End_Date'] = pd.to_datetime(reg_df['End_Date'], errors='coerce')
        active_mask = reg_df['End_Date'].isna()
        
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Unique Sensors", reg_df['NodeNum'].nunique())
        m2.metric("Currently Assigned", len(reg_df[active_mask & (reg_df['Project'] != 'Office')]))
        m3.metric("Available In Stock", len(reg_df[active_mask & (reg_df['Project'] == 'Office')]))
        m4.metric("Diagnostic/Dead", len(reg_df[active_mask & reg_df['SensorStatus'].isin(['Dead', 'Flagged', 'Diagnostic'])]))
    
    st.divider()

    # --- 2. LOCATION DRILL-DOWN MENU ---
    st.subheader(f"📍 Location Drill-Down: {selected_project}")
    
    # Fetch all active nodes for the project grouped by location
    loc_q = f"""
        SELECT 
            n.Location, 
            COUNT(n.NodeNum) as pipe_count,
            AVG(m.temperature) as avg_temp,
            COUNTIF(m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)) as seen_6h
        FROM `{TARGET_REGISTRY}` n
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.master_data_view` m ON n.NodeNum = m.NodeNum
        WHERE n.Project = @proj_id AND n.End_Date IS NULL
        GROUP BY n.Location
    """
    
    loc_df = client.query(loc_q, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)]
    )).to_dataframe()

    if loc_df.empty:
        st.info("No active locations found for this project.")
    else:
        # Create an expander for each location (The Drill-Down)
        for _, row in loc_df.iterrows():
            loc_name = row['Location']
            avg_t = f"{row['avg_temp']:.1f}°F" if pd.notnull(row['avg_temp']) else "N/A"
            
            with st.expander(f"📁 {loc_name} | {row['pipe_count']} Pipes | Avg: {avg_t}"):
                # Inside the expander, show individual pipes (nodes)
                pipe_df = reg_df[(reg_df['Project'] == selected_project) & 
                                 (reg_df['Location'] == loc_name) & 
                                 (reg_df['End_Date'].isna())]
                
                # Clean up display for individual pipe details
                display_pipes = pipe_df[['NodeNum', 'Bank', 'Depth', 'SensorStatus']].rename(
                    columns={'NodeNum': 'Pipe ID', 'SensorStatus': 'Status'}
                )
                
                # Apply styling for diagnostic nodes directly in the drill-down
                def highlight_diagnostic(val):
                    color = 'red' if val == 'Diagnostic' else 'black'
                    return f'color: {color}'

                st.dataframe(
                    display_pipes.style.map(highlight_diagnostic, subset=['Status']),
                    use_container_width=True,
                    hide_index=True
                )

    st.divider()

    # --- 3. HARDWARE INVESTIGATOR (Global Search) ---
    st.subheader("🔦 Global Hardware Investigator")
    search_node = st.text_input("Quick Search Node ID (e.g., TP-0009)").strip().upper()
    
    if search_node:
        match = reg_df[reg_df['NodeNum'].astype(str).str.upper() == search_node]
        
        if match.empty:
            st.error(f"Node '{search_node}' not found in registry.")
        else:
            # A. Current Status
            curr = match[match['End_Date'].isna()]
            if not curr.empty:
                st.info(f"📍 **Current Assignment:** {curr.iloc[0]['Project']} | {curr.iloc[0]['Location']} ({curr.iloc[0]['SensorStatus']})")
            
            # B. History Table
            st.markdown("### 📜 Deployment History")
            history_q = f"""
                SELECT Project, Location, Start_Date, End_Date, SensorStatus,
                (SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m 
                 WHERE m.NodeNum = r.NodeNum AND m.timestamp BETWEEN CAST(r.Start_Date AS TIMESTAMP) AND IFNULL(CAST(r.End_Date AS TIMESTAMP), CURRENT_TIMESTAMP())) as pings
                FROM `{TARGET_REGISTRY}` r WHERE NodeNum = '{search_node}' ORDER BY Start_Date DESC
            """
            st.dataframe(client.query(history_q).to_dataframe(), use_container_width=True, hide_index=True)

            # C. Lifetime Graph
            st.markdown("### 📈 Lifetime Thermal Profile")
            tel_df = client.query(f"SELECT timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` WHERE NodeNum = '{search_node}' ORDER BY timestamp ASC").to_dataframe()
            if not tel_df.empty:
                fig = go.Figure(go.Scatter(x=tel_df['timestamp'], y=tel_df['temperature'], mode='lines', line=dict(color='#00d4ff')))
                fig.update_layout(height=300, template="plotly_dark", margin=dict(l=10, r=10, t=10, b=10))
                st.plotly_chart(fig, use_container_width=True)
        pass
        
    # --- 4. REGISTRY HEALTH ---
    with st.expander("🛠️ Registry Integrity Check"):
        health_df = client.query(f"SELECT NodeNum, PhysicalID, Project, Start_Date FROM `{TARGET_REGISTRY}` WHERE Start_Date IS NULL").to_dataframe()
        if health_df.empty:
            st.success("✅ Registry Integrity looks good!")
        else:
            st.warning("⚠️ Found orphaned records (Missing Start Dates):")
            st.dataframe(health_df, use_container_width=True)
  

# ===============================================================
# PAGE: SENSOR REPLACE (Physical Swap Logic)
# ===============================================================
elif admin_page == "🔄 Sensor Replace":
    st.header("🔄 Sensor Replace")
    st.info("Use this tool when physically swapping out hardware on-site while maintaining the same Node ID.")
    
    # Production Target
    TARGET_REGISTRY = f"{PROJECT_ID}.{DATASET_ID}.node_registry"
    
    # 1. LOAD ACTIVE INVENTORY
    # We only care about sensors that don't have an End_Date yet
    active_reg = client.query(f"SELECT * FROM `{TARGET_REGISTRY}` WHERE End_Date IS NULL").to_dataframe()

    # 2. SEARCH & IDENTIFY
    search_node = st.text_input("🔍 Search Node ID or Current Serial", placeholder="e.g., TP-0001 or 12345.67")
    found_row = None
    
    if search_node:
        search_clean = str(search_node).strip().upper()
        match = active_reg[
            (active_reg['NodeNum'].astype(str).str.upper().str.contains(search_clean)) | 
            (active_reg['PhysicalID'].astype(str).str.contains(search_clean))
        ]
        if not match.empty:
            found_row = match.iloc[0]
            st.success(f"📍 Node identified at: **{found_row['Project']} | {found_row['Location']}**")
        else:
            st.error("No active record found matching that ID.")

    if found_row is not None:
        st.divider()
        st.subheader(f"⚡ Verification: {found_row['NodeNum']}")
        
        # 3. VISUAL OVERLAP CHECK
        col_g1, col_g2 = st.columns(2)
        
        with col_g1:
            st.markdown(f"**Old Hardware** (S/N: {found_row['PhysicalID']})")
            old_q = f"SELECT timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` WHERE NodeNum = '{found_row['NodeNum']}' AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 3 DAY ORDER BY timestamp"
            old_data = client.query(old_q).to_dataframe()
            if not old_data.empty:
                fig_old = go.Figure(go.Scatter(x=old_data['timestamp'], y=old_data['temperature'], name="Old Node", line=dict(color='#888888')))
                fig_old.update_layout(height=200, margin=dict(t=0,b=0), template="plotly_dark")
                st.plotly_chart(fig_old, use_container_width=True)

        new_sn = st.text_input("Enter NEW Hardware Serial Number (Physical ID)")

        with col_g2:
            if new_sn:
                st.markdown(f"**New Hardware** (S/N: {new_sn})")
                new_q = f"SELECT timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` WHERE SAFE_CAST(PhysicalID AS STRING) LIKE '%{new_sn}%' AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 3 DAY ORDER BY timestamp"
                new_data = client.query(new_q).to_dataframe()
                if not new_data.empty:
                    fig_new = go.Figure(go.Scatter(x=new_data['timestamp'], y=new_data['temperature'], name="New Node", line=dict(color='orange')))
                    fig_new.update_layout(height=200, margin=dict(t=0,b=0), template="plotly_dark")
                    st.plotly_chart(fig_new, use_container_width=True)
                else:
                    st.caption("No recent telemetry seen for this new Serial Number yet.")

        # 4. FINAL TRANSACTION FORM
        st.divider()
        with st.form("replacement_commit_form"):
            st.write("### 🚀 Commit Hardware Swap")
            st.warning("This will end the current record and create a new assignment for this Node ID.")
            
            replace_date = st.date_input("Actual Swap Date", value=datetime.now().date())
            confirm_check = st.checkbox("I verify the new hardware is communicating and the old hardware is removed.")
            
            if st.form_submit_button("EXECUTE REPLACEMENT"):
                # Clean the Serial Number (remove non-numeric chars for FLOAT64 compatibility)
                clean_sn = re.sub(r'[^0-9.]', '', str(new_sn))
                
                if not clean_sn or not confirm_check:
                    st.error("Missing Serial Number or verification checkbox.")
                else:
                    try:
                        date_str = replace_date.isoformat()
                        sql = f"""
                        BEGIN TRANSACTION;
                        
                        -- 1. Close old assignment
                        UPDATE `{TARGET_REGISTRY}` 
                        SET End_Date = DATE('{date_str}'), 
                            SensorStatus = 'Replaced' 
                        WHERE NodeNum = '{found_row['NodeNum']}' 
                          AND Project = '{found_row['Project']}'
                          AND End_Date IS NULL;
                        
                        -- 2. Open new assignment
                        INSERT INTO `{TARGET_REGISTRY}` 
                        (NodeNum, PhysicalID, Project, Location, Bank, Depth, Start_Date, SensorStatus)
                        VALUES (
                            '{found_row['NodeNum']}', 
                            SAFE_CAST('{clean_sn}' AS FLOAT64), 
                            '{found_row['Project']}', 
                            '{found_row['Location']}', 
                            '{found_row.get('Bank', '')}', 
                            {float(found_row['Depth']) if pd.notnull(found_row['Depth']) else 'NULL'}, 
                            DATE('{date_str}'), 
                            'Active'
                        );
                        
                        COMMIT;
                        """
                        client.query(sql).result()
                        st.success(f"Successfully replaced {found_row['NodeNum']} with S/N {clean_sn}")
                        st.balloons()
                        time.sleep(1.5)
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Hardware Replacement Failed: {e}")

# ===============================================================
# PAGE: SENSOR SWITCH (Correction Logic)
# ===============================================================
elif admin_page == "🩹 Sensor Switch":
    st.header("🩹 Sensor Designation Switch")
    st.info("""
        **Purpose:** Use this for metadata corrections only (e.g., a typo during setup). 
        This will update the existing active record without changing start dates or history.
    """)
    
    # Production Target
    TARGET_REGISTRY = f"{PROJECT_ID}.{DATASET_ID}.node_registry"
    
    node_id = st.text_input("Enter Node ID to correct (e.g., TP-0001)").strip().upper()
    
    if node_id:
        # We only want to switch IDs for active assignments
        query = f"SELECT * FROM `{TARGET_REGISTRY}` WHERE NodeNum = '{node_id}' AND End_Date IS NULL"
        df = client.query(query).to_dataframe()
        
        if not df.empty:
            row = df.iloc[0]
            
            # Show current configuration for verification
            st.subheader(f"Current Config: {node_id}")
            c1, c2, c3 = st.columns(3)
            c1.write(f"**Project:** {row['Project']}")
            c2.write(f"**Location:** {row['Location']}")
            c3.write(f"**Physical ID:** `{row['PhysicalID']}`")
            
            st.divider()
            
            # Correction Input
            new_id = st.text_input("Enter Corrected Physical ID (Serial Number)")
            
            if st.button("🚀 Apply Designation Correction"):
                if not new_id:
                    st.warning("Please provide a new Physical ID.")
                else:
                    # Clean the input to ensure it's numeric for FLOAT64 column
                    clean_id = re.sub(r'[^0-9.]', '', str(new_id))
                    
                    update_sql = f"""
                        UPDATE `{TARGET_REGISTRY}`
                        SET PhysicalID = SAFE_CAST('{clean_id}' AS FLOAT64)
                        WHERE NodeNum = '{node_id}' 
                          AND Project = '{row['Project']}'
                          AND End_Date IS NULL
                    """
                    
                    try:
                        with st.spinner("Correcting designation..."):
                            client.query(update_sql).result()
                        st.success(f"Successfully updated {node_id} to Physical ID: {clean_id}")
                        time.sleep(1.5)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Update failed: {e}")
        else:
            st.error(f"No active record found for Node '{node_id}'. Verify the ID or check the Sensor Status page.")
                
# ===============================================================
# PAGE: SENSOR EDIT (Interactive Registry Editor)
# ===============================================================
elif admin_page == "📝 Sensor Edit":
    st.header("📝 Sensor Edit")
    st.info("Use this tool to modify location metadata or remove incorrect registry entries.")
    
    TARGET_REGISTRY = f"{PROJECT_ID}.{DATASET_ID}.node_registry"

    # 1. Advanced Filtering UI
    st.subheader("🔍 Find & Select Record")
    
    # New Archival Toggle
    show_archived = st.checkbox("Show Archived/Historical Data", value=False)
    
    col_f1, col_f2, col_f3, col_f4 = st.columns(4)
    
    with col_f1:
        u_projects = ["All"] + sorted(reg_df['Project'].unique().tolist())
        sel_proj = st.selectbox("Filter by Project", u_projects)
        
    with col_f2:
        # Standardize search to be case-insensitive
        search_node = st.text_input("Search Node ID", "").strip().upper()

    with col_f3:
        # New Location Filter
        u_locs = ["All"] + sorted(reg_df['Location'].unique().tolist())
        sel_loc = st.selectbox("Filter by Location", u_locs)
        
    with col_f4:
        u_status = ["All"] + sorted(reg_df['SensorStatus'].unique().tolist())
        sel_stat = st.selectbox("Filter by Status", u_status)

    # 2. Apply Filtering Logic
    edit_df = reg_df.copy()
    
    # Filter out archived if toggle is off
    if not show_archived:
        edit_df = edit_df[edit_df['End_Date'].isna()]
        edit_df = edit_df[edit_df['SensorStatus'] != "Archived"]

    if sel_proj != "All":
        edit_df = edit_df[edit_df['Project'] == sel_proj]
    if sel_loc != "All":
        edit_df = edit_df[edit_df['Location'] == sel_loc]
    if sel_stat != "All":
        edit_df = edit_df[edit_df['SensorStatus'] == sel_stat]
    if search_node:
        edit_df = edit_df[edit_df['NodeNum'].str.upper().str.contains(search_node)]

    # 3. Interactive Table Selection
    # This allows you to click a row to select it
    st.write(f"Showing **{len(edit_df)}** matching records.")
    
    selected_rows = st.dataframe(
        edit_df,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row"
    )

    # 4. Form Logic for Selected Record
    if len(selected_rows.selection.rows) > 0:
        # Get the actual data for the selected row
        row_index = selected_rows.selection.rows[0]
        data = edit_df.iloc[row_index]
        
        st.divider()
        with st.form("edit_entry_form"):
            st.subheader(f"🛠️ Modifying {data['NodeNum']}")
            st.caption(f"Entry ID: {data['PhysicalID']} | Started: {data['Start_Date']}")
            
            # Edit inputs
            new_loc = st.text_input("Update Location", value=str(data['Location']))
            
            # Dynamic Status Selection
            status_list = ["Active", "Available", "Archived", "Dead", "Diagnostic", "Moved", "Replaced"]
            try:
                current_stat_idx = status_list.index(data['SensorStatus'])
            except ValueError:
                current_stat_idx = 0
                
            new_status = st.selectbox("Update Status", status_list, index=current_stat_idx)
            
            c1, c2 = st.columns(2)
            
            if c1.form_submit_button("💾 Save Changes"):
                update_sql = f"""
                    UPDATE `{TARGET_REGISTRY}`
                    SET Location = '{new_loc}', 
                        SensorStatus = '{new_status}'
                    WHERE NodeNum = '{data['NodeNum']}' 
                      AND Start_Date = DATE('{data['Start_Date']}')
                      AND PhysicalID = {data['PhysicalID']}
                """
                client.query(update_sql).result()
                st.success(f"Successfully updated {data['NodeNum']}")
                time.sleep(1)
                st.rerun()

            if c2.form_submit_button("🗑️ DELETE RECORD", type="primary"):
                delete_sql = f"""
                    DELETE FROM `{TARGET_REGISTRY}` 
                    WHERE NodeNum = '{data['NodeNum']}' 
                      AND Start_Date = DATE('{data['Start_Date']}')
                      AND PhysicalID = {data['PhysicalID']}
                """
                client.query(delete_sql).result()
                st.warning(f"Deleted {data['NodeNum']} record from registry.")
                time.sleep(1)
                st.rerun()
    else:
        st.info("💡 Select a row in the table above to edit its details.")
        
# ===============================================================
# PAGE: DATA RECOVERY (SensorPush API Bridge)
# ===============================================================
elif admin_page == "📡 Data Recovery":
    st.header("📡 Data Recovery")
    st.info("Triggers the Cloud Run service to backfill missing telemetry from the SensorPush API.")

    # 1. GATEWAY: Filter for SensorPush hardware only (TP-Prefix)
    # Lord sensors (usually 5XXXX serials) are excluded from this tool
    sp_reg = reg_df[reg_df['NodeNum'].str.startswith('TP', na=False)].copy()

    # 2. FILTERING UI
    st.subheader("🔍 Select Target Hardware")
    col_f1, col_f2, col_f3 = st.columns(3)
    
    with col_f1:
        u_projects = ["All"] + sorted(sp_reg['Project'].unique().tolist())
        rec_proj = st.selectbox("Filter by Project", u_projects)
    
    # Filter the subset based on project for the next dropdown
    proj_filtered = sp_reg if rec_proj == "All" else sp_reg[sp_reg['Project'] == rec_proj]
    
    with col_f2:
        u_locs = ["All"] + sorted(proj_filtered['Location'].unique().tolist())
        rec_loc = st.selectbox("Filter by Location", u_locs)
        
    with col_f3:
        # Final list based on Project + Location filters
        loc_filtered = proj_filtered if rec_loc == "All" else proj_filtered[proj_filtered['Location'] == rec_loc]
        available_nodes = sorted(loc_filtered['NodeNum'].unique().tolist())
        
        selected_nodes = st.multiselect(
            "Select Node Numbers", 
            available_nodes, 
            default=available_nodes if len(available_nodes) < 10 else None,
            help="Choose the specific sensors to backfill."
        )

    # 3. DATE RANGE & TRIGGER
    st.divider()
    c_d1, c_d2 = st.columns(2)
    with c_d1:
        start_date = st.date_input("Recovery Start Date", value=datetime.now() - timedelta(days=3))
    with c_d2:
        end_date = st.date_input("Recovery End Date", value=datetime.now())

    if st.button("🚀 Run Recovery Service", type="primary"):
        if not selected_nodes:
            st.error("Operation Aborted: No sensors selected for recovery.")
        else:
            # Prepare API Request
            cloud_run_url = "https://sensorpushtobigquery-1013288934882.us-west1.run.app/recover_data"
            
            payload = {
                "start": start_date.strftime("%Y-%m-%d"),
                "end": end_date.strftime("%Y-%m-%d"),
                "nodes": ",".join(selected_nodes)
            }

            with st.spinner(f"Requesting data for {len(selected_nodes)} sensors..."):
                try:
                    # Requests isn't standard in your imports yet, adding import check logic:
                    import requests
                    
                    response = requests.get(cloud_run_url, params=payload, timeout=300)
                    
                    if response.status_code == 200:
                        st.success("✅ Recovery Triggered Successfully")
                        st.code(response.text)
                    else:
                        st.error(f"Cloud Service Error ({response.status_code}): {response.text}")
                except ImportError:
                    st.error("System Error: 'requests' library is not installed. Contact Engineering.")
                except Exception as e:
                    st.error(f"Connectivity Failure: {e}")

    # 4. SYSTEM LOGIC FOOTER
    st.divider()
    with st.expander("🛠️ How the Recovery Engine Works"):
        st.markdown(f"""
        1. **Filtered Registry**: This tool only views sensors starting with `TP` (SensorPush).
        2. **API Handshake**: The app sends the `start`, `end`, and `nodes` parameters to a secure GCP Cloud Run endpoint.
        3. **Processing**: Cloud Run fetches raw data from the SensorPush Cloud API and pushes it directly into BigQuery `raw_sensorpush`.
        4. **Verification**: Once finished, data will propagate to the `master_data_view` within minutes.
        """)
    
# ===============================================================
# PAGE: BULK REGISTRY MANAGER
# ===============================================================
elif admin_page == "📦 Bulk Registry Manager":
    st.header("📦 Bulk Registry Operations")
    
    # Direct production target
    TARGET_REGISTRY = f"{PROJECT_ID}.{DATASET_ID}.node_registry"

    bt1, bt2 = st.tabs(["📥 Site Deployment (CSV)", "🔚 Site Decommission"])

    with bt1:
        st.subheader("Initialize New Site Registry")
        st.info("Upload a CSV to register all sensors for a new project at once.")
        
        with st.expander("📊 View Required CSV Format"):
            st.code("NodeNum,PhysicalID,Project,Location,Bank,Depth,Start_Date,SensorStatus")
        
        u_csv = st.file_uploader("Upload Deployment CSV", type="csv")
        
        if u_csv:
            df_upload = pd.read_csv(u_csv)
            st.write("### Preview Data")
            st.dataframe(df_upload.head(), use_container_width=True)
            
            if st.button("🚀 Commit New Project Hardware"):
                try:
                    # Basic validation
                    required = {'NodeNum', 'PhysicalID', 'Project', 'Location'}
                    if not required.issubset(df_upload.columns):
                        st.error(f"Missing required columns: {required - set(df_upload.columns)}")
                    else:
                        with st.spinner("Uploading to BigQuery..."):
                            # Ensure Start_Date is treated as a date string for BQ
                            if 'Start_Date' in df_upload.columns:
                                df_upload['Start_Date'] = pd.to_datetime(df_upload['Start_Date']).dt.date
                            
                            job_config = bigquery.LoadTableConfig(write_disposition="WRITE_APPEND")
                            client.load_table_from_dataframe(df_upload, TARGET_REGISTRY, job_config=job_config).result()
                            
                        st.success(f"Successfully registered {len(df_upload)} nodes to project {df_upload['Project'].iloc[0]}.")
                        st.balloons()
                except Exception as e:
                    st.error(f"Upload Failed: {e}")

    with bt2:
        st.subheader("Project-Wide Decommission")
        st.warning("This action will set an End Date for ALL active sensors on the specified project.")
        
        ret_p = st.selectbox("Select Project to Retire", ["-- Select --"] + proj_list)
        ret_date = st.date_input("Decommission Date", value=datetime.now().date())
        
        if st.button("🔚 Retire All Nodes on Site", type="primary"):
            if ret_p == "-- Select --":
                st.error("Please select a valid Project ID.")
            else:
                try:
                    # Update active sensors to 'Available' and set their End_Date
                    decom_sql = f"""
                        UPDATE `{TARGET_REGISTRY}` 
                        SET End_Date = DATE('{ret_date.isoformat()}'), 
                            SensorStatus = 'Available' 
                        WHERE Project = '{ret_p}' 
                          AND End_Date IS NULL
                    """
                    
                    with st.spinner(f"Retiring Project {ret_p}..."):
                        query_job = client.query(decom_sql)
                        query_job.result()
                        
                    st.success(f"Project {ret_p} has been retired. {query_job.num_dml_affected_rows} sensors moved to 'Available'.")
                except Exception as e:
                    st.error(f"Decommission Failed: {e}")
# ===============================================================
# PAGE: PROJECT MASTER
# ===============================================================
elif admin_page == "⚙️ Project Master":
    st.header("⚙️ Project Lifecycle Management")
    
    # Navigation matching your reference image
    action = st.radio("Action", ["Overview", "New Project", "Update Existing"], horizontal=True)
    
    TABLE_PROJECTS = f"{PROJECT_ID}.{DATASET_ID}.project_registry"

    # --- OPTION 1: OVERVIEW ---
    if action == "Overview":
        st.subheader("📋 Project Fleet Status")
        # Fetch all projects not yet archived
        all_p_q = f"SELECT Project, ProjectStatus, Date_Freezedown, EngNotes FROM `{TABLE_PROJECTS}` WHERE ProjectStatus != 'Archived' ORDER BY Project ASC"
        all_p_df = client.query(all_p_q).to_dataframe()
        
        if not all_p_df.empty:
            st.dataframe(all_p_df, use_container_width=True, hide_index=True)
        else:
            st.info("No active projects found in the registry.")

    # --- OPTION 2: NEW PROJECT ---
    elif action == "New Project":
        st.subheader("🏗️ Register New Project Code")
        with st.form("new_project_form"):
            n_code = st.text_input("Project ID (e.g., 2538)")
            n_notes = st.text_area("Initial Engineering Notes")
            
            if st.form_submit_button("🚀 Initialize Project"):
                if not n_code:
                    st.error("Project ID is required.")
                else:
                    # Check for duplicates
                    check_q = f"SELECT Project FROM `{TABLE_PROJECTS}` WHERE Project = '{n_code}'"
                    if not client.query(check_q).to_dataframe().empty:
                        st.error(f"Project {n_code} already exists in the registry.")
                    else:
                        insert_q = f"""
                            INSERT INTO `{TABLE_PROJECTS}` (Project, ProjectStatus, EngNotes)
                            VALUES ('{n_code}', 'Initialized', '{n_notes}')
                        """
                        client.query(insert_q).result()
                        st.success(f"Project {n_code} successfully initialized.")
                        time.sleep(1)
                        st.rerun()

    # --- OPTION 3: UPDATE EXISTING ---
    elif action == "Update Existing":
        st.subheader(f"⚙️ Modifying: {selected_project}")
        
        # Fetch current data for the sidebar-selected project
        proj_q = f"SELECT * FROM `{TABLE_PROJECTS}` WHERE Project = '{selected_project}'"
        p_res = client.query(proj_q).to_dataframe()
        
        if p_res.empty:
            st.error("Project not found in registry.")
        else:
            p_data = p_res.iloc[0]
            status_options = ["Initialized", "Pre-freeze", "Freezedown", "Maintenance", "Archived"]
            
            # Safe index logic
            current_status = p_data.get('ProjectStatus', 'Initialized')
            try:
                default_status_idx = status_options.index(current_status)
            except ValueError:
                default_status_idx = 0

            with st.form("edit_project"):
                u_status = st.selectbox("Update Lifecycle Status", status_options, index=default_status_idx)
                u_notes = st.text_area("Update Engineering Notes", value=p_data.get('EngNotes', ''))
                
                if st.form_submit_button("💾 Save Project Rules"):
                    # Automated date stamping for Freezedown
                    date_sql = ""
                    if u_status == "Freezedown" and pd.isnull(p_data['Date_Freezedown']):
                        date_sql = ", Date_Freezedown = CURRENT_DATE()"
                    
                    update_q = f"""
                        UPDATE `{TABLE_PROJECTS}` 
                        SET ProjectStatus='{u_status}', EngNotes='{u_notes}' {date_sql} 
                        WHERE Project='{selected_project}'
                    """
                    client.query(update_q).result()
                    st.success(f"✅ Project {selected_project} updated.")
                    time.sleep(1)
                    st.rerun()

# ===============================================================
# PAGE: REF CURVE LIBRARY
# ===============================================================
elif admin_page == "📈 Ref Curve Library":
    st.header("📈 Theoretical Curve Management")
    
    # 1. DATABASE SCHEMA CHECK & INVENTORY FETCH
    inventory_df = pd.DataFrame()
    TABLE_CURVES = f"{PROJECT_ID}.{DATASET_ID}.reference_curves"
    
    try:
        # Check for upload_date column to prevent query errors
        schema_q = f"SELECT column_name FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'reference_curves' AND column_name = 'upload_date'"
        has_date_col = not client.query(schema_q).to_dataframe().empty
        
        date_select = "MAX(upload_date)" if has_date_col else "CAST(NULL AS STRING)"
        
        inv_q = f"""
            SELECT 
                CurveID, 
                MAX(Day) as Max_Day, 
                COUNT(*) as Total_Points,
                {date_select} as Last_Upload
            FROM `{TABLE_CURVES}`
            GROUP BY CurveID
            ORDER BY CurveID ASC
        """
        inventory_df = client.query(inv_q).to_dataframe()
        
        st.subheader("📚 Theoretical Library Inventory")
        if not inventory_df.empty:
            st.dataframe(
                inventory_df.rename(columns={
                    "CurveID": "Curve Identifier",
                    "Max_Day": "Duration (Days)",
                    "Total_Points": "Data Density",
                    "Last_Upload": "Upload Date"
                }),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("The library is currently empty. Upload curve CSVs below.")
    except Exception as e:
        st.error(f"Inventory Sync Error: {e}")

    st.divider()

    # 2. MANAGEMENT TOOLS
    c1, c2 = st.columns(2)
    with c1.expander("🗑️ Individual Curve Delete"):
        if not inventory_df.empty:
            to_delete = st.selectbox("Select Curve to Remove", sorted(inventory_df['CurveID'].tolist()))
            if st.button(f"Permanently Delete {to_delete}", type="primary"):
                client.query(f"DELETE FROM `{TABLE_CURVES}` WHERE CurveID = '{to_delete}'").result()
                st.success(f"Removed {to_delete} from library.")
                time.sleep(1)
                st.rerun()

    with c2.expander("🧨 Library Wipe"):
        st.warning("This will delete EVERY theoretical curve in the database.")
        if st.button("EXECUTE TOTAL PURGE", key="purge_all"):
            client.query(f"TRUNCATE TABLE `{TABLE_CURVES}`").result()
            st.success("Library wiped successfully.")
            time.sleep(1)
            st.rerun()

    st.divider()

    # 3. BULK UPLOAD ENGINE
    st.subheader("📤 Upload New Curves")
    u_files = st.file_uploader(
        "Upload Curve CSVs", 
        type=['csv'], 
        accept_multiple_files=True,
        help="Format: Data starts on Row 3. Column 1: Day, Column 2: Temp."
    )

    if u_files:
        if st.button("🚀 Commit Uploads to Database"):
            today_str = datetime.now().strftime('%Y-%m-%d')
            total_imported = 0
            
            for f in u_files:
                try:
                    # Skip header rows (usually site info), take Day and Temp
                    df = pd.read_csv(f, skiprows=2, usecols=[0, 1], names=['Day', 'Temp'])
                    df['CurveID'] = f.name.rsplit('.', 1)[0]
                    df['upload_date'] = today_str
                    
                    # Data Cleaning
                    df['Day'] = pd.to_numeric(df['Day'], errors='coerce')
                    df['Temp'] = pd.to_numeric(df['Temp'], errors='coerce')
                    df = df.dropna(subset=['Day', 'Temp'])

                    if not df.empty:
                        job_config = bigquery.LoadTableConfig(write_disposition="WRITE_APPEND")
                        client.load_table_from_dataframe(df, TABLE_CURVES, job_config=job_config).result()
                        total_imported += 1
                except Exception as e:
                    st.error(f"Error processing {f.name}: {e}")

            if total_imported > 0:
                st.success(f"✅ Successfully imported {total_imported} curves.")
                time.sleep(1.5)
                st.rerun()

# ===============================================================
# PAGE: DATA MANAGEMENT (Flagging & Maintenance)
# ===============================================================
elif admin_page == "🧨 Data Management":
    st.header("🧨 Data Management (Approval & Flagging)")
    st.info("Use this tool to flag data as 'Bad' or 'Restricted' for engineering analysis without deleting the underlying records.")

    # 1. SCOPE & ACTION (Top Row)
    c1, c2 = st.columns(2)
    with c1:
        target_scope = st.radio("Target Scope", ["Project Wide", "Specific Location", "Specific Node"], horizontal=True)
    with c2:
        # Renamed modes to reflect your 'Keep Data' intent
        mode = st.radio("Action Type", ["🚫 Mask (Flag as Bad)", "✅ Approve (Restore)"], horizontal=True)

    st.divider()

    # 2. FILTERS (Middle Section)
    col_f1, col_f2, col_f3 = st.columns(3)
    
    with col_f1:
        temporal_dir = st.selectbox("Temporal Direction", ["Between Range", "Older Than", "Newer Than"])
        s_date = st.date_input("Start Date", value=datetime.now() - timedelta(days=7))
        e_date = st.date_input("End Date", value=datetime.now())

    with col_f2:
        val_filter = st.selectbox("Value Filter", ["No Threshold", "Above Threshold", "Below Threshold"])
        threshold = st.number_input("Threshold Value (°F)", value=100.0)

    with col_f3:
        # Dynamic scope inputs
        scope_val = None
        if target_scope == "Specific Location":
            u_locs = sorted(reg_df[reg_df['Project'] == selected_project]['Location'].unique().tolist())
            scope_val = st.selectbox("Select Location", u_locs)
        elif target_scope == "Specific Node":
            u_nodes = sorted(reg_df[reg_df['Project'] == selected_project]['NodeNum'].unique().tolist())
            scope_val = st.selectbox("Select Node", u_nodes)
        else:
            st.write(f"**Target:** All nodes in {selected_project}")

    # 3. SQL CONSTRUCTION
    # Filtering Logic
    where_clauses = [f"Project = '{selected_project}'"]
    
    if temporal_dir == "Between Range":
        where_clauses.append(f"timestamp BETWEEN '{s_date}' AND '{e_date}'")
    elif temporal_dir == "Older Than":
        where_clauses.append(f"timestamp < '{s_date}'")
    
    if val_filter == "Above Threshold":
        where_clauses.append(f"temperature > {threshold}")
    elif val_filter == "Below Threshold":
        where_clauses.append(f"temperature < {threshold}")

    if target_scope == "Specific Location":
        where_clauses.append(f"Location = '{scope_val}'")
    elif target_scope == "Specific Node":
        where_clauses.append(f"NodeNum = '{scope_val}'")

    where_str = " AND ".join(where_clauses)
    
    # 4. VERIFICATION STEP
    if st.button("🔍 Step 1: Verify Match Count"):
        count_q = f"SELECT COUNT(*) as total FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` WHERE {where_str}"
        res = client.query(count_q).to_dataframe()
        count = res.iloc[0]['total']
        st.metric("Points Found", f"{count:,}")
        st.session_state['data_ready'] = True if count > 0 else False

    # 5. EXECUTION STEP
    if st.checkbox("I confirm these data points should be flagged/updated in the master registry."):
        new_status = "Bad" if "Mask" in mode else "Approved"
        
        if st.button(f"🚀 Execute {mode}"):
            # We update the 'raw_sensorpush' or 'master_data' table depending on your schema
            # Assuming an 'ApprovalStatus' column exists for trend analysis
            update_sql = f"""
                UPDATE `{PROJECT_ID}.{DATASET_ID}.master_data_view`
                SET ApprovalStatus = '{new_status}'
                WHERE {where_str}
            """
            
            try:
                with st.spinner("Updating data flags..."):
                    client.query(update_sql).result()
                st.success(f"Successfully flagged matching data as '{new_status}'.")
                st.balloons()
            except Exception as e:
                st.error(f"Execution Failed: {e}")
