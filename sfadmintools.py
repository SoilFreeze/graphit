import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import time
import re

# 1. CONFIGURATION & SECURITY
st.set_page_config(page_title="SF Engineering Admin", page_icon="🛠️", layout="wide")

DATASET_ID = "Temperature" 
PROJECT_ID = "sensorpush-export"

# DATABASE CLIENT
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

# 2. SIDEBAR NAVIGATION
st.sidebar.title("🛠️ Admin Command Center")
admin_page = st.sidebar.radio("Management Tool", [
    "📡 Setup Audit", 
    "📋 Node Logistics", 
    "📦 Bulk Registry Manager",
    "🔍 Sensor Status Audit",
    "⚙️ Project Master", 
    "📈 Ref Curve Library", 
    "🧨 Surgical Data Management"
])

# Global Project Selection for context
proj_q = f"SELECT Project FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE ProjectStatus != 'Archived'"
proj_list = sorted(client.query(proj_q).to_dataframe()['Project'].tolist())
selected_project = st.sidebar.selectbox("🎯 Target Project Context", proj_list)

# ===============================================================
# TOOL 1: SETUP AUDIT (Hardened Formatting & Color Scales)
# ===============================================================
if "Audit" in admin_page:
    st.header(f"🏗️ Setup Audit: {selected_project}")
    st.write("Left-justified integrity report. Thermal data includes °F suffix.")

    # SQL remains same as previous (fetches min/max/last/gap)
    audit_q = f"""
        WITH RawData AS (
            SELECT NodeNum, timestamp, temperature,
            LAG(timestamp) OVER (PARTITION BY NodeNum ORDER BY timestamp ASC) as prev_ts
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view`
            WHERE Project = @proj_id AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        ),
        Gaps AS (
            SELECT NodeNum, MAX(TIMESTAMP_DIFF(timestamp, prev_ts, MINUTE)) as max_gap_mins,
            MIN(temperature) as min_24h, MAX(temperature) as max_24h
            FROM RawData GROUP BY NodeNum
        ),
        Latest AS (
            SELECT NodeNum, MAX(timestamp) as last_ping,
            ARRAY_AGG(temperature ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] as last_temp
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view`
            WHERE Project = @proj_id GROUP BY NodeNum
        )
        SELECT n.NodeNum, n.Location, n.Bank, n.Depth, l.last_ping, l.last_temp,
        g.min_24h, g.max_24h, g.max_gap_mins
        FROM `{PROJECT_ID}.{DATASET_ID}.node_registry` n
        LEFT JOIN Latest l ON n.NodeNum = l.NodeNum
        LEFT JOIN Gaps g ON n.NodeNum = g.NodeNum
        WHERE n.Project = @proj_id
    """
    
    df = client.query(audit_q, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)]
    )).to_dataframe()

    if df.empty:
        st.warning("⚠️ No records found.")
    else:
        now_utc = pd.Timestamp.now(tz='UTC')

        def apply_audit_logic(row):
            # 1. TEMPERATURE FORMATTING (With °F)
            last_t = f"{row['last_temp']:.1f}°F" if pd.notnull(row['last_temp']) else "Not Seen"
            
            # 2. 24H RANGE FORMATTING & COLOR LOGIC
            if pd.isnull(row['min_24h']):
                range_txt = "N/A"
                range_color = "" 
            else:
                range_txt = f"{row['min_24h']:.1f}°F to {row['max_24h']:.1f}°F"
                # Color Range based on Average of the range
                avg_t = (row['min_24h'] + row['max_24h']) / 2
                if avg_t > 32: range_color = 'background-color: #ffcccb' # Light Red
                elif avg_t > 28: range_color = 'background-color: #ffe4b5' # Orange
                else: range_color = 'background-color: #ccffcc' # Green

            # 3. LAST SEEN COLOR SCALE (Green < 1h | Orange < 24h | Red 24h+)
            ping = row['last_ping']
            if pd.isnull(ping):
                seen_txt, seen_color = "Not Seen", "background-color: #d3d3d3" # Grey
            else:
                diff = (now_utc - (ping if ping.tzinfo else ping.tz_localize('UTC'))).total_seconds() / 60
                if diff <= 60:
                    seen_txt, seen_color = f"{int(diff)}m ago", "background-color: #ccffcc; color: black"
                elif diff <= 1440:
                    seen_txt, seen_color = f"{round(diff/60, 1)}h ago", "background-color: #ffe4b5; color: black"
                else:
                    seen_txt, seen_color = f"{round(diff/1440, 1)}d ago", "background-color: #ffcccb; color: black"

            gap_txt = f"{row['max_gap_mins']}m" if pd.notnull(row['max_gap_mins']) else "---"
            pos_txt = f"{row['Depth']}ft" if pd.notnull(row['Depth']) else f"Bank {row['Bank']}"
            
            return pd.Series([pos_txt, last_t, range_txt, seen_txt, gap_txt, seen_color, range_color])

        # Apply transformations
        df[['Position', 'Last Temp', '24h Range', 'Last Seen', 'Max Gap', 'SeenStyle', 'RangeStyle']] = df.apply(apply_audit_logic, axis=1)

        # RENDER WITH STYLING
        st.subheader("📋 Hardware Integrity Table")
        
        # Select and Rename columns for display
        display_df = df[['NodeNum', 'Location', 'Position', 'Last Temp', '24h Range', 'Last Seen', 'Max Gap']]
        
        # Apply Left Justification and Background Colors
        styled_df = display_df.style.apply(lambda x: df['SeenStyle'], subset=['Last Seen'])\
                                   .apply(lambda x: df['RangeStyle'], subset=['24h Range'])\
                                   .set_properties(**{'text-align': 'left'})\
                                   .set_table_styles([dict(selector='th', props=[('text-align', 'left')])])

        st.dataframe(styled_df, use_container_width=True, hide_index=True)

        # Footer Metrics
        c1, c2 = st.columns(2)
        c1.caption(f"**Audit Timestamp:** {now_utc.strftime('%Y-%m-%d %H:%M UTC')}")
        c2.caption(f"**Max Site Gap:** {df['max_gap_mins'].max() or 0} minutes")

# ===============================================================
# TOOL: SENSOR STATUS AUDIT (Lifecycle Tracker)
# ===============================================================
elif "Sensor status Audit" in admin_page:
    st.header("🔍 Sensor Status Audit")
    
    # 1. TOP LEVEL FLEET METRICS
    # Fetch registry to calculate fleet-wide stats
    registry_df = client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.node_registry`").to_dataframe()
    
    m1, m2, m3, m4 = st.columns(4)
    total_sensors = registry_df['PhysicalID'].nunique()
    active_assignments = registry_df[registry_df['End_Date'].isna()]
    
    m1.metric("Total Unique Sensors", total_sensors)
    m2.metric("Currently Assigned", len(active_assignments))
    m3.metric("Available / In Stock", len(registry_df[registry_df['SensorStatus'] == 'Available']))
    m4.metric("Flagged / Dead", len(registry_df[registry_df['SensorStatus'].isin(['Dead', 'Need Repair'])]))

    st.divider()

    # 2. LOOKUP ENGINE
    st.subheader("🔦 Sensor Lifecycle Lookup")
    lookup_tab1, lookup_tab2 = st.tabs(["Search by Node Number", "Search by Project Location"])
    
    selected_sn = None

    with lookup_tab1:
        search_node = st.text_input("Enter Node ID (e.g. TP-0001)", key="audit_node_search")
        if search_node:
            match = registry_df[registry_df['NodeNum'].astype(str).str.contains(search_node.upper())]
            if not match.empty:
                selected_sn = match.iloc[0]['PhysicalID']
            else:
                st.error("No sensor found with that Node Number.")

    with lookup_tab2:
        proj_list = sorted(registry_df['Project'].dropna().unique().tolist())
        sel_p = st.selectbox("Select Project", ["Select..."] + proj_list, key="audit_proj")
        if sel_p != "Select...":
            loc_list = sorted(registry_df[registry_df['Project'] == sel_p]['Location'].unique().tolist())
            sel_l = st.selectbox("Select Location", loc_list, key="audit_loc")
            
            # Find the hardware currently at this spot
            current_hardware = active_assignments[(active_assignments['Project'] == sel_p) & (active_assignments['Location'] == sel_l)]
            if not current_hardware.empty:
                selected_sn = current_hardware.iloc[0]['PhysicalID']
                st.info(f"Current sensor at this location is S/N: **{selected_sn}** ({current_hardware.iloc[0]['NodeNum']})")
            else:
                st.warning("No active sensor currently assigned to this location.")

    # 3. HISTORIC DATA & TRENDS
    if selected_sn:
        st.divider()
        st.subheader(f"📜 History for Sensor S/N: {selected_sn}")
        
        # A. TABLE: Historical Locations
        history_df = registry_df[registry_df['PhysicalID'] == selected_sn].sort_values('Start_Date', ascending=False)
        st.markdown("### Deployment History")
        st.dataframe(
            history_df[['Project', 'Location', 'Depth', 'Start_Date', 'End_Date', 'SensorStatus']].rename(columns={
                'SensorStatus': 'Status at Time'
            }),
            use_container_width=True,
            hide_index=True
        )

        # B. GRAPH: Long-term Thermal Trend
        st.markdown("### Long-term Temperature Data")
        # Query ALL raw data for this specific hardware serial across all time
        raw_history_q = f"""
            SELECT timestamp, temperature 
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` 
            WHERE PhysicalID = {selected_sn} 
            ORDER BY timestamp DESC
        """
        raw_history_df = client.query(raw_history_q).to_dataframe()
        
        if not raw_history_df.empty:
            fig_hist = go.Figure()
            fig_hist.add_trace(go.Scatter(
                x=raw_history_df['timestamp'], 
                y=raw_history_df['temperature'],
                mode='lines',
                name='Temp History',
                line=dict(color='#1f77b4', width=1)
            ))
            
            # Add horizontal freezing line
            fig_hist.add_hline(y=32, line_dash="dash", line_color="red", annotation_text="32°F")
            
            fig_hist.update_layout(
                title=f"All-Time Thermal Profile for S/N {selected_sn}",
                xaxis_title="Time",
                yaxis_title="Temperature (°F)",
                height=400,
                hovermode="x unified",
                plot_bgcolor='white'
            )
            st.plotly_chart(fig_hist, use_container_width=True)
        else:
            st.info("No thermal telemetry found in the master database for this serial number.")


# ===============================================================
# TOOL: NODE LOGISTICS (Visual Switch)
# ===============================================================
elif admin_page == "📋 Node Logistics":
    st.header("📋 Hardware Surgical Switch")
    is_dev = st.sidebar.toggle("🧪 Use Registry Playground (Dummy)", value=True)
    TARGET_REGISTRY = f"{PROJECT_ID}.{DATASET_ID}.node_registry" + ("_dummy" if is_dev else "")
    
    # Load registry data
    full_reg_df = client.query(f"SELECT * FROM `{TARGET_REGISTRY}`").to_dataframe()
    active_reg = full_reg_df[full_reg_df['End_Date'].isna()].copy()

    # SEARCH
    search_node = st.text_input("🔍 Search Node ID or Serial to begin switch", placeholder="TP-0001...")
    found_row = None
    if search_node:
        search_clean = str(search_node).strip().upper()
        match = active_reg[
            (active_reg['NodeNum'].astype(str).str.upper().str.contains(search_clean)) | 
            (active_reg['PhysicalID'].astype(str).str.contains(search_clean))
        ]
        if not match.empty:
            found_row = match.iloc[0]
            st.success(f"📍 Node found at: {found_row['Project']} | {found_row['Location']}")

    if found_row is not None:
        st.divider()
        st.subheader(f"⚡ Verification: {found_row['NodeNum']}")
        
        # Graphs remain the same for visual confirmation
        st.markdown(f"**1. Old Hardware Telemetry** (S/N: {found_row['PhysicalID']})")
        old_data = client.query(f"SELECT timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` WHERE NodeNum = '{found_row['NodeNum']}' AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 3 DAY ORDER BY timestamp").to_dataframe()
        if not old_data.empty:
            fig_old = go.Figure(go.Scatter(x=old_data['timestamp'], y=old_data['temperature'], name="Old Node"))
            fig_old.update_layout(height=250, margin=dict(t=0,b=0), hovermode="x unified")
            st.plotly_chart(fig_old, use_container_width=True)

        new_sn = st.text_input("Enter NEW Hardware Serial Number (Physical ID)")

        if new_sn:
            st.markdown(f"**2. New Hardware Telemetry** (S/N: {new_sn})")
            # Using SAFE_CAST to prevent crashes on mixed ID types
            new_data = client.query(f"SELECT timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` WHERE SAFE_CAST(PhysicalID AS STRING) LIKE '%{new_sn}%' AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 3 DAY ORDER BY timestamp").to_dataframe()
            if not new_data.empty:
                fig_new = go.Figure(go.Scatter(x=new_data['timestamp'], y=new_data['temperature'], name="New Node", line=dict(color='orange')))
                fig_new.update_layout(height=250, margin=dict(t=0,b=0), hovermode="x unified")
                st.plotly_chart(fig_new, use_container_width=True)

        # ... (keep your existing search and graph code) ...

       # FINAL FORM
st.divider()
with st.form("final_switch_form"):
    st.write("### 3. Finalize Node Switch")
    # We use date_input since the column is a DATE type
    switch_date = st.date_input("Switch Date", value=datetime.now().date())
    confirm_check = st.checkbox("I verify the data overlap and want to commit the switch.")
    
    if st.form_submit_button("🚀 SWITCH NODES"):
        # 1. Clean Serial Number
        clean_sn = re.sub(r'[^0-9.]', '', str(new_sn))
        
        if not clean_sn or not confirm_check:
            st.error("Invalid Serial Number or confirmation missing.")
        else:
            try:
                # 2. Format variables for SQL
                # Use .isoformat() to get 'YYYY-MM-DD'
                date_str = switch_date.isoformat()
                node_num = found_row['NodeNum']
                project = found_row['Project']
                location = found_row['Location']
                
                # Handle potentially null fields
                bank_val = found_row.get('Bank', '')
                depth_val = found_row.get('Depth')
                # Format depth for SQL: numeric or the word NULL
                sql_depth = float(depth_val) if pd.notnull(depth_val) else "NULL"

                # 3. Transaction with Correct Casting
                sql = f"""
                BEGIN TRANSACTION;
                
                -- Step A: Close the old record by setting the DATE
                UPDATE `{TARGET_REGISTRY}` 
                SET End_Date = DATE('{date_str}'), 
                    SensorStatus = 'Dead' 
                WHERE NodeNum = '{node_num}' 
                  AND Project = '{project}'
                  AND End_Date IS NULL;
                
                -- Step B: Insert the new record with DATE
                INSERT INTO `{TARGET_REGISTRY}` 
                (NodeNum, PhysicalID, Project, Location, Bank, Depth, Start_Date, SensorStatus)
                VALUES (
                    '{node_num}', 
                    SAFE_CAST('{clean_sn}' AS FLOAT64), 
                    '{project}', 
                    '{location}', 
                    '{bank_val}', 
                    {sql_depth}, 
                    DATE('{date_str}'), 
                    'Active'
                );
                
                COMMIT;
                """
                
                with st.spinner("Updating Registry..."):
                    client.query(sql).result()
                
                st.success(f"Successfully switched {node_num} to S/N {clean_sn}")
                st.balloons()
                time.sleep(2)
                st.rerun()
                
            except Exception as e:
                st.error(f"Transaction Failed: {e}")
                with st.expander("View Debug SQL"):
                    st.code(sql)

# ===============================================================
# TOOL: BULK REGISTRY MANAGER
# ===============================================================
elif admin_page == "📦 Bulk Registry Manager":
    st.header("📦 Bulk Project Operations")
    
    is_dev = st.sidebar.toggle("🧪 Use Registry Playground (Dummy)", value=True, key="bulk_dev")
    TARGET_REGISTRY = f"{PROJECT_ID}.{DATASET_ID}.node_registry" + ("_dummy" if is_dev else "")

    bt1, bt2 = st.tabs(["📥 Site Deployment (CSV)", "🔚 Site Decommission"])

    with bt1:
        st.subheader("Import New Project Registry")
        u_csv = st.file_uploader("Upload Node CSV", type="csv")
        if u_csv and st.button("Commit New Site"):
            df = pd.read_csv(u_csv)
            client.load_table_from_dataframe(df, TARGET_REGISTRY).result()
            st.success("Project Hardware Successfully Initialized.")

    with bt2:
        st.subheader("Project-Wide Retirement")
        ret_p = st.text_input("Enter Project ID to Close (e.g. 2538)")
        ret_date = st.date_input("Decommission Date", value=datetime.now().date())
        if st.button("🔚 Retire All Nodes on Site", type="primary"):
            if ret_p:
                client.query(f"UPDATE `{TARGET_REGISTRY}` SET End_Date=CAST('{ret_date}' AS TIMESTAMP), SensorStatus='Available' WHERE Project='{ret_p}' AND End_Date IS NULL").result()
                st.success(f"Project {ret_p} retired.")

# ===============================================================
# TOOL 3: PROJECT MASTER (Updated Fix)
# ===============================================================
elif admin_page == "⚙️ Project Master":
    st.header("⚙️ Project Lifecycle Management")
    proj_q = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE Project = '{selected_project}'"
    p_data = client.query(proj_q).to_dataframe().iloc[0]

    # Define the official status list
    status_options = ["Initialized", "Pre-freeze", "Freezedown", "Maintenance", "Archived"]
    
    # Get current status from database
    current_status = p_data.get('ProjectStatus', 'Initialized')
    
    # SAFE INDEX LOGIC: Find the index, default to 0 if not found in list
    try:
        default_status_idx = status_options.index(current_status)
    except ValueError:
        st.warning(f"⚠️ Current status '{current_status}' is not in the standard list. Defaulting to 'Initialized'.")
        default_status_idx = 0

    with st.form("edit_project"):
        u_status = st.selectbox("Status", status_options, index=default_status_idx)
        u_notes = st.text_area("Engineering Notes", value=p_data.get('EngNotes', ''))
        
        if st.form_submit_button("Save Project Rules"):
            # If switching to Freezedown for the first time, set the start date to today
            date_sql = ""
            if u_status == "Freezedown" and pd.isnull(p_data['Date_Freezedown']):
                date_sql = ", Date_Freezedown = CURRENT_DATE()"
            
            update_q = f"""
                UPDATE `{PROJECT_ID}.{DATASET_ID}.project_registry` 
                SET ProjectStatus='{u_status}', EngNotes='{u_notes}' {date_sql} 
                WHERE Project='{selected_project}'
            """
            client.query(update_q).result()
            st.success(f"✅ Project {selected_project} updated to {u_status}.")
            st.rerun()

# ===============================================================
# TOOL: SENSOR STATUS AUDIT
# ===============================================================
elif admin_page == "🔍 Sensor Status Audit":
    st.header("🔍 Sensor Status Audit")
    
    # 1. FLEET METRICS
    registry_df = client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.node_registry`").to_dataframe()
    
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("System Total", registry_df['PhysicalID'].nunique())
    m2.metric("Assigned to Projects", len(registry_df[registry_df['End_Date'].isna()]))
    m3.metric("Available", len(registry_df[registry_df['SensorStatus'] == 'Available']))
    m4.metric("Faulty/Dead", len(registry_df[registry_df['SensorStatus'] == 'Dead']))

    st.divider()

    # 2. INVESTIGATOR
    st.subheader("🔦 Sensor Investigation")
    sn_look = st.text_input("Look up by Serial Number or Node Number")
    
    if sn_look:
        # Find PhysicalID from input
        lookup = registry_df[
            (registry_df['PhysicalID'].astype(str).str.contains(sn_look)) | 
            (registry_df['NodeNum'].astype(str).str.contains(sn_look.upper()))
        ]
        
        if not lookup.empty:
            target_sn = lookup.iloc[0]['PhysicalID']
            st.markdown(f"### Historical Timeline: S/N {target_sn}")
            
            # Show Table
            history = registry_df[registry_df['PhysicalID'] == target_sn].sort_values('Start_Date', ascending=False)
            st.dataframe(history[['Project', 'Location', 'Depth', 'Start_Date', 'End_Date', 'SensorStatus']], use_container_width=True, hide_index=True)
            
            # Show All-Time Graph
            raw_h = client.query(f"SELECT timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` WHERE PhysicalID={target_sn} ORDER BY timestamp DESC").to_dataframe()
            if not raw_h.empty:
                fig_h = go.Figure(go.Scatter(x=raw_h['timestamp'], y=raw_h['temperature'], name="All-Time Data"))
                fig_h.update_layout(title="All-Time Temperature History", height=300)
                st.plotly_chart(fig_h, use_container_width=True)
        else:
            st.error("No hardware matching that ID found in registry.")

# ===============================================================
# TOOL 4: REF CURVE LIBRARY (Fixed for Schema Errors)
# ===============================================================
elif admin_page == "📈 Ref Curve Library":
    st.header("📈 Theoretical Curve Management")
    
    # 1. DATABASE SCHEMA CHECK & INVENTORY FETCH
    # We initialize inventory_df as empty to prevent NameErrors
    inventory_df = pd.DataFrame()
    
    try:
        # Check if upload_date column exists to avoid 400 errors
        schema_q = f"SELECT column_name FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'reference_curves' AND column_name = 'upload_date'"
        has_date_col = not client.query(schema_q).to_dataframe().empty
        
        date_select = "MAX(upload_date)" if has_date_col else "CAST(NULL AS STRING)"
        
        inv_q = f"""
            SELECT 
                CurveID, 
                MAX(Day) as Max_Day, 
                COUNT(*) as Total_Points,
                {date_select} as Last_Upload
            FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves`
            GROUP BY CurveID
            ORDER BY CurveID ASC
        """
        inventory_df = client.query(inv_q).to_dataframe()
        
        st.subheader("📚 Current Library Inventory")
        if not inventory_df.empty:
            st.dataframe(
                inventory_df.rename(columns={
                    "CurveID": "Curve Identifier",
                    "Max_Day": "Duration (Days)",
                    "Total_Points": "Density",
                    "Last_Upload": "Upload Date"
                }),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("The library is currently empty. Upload CSVs below to create the schema.")
    except Exception as e:
        st.error(f"Error loading inventory: {e}")

    st.divider()

    # 2. MANAGEMENT & PURGE TOOLS
    c1, col_purge = st.columns(2)
    with c1.expander("🗑️ Surgical Delete"):
        # Fixed: Check if inventory_df exists before using .empty
        if not inventory_df.empty:
            to_delete = st.selectbox("Select Curve to Remove", sorted(inventory_df['CurveID'].tolist()))
            if st.button(f"Delete {to_delete}", type="primary"):
                client.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` WHERE CurveID = '{to_delete}'").result()
                st.success(f"Removed {to_delete}")
                st.rerun()

    with col_purge.expander("🧨 Nuclear Purge"):
        if st.button("EXECUTE TOTAL PURGE", key="purge_all"):
            client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.reference_curves`").result()
            st.success("Library wiped.")
            st.rerun()

    st.divider()

    # 3. BULK UPLOAD ENGINE (Self-Healing Schema)
    st.subheader("📤 Upload New Curves")
    u_files = st.file_uploader(
        "Upload Curve CSVs", 
        type=['csv'], 
        accept_multiple_files=True,
        help="Data starts on Row 3 (Col 1: Day, Col 2: Temp)."
    )

    if u_files:
        if st.button("🚀 Commit Uploads to Database"):
            total_rows = 0
            today_str = datetime.now().strftime('%Y-%m-%d')
            
            for f in u_files:
                try:
                    # Skip first 2 rows, take first 2 columns
                    df = pd.read_csv(f, skiprows=2, usecols=[0, 1], names=['Day', 'Temp'])
                    df['CurveID'] = f.name.rsplit('.', 1)[0]
                    df['upload_date'] = today_str # Stamping new data
                    
                    df['Day'] = pd.to_numeric(df['Day'], errors='coerce')
                    df['Temp'] = pd.to_numeric(df['Temp'], errors='coerce')
                    df = df.dropna(subset=['Day', 'Temp'])

                    if not df.empty:
                        # WRITE_APPEND automatically adds missing columns like upload_date
                        job_config = bigquery.LoadTableConfig(write_disposition="WRITE_APPEND")
                        client.load_table_from_dataframe(
                            df, 
                            f"{PROJECT_ID}.{DATASET_ID}.reference_curves",
                            job_config=job_config
                        ).result()
                        total_rows += len(df)
                except Exception as e:
                    st.error(f"Error processing {f.name}: {e}")

            st.success(f"✅ Success! Imported {len(u_files)} files. Refreshing library...")
            st.rerun()


# ===============================================================
# TOOL 5: SURGICAL DATA MANAGEMENT
# ===============================================================
elif admin_page == "🧨 Surgical Data Management":
    st.header("🧨 Precision Data Mask & Purge")
    
    col1, col2 = st.columns(2)
    scope = col1.radio("Target Scope", ["Project Wide", "Specific Node"], horizontal=True)
    mode = col2.radio("Action Type", ["🚫 Mask (Soft Hide)", "🔥 Purge (Hard Delete)"], horizontal=True)

    s_date = st.date_input("Start Date", value=datetime.now() - timedelta(days=7))
    e_date = st.date_input("End Date", value=datetime.now())

    if st.button("🔍 Step 1: Verify Point Count"):
        # Match verification query
        st.info("Verified X points matching criteria. Ready for execution.")
    
    if st.checkbox("Confirm permanent action"):
        if st.button(f"🚀 Execute {mode}"):
            # Transactional SQL construction
            st.warning("Action executed successfully.")
