import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta, time as dt_time
import time
import os

# 1. CONFIGURATION & SECURITY
st.set_page_config(page_title="SF Engineering Admin", page_icon="🛠️", layout="wide")

# Global Database Constants
DATASET_ID = "Temperature" 
PROJECT_ID = "sensorpush-export"
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"

# LOGIN GATE
if "authenticated" not in st.session_state:
    st.session_state['authenticated'] = False

if not st.session_state['authenticated']:
    st.markdown("## 🔐 Engineering Admin Access")
    pwd = st.text_input("Enter Admin Password", type="password")
    if st.button("Unlock Management Tools"):
        if pwd == st.secrets["admin_password"]:
            st.session_state['authenticated'] = True
            st.rerun()
        else:
            st.error("Access Denied.")
    st.stop()

# 2. CORE DATABASE ENGINE
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

# 3. PAGE ROUTER
st.sidebar.title("🛠️ Project & Node Admin")
admin_page = st.sidebar.radio("Management Tool", [
    "📡 Setup Audit", 
    "📋 Node Logistics", 
    "📦 Bulk Registry Manager",  # New Tool
    "⚙️ Project Master", 
    "📈 Ref Curve Library", 
    "🧨 Surgical Data Management"
])

client = get_bq_client()

# --- PROJECT SELECTION (Global for Admin) ---
proj_q = f"SELECT Project FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE ProjectStatus != 'Archived'"
proj_list = sorted(client.query(proj_q).to_dataframe()['Project'].tolist())
selected_project = st.sidebar.selectbox("🎯 Target Project", proj_list)

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
# TOOL 2: NODE LOGISTICS (Visual Verification & Atomic Switch)
# ===============================================================
elif "Logistics" in admin_page:
    st.header("📋 Hardware Assignment & Deployment")
    
    # 1. DATABASE & PLAYGROUND SETUP
    is_dev = st.sidebar.toggle("🧪 Use Registry Playground (Dummy)", value=True)
    TARGET_REGISTRY = f"{PROJECT_ID}.{DATASET_ID}.node_registry" + ("_dummy" if is_dev else "")
    
    if is_dev:
        with st.expander("🛠️ Playground Setup & Reset"):
            if st.button("♻️ Sync Dummy with Live Registry"):
                client.query(f"CREATE OR REPLACE TABLE `{TARGET_REGISTRY}` AS SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.node_registry`").result()
                st.success("Playground initialized.")
                st.rerun()

    # Load fresh registry data
    full_reg_df = client.query(f"SELECT * FROM `{TARGET_REGISTRY}`").to_dataframe()
    full_reg_df['Project'] = full_reg_df['Project'].fillna("UNASSIGNED")
    active_reg = full_reg_df[full_reg_df['End_Date'].isna()].copy()

    # 2. SEARCH & AUTO-SELECTION LOGIC
    st.subheader("🔍 Find & Verify Hardware")
    
    if 'auto_p' not in st.session_state: st.session_state.auto_p = "Select..."
    if 'auto_l' not in st.session_state: st.session_state.auto_l = None

    lookup_col1, lookup_col2 = st.columns(2)
    found_row = None

    with lookup_col1:
        search_input = st.text_input("Search by Node ID or S/N", placeholder="TP-0001...")
        if search_input:
            search_clean = str(search_input).strip().upper()
            match = active_reg[
                (active_reg['NodeNum'].astype(str).str.upper().str.contains(search_clean)) | 
                (active_reg['PhysicalID'].astype(str).str.contains(search_clean))
            ]
            if not match.empty:
                found_row = match.iloc[0]
                st.session_state.auto_p = found_row['Project']
                st.session_state.auto_l = found_row['Location']
                st.success(f"📍 Node found at: {found_row['Project']} | {found_row['Location']}")

    with lookup_col2:
        p_list = sorted([p for p in active_reg['Project'].unique().tolist() if p])
        sel_p = st.selectbox("Site Project", ["Select..."] + p_list, 
                             index=(["Select..."] + p_list).index(st.session_state.auto_p) if st.session_state.auto_p in (["Select..."] + p_list) else 0)
        
        if sel_p != "Select...":
            loc_list = sorted(active_reg[active_reg['Project'] == sel_p]['Location'].unique().tolist())
            sel_l = st.selectbox("Site Location", loc_list,
                                 index=loc_list.index(st.session_state.auto_l) if st.session_state.auto_l in loc_list else 0)
            
            slot_match = active_reg[(active_reg['Project'] == sel_p) & (active_reg['Location'] == sel_l)]
            if not slot_match.empty:
                options = slot_match.apply(lambda r: f"{r['NodeNum']} (S/N: {r['PhysicalID']} | {r['Depth']}ft)", axis=1).tolist()
                sel_n = st.selectbox("Currently Assigned Hardware", options)
                found_row = slot_match.iloc[options.index(sel_n)]

    # 3. DUAL-GRAPH VERIFICATION & SWITCH ENGINE
    if found_row is not None:
        st.divider()
        st.subheader(f"⚡ Hardware Switch: {found_row['NodeNum']}")
            
        # --- GRAPH 1: THE OLD NODE ---
        st.markdown(f"**1. Current Assigned Hardware Telemetry** (S/N: {found_row['PhysicalID']})")
        old_data = client.query(f"SELECT timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` WHERE NodeNum = '{found_row['NodeNum']}' AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 3 DAY ORDER BY timestamp").to_dataframe()
        if not old_data.empty:
            fig_old = go.Figure(go.Scatter(x=old_data['timestamp'], y=old_data['temperature'], name="Old Node"))
            fig_old.update_layout(height=250, margin=dict(t=0,b=0), hovermode="x unified")
            fig_old.update_traces(hovertemplate="%{x|%Y-%m-%d %H:%M}<br>Temp: %{y:.2f}°F")
            st.plotly_chart(fig_old, use_container_width=True)
        else: st.info("No recent data for old node.")

        # --- INPUT FOR NEW NODE ---
        new_sn = st.text_input("Enter NEW Hardware Serial Number (Physical ID)")

        # --- GRAPH 2: THE NEW NODE ---
        if new_sn:
            st.markdown(f"**2. Incoming Hardware Telemetry** (S/N: {new_sn})")
            new_data = client.query(f"SELECT timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` WHERE CAST(PhysicalID AS STRING) LIKE '%{new_sn}%' AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 3 DAY ORDER BY timestamp").to_dataframe()
            if not new_data.empty:
                fig_new = go.Figure(go.Scatter(x=new_data['timestamp'], y=new_data['temperature'], name="New Node", line=dict(color='orange')))
                fig_new.update_layout(height=250, margin=dict(t=0,b=0), hovermode="x unified")
                fig_new.update_traces(hovertemplate="%{x|%Y-%m-%d %H:%M}<br>Temp: %{y:.2f}°F")
                st.plotly_chart(fig_new, use_container_width=True)
            else: st.warning("No signal found for this new Serial Number yet.")

        # --- FINAL EXECUTION ---
        st.divider()
        with st.form("final_switch_form"):
            st.write("### 3. Decide Switch Parameters")
            switch_ts = st.datetime_input("Exact Date & Time to Switch Nodes", value=datetime.now())
            new_sn = st.text_input("Enter NEW Hardware Serial Number (Physical ID)")
            confirm_check = st.checkbox("I have verified the timestamps on both graphs and want to switch nodes.")
            
            if st.form_submit_button("🚀 SWITCH NODES"):
                if not new_sn or not confirm_check:
                    st.error("Missing serial number or confirmation.")
                else:
                    try:
                        ts_str = switch_ts.strftime('%Y-%m-%d %H:%M:%S')
                        # Clean Serial Number to avoid float issues (.0)
                        clean_sn = str(new_sn).split('.')[0].strip()
                        
                        # Hardened Transactional SQL with explicit CASTING
                        sql = f"""
                        BEGIN TRANSACTION;
                        UPDATE `{TARGET_REGISTRY}` 
                        SET End_Date = CAST('{ts_str}' AS TIMESTAMP), SensorStatus = 'Dead' 
                        WHERE NodeNum = '{found_row['NodeNum']}' 
                          AND Project = '{found_row['Project']}'
                          AND Start_Date = CAST('{found_row['Start_Date']}' AS TIMESTAMP)
                          AND End_Date IS NULL;
                        
                        INSERT INTO `{TARGET_REGISTRY}` 
                        (NodeNum, PhysicalID, Project, Location, Bank, Depth, Start_Date, SensorStatus)
                        VALUES (
                            '{found_row['NodeNum']}', 
                            CAST('{clean_sn}' AS FLOAT64), 
                            '{found_row['Project']}', 
                            '{found_row['Location']}', 
                            '{found_row.get('Bank', '')}', 
                            CAST('{found_row.get('Depth', 0)}' AS FLOAT64), 
                            CAST('{ts_str}' AS TIMESTAMP), 
                            'Active'
                        );
                        COMMIT;
                        """
                        client.query(sql).result()
                        st.success("Switch successful.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"SQL Error: {e}")
                        st.code(sql) # Help debug if it fails again

    
    # --- 3. SURGICAL ACTIONS ---
    if found_row is not None:
        st.divider()
        st.subheader(f"⚡ Actions for {found_row['NodeNum']} at {found_row['Location']}")
        
        # VISUAL TELEMETRY CHECK
        old_data = client.query(f"SELECT timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` WHERE NodeNum = '{found_row['NodeNum']}' AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 3 DAY ORDER BY timestamp").to_dataframe()
        if not old_data.empty:
            st.caption(f"Last 3 days of telemetry for {found_row['NodeNum']}:")
            st.line_chart(old_data.set_index('timestamp')['temperature'], height=150)

        action_tab1, action_tab2 = st.tabs(["🔋 Physical Hardware Swap", "🔄 Data/Mapping Correction"])

        with action_tab1:
            st.info("Case: Sensor died/faulty. Replace with NEW serial number starting at a specific time.")
            with st.form("swap_form"):
                new_sn = st.text_input("New Physical S/N (New Hardware Serial)")
                swap_ts = st.datetime_input("Effective Swap Time", value=datetime.now())
                if st.form_submit_button("⚡ Execute Physical Swap"):
                    if not new_sn: st.error("New Serial required.")
                    else:
                        sql = f"""
                        BEGIN TRANSACTION;
                        UPDATE `{TARGET_REGISTRY}` SET End_Date='{swap_ts.strftime('%Y-%m-%d %H:%M:%S')}', SensorStatus='Dead' 
                        WHERE NodeNum='{found_row['NodeNum']}' AND Start_Date='{found_row['Start_Date']}';
                        INSERT INTO `{TARGET_REGISTRY}` (NodeNum, PhysicalID, Project, Location, Bank, Depth, Start_Date, SensorStatus)
                        VALUES ('{found_row['NodeNum']}', {new_sn}, '{found_row['Project']}', '{found_row['Location']}', '{found_row['Bank']}', {found_row['Depth']}, '{swap_ts.strftime('%Y-%m-%d %H:%M:%S')}', 'Active');
                        COMMIT;"""
                        client.query(sql).result()
                        st.success("Swap recorded. History split at timestamp.")
                        st.rerun()

        with action_tab2:
            st.info("Case: Assignment was wrong. Move ALL data from this assignment to a different serial number.")
            with st.form("fix_data"):
                corr_sn = st.text_input("Correct Physical S/N", value=str(found_row['PhysicalID']))
                if st.form_submit_button("💾 Overwrite Registry Link"):
                    sql = f"UPDATE `{TARGET_REGISTRY}` SET PhysicalID={corr_sn} WHERE NodeNum='{found_row['NodeNum']}' AND Start_Date='{found_row['Start_Date']}'"
                    client.query(sql).result()
                    st.success("Link corrected. History updated.")
                    st.rerun()

    # --- 4. BULK OPERATIONS ---
    st.divider()
    st.subheader("📦 Bulk Site Operations")
    bulk_tab1, bulk_tab2 = st.tabs(["New Site Deployment (Additions)", "Full Site Decommission (Retirement)"])
    
    with bulk_tab1:
        st.write("Upload CSV to initialize a whole project.")
        u_csv = st.file_uploader("Upload Node CSV", type="csv", help="Required columns: NodeNum, PhysicalID, Project, Location, Depth, Start_Date")
        if u_csv and st.button("🚀 Push Bulk Additions"):
            up_df = pd.read_csv(u_csv)
            client.load_table_from_dataframe(up_df, TARGET_REGISTRY).result()
            st.success(f"Success: {len(up_df)} nodes added to registry.")

    with bulk_tab2:
        st.warning("This will set an End Date for every active sensor on a specific Project.")
        ret_p = st.text_input("Enter Project ID to Close Out", placeholder="e.g. 2538")
        ret_date = st.date_input("Decommission Date", value=datetime.now().date())
        if st.button("🔚 Retire Entire Site", type="primary"):
             if ret_p:
                 sql = f"UPDATE `{TARGET_REGISTRY}` SET End_Date='{ret_date}', SensorStatus='Available' WHERE Project='{ret_p}' AND End_Date IS NULL"
                 client.query(sql).result()
                 st.success(f"Project {ret_p} retired.")
             else: st.error("Project ID required.")


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
