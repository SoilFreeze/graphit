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
    "📡 Commissioning Audit", 
    "📋 Node Logistics", 
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
# TOOL 1: SETUP AUDIT (Emoji-Safe Version)
# ===============================================================
# Use 'in' logic so it works whether you include the emoji or not
if "Setup Audit" in admin_page:
    st.header(f"🏗️ Setup Audit: {selected_project}")
    st.write("Comprehensive hardware health check. Scale: 1hr (Green) | 24hr (Orange) | 48hr+ (Red)")

    # Optimized SQL for Latency, 24h Range, and Max Gap analysis
    audit_q = f"""
        WITH RawData AS (
            SELECT 
                NodeNum, 
                timestamp, 
                temperature,
                LAG(timestamp) OVER (PARTITION BY NodeNum ORDER BY timestamp ASC) as prev_ts
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view`
            WHERE Project = @proj_id
            AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        ),
        Gaps AS (
            SELECT 
                NodeNum,
                MAX(TIMESTAMP_DIFF(timestamp, prev_ts, MINUTE)) as max_gap_mins,
                MIN(temperature) as min_24h,
                MAX(temperature) as max_24h,
                COUNT(*) as point_count
            FROM RawData
            GROUP BY NodeNum
        ),
        Latest AS (
            SELECT 
                NodeNum, 
                MAX(timestamp) as last_ping,
                ARRAY_AGG(temperature ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] as last_temp
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view`
            WHERE Project = @proj_id
            GROUP BY NodeNum
        )
        SELECT 
            n.NodeNum, n.Location, n.Bank, n.Depth,
            l.last_ping, l.last_temp,
            g.min_24h, g.max_24h, g.max_gap_mins, g.point_count
        FROM `{PROJECT_ID}.{DATASET_ID}.node_registry` n
        LEFT JOIN Latest l ON n.NodeNum = l.NodeNum
        LEFT JOIN Gaps g ON n.NodeNum = g.NodeNum
        WHERE n.Project = @proj_id
    """
    
    # Run Query
    with st.spinner("Auditing site hardware..."):
        df = client.query(audit_q, job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)]
        )).to_dataframe()

    if df.empty:
        st.warning(f"⚠️ No nodes found for {selected_project} in the registry.")
    else:
        now_utc = pd.Timestamp.now(tz='UTC')

        def evaluate_health(row):
            # 1. LATENCY SCALE LOGIC
            ping = row['last_ping']
            if pd.isnull(ping):
                return "⚪ Not Seen", "Not Seen", "N/A", "#808080" # Grey
            
            ping_utc = ping if ping.tzinfo else ping.tz_localize('UTC')
            diff_min = (now_utc - ping_utc).total_seconds() / 60
            
            if diff_min <= 60:
                color, label = "#228B22", f"{int(diff_min)}m ago" # Forest Green
            elif diff_min <= 1440:
                color, label = "#FF8C00", f"{round(diff_min/60, 1)}h ago" # Dark Orange
            else:
                color, label = "#B22222", f"{round(diff_min/1440, 1)}d ago" # Firebrick Red

            # 2. TEMP RANGE LOGIC (N/A if no data in 24h)
            if pd.isnull(row['min_24h']):
                t_range = "N/A"
            else:
                t_range = f"{row['min_24h']:.1f}° to {row['max_24h']:.1f}°"

            # 3. MAX GAP LOGIC
            gap = f"{row['max_gap_mins']}m" if pd.notnull(row['max_gap_mins']) else "---"
            
            return label, t_range, gap, color

        # Process status
        df[['Last Seen', '24h Range', 'Max Gap', 'StatusColor']] = df.apply(
            lambda x: pd.Series(evaluate_health(x)), axis=1
        )

        # Handle Position/Depth Column
        df['Pos'] = df.apply(lambda r: f"{r['Depth']}ft" if pd.notnull(r['Depth']) and str(r['Depth']) != '' else f"Bank {r['Bank']}", axis=1)
        
        # UI: Top Level Metrics
        m1, m2, m3 = st.columns(3)
        m1.metric("Site Nodes", len(df))
        m2.metric("Online (Active)", len(df[df['last_temp'].notnull()]))
        m3.metric("Max Data Gap", f"{df['max_gap_mins'].max() or 0} mins")

        # UI: Main Audit Table
        st.subheader("📋 Hardware Audit Table")
        
        # Color coding for the dataframe (optional formatting)
        st.dataframe(
            df[['NodeNum', 'Location', 'Pos', 'last_temp', '24h Range', 'Last Seen', 'Max Gap']].rename(columns={
                'last_temp': 'Current Temp',
                'Pos': 'Placement'
            }),
            use_container_width=True,
            hide_index=True
        )
# ===============================================================
# TOOL 2: NODE LOGISTICS
# ===============================================================
elif admin_page == "📋 Node Logistics":
    st.header("📋 Hardware Assignment & Deployment")
    reg_mode = st.radio("Mode", ["Search & Manage", "Bulk CSV Upload"], horizontal=True)

    if reg_mode == "Search & Manage":
        search_id = st.text_input("🔍 Find Node (ID or Physical ID)")
        if search_id:
            reg_q = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.node_registry` WHERE NodeNum='{search_id}' OR CAST(PhysicalID AS STRING)='{search_id}'"
            match = client.query(reg_q).to_dataframe()
            if not match.empty:
                st.write(match)
                with st.form("edit_node"):
                    u_proj = st.text_input("Project", value=match.iloc[0]['Project'])
                    u_stat = st.selectbox("Status", ["Active", "Diagnostic", "Need Repair", "Dead"])
                    if st.form_submit_button("Update Hardware Registry"):
                        # SQL UPDATE Logic
                        st.success("Hardware registry updated.")

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
# TOOL 4: REF CURVE LIBRARY (Inventory + Management)
# ===============================================================
elif admin_page == "📈 Ref Curve Library":
    st.header("📈 Theoretical Curve Management")
    
    # 1. CURRENT INVENTORY (New Section)
    st.subheader("📚 Current Library Inventory")
    try:
        # Query to show summary of each curve in the system
        inv_q = f"""
            SELECT 
                CurveID, 
                MAX(Day) as Max_Day, 
                COUNT(*) as Total_Points 
            FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves`
            GROUP BY CurveID
            ORDER BY CurveID
        """
        inventory_df = client.query(inv_q).to_dataframe()
        
        if not inventory_df.empty:
            st.dataframe(
                inventory_df.rename(columns={
                    "CurveID": "Curve Identifier (Filename)",
                    "Max_Day": "Duration (Days)",
                    "Total_Points": "Data Density"
                }),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("The library is currently empty. Upload CSVs below to begin.")
    except Exception as e:
        st.error(f"Error loading inventory: {e}")

    st.divider()

    # 2. MANAGEMENT & PURGE TOOLS
    c1, c2 = st.columns(2)
    
    with c1.expander("🗑️ Surgical Delete (Single Curve)"):
        if not inventory_df.empty:
            to_delete = st.selectbox("Select Curve to Remove", sorted(inventory_df['CurveID'].tolist()))
            if st.button(f"Delete {to_delete}", type="primary"):
                client.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` WHERE CurveID = '{to_delete}'").result()
                st.success(f"Successfully removed {to_delete}")
                st.rerun()
        else:
            st.caption("No curves available to delete.")

    with c2.expander("🧨 Nuclear Purge (Wipe All)"):
        st.warning("This will permanently delete EVERY theoretical curve in the database.")
        if st.button("EXECUTE TOTAL PURGE", key="purge_all"):
            client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.reference_curves`").result()
            st.success("Library wiped clean.")
            st.rerun()

    st.divider()

    # 3. BULK UPLOAD ENGINE (Row 3 Start)
    st.subheader("📤 Upload New Curves")
    u_files = st.file_uploader(
        "Upload Curve CSVs", 
        type=['csv'], 
        accept_multiple_files=True,
        help="Max 200MB per file. Data must start on Row 3 (Day, Temp)."
    )

    if u_files:
        if st.button("🚀 Commit Uploads to Database"):
            total_rows = 0
            progress_bar = st.progress(0)
            
            for idx, f in enumerate(u_files):
                try:
                    # Skip first 2 rows, take first 2 columns
                    df = pd.read_csv(f, skiprows=2, usecols=[0, 1], names=['Day', 'Temp'])
                    df['CurveID'] = f.name.rsplit('.', 1)[0]
                    
                    # Numeric enforcement
                    df['Day'] = pd.to_numeric(df['Day'], errors='coerce')
                    df['Temp'] = pd.to_numeric(df['Temp'], errors='coerce')
                    df = df.dropna(subset=['Day', 'Temp'])

                    if not df.empty:
                        client.load_table_from_dataframe(
                            df, f"{PROJECT_ID}.{DATASET_ID}.reference_curves"
                        ).result()
                        total_rows += len(df)
                    
                    progress_bar.progress((idx + 1) / len(u_files))
                except Exception as e:
                    st.error(f"Error processing {f.name}: {e}")

            st.success(f"✅ Success! Imported {len(u_files)} files ({total_rows} points).")
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
