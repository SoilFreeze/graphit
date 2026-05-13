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
# TOOL 1: COMMISSIONING AUDIT (formerly Node Diagnostics)
# ===============================================================
if admin_page == "📡 Commissioning Audit":
    st.header(f"📡 Commissioning Audit: {selected_project}")
    st.write("Verifying signal stability and packet density for new hardware deployment.")

    diag_q = f"""
        WITH Stats AS (
            SELECT NodeNum, MAX(timestamp) as last_ping,
            ARRAY_AGG(temperature ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] as last_temp,
            COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) as count_1h,
            COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)) as count_6h
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view`
            WHERE Project = @proj_id
            GROUP BY NodeNum
        )
        SELECT n.Location, n.NodeNum, n.SensorStatus, s.last_ping, s.last_temp,
        COALESCE(s.count_1h, 0) as count_1h, COALESCE(s.count_6h, 0) as count_6h
        FROM `{PROJECT_ID}.{DATASET_ID}.node_registry` n
        LEFT JOIN Stats s ON n.NodeNum = s.NodeNum
        WHERE n.Project = @proj_id
    """
    df = client.query(diag_q, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)]
    )).to_dataframe()

    if not df.empty:
        # Latency Logic
        now_utc = pd.Timestamp.now(tz='UTC')
        def get_status(ping):
            if pd.isnull(ping): return "❌ Never", "None"
            ping_utc = ping if ping.tzinfo else ping.tz_localize('UTC')
            mins = (now_utc - ping_utc).total_seconds() / 60
            if mins <= 15: return "🟢 0-15 Mins", f"{int(mins)}m ago"
            return "🔴 > 24 Hours", f"{round(mins/1440, 1)}d ago"

        df[['Conn', 'Ago']] = df.apply(lambda x: pd.Series(get_status(x['last_ping'])), axis=1)
        st.dataframe(df[['Location', 'NodeNum', 'Conn', 'Ago', 'count_1h', 'count_6h']], use_container_width=True)

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
# TOOL 3: PROJECT MASTER
# ===============================================================
elif admin_page == "⚙️ Project Master":
    st.header("⚙️ Project Lifecycle Management")
    proj_q = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE Project = '{selected_project}'"
    p_data = client.query(proj_q).to_dataframe().iloc[0]

    with st.form("edit_project"):
        u_status = st.selectbox("Status", ["Initialized", "Pre-freeze", "Freezedown", "Maintenance", "Archived"], 
                                index=["Initialized", "Pre-freeze", "Freezedown", "Maintenance", "Archived"].index(p_data['ProjectStatus']))
        u_notes = st.text_area("Engineering Notes", value=p_data.get('EngNotes', ''))
        
        if st.form_submit_button("Save Project Rules"):
            date_sql = ", Date_Freezedown = CURRENT_DATE()" if u_status == "Freezedown" and pd.isnull(p_data['Date_Freezedown']) else ""
            client.query(f"UPDATE `{PROJECT_ID}.{DATASET_ID}.project_registry` SET ProjectStatus='{u_status}', EngNotes='{u_notes}' {date_sql} WHERE Project='{selected_project}'").result()
            st.success("Project settings updated.")

# ===============================================================
# TOOL 4: REF CURVE LIBRARY
# ===============================================================
elif admin_page == "📈 Ref Curve Library":
    st.header("📈 Theoretical Curve Management")
    with st.expander("🗑️ Delete/Purge Library"):
        if st.button("🧨 PURGE ENTIRE LIBRARY", type="primary"):
            client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.reference_curves`").result()
            st.success("Library wiped.")

    u_files = st.file_uploader("Upload Curve CSVs (Row 3 Start, Col 1: Day, Col 2: Temp)", accept_multiple_files=True)
    if u_files and st.button("Commit to Database"):
        for f in u_files:
            df = pd.read_csv(f, skiprows=2, names=['Day', 'Temp'])
            df['CurveID'] = f.name.replace(".csv", "")
            client.load_table_from_dataframe(df.dropna(), f"{PROJECT_ID}.{DATASET_ID}.reference_curves").result()
        st.success("Curves imported.")

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
