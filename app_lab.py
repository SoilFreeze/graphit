import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery, secretmanager
from google.oauth2 import service_account
from datetime import datetime, timedelta, time, date
import pytz
import json
import requests


# --- 0. PAGE CONFIG & SOILFREEZE PALETTE ---
st.set_page_config(layout="wide", page_title="SoilFreeze Engineering Hub")

def apply_sf_style():
    st.markdown("""
        <style>
            .stApp { background-color: #FFFFFF; }
            .stSidebar { background-color: #F8F9FA; border-right: 1px solid #E0E0E0; }
            h1, h2, h3 { color: #003366 !important; font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; }
            .stButton>button { background-color: #003366; color: white; border-radius: 4px; border: none; width: 100%; }
            .stMetric { background-color: #F8F9FA; padding: 10px; border-radius: 5px; border: 1px solid #E0E0E0; }
        </style>
    """, unsafe_allow_html=True)

apply_sf_style()

# --- 1. AUTHENTICATION (SECRET MANAGER) ---
@st.cache_resource
def get_bq_client():
    # 1. TRY SECRET MANAGER (The "One Source" Plan)
    try:
        from google.cloud import secretmanager
        sm_client = secretmanager.SecretManagerServiceClient()
        # Ensure 'sensorpush-export' is your EXACT project ID
        name = "projects/sensorpush-export/secrets/BIGQUERY_SERVICE_ACCOUNT_JSON/versions/latest"
        
        # We add a 5-second timeout so it doesn't get "stuck"
        response = sm_client.access_secret_version(request={"name": name}, timeout=5)
        info = json.loads(response.payload.data.decode("UTF-8"))
        credentials = service_account.Credentials.from_service_account_info(info)
        scoped_creds = credentials.with_scopes([
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery"
        ])
        return bigquery.Client(credentials=scoped_creds, project=info["project_id"])

    except Exception as e:
        # 2. FALLBACK TO LOCAL SECRETS (So you aren't stuck!)
        st.sidebar.warning("⚠️ Secret Manager failed. Using local secrets.")
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(info)
            # Make sure we still add the Drive scope here for the Metadata join
            scoped_creds = credentials.with_scopes([
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/bigquery"
            ])
            return bigquery.Client(credentials=scoped_creds, project=info["project_id"])
        else:
            raise Exception("No credentials found in Secret Manager or st.secrets")

client = get_bq_client()

# --- 2. DATA FETCHING (Using the Cleaned Master Table) ---
@st.cache_data(ttl=600)
def fetch_engineering_data():
    # We now pull from the 'final_databoard_master' which is already scrubbed and joined
    query = """
    SELECT timestamp, value, nodenumber, Project, Location, Depth, is_approved
    FROM `sensorpush-export.sensor_data.final_databoard_master`
    """
    df = client.query(query).to_dataframe()
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df

full_df = pd.DataFrame()
try:
    full_df = fetch_engineering_data()
except Exception as e:
    st.error(f"⚠️ Master Table Missing or Error: {e}. Run 'Database Maintenance' to build it.")

# --- 3. SIDEBAR NAVIGATION ---
st.sidebar.title("🛠 Engineering Hub")

# Update this list to include all your new tools
service = st.sidebar.selectbox(
    "Select Service", 
    [
        "🏠 Executive Summary", 
        "🔍 Node Diagnostics", 
        "📋 Data Approval Portal",
        "📥 Data Export Lab", 
        "📤 Data Intake Lab",      # New: For Manual Uploads
        "🧹 Data Cleaning Tool",   # New: For Surgical Deletes
        "⚙️ Database Maintenance"   # New: For the Master Scrub
    ]
)

# --- SHARED SERVICE SELECTION ---
service = st.sidebar.selectbox("Select Service", [
    "🏠 Executive Summary", 
    "📈 Node Diagnostics", 
    "📤 Data Intake Lab", 
    "⚙️ Database Maintenance"
])

# --- SERVICE 1: EXECUTIVE SUMMARY (RESTORED) ---
if service == "🏠 Executive Summary":
    st.header("🏠 Executive Summary")
    try:
        # Get Project List
        m_q = "SELECT DISTINCT Project FROM `sensorpush-export.sensor_data.master_metadata` ORDER BY Project"
        p_list = client.query(m_q).to_dataframe()['Project'].tolist()
        sel_proj = st.selectbox("Select Project Focus", p_list if p_list else ["Maltby"])

        # Restored Query with Min/Max and Latest Temp
        exec_q = f"""
            SELECT 
                nodenumber, 
                MAX(timestamp) as last_seen,
                MIN(value) as min_temp,
                MAX(value) as max_temp,
                -- Get the actual most recent reading
                ARRAY_AGG(value ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] as current_temp,
                '--' as engineer_note
            FROM `sensorpush-export.sensor_data.final_databoard_master`
            WHERE Project = '{sel_proj}'
            GROUP BY nodenumber
        """
        df_ex = client.query(exec_q).to_dataframe()

        if df_ex.empty:
            st.warning("Master Table is empty. Run 'Master Scrub' in Maintenance.")
        else:
            # Metrics
            avg_t = df_ex['current_temp'].mean()
            m1, m2 = st.columns(2)
            m1.metric("Project Avg", f"{avg_t:.1f}°F")
            m2.metric("Sensors Online", len(df_ex))

            # Styling logic
            df_disp = df_ex.copy()
            df_disp['current_temp'] = df_disp['current_temp'].round(1)
            df_disp['min_temp'] = df_disp['min_temp'].round(1)
            df_disp['max_temp'] = df_disp['max_temp'].round(1)
            df_disp['last_seen'] = pd.to_datetime(df_disp['last_seen']).dt.strftime('%m/%d %H:%M')

            def color_t(v):
                if v > 32: return 'color: #ff4b4b'
                if 28 <= v <= 32: return 'color: #ffa500'
                return 'color: #28a745'

            st.dataframe(
                df_disp.style.applymap(color_t, subset=['current_temp', 'min_temp', 'max_temp']),
                use_container_width=True,
                hide_index=True
            )
    except Exception as e:
        st.error(f"Executive Summary Error: {e}")


# --- SERVICE 2: NODE DIAGNOSTICS (NULL-SAFE VERSION) ---
elif service == "📈 Node Diagnostics":
    st.header("📈 Node Diagnostics")
    
    # 1. SIDE-BY-SIDE FILTERS
    col_filt1, col_filt2, col_filt3 = st.columns(3)
    
    with col_filt1:
        # Project Filter
        m_q = "SELECT DISTINCT Project FROM `sensorpush-export.sensor_data.master_metadata` ORDER BY Project"
        projects = client.query(m_q).to_dataframe()['Project'].tolist()
        sel_projects = st.multiselect("Filter Projects", projects)
    
    with col_filt2:
        # Location Filter - only populates if projects are selected
        if sel_projects:
            loc_q = f"SELECT DISTINCT Location FROM `sensorpush-export.sensor_data.master_metadata` WHERE Project IN UNNEST({sel_projects}) ORDER BY Location"
            locations = client.query(loc_q).to_dataframe()['Location'].tolist()
            sel_locations = st.multiselect("Filter Specific Pipes", locations, default=locations)
        else:
            sel_locations = []
            st.info("👈 Select a project to see pipes.")

    with col_filt3:
        weeks = st.slider("Weeks to Display", 1, 12, 6)

    # 2. THE GRAPHING ENGINE - Only runs if BOTH filters have values
    if sel_projects and sel_locations:
        try:
            days = weeks * 7
            # Use tuple() to format the lists correctly for BigQuery's 'IN' clause
            # This avoids the 'None' error
            graph_q = f"""
                SELECT timestamp, value, nodenumber, Location, Depth
                FROM `sensorpush-export.sensor_data.final_databoard_master`
                WHERE Project IN UNNEST({list(sel_projects)})
                AND Location IN UNNEST({list(sel_locations)})
                AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
                ORDER BY timestamp ASC
            """
            df_graph = client.query(graph_q).to_dataframe()

            if not df_graph.empty:
                import plotly.express as px
                df_graph['label'] = df_graph['Location'] + " (" + df_graph['Depth'] + ")"
                
                fig = px.line(
                    df_graph, 
                    x='timestamp', 
                    y='value', 
                    color='label',
                    title=f"Temperature Trends: Last {weeks} Weeks",
                    labels={'value': 'Temp (°F)', 'timestamp': 'Time', 'label': 'Pipe'}
                )
                
                # Restore Reference Lines
                fig.add_hline(y=32, line_dash="dash", line_color="#ff4b4b", annotation_text="32°F")
                fig.add_hline(y=28, line_dash="dot", line_color="#28a745", annotation_text="28°F")
                
                fig.update_layout(hovermode="x unified", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
                st.plotly_chart(fig, use_container_width=True)
                
                # Stats Table
                st.subheader("Sensor Summary")
                stats = df_graph.groupby('label')['value'].agg(['min', 'max', 'mean']).round(1)
                st.table(stats)
            else:
                st.info("No data found for these specific pipes in the last few weeks.")
        except Exception as e:
            st.error(f"Graph Error: {e}")
    else:
        st.info("Please select both a Project and at least one Pipe to view the data.")

# --- SERVICE 3: DATA INTAKE LAB ---
elif service == "📤 Data Intake Lab":
    st.header("📤 Manual Data Ingestion")
    source = st.radio("Source", ["SensorPush (CSV)", "Lord (SensorConnect)"], horizontal=True)
    u_file = st.file_uploader("Upload", type=['csv'], key="lab_u")

    if u_file:
        try:
            if "Lord" in source:
                lines = u_file.getvalue().decode("utf-8").splitlines()
                start = next((i for i, l in enumerate(lines) if "DATA_START" in l), 0)
                u_file.seek(0)
                df = pd.read_csv(u_file, skiprows=start + 1)
                df_up = df.melt(id_vars=[df.columns[0]], var_name='nodenumber', value_name='value')
                df_up = df_up.rename(columns={df.columns[0]: 'timestamp'})
                df_up['nodenumber'] = df_up['nodenumber'].str.replace(':', '-', regex=False)
                t_table, v_col = "sensorpush-export.sensor_data.raw_lord", 'value'
            else:
                df_up = pd.read_csv(u_file).rename(columns={'Timestamp':'timestamp','Temperature':'temperature','Sensor':'sensor_name'})
                df_up['sensor_name'] = df_up['sensor_name'].str.replace(':', '-', regex=False)
                t_table, v_col = "sensorpush-export.sensor_data.raw_sensorpush", 'temperature'

            # Clean and Force Types
            df_up['timestamp'] = pd.to_datetime(df_up['timestamp'], errors='coerce', utc=True)
            df_up[v_col] = pd.to_numeric(df_up[v_col], errors='coerce')
            df_up = df_up.dropna(subset=['timestamp', v_col])

            if st.button("🚀 PUSH TO CLOUD", key="push_lab"):
                client.load_table_from_dataframe(df_up, t_table).result()
                st.success("Uploaded!")
        except Exception as e:
            st.error(f"Intake Error: {e}")

# --- SERVICE 4: DATABASE MAINTENANCE ---
elif service == "⚙️ Database Maintenance":
    st.header("⚙️ Database Maintenance")
    
    if st.button("🔄 EXECUTE MASTER SCRUB", key="scrub_final"):
        with st.spinner("Rebuilding Master Table..."):
            try:
                # This query recreates the columns you deleted from RAW into the MASTER table
                scrub_q = """
                CREATE OR REPLACE TABLE `sensorpush-export.sensor_data.final_databoard_master` AS
                WITH UnifiedRaw AS (
                    SELECT CAST(timestamp AS TIMESTAMP) as ts, value, REPLACE(nodenumber, ':', '-') as nodenumber
                    FROM `sensorpush-export.sensor_data.raw_lord` WHERE value <= 90
                    UNION ALL
                    SELECT CAST(timestamp AS TIMESTAMP) as ts, temperature AS value, REPLACE(sensor_name, ':', '-') as nodenumber
                    FROM `sensorpush-export.sensor_data.raw_sensorpush` WHERE temperature <= 90
                ),
                HourlyAgg AS (
                    SELECT TIMESTAMP_TRUNC(ts, HOUR) as timestamp, nodenumber, AVG(value) as value
                    FROM UnifiedRaw GROUP BY 1, 2
                )
                SELECT 
                    d.*, m.Project, m.Location, m.Depth,
                    CAST(NULL AS STRING) as engineer_note,
                    CAST(FALSE AS BOOL) as is_approved
                FROM HourlyAgg d
                INNER JOIN `sensorpush-export.sensor_data.master_metadata` m 
                    ON d.nodenumber = REPLACE(m.NodeNum, ':', '-')
                """
                client.query(scrub_q).result()
                st.success("✅ Master Table Rebuilt!")
            except Exception as e:
                st.error(f"Scrub error: {e}")
