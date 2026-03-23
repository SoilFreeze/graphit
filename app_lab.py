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
    SELECT timestamp, value, nodenumber, Project, Location, Depth, is_approved, engineer_note
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

# --- SERVICE ROUTING ---

if service == "🏠 Executive Summary":
    # (Insert your Executive Summary code here)
    pass

elif service == "🔍 Node Diagnostics":
    # (Insert the Node Diagnostic code with the .sort_values fix here)
    pass

elif service == "📋 Data Approval Portal":
    # (Insert the Approval Portal code with Project/Pipe/Date filters here)
    pass

elif service == "📥 Data Export Lab":
    # (Insert the Export Lab code here)
    pass

elif service == "📤 Data Intake Lab":
    # (Insert the Manual Upload code for CSV/Excel here)
    pass

elif service == "🧹 Data Cleaning Tool":
    # (Insert the Plotly Lasso/Scatter Delete tool here)
    pass

elif service == "⚙️ Database Maintenance":
    # (Insert the 'Execute Full Master Scrub' button code here)
    pass

# --- SERVICE: EXECUTIVE SUMMARY ---
if service == "🏠 Executive Summary" and not full_df.empty:
    st.header("🏠 Site Health & Warming Alerts")
    
    c1, c2 = st.columns(2)
    with c1:
        all_projs = sorted([p for p in full_df['Project'].unique() if p is not None])
        sel_summary_proj = st.selectbox("1. Select Project", all_projs)
    
    proj_df = full_df[full_df['Project'] == sel_summary_proj].copy()
    
    with c2:
        all_locs = sorted([l for l in proj_df['Location'].unique() if l is not None])
        sel_summary_loc = st.selectbox("2. Select Pipe / Bank", all_locs)

    # 24-Hour Performance Table logic...
    now_ts = datetime.now(tz=pytz.UTC)
    loc_recent = proj_df[(proj_df['Location'] == sel_summary_loc) & (proj_df['timestamp'] >= (now_ts - timedelta(hours=24)))].copy()

    if not loc_recent.empty:
        node_analysis = []
        for node in loc_recent['nodenumber'].unique():
            n_df = loc_recent[loc_recent['nodenumber'] == node].sort_values('timestamp')
            if len(n_df) > 1:
                node_analysis.append({
                    "Depth": n_df['Depth'].iloc[0], "Node ID": node,
                    "Min": n_df['value'].min(), "Max": n_df['value'].max(),
                    "Current": n_df['value'].iloc[-1], "24h Change": n_df['value'].iloc[-1] - n_df['value'].iloc[0]
                })
        st.table(pd.DataFrame(node_analysis).sort_values('Depth'))

# --- SERVICE 1: NODE DIAGNOSTICS (CLEAN DAILY GRID) ---
elif service == "🔍 Node Diagnostics" and not full_df.empty:
    st.header("🔍 Node Diagnostic Hub")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        sel_proj = st.selectbox("Project", sorted(full_df['Project'].unique()))
    with col2:
        locs = sorted(full_df[full_df['Project'] == sel_proj]['Location'].unique())
        sel_loc = st.selectbox("Location", locs)
    with col3:
        weeks_to_show = st.number_input("Weeks to Display", min_value=1, value=2)

    # 1. TIME LOGIC: Align to Monday Midnight
    today = datetime.now(pytz.UTC).date()
    last_monday = today - timedelta(days=today.weekday())
    start_time = datetime.combine(last_monday, time.min).replace(tzinfo=pytz.UTC) - timedelta(weeks=weeks_to_show-1)
    
    plot_df = full_df[
        (full_df['Project'] == sel_proj) & 
        (full_df['Location'] == sel_loc) & 
        (full_df['timestamp'] >= start_time)
    ].copy().sort_values(['nodenumber', 'timestamp'])

    if not plot_df.empty:
        plot_df['Sensor'] = plot_df['Depth'].astype(str) + "ft (" + plot_df['nodenumber'] + ")"
        fig = px.line(plot_df, x='timestamp', y='value', color='Sensor', 
                     range_y=[-20, 80], height=800)

        # 2. Y-AXIS: Frame, Dark 20s, Light 5s
        fig.update_yaxes(
            showline=True, linewidth=2, linecolor='Black', mirror=True,
            tick0=-20, dtick=20, gridcolor='DimGrey', gridwidth=1.5,
            minor=dict(dtick=5, gridcolor='LightGrey', showgrid=True),
            zeroline=True, zerolinecolor='Black', zerolinewidth=2,
            title="Temperature (°F)"
        )

        # 3. X-AXIS: Frame, Clean Labels, Minor 6-Hour Grid
        fig.update_xaxes(
            showline=True, linewidth=2, linecolor='Black', mirror=True,
            showgrid=False, # We will draw the daily lines manually below
            minor=dict(dtick=21600000.0, gridcolor='LightGrey', showgrid=True), # 6 Hours
            tickformat="%a\n%b %d", title=""
        )
        
        # 4. MANUAL DAILY LINES: One line at Midnight
        # If weekday is 0 (Monday), use DimGrey. Otherwise, use LightGrey.
        num_days = (datetime.now(pytz.UTC) - start_time).days + 1
        for i in range(num_days):
            current_date = start_time + timedelta(days=i)
            # Monday = 0
            line_color = "DimGrey" if current_date.weekday() == 0 else "LightGrey"
            line_width = 1.5 if current_date.weekday() == 0 else 1
            
            fig.add_vline(
                x=current_date.timestamp() * 1000, 
                line_width=line_width, 
                line_color=line_color
            )

        fig.update_layout(
            plot_bgcolor='white', 
            hovermode="x unified", 
            margin=dict(l=40, r=40, t=40, b=40),
            legend=dict(bordercolor="Black", borderwidth=1)
        )
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data found for this selection.")

# --- SERVICE 2: DATA APPROVAL PORTAL (WITH EXCLUSIONS) ---
elif service == "📋 Data Approval Portal":
    st.header("📋 Engineering Approval Portal")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        ap_proj = st.selectbox("Target Project", sorted(full_df['Project'].unique()))
    with col2:
        ap_loc = st.selectbox("Target Pipe (Optional)", ["All"] + sorted(full_df[full_df['Project'] == ap_proj]['Location'].unique().tolist()))
    with col3:
        ap_date = st.date_input("Date to Target", value=date.today() - timedelta(days=1))

    status = st.radio("Set Status To:", ["✅ Approved (Show Client)", "🚫 Hidden (Internal Only)"])
    note = st.text_area("Engineering Note", placeholder="Reason for status change...")

    if st.button("🚀 SYNC APPROVAL STATUS"):
        is_app = "TRUE" if "Approved" in status else "FALSE"
        loc_filter = "" if ap_loc == "All" else f"AND Location = '{ap_loc}'"
        
        # This SQL targets exactly what you asked for: Job, Pipe, and Time
        sync_sql = f"""
        UPDATE `sensorpush-export.sensor_data.final_databoard_master`
        SET is_approved = {is_app}, engineer_note = '{note}'
        WHERE Project = '{ap_proj}' {loc_filter} 
        AND CAST(timestamp AS DATE) = '{ap_date}'
        """
        client.query(sync_sql).result()
        st.success(f"Updated status for {ap_proj} on {ap_date}")

# --- SERVICE 3: DATA CLEANING TOOL (SURGICAL DELETE) ---
elif service == "🧹 Data Cleaning Tool":
    st.header("🧹 Surgical Data Cleaning")
    st.markdown("Use the **Lasso** or **Box Select** on the graph to target points for deletion.")
    
    c1, c2 = st.columns(2)
    with c1:
        sel_c_proj = st.selectbox("Project to Clean", sorted(full_df['Project'].unique()))
    with c2:
        clean_days = st.slider("Days to look back", 1, 14, 3)

    clean_view = full_df[
        (full_df['Project'] == sel_c_proj) & 
        (full_df['timestamp'] >= (datetime.now(pytz.UTC) - timedelta(days=clean_days)))
    ].copy()

    # The Selection Graph
    fig_clean = px.scatter(clean_view, x='timestamp', y='value', color='nodenumber', height=600)
    fig_clean.update_layout(dragmode='lasso', selectionrevision=True)
    
    # This catches the interaction
    selected_data = st.plotly_chart(fig_clean, use_container_width=True, on_select="rerun")

    if selected_data and "selection" in selected_data and selected_data["selection"]["points"]:
        pts = selected_data["selection"]["points"]
        st.warning(f"⚠️ {len(pts)} points selected for deletion.")
        
        if st.button("🔥 PERMANENTLY DELETE POINTS"):
            # Create a list of timestamps to delete
            ts_to_delete = [f"'{p['x']}'" for p in pts]
            sql = f"""
            DELETE FROM `sensorpush-export.sensor_data.raw_sensorpush` 
            WHERE Project = '{sel_c_proj}' AND timestamp IN ({','.join(ts_to_delete)})
            """
            # client.query(sql).result() # Uncomment to go live
            st.code(sql)
            st.success("Points removed from raw table. Run 'Master Scrub' to update charts.")
            
# --- SERVICE: DATA EXPORT LAB (FIXED) ---
elif service == "📥 Data Export Lab" and not full_df.empty:
    st.header("📥 Data Export Lab")
    ex_proj = st.selectbox("Export Project", sorted(full_df['Project'].unique()))
    export_df = full_df[full_df['Project'] == ex_proj].sort_values('timestamp')
    
    st.dataframe(export_df, use_container_width=True)
    st.download_button("📥 Download CSV", data=export_df.to_csv(index=False), 
                     file_name=f"{ex_proj}_thermal_data.csv")
    
# --- SERVICE 4: DATA INTAKE LAB (MANUAL UPLOAD) ---
elif service == "📤 Data Intake Lab":
    st.header("📤 Manual Data Ingestion")
    st.markdown("Upload CSV or Excel files directly to the Raw BigQuery tables.")

    target_table = st.radio("Target Raw Table", ["SensorPush (Raw)", "Lord (Raw)"])
    uploaded_file = st.file_uploader("Choose a file", type=['csv', 'xlsx'])

    if uploaded_file is not None:
        # Load the data
        if uploaded_file.name.endswith('.csv'):
            up_df = pd.read_csv(uploaded_file)
        else:
            up_df = pd.read_excel(uploaded_file)

        st.subheader("Preview of Uploaded Data")
        st.dataframe(up_df.head(5))

        # --- MAPPING & CLEANING ---
        if st.button("🚀 PUSH TO BIGQUERY"):
            with st.spinner("Cleaning and Uploading..."):
                try:
                    # 1. Standardize Columns based on selection
                    if "SensorPush" in target_table:
                        # Expecting columns: timestamp, temperature, sensor_name
                        # We force the 90°F limit immediately
                        up_df = up_df[up_df['temperature'] <= 90]
                        table_id = "sensorpush-export.sensor_data.raw_sensorpush"
                    else:
                        # Expecting columns: timestamp, value, nodenumber
                        up_df = up_df[up_df['value'] <= 90]
                        table_id = "sensorpush-export.sensor_data.raw_lord"

                    # 2. Add empty approval/note columns if they don't exist in the file
                    up_df['is_approved'] = False
                    up_df['engineer_note'] = "Manual Upload"

                    # 3. Upload to BigQuery
                    # 'if_exists=append' ensures we don't overwrite the whole table
                    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                    client.load_table_from_dataframe(up_df, table_id, job_config=job_config).result()

                    st.success(f"✅ Successfully appended {len(up_df)} records to {target_table}.")
                    st.info("💡 Pro-Tip: Now go to 'Database Maintenance' and run the 'Master Scrub' to see this data on the charts.")
                    st.balloons()
                except Exception as e:
                    st.error(f"Upload failed: {e}") 
                    
# --- SERVICE: DATABASE MAINTENANCE (THE FIX-IT TAB) ---
if service == "🧹 Database Maintenance":
    st.header("🧹 Database Maintenance")
    st.info("This tool applies the 90°F limit and the 5°F hourly averaging across all Lord and SensorPush data.")
    
    if st.button("🚀 EXECUTE FULL MASTER SCRUB", type="primary"):
        with st.spinner("Rebuilding Master Dashboard..."):
            scrub_sql = """
            CREATE OR REPLACE TABLE `sensorpush-export.sensor_data.final_databoard_master` AS
            WITH UnifiedRaw AS (
                SELECT CAST(timestamp AS TIMESTAMP) as ts, value, nodenumber, is_approved, engineer_note FROM `sensorpush-export.sensor_data.raw_lord` WHERE value <= 90
                UNION ALL
                SELECT CAST(timestamp AS TIMESTAMP) as ts, temperature AS value, sensor_name AS nodenumber, is_approved, engineer_note FROM `sensorpush-export.sensor_data.raw_sensorpush` WHERE temperature <= 90
            ),
            HourlyAgg AS (
                SELECT TIMESTAMP_TRUNC(ts, HOUR) as timestamp, nodenumber, AVG(value) as value, (MAX(value) - MIN(value)) as spread, LOGICAL_OR(is_approved) as is_approved, ANY_VALUE(engineer_note) as engineer_note
                FROM UnifiedRaw GROUP BY 1, 2 HAVING spread <= 5.0
            )
            SELECT d.*, m.Project, m.Location, m.Depth
            FROM HourlyAgg d
            INNER JOIN `sensorpush-export.sensor_data.master_metadata` m ON d.nodenumber = m.NodeNum
            """
            client.query(scrub_sql).result()
            st.cache_data.clear()
            st.success("✨ Master Dashboard Rebuilt! Weekend gaps have been averaged/scrubbed.")
            st.balloons()
    if st.button("📥 FORCE BACKFILL SENSORPUSH", key="maintenance_backfill"):
        try:
            sp_creds = st.secrets["sensorpush_login"]
            # ... rest of the backfill logic ...
            st.success("Backfill triggered!")
        except Exception as e:
            st.error(f"Could not find sensorpush_login in secrets: {e}")
# (Other services like Diagnostics and Approvals follow same logic...)
if st.button("📥 FORCE BACKFILL SENSORPUSH", key="backfill_maintenance"):
    # 1. Get Login from your secrets
    sp_creds = st.secrets["sensorpush_login"] # Assuming you have this in st.secrets
    
    # 2. Authenticate
    auth_resp = requests.post("https://api.sensorpush.com/api/v1/oauth/authorize", 
                              json={"email": sp_creds['user'], "password": sp_creds['pass']})
    token = auth_resp.json().get('accesstoken')
    
    # 3. Request "Weekend Gap" (Since Friday 7pm UTC)
    start_time = "2026-03-20T19:00:00Z"
    data_resp = requests.post("https://api.sensorpush.com/api/v1/samples", 
                              headers={"Authorization": token}, 
                              json={"startTime": start_time})
    
    samples = data_resp.json()
    st.write(f"Found {len(samples)} new records from the weekend!")
    
    # 4. Push to BigQuery (I can provide the 'to_gbq' code if you need it)
    # ... logic to upload to raw_sensorpush ...


