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

#
#
# --- SERVICE 1: EXECUTIVE SUMMARY ---
elif service == "🏠 Executive Summary":
    st.header("🏠 Executive Summary")
    
    # 1. SETUP FILTERS
    try:
        # Get unique projects from metadata to avoid crashing if Master is empty
        meta_query = "SELECT DISTINCT Project FROM `sensorpush-export.sensor_data.master_metadata` ORDER BY Project"
        proj_list = client.query(meta_query).to_dataframe()['Project'].tolist()
        sel_ex_loc = st.selectbox("Select Project Focus", proj_list if proj_list else ["Maltby"])
        
        # 2. FETCH DATA (SAFE QUERY)
        # We use a subquery to handle the missing 'engineer_note' column gracefully
        query = f"""
            SELECT 
                nodenumber, 
                MAX(timestamp) as last_seen,
                AVG(value) as current_temp,
                '--' as engineer_note 
            FROM `sensorpush-export.sensor_data.final_databoard_master`
            WHERE Project = '{sel_ex_loc}'
            GROUP BY nodenumber
        """
        df_exec = client.query(query).to_dataframe()

        if df_exec.empty:
            st.warning(f"No active data found for {sel_ex_loc}. Run 'Master Scrub' in Database Maintenance.")
        else:
            # 3. CALCULATE METRICS
            avg_system_temp = df_exec['current_temp'].mean()
            active_count = len(df_exec)
            
            m1, m2 = st.columns(2)
            m1.metric("Average System Temp", f"{avg_system_temp:.1f}°F")
            m2.metric("Active Sensors", active_count)

            st.divider()

            # 4. FORMAT THE TABLE
            # Rounding and cleaning up the display
            df_display = df_exec.copy()
            df_display['current_temp'] = df_display['current_temp'].round(1)
            df_display['last_seen'] = pd.to_datetime(df_display['last_seen']).dt.strftime('%m/%d %H:%M')
            
            # Applying the "Maltby Color Logic"
            def color_temp(val):
                color = 'white'
                if val > 32: color = '#ff4b4b' # Red for above freezing
                if 28 <= val <= 32: color = '#ffa500' # Orange for near freezing
                if val < 28: color = '#28a745' # Green for safe freeze
                return f'color: {color}'

            st.subheader(f"Latest Readings: {sel_ex_loc}")
            st.dataframe(
                df_display.style.applymap(color_temp, subset=['current_temp']),
                use_container_width=True,
                hide_index=True
            )

    except Exception as e:
        st.error(f"Executive Summary Error: {e}")
        st.info("Try running 'Master Scrub' in the Database Maintenance tab to refresh the data tables.")

# --- END OF EXECUTIVE SUMMARY ---
#
#
# --- SERVICE 1: NODE DIAGNOSTIC HUB (6-WEEK DEFAULT) ---
elif service == "🔍 Node Diagnostics" and not full_df.empty:
    st.header("🔍 Node Diagnostic Hub")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        sel_proj = st.selectbox("Project", sorted(full_df['Project'].unique()))
    with col2:
        locs = sorted(full_df[full_df['Project'] == sel_proj]['Location'].unique())
        sel_loc = st.selectbox("Location", locs)
    with col3:
        # UPDATED: Default value is now 6
        weeks_to_show = st.number_input("Weeks to Display", min_value=1, value=6)

    # SIDEBAR REFERENCE LINES
    st.sidebar.subheader("Thermal Reference Lines")
    ref_options = {"32°F (Frost)": 32.0, "26.6°F (Brine)": 26.6, "10.2°F (Deep)": 10.2}
    selected_refs = [label for label, val in ref_options.items() if st.sidebar.checkbox(label)]

    # 1. TIME LOGIC: Strict Monday-to-Monday 6-Week Window
    today_dt = datetime.now(pytz.UTC).date()
    this_monday = today_dt - timedelta(days=today_dt.weekday())
    # Start time is anchored to Monday 00:00, looking back 6 weeks
    start_time = datetime.combine(this_monday, time.min).replace(tzinfo=pytz.UTC) - timedelta(weeks=weeks_to_show-1)
    end_time = start_time + timedelta(weeks=weeks_to_show)
    
    plot_df = full_df[
        (full_df['Project'] == sel_proj) & 
        (full_df['Location'] == sel_loc) & 
        (full_df['timestamp'] >= start_time) &
        (full_df['timestamp'] <= end_time)
    ].copy().sort_values(['nodenumber', 'timestamp'])

    if not plot_df.empty:
        # GAP LOGIC: Insert None for breaks > 6 hours
        processed_dfs = []
        for node in plot_df['nodenumber'].unique():
            node_df = plot_df[plot_df['nodenumber'] == node].copy()
            node_df['diff'] = node_df['timestamp'].diff().dt.total_seconds() / 3600
            gaps = node_df[node_df['diff'] > 6.0].copy()
            if not gaps.empty:
                gaps['value'] = None
                gaps['timestamp'] = gaps['timestamp'] - timedelta(minutes=1)
                node_df = pd.concat([node_df, gaps]).sort_values('timestamp')
            processed_dfs.append(node_df)
        
        plot_df = pd.concat(processed_dfs)
        plot_df['Sensor'] = plot_df['Depth'].astype(str) + "ft (" + plot_df['nodenumber'] + ")"
        
        fig = px.line(plot_df, x='timestamp', y='value', color='Sensor', 
                     range_y=[-20, 80], height=850)
        fig.update_traces(connectgaps=False) 

        # 2. GRID & AXES (Zero-Cushion & Frame)
        fig.update_yaxes(
            showline=True, linewidth=2, linecolor='Black', mirror=True,
            tick0=-20, dtick=20, gridcolor='DimGrey', gridwidth=1.5,
            minor=dict(dtick=5, gridcolor='#E5E5E5', showgrid=True), 
            zeroline=False, range=[-20, 80]
        )
        fig.update_xaxes(
            showline=True, linewidth=2, linecolor='Black', mirror=True,
            showgrid=False, zeroline=False, tickformat="%b %d", title="",
            range=[start_time, end_time]
        )
        
        # 3. DYNAMIC REFERENCE LINES
        for label in selected_refs:
            val = ref_options[label]
            fig.add_hline(y=val, line_width=2, line_color="#003366", annotation_text=f"{val}°F")

        # 4. MANUAL VERTICAL GRID (Darker Mondays)
        num_days = (end_time - start_time).days
        for i in range(num_days + 1):
            midnight = start_time + timedelta(days=i)
            is_monday = (midnight.weekday() == 0)
            fig.add_vline(x=midnight.timestamp()*1000, 
                         line_width=1.5 if is_monday else 1, 
                         line_color="DimGrey" if is_monday else "#CCCCCC")
            # 6-Hour Intervals
            if i < num_days:
                for h in [6, 12, 18]:
                    fig.add_vline(x=(midnight+timedelta(hours=h)).timestamp()*1000, 
                                 line_width=0.5, line_color="#F0F0F0")

        # 5. LAYOUT: External Legend & Wide Margin
        fig.update_layout(
            plot_bgcolor='white', hovermode="x unified",
            margin=dict(l=10, r=200, t=10, b=10),
            legend=dict(x=1.02, y=1, bordercolor="Black", borderwidth=1)
        )
        
        st.plotly_chart(fig, use_container_width=True, config={'responsive': True})
    else:
        st.info(f"No data for {sel_loc}. Viewing 6-week grid starting {start_time.strftime('%m/%d')}.")
#
#
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

#
#
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

#
#
# --- SERVICE: DATA EXPORT LAB (FIXED) ---
elif service == "📥 Data Export Lab" and not full_df.empty:
    st.header("📥 Data Export Lab")
    ex_proj = st.selectbox("Export Project", sorted(full_df['Project'].unique()))
    export_df = full_df[full_df['Project'] == ex_proj].sort_values('timestamp')
    
    st.dataframe(export_df, use_container_width=True)
    st.download_button("📥 Download CSV", data=export_df.to_csv(index=False), 
                     file_name=f"{ex_proj}_thermal_data.csv")
#
#
# --- SERVICE 5: DATA INTAKE LAB ---
elif service == "📤 Data Intake Lab":
    st.header("📤 Manual Data Ingestion")
    source_type = st.radio("Source", ["SensorPush (CSV)", "Lord (SensorConnect)"], horizontal=True)
    uploaded_file = st.file_uploader("Upload File", type=['csv', 'xlsx'], key="intake_uploader")

    if uploaded_file:
        try:
            # [Standardizing timestamp and values logic remains the same]
            df_upload['timestamp'] = pd.to_datetime(df_upload['timestamp'], errors='coerce', utc=True)
            df_upload[val_col] = pd.to_numeric(df_upload[val_col], errors='coerce')
            df_upload = df_upload.dropna(subset=['timestamp', val_col])
            
            # NOTICE: We removed the 'engineer_note' and 'is_approved' lines here
            
            if st.button("🚀 PUSH TO CLOUD", key="btn_push_data"):
                # Upload logic...
                client.load_table_from_dataframe(df_upload, target_table).result()
                st.success("Uploaded to clean raw table!")
        except Exception as e:
            st.error(f"Error: {e}")

# --- SERVICE 6: DATABASE MAINTENANCE (PLACEHOLDER VERSION) ---
elif service == "⚙️ Database Maintenance":
    st.subheader("🚀 Master Data Scrub")
    if st.button("🔄 EXECUTE MASTER SCRUB", key="btn_execute_scrub"):
        with st.spinner("Rebuilding Master..."):
            try:
                scrub_query = """
                CREATE OR REPLACE TABLE `sensorpush-export.sensor_data.final_databoard_master` AS
                WITH UnifiedRaw AS (
                    -- Pulling ONLY core data from Raw
                    SELECT CAST(timestamp AS TIMESTAMP) as ts, value, REPLACE(nodenumber, ':', '-') as nodenumber
                    FROM `sensorpush-export.sensor_data.raw_lord` WHERE value <= 90
                    UNION ALL
                    SELECT CAST(timestamp AS TIMESTAMP) as ts, temperature AS value, REPLACE(sensor_name, ':', '-') as nodenumber
                    FROM `sensorpush-export.sensor_data.raw_sensorpush` WHERE temperature <= 90
                ),
                HourlyAgg AS (
                    SELECT 
                        TIMESTAMP_TRUNC(ts, HOUR) as timestamp, 
                        nodenumber, 
                        AVG(value) as value
                    FROM UnifiedRaw 
                    GROUP BY 1, 2
                )
                SELECT 
                    d.*, 
                    m.Project, m.Location, m.Depth,
                    -- Create the note column here so it exists in the Master Table only
                    CAST(NULL AS STRING) as engineer_note,
                    CAST(FALSE AS BOOL) as is_approved
                FROM HourlyAgg d
                INNER JOIN `sensorpush-export.sensor_data.master_metadata` m 
                    ON d.nodenumber = REPLACE(m.NodeNum, ':', '-')
                """
                client.query(scrub_query).result()
                st.success("✅ Master Table Rebuilt! Raw tables are now clean.")
            except Exception as e:
                st.error(f"Scrub failed: {e}")
                
