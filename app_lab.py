import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
import io
import plotly.graph_objects as go
from datetime import timedelta

# --- STANDARDIZED GRAPHING FUNCTION ---
def build_standard_sf_graph(df, title, start_time, end_time):
    # 1. DATA GAP LOGIC: Insert None for breaks > 6 hours
    # This prevents the line from "stretching" across missing data periods
    processed_dfs = []
    for sensor in df['Sensor'].unique():
        s_df = df[df['Sensor'] == sensor].copy().sort_values('timestamp')
        s_df['gap'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
        gaps = s_df[s_df['gap'] > 6.0].copy()
        if not gaps.empty:
            gaps['value'] = None
            gaps['timestamp'] = gaps['timestamp'] - timedelta(minutes=1)
            s_df = pd.concat([s_df, gaps]).sort_values('timestamp')
        processed_dfs.append(s_df)
    
    clean_df = pd.concat(processed_dfs) if processed_dfs else df

    # 2. CREATE BASE CHART
    fig = px.line(clean_df, x='timestamp', y='value', color='Sensor',
                 hover_data={'timestamp': '|%b %d, %H:%M'},
                 range_y=[-20, 80])
    
    # 3. Y-AXIS: Temperature Labeling & 20/5 Degree Grid
    fig.update_yaxes(
        title="Temperature (°F)",
        tickmode='array',
        tickvals=[-20, 0, 20, 40, 60, 80],
        gridcolor='DimGray', gridwidth=1.5, # Dark gray at 20° increments
        minor=dict(dtick=5, gridcolor='Silver', showgrid=True), # Medium gray at 5°
        range=[-20, 80],
        mirror=True, showline=True, linecolor='black', linewidth=2
    )

    # 4. X-AXIS: Custom Time Grid (Monday/Midnight/6-Hour)
    # We remove default grid to draw our own
    fig.update_xaxes(showgrid=False, range=[start_time, end_time], 
                     mirror=True, showline=True, linecolor='black', linewidth=2)

    # Generate the Custom Gridlines
    shapes = []
    curr = start_time.replace(hour=0, minute=0, second=0)
    while curr <= end_time:
        for h in [0, 6, 12, 18]:
            check_time = curr + timedelta(hours=h)
            if check_time < start_time or check_time > end_time: continue
            
            # Monday Midnight = Dark Gray
            if check_time.weekday() == 0 and h == 0:
                color, width = "DimGray", 2
            # Daily Midnight = Medium Gray
            elif h == 0:
                color, width = "DarkGray", 1
            # 6-Hour = Light Gray
            else:
                color, width = "LightGray", 0.5
            
            shapes.append(dict(type="line", xref="x", yref="paper",
                               x0=check_time, y0=0, x1=check_time, y1=1,
                               line=dict(color=color, width=width), layer="below"))
        curr += timedelta(days=1)

    # 5. FINAL LAYOUT: Centered Title & Right Legend
    fig.update_layout(
        title={'text': title, 'x': 0.5, 'xanchor': 'center'},
        shapes=shapes,
        plot_bgcolor='white',
        legend=dict(title="Depth / Location", x=1.02, y=1, bordercolor="Black", borderwidth=1),
        margin=dict(l=60, r=150, t=80, b=60),
        height=700
    )
    
    return fig

# --- 1. CONFIGURATION & STYLING ---
st.set_page_config(page_title="SoilFreeze Data Lab", layout="wide")

# Set these variables to toggle between environments
DATASET_ID = "sensor_data"  # Set to "sensor_data_dev" for your dev app
PROJECT_ID = "sensorpush-export"

# --- 2. AUTHENTICATION ENGINE ---
@st.cache_resource
def get_bq_client():
    """Retrieves credentials from Secrets to prevent TransportErrors."""
    try:
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(info)
            scoped_creds = credentials.with_scopes([
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/bigquery"
            ])
            return bigquery.Client(credentials=scoped_creds, project=info["project_id"])
        else:
            return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"Authentication Failed: {e}")
        return None

client = get_bq_client()

# --- 3. SIDEBAR NAVIGATION ---
st.sidebar.title("❄️ SoilFreeze Lab")
service = st.sidebar.selectbox("Select Service", [
    "🏠 Executive Summary", 
    "📈 Node Diagnostics", 
    "📤 Data Intake Lab", 
    "⚙️ Database Maintenance"
])

# --- SERVICE 1: EXECUTIVE SUMMARY ---
if service == "🏠 Executive Summary":
    st.header("🏠 Executive Summary")
    
    query = f"""
        SELECT 
            nodenumber, Project, Location, Depth,
            MAX(timestamp) as last_seen,
            AVG(value) as current_temp
        FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master`
        GROUP BY 1, 2, 3, 4
    """
    try:
        df_ex = client.query(query).to_dataframe()
        
        if df_ex.empty:
            st.warning("Master Table is empty. Run 'Database Maintenance' first.")
        else:
            avg_t = df_ex['current_temp'].mean()
            c1, c2 = st.columns(2)
            c1.metric("Project Avg Temp", f"{avg_t:.1f}°F")
            c2.metric("Active Sensors", len(df_ex))

            def thermal_style(v):
                if v > 32: return 'background-color: #ff4b4b; color: white' 
                if 28 <= v <= 32: return 'background-color: #ffa500'        
                return 'background-color: #28a745; color: white'           
            
            st.dataframe(df_ex.style.applymap(thermal_style, subset=['current_temp']), use_container_width=True)
    except Exception as e:
        st.error(f"Error loading summary: {e}")

# --- SERVICE 2: NODE DIAGNOSTICS (THE MISSING PIECE) ---
elif service == "📈 Node Diagnostics":
    st.header("📈 Node Diagnostics")
    
    try:
        # 1. Fetch metadata for the dynamic filters
        meta_q = f"SELECT DISTINCT Project, Location FROM `{PROJECT_ID}.{DATASET_ID}.master_metadata` ORDER BY Project"
        meta_df = client.query(meta_q).to_dataframe()
        
        col1, col2, col3 = st.columns(3)
        with col1:
            all_projs = sorted(meta_df['Project'].unique())
            sel_projs = st.multiselect("1. Filter Projects", all_projs)
        
        with col2:
            # Only show locations for the selected projects
            available_locs = meta_df[meta_df['Project'].isin(sel_projs)]['Location'].unique() if sel_projs else []
            sel_locs = st.multiselect("2. Filter Specific Pipes", sorted(available_locs))
            
        with col3:
            weeks = st.slider("3. Trend Duration (Weeks)", 1, 12, 6)

        # 2. Query and Graphing Logic
        if sel_projs and sel_locs:
            days = weeks * 7
            graph_q = f"""
                SELECT timestamp, value, Location, Depth
                FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master`
                WHERE Project IN UNNEST({list(sel_projs)})
                AND Location IN UNNEST({list(sel_locs)})
                AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
                ORDER BY timestamp ASC
            """
            df_g = client.query(graph_q).to_dataframe()

            if not df_g.empty:
                # Combine Location and Depth for a clear legend: "Pipe 4 (15ft)"
                df_g['Sensor'] = df_g['Location'] + " (" + df_g['Depth'].astype(str) + "ft)"
                
                fig = px.line(df_g, x='timestamp', y='value', color='Sensor', 
                             title=f"Thermal Trends: Last {weeks} Weeks")
                
                # Maltby engineering reference lines
                fig.add_hline(y=32, line_dash="dash", line_color="#ff4b4b", annotation_text="32°F Warning")
                fig.add_hline(y=28, line_dash="dot", line_color="#28a745", annotation_text="28°F Target")
                
                fig.update_layout(hovermode="x unified", legend=dict(orientation="h", y=-0.2))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No data found for the selected projects/pipes within this timeframe.")
        else:
            st.info("Please select at least one Project and one Pipe to view diagnostics.")
            
    except Exception as e:
        st.error(f"Diagnostics Error: {e}")

# --- IMPLEMENTATION IN NODE DIAGNOSTICS ---
if service == "📈 Node Diagnostics":
    # ... (Keep your filter code from before) ...
    
    if sel_projs and sel_locs:
        # Define window and fetch df...
        # Ensure Depth has 'ft' and Node Location (S1, R3) is clean
        df_g['Sensor'] = df_g.apply(lambda x: f"{x['Depth']}ft" if str(x['Depth']).isdigit() else x['Location'], axis=1)
        
        graph_title = f"Temperature: {', '.join(sel_locs)} | {weeks} Week Trend"
        
        fig = build_standard_sf_graph(df_g, graph_title, start_time, end_time)
        st.plotly_chart(fig, use_container_width=True)

# --- SERVICE 3: DATA INTAKE LAB ---
elif service == "📤 Data Intake Lab":
    st.header("📤 Manual Data Ingestion")
    source = st.radio("Device Type", ["SensorPush (CSV)", "Lord (SensorConnect)"])
    u_file = st.file_uploader("Upload Logger File", type=['csv'])

    if u_file:
        try:
            if "Lord" in source:
                content = u_file.getvalue().decode("utf-8").splitlines()
                start_idx = next((i for i, l in enumerate(content) if "DATA_START" in l), 0)
                u_file.seek(0)
                df_raw = pd.read_csv(u_file, skiprows=start_idx + 1)
                df_up = df_raw.melt(id_vars=[df_raw.columns[0]], var_name='nodenumber', value_name='value')
                df_up = df_up.rename(columns={df_raw.columns[0]: 'timestamp'})
                df_up['nodenumber'] = df_up['nodenumber'].str.replace(':', '-', regex=False)
                table_ref = f"{PROJECT_ID}.{DATASET_ID}.raw_lord"
            
            if st.button("🚀 PUSH TO BIGQUERY"):
                client.load_table_from_dataframe(df_up, table_ref).result()
                st.success("Data ingested successfully!")
        except Exception as e:
            st.error(f"Intake Error: {e}")

# --- SERVICE 4: DATABASE MAINTENANCE ---
elif service == "⚙️ Database Maintenance":
    st.header("⚙️ Database Maintenance")
    if st.button("🔄 EXECUTE MASTER SCRUB"):
        with st.spinner("Rebuilding Master Table..."):
            try:
                scrub_q = f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` AS
                WITH Unified AS (
                    SELECT timestamp, value, REPLACE(nodenumber, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
                    UNION ALL
                    SELECT timestamp, temperature as value, REPLACE(sensor_name, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
                )
                SELECT u.*, m.Project, m.Location, m.Depth
                FROM Unified u
                INNER JOIN `{PROJECT_ID}.{DATASET_ID}.master_metadata` m ON u.node = REPLACE(m.NodeNum, ':', '-')
                """
                client.query(scrub_q).result()
                st.success("Master Table Rebuilt & Standardized!")
            except Exception as e:
                st.error(f"Scrub Error: {e}")
