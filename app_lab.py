import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz
import io

# --- 1. CONFIGURATION & STYLING ---
st.set_page_config(page_title="SoilFreeze Data Lab", layout="wide")

# Set these to your specific BigQuery details
DATASET_ID = "sensor_data" 
PROJECT_ID = "sensorpush-export"

@st.cache_resource
def get_bq_client():
    """Authenticates using Streamlit Secrets to prevent TransportErrors."""
    try:
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(info)
            scoped_creds = credentials.with_scopes([
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/bigquery"
            ])
            return bigquery.Client(credentials=scoped_creds, project=info["project_id"])
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"Authentication Failed: {e}")
        return None

client = get_bq_client()

# --- 2. STANDARDIZED GRAPH ENGINE ---
def build_standard_sf_graph(df, title, start_view, end_view):
    """Handles 6hr gaps, custom gridlines (Mon/Mid/6hr), and Maltby labeling."""
    processed_dfs = []
    for sensor in df['Sensor'].unique():
        s_df = df[df['Sensor'] == sensor].copy().sort_values('timestamp')
        # Logic: Break line if gap > 6 hours
        s_df['gap'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
        gaps = s_df[s_df['gap'] > 6.0].copy()
        if not gaps.empty:
            gaps['value'] = None
            gaps['timestamp'] = gaps['timestamp'] - timedelta(minutes=1)
            s_df = pd.concat([s_df, gaps]).sort_values('timestamp')
        processed_dfs.append(s_df)
    
    clean_df = pd.concat(processed_dfs) if processed_dfs else df
    fig = px.line(clean_df, x='timestamp', y='value', color='Sensor')
    
    # Y-Axis: 80 to -20 F, Dark Gray every 20, Medium every 5
    fig.update_yaxes(
        title="Temperature (°F)", tickmode='array', tickvals=[-20, 0, 20, 40, 60, 80],
        gridcolor='DimGray', gridwidth=1.5, minor=dict(dtick=5, gridcolor='Silver', showgrid=True),
        range=[-20, 80], mirror=True, showline=True, linecolor='black', linewidth=2
    )

    # X-Axis: Remove default grid to draw custom Monday/Midnight/6hr lines
    fig.update_xaxes(showgrid=False, range=[start_view, end_view], 
                     mirror=True, showline=True, linecolor='black', linewidth=2)

    shapes = []
    curr = start_view.replace(hour=0, minute=0, second=0)
    while curr <= end_view:
        for h in [0, 6, 12, 18]:
            check_time = curr + timedelta(hours=h)
            if check_time < start_view or check_time > end_view: continue
            
            # Monday Midnight (Dark), Daily Midnight (Medium), 6-Hour (Light)
            if check_time.weekday() == 0 and h == 0: color, width = "DimGray", 2
            elif h == 0: color, width = "DarkGray", 1
            else: color, width = "LightGray", 0.5
            
            shapes.append(dict(type="line", xref="x", yref="paper", x0=check_time, y0=0, x1=check_time, y1=1,
                               line=dict(color=color, width=width), layer="below"))
        curr += timedelta(days=1)

    fig.update_layout(
        title={'text': title, 'x': 0.5, 'xanchor': 'center'}, shapes=shapes, plot_bgcolor='white',
        legend=dict(title="Depth / Location", x=1.02, y=1, bordercolor="Black", borderwidth=1),
        margin=dict(l=60, r=150, t=80, b=60), height=750
    )
    fig.add_hline(y=32, line_dash="dash", line_color="blue", annotation_text="32°F")
    return fig

# --- 3. UI NAVIGATION ---
service = st.sidebar.selectbox("Select Service", [
    "🏠 Executive Summary", "📈 Node Diagnostics", "📤 Data Intake Lab", "⚙️ Database Maintenance"
])

# --- SERVICE 1: EXECUTIVE SUMMARY ---
if service == "🏠 Executive Summary":
    st.header("🏠 Executive Summary")
    try:
        query = f"SELECT nodenumber, Project, Location, Depth, MAX(timestamp) as last_seen, AVG(value) as current_temp FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` GROUP BY 1, 2, 3, 4"
        df_ex = client.query(query).to_dataframe(create_bqstorage_client=False)
        
        if not df_ex.empty:
            def thermal_style(v):
                if v > 32: return 'background-color: #ff4b4b; color: white' 
                return 'background-color: #ffa500' if 28 <= v <= 32 else 'background-color: #28a745; color: white'
            
            st.dataframe(df_ex.style.map(thermal_style, subset=['current_temp']), width='stretch')
    except Exception as e: st.error(f"Summary Error: {e}")

# --- SERVICE 2: NODE DIAGNOSTICS ---
elif service == "📈 Node Diagnostics":
    st.header("📈 Node Diagnostics")
    meta_df = client.query(f"SELECT DISTINCT Project, Location FROM `{PROJECT_ID}.{DATASET_ID}.master_metadata`").to_dataframe(create_bqstorage_client=False)
    
    c1, c2, c3 = st.columns(3)
    with c1: sel_projs = st.multiselect("Projects", sorted(meta_df['Project'].unique()))
    with c2: 
        avail_locs = meta_df[meta_df['Project'].isin(sel_projs)]['Location'].unique() if sel_projs else []
        sel_locs = st.multiselect("Pipes", sorted(avail_locs))
    with c3: weeks = st.slider("Trend Duration (Weeks)", 1, 12, 6)

    if sel_projs and sel_locs:
        now_utc = datetime.now(pytz.UTC)
        end_view = (now_utc + timedelta(days=(7 - now_utc.weekday()) % 7)).replace(hour=0, minute=0, second=0, microsecond=0)
        start_view = end_view - timedelta(weeks=weeks)
        
        query = f"SELECT timestamp, value, Location, Depth FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE Project IN UNNEST({list(sel_projs)}) AND Location IN UNNEST({list(sel_locs)}) AND timestamp >= '{start_view.strftime('%Y-%m-%d %H:%M:%S')}'"
        df_g = client.query(query).to_dataframe(create_bqstorage_client=False)
        
        if not df_g.empty:
            # Legend Logic: Depth gets 'ft', node locations (S1, R3) stay as-is
            df_g['Sensor'] = df_g.apply(lambda x: f"{x['Depth']}ft" if str(x['Depth']).replace('.','',1).isdigit() else x['Location'], axis=1)
            fig = build_standard_sf_graph(df_g, f"Temperature: {', '.join(sel_locs)}", start_view, end_view)
            st.plotly_chart(fig, width='stretch')

# --- SERVICE 3: DATA INTAKE LAB ---
elif service == "📤 Data Intake Lab":
    st.header("📤 Manual Data Ingestion")
    source = st.radio("Device Type", ["SensorPush (CSV)", "Lord (SensorConnect)"])
    u_file = st.file_uploader("Upload Logger File", type=['csv'])

    if u_file:
        try:
            content = u_file.getvalue().decode("utf-8").splitlines()
            if "Lord" in source:
                start_idx = next((i for i, l in enumerate(content) if "DATA_START" in l), 0)
                u_file.seek(0)
                df_raw = pd.read_csv(u_file, skiprows=start_idx + 1)
                df_up = df_raw.melt(id_vars=[df_raw.columns[0]], var_name='nodenumber', value_name='value')
                df_up = df_up.rename(columns={df_raw.columns[0]: 'timestamp'})
                df_up['nodenumber'] = df_up['nodenumber'].str.replace(':', '-', regex=False)
                table_ref = f"{PROJECT_ID}.{DATASET_ID}.raw_lord"
            else:
                df_up = pd.read_csv(u_file).rename(columns={'Timestamp':'timestamp','Temperature':'value','Sensor':'nodenumber'})
                df_up['nodenumber'] = df_up['nodenumber'].str.replace(':', '-', regex=False)
                table_ref = f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush"
            
            if st.button("🚀 PUSH TO BIGQUERY"):
                client.load_table_from_dataframe(df_up, table_ref).result()
                st.success("Data ingested!")
        except Exception as e: st.error(f"Intake Error: {e}")

# --- SERVICE 4: DATABASE MAINTENANCE ---
elif service == "⚙️ Database Maintenance":
    st.header("⚙️ Database Maintenance")
    if st.button("🔄 EXECUTE MASTER SCRUB"):
        with st.spinner("Rebuilding Master Table..."):
            try:
                scrub_q = f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` AS
                WITH Unified AS (
                    SELECT CAST(timestamp AS TIMESTAMP) as timestamp, value, REPLACE(nodenumber, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
                    UNION ALL
                    SELECT CAST(timestamp AS TIMESTAMP) as timestamp, value, REPLACE(nodenumber, ':', '-') as node FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
                )
                SELECT u.*, m.Project, m.Location, m.Depth FROM Unified u
                INNER JOIN `{PROJECT_ID}.{DATASET_ID}.master_metadata` m ON u.node = REPLACE(m.NodeNum, ':', '-')
                """
                client.query(scrub_q).result()
                st.success("Master Table Rebuilt & Standardized!")
            except Exception as e: st.error(f"Scrub Error: {e}")
