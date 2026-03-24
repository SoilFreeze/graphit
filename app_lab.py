import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz

# --- 1. CONFIGURATION ---
st.set_page_config(page_title="SoilFreeze Data Lab", layout="wide")

DATASET_ID = "sensor_data" 
PROJECT_ID = "sensorpush-export"

@st.cache_resource
def get_bq_client():
    """Authenticates using Streamlit Secrets."""
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
    """Handles 6hr gaps, custom gridlines, and Maltby labeling."""
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
    fig = px.line(clean_df, x='timestamp', y='value', color='Sensor')
    
    # Y-Axis Styling (80 to -20 F)
    fig.update_yaxes(
        title="Temperature (°F)", tickmode='array', tickvals=[-20, 0, 20, 40, 60, 80],
        gridcolor='DimGray', gridwidth=1.5, minor=dict(dtick=5, gridcolor='Silver', showgrid=True),
        range=[-20, 80], mirror=True, showline=True, linecolor='black', linewidth=2
    )

    # X-Axis Gridline Logic
    fig.update_xaxes(showgrid=False, range=[start_view, end_view], mirror=True, showline=True, linecolor='black', linewidth=2)
    shapes = []
    curr = start_view.replace(hour=0, minute=0, second=0)
    while curr <= end_view:
        for h in [0, 6, 12, 18]:
            check_time = curr + timedelta(hours=h)
            if check_time < start_view or check_time > end_view: continue
            color, width = ("DimGray", 2) if (check_time.weekday() == 0 and h == 0) else (("DarkGray", 1) if h == 0 else ("LightGray", 0.5))
            shapes.append(dict(type="line", xref="x", yref="paper", x0=check_time, y0=0, x1=check_time, y1=1, line=dict(color=color, width=width), layer="below"))
        curr += timedelta(days=1)

    fig.update_layout(
        title={'text': title, 'x': 0.5, 'xanchor': 'center'}, shapes=shapes, plot_bgcolor='white',
        legend=dict(title="Depth / Location", x=1.02, y=1, bordercolor="Black", borderwidth=1),
        margin=dict(l=60, r=150, t=80, b=60), height=750
    )
    # Add 32F Blue Reference Line
    fig.add_hline(y=32, line_dash="dash", line_color="blue", annotation_text="32°F")
    return fig

# --- 3. UI ROUTING ---
st.sidebar.title("❄️ SoilFreeze Lab")
service = st.sidebar.selectbox("Select Service", ["🏠 Executive Summary", "📈 Node Diagnostics", "📤 Data Intake Lab", "⚙️ Database Maintenance"])

if service == "🏠 Executive Summary":
    st.header("🏠 Executive Summary")
    try:
        df_ex = client.query(f"SELECT nodenumber, Project, Location, Depth, MAX(timestamp) as last_seen, AVG(value) as current_temp FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` GROUP BY 1, 2, 3, 4").to_dataframe()
        if not df_ex.empty:
            def thermal_style(v):
                if v > 32: return 'background-color: #ff4b4b; color: white' 
                return 'background-color: #ffa500' if 28 <= v <= 32 else 'background-color: #28a745; color: white'
            # Updated .map() and width='stretch'
            st.dataframe(df_ex.style.map(thermal_style, subset=['current_temp']), width='stretch')
    except Exception as e: st.error(f"Summary Error: {e}")

elif service == "📈 Node Diagnostics":
    st.header("📈 Node Diagnostics")
    meta_df = client.query(f"SELECT DISTINCT Project, Location FROM `{PROJECT_ID}.{DATASET_ID}.master_metadata`").to_dataframe()
    c1, c2, c3 = st.columns(3)
    with c1: sel_projs = st.multiselect("Projects", sorted(meta_df['Project'].unique()))
    with c2: 
        avail_locs = meta_df[meta_df['Project'].isin(sel_projs)]['Location'].unique() if sel_projs else []
        sel_locs = st.multiselect("Pipes", sorted(avail_locs))
    with c3: weeks = st.slider("Weeks", 1, 12, 6)

    if sel_projs and sel_locs:
        # Define time boundaries
        now_utc = datetime.now(pytz.UTC)
        end_view = (now_utc + timedelta(days=(7 - now_utc.weekday()) % 7)).replace(hour=0, minute=0, second=0, microsecond=0)
        start_view = end_view - timedelta(weeks=weeks)
        
        query = f"SELECT timestamp, value, Location, Depth FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE Project IN UNNEST({list(sel_projs)}) AND Location IN UNNEST({list(sel_locs)}) AND timestamp >= '{start_view.strftime('%Y-%m-%d %H:%M:%S')}'"
        df_g = client.query(query).to_dataframe()
        
        if not df_g.empty:
            df_g['Sensor'] = df_g.apply(lambda x: f"{x['Depth']}ft" if str(x['Depth']).replace('.','',1).isdigit() else x['Location'], axis=1)
            fig = build_standard_sf_graph(df_g, f"Temperature: {', '.join(sel_locs)}", start_view, end_view)
            st.plotly_chart(fig, width='stretch')

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
        # Added create_bqstorage_client=False to bypass the 403 Permission error
        df_ex = client.query(query).to_dataframe(create_bqstorage_client=False)
        
        if df_ex.empty:
            st.warning("Master Table is empty. Run 'Database Maintenance' first.")
        else:
            avg_t = df_ex['current_temp'].mean()
            c1, c2 = st.columns(2)
            c1.metric("Project Avg Temp", f"{avg_t:.1f}°F")
            c2.metric("Active Sensors", len(df_ex))

            # Maltby Engineering Requirements: Red (>32), Orange (28-32), Green (<28)
            def thermal_style(v):
                if v > 32: return 'background-color: #ff4b4b; color: white' 
                if 28 <= v <= 32: return 'background-color: #ffa500'        
                return 'background-color: #28a745; color: white'           
            
            # Using .map() for Pandas 2.x+ and width='stretch' for Streamlit 2026 standards
            st.dataframe(df_ex.style.map(thermal_style, subset=['current_temp']), width='stretch')
    except Exception as e:
        st.error(f"Summary Error: {e}")

# --- SERVICE 2: NODE DIAGNOSTICS ---
elif service == "📈 Node Diagnostics":
    # ... (Filter logic here) ...
    if sel_projs and sel_locs:
        # (Time boundary logic here) ...
        
        # Apply the same 403 fix to the Diagnostic query
        df_g = client.query(graph_q).to_dataframe(create_bqstorage_client=False)
        
        if not df_g.empty:
            # (Graphing logic here) ...
            st.plotly_chart(fig, width='stretch')
