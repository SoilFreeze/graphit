import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz
import io

# --- 1. CONFIGURATION ---
st.set_page_config(page_title="SoilFreeze Data Lab", layout="wide")
DATASET_ID = "sensor_data" 
PROJECT_ID = "sensorpush-export"

@st.cache_resource
def get_bq_client():
    """Authenticates via Streamlit Secrets to prevent TransportErrors."""
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
def build_standard_sf_graph(df, title, start_view, end_view, unit="Fahrenheit", active_refs=None):
    """
    Standardized SF Engine: Handles 6hr gaps, C/F units, and 'Right Now' red line.
    Explicitly uses go.Scatter with fill=None to prevent overlapping blobs.
    """
    if active_refs is None: 
        active_refs = []
    
    # 1. Unit Conversion Logic
    display_df = df.copy()
    if unit == "Celsius":
        display_df['value'] = (display_df['value'] - 32) * 5/9
        y_range, y_ticks, y_label, m_step = [-30, 30], [-30, -20, -10, 0, 10, 20, 30], "Temp (°C)", 2.5
    else:
        y_range, y_ticks, y_label, m_step = [-20, 80], [-20, 0, 20, 40, 60, 80], "Temp (°F)", 5

    # 2. Gap Logic: Insert None if data gap > 6 hours
    processed_dfs = []
    for sensor in display_df['Sensor'].unique():
        s_df = display_df[display_df['Sensor'] == sensor].copy().sort_values('timestamp')
        s_df['gap'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
        gaps = s_df[s_df['gap'] > 6.0].copy()
        if not gaps.empty:
            gaps['value'] = None
            gaps['timestamp'] = gaps['timestamp'] - timedelta(minutes=1)
            s_df = pd.concat([s_df, gaps]).sort_values('timestamp')
        processed_dfs.append(s_df)
    
    clean_df = pd.concat(processed_dfs) if processed_dfs else display_df

    # 3. TRACE CREATION: Explicitly separate lines and disable fills
    fig = go.Figure()
    for sensor in sorted(clean_df['Sensor'].unique()):
        sensor_df = clean_df[clean_df['Sensor'] == sensor]
        fig.add_trace(go.Scatter(
            x=sensor_df['timestamp'], 
            y=sensor_df['value'],
            name=sensor,
            mode='lines',
            fill=None,  # CRITICAL: Prevents the color shading between lines
            connectgaps=False,
            line=dict(width=2)
        ))

    # 4. Grid & Axis Styling
    fig.update_yaxes(title=y_label, tickmode='array', tickvals=y_ticks, range=y_range,
                     gridcolor='DimGray', gridwidth=1.5, minor=dict(dtick=m_step, gridcolor='Silver', showgrid=True),
                     mirror=True, showline=True, linecolor='black', linewidth=2)
    fig.update_xaxes(showgrid=False, range=[start_view, end_view], mirror=True, showline=True, linecolor='black', linewidth=2)

    # 5. Custom Vertical Grid (Mon/Mid/6hr) using numeric timestamps
    shapes = []
    curr = start_view.replace(hour=0, minute=0, second=0)
    while curr <= end_view:
        for h in [0, 6, 12, 18]:
            t = curr + timedelta(hours=h)
            if t < start_view or t > end_view: continue
            t_ms = t.timestamp() * 1000
            c, w = ("DimGray", 2) if (t.weekday() == 0 and h == 0) else (("DarkGray", 1) if h == 0 else ("LightGray", 0.5))
            shapes.append(dict(type="line", xref="x", yref="paper", x0=t_ms, y0=0, x1=t_ms, y1=1, line=dict(color=c, width=w), layer="below"))
        curr += timedelta(days=1)

    # 6. 'Right Now' Red Line
    now_ms = datetime.now(pytz.UTC).timestamp() * 1000
    fig.add_vline(x=now_ms, line_width=2, line_color="red", annotation_text="NOW")
    
    for ref_f, label in active_refs:
        val = (ref_f - 32) * 5/9 if unit == "Celsius" else ref_f
        fig.add_hline(y=val, line_dash="dash", line_color="blue", annotation_text=f"{label} {round(val,1)}°")

    fig.update_layout(title={'text': title, 'x': 0.5}, shapes=shapes, plot_bgcolor='white',
                      legend=dict(x=1.02, y=1, bordercolor="Black", borderwidth=1), margin=dict(r=150), height=750)
    return fig
    
# --- 3. SERVICE ROUTING ---
service = st.sidebar.selectbox("Select Service", ["🏠 Executive Summary", "📈 Node Diagnostics", "📤 Data Intake Lab", "⚙️ Database Maintenance"])

if service == "🏠 Executive Summary":
    st.header("🏠 Executive Summary")
    try:
        q = f"SELECT nodenumber, Project, Location, Depth, MAX(timestamp) as last_seen, AVG(value) as current_temp FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` GROUP BY 1, 2, 3, 4"
        df_ex = client.query(q).to_dataframe(create_bqstorage_client=False)
        if not df_ex.empty:
            def thermal_style(v):
                if v > 32: return 'background-color: #ff4b4b; color: white' 
                return 'background-color: #ffa500' if 28 <= v <= 32 else 'background-color: #28a745; color: white'
            st.dataframe(df_ex.style.map(thermal_style, subset=['current_temp']), width='stretch')
    except Exception as e: 
        st.error(f"Summary Error: {e}")

elif service == "📈 Node Diagnostics":
    st.header("📈 Node Diagnostics")
    
    # Sidebar Options
    temp_unit = st.sidebar.radio("Unit", ["Fahrenheit", "Celsius"])
    ref_list = []
    if st.sidebar.checkbox("32°F (Frost)"): ref_list.append((32.0, "Frost"))
    if st.sidebar.checkbox("26.6°F (Brine)"): ref_list.append((26.6, "Brine"))
    if st.sidebar.checkbox("10.2°F (Deep)"): ref_list.append((10.2, "Deep"))

    # Metadata filters with Null-Safe Sorting
    meta_df = client.query(f"SELECT DISTINCT Project, Location FROM `{PROJECT_ID}.{DATASET_ID}.master_metadata`").to_dataframe(create_bqstorage_client=False)
    c1, c2, c3 = st.columns(3)
    with c1: 
        projs = sorted([p for p in meta_df['Project'].unique() if p is not None])
        sel_projs = st.multiselect("Projects", projs)
    with c2: 
        raw_locs = meta_df[meta_df['Project'].isin(sel_projs)]['Location'].unique() if sel_projs else []
        locs = sorted([l for l in raw_locs if l is not None])
        sel_locs = st.multiselect("Pipes", locs)
    with c3: 
        weeks = st.slider("Duration (Weeks)", 1, 12, 6)

    if sel_projs and sel_locs:
        now_utc = datetime.now(pytz.UTC)
        end_view = (now_utc + timedelta(days=(7 - now_utc.weekday()) % 7)).replace(hour=0, minute=0, second=0, microsecond=0)
        start_view = end_view - timedelta(weeks=weeks)
        
        q = f"SELECT timestamp, value, Location, Depth FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` WHERE Project IN UNNEST({list(sel_projs)}) AND Location IN UNNEST({list(sel_locs)}) AND timestamp >= '{start_view.strftime('%Y-%m-%d %H:%M:%S')}'"
        df_g = client.query(q).to_dataframe(create_bqstorage_client=False)
        
        if not df_g.empty:
            # Legend Logic: Depth gets 'ft', node locations (S1, R3) stay as-is
            df_g['Sensor'] = df_g.apply(lambda x: f"{x['Depth']}ft" if str(x['Depth']).replace('.','',1).isdigit() else x['Location'], axis=1)
            fig = build_standard_sf_graph(df_g, f"Trends: {', '.join(sel_locs)}", start_view, end_view, unit=temp_unit, active_refs=ref_list)
            st.plotly_chart(fig, width='stretch')

elif service == "📤 Data Intake Lab":
    st.header("📤 Manual Data Ingestion")
    # ... (Lord Parser Logic goes here, ensure same indentation level as above) ...
    pass

elif service == "⚙️ Database Maintenance":
    st.header("⚙️ Database Maintenance")
    # ... (Master Scrub Logic goes here, ensure same indentation level as above) ...
    pass
                                  
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
    
    # Sidebar Controls for Customization
    temp_unit = st.sidebar.radio("Temperature Unit", ["Fahrenheit", "Celsius"])
    ref_list = []
    if st.sidebar.checkbox("32°F (Frost)"): ref_list.append((32.0, "Frost"))
    if st.sidebar.checkbox("26.6°F (Brine)"): ref_list.append((26.6, "Brine"))
    if st.sidebar.checkbox("10.2°F (Target Deep)"): ref_list.append((10.2, "Deep"))

    # 1. METADATA FILTERS (Single Selection swapped from Multiselect)
    meta_df = client.query(f"SELECT DISTINCT Project, Location FROM `{PROJECT_ID}.{DATASET_ID}.master_metadata`").to_dataframe(create_bqstorage_client=False)
    
    c1, c2, c3 = st.columns(3)
    with c1: 
        # Default project to 'Office'
        all_projs = sorted([p for p in meta_df['Project'].unique() if p is not None])
        default_idx = all_projs.index("Office") if "Office" in all_projs else 0
        sel_proj = st.selectbox("1. Select Project", all_projs, index=default_idx)
        
    with c2: 
        # Filter pipes based on the single selected project
        raw_locs = meta_df[meta_df['Project'] == sel_proj]['Location'].unique()
        avail_locs = sorted([l for l in raw_locs if l is not None])
        sel_loc = st.selectbox("2. Select Pipe / Bank", avail_locs)
        
    with c3: 
        weeks = st.slider("3. Trend Duration (Weeks)", 1, 12, 6)

    # 2. DATA FETCHING & UNIQUE LABELING
    if sel_proj and sel_loc:
        now_utc = datetime.now(pytz.UTC)
        end_view = (now_utc + timedelta(days=(7 - now_utc.weekday()) % 7)).replace(hour=0, minute=0, second=0, microsecond=0)
        start_view = end_view - timedelta(weeks=weeks)
        
        # Pull nodenumber to ensure each physical sensor in a Bank is separated
        q = f"""
            SELECT timestamp, value, Location, Depth, nodenumber 
            FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` 
            WHERE Project = '{sel_proj}' 
            AND Location = '{sel_loc}' 
            AND timestamp >= '{start_view.strftime('%Y-%m-%d %H:%M:%S')}'
        """
        df_g = client.query(q).to_dataframe(create_bqstorage_client=False)
        
        if not df_g.empty:
            # UNIQUE LABELER: Ensures S1, R1, etc. are treated as separate traces
            def format_sensor_label(row):
                depth_str = str(row['Depth'])
                # If numeric (15), add ft. If location name (S1), use as-is.
                base_label = f"{depth_str}ft" if depth_str.replace('.','',1).isdigit() else depth_str
                return f"{base_label} ({row['nodenumber']})"

            df_g['Sensor'] = df_g.apply(format_sensor_label, axis=1)
            
            # Sort to keep the lines and legend sequential
            df_g = df_g.sort_values(by=['Sensor', 'timestamp'])
            
            title = f"Temperature Trend: {sel_proj} - {sel_loc} ({weeks} Weeks)"
            fig = build_standard_sf_graph(df_g, title, start_view, end_view, unit=temp_unit, active_refs=ref_list)
            st.plotly_chart(fig, width='stretch')
        else:
            st.info(f"No data found for {sel_loc} in the requested timeframe.")

    # 3. Data Processing and Graphing
    if sel_proj and sel_loc:
        now_utc = datetime.now(pytz.UTC)
        end_view = (now_utc + timedelta(days=(7 - now_utc.weekday()) % 7)).replace(hour=0, minute=0, second=0, microsecond=0)
        start_view = end_view - timedelta(weeks=weeks)
        
        # Query for the single selected pipe
        q = f"""
            SELECT timestamp, value, Location, Depth, nodenumber 
            FROM `{PROJECT_ID}.{DATASET_ID}.final_databoard_master` 
            WHERE Project = '{sel_proj}' 
            AND Location = '{sel_loc}' 
            AND timestamp >= '{start_view.strftime('%Y-%m-%d %H:%M:%S')}'
        """
        df_g = client.query(q).to_dataframe(create_bqstorage_client=False)
        
        if not df_g.empty:
            # THE FIX: Ensure S1, R1, etc. are separated by combining Depth/Location with Node ID
            # This forces Plotly to see them as different traces even if Location is the same
            def label_sensor(row):
                depth_val = str(row['Depth'])
                # If Depth is numeric (e.g. 15), add "ft". If it's "S1", just use the name.
                label = f"{depth_val}ft" if depth_val.replace('.','',1).isdigit() else depth_val
                return f"{label} ({row['nodenumber']})"

            df_g['Sensor'] = df_g.apply(label_sensor, axis=1)
            
            # Sort by Sensor and Time for clean plotting
            df_g = df_g.sort_values(by=['Sensor', 'timestamp'])
            
            title = f"Temperature Trend: {sel_proj} - {sel_loc} ({weeks} Weeks)"
            fig = build_standard_sf_graph(df_g, title, start_view, end_view, unit=temp_unit, active_refs=ref_list)
            st.plotly_chart(fig, width='stretch')
        else:
            st.info(f"No data found for {sel_loc} in this timeframe.")


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
