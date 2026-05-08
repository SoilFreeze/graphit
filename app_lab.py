import streamlit as st
import pandas as pd
import time
import plotly.express as px
import plotly.graph_objects as go  # This defines 'go'
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta, timezone, time as dt_time
import pytz
import traceback
import io
import re
from streamlit_plotly_events import plotly_events


##################################
# - 1. CONFIGURATION & STYLING - #
##################################
st.set_page_config(page_title="SoilFreeze Data Lab", layout="wide")

# Database Constants
DATASET_ID = "Temperature" 
PROJECT_ID = "sensorpush-export"
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"

# MASTER VISIBILITY SWITCHES
PROJECT_VISIBILITY_MASKS = {
    "Office": "2026-03-03 15:00:00", 
    "Main_Site": "2026-01-01 00:00:00",
    "2527": "2026-01-01 00:00:00"
}

@st.cache_resource
def get_bq_client():
    try:
        SCOPES = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive"]
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
            return bigquery.Client(credentials=credentials, project=info["project_id"])
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"Authentication Failed: {e}")
        return None

client = get_bq_client()

############################
# - 2. DATA ENGINE LOGIC - #
############################

@st.cache_data(ttl=600)
def get_universal_portal_data(project_id, view_mode="engineering"):
    """
    Unified Data Engine: Joins Raw Data + Project Registry + Manual Rejections.
    """
    # 1. Get visibility cutoff from your masks
    cutoff = PROJECT_VISIBILITY_MASKS.get(project_id, "2000-01-01 00:00:00")
    
    # 2. DEFINE THE FILTER (MUST happen before the query string is built)
    if view_mode == "client":
        # Client sees only Approved (TRUE) data after the mask cutoff
        query_filter = f"AND rej.approve = 'TRUE' AND r.timestamp >= '{cutoff}'"
    else:
        # Engineering sees everything except explicit deletions (FALSE)
        query_filter = "AND (rej.approve IS NULL OR rej.approve != 'FALSE')"

    # 3. CONSTRUCT THE REGISTRY-CENTRIC QUERY
    query = f"""
        SELECT 
            reg.Location, 
            r.timestamp, 
            r.temperature,
            reg.NodeNum,
            reg.Bank,
            reg.Depth,
            reg.Project
        FROM (
            SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
            UNION ALL
            SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
        ) AS r
        -- JOIN TO REGISTRY INSTEAD OF OLD METADATA
        INNER JOIN `{PROJECT_ID}.{DATASET_ID}.project_registry` AS reg 
            ON r.NodeNum = reg.NodeNum
        LEFT JOIN `{OVERRIDE_TABLE}` AS rej 
            ON r.NodeNum = rej.NodeNum 
            AND TIMESTAMP_TRUNC(r.timestamp, HOUR) = rej.timestamp
        WHERE reg.Project = '{project_id}'
        {query_filter}
        -- Match data to the specific window the sensor was at this location
        AND r.timestamp >= reg.StartDate 
        AND (r.timestamp <= reg.EndDate OR reg.EndDate IS NULL)
        ORDER BY reg.Location ASC, r.timestamp ASC
    """
    
    try:
        df = client.query(query).to_dataframe()
        
        if not df.empty:
            # Force numeric types to prevent "str vs float" errors in math/graphing
            df['Depth'] = pd.to_numeric(df['Depth'], errors='coerce')
            df['temperature'] = pd.to_numeric(df['temperature'], errors='coerce')
            
            # Ensure timestamp is UTC-aware for the graphing engine
            if df['timestamp'].dt.tz is None:
                df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
                
        return df
    except Exception as e:
        st.error(f"Registry Engine Error: {e}")
        return pd.DataFrame()
        
###########################
#- 3. SIDEBAR UI & STATE -#
###########################
st.sidebar.title("❄️ SoilFreeze Lab")

# --- 1. INITIALIZE FALLBACKS ---
service = "🏠 Executive Summary"
unit_mode = "Fahrenheit"
unit_label = "°F"
selected_project = "All Projects"
active_refs = [(32.0, "Freezing")]

# --- 2. TIMEZONE DEFAULT LOGIC ---
tz_lookup = {
    "UTC": "UTC", 
    "Local (US/Eastern)": "US/Eastern", 
    "Local (US/Pacific)": "US/Pacific"
}

# Force Pacific as the initial session state if nothing exists
if "tz_selection" not in st.session_state:
    st.session_state["tz_selection"] = "Local (US/Pacific)"

# --- 3. SIDEBAR WIDGETS ---
page = st.sidebar.selectbox("Navigate To:", [
    "Executive Summary", 
    "Global Overview", 
    "Depth Charts",        # New Page
    "Node Diagnostics",     # Updated to 15-min table
    "Client Portal", 
    "Data Intake Lab", 
    "Admin Tools"
])

st.sidebar.divider()

unit_mode = st.sidebar.radio("Unit", ["Fahrenheit", "Celsius"])
unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"

# 1. Define the widget first so 'tz_mode' exists
tz_mode = st.sidebar.selectbox(
    "Timezone Display", 
    list(tz_lookup.keys()), 
    index=list(tz_lookup.keys()).index(st.session_state["tz_selection"])
)

# 2. Now you can use 'tz_mode' safely
st.session_state["tz_selection"] = tz_mode
display_tz = tz_lookup[tz_mode]

# Global Project Selection
if client is not None:
    try:
        proj_q = f"SELECT DISTINCT TRIM(Project) as Project FROM `{PROJECT_ID}.{DATASET_ID}.metadata` WHERE Project IS NOT NULL"
        proj_df = client.query(proj_q).to_dataframe()
        proj_list = sorted(proj_df['Project'].dropna().unique())
        options = ["All Projects"] + proj_list
        selected_project = st.sidebar.selectbox("🎯 Active Project", options, index=0, key="sidebar_proj_picker_global")
    except Exception as e:
        st.sidebar.error("Database connection lag. Defaulting to 'All Projects'.")
        selected_project = "All Projects"

# Reference Lines
st.sidebar.subheader("📏 Reference Lines")
active_refs = [] 
if st.sidebar.checkbox("Freezing (32°F)", value=True): 
    active_refs.append((32.0, "Freezing"))
if st.sidebar.checkbox("Type B (26.6°F)", value=False): 
    active_refs.append((26.6, "Type B"))
if st.sidebar.checkbox("Type A (10.2°F)", value=False): 
    active_refs.append((10.2, "Type A"))
########################
#- 4. GRAPHING ENGINE -#
########################

def build_high_speed_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label, display_tz="UTC"):
    """
    Stabilized Engine: Optimized for Mobile with bottom-horizontal legends 
    and a 1-day X-axis buffer.
    """
    if df.empty:
        return go.Figure().update_layout(title="No data available for the selected period.")

    plot_df = df.copy()
    
    # 1. TIMEZONE & UNIT CONVERSION
    if plot_df['timestamp'].dt.tz is None:
        plot_df['timestamp'] = plot_df['timestamp'].dt.tz_localize('UTC')
    
    plot_df['timestamp'] = plot_df['timestamp'].dt.tz_convert(display_tz)
    
    # Timezone-aware endpoints
    start_local = start_view.tz_convert(display_tz) if start_view.tzinfo else start_view.tz_localize('UTC').tz_convert(display_tz)
    end_local = end_view.tz_convert(display_tz) if end_view.tzinfo else end_view.tz_localize('UTC').tz_convert(display_tz)
    now_local = pd.Timestamp.now(tz=display_tz)
    
    # Apply 1-day buffer for visual spacing
    range_start = start_local - pd.Timedelta(days=1)
    range_end = end_local + pd.Timedelta(days=1)
    
    if unit_mode == "Celsius":
        plot_df['temperature'] = (plot_df['temperature'] - 32) * 5/9
        y_range, dt_major, dt_minor = [-30, 30], 10, 5
    else:
        y_range, dt_major, dt_minor = [-20, 80], 10, 5

    # 2. LABELING & SORTING
    def get_sort_info(r):
        node_id = r.get('NodeNum') or r.get('node_id') or "Unknown"
        if pd.notnull(r.get('Depth')):
            return f"{r['Depth']}ft", float(r['Depth'])
        if pd.notnull(r.get('Bank')):
            return f"Bank {r['Bank']}", 999.0
        return f"Node {node_id}", 1000.0

    plot_df[['depth_label', 'sort_val']] = plot_df.apply(lambda x: pd.Series(get_sort_info(x)), axis=1)
    
    # 3. TRACE GENERATION
    fig = go.Figure()
    is_surgical = any(word in title for word in ["Scrubbing", "Surgical", "Diag"])
    unique_groups = plot_df[['depth_label', 'sort_val']].drop_duplicates().sort_values('sort_val')
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']

    for i, (_, g_row) in enumerate(unique_groups.iterrows()):
        group_lbl = g_row['depth_label']
        group_data = plot_df[plot_df['depth_label'] == group_lbl]
        color = colors[i % len(colors)]
        sensors = group_data['NodeNum'].unique()
        
        for j, sn in enumerate(sensors):
            s_df = group_data[group_data['NodeNum'] == sn].sort_values('timestamp')
            
            if not is_surgical:
                # Gap handling: break lines if > 6 hours
                s_df['gap_hrs'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
                gap_mask = s_df['gap_hrs'] > 6.0
                if gap_mask.any():
                    gaps = s_df[gap_mask].copy()
                    gaps['temperature'] = None
                    gaps['timestamp'] = gaps['timestamp'] - pd.Timedelta(minutes=1)
                    s_df = pd.concat([s_df, gaps]).sort_values('timestamp')

            fig.add_trace(go.Scatter(
                x=s_df['timestamp'], 
                y=s_df['temperature'], 
                name=f"{group_lbl} ({sn})", 
                legendgroup=group_lbl, 
                showlegend=True if j == 0 else False,
                mode='lines+markers' if not is_surgical else 'markers',
                connectgaps=False,
                line=dict(color=color, width=1.5),
                marker=dict(size=4, opacity=0.8),
                hovertemplate=f"<b>{group_lbl} ({sn})</b>: %{{y:.1f}}{unit_label}<extra></extra>"
            ))

    # 4. REFERENCE LINES & NOW MARKER
    for val, ref_label in active_refs:
        c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
        fig.add_hline(y=c_val, line_dash="dash", line_color="RoyalBlue", 
                      annotation_text=ref_label, annotation_position="top right")

    fig.add_vline(x=now_local, line_width=2, line_color="Red", layer='above', line_dash="dash")

    # 5. MOBILE-OPTIMIZED LAYOUT
    fig.update_layout(
        title={'text': f"<b>{title}</b>", 'x': 0},
        plot_bgcolor='white', 
        hovermode="x unified", 
        height=650, # Slightly taller to accommodate bottom legend
        margin=dict(t=80, l=50, r=20, b=120), # Large bottom margin for legend
        xaxis=dict(
            range=[range_start, range_end], # 1-day buffer applied here
            showline=True, mirror=True, linecolor='black',
            showgrid=True, dtick="D1", gridcolor='DarkGray', gridwidth=1,
            minor=dict(dtick=6*60*60*1000, showgrid=True, gridcolor='Gainsboro', griddash='dash'),
            tickformat='%b %d\n%H:%M'
        ),
        yaxis=dict(
            title=f"Temperature ({unit_label})", range=y_range, dtick=dt_major, 
            gridcolor='DarkGray', showline=True, mirror=True, linecolor='black',
            minor=dict(dtick=dt_minor, showgrid=True, gridcolor='whitesmoke')
        ),
        # LEGEND: Bottom-horizontal for Mobile
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.2, 
            xanchor="center",
            x=0.5,
            title="Sensors"
        )
    )
    
    # Add vertical Monday lines
    mondays = pd.date_range(start=range_start, end=range_end, freq='W-MON', tz=display_tz)
    for mon in mondays:
        fig.add_vline(x=mon, line_width=2, line_color="dimgray", layer="below")

    return fig
##################
# Page Functions #
##################

###########
# - 5. PAGE: GLOBAL OVERVIEW - #
###########

def render_global_overview(selected_project, display_tz):
    """
    Shows all pipes/banks for a selected project in one scrolling view.
    Engineering view: shows everything except 'FALSE'.
    """
    st.header("🌐 Global Project Overview")
    
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to begin.")
        return

    with st.spinner(f"Syncing {selected_project} (Engineering View)..."):
        # Fetch data using the updated engine
        p_df = get_universal_portal_data(selected_project, view_mode="engineering")

    if not p_df.empty:
        # 1. View Constraints
        lookback = st.sidebar.slider("Lookback (Weeks)", 1, 12, 4, key="global_lookback_slider")
        
        # Snap time window to the current Pacific (or selected) time
        now_local = pd.Timestamp.now(tz=display_tz)
        end_view = (now_local + pd.Timedelta(days=(7-now_local.weekday())%7 or 7)).replace(hour=0, minute=0, second=0, microsecond=0)
        start_view = end_view - timedelta(weeks=lookback)

        # 2. Render a graph for every physical location (Pipe/Bank) in the project [cite: 6]
        locations = sorted(p_df['Location'].dropna().unique())
        
        for loc in locations:
            with st.expander(f"📍 Location: {loc}", expanded=True):
                loc_df = p_df[p_df['Location'] == loc]
                fig = build_high_speed_graph(
                    df=loc_df, 
                    title=f"📈 {selected_project} - {loc}", 
                    start_view=start_view, 
                    end_view=end_view, 
                    active_refs=tuple(active_refs), 
                    unit_mode=unit_mode, 
                    unit_label=unit_label, 
                    display_tz=display_tz
                )
                st.plotly_chart(fig, use_container_width=True, key=f"ov_{selected_project}_{loc}")
    else:
        st.warning(f"No engineering data found for '{selected_project}' in the last 84 days.")
        st.info("Check if sensors are mapped to this project name in the metadata table[cite: 5].")

###########
# - 6. PAGE: EXECUTIVE SUMMARY - #
###########

def render_executive_summary(client, selected_project, unit_label, display_tz):
    st.header(f"🏠 Executive Summary: Health Monitor")
    
    proj_filter = ""
    if selected_project and selected_project != "All Projects":
        proj_filter = f"AND TRIM(Project) = '{selected_project.strip()}'"

    # COMPREHENSIVE QUERY: Health Metrics + Temperature Extremes + Registry Integration
    query = f"""
        WITH MappedNodes AS (
            SELECT Project, NodeNum, Location, Bank, Depth, StartDate, EndDate
            FROM `{PROJECT_ID}.{DATASET_ID}.project_registry`
            WHERE ProjectStatus = 'Active' {proj_filter}
        ),
        BaseReporting AS (
            SELECT r.NodeNum, r.timestamp, r.temperature, m.StartDate
            FROM (
                SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
                UNION ALL
                SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
            ) AS r
            INNER JOIN MappedNodes m ON r.NodeNum = m.NodeNum
            WHERE r.timestamp >= m.StartDate
            AND (r.timestamp <= m.EndDate OR m.EndDate IS NULL)
        ),
        GapAnalysis AS (
            SELECT 
                NodeNum, timestamp, temperature,
                LAG(timestamp) OVER (PARTITION BY NodeNum ORDER BY timestamp) as prev_ts
            FROM BaseReporting
        ),
        HistoricalStats AS (
            SELECT 
                NodeNum, 
                MAX(timestamp) as last_ping,
                ARRAY_AGG(temperature ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] as current_temp,
                MIN(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) 
                    THEN temperature ELSE NULL END) as low_24h,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) 
                    THEN temperature ELSE NULL END) as high_24h,
                MAX(TIMESTAMP_DIFF(timestamp, prev_ts, HOUR)) as gap_7d,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) 
                    THEN TIMESTAMP_DIFF(timestamp, prev_ts, HOUR) ELSE 0 END) as gap_24h,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN 1 ELSE 0 END) as active_24h,
                COUNT(DISTINCT CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) 
                    THEN TIMESTAMP_TRUNC(timestamp, HOUR) END) as hours_24h,
                COUNT(DISTINCT CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) 
                    THEN TIMESTAMP_TRUNC(timestamp, HOUR) END) as hours_7d
            FROM GapAnalysis GROUP BY NodeNum
        )
        SELECT m.*, h.last_ping, h.current_temp, h.low_24h, h.high_24h,
               COALESCE(h.gap_24h, 0) as gap_24h, COALESCE(h.gap_7d, 0) as gap_7d,
               COALESCE(h.active_24h, 0) as active_24h,
               COALESCE(h.hours_24h, 0) as hours_24h, COALESCE(h.hours_7d, 0) as hours_7d
        FROM MappedNodes m
        LEFT JOIN HistoricalStats h ON m.NodeNum = h.NodeNum
    """
    
    try:
        raw_df = client.query(query).to_dataframe()
        if raw_df.empty:
            st.warning("No data found for this project in the registry.")
            return

        # --- CRITICAL: FORCE NUMERIC TYPES ---
        # This prevents the '<' not supported between str and float error
        raw_df['Depth'] = pd.to_numeric(raw_df['Depth'], errors='coerce')
        raw_df['low_24h'] = pd.to_numeric(raw_df['low_24h'], errors='coerce')
        raw_df['high_24h'] = pd.to_numeric(raw_df['high_24h'], errors='coerce')
        raw_df['current_temp'] = pd.to_numeric(raw_df['current_temp'], errors='coerce')

        now_local = pd.Timestamp.now(tz=display_tz)

        # Helper for unit-aware temperature formatting
        def fmt_temp(val):
            if pd.isnull(val): return "N/A"
            c_val = (val - 32) * 5/9 if unit_label == "°C" else val
            return f"{round(c_val, 1)}{unit_label}"

        # 1. MAIN SUMMARY TABLE (Location Level)
        summary_df = raw_df.groupby(['Project', 'Location']).agg(
            Nodes=('NodeNum', 'count'),
            Seen_24h=('active_24h', 'sum'),
            Sum_Hrs_24=('hours_24h', 'sum'),
            Sum_Hrs_7d=('hours_7d', 'sum'),
            Gap_24h=('gap_24h', 'max'),
            Min_24h_All=('low_24h', 'min'), 
            Max_24h_All=('high_24h', 'max'), 
            Latest_Ping=('last_ping', 'max')
        ).reset_index()

        # Add a "Project Total" row for the top of the table
        total_df = summary_df.groupby('Project').agg({
            'Nodes': 'sum', 'Seen_24h': 'sum', 'Sum_Hrs_24': 'sum', 'Sum_Hrs_7d': 'sum',
            'Gap_24h': 'max', 'Min_24h_All': 'min', 'Max_24h_All': 'max', 'Latest_Ping': 'max'
        }).reset_index()
        total_df['Location'] = 'PROJECT TOTAL'

        final_df = pd.concat([total_df, summary_df], ignore_index=True)
        final_df['is_total'] = (final_df['Location'] == 'PROJECT TOTAL').astype(int)
        final_df = final_df.sort_values(by=['Project', 'is_total', 'Location'], ascending=[True, False, True])

        def format_summary_table(row):
            latest = row['Latest_Ping']
            last_seen_str = "Never"
            if pd.notnull(latest):
                if latest.tzinfo is None: latest = latest.tz_localize('UTC')
                lag_hrs = (now_local - latest.tz_convert(display_tz)).total_seconds() / 3600
                last_seen_str = f"{round(lag_hrs, 1)}h ago"

            avg_24h = (row['Sum_Hrs_24'] / (row['Nodes'] * 24)) * 100
            avg_7d = (row['Sum_Hrs_7d'] / (row['Nodes'] * 168)) * 100

            return pd.Series({
                "Project": row['Project'], "Location": row['Location'], 
                "Min (24h)": fmt_temp(row['Min_24h_All']), "Max (24h)": fmt_temp(row['Max_24h_All']),
                "Nodes": int(row['Nodes']), "Seen (24h)": int(row['Seen_24h']),
                "% Active (24h)": f"{round(avg_24h, 1)}%", "% Active (7d)": f"{round(avg_7d, 1)}%",
                "Last Seen": last_seen_str, "Max Gap": f"{int(row['Gap_24h'])}h"
            })

        st.subheader("📍 Location Overview")
        st.dataframe(final_df.apply(format_summary_table, axis=1).style.apply(
            lambda x: ['background-color: #f0f2f6; font-weight: bold'] * len(x) if x['Location'] == 'PROJECT TOTAL' else [''] * len(x), axis=1
        ), use_container_width=True, hide_index=True)

        # 2. SENSOR DRILL-DOWN
        st.divider()
        st.subheader("🔍 Sensor Drill-Down")
        loc_list = sorted(raw_df['Location'].unique().tolist())
        selected_loc = st.selectbox("Detailed view for:", ["--- Select Location ---"] + loc_list)

        if selected_loc != "--- Select Location ---":
            sensor_df = raw_df[raw_df['Location'] == selected_loc].copy()
            def format_sensor_row(row):
                ping = row['last_ping']
                status_str = "Never Seen"
                if pd.notnull(ping):
                    if ping.tzinfo is None: ping = ping.tz_localize('UTC')
                    lag = round((now_local - ping.tz_convert(display_tz)).total_seconds() / 3600, 1)
                    status_str = f"{lag}h {'🔴' if lag > 24 else ('🟡' if lag > 6 else '🟢')}"
                
                # Handling NULL Bank/Depth for display
                pos = f"{row['Depth']}ft" if pd.notnull(row['Depth']) else (f"Bank {row['Bank']}" if pd.notnull(row['Bank']) else "N/A")

                return pd.Series({
                    "Node ID": row['NodeNum'], "Position": pos,
                    "Current Temp": fmt_temp(row['current_temp']), "High (24h)": fmt_temp(row['high_24h']), "Low (24h)": fmt_temp(row['low_24h']),
                    "Seen (24h)": "✅" if row['active_24h'] == 1 else "❌",
                    "% Active (24h)": f"{round((row['hours_24h'] / 24) * 100, 1)}%", 
                    "Gap (24h)": f"{int(row['gap_24h'])}h", "Status": status_str
                })
            st.dataframe(sensor_df.apply(format_sensor_row, axis=1), use_container_width=True, hide_index=True)
            
    except Exception as e:
        st.error(f"Executive Summary Error: {e}")
        
###########
# - 7. PAGE: CLIENT PORTAL - #
###########

def render_client_portal(selected_project, display_tz, unit_mode, unit_label, active_refs):
    st.header(f"📊 Project Status: {selected_project}")
    global client

    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar.")
        return
    
    with st.spinner("Loading approved data..."):
        p_df = get_universal_portal_data(selected_project, view_mode="client")
    
    if p_df.empty:
        st.warning(f"⚠️ No data marked as 'Approved' found for {selected_project}.")
        return

    tab_time, tab_depth, tab_table = st.tabs(["📈 Timeline Analysis", "📏 Depth Profile", "📋 Summary Table"])

    with tab_time:
        weeks_view = st.slider("Weeks to View", 1, 12, 6, key="client_weeks_slider")
        now_utc = pd.Timestamp.now(tz='UTC')
        start_view = now_utc - timedelta(weeks=weeks_view)
        
        locations = sorted(p_df['Location'].dropna().unique())
        for loc in locations:
            with st.expander(f"📍 {loc}", expanded=(len(locations) == 1)):
                loc_data = p_df[p_df['Location'] == loc].copy()
                fig = build_high_speed_graph(
                    df=loc_data, 
                    title=f"{loc} Approved Data", 
                    start_view=start_view, 
                    end_view=now_utc, 
                    active_refs=tuple(active_refs), 
                    unit_mode=unit_mode, 
                    unit_label=unit_label, 
                    display_tz=display_tz 
                )
                st.plotly_chart(fig, use_container_width=True, key=f"portal_grid_{loc}")

    with tab_depth:
        st.subheader("📏 Vertical Temperature Profile")
        
        # --- 1. AXIS CONFIGURATION (Matching Engineering Depth Charts) ---
        x_min_f, x_max_f, ref_f = -20, 60, 32.0
        if unit_label == "°C":
            x_min, x_max, ref_val = (x_min_f-32)*5/9, (x_max_f-32)*5/9, 0.0
        else:
            x_min, x_max, ref_val = x_min_f, x_max_f, ref_f

        p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
        depth_only = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
        
        for loc in sorted(depth_only['Location'].unique()):
            with st.expander(f"📏 {loc} Weekly Snapshots", expanded=False):
                loc_data = depth_only[depth_only['Location'] == loc].copy()
                fig_d = go.Figure()
                
                # Snapshot Logic: Last 6 Mondays at 6 AM
                mondays = pd.date_range(end=pd.Timestamp.now(tz='UTC'), periods=6, freq='W-MON')
                for m_date in mondays:
                    target_ts = m_date.replace(hour=6, minute=0, second=0)
                    window = loc_data[(loc_data['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & 
                                     (loc_data['timestamp'] <= target_ts + pd.Timedelta(hours=12))]
                    
                    if not window.empty:
                        snap_df = (
                            window.assign(diff=(window['timestamp'] - target_ts).abs())
                            .sort_values(['NodeNum', 'diff'])
                            .drop_duplicates('NodeNum')
                            .sort_values('Depth_Num')
                        )
                        
                        conv_temps = snap_df['temperature'].apply(
                            lambda x: (x - 32) * 5/9 if unit_mode == "Celsius" else x
                        )
                        
                        fig_d.add_trace(go.Scatter(
                            x=conv_temps, 
                            y=snap_df['Depth_Num'], 
                            mode='lines+markers', 
                            name=target_ts.strftime('%m/%d/%y'),
                            line=dict(shape='spline', smoothing=0.5)
                        ))

                # Add 32 degree line
                fig_d.add_vline(x=ref_val, line_dash="dash", line_color="RoyalBlue", 
                                annotation_text="Freezing", annotation_position="top right")

                y_limit = int(((loc_data['Depth_Num'].max() // 10) + 1) * 10) if not loc_data.empty else 50
                fig_d.update_layout(
                    plot_bgcolor='white', height=600,
                    xaxis=dict(title=f"Temp ({unit_label})", range=[x_min, x_max], gridcolor='Gainsboro'),
                    yaxis=dict(title="Depth (ft)", range=[y_limit, 0], dtick=10, gridcolor='Silver'),
                    legend=dict(orientation="h", y=-0.2)
                )
                st.plotly_chart(fig_d, use_container_width=True, key=f"portal_depth_{loc}")

    with tab_table:
        latest = p_df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        latest['Current Temp'] = latest['temperature'].apply(
            lambda x: f"{round((x - 32) * 5/9 if unit_mode == 'Celsius' else x, 1)}{unit_label}"
        )
        latest['Position'] = latest.apply(
            lambda r: f"Bank {r['Bank']}" if pd.notnull(r['Bank']) and str(r['Bank']).strip() != "" 
            else f"{r.get('Depth', '??')} ft", axis=1
        )
        st.dataframe(
            latest[['Location', 'Position', 'Current Temp', 'NodeNum']].sort_values(['Location', 'Position']), 
            use_container_width=True, hide_index=True
        )
            
###########
# - 7. PAGE: NODE DIAGNOSTICS - #
###########

def render_node_diagnostics(selected_project, display_tz):
    st.header(f"📡 Real-Time Commissioning: {selected_project}")
    st.write("Nodes categorized by check-in freshness (15-minute increments).")

    diag_q = f"""
        SELECT 
            reg.Location, reg.NodeNum, reg.Depth, reg.Bank,
            MAX(r.timestamp) as last_ping
        FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` reg
        LEFT JOIN (
            SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
            UNION ALL
            SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
        ) r ON reg.NodeNum = r.NodeNum
        WHERE reg.Project = '{selected_project}' AND reg.ProjectStatus = 'Active'
        GROUP BY 1, 2, 3, 4
    """
    df = client.query(diag_q).to_dataframe()
    
    if df.empty:
        st.warning("No nodes found for this project.")
        return

    now = pd.Timestamp.now(tz='UTC')

    def get_latency_cat(ping):
        if pd.isnull(ping): return "❌ Never Seen"
        if ping.tzinfo is None: ping = ping.tz_localize('UTC')
        diff = (now - ping).total_seconds() / 60
        if diff <= 15: return "🟢 0-15 Mins"
        if diff <= 30: return "🟡 15-30 Mins"
        if diff <= 45: return "🟠 30-45 Mins"
        if diff <= 60: return "🔴 45-60 Mins"
        return "⏳ > 1 Hour"

    df['Status'] = df['last_ping'].apply(get_latency_cat)
    order = ["🟢 0-15 Mins", "🟡 15-30 Mins", "🟠 30-45 Mins", "🔴 45-60 Mins", "⏳ > 1 Hour", "❌ Never Seen"]
    df['Status'] = pd.Categorical(df['Status'], categories=order, ordered=True)
    
    st.dataframe(df.sort_values('Status'), use_container_width=True, hide_index=True)
    
###########
# - 9. PAGE: DATA INTAKE LAB - #
###########

def render_data_intake_page(selected_project):
    st.header("📤 Data Ingestion Lab")
    tab_upload, tab_export = st.tabs(["📄 Upload", "📥 Export"])
    
    with tab_upload:
        st.subheader("📄 Manual File Ingestion")
        st.info("Standardized Rule: All Lord Node IDs will use '-' as a separator (e.g., 58014-ch1).")
        
        u_file = st.file_uploader("Upload Data File", type=['csv', 'xlsx'], key="manual_upload_main")
        
        if u_file is not None:
            try:
                # --- 1. DETECTION FOR SENSORCONNECT (WIDE) ---
                is_sensorconnect = False
                skip_rows = 0
                
                if u_file.name.endswith('.csv'):
                    u_file.seek(0)
                    for i, line in enumerate(u_file):
                        if b"DATA_START" in line:
                            is_sensorconnect = True
                            skip_rows = i + 1 
                            break
                    u_file.seek(0)

                # --- 2. INITIAL READ ---
                if is_sensorconnect:
                    st.info("Format Detected: Lord SensorConnect (Wide)")
                    df_raw = pd.read_csv(u_file, encoding='latin1', skiprows=skip_rows, dtype=str)
                elif u_file.name.endswith('.csv'):
                    df_raw = pd.read_csv(u_file, encoding='latin1', dtype=str)
                else:
                    df_raw = pd.read_excel(u_file, dtype=str)

                if not df_raw.empty:
                    df_processed = pd.DataFrame()
                    actual_headers = list(df_raw.columns)
                    clean_headers = [str(h).strip().lower() for h in actual_headers]
                    
                    # --- BRANCH A: SENSORCONNECT (Wide Format) ---
                    if is_sensorconnect:
                        time_col = [h for h in actual_headers if 'time' in h.lower()][0]
                        value_vars = [h for h in actual_headers if h != time_col]
                        
                        df_melted = df_raw.melt(
                            id_vars=[time_col], 
                            value_vars=value_vars, 
                            var_name='NodeNum', 
                            value_name='temperature'
                        )
                        
                        df_processed['timestamp'] = pd.to_datetime(df_melted[time_col], format='mixed')
                        # STANDARDIZATION: Swap ':' for '-'
                        df_processed['NodeNum'] = df_melted['NodeNum'].str.strip().str.replace(':', '-')
                        df_processed['temperature'] = pd.to_numeric(df_melted['temperature'], errors='coerce')

                    # --- BRANCH B: LORD (Long/Narrow Format) ---
                    elif any('channel' in h or 'node' in h for h in clean_headers) and any('time' in h for h in clean_headers):
                        st.info("Format Detected: Lord (Channel-based)")
                        time_idx = next(i for i, h in enumerate(clean_headers) if 'time' in h)
                        node_idx = next(i for i, h in enumerate(clean_headers) if 'channel' in h or 'node' in h)
                        
                        time_header = actual_headers[time_idx]
                        node_header = actual_headers[node_idx]
                        temp_match = [h for h in actual_headers if 'temp' in h.lower()]
                        
                        if temp_match:
                            df_processed['timestamp'] = pd.to_datetime(df_raw[time_header], format='mixed')
                            # STANDARDIZATION: Swap ':' for '-'
                            df_processed['NodeNum'] = df_raw[node_header].str.strip().str.replace(':', '-')
                            df_processed['temperature'] = pd.to_numeric(df_raw[temp_match[0]], errors='coerce')

                    # --- BRANCH C: SENSORPUSH ---
                    else:
                        st.info("Format Detected: SensorPush")
                        t_match = [h for h in actual_headers if 'timestamp' in h.lower()]
                        v_match = [h for h in actual_headers if 'temp' in h.lower()]
                        if t_match and v_match:
                            import re
                            match = re.search(r'^([^ \(\.]+)', u_file.name)
                            df_processed['timestamp'] = pd.to_datetime(df_raw[t_match[0]], format='mixed')
                            df_processed['temperature'] = pd.to_numeric(df_raw[v_match[0]], errors='coerce')
                            df_processed['NodeNum'] = match.group(1) if match else "Unknown"

                    # --- 3. PREVIEW & UPLOAD ---
                    if not df_processed.empty:
                        df_processed = df_processed.dropna(subset=['timestamp', 'temperature'])
                        
                        found_nodes = df_processed['NodeNum'].unique()
                        st.success(f"✅ Ready: Standardized Node IDs: {', '.join(found_nodes)}")
                        st.dataframe(df_processed.head(10))

                        target_table = "raw_lord" if (is_sensorconnect or 'channel' in clean_headers or 'node' in clean_headers) else "raw_sensorpush"
                        
                        if st.button("🚀 Push to BigQuery"):
                            with st.spinner("Uploading data..."):
                                table_id = f"{PROJECT_ID}.{DATASET_ID}.{target_table}"
                                config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                                client.load_table_from_dataframe(df_processed, table_id, job_config=config).result()
                                
                                st.success(f"Successfully uploaded {len(df_processed)} rows to {target_table}!")
                                st.cache_data.clear()
            except Exception as e:
                st.error(f"Error processing file: {e}")

    with tab_export:
        st.subheader("📥 Export Project Data")
        if not selected_project or selected_project == "All Projects":
            st.warning("⚠️ Please select a specific project in the sidebar to perform an export.")
        else:
            # 1. Date Selection
            c1, c2 = st.columns(2)
            with c1:
                e_start = st.date_input("Start Date", value=datetime.now() - timedelta(days=30), key="exp_start")
            with c2:
                e_end = st.date_input("End Date", value=datetime.now(), key="exp_end")
            
            # 2. Scope Selection (Whole Project vs Single Pipe)
            st.write("---")
            export_scope = st.radio("Export Scope", ["Whole Project", "Specific Pipe / Bank"], horizontal=True)
            
            # Fetch data once to populate location options and for filtering
            with st.spinner("Preparing export options..."):
                full_df = get_universal_portal_data(selected_project, view_mode="engineering")
            
            target_loc = None
            if export_scope == "Specific Pipe / Bank" and not full_df.empty:
                loc_list = sorted(full_df['Location'].dropna().unique())
                target_loc = st.selectbox("Select Pipe/Bank to Export", loc_list)

            # 3. Export Action
            if st.button("📦 Prepare Data for Download"):
                if full_df.empty:
                    st.error("No data found for this project in the engineering database.")
                else:
                    # Filter by Date
                    mask = (full_df['timestamp'].dt.date >= e_start) & (full_df['timestamp'].dt.date <= e_end)
                    export_df = full_df.loc[mask].copy()

                    # Filter by Scope
                    filename_suffix = "Whole_Project"
                    if export_scope == "Specific Pipe / Bank" and target_loc:
                        export_df = export_df[export_df['Location'] == target_loc]
                        filename_suffix = target_loc.replace(" ", "_")

                    if export_df.empty:
                        st.warning("No data found matching the combined date and scope filters.")
                    else:
                        # Success Message & Download
                        st.success(f"✅ Prepared {len(export_df)} rows for {filename_suffix}.")
                        
                        # Clean up timestamps for the CSV
                        export_df['timestamp'] = export_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                        
                        csv = export_df.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label=f"💾 Download {filename_suffix} CSV",
                            data=csv,
                            file_name=f"{selected_project}_{filename_suffix}_Export.csv",
                            mime="text/csv"
                        )
###########
# - 10. PAGE: ADMIN TOOLS - #
###########

def render_admin_page(selected_project, display_tz, unit_mode, unit_label, active_refs):
    st.header("🛠️ Admin Tools")
    
    # 1. GLOBAL REGISTRY FETCH
    reg_q = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.project_registry`"
    try:
        full_reg_df = client.query(reg_q).to_dataframe()
        # Force numeric depth for logic consistency
        full_reg_df['Depth'] = pd.to_numeric(full_reg_df['Depth'], errors='coerce')
    except:
        full_reg_df = pd.DataFrame()
    
    # Identify Active Sensors for the current sidebar project
    active_project_df = pd.DataFrame()
    if not full_reg_df.empty:
        active_project_df = full_reg_df[(full_reg_df['Project'] == selected_project) & (full_reg_df['EndDate'].isna())]
    
    # Stable Location list for tabs
    loc_options = ["All Locations"] + sorted(active_project_df['Location'].unique().tolist()) if not active_project_df.empty else ["All Locations"]
    
    # --- 3. TABS DEFINITION ---
    # Ensure 4 variables are present to match the 4 labels in the list
    tab_reg, tab_bulk, tab_scrub, tab_surgical = st.tabs([
        "📋 Registry Manager", 
        "✅ Bulk Approval", 
        "🧹 Scrub", 
        "🧨 Surgical & Mask"
    ])
    
    # --- TAB 0: REGISTRY MANAGER ---
    with tab_reg:
        # 1. PRIMARY ACTION SELECTOR
        reg_mode = st.radio("Registry Action", 
                            ["🔍 Registry Intelligence", "🔄 Replace Sensor", "📁 Project Lifecycle", "📥 Register New Hardware"], 
                            horizontal=True, key="reg_main_action_radio")
        st.divider()

        # --- MODE A: REGISTRY INTELLIGENCE (SEARCH, FILTER & EDIT) ---
        if reg_mode == "🔍 Registry Intelligence":
            st.subheader("🔍 Registry Intelligence")
            
            # Layered Filters
            f_col1, f_col2, f_col3 = st.columns(3)
            
            with f_col1:
                # 1. Filter by SENSOR STATUS (Active, Swapped, Available, Template)
                # This helps you find hardware that needs attention
                raw_sens_statuses = full_reg_df['SensorStatus'].dropna().unique().tolist()
                sens_status_list = ["All Sensor Statuses"] + sorted(raw_sens_statuses)
                sens_status_sel = st.selectbox("Filter by Sensor Status:", sens_status_list, key="intel_sens_status_filter")
            
            with f_col2:
                # 2. Filter by PROJECT STATUS (Active vs Archived)
                raw_proj_statuses = full_reg_df['ProjectStatus'].dropna().unique().tolist()
                proj_status_list = ["All Project Statuses"] + sorted(raw_proj_statuses)
                proj_status_sel = st.selectbox("Filter by Project Status:", proj_status_list, key="intel_proj_status_filter")
            
            # Apply Initial Filters
            search_df = full_reg_df.copy()
            if sens_status_sel != "All Sensor Statuses":
                search_df = search_df[search_df['SensorStatus'] == sens_status_sel]
            if proj_status_sel != "All Project Statuses":
                search_df = search_df[search_df['ProjectStatus'] == proj_status_sel]

            with f_col3:
                # 3. Drill-down Search
                intel_mode = st.radio("Search Mode:", ["By Project", "By Node ID"], horizontal=True, key="intel_mode_filter")
                
                if intel_mode == "By Project":
                    proj_list = ["All Projects"] + sorted(search_df['Project'].dropna().unique().tolist())
                    proj_sel = st.selectbox("Select Project:", proj_list, key="intel_proj_filter")
                    if proj_sel != "All Projects":
                        search_df = search_df[search_df['Project'] == proj_sel]
                else:
                    node_search = st.text_input("Enter Node ID (Partial OK)", key="intel_node_search_input")
                    if node_search:
                        search_df = search_df[search_df['NodeNum'].fillna('').str.contains(node_search, na=False, case=False)]

            st.divider()
            st.write(f"Found **{len(search_df)}** matching sensor records.")
            
            # SORTING LOGIC: We sort by SensorStatus first so "Active" sensors float to the top
            search_df = search_df.sort_values(by=['SensorStatus', 'Project', 'Location'], ascending=[True, True, True])
            
            # SPREADSHEET EDITOR
            edit_enabled = st.checkbox("✍️ Enable Manual Spreadsheet Edits", key="intel_edit_toggle")
            if edit_enabled:
                st.info("💡 **Admin Note:** You can manually change a status to 'Active' or 'Swapped' here.")
                edited_df = st.data_editor(search_df, num_rows="dynamic", key="intel_data_editor_widget", use_container_width=True)
                
                if st.button("💾 Push Changes to BigQuery", key="intel_save_btn", use_container_width=True):
                    with st.spinner("Syncing Registry..."):
                        final_sync_df = full_reg_df.copy()
                        final_sync_df.update(edited_df)
                        
                        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                        client.load_table_from_dataframe(final_sync_df, f"{PROJECT_ID}.{DATASET_ID}.project_registry", job_config=job_config).result()
                        
                        st.success("✅ Registry Synchronized.")
                        st.cache_data.clear()
                        st.rerun()
            else:
                st.dataframe(search_df, use_container_width=True, hide_index=True)
            st.write(f"Found **{len(search_df)}** matching assignments.")
            
            # SPREADSHEET EDITOR
            edit_enabled = st.checkbox("✍️ Enable Manual Spreadsheet Edits", key="intel_edit_toggle")
            if edit_enabled:
                st.info("💡 **Admin Note:** Changes are staged locally. Click 'Push Changes' to sync to BigQuery.")
                edited_df = st.data_editor(search_df, num_rows="dynamic", key="intel_data_editor_widget", use_container_width=True)
                
                if st.button("💾 Push Changes to BigQuery", key="intel_save_btn", use_container_width=True):
                    with st.spinner("Syncing Registry..."):
                        # Merge updates back to prevent deleting other filtered projects
                        final_sync_df = full_reg_df.copy()
                        final_sync_df.update(edited_df)
                        
                        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                        client.load_table_from_dataframe(final_sync_df, f"{PROJECT_ID}.{DATASET_ID}.project_registry", job_config=job_config).result()
                        
                        st.success("✅ Registry Synchronized Successfully.")
                        st.cache_data.clear()
                        st.rerun()
            else:
                st.dataframe(search_df.sort_values(['Project', 'Location', 'Depth']), use_container_width=True, hide_index=True)

        # --- MODE B: INDIVIDUAL SENSOR SWAP ---
        elif reg_mode == "🔄 Replace Sensor":
            st.subheader("🔄 Hardware Swap")
            if not active_project_df.empty:
                c1, c2 = st.columns(2)
                t_loc = c1.selectbox("Select Location", sorted(active_project_df['Location'].unique()), key="swap_loc_sel")
                
                loc_rows = active_project_df[active_project_df['Location'] == t_loc]
                t_row = c2.selectbox(
                    "Select Position", 
                    loc_rows.to_dict('records'), 
                    format_func=lambda x: f"{x['Depth']}ft ({x['NodeNum']})" if pd.notnull(x['Depth']) else f"Bank {x['Bank']} ({x['NodeNum']})",
                    key="swap_row_sel"
                )
                
                # Inventory Availability Check
                inv_q = f"SELECT NodeNum FROM `{PROJECT_ID}.{DATASET_ID}.hardware_inventory`"
                try:
                    inventory = client.query(inv_q).to_dataframe()['NodeNum'].tolist()
                    active_now = full_reg_df[full_reg_df['EndDate'].isna()]['NodeNum'].tolist()
                    available = [n for n in inventory if n not in active_now]
                except:
                    available = []

                new_node = st.selectbox("Select Available Hardware", available if available else ["No hardware found"], key="swap_node_sel")
                
                if st.button("🚀 Execute Swap", use_container_width=True, key="swap_exec_btn"):
                    s_bank = f"'{t_row['Bank']}'" if pd.notnull(t_row['Bank']) else "NULL"
                    s_depth = str(t_row['Depth']) if pd.notnull(t_row['Depth']) else "NULL"
                    
                    swap_sql = f"""
                        BEGIN TRANSACTION;
                        UPDATE `{PROJECT_ID}.{DATASET_ID}.project_registry`
                        SET EndDate = CURRENT_TIMESTAMP(), SensorStatus = 'Swapped'
                        WHERE NodeNum = '{t_row['NodeNum']}' AND Location = '{t_loc}' AND EndDate IS NULL;
                    
                        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.project_registry` 
                        (Project, Location, NodeNum, Bank, Depth, StartDate, SensorStatus, ProjectStatus)
                        VALUES ('{selected_project}', '{t_loc}', '{new_node}', {s_bank}, {s_depth}, CURRENT_TIMESTAMP(), 'Active', 'Active');
                        COMMIT;
                    """
                    client.query(swap_sql).result()
                    st.success(f"Swapped {t_row['NodeNum']} for {new_node}.")
                    st.cache_data.clear()
            else:
                st.info("No active sensors in the current sidebar project.")

        # --- MODE C: PROJECT LIFECYCLE ---
        elif reg_mode == "📁 Project Lifecycle":
            st.subheader("📁 Project Management")
            action = st.selectbox("Action Type", ["Initialize New Project", "Retire Current Project"], key="life_action_sel")

            if action == "Initialize New Project":
                with st.form("bulk_init_form_v2"):
                    n_id = st.text_input("New Project Code")
                    n_locs = st.text_area("Pipe/Bank Names (One per line)")
                    if st.form_submit_button("🚀 Build Skeleton"):
                        if n_id and n_locs:
                            l_list = [l.strip() for l in n_locs.split('\n') if l.strip()]
                            rows = [f"('{n_id}', '{loc}', 'TBD', CURRENT_TIMESTAMP(), 'Template', 'Active')" for loc in l_list]
                            sql = f"INSERT INTO `{PROJECT_ID}.{DATASET_ID}.project_registry` (Project, Location, NodeNum, StartDate, SensorStatus, ProjectStatus) VALUES {', '.join(rows)}"
                            client.query(sql).result()
                            st.success(f"Skeleton created for {n_id}.")
                            st.cache_data.clear()

            elif action == "Retire Current Project":
                st.warning(f"Archive **{selected_project}**? Hardware will be released to inventory.")
                if st.button(f"🔥 Finalize Retirement", type="primary", key="life_retire_btn"):
                    client.query(f"UPDATE `{PROJECT_ID}.{DATASET_ID}.project_registry` SET ProjectStatus = 'Archived', EndDate = CURRENT_TIMESTAMP(), SensorStatus = 'Available' WHERE Project = '{selected_project}' AND EndDate IS NULL").result()
                    st.success("Project retired.")
                    st.cache_data.clear()

        # --- MODE D: INVENTORY ---
        elif reg_mode == "📥 Register New Hardware":
            st.subheader("📥 Master Inventory")
            i_type = st.radio("Input", ["Manual", "Bulk CSV"], horizontal=True, key="inv_input_radio")
            if i_type == "Manual":
                c1, c2 = st.columns(2)
                l_id = c1.text_input("Long ID", key="inv_manual_long")
                f_id = c2.text_input("Friendly ID", key="inv_manual_friend")
                if st.button("💾 Add to Inventory", key="inv_manual_btn"):
                    client.query(f"INSERT INTO `{PROJECT_ID}.{DATASET_ID}.hardware_inventory` (RawID, NodeNum, DateAdded) VALUES ('{l_id}', '{f_id}', CURRENT_DATE())").result()
                    st.success(f"Registered {f_id}")
            else:
                u_f = st.file_uploader("CSV (RawID, NodeNum)", type=['csv'], key="inv_csv_uploader")
                if u_f and st.button("📤 Upload", key="inv_csv_btn"):
                    client.load_table_from_dataframe(pd.read_csv(u_f), f"{PROJECT_ID}.{DATASET_ID}.hardware_inventory").result()
                    st.success("Inventory updated.")

    # --- TAB 1: BULK APPROVAL ---
    with tab_bulk:
        st.subheader("✅ Range-Based Bulk Approval")
        sel_loc_bulk = st.selectbox("Target Location", loc_options, key="bulk_loc_sel")
        c1, c2 = st.columns(2)
        b_start = c1.date_input("Start", value=datetime.now() - timedelta(days=7), key="bulk_start")
        b_end = c2.date_input("End", value=datetime.now(), key="bulk_end")

        if st.button(f"🚀 Approve Range", use_container_width=True):
            loc_filter = f"AND m.Location = '{sel_loc_bulk}'" if sel_loc_bulk != "All Locations" else ""
            bulk_sql = f"""
                INSERT INTO `{OVERRIDE_TABLE}` (NodeNum, timestamp, approve)
                SELECT DISTINCT r.NodeNum, TIMESTAMP_TRUNC(r.timestamp, HOUR), 'TRUE'
                FROM (SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` UNION ALL SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`) AS r
                INNER JOIN `{PROJECT_ID}.{DATASET_ID}.project_registry` AS m ON r.NodeNum = m.NodeNum
                WHERE m.Project = '{selected_project}' {loc_filter}
                AND r.timestamp >= '{b_start}' AND r.timestamp <= '{b_end}'
                AND NOT EXISTS (SELECT 1 FROM `{OVERRIDE_TABLE}` x WHERE x.NodeNum = r.NodeNum AND x.timestamp = TIMESTAMP_TRUNC(r.timestamp, HOUR))
            """
            client.query(bulk_sql).result()
            st.success("Range approved.")
            st.cache_data.clear()

    # --- TAB 2: DEEP DATA SCRUB ---
    with tab_scrub:
        st.subheader("🧹 Deep Data Scrub")
        scrub_target = st.radio("Target Table", ["SensorPush", "Lord"], horizontal=True)
        sel_loc_scrub = st.selectbox("Target Location", loc_options, key="scrub_loc_sel")
        if st.button(f"🧨 Purge & Average {scrub_target}", use_container_width=True):
            t_table = f"{PROJECT_ID}.{DATASET_ID}.raw_{scrub_target.lower()}"
            loc_subquery = f"SELECT NodeNum FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE Project = '{selected_project}'"
            if sel_loc_scrub != "All Locations":
                loc_subquery += f" AND Location = '{sel_loc_scrub}'"
            scrub_sql = f"""
                CREATE OR REPLACE TABLE `{t_table}` AS 
                SELECT TIMESTAMP_TRUNC(TIMESTAMP_ADD(timestamp, INTERVAL 30 MINUTE), HOUR) as timestamp, NodeNum, AVG(temperature) as temperature
                FROM `{t_table}`
                WHERE NodeNum IN ({loc_subquery}) OR NodeNum NOT IN (SELECT NodeNum FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE Project = '{selected_project}')
                GROUP BY 1, 2
            """
            client.query(scrub_sql).result()
            st.success("Table scrubbed.")
            st.cache_data.clear()

    # --- TAB 3: SURGICAL & MASK ---
    with tab_surgical:
        if not selected_project or selected_project == "All Projects":
            st.warning("Please select a project.")
        else:
            render_surgical_cleaner(selected_project, display_tz, unit_mode, unit_label, active_refs)
###########
# - 11. SURGICAL CLEANER FUNCTIONS - #
###########

def render_surgical_cleaner(selected_project, display_tz, unit_mode, unit_label, active_refs):
    st.subheader("🧨 Unified Data Management (Mask & Purge)")
    
    # 1. SCOPE & ACTION MODE
    c1, c2 = st.columns(2)
    with c1:
        scope = st.radio("Target Scope", ["Project Wide", "Specific Location", "Specific Node"], horizontal=True)
    with c2:
        action_mode = st.radio("Action Type", ["🚫 Mask (Soft Hide)", "🔥 Purge (Hard Delete)"], horizontal=True)

    # Fetch registry for selectors
    reg_q = f"SELECT NodeNum, Location FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE Project = '{selected_project}'"
    reg_df = client.query(reg_q).to_dataframe()
    
    target_node, target_loc = None, None
    if scope == "Specific Location":
        target_loc = st.selectbox("Select Location", sorted(reg_df['Location'].unique()))
    elif scope == "Specific Node":
        target_node = st.selectbox("Select Node ID", sorted(reg_df['NodeNum'].unique()))

    # 2. TEMPORAL LOGIC (Directional)
    st.divider()
    t_col1, t_col2 = st.columns([1, 2])
    direction = t_col1.selectbox("Temporal Direction", ["Between Range", "Everything Older Than", "Everything Newer Than"])
    
    with t_col2:
        if direction == "Between Range":
            sc1, sc2 = st.columns(2)
            s_dt = datetime.combine(sc1.date_input("Start Date", value=datetime.now() - timedelta(days=7)), dt_time(0,0))
            e_dt = datetime.combine(sc2.date_input("End Date", value=datetime.now()), dt_time(23,59))
        else:
            anchor_dt = datetime.combine(
                st.date_input("Anchor Date"), 
                st.time_input("Anchor Time", value=dt_time(6,0)))
            # Set virtual range for SQL
            s_dt = datetime(2000, 1, 1) if direction == "Everything Older Than" else anchor_dt
            e_dt = anchor_dt if direction == "Everything Older Than" else datetime(2100, 1, 1)


    # 3. THRESHOLD
    thr_col1, thr_col2 = st.columns([1, 2])
    operator = thr_col1.selectbox("Value Filter", ["No Threshold", "Greater Than (>)", "Less Than (<)"])
    thresh_val = thr_col2.number_input(f"Threshold Value ({unit_label})", value=100.0)
    thresh_val_f = (thresh_val * 9/5) + 32 if unit_mode == "Celsius" else thresh_val

    # 4. SQL LOGIC CONSTRUCTION
    if scope == "Project Wide":
        where_clause, sub_where = f"m.Project = '{selected_project}'", f"Project = '{selected_project}'"
    elif scope == "Specific Location":
        where_clause, sub_where = f"m.Project = '{selected_project}' AND m.Location = '{target_loc}'", f"Project = '{selected_project}' AND Location = '{target_loc}'"
    else:
        where_clause, sub_where = f"r.NodeNum = '{target_node}'", f"NodeNum = '{target_node}'"

    threshold_clause = ""
    if operator == "Greater Than (>)": threshold_clause = f"AND r.temperature > {thresh_val_f}"
    elif operator == "Less Than (<)": threshold_clause = f"AND r.temperature < {thresh_val_f}"

    s_str, e_str = s_dt.strftime('%Y-%m-%d %H:%M:%S'), e_dt.strftime('%Y-%m-%d %H:%M:%S')

    # 5. EXECUTION GATE
    st.divider()
    if st.button("🔍 Step 1: Verify Match Count", use_container_width=True):
        status_q = f"""
            SELECT COALESCE(rej.approve, 'PENDING') as status, COUNT(*) as point_count
            FROM (SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` UNION ALL SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`) AS r
            INNER JOIN `{PROJECT_ID}.{DATASET_ID}.project_registry` AS m ON r.NodeNum = m.NodeNum
            LEFT JOIN `{OVERRIDE_TABLE}` AS rej ON r.NodeNum = rej.NodeNum AND TIMESTAMP_TRUNC(r.timestamp, HOUR) = rej.timestamp
            WHERE {where_clause} {threshold_clause} AND r.timestamp BETWEEN '{s_str}' AND '{e_str}'
            GROUP BY status
        """
        st.session_state["purge_staged_df"] = client.query(status_q).to_dataframe()

    if "purge_staged_df" in st.session_state:
        staged_df = st.session_state["purge_staged_df"]
        total = staged_df['point_count'].sum() if not staged_df.empty else 0
        
        if total > 0:
            st.warning(f"### ⚠️ Action Staged: {total} Points")
            st.table(staged_df.set_index('status'))
            
            confirm = st.checkbox(f"Confirm {action_mode} for these records.")
            
            if st.button(f"🚀 Execute {action_mode}", use_container_width=True, disabled=not confirm):
                if "Mask" in action_mode:
                    # MASKING LOGIC (MERGE INTO OVERRIDE TABLE)
                    sql = f"""
                        MERGE `{OVERRIDE_TABLE}` T
                        USING (
                            SELECT DISTINCT r.NodeNum, TIMESTAMP_TRUNC(r.timestamp, HOUR) as ts
                            FROM (SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` UNION ALL SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`) AS r
                            INNER JOIN `{PROJECT_ID}.{DATASET_ID}.project_registry` AS m ON r.NodeNum = m.NodeNum
                            WHERE {where_clause} {threshold_clause} AND r.timestamp BETWEEN '{s_str}' AND '{e_str}'
                        ) S ON T.NodeNum = S.NodeNum AND T.timestamp = S.ts
                        WHEN MATCHED THEN UPDATE SET approve = 'MASKED'
                        WHEN NOT MATCHED THEN INSERT (NodeNum, timestamp, approve) VALUES (S.NodeNum, S.ts, 'MASKED')
                    """
                else:
                    # PURGE LOGIC (HARD DELETE FROM ALL TABLES)
                    sql = f"""
                        BEGIN TRANSACTION;
                        DELETE FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` r WHERE EXISTS (SELECT 1 FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` m WHERE r.NodeNum = m.NodeNum AND {where_clause}) {threshold_clause} AND r.timestamp BETWEEN '{s_str}' AND '{e_str}';
                        DELETE FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord` r WHERE EXISTS (SELECT 1 FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` m WHERE r.NodeNum = m.NodeNum AND {where_clause}) {threshold_clause} AND r.timestamp BETWEEN '{s_str}' AND '{e_str}';
                        DELETE FROM `{OVERRIDE_TABLE}` WHERE NodeNum IN (SELECT NodeNum FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE {sub_where}) AND timestamp BETWEEN '{s_str}' AND '{e_str}';
                        COMMIT;
                    """
                client.query(sql).result()
                st.success(f"Successfully processed {total} points.")
                del st.session_state["purge_staged_df"]
                st.cache_data.clear()
                st.rerun()
            
###########
# - 11. SURGICAL CLEANER HELPERS - #
###########

def update_records(pts, df, val):
    """
    Writes status updates (TRUE, FALSE, MASKED) to the manual_rejections table.
    """
    recs = []
    for p in pts:
        try:
            # Snap timestamp to the hour for database alignment [cite: 14]
            ts = pd.to_datetime(p['x']).tz_convert('UTC').floor('h')
            node = df.iloc[p['point_index']]['NodeNum']
            
            recs.append({
                "NodeNum": str(node), 
                "timestamp": ts, 
                "approve": val 
            })
        except Exception:
            continue
    
    if recs:
        # Deduplicate to prevent multiple entries for the same hour [cite: 15]
        status_df = pd.DataFrame(recs).drop_duplicates(subset=['NodeNum', 'timestamp'])
        try:
            # Load updates into BigQuery [cite: 1, 11]
            job = client.load_table_from_dataframe(status_df, OVERRIDE_TABLE)
            job.result() 
            
            # Reset state and clear cache to reflect changes immediately [cite: 15]
            st.session_state.locked_selection = []
            st.cache_data.clear()
            st.success(f"Successfully marked {len(status_df)} records as {val}")
            time.sleep(1) 
            st.rerun()
        except Exception as e:
            st.error(f"Database Error: {e}")

###########
# - 12. PAGE: DEPTH CHARTS - #
###########

def render_depth_charts(selected_project, unit_label, display_tz):
    st.header(f"📏 Weekly Depth Profiles: {selected_project}")
    st.write("Vertical snapshots captured every Monday at 6:00 AM.")
    
    # Fetch data using the standard engine
    df = get_universal_portal_data(selected_project, view_mode="engineering")
    
    if df.empty:
        st.warning("No data found for this project.")
        return

    # Filter for nodes with numeric depth assignments
    df['Depth_Num'] = pd.to_numeric(df['Depth'], errors='coerce')
    depth_only = df.dropna(subset=['Depth_Num', 'Location']).copy()
    
    if depth_only.empty:
        st.info("No sensors with depth assignments found in the registry.")
        return

    # --- 1. AXIS RANGE & REFERENCE LINE CONFIGURATION ---
    # Define values in Fahrenheit first
    x_min_f, x_max_f, ref_f = -20, 60, 32.0
    
    # Convert if the user has selected Celsius
    if unit_label == "°C":
        x_min = (x_min_f - 32) * 5/9
        x_max = (x_max_f - 32) * 5/9
        ref_val = (ref_f - 32) * 5/9
    else:
        x_min, x_max, ref_val = x_min_f, x_max_f, ref_f

    locations = sorted(depth_only['Location'].unique())
    
    for loc in locations:
        with st.expander(f"📍 Location: {loc}", expanded=True):
            loc_data = depth_only[depth_only['Location'] == loc].copy()
            fig_d = go.Figure()
            
            # 2. GENERATE WEEKLY SNAPSHOTS
            mondays = pd.date_range(end=pd.Timestamp.now(tz='UTC'), periods=6, freq='W-MON')
            
            for m_date in mondays:
                target_ts = m_date.replace(hour=6, minute=0, second=0)
                window = loc_data[(loc_data['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & 
                                 (loc_data['timestamp'] <= target_ts + pd.Timedelta(hours=12))]
                
                if not window.empty:
                    snap_df = (
                        window.assign(diff=(window['timestamp'] - target_ts).abs())
                        .sort_values(['NodeNum', 'diff'])
                        .drop_duplicates('NodeNum')
                        .sort_values('Depth_Num')
                    )
                    
                    conv_temps = snap_df['temperature'].apply(
                        lambda x: (x - 32) * 5/9 if unit_label == "°C" else x
                    )
                    
                    fig_d.add_trace(go.Scatter(
                        x=conv_temps, 
                        y=snap_df['Depth_Num'], 
                        mode='lines+markers', 
                        name=target_ts.strftime('%m/%d/%y'),
                        line=dict(shape='spline', smoothing=0.5),
                        hovertemplate=f"Depth: %{{y}}ft<br>Temp: %{{x:.1f}}{unit_label}<extra></extra>"
                    ))

            # --- 3. ADD THE 32° LINE ---
            fig_d.add_vline(
                x=ref_val, 
                line_dash="dash", 
                line_color="RoyalBlue", 
                annotation_text="Freezing", 
                annotation_position="top right"
            )

            # --- 4. STYLING & AXIS LIMITS ---
            y_max = int(((loc_data['Depth_Num'].max() // 10) + 1) * 10) if not loc_data.empty else 50
            
            fig_d.update_layout(
                plot_bgcolor='white', 
                height=750,
                xaxis=dict(
                    title=f"Temperature ({unit_label})", 
                    gridcolor='Gainsboro', 
                    zeroline=False,
                    range=[x_min, x_max],  # Fixed Temp Range
                    fixedrange=False
                ),
                yaxis=dict(
                    title="Depth (ft) below Surface", 
                    range=[y_max, 0], # Inverted Surface-Down View
                    dtick=10, 
                    gridcolor='Silver',
                    zeroline=False
                ),
                legend=dict(title="Snapshot Date", orientation="h", y=-0.15),
                margin=dict(l=40, r=40, t=40, b=100)
            )
            
            st.plotly_chart(fig_d, use_container_width=True, key=f"depth_snapshot_{loc}")


###########
# - 12. MAIN ROUTER - #
###########

# 2. PAGE EXECUTION LOGIC
if page == "Executive Summary":
    render_executive_summary(client, selected_project, unit_label, display_tz)

elif page == "Global Overview":
    render_global_overview(selected_project, display_tz)

elif page == "Depth Charts":
    # New function call using your standard variables
    render_depth_charts(selected_project, unit_label, display_tz)

elif page == "Node Diagnostics":
    # Updated function call for the 15-minute diagnostic table
    render_node_diagnostics(selected_project, display_tz)

elif page == "Client Portal":
    render_client_portal(selected_project, display_tz, unit_mode, unit_label, active_refs)

elif page == "Data Intake Lab":
    render_data_intake_page(selected_project) # Use the correct function name

elif page == "Admin Tools":
    if st.session_state.get('authenticated', False):
        render_admin_page(selected_project, display_tz, unit_mode, unit_label, active_refs)
    else:
        pwd = st.text_input("Enter Admin Password", type="password")
        if pwd == st.secrets["admin_password"]:
            st.session_state['authenticated'] = True
            st.rerun()
