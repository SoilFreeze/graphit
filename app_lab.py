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
    New Relational Engine: Joins Raw + Node Registry + Project Registry.
    """
    cutoff = PROJECT_VISIBILITY_MASKS.get(project_id, "2000-01-01 00:00:00")
    
    # 1. DEFINE THE FILTER
    if view_mode == "client":
        query_filter = f"AND rej.approve = 'TRUE' AND r.timestamp >= '{cutoff}'"
    else:
        query_filter = "AND (rej.approve IS NULL OR rej.approve != 'FALSE')"

    # 2. THE THREE-WAY JOIN
    query = f"""
        SELECT 
            n.Location, 
            r.timestamp, 
            r.temperature,
            n.NodeNum,
            n.Bank,
            n.Depth,
            n.Project,
            p.ProjectName,
            p.Timezone
        FROM (
            SELECT NodeNum, timestamp, temperature FROM `sensorpush-export.Temperature.raw_sensorpush`
            UNION ALL
            SELECT NodeNum, timestamp, temperature FROM `sensorpush-export.Temperature.raw_lord`
        ) AS r
        -- JOIN 1: Match raw data to its specific assignment in the Node Registry
        INNER JOIN `sensorpush-export.Temperature.node_registry` AS n 
            ON r.NodeNum = n.NodeNum
        -- JOIN 2: Get the site-wide settings from the Project Registry
        INNER JOIN `sensorpush-export.Temperature.project_registry` AS p 
            ON n.Project = p.Project
        -- JOIN 3: Check for manual approvals
        LEFT JOIN `{OVERRIDE_TABLE}` AS rej 
            ON r.NodeNum = rej.NodeNum 
            AND TIMESTAMP_TRUNC(r.timestamp, HOUR) = rej.timestamp
        WHERE n.Project = '{project_id}'
        {query_filter}
        -- Ensure data is within the specific window the sensor was assigned to this spot
        AND r.timestamp >= n.StartDate 
        AND (r.timestamp <= n.EndDate OR n.EndDate IS NULL)
        ORDER BY n.Location ASC, r.timestamp ASC
    """
    
    try:
        df = client.query(query).to_dataframe()
        if not df.empty:
            df['Depth'] = pd.to_numeric(df['Depth'], errors='coerce')
            df['temperature'] = pd.to_numeric(df['temperature'], errors='coerce')
            if df['timestamp'].dt.tz is None:
                df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
        return df
    except Exception as e:
        st.error(f"Registry Engine Error: {e}")
        return pd.DataFrame()
        
###########################
#- 3. SIDEBAR UI & STATE -#
###########################
###########################
# - SIDEBAR NAVIGATION -  #
###########################
st.sidebar.title("❄️ SoilFreeze Lab")

# --- SECTION 1: PAGE ROUTING ---
# This determines which function is called in the main router
page = st.sidebar.selectbox("Navigate To:", [
    "Executive Summary", 
    "Global Overview", 
    "Depth Charts", 
    "Node Diagnostics", 
    "Client Portal", 
    "Data Intake Lab", 
    "Admin Tools"
])

st.sidebar.divider()

# --- SECTION 2: PROJECT SELECTION ---
# Source of truth: project_registry
selected_project = "All Projects" # Default initialization
proj_filter = ""                  # Default filter

if client is not None:
    try:
        # Fetching names from project_registry
        proj_q = f"SELECT Project FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE ProjectStatus = 'Active'"
        proj_df = client.query(proj_q).to_dataframe()
        proj_list = sorted(proj_df['Project'].dropna().unique())
        
        # UI Component
        selected_project = st.sidebar.selectbox(
            "🎯 Active Project", 
            ["All Projects"] + proj_list, 
            key="sidebar_proj_picker_global"
        )
        
        # BUILD SQL FILTER: 
        # Using 'n.' prefix to target node_registry in relational JOINS
        if selected_project != "All Projects":
            proj_filter = f"AND n.Project = '{selected_project}'"
            
    except Exception as e:
        st.sidebar.error(f"Registry Link Offline: {e}")

st.sidebar.divider()

# --- SECTION 3: UNIT & MEASUREMENT ---
unit_mode = st.sidebar.radio("Temperature Unit", ["Fahrenheit", "Celsius"], horizontal=True)
unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"

st.sidebar.divider()

# --- SECTION 4: TIME & DISPLAY ---
st.sidebar.subheader("📱 Display & Time")

tz_lookup = {
    "UTC": "UTC", 
    "Local (US/Eastern)": "US/Eastern", 
    "Local (US/Pacific)": "US/Pacific"
}

tz_mode = st.sidebar.selectbox(
    "Timezone Display", 
    list(tz_lookup.keys()), 
    index=2  # Default: Pacific
)

display_tz = tz_lookup[tz_mode]
st.session_state["tz_selection"] = tz_mode 

# UI Optimization for small screens
mobile_optimized = st.sidebar.toggle(
    "Mobile Layout", 
    value=False, 
    key="mobile_optimized_toggle"
)

st.sidebar.divider()

# --- SECTION 5: SAFETY & REFERENCE LINES ---
# These values are passed to build_high_speed_graph
st.sidebar.subheader("📏 Reference Lines")
active_refs = [] 

if st.sidebar.checkbox("Freezing (32°F)", value=True): 
    active_refs.append((32.0, "Freezing"))
    
if st.sidebar.checkbox("Type B (26.6°F)", value=False): 
    active_refs.append((26.6, "Type B"))
    
if st.sidebar.checkbox("Type A (10.2°F)", value=False): 
    active_refs.append((10.2, "Type A"))

# --- END OF SIDEBAR ---

########################
#- 4. GRAPHING ENGINE -#
########################

def build_high_speed_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label, display_tz="UTC", mobile_mode=False):
    """
    Smart Responsive Engine:
    - Modular sorting for Relational Schema (Bank vs Depth).
    - Legend grouping to keep 1,500 sensors organized.
    """
    if df.empty:
        return go.Figure().update_layout(title="No data available for the selected period.")

    plot_df = df.copy()
    
    # 1. TIMEZONE & UNIT CONVERSION
    if plot_df['timestamp'].dt.tz is None:
        plot_df['timestamp'] = plot_df['timestamp'].dt.tz_localize('UTC')
    plot_df['timestamp'] = plot_df['timestamp'].dt.tz_convert(display_tz)
    
    # Context-aware time windows
    start_local = start_view.tz_convert(display_tz) if start_view.tzinfo else start_view.tz_localize('UTC').tz_convert(display_tz)
    end_local = end_view.tz_convert(display_tz) if end_view.tzinfo else end_view.tz_localize('UTC').tz_convert(display_tz)
    now_local = pd.Timestamp.now(tz=display_tz)
    
    range_start = start_local - pd.Timedelta(days=1)
    range_end = end_local + pd.Timedelta(days=1)
    
    if unit_mode == "Celsius":
        plot_df['temperature'] = (plot_df['temperature'] - 32) * 5/9
        y_range, dt_major, dt_minor = [-30, 30], 10, 5
    else:
        y_range, dt_major, dt_minor = [-20, 80], 10, 5

    # 2. UPDATED LABELING & SORTING (Relational Logic)
    def get_sort_info(r):
        node_id = r.get('NodeNum') or "Unknown"
        # Priority 1: Depth (Numeric sort for SensorPush/Vertical Pipes)
        if pd.notnull(r.get('Depth')):
            return f"{r['Depth']}ft", float(r['Depth'])
        # Priority 2: Bank (Categorical sort for Lord/Horizontal Banks)
        if pd.notnull(r.get('Bank')) and str(r['Bank']).strip() != "":
            return f"Bank {r['Bank']}", 999.0
        # Fallback: Node ID
        return f"Node {node_id}", 1000.0

    plot_df[['depth_label', 'sort_val']] = plot_df.apply(lambda x: pd.Series(get_sort_info(x)), axis=1)
    
    # 3. TRACE GENERATION
    fig = go.Figure()
    is_surgical = any(word in title for word in ["Scrubbing", "Surgical", "Diag"])
    
    # Sort groups so legends and trace order follow physical reality (Surface -> Down)
    unique_groups = plot_df[['depth_label', 'sort_val']].drop_duplicates().sort_values('sort_val')
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']

    for i, (_, g_row) in enumerate(unique_groups.iterrows()):
        group_lbl = g_row['depth_label']
        group_data = plot_df[plot_df['depth_label'] == group_lbl]
        color = colors[i % len(colors)]
        sensors = group_data['NodeNum'].unique()
        
        for j, sn in enumerate(sensors):
            s_df = group_data[group_data['NodeNum'] == sn].sort_values('timestamp')
            
            # Gap detection to prevent lines connecting across sensor outages
            if not is_surgical:
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
                legendgroup=group_lbl,  # Group by Location/Depth for cleaner legend
                showlegend=True if j == 0 else False, # Only show one legend entry per group
                mode='lines+markers' if not is_surgical else 'markers',
                connectgaps=False, 
                line=dict(color=color, width=1.5),
                marker=dict(size=4, opacity=0.8),
                hovertemplate=f"<b>{group_lbl} ({sn})</b>: %{{y:.1f}}{unit_label}<extra></extra>"
            ))

    # 4. CONDITIONAL LAYOUT LOGIC (Mobile vs PC)
    if mobile_mode:
        legend_cfg = dict(orientation="h", yanchor="top", y=-0.25, xanchor="center", x=0.5)
        margin_cfg = dict(t=80, l=40, r=20, b=160)
    else:
        legend_cfg = dict(orientation="v", x=1.02, y=1, xanchor="left", yanchor="top")
        margin_cfg = dict(t=80, l=50, r=160, b=50)

    fig.update_layout(
        title={'text': f"<b>{title}</b>", 'x': 0},
        plot_bgcolor='white', 
        hovermode="x unified", 
        height=600,
        margin=margin_cfg,
        legend=legend_cfg,
        xaxis=dict(
            range=[range_start, range_end], 
            showline=True, mirror=True, linecolor='black',
            showgrid=True, dtick="D1", gridcolor='DarkGray', gridwidth=1,
            minor=dict(dtick=6*60*60*1000, showgrid=True, gridcolor='Gainsboro', griddash='dash'),
            tickformat='%b %d\n%H:%M'
        ),
        yaxis=dict(
            title=f"Temperature ({unit_label})", 
            range=y_range, dtick=dt_major, 
            gridcolor='DarkGray', showline=True, mirror=True, linecolor='black',
            minor=dict(dtick=dt_minor, showgrid=True, gridcolor='whitesmoke')
        )
    )
    
    # 5. REFERENCE LINES & MONDAYS
    for val, ref_label in active_refs:
        c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
        fig.add_hline(y=c_val, line_dash="dash", line_color="RoyalBlue", 
                      annotation_text=ref_label, annotation_position="top right")
    
    fig.add_vline(x=now_local, line_width=2, line_color="Red", layer='above', line_dash="dash")

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
    Updated to use the relational node_registry structure.
    """
    st.header("🌐 Global Project Overview")
    
    # 1. FIX: Ensure variable names match the new sidebar keys
    # Use .get() to prevent crashes if the key hasn't been initialized yet
    mobile_mode = st.session_state.get("mobile_optimized_toggle", False)
    
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to begin.")
        return

    # Using the new engine we built in Section 2
    with st.spinner(f"Syncing {selected_project} (Engineering View)..."):
        p_df = get_universal_portal_data(selected_project, view_mode="engineering")

    if not p_df.empty:
        # 2. View Constraints
        # We keep the lookback slider here for fine-tuning the specific view
        lookback = st.sidebar.slider("Lookback (Weeks)", 1, 12, 4, key="global_lookback_slider")
        
        # Snap time window
        now_local = pd.Timestamp.now(tz=display_tz)
        
        # End view snaps to the upcoming Monday at midnight for clean weekly reporting
        end_view = (now_local + pd.Timedelta(days=(7-now_local.weekday())%7 or 7)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        start_view = end_view - timedelta(weeks=lookback)

        # 3. Render a graph for every physical location (Pipe/Bank)
        # Note: 'Location' now comes from node_registry
        locations = sorted(p_df['Location'].dropna().unique())
        
        for loc in locations:
            with st.expander(f"📍 Location: {loc}", expanded=True):
                loc_df = p_df[p_df['Location'] == loc]
                
                # The graphing engine now handles 'Bank' vs 'Depth' automatically
                fig = build_high_speed_graph(
                    df=loc_df, 
                    title=f"📈 {selected_project} - {loc}", 
                    start_view=start_view, 
                    end_view=end_view, 
                    active_refs=tuple(active_refs), 
                    unit_mode=unit_mode, 
                    unit_label=unit_label, 
                    display_tz=display_tz,
                    mobile_mode=mobile_mode 
                )
                
                st.plotly_chart(fig, use_container_width=True, key=f"ov_{selected_project}_{loc}")
    else:
        # This warning is now more helpful because it points to the specific new table
        st.warning(f"No engineering data found for '{selected_project}' in the registry.")
        st.info("Verify that your sensors are assigned to this project in the **Node Registry** (Admin Tools).")
###########
# - 6. PAGE: EXECUTIVE SUMMARY - #
###########

def render_executive_summary(client, selected_project, unit_label, display_tz):
    st.header(f"🏠 Executive Summary: Health Monitor")
    
    # Ensure the filter targets the node_registry 'n' alias used in the query below
    proj_filter = ""
    if selected_project and selected_project != "All Projects":
        proj_filter = f"AND n.Project = '{selected_project}'"

    # COMPREHENSIVE QUERY: Health Metrics + Temperature Extremes + Registry Integration
    query = f"""
        WITH MappedNodes AS (
            SELECT 
                n.NodeNum, 
                n.Project, 
                n.Location, 
                n.Bank, 
                n.Depth, 
                n.PhysicalID, -- Pulling this from node_registry now
                n.StartDate AS NodeStartDate, 
                n.EndDate AS NodeEndDate,
                p.ProjectName,
                p.Timezone
            FROM `sensorpush-export.Temperature.node_registry` AS n
            INNER JOIN `sensorpush-export.Temperature.project_registry` AS p ON n.Project = p.Project
            WHERE p.ProjectStatus = 'Active' 
            AND n.SensorStatus = 'Active'
            {proj_filter}
        ),
        BaseReporting AS (
            SELECT r.NodeNum, r.timestamp, r.temperature
            FROM (
                SELECT NodeNum, timestamp, temperature FROM `sensorpush-export.Temperature.raw_sensorpush`
                UNION ALL
                SELECT NodeNum, timestamp, temperature FROM `sensorpush-export.Temperature.raw_lord`
            ) AS r
            INNER JOIN MappedNodes m ON r.NodeNum = m.NodeNum
            -- RESPECT TENURE: Only show data for the window assigned to this project
            WHERE r.timestamp >= m.NodeStartDate
            AND (r.timestamp <= m.NodeEndDate OR m.NodeEndDate IS NULL)
        ),
        GapAnalysis AS (
            SELECT 
                NodeNum, timestamp, temperature,
                LAG(timestamp) OVER (PARTITION BY NodeNum ORDER BY timestamp) AS prev_ts
            FROM BaseReporting
        ),
        HistoricalStats AS (
            SELECT 
                NodeNum, 
                MAX(timestamp) AS last_ping,
                ARRAY_AGG(temperature ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS current_temp,
                -- Rolling 24h stats
                MIN(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature ELSE NULL END) AS low_24h,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature ELSE NULL END) AS high_24h,
                -- Connectivity stats (Gap logic)
                MAX(TIMESTAMP_DIFF(timestamp, prev_ts, HOUR)) AS gap_7d,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN TIMESTAMP_DIFF(timestamp, prev_ts, HOUR) ELSE 0 END) AS gap_24h,
                -- Active counters for % calculation
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN 1 ELSE 0 END) as active_24h,
                COUNT(DISTINCT CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN TIMESTAMP_TRUNC(timestamp, HOUR) END) as hours_24h,
                COUNT(DISTINCT CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) THEN TIMESTAMP_TRUNC(timestamp, HOUR) END) as hours_7d
            FROM GapAnalysis 
            GROUP BY NodeNum
        )
        SELECT 
            m.*, 
            h.last_ping, h.current_temp, h.low_24h, h.high_24h,
            COALESCE(h.gap_24h, 0) AS gap_24h, 
            COALESCE(h.gap_7d, 0) AS gap_7d,
            COALESCE(h.active_24h, 0) as active_24h,
            COALESCE(h.hours_24h, 0) as hours_24h,
            COALESCE(h.hours_7d, 0) as hours_7d
        FROM MappedNodes m
        LEFT JOIN HistoricalStats h ON m.NodeNum = h.NodeNum
    """
    
    try:
        raw_df = client.query(query).to_dataframe()
        if raw_df.empty:
            st.warning("No data found for this project in the registry.")
            return

        # Numeric Enforcement
        for col in ['Depth', 'low_24h', 'high_24h', 'current_temp', 'PhysicalID']:
            if col in raw_df.columns:
                raw_df[col] = pd.to_numeric(raw_df[col], errors='coerce')

        now_local = pd.Timestamp.now(tz=display_tz)

        def fmt_temp(val):
            if pd.isnull(val): return "N/A"
            c_val = (val - 32) * 5/9 if unit_label == "°C" else val
            return f"{round(c_val, 1)}{unit_label}"

        # 1. LOCATION OVERVIEW
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

            # Reliability % Calculation
            avg_24h = (row['Sum_Hrs_24'] / (row['Nodes'] * 24)) * 100
            avg_7d = (row['Sum_Hrs_7d'] / (row['Nodes'] * 168)) * 100

            return pd.Series({
                "Location": row['Location'], 
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
                
                # Dynamic Position Label
                pos = f"{row['Depth']}ft" if pd.notnull(row['Depth']) else (f"Bank {row['Bank']}" if pd.notnull(row['Bank']) else "N/A")

                return pd.Series({
                    "Node ID": row['NodeNum'], 
                    "Phys ID": int(row['PhysicalID']) if pd.notnull(row['PhysicalID']) else "N/A",
                    "Position": pos,
                    "Current Temp": fmt_temp(row['current_temp']), 
                    "Status": status_str,
                    "% Active (24h)": f"{round((row['hours_24h'] / 24) * 100, 1)}%", 
                    "Gap (24h)": f"{int(row['gap_24h'])}h"
                })
            st.dataframe(sensor_df.apply(format_sensor_row, axis=1), use_container_width=True, hide_index=True)
            
    except Exception as e:
        st.error(f"Executive Summary Error: {e}")
        
###########
# - 7. PAGE: CLIENT PORTAL - #
###########

def render_client_portal(selected_project, display_tz, unit_mode, unit_label, active_refs):
    st.header(f"📊 Project Status: {selected_project}")
    
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar.")
        return
    
    with st.spinner("Loading approved data..."):
        # Relational Engine handles the logic for only showing 'Approved' points
        p_df = get_universal_portal_data(selected_project, view_mode="client")
    
    if p_df.empty:
        st.warning(f"⚠️ No data marked as 'Approved' found for {selected_project}.")
        return

    tab_time, tab_depth, tab_table = st.tabs(["📈 Timeline Analysis", "📏 Depth Profile", "📋 Summary Table"])

    # --- TAB 1: TIMELINE ---
    with tab_time:
        weeks_view = st.sidebar.slider("Weeks to View", 1, 12, 6, key="client_weeks_slider")
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

    # --- TAB 2: DEPTH PROFILE ---
    with tab_depth:
        st.subheader("📏 Vertical Temperature Profile")
        
        # Consistent scale across sites for client confidence
        x_min_f, x_max_f, ref_f = -20, 60, 32.0
        if unit_label == "°C":
            x_min, x_max, ref_val = (x_min_f-32)*5/9, (x_max_f-32)*5/9, 0.0
        else:
            x_min, x_max, ref_val = x_min_f, x_max_f, ref_f

        # Force numeric depth for proper vertical axis scaling
        p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
        depth_only = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
        
        if depth_only.empty:
            st.info("This project does not currently have vertical depth sensors mapped.")
        else:
            for loc in sorted(depth_only['Location'].unique()):
                with st.expander(f"📏 {loc} Weekly Snapshots", expanded=False):
                    loc_data = depth_only[depth_only['Location'] == loc].copy()
                    fig_d = go.Figure()
                    
                    # Last 6 weeks at Monday 6 AM
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

    # --- TAB 3: SUMMARY TABLE ---
    with tab_table:
        # Get the latest approved point for each node
        latest = p_df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        
        latest['Current Temp'] = latest['temperature'].apply(
            lambda x: f"{round((x - 32) * 5/9 if unit_mode == 'Celsius' else x, 1)}{unit_label}"
        )
        
        # Improved Position Logic using the node_registry columns
        def get_position(r):
            if pd.notnull(r.get('Depth')): return f"{r['Depth']} ft"
            if pd.notnull(r.get('Bank')): return f"Bank {r['Bank']}"
            return "Surface/Ambient"

        latest['Position'] = latest.apply(get_position, axis=1)
        
        # Clean display for the client
        st.dataframe(
            latest[['Location', 'Position', 'Current Temp', 'NodeNum']].sort_values(['Location', 'Position']), 
            use_container_width=True, hide_index=True
        )
            
###########
# - 7. PAGE: NODE DIAGNOSTICS - #
###########

def render_node_diagnostics(selected_project, display_tz):
    st.header(f"📡 Real-Time Commissioning: {selected_project}")
    st.write("Live connectivity audit and data density check.")

    # High-Performance Diagnostic Query
    diag_q = f"""
        WITH RawData AS (
            SELECT NodeNum, timestamp, temperature 
            FROM `sensorpush-export.Temperature.raw_sensorpush`
            UNION ALL
            SELECT NodeNum, timestamp, temperature 
            FROM `sensorpush-export.Temperature.raw_lord`
        ),
        Stats AS (
            SELECT 
                NodeNum,
                MAX(timestamp) as last_ping,
                -- Get the absolute latest temperature reading
                ARRAY_AGG(temperature ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] as last_temp,
                -- Count check-ins in specific rolling windows
                COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) as count_1h,
                COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)) as count_6h
            FROM RawData
            GROUP BY NodeNum
        )
        SELECT 
            n.Location, 
            n.NodeNum, 
            n.Bank, 
            n.Depth,
            s.last_ping,
            s.last_temp,
            COALESCE(s.count_1h, 0) as count_1h,
            COALESCE(s.count_6h, 0) as count_6h
        FROM `sensorpush-export.Temperature.node_registry` n
        LEFT JOIN Stats s ON n.NodeNum = s.NodeNum
        WHERE n.Project = '{selected_project}' 
        AND n.SensorStatus = 'Active'
    """
    
    try:
        df = client.query(diag_q).to_dataframe()
        
        if df.empty:
            st.warning("No active sensors found for this project in the Node Registry.")
            return

        now = pd.Timestamp.now(tz='UTC')

        # Helper for Latency Category
        def get_latency_info(row):
            ping = row['last_ping']
            if pd.isnull(ping): 
                return "❌ Never", "Never Seen"
            
            if ping.tzinfo is None: ping = ping.tz_localize('UTC')
            diff_mins = (now - ping).total_seconds() / 60
            
            if diff_mins <= 15: cat = "🟢 0-15 Mins"
            elif diff_mins <= 30: cat = "🟡 15-30 Mins"
            elif diff_mins <= 60: cat = "🔴 45-60 Mins"
            else: cat = "⏳ > 1 Hour"
            
            return cat, f"{round(diff_mins/60, 1)}h ago"

        # Apply logic
        df[['Latency_Cat', 'Time_Ago']] = df.apply(lambda x: pd.Series(get_latency_info(x)), axis=1)
        
        # Format Temperatures
        def fmt_temp(val):
            if pd.isnull(val): return "N/A"
            c_val = (val - 32) * 5/9 if unit_label == "°C" else val
            return f"{round(c_val, 1)}{unit_label}"

        # Build final display table
        display_df = pd.DataFrame({
            "Location": df['Location'],
            "Node ID": df['NodeNum'],
            "Position": df.apply(lambda r: f"{r['Depth']}ft" if pd.notnull(r['Depth']) else f"Bank {r['Bank']}", axis=1),
            "Status": df['Latency_Cat'],
            "Last Seen": df['Time_Ago'],
            "Last Temp": df['last_temp'].apply(fmt_temp),
            "Pings (1h)": df['count_1h'],
            "Pings (6h)": df['count_6h']
        })

        # Sort by Status (Freshness) then Location
        order = ["🟢 0-15 Mins", "🟡 15-30 Mins", "🔴 45-60 Mins", "⏳ > 1 Hour", "Never Seen"]
        display_df['Status'] = pd.Categorical(display_df['Status'], categories=order, ordered=True)
        display_df = display_df.sort_values(['Status', 'Location'])

        # Display with conditional formatting
        st.dataframe(
            display_df, 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "Pings (1h)": st.column_config.NumberColumn(help="Target: 1 for SP, 60+ for Lord"),
                "Pings (6h)": st.column_config.NumberColumn(help="Check for sustained data density"),
            }
        )
        
    except Exception as e:
        st.error(f"Diagnostics Error: {e}")
    
###########
# - 9. PAGE: DATA INTAKE LAB - #
###########

def render_data_intake_page(selected_project):
    st.header("📤 Data Ingestion Lab")
    tab_upload, tab_export = st.tabs(["📄 Upload", "📥 Export"])
    
    with tab_upload:
        st.subheader("📄 Manual File Ingestion")
        st.info("Standardized Rule: Lord IDs use '-' (e.g., 58014-ch1). SensorPush IDs are numeric.")
        
        u_file = st.file_uploader("Upload Data File", type=['csv', 'xlsx'], key="manual_upload_main")
        
        if u_file is not None:
            try:
                # --- 1. DETECTION FOR SENSORCONNECT (WIDE) ---
                is_sensorconnect, skip_rows = False, 0
                if u_file.name.endswith('.csv'):
                    u_file.seek(0)
                    for i, line in enumerate(u_file):
                        if b"DATA_START" in line:
                            is_sensorconnect, skip_rows = True, i + 1
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
                    
                    # --- BRANCH A: SENSORCONNECT (Wide) ---
                    if is_sensorconnect:
                        time_col = [h for h in actual_headers if 'time' in h.lower()][0]
                        value_vars = [h for h in actual_headers if h != time_col]
                        df_melted = df_raw.melt(id_vars=[time_col], value_vars=value_vars, var_name='NodeNum', value_name='temperature')
                        df_processed['timestamp'] = pd.to_datetime(df_melted[time_col], format='mixed')
                        df_processed['NodeNum'] = df_melted['NodeNum'].str.strip().str.replace(':', '-')
                        df_processed['temperature'] = pd.to_numeric(df_melted['temperature'], errors='coerce')

                    # --- BRANCH B: LORD (Long/Narrow) ---
                    elif any(k in clean_headers for k in ['channel', 'node']) and any('time' in h for h in clean_headers):
                        st.info("Format Detected: Lord (Long)")
                        time_h = actual_headers[next(i for i, h in enumerate(clean_headers) if 'time' in h)]
                        node_h = actual_headers[next(i for i, h in enumerate(clean_headers) if 'channel' in h or 'node' in h)]
                        temp_h = [h for h in actual_headers if 'temp' in h.lower()][0]
                        df_processed['timestamp'] = pd.to_datetime(df_raw[time_h], format='mixed')
                        df_processed['NodeNum'] = df_raw[node_h].str.strip().str.replace(':', '-')
                        df_processed['temperature'] = pd.to_numeric(df_raw[temp_h], errors='coerce')

                    # --- BRANCH C: SENSORPUSH ---
                    else:
                        st.info("Format Detected: SensorPush")
                        t_match = [h for h in actual_headers if 'timestamp' in h.lower()][0]
                        v_match = [h for h in actual_headers if 'temp' in h.lower()][0]
                        # Extract Node ID from Filename (e.g., "1234.csv" -> "1234")
                        match = re.search(r'^([^ \(\.]+)', u_file.name)
                        df_processed['timestamp'] = pd.to_datetime(df_raw[t_match], format='mixed')
                        df_processed['temperature'] = pd.to_numeric(df_raw[v_match], errors='coerce')
                        df_processed['NodeNum'] = match.group(1) if match else "Unknown"

                    # --- 3. UPLOAD ---
                    if not df_processed.empty:
                        df_processed = df_processed.dropna(subset=['timestamp', 'temperature'])
                        st.success(f"✅ Ready: IDs: {', '.join(df_processed['NodeNum'].unique())}")
                        
                        target_table = "raw_lord" if ("-" in str(df_processed['NodeNum'].iloc[0])) else "raw_sensorpush"
                        
                        if st.button("🚀 Push to BigQuery"):
                            with st.spinner("Uploading..."):
                                table_id = f"sensorpush-export.Temperature.{target_table}"
                                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                                client.load_table_from_dataframe(df_processed, table_id, job_config=job_config).result()
                                st.success(f"Uploaded {len(df_processed)} rows to {target_table}!")
                                st.cache_data.clear()

            except Exception as e:
                st.error(f"Ingestion Error: {e}")

    with tab_export:
        st.subheader("📥 Export Project Data")
        if not selected_project or selected_project == "All Projects":
            st.warning("⚠️ Select a project in the sidebar first.")
        else:
            c1, c2 = st.columns(2)
            e_start = c1.date_input("Start Date", value=datetime.now() - timedelta(days=30))
            e_end = c2.date_input("End Date", value=datetime.now())
            
            export_scope = st.radio("Export Scope", ["Whole Project", "Specific Location"], horizontal=True)
            
            with st.spinner("Fetching engineering records..."):
                # Relational engine ensures we have correct Location mapping
                full_df = get_universal_portal_data(selected_project, view_mode="engineering")
            
            if not full_df.empty:
                if export_scope == "Specific Location":
                    target_loc = st.selectbox("Select Location", sorted(full_df['Location'].unique()))
                    full_df = full_df[full_df['Location'] == target_loc]

                if st.button("📦 Generate CSV"):
                    mask = (full_df['timestamp'].dt.date >= e_start) & (full_df['timestamp'].dt.date <= e_end)
                    export_df = full_df.loc[mask].copy()
                    
                    if export_df.empty:
                        st.warning("No data found for this date range.")
                    else:
                        export_df['timestamp'] = export_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                        csv = export_df.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="💾 Download CSV",
                            data=csv,
                            file_name=f"{selected_project}_Export.csv",
                            mime="text/csv"
                        )
###########
# - 10. PAGE: ADMIN TOOLS - #
###########

def render_admin_page(selected_project, display_tz, unit_mode, unit_label, active_refs):
    st.header("🛠️ Admin Tools")
    
    # 1. GLOBAL REGISTRY FETCH
    # This JOIN allows us to see project metadata while editing individual nodes
    reg_q = f"""
        SELECT 
            n.*, 
            p.ProjectName, p.City, p.Timezone, p.ProjectStatus as MasterProjectStatus
        FROM `sensorpush-export.Temperature.node_registry` n
        LEFT JOIN `sensorpush-export.Temperature.project_registry` p ON n.Project = p.Project
    """
    try:
        full_reg_df = client.query(reg_q).to_dataframe()
        if 'Depth' in full_reg_df.columns:
            full_reg_df['Depth'] = pd.to_numeric(full_reg_df['Depth'], errors='coerce')
        if 'PhysicalID' in full_reg_df.columns:
            full_reg_df['PhysicalID'] = pd.to_numeric(full_reg_df['PhysicalID'], errors='coerce')
    except Exception as e:
        st.error(f"Error joining registries: {e}")
        full_reg_df = pd.DataFrame()
    
    # Context for the selected project
    active_project_df = pd.DataFrame()
    if not full_reg_df.empty:
        active_project_df = full_reg_df[(full_reg_df['Project'] == selected_project) & (full_reg_df['EndDate'].isna())]
    
    loc_options = ["All Locations"] + sorted(active_project_df['Location'].unique().tolist()) if not active_project_df.empty else ["All Locations"]

    # --- 2. UNIFIED NAVIGATION ---
    (tab_bulk, tab_registry, tab_project, tab_scrub, tab_surgical, tab_audit) = st.tabs([
        "✅ Bulk Approval", "📋 Node Registry", "⚙️ Project Master", 
        "🧹 Scrub", "🧨 Surgical", "🕒 Audit"
    ])

    # --- TAB 1: BULK APPROVAL ---
    with tab_bulk:
        st.subheader("✅ Range-Based Bulk Approval")
        sel_loc = st.selectbox("Target Location", loc_options, key="bulk_loc_main")
        c1, c2 = st.columns(2)
        b_s = c1.date_input("Start", value=datetime.now()-timedelta(7))
        b_e = c2.date_input("End", value=datetime.now())
        
        if st.button("🚀 Execute Bulk Approval", use_container_width=True):
            # Target node_registry (n) to find which nodes belong to the selection
            loc_f = f"AND n.Location = '{sel_loc}'" if sel_loc != "All Locations" else ""
            sql = f"""
                INSERT INTO `{OVERRIDE_TABLE}` (NodeNum, timestamp, approve)
                SELECT DISTINCT r.NodeNum, TIMESTAMP_TRUNC(r.timestamp, HOUR), 'TRUE'
                FROM (SELECT NodeNum, timestamp FROM `sensorpush-export.Temperature.raw_sensorpush` UNION ALL SELECT NodeNum, timestamp FROM `sensorpush-export.Temperature.raw_lord`) AS r
                INNER JOIN `sensorpush-export.Temperature.node_registry` AS n ON r.NodeNum = n.NodeNum
                WHERE n.Project = '{selected_project}' {loc_f} 
                AND r.timestamp >= '{b_s}' AND r.timestamp <= '{b_e}'
                AND NOT EXISTS (SELECT 1 FROM `{OVERRIDE_TABLE}` x WHERE x.NodeNum = r.NodeNum AND x.timestamp = TIMESTAMP_TRUNC(r.timestamp, HOUR))
            """
            client.query(sql).result()
            st.success(f"Approved data for {sel_loc} in {selected_project}.")
            st.cache_data.clear()

    # --- TAB 2: NODE REGISTRY (HARDWARE ASSIGNMENTS) ---
    with tab_registry:
        st.subheader("📋 Hardware Assignment Manager")
        
        # 1. Filter the view for the registry
        with st.expander("🔍 Filter Hardware View", expanded=False):
            f1, f2 = st.columns(2)
            p_filter = f1.selectbox("View Project", ["All"] + sorted(full_reg_df['Project'].unique().tolist()))
            s_filter = f2.selectbox("View Status", ["All"] + sorted(full_reg_df['SensorStatus'].unique().tolist()))
            
            view_df = full_reg_df.copy()
            if p_filter != "All": view_df = view_df[view_df['Project'] == p_filter]
            if s_filter != "All": view_df = view_df[view_df['SensorStatus'] == s_filter]

        # 2. Spreadsheet Editor
        st.info("💡 Edit rows below and click 'Sync' to reassign sensors or update PhysicalIDs.")
        # Only these columns exist in the physical node_registry table
        node_cols = ['NodeNum', 'Project', 'Location', 'Bank', 'Depth', 'StartDate', 'EndDate', 'SensorStatus', 'PhysicalID']
        
        edited_df = st.data_editor(
            view_df[node_cols].sort_values(['Project', 'Location']), 
            num_rows="dynamic", 
            key="node_registry_editor"
        )
        
        if st.button("💾 Sync Registry Changes", type="primary", use_container_width=True):
            # WRITE_TRUNCATE replaces the table with the new assigned values
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            client.load_table_from_dataframe(edited_df, f"sensorpush-export.Temperature.node_registry", job_config=job_config).result()
            st.success("Node Registry synchronized successfully.")
            st.cache_data.clear()
            st.rerun()

    # --- TAB 3: PROJECT MASTER (SITE SETTINGS) ---
    with tab_project:
        st.subheader("⚙️ Project Management")
        p_mode = st.radio("Primary Action", ["Update Existing Project", "Initialize New Project"], horizontal=True)
        
        try:
            proj_reg_df = client.query(f"SELECT * FROM `sensorpush-export.Temperature.project_registry`").to_dataframe()
        except:
            proj_reg_df = pd.DataFrame()

        if p_mode == "Update Existing Project" and not proj_reg_df.empty:
            target_proj = st.selectbox("Select Project to Manage", sorted(proj_reg_df['Project'].unique().tolist()))
            p_data = proj_reg_df[proj_reg_df['Project'] == target_proj].iloc[0]

            with st.form("p_update_form"):
                c1, c2 = st.columns(2)
                u_name = c1.text_input("Project Name", value=p_data.get('ProjectName', ''))
                u_city = c2.text_input("City/Location", value=p_data.get('City', ''))
                
                u_upload = st.text_input("Upload Note", value=p_data.get('UploadNote', ''))
                u_eng = st.text_area("Engineering Notes", value=p_data.get('EngNotes', ''))
                
                if st.form_submit_button("💾 Save Project Settings"):
                    # Update ONLY the project_registry table
                    sql = f"""
                        UPDATE `sensorpush-export.Temperature.project_registry` 
                        SET ProjectName='{u_name}', City='{u_city}', UploadNote='{u_upload}', EngNotes='{u_eng}'
                        WHERE Project='{target_proj}'
                    """
                    client.query(sql).result()
                    st.success(f"Updated metadata for {target_proj}")
                    st.cache_data.clear()

        elif p_mode == "Initialize New Project":
            with st.form("p_init_form"):
                st.write("### 🏗️ New Project Registration")
                n_id = st.text_input("Project ID (e.g., 2329-Hudson)")
                n_name = st.text_input("Project Name")
                if st.form_submit_button("🚀 Initialize Project"):
                    sql = f"INSERT INTO `sensorpush-export.Temperature.project_registry` (Project, ProjectName, ProjectStatus, StartDate) VALUES ('{n_id}', '{n_name}', 'Active', CURRENT_TIMESTAMP())"
                    client.query(sql).result()
                    st.success(f"Project {n_id} created. Now assign sensors in the 'Node Registry' tab.")
                    st.cache_data.clear()

    # --- TAB 4: SCRUB ---
    with tab_scrub:
        st.subheader("🧹 Data Averaging (Cleanup)")
        target = st.radio("Target Table", ["SensorPush", "Lord"], horizontal=True)
        if st.button("🧨 Execute Hourly Averaging"):
            t_tab = f"sensorpush-export.Temperature.raw_{target.lower()}"
            # Subquery targets node_registry to find the relevant nodes
            sub_q = f"SELECT NodeNum FROM `sensorpush-export.Temperature.node_registry` WHERE Project = '{selected_project}'"
            sql = f"""
                CREATE OR REPLACE TABLE `{t_tab}` AS 
                SELECT TIMESTAMP_TRUNC(TIMESTAMP_ADD(timestamp, INTERVAL 30 MINUTE), HOUR) as timestamp, NodeNum, AVG(temperature) as temperature
                FROM `{t_tab}`
                WHERE NodeNum IN ({sub_q})
                GROUP BY 1, 2
            """
            client.query(sql).result()
            st.success(f"Scrubbed {target} data for {selected_project}.")
            st.cache_data.clear()

    # --- TAB 6: AUDIT ---
    with tab_audit:
        st.subheader("🕒 Registry Audit Log")
        st.dataframe(full_reg_df.sort_values('StartDate', ascending=False), use_container_width=True, hide_index=True)
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

    # UPDATED: Fetch from node_registry instead of project_registry
    reg_q = f"SELECT NodeNum, Location FROM `sensorpush-export.Temperature.node_registry` WHERE Project = '{selected_project}'"
    reg_df = client.query(reg_q).to_dataframe()
    
    target_node, target_loc = None, None
    if not reg_df.empty:
        if scope == "Specific Location":
            target_loc = st.selectbox("Select Location", sorted(reg_df['Location'].unique()))
        elif scope == "Specific Node":
            target_node = st.selectbox("Select Node ID", sorted(reg_df['NodeNum'].unique()))
    else:
        st.warning("No nodes found in registry for this project.")
        return

    # 2. TEMPORAL LOGIC
    st.divider()
    t_col1, t_col2 = st.columns([1, 2])
    direction = t_col1.selectbox("Temporal Direction", ["Between Range", "Everything Older Than", "Everything Newer Than"])
    
    with t_col2:
        if direction == "Between Range":
            sc1, sc2 = st.columns(2)
            s_dt = datetime.combine(sc1.date_input("Start Date", value=datetime.now() - timedelta(days=7)), dt_time(0,0))
            e_dt = datetime.combine(sc2.date_input("End Date", value=datetime.now()), dt_time(23,59))
        else:
            anchor_dt = datetime.combine(st.date_input("Anchor Date"), st.time_input("Anchor Time", value=dt_time(6,0)))
            s_dt = datetime(2000, 1, 1) if direction == "Everything Older Than" else anchor_dt
            e_dt = anchor_dt if direction == "Everything Older Than" else datetime(2100, 1, 1)

    # 3. THRESHOLD
    thr_col1, thr_col2 = st.columns([1, 2])
    operator = thr_col1.selectbox("Value Filter", ["No Threshold", "Greater Than (>)", "Less Than (<)"])
    thresh_val = thr_col2.number_input(f"Threshold Value ({unit_label})", value=100.0)
    thresh_val_f = (thresh_val * 9/5) + 32 if unit_mode == "Celsius" else thresh_val

    # 4. SQL LOGIC CONSTRUCTION (Targeting node_registry 'n')
    if scope == "Project Wide":
        where_clause = f"n.Project = '{selected_project}'"
        sub_where = f"Project = '{selected_project}'"
    elif scope == "Specific Location":
        where_clause = f"n.Project = '{selected_project}' AND n.Location = '{target_loc}'"
        sub_where = f"Project = '{selected_project}' AND Location = '{target_loc}'"
    else:
        where_clause = f"r.NodeNum = '{target_node}'"
        sub_where = f"NodeNum = '{target_node}'"

    threshold_clause = ""
    if operator == "Greater Than (>)": threshold_clause = f"AND r.temperature > {thresh_val_f}"
    elif operator == "Less Than (<)": threshold_clause = f"AND r.temperature < {thresh_val_f}"

    s_str, e_str = s_dt.strftime('%Y-%m-%d %H:%M:%S'), e_dt.strftime('%Y-%m-%d %H:%M:%S')

    # 5. EXECUTION GATE
    st.divider()
    if st.button("🔍 Step 1: Verify Match Count", use_container_width=True):
        status_q = f"""
            SELECT COALESCE(rej.approve, 'PENDING') as status, COUNT(*) as point_count
            FROM (
                SELECT NodeNum, timestamp, temperature FROM `sensorpush-export.Temperature.raw_sensorpush` 
                UNION ALL 
                SELECT NodeNum, timestamp, temperature FROM `sensorpush-export.Temperature.raw_lord`
            ) AS r
            INNER JOIN `sensorpush-export.Temperature.node_registry` AS n ON r.NodeNum = n.NodeNum
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
                    sql = f"""
                        MERGE `{OVERRIDE_TABLE}` T
                        USING (
                            SELECT DISTINCT r.NodeNum, TIMESTAMP_TRUNC(r.timestamp, HOUR) as ts
                            FROM (SELECT NodeNum, timestamp, temperature FROM `sensorpush-export.Temperature.raw_sensorpush` UNION ALL SELECT NodeNum, timestamp, temperature FROM `sensorpush-export.Temperature.raw_lord`) AS r
                            INNER JOIN `sensorpush-export.Temperature.node_registry` AS n ON r.NodeNum = n.NodeNum
                            WHERE {where_clause} {threshold_clause} AND r.timestamp BETWEEN '{s_str}' AND '{e_str}'
                        ) S ON T.NodeNum = S.NodeNum AND T.timestamp = S.ts
                        WHEN MATCHED THEN UPDATE SET approve = 'MASKED'
                        WHEN NOT MATCHED THEN INSERT (NodeNum, timestamp, approve) VALUES (S.NodeNum, S.ts, 'MASKED')
                    """
                else:
                    # UPDATED: Hard delete logic targeting new node_registry structure
                    sql = f"""
                        BEGIN TRANSACTION;
                        DELETE FROM `sensorpush-export.Temperature.raw_sensorpush` r 
                        WHERE EXISTS (SELECT 1 FROM `sensorpush-export.Temperature.node_registry` n WHERE r.NodeNum = n.NodeNum AND {where_clause}) 
                        {threshold_clause} AND r.timestamp BETWEEN '{s_str}' AND '{e_str}';
                        
                        DELETE FROM `sensorpush-export.Temperature.raw_lord` r 
                        WHERE EXISTS (SELECT 1 FROM `sensorpush-export.Temperature.node_registry` n WHERE r.NodeNum = n.NodeNum AND {where_clause}) 
                        {threshold_clause} AND r.timestamp BETWEEN '{s_str}' AND '{e_str}';
                        
                        DELETE FROM `{OVERRIDE_TABLE}` 
                        WHERE NodeNum IN (SELECT NodeNum FROM `sensorpush-export.Temperature.node_registry` WHERE {sub_where}) 
                        AND timestamp BETWEEN '{s_str}' AND '{e_str}';
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
    Aligned with the new sensorpush-export.Temperature schema.
    """
    recs = []
    for p in pts:
        try:
            # 1. Capture the timestamp from the click event
            # Use tz_localize if the graph data is naive, or convert to UTC
            ts_raw = pd.to_datetime(p['x'])
            if ts_raw.tzinfo is None:
                ts = ts_raw.tz_localize(display_tz).tz_convert('UTC').floor('h')
            else:
                ts = ts_raw.tz_convert('UTC').floor('h')
            
            # 2. Grab the NodeNum directly from the dataframe row that was clicked
            node = df.iloc[p['point_index']]['NodeNum']
            
            recs.append({
                "NodeNum": str(node), 
                "timestamp": ts, 
                "approve": val 
            })
        except Exception:
            continue
    
    if recs:
        # 3. Deduplicate to avoid PK violations in the rejection table
        status_df = pd.DataFrame(recs).drop_duplicates(subset=['NodeNum', 'timestamp'])
        
        try:
            # 4. APPEND the new status to the manual_rejections table
            # OVERRIDE_TABLE = "sensorpush-export.Temperature.manual_rejections"
            job = client.load_table_from_dataframe(status_df, OVERRIDE_TABLE)
            job.result() 
            
            # 5. UI Feedback & Cache Clearing
            st.session_state.locked_selection = []
            st.cache_data.clear() # Forces graphs to redraw with the new MASK/HIDE status
            st.success(f"✅ Successfully marked {len(status_df)} records as {val}")
            
            # Brief pause so the user sees the success message before rerun
            time.sleep(0.5) 
            st.rerun()
        except Exception as e:
            st.error(f"Failed to update database: {e}")

###########
# - 12. PAGE: DEPTH CHARTS - #
###########

def render_depth_charts(selected_project, unit_label, display_tz):
    st.header(f"📏 Weekly Depth Profiles: {selected_project}")
    st.write("Vertical snapshots captured every Monday at 6:00 AM (Project Time).")
    
    # 1. FETCH DATA (Uses the 3-way join engine)
    with st.spinner("Analyzing vertical profiles..."):
        df = get_universal_portal_data(selected_project, view_mode="engineering")
    
    if df.empty:
        st.warning("No data found for this project in the registry.")
        return

    # 2. FILTER & SORT FOR DEPTH
    # We force numeric conversion here just in case the BigQuery column contains strings
    df['Depth_Num'] = pd.to_numeric(df['Depth'], errors='coerce')
    depth_only = df.dropna(subset=['Depth_Num', 'Location']).copy()
    
    if depth_only.empty:
        st.info("No sensors with valid depth assignments (ft) found in the registry.")
        return

    # 3. AXIS CONFIGURATION
    # Standard engineering range: -20 to 60°F
    x_min_f, x_max_f, ref_f = -20, 60, 32.0
    if unit_label == "°C":
        x_min, x_max, ref_val = (x_min_f-32)*5/9, (x_max_f-32)*5/9, 0.0
    else:
        x_min, x_max, ref_val = x_min_f, x_max_f, ref_f

    locations = sorted(depth_only['Location'].unique())
    
    for loc in locations:
        with st.expander(f"📍 Location: {loc}", expanded=True):
            loc_data = depth_only[depth_only['Location'] == loc].copy()
            fig_d = go.Figure()
            
            # 4. GENERATE SNAPSHOTS (Last 6 Mondays)
            # freq='W-MON' ensures we are looking at specific reporting intervals
            mondays = pd.date_range(end=pd.Timestamp.now(tz='UTC'), periods=6, freq='W-MON')
            
            for m_date in mondays:
                # Snap to 6:00 AM UTC (or convert to local project time if preferred)
                target_ts = m_date.replace(hour=6, minute=0, second=0)
                
                # We allow a +/- 12 hour window to find the closest reading to the snapshot target
                window = loc_data[(loc_data['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & 
                                 (loc_data['timestamp'] <= target_ts + pd.Timedelta(hours=12))]
                
                if not window.empty:
                    # Find the single point closest to the 6 AM target for each Node
                    snap_df = (
                        window.assign(diff=(window['timestamp'] - target_ts).abs())
                        .sort_values(['NodeNum', 'diff'])
                        .drop_duplicates('NodeNum')
                        .sort_values('Depth_Num')
                    )
                    
                    # Convert values based on unit selection
                    conv_temps = snap_df['temperature'].apply(
                        lambda x: (x - 32) * 5/9 if unit_label == "°C" else x
                    )
                    
                    fig_d.add_trace(go.Scatter(
                        x=conv_temps, 
                        y=snap_df['Depth_Num'], 
                        mode='lines+markers', 
                        name=target_ts.strftime('%b %d'),
                        line=dict(shape='spline', smoothing=0.5), # Smooth curves for soil profile
                        hovertemplate=f"Date: {target_ts.strftime('%Y-%m-%d')}<br>Depth: %{{y}}ft<br>Temp: %{{x:.1f}}{unit_label}<extra></extra>"
                    ))

            # 5. REFERENCE LINES
            fig_d.add_vline(x=ref_val, line_dash="dash", line_color="RoyalBlue", 
                            annotation_text="Freezing", annotation_position="top right")

            # Calculate dynamic Y-axis limit (rounded up to nearest 10ft)
            y_limit = int(((loc_data['Depth_Num'].max() // 10) + 1) * 10) if not loc_data.empty else 50
            
            # 6. STYLING: Inverted Y-axis is critical here (Surface = 0)
            fig_d.update_layout(
                plot_bgcolor='white', 
                height=750,
                xaxis=dict(
                    title=f"Temperature ({unit_label})", 
                    gridcolor='Gainsboro', 
                    range=[x_min, x_max]
                ),
                yaxis=dict(
                    title="Depth (ft) below Surface", 
                    range=[y_limit, 0], # INVERTED: Deepest at bottom
                    dtick=10, 
                    gridcolor='Silver'
                ),
                legend=dict(title="Weekly Snapshots", orientation="h", y=-0.15),
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
