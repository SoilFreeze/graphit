import streamlit as st
import pandas as pd
import time
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import re

# =============================================================================
# 1. INITIALIZATION & GLOBAL CONSTANTS
# =============================================================================
st.set_page_config(
    page_title="SoilFreeze Data Lab", 
    page_icon="❄️", 
    layout="wide"
)

DATASET_ID = "Temperature" 
PROJECT_ID = "sensorpush-export"
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"

# Production Google Sheets / Federated Tables
PROJECT_REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.project_registry"
NODE_REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.node_registry"
CHILLER_EVENTS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.chiller_events"

@st.cache_resource
def get_bq_client():
    """Initializes and caches the authenticated BigQuery connection client."""
    try:
        SCOPES = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive"]
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
            return bigquery.Client(credentials=credentials, project=info["project_id"])
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"❌ BigQuery Authentication Failed: {e}")
        return None

# =============================================================================
# 2. CORE ENGINE DATA RETRIEVAL LOGIC
# =============================================================================
@st.cache_data(ttl=300)
def get_universal_portal_data(project_id, lookback_days):
    """Streams and filters real-time engineering records from master view dataset."""
    client = get_bq_client()
    if client is None: return pd.DataFrame()

    is_office = "OFFICE" in str(project_id).upper()
    filter_sql = (
        "AND UPPER(COALESCE(CAST(m.approval_status AS STRING), 'PENDING')) != 'BADDATA'"
        if is_office else
        "AND UPPER(COALESCE(CAST(m.approval_status AS STRING), 'PENDING')) NOT IN ('BADDATA', 'FALSE', '0')"
    )

    query = f"""
        SELECT m.* FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
        JOIN `{PROJECT_REGISTRY_TABLE}` p ON m.Project = p.Project
        WHERE m.Project = @project_id
          AND m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
          {filter_sql}
        ORDER BY m.timestamp ASC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("project_id", "STRING", project_id),
            bigquery.ScalarQueryParameter("days", "INT64", lookback_days)
        ]
    )
    try:
        return client.query(query, job_config=job_config).to_dataframe()
    except Exception as e:
        st.error(f"⚠️ Data Sync Error: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_chiller_events_overlay(project_id):
    """Pulls recorded chiller event windows to overlay into spatial trend graphs."""
    client = get_bq_client()
    if client is None: return pd.DataFrame()
    
    base_job_num = str(project_id).split('-')[0].strip()
    query = f"""
        SELECT Chiller, TypeofEvent, Notes, PowerSource,
               CAST(StartDate AS TIMESTAMP) as event_start,
               CASE WHEN EndDate IS NULL THEN CURRENT_TIMESTAMP() ELSE CAST(EndDate AS TIMESTAMP) END as event_end
        FROM `{CHILLER_EVENTS_TABLE}`
        WHERE (CAST(Project AS STRING) = @project_id OR CAST(Project AS STRING) = '{base_job_num}' OR CAST(Project AS STRING) LIKE '{base_job_num}%')
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("project_id", "STRING", project_id)])
    try:
        return client.query(query, job_config=job_config).to_dataframe()
    except Exception:
        return pd.DataFrame()

# =============================================================================
# 3. SIDEBAR NAVIGATION & DYNAMIC CONTROLS
# =============================================================================
st.sidebar.title("❄️ SoilFreeze Lab")
page = st.sidebar.selectbox("Navigation", ["Summary", "Time vs Temp", "Depth Charts", "Sensor Status", "Node Diagnostics", "Data Processing", "Admin Tools"], key="nav_page")
st.sidebar.divider()

selected_project = "All Projects"
project_metadata = None  
sidebar_client = get_bq_client()

if sidebar_client is not None:
    try:
        proj_q = f"""
            SELECT CAST(Project AS STRING) as Project, ProjectName, Timezone, ProjectStatus, Date_Freezedown 
            FROM `{PROJECT_REGISTRY_TABLE}` 
            WHERE Project IS NOT NULL AND TRIM(CAST(Project AS STRING)) != ''
              AND (ProjectStatus != 'Archived' OR UPPER(CAST(Project AS STRING)) LIKE '%OFFICE%')
        """
        proj_df = sidebar_client.query(proj_q).to_dataframe()
        proj_list = sorted([str(p).strip() for p in proj_df['Project'].unique() if p and str(p).strip().lower() not in ['none', 'nan', 'null', '']])
        
        selected_project = st.sidebar.selectbox("🎯 Active Project", ["All Projects"] + proj_list, key="sidebar_proj_picker_global")
        st.session_state['selected_project'] = selected_project
        
        if selected_project != "All Projects":
            meta_row = proj_df[proj_df['Project'] == selected_project]
            if not meta_row.empty:
                project_metadata = meta_row.iloc[0].to_dict()
                st.session_state['project_metadata'] = project_metadata
        else:
            st.session_state['project_metadata'] = None
    except Exception as e:
        st.sidebar.error(f"Registry Link Offline: {e}")

# Live Sync Ages Engine
st.sidebar.subheader("⏱️ Current Data Ages")
if sidebar_client is not None:
    try:
        where_clause = "" if selected_project == "All Projects" else f"WHERE Project = '{selected_project}'"
        scope_label = "Last Data" if selected_project == "All Projects" else f"Job {selected_project.split('-')[0]} Age"
        
        pulse_df = sidebar_client.query(f"SELECT FORMAT_TIMESTAMP('%m/%d/%Y %H:%M UTC', MAX(timestamp)) as last_sync FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` {where_clause}").to_dataframe()
        
        if not pulse_df.empty and pulse_df['last_sync'].iloc[0]:
            last_sync_ts = pd.to_datetime(str(pulse_df['last_sync'].iloc[0]), utc=True)
            elapsed_mins = int((pd.Timestamp.now(tz='UTC') - last_sync_ts).total_seconds() / 60)
            status_tag = "🟢 **Live** " if elapsed_mins <= 60 else "🟠 **Delayed** " if elapsed_mins <= 180 else "🔴 **Stale** "
            st.sidebar.markdown(f"**{scope_label}:** {status_tag}({elapsed_mins}m ago)")
        else:
            st.sidebar.markdown(f"**{scope_label}:** ❌ No Sync Records")
    except Exception as err:
        st.sidebar.caption(f"Pulse tracking suspended: {err}")

if st.sidebar.button("🔄 Refresh Data", use_container_width=True):
    st.cache_data.clear()
    st.toast("System cache completely cleared!", icon="🔄")
    time.sleep(0.5)
    st.rerun()

st.sidebar.divider()
st.sidebar.subheader("👁️ Visibility Toggles & Timeline")
st.sidebar.toggle("Show Theoretical Curves", value=True, key="global_show_ref")
st.sidebar.toggle("Show Masked Data", value=False, key="global_show_masked")
st.sidebar.toggle("Mobile Layout", value=False, key="mobile_optimized_toggle")

selected_weeks = st.sidebar.slider("Select History Window (Weeks)", min_value=1, max_value=12, value=5, step=1, key="global_lookback_weeks_slider")
lookback_days = selected_weeks * 7
st.session_state["global_lookback_days"] = lookback_days

# CSS Custom Theme Injectors (Red Accent theme sync)
st.sidebar.markdown("<style>div[data-baseweb=\"slider\"] > div > div { background: linear-gradient(to right, rgb(214, 39, 40) 0%, rgb(214, 39, 40) var(--slider-progress, 100%), rgb(230, 230, 230) var(--slider-progress, 100%)) !important; } div[role=\"slider\"] { background-color: rgb(214, 39, 40) !important; border: 2px solid rgb(214, 39, 40) !important; } div[data-testid=\"stDataFrame\"] div[role=\"progressbar\"] > div { background-color: rgb(214, 39, 40) !important; }</style>", unsafe_allow_html=True)

unit_mode = st.sidebar.radio("Temperature Scale", ["Fahrenheit", "Celsius"], horizontal=True, key="unit_toggle")
unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"
st.session_state["unit_mode"], st.session_state["unit_label"] = unit_mode, unit_label

tz_mode = st.sidebar.selectbox("Timezone Display", ["UTC", "Local (US/Eastern)", "Local (US/Pacific)"], index=2, key="tz_picker")
st.session_state["display_tz"] = {"UTC": "UTC", "Local (US/Eastern)": "US/Eastern", "Local (US/Pacific)": "US/Pacific"}[tz_mode]

# =============================================================================
# 4. HIGH SPEED TREND PLOTTING GRAPH LAYER
# =============================================================================
def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', str(s))]

def build_high_speed_graph(df, title, start_view, end_view, unit_mode, unit_label, display_tz="UTC", f_start_date=None, curve_id=None, events_df=pd.DataFrame()):
    """High-performance trend graphing system supporting real-time operational event overlays."""
    if df.empty: return go.Figure().update_layout(title="No data available")
    
    client = get_bq_client()
    plot_df = df.copy() 
    if plot_df['timestamp'].dt.tz is None: plot_df['timestamp'] = plot_df['timestamp'].dt.tz_localize('UTC')
    plot_df['timestamp'] = plot_df['timestamp'].dt.tz_convert(display_tz)
    
    freeze_pt = 0 if unit_mode == "Celsius" else 32
    y_range = [-30, 30] if unit_mode == "Celsius" else [-20, 80]
    fig = go.Figure()

    # A. CHILLER EVENT SHADING INTEGRATION
    if not events_df.empty:
        events_df['event_start'] = pd.to_datetime(events_df['event_start']).dt.tz_convert(display_tz)
        events_df['event_end'] = pd.to_datetime(events_df['event_end']).dt.tz_convert(display_tz)
        
        for _, ev in events_df.iterrows():
            fig.add_vrect(
                x0=ev['event_start'], x1=ev['event_end'],
                fillcolor="rgba(214, 39, 40, 0.12)", layer="below", line_width=0,
                annotation_text=f"🚨 {ev['Chiller']}: {ev['TypeofEvent']}",
                annotation_position="top left", annotation_font=dict(size=10, color="darkred")
            )

    # B. THEORETICAL TARGET REF CURVES
    if curve_id and curve_id != "None" and f_start_date and client is not None:
        try:
            proj_num = re.findall(r'\d+', str(st.session_state.get('selected_project', '')))[0]
            loc_part = str(curve_id).split('-')[-1].strip()
            
            target_df = client.query(f"SELECT CurveID, Day, Temp FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` WHERE REGEXP_CONTAINS(CurveID, r'^{proj_num}.*{loc_part}$') ORDER BY Day").to_dataframe()
            if not target_df.empty:
                dash_styles, gray_shades = ['dashdot', 'dash', 'dot'], ['rgba(30,30,30,0.8)', 'rgba(70,70,70,0.75)', 'rgba(110,110,110,0.7)']
                for c_idx, (cid, c_df) in enumerate(target_df.groupby('CurveID')):
                    c_df['timestamp'] = c_df['Day'].apply(lambda d: pd.Timestamp(f_start_date) + pd.Timedelta(days=d))
                    c_df['timestamp'] = c_df['timestamp'].dt.tz_localize('UTC').dt.tz_convert(display_tz)
                    ref_y = c_df['Temp'] if unit_mode == "Fahrenheit" else (c_df['Temp'] - 32) * 5/9
                    label_clean = str(cid).replace(f"{proj_num}-", "").replace(f"-{loc_part}", "")
                    
                    fig.add_trace(go.Scatter(
                        x=c_df['timestamp'], y=ref_y, name=f"Goal: {label_clean if label_clean != loc_part else loc_part}", mode='lines',
                        line=dict(color=gray_shades[c_idx % 3], width=3, dash=dash_styles[c_idx % 3], shape='spline', smoothing=1.3)
                    ))
        except Exception: pass

    # C. RE-SAMPLED SENSOR PLOTS
    sf_15_palette = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf', '#FF1493', '#00CED1', '#FFD700', '#8A2BE2', '#32CD32']
    node_metadata = []
    
    for sn in plot_df['NodeNum'].unique():
        n_df = plot_df[plot_df['NodeNum'] == sn]
        d_v, b_v, l_v = n_df['Depth'].iloc[0], n_df['Bank'].iloc[0], n_df['Location'].iloc[0]
        lbl = f"{b_v} ({sn})" if pd.notnull(b_v) and any(x in str(b_v).upper() for x in ['S', 'R']) else f"{d_v}ft ({sn})" if pd.notnull(d_v) and not pd.isna(d_v) else f"{l_v} ({sn})"
        sort_v = str(b_v) if pd.notnull(b_v) and any(x in str(b_v).upper() for x in ['S', 'R']) else f"depth_{float(d_v):05.1f}" if pd.notnull(d_v) and not pd.isna(d_v) else str(lbl)
        node_metadata.append({'node_num': sn, 'display_name': lbl, 'sort_key': sort_v})

    for i, cfg in enumerate(sorted(node_metadata, key=lambda x: natural_sort_key(x['sort_key']))):
        sn, d_name = cfg['node_num'], cfg['display_name']
        s_df = plot_df[plot_df['NodeNum'] == sn].sort_values('timestamp').set_index('timestamp').resample('1h').first().reset_index()
        
        fig.add_trace(go.Scatter(
            x=s_df['timestamp'], y=s_df['temperature'], name=d_name, mode='lines', connectgaps=False, 
            line=dict(shape='spline', smoothing=1.3, width=2, color=sf_15_palette[i % 15]),
            hovertemplate="<b>%{fullData.name}</b><br>Time: %{x|%H:%M}<br>Temp: %{y:.1f}" + unit_label + "<extra></extra>"
        ))

    # Reference Baselines
    fig.add_hline(y=freeze_pt, line_width=1.5, line_dash="dash", line_color="RoyalBlue", annotation_text="32°F FREEZE")
    fig.add_vline(x=pd.Timestamp.now(tz=display_tz).to_pydatetime(), line_width=1.5, line_color="red", line_dash="dash")
    for m_dt in pd.date_range(start=start_view, end=end_view, freq='W-MON'):
        fig.add_vline(x=m_dt, line_width=1, line_color="black", opacity=0.3)

    fig.update_layout(
        title=dict(text=f"<b>{st.session_state.get('selected_project', 'Project')} - Thermal Trend - {title}</b>", x=0.02, y=0.98, font=dict(size=18)),
        plot_bgcolor='white', hovermode="x unified", height=650,
        xaxis=dict(range=[start_view, end_view], showgrid=True, gridcolor='Gainsboro', showline=True, mirror=True, linecolor='black', tickformat='%b %d'),
        yaxis=dict(title=f"Temperature ({unit_label})", range=y_range, dtick=10, showgrid=True, gridcolor='Gainsboro', showline=True, mirror=True, linecolor='black'),
        legend=dict(orientation="v", x=1.02, y=1, xanchor="left", yanchor="top")
    )
    return fig
# =============================================================================
# 5. DATA CLEANING & REACTION MASK FILTER
# =============================================================================
def apply_sanity_filter(df):
    """Automated filter for dropping raw device outliers outside absolute safety thresholds."""
    if df.empty: return df
    
    # Flag out-of-bounds readings outside the physical limits of ground-freezing physics [-30°F, 120°F]
    bad_condition = (df['temperature'] > 120) | (df['temperature'] < -30)
    target_col = 'approval_status' if 'approval_status' in df.columns else 'approve'
    
    if target_col in df.columns:
        df.loc[bad_condition, target_col] = 'BADDATA'
    return df

# =============================================================================
# WORKSPACE PAGE 1: GLOBAL DASHBOARD SUMMARY
# =============================================================================
def render_summary_dashboard(unit_label, unit_mode, display_tz):
    """
    Renders high-level project multi-column status metrics and active counters.
    Fixed: Uses LEFT JOIN on node registry to prevent blank project views when nodes are streaming.
    """
    st.header("🌐 Global Project Summary")
    client = get_bq_client()
    if client is None: return

    mobile_mode = st.session_state.get("mobile_optimized_toggle", False)

    summary_q = f"""
        WITH active_projects AS (
            SELECT CAST(Project AS STRING) as Project, ProjectName, ProjectStatus, Date_Freezedown
            FROM `{PROJECT_REGISTRY_TABLE}`
            WHERE ProjectStatus IN ('Freezedown', 'Maintenance', 'Pre-freeze')
              AND UPPER(CAST(Project AS STRING)) NOT LIKE '%OFFICE%'
        ),
        raw_data AS (
            SELECT 
                COALESCE(CAST(n.Project AS STRING), CAST(m.Project AS STRING)) as Project, 
                COALESCE(n.Bank, '') as Bank, 
                COALESCE(n.Location, '') as Location, 
                n.Depth, 
                m.temperature, 
                m.timestamp, 
                m.NodeNum
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
            LEFT JOIN `{NODE_REGISTRY_TABLE}` n ON TRIM(CAST(m.NodeNum AS STRING)) = TRIM(CAST(n.NodeNum AS STRING))
            WHERE m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
              AND UPPER(COALESCE(CAST(m.approval_status AS STRING), 'PENDING')) NOT IN ('BADDATA', 'FALSE', '0')
              AND NOT (m.temperature > 100 AND NOT STARTS_WITH(m.NodeNum, 'SP'))
        ),
        MaxTime AS (
            SELECT MAX(timestamp) as max_ts FROM raw_data
        ),
        LatestStats AS (
            SELECT 
                r.Project, r.Bank, r.Location, r.Depth, r.NodeNum,
                AVG(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR) THEN r.temperature END) as avg_now,
                MIN(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR) THEN r.temperature END) as min_now,
                MAX(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR) THEN r.temperature END) as max_now,
                MIN(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR) THEN r.temperature END) as min_24h,
                MAX(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR) THEN r.temperature END) as max_24h,
                COUNTIF(r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR)) as checkins_1h,
                COUNTIF(r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR)) as checkins_24h,
                ARRAY_AGG(r.temperature ORDER BY r.timestamp DESC LIMIT 1)[OFFSET(0)] as latest_temp
            FROM raw_data r 
            CROSS JOIN MaxTime m 
            GROUP BY 1, 2, 3, 4, 5
        )
        SELECT 
            p.Project as ProjectID,
            p.ProjectName,
            p.ProjectStatus,
            p.Date_Freezedown,
            ls.*,
            (COUNTIF(ls.Bank LIKE 'S%' AND ls.latest_temp <= -10) OVER(PARTITION BY p.Project) / NULLIF(COUNTIF(ls.Bank LIKE 'S%') OVER(PARTITION BY p.Project), 0)) * 100 as supply_kpi,
            (COUNTIF(ls.Bank LIKE 'R%' AND ls.latest_temp <= 0) OVER(PARTITION BY p.Project) / NULLIF(COUNTIF(ls.Bank LIKE 'R%') OVER(PARTITION BY p.Project), 0)) * 100 as return_kpi,
            (COUNTIF(ls.Depth IS NOT NULL AND ls.latest_temp <= 32) OVER(PARTITION BY p.Project) / NULLIF(COUNTIF(ls.Depth IS NOT NULL) OVER(PARTITION BY p.Project), 0)) * 100 as freeze_kpi
        FROM active_projects p 
        LEFT JOIN LatestStats ls ON p.Project = ls.Project
    """
    try:
        df = client.query(summary_q).to_dataframe()
        df['Bank'] = df['Bank'].fillna('')
        df['Location'] = df['Location'].fillna('')
    except Exception as e:
        st.error(f"Dashboard Query Failed: {e}")
        return

    if df.empty:
        st.info("No active projects found with streaming data profiles.")
        return

    # Loop over unique verified project profiles found in the registry tables
    for project in sorted(df['ProjectID'].unique()):
        p_df = df[df['ProjectID'] == project]
        p_name = p_df['ProjectName'].iloc[0] or project
        f_date = p_df['Date_Freezedown'].iloc[0]
        
        day_text, f_date_display = "", "Not Set"
        if pd.notnull(f_date):
            f_date_display = pd.to_datetime(f_date).strftime('%b %d, %Y')
            day_text = f"🗓️ **Day {max(0, (pd.Timestamp.now(tz=display_tz).date() - pd.to_datetime(f_date).date()).days)}**"
        
        with st.container(border=True):
            h1, h2 = st.columns([2, 1])
            h1.subheader(f"🏗️ {p_name}")
            h2.markdown(f"<div style='text-align: right;'>{day_text}<br><small>Start: {f_date_display}</small></div>", unsafe_allow_html=True)
            
            proj_match = re.search(r'\b(\d{4})\b', str(project))
            if proj_match:
                st.markdown(f"🔗 **External Client Portal:** [{p_name} Portal Site Link](https://sf{proj_match.group(1)}.streamlit.app)")
            
            # Recalculate node counters precisely out of selection pools
            act_1h = p_df[p_df['checkins_1h'] > 0]['NodeNum'].nunique()
            act_24h = p_df[p_df['checkins_24h'] > 0]['NodeNum'].nunique()
            tot_nodes = p_df['NodeNum'].dropna().nunique()
            
            st.markdown(f"📡 **Hardware Status:** `{act_1h}` nodes pinged in the last hour | `{act_24h}` nodes pinged in the last 24h (Total Pool: `{tot_nodes}` registered)")
            st.divider() 

            # Spatial Group Splitting
            is_amb = p_df['Bank'].str.contains('Amb', case=False) | p_df['Location'].str.contains('Amb', case=False)
            is_s = (p_df['Bank'].str.startswith('S') | p_df['Location'].str.startswith('S')) & ~is_amb
            is_r = (p_df['Bank'].str.startswith('R') | p_df['Location'].str.startswith('R')) & ~is_amb
            is_tp = p_df['Depth'].notnull() & ~is_s & ~is_r & ~is_amb

            groups_data = [
                ("📥 Supply", p_df[is_s], "supply_kpi", -10), 
                ("📤 Return", p_df[is_r], "return_kpi", 0), 
                ("📏 TempPipes", p_df[is_tp], "freeze_kpi", 32), 
                ("☁️ Ambient", p_df[is_amb], None, None)
            ]

            if mobile_mode:
                for title, g_df, kpi_col, kpi_val in groups_data:
                    render_dashboard_column(title, g_df, kpi_col, kpi_val, unit_mode, unit_label)
                    st.markdown("<hr style='border: 1px dashed #ccc; margin: 15px 0;'>", unsafe_allow_html=True)
            else:
                cols = st.columns([1, 0.1, 1, 0.1, 1, 0.1, 1])
                for s_idx in [1, 3, 5]: 
                    cols[s_idx].markdown("<div style='border-left: 1px solid #ddd; height: 280px; margin: auto;'></div>", unsafe_allow_html=True)
                for idx, (title, g_df, kpi_col, kpi_val) in enumerate(groups_data):
                    with cols[[0, 2, 4, 6][idx]]: 
                        render_dashboard_column(title, g_df, kpi_col, kpi_val, unit_mode, unit_label)

def render_dashboard_column(title, g_df, kpi_col, kpi_val, unit_mode, unit_label):
    """Layout engine displaying specific array KPIs and standard ranges."""
    st.markdown(f"**{title}**")
    if g_df.empty or g_df['latest_temp'].isnull().all():
        st.caption("No recent data"); return
    
    def convert(v): return (v - 32) * 5/9 if (unit_mode == "Celsius" and pd.notnull(v)) else v
    l_conv, c_min, c_max, m24, x24 = map(convert, [g_df['latest_temp'].mean(), g_df['min_now'].min(), g_df['max_now'].max(), g_df['min_24h'].min(), g_df['max_24h'].max()])

    st.metric("Avg (Latest)", f"{l_conv:.1f}{unit_label}")
    if kpi_col:
        pct = g_df[kpi_col].iloc[0]
        st.markdown(f"<p style='font-size:0.85rem; color:{'green' if pct == 100 else '#FF8C00' if pct > 0 else 'gray'};'><b>{pct:.0f}%</b> Nodes ≤ {kpi_val}°F</p>", unsafe_allow_html=True)

    st.markdown(f"<div style='font-size: 0.8rem; line-height: 1.2;'><b>Normal Ranges:</b><br>Current: {f'{c_min:.1f} to {c_max:.1f}{unit_label}' if c_min is not None else 'No Data'}<br>24h Range: {f'{m24:.1f} to {x24:.1f}{unit_label}' if m24 is not None else 'No Data'}</div>", unsafe_allow_html=True)

def get_trend_arrow(current, previous):
    if pd.isnull(current) or pd.isnull(previous): return "N/A"
    return f"🔺 +{current-previous:.1f}" if (current-previous) > 0.1 else f"🔹 {current-previous:.1f}" if (current-previous) < -0.1 else "➡️ 0.0"

# =============================================================================
# WORKSPACE PAGE 2: TIME VS TEMP SCROLLING OVERVIEW
# =============================================================================
def render_global_overview(selected_project, project_metadata, display_tz):
    """Renders cascading expanded layout trend charts tied directly to the lookback sliders."""
    show_ref = st.session_state.get("global_show_ref", True)
    show_masked = st.session_state.get("global_show_masked", False)
    unit_mode, unit_label = st.session_state.get("unit_mode", "Fahrenheit"), st.session_state.get("unit_label", "°F")

    if not selected_project or selected_project == "All Projects":
        st.info("💡 Select a project in the sidebar to view engineering trends."); return

    p_name, f_start_date = selected_project, None
    if project_metadata:
        p_name = project_metadata.get('ProjectName', selected_project)
        if pd.notnull(project_metadata.get('Date_Freezedown')): f_start_date = pd.to_datetime(project_metadata.get('Date_Freezedown')).date()

    st.header(f"📈 Time vs Temp: {p_name}")
    if f_start_date:
        st.markdown(f"### 🗓️ Day **{max(0, (pd.Timestamp.now(tz=display_tz).date() - f_start_date).days)}** of Freezedown")

    with st.spinner("Syncing timeline data..."):
        p_df = get_universal_portal_data(selected_project, st.session_state.get("global_lookback_days", 35))
        events_df = get_chiller_events_overlay(selected_project)

    if p_df.empty:
        st.warning("No data found for this project scope."); return

    mask_col = 'approval_status' if 'approval_status' in p_df.columns else 'approve'
    if not show_masked and mask_col in p_df.columns:
        p_df = p_df[p_df[mask_col].astype(str).str.upper() != 'MASKED'].copy()

    # Timeline calculations bound directly to slider values
    now_local = pd.Timestamp.now(tz=display_tz)
    end_view = (now_local + pd.Timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    start_view = end_view - pd.Timedelta(weeks=st.session_state.get("global_lookback_weeks_slider", 5))

    for loc in sorted([str(l) for l in p_df['Location'].dropna().unique()], key=natural_sort_key):
        with st.expander(f"📍 Location Array: {loc}", expanded=True):
            loc_df = p_df[p_df['Location'] == loc].copy()
            if unit_mode == "Celsius": loc_df['temperature'] = (loc_df['temperature'] - 32) * 5/9
            
            st.plotly_chart(build_high_speed_graph(
                df=loc_df, title=f"Thermal Trends: {loc}", start_view=start_view, end_view=end_view,
                unit_mode=unit_mode, unit_label=unit_label, display_tz=display_tz, f_start_date=f_start_date,
                curve_id=f"{str(selected_project).split('-')[0]}-{loc}" if (show_ref and any(x in loc.upper() for x in ["TP", "T", "PIPE", "TEMP"])) else None,
                events_df=events_df
            ), use_container_width=True, key=f"tvt_{selected_project}_{loc}")

# =============================================================================
# WORKSPACE PAGE 3: TEMPERATURE VS DEPTH GRADIENTS
# =============================================================================
def render_depth_charts(selected_project, unit_label, display_tz):
    """Generates standardized vertical profile plots parsing localized snapshot layers."""
    st.header(f"📏 Depth Profile Analysis: {selected_project}")
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to view depth profiles."); return

    lookback_weeks = st.sidebar.slider("Historical Snapshots (Weeks)", 1, 24, 8, key="depth_lookback")
    with st.spinner("Compiling depth analysis records..."):
        p_df = get_universal_portal_data(selected_project, lookback_weeks * 7)

    if p_df is None or p_df.empty:
        st.warning("No telemetric data profiles logged for this coordinate scope."); return

    p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
    p_df = p_df[p_df['temperature'] <= 50.0] # 50°F Upper filter criterion block
    depth_df = p_df.dropna(subset=['Depth_Num', 'Location']).copy()

    if depth_df.empty:
        st.info("No active subsurface telemetry readings discovered under 50°F threshold limits."); return

    unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
    freeze_pt = 0 if unit_mode == "Celsius" else 32
    mondays = pd.date_range(end=pd.Timestamp.now(tz='UTC'), periods=lookback_weeks, freq='W-MON')

    for loc in sorted(depth_df['Location'].unique()):
        with st.expander(f"📍 Subsurface Profile Array - Location Core: {loc}", expanded=True):
            loc_data = depth_df[depth_df['Location'] == loc].copy()
            loc_data['timestamp_local'] = loc_data['timestamp'].dt.tz_localize('UTC').dt.tz_convert(display_tz) if loc_data['timestamp'].dt.tz is None else loc_data['timestamp'].dt.tz_convert(display_tz)
            
            fig = go.Figure()

            # Baseline Raw Horizon
            baseline_ts = loc_data['timestamp_local'].min()
            b_win = loc_data[(loc_data['timestamp_local'] >= baseline_ts - pd.Timedelta(hours=12)) & (loc_data['timestamp_local'] <= baseline_ts + pd.Timedelta(hours=12))]
            if not b_win.empty:
                b_snap = b_win.assign(diff=(b_win['timestamp_local'] - baseline_ts).abs()).sort_values(['NodeNum', 'diff']).drop_duplicates('NodeNum').sort_values('Depth_Num')
                fig.add_trace(go.Scatter(x=b_snap['temperature'] if unit_mode == "Fahrenheit" else (b_snap['temperature']-32)*5/9, y=b_snap['Depth_Num'], mode='lines+markers', name=f"<b>Baseline ({baseline_ts.strftime('%Y-%m-%d')})</b>", line=dict(color='black', width=2.5, dash='dash'), marker=dict(size=5, color='black')))

            # Hardened Recent Profile Finder with Fallback Logic
            loc_data['date_str'], loc_data['hour_int'] = loc_data['timestamp_local'].dt.strftime('%Y-%m-%d'), loc_data['timestamp_local'].dt.hour
            recent_rows, r_date_str = [], ""
            
            if not loc_data.empty:
                for candidate in sorted(loc_data['date_str'].unique(), reverse=True):
                    pool = loc_data[loc_data['date_str'] == candidate]
                    if pool.empty: continue
                    r_date_str = candidate
                    for _, gp in pool.groupby('NodeNum'):
                        exact = gp[gp['hour_int'] == 6]
                        recent_rows.append(exact.sort_values('timestamp_local').iloc[-1] if not exact.empty else gp.assign(hd=(gp['hour_int']-6).abs()).sort_values(by=['hd', 'timestamp_local']).iloc[0])
                    break

            # Plot Recent Line
            if recent_rows:
                r_snap = pd.DataFrame(recent_profile_rows).sort_values('Depth_Num') if 'recent_profile_rows' in locals() else pd.DataFrame(recent_rows).sort_values('Depth_Num')
                fig.add_trace(go.Scatter(x=r_snap['temperature'] if unit_mode == "Fahrenheit" else (r_snap['temperature']-32)*5/9, y=r_snap['Depth_Num'], mode='lines+markers', name=f"<b>Most Recent ({r_date_str} 6AM*)</b>", line=dict(color='#ff7f0e', width=3.5, shape='spline', smoothing=1.1), marker=dict(size=6, color='#ff7f0e')))

            # Historical Snapshot Overlay Layers
            for m_date in mondays:
                t_ts = m_date.replace(hour=6, minute=0, second=0)
                if t_ts.strftime('%Y-%m-%d') in [baseline_ts.strftime('%Y-%m-%d'), r_date_str]: continue
                
                win = loc_data[(loc_data['timestamp_local'] >= t_ts - pd.Timedelta(hours=12)) & (loc_data['timestamp_local'] <= t_ts + pd.Timedelta(hours=12))]
                if not win.empty:
                    snap = win.assign(diff=(win['timestamp_local'] - t_ts).abs()).sort_values(['NodeNum', 'diff']).drop_duplicates('NodeNum').sort_values('Depth_Num')
                    fig.add_trace(go.Scatter(x=snap['temperature'] if unit_mode == "Fahrenheit" else (snap['temperature']-32)*5/9, y=snap['Depth_Num'], mode='lines+markers', name=t_ts.strftime('%Y-%m-%d'), line=dict(shape='spline', smoothing=1.1, width=1.5), marker=dict(size=4)))

            fig.add_vline(x=freeze_pt, line_width=2, line_dash="solid", line_color="#ADD8E6")
            max_d = loc_data['Depth_Num'].max()
            
            fig.update_layout(
                title=f"<b>Temp vs Depth Gradient - Borehole Vector: {loc}</b>", plot_bgcolor='white', height=750,
                xaxis=dict(title=f"Temperature ({unit_label})", range=[-20, 80] if unit_mode == "Fahrenheit" else [-30, 30], gridcolor='Gainsboro', showline=True, linewidth=1.5, linecolor='black', mirror=True),
                yaxis=dict(title="Depth Below Collar (ft)", range=[int(((max_d // 10) + 1) * 10) if pd.notnull(max_d) else 50, 0], gridcolor='Silver', showline=True, linewidth=1.5, linecolor='black', mirror=True),
                legend=dict(orientation="h", y=-0.12, xanchor="center", x=0.5)
            )
            st.plotly_chart(fig, use_container_width=True, key=f"depth_cht_{selected_project}_{loc}")

###########################
# PAGE 4: SENSOR STATUS - #
###########################

def fmt_temp(val, unit_mode, unit_label):
    """Standalone helper utility to safely format raw float metrics into clean text values."""
    if pd.isnull(val) or pd.isna(val):
        return "N/A"
    v = (val - 32) * 5/9 if unit_mode == "Celsius" else val
    return f"{v:.1f}{unit_label}"


def assign_row_color(hours):
    """Standalone utility mapping data latency windows directly to CSS background colors."""
    if hours is None or pd.isna(hours) or hours == float('inf'):
        return "background-color: #d1d5db; color: #1f2937;"  # Gray / Offline
    if hours < 1.0:
        return "background-color: #d1fae5; color: #065f46;"  # Green / Online
    if 1.0 <= hours <= 6.0:
        return "background-color: #fef08a; color: #854d0e;"  # Yellow / Warning
    if 6.0 < hours <= 12.0:
        return "background-color: #fed7aa; color: #9a3412;"  # Orange / Stale
    return "background-color: #fca5a5; color: #991b1b;"      # Red / Critical


def render_sensor_status(client, selected_project, unit_label, unit_mode, display_tz):
    """
    Page Name: Sensor Status
    Strictly locked to read-only views across: project_registry, master_data_view, and manual_rejections.
    """
    # 1. HEADER LOGIC (Source: project_registry via Sidebar Session State)
    p_meta = st.session_state.get('project_metadata')
    if not p_meta or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to view sensor health.")
        return

    p_name = p_meta.get('ProjectName', selected_project)
    f_date = p_meta.get('Date_Freezedown')
    st.title(f"❄️ {p_name}")
    
    if pd.notnull(f_date):
        days = (pd.Timestamp.now(tz=display_tz).date() - pd.to_datetime(f_date).date()).days
        st.markdown(f"## 🗓️ Day **{max(0, days)}** of Freezedown")
    st.divider()

    # 2. TELEMETRY & COVERAGE QUERY (Uses master_data_view)
    query = f"""
        WITH BaseReporting AS (
            SELECT m.NodeNum, m.timestamp, m.temperature, m.Location, m.Bank, m.Depth
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
            WHERE m.Project = @proj_id
        ),
        GapAnalysis AS (
            SELECT *, LAG(timestamp) OVER (PARTITION BY NodeNum ORDER BY timestamp) AS prev_ts
            FROM BaseReporting
        ),
        HistoricalStats AS (
            SELECT 
                NodeNum, Location, Bank, Depth,
                MAX(timestamp) AS last_ping,
                ARRAY_AGG(temperature ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS current_temp,
                AVG(CASE WHEN timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN temperature END) as avg_1h,
                AVG(CASE WHEN timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 25 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature END) as avg_24h,
                
                -- Pulse Check Flags
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN 1 ELSE 0 END) as seen_1h_f,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR) THEN 1 ELSE 0 END) as seen_6h_f,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN 1 ELSE 0 END) as seen_24h_f,

                -- Hourly Coverage Calculation (Distinct hours seen / Total hours in period)
                (COUNT(DISTINCT CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN TIMESTAMP_TRUNC(timestamp, HOUR) END) / 24.0) * 100 as coverage_24h,
                (COUNT(DISTINCT CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 168 HOUR) THEN TIMESTAMP_TRUNC(timestamp, HOUR) END) / 168.0) * 100 as coverage_7d,

                -- Extremes & Gaps
                MIN(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature END) AS low_24h,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature END) AS high_24h,
                MAX(TIMESTAMP_DIFF(timestamp, prev_ts, HOUR)) AS max_gap_7d
            FROM GapAnalysis 
            GROUP BY NodeNum, Location, Bank, Depth
        )
        SELECT * FROM HistoricalStats
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)]
    )

    try:
        df = client.query(query, job_config=job_config).to_dataframe()
        if df.empty:
            st.warning("No data found in master_data_view for this project.")
            return

        # 3. STATUS & LAG CALCULATIONS
        now_local = pd.Timestamp.now(tz=display_tz)
        
        def get_lag(ts):
            if pd.isnull(ts): return 999.0
            ts_aware = ts if ts.tzinfo else ts.tz_localize('UTC')
            return (now_local - ts_aware.tz_convert(display_tz)).total_seconds() / 3600.0

        df['last_seen_hrs'] = df['last_ping'].apply(get_lag)

        # 4. FORMATTING HELPERS
        def get_status_icon(hrs):
            if hrs >= 999.0: return "❌ Never"
            if hrs <= 1.0: return f"🟢 {hrs:.1f}h"
            if hrs <= 6.0: return f"🟠 {hrs:.1f}h"
            return f"🔴 {hrs:.1f}h"

        def fmt_t(val):
            if pd.isnull(val): return "N/A"
            v = (val - 32) * 5/9 if unit_mode == "Celsius" else val
            return f"{v:.1f}{unit_label}"

        def get_arrow(cur, prev):
            if pd.isnull(cur) or pd.isnull(prev): return "N/A"
            d = cur - prev
            return f"🔺 +{d:.1f}" if d > 0.1 else f"🔹 {d:.1f}" if d < -0.1 else "➡️ 0.0"

        # 5. LOCATION PERFORMANCE SUMMARY
        st.subheader("📍 Location Performance Summary")
        
        summary_rows = []
        for loc, loc_group in df.groupby('Location'):
            summary_rows.append({
                'Location': loc,
                'Total Nodes': int(len(loc_group)),
                'Seen 1h': int(loc_group['seen_1h_f'].sum()),
                'Seen 6h': int(loc_group['seen_6h_f'].sum()),
                'Seen 24h': int(loc_group['seen_24h_f'].sum()),
                '24h Coverage': f"{loc_group['coverage_24h'].mean():.1f}%",
                '7d Coverage': f"{loc_group['coverage_7d'].mean():.1f}%",
                'Avg Temp': fmt_t(loc_group['current_temp'].mean()),
                'Low 24h': fmt_t(loc_group['low_24h'].min()),
                'High 24h': fmt_t(loc_group['high_24h'].max()),
                'Best Seen': get_status_icon(loc_group['last_seen_hrs'].min()),
                'Worst Seen': get_status_icon(loc_group['last_seen_hrs'].max())
            })
            
        summary_df = pd.DataFrame(summary_rows)

        def style_missing_counters(val_df):
            canvas = pd.DataFrame('', index=val_df.index, columns=val_df.columns)
            target_cols = ['Seen 1h', 'Seen 6h', 'Seen 24h']
            
            for idx in val_df.index:
                total = val_df.loc[idx, 'Total Nodes']
                for col in target_cols:
                    missing = total - val_df.loc[idx, col]
                    if missing == 0:
                        bg_style = "background-color: #d1fae5; color: #065f46; font-weight: bold;"
                    elif 1 <= missing <= 3:
                        bg_style = "background-color: #bbf7d0; color: #14532d; font-weight: bold;"
                    elif 4 <= missing <= 6:
                        bg_style = "background-color: #fef08a; color: #713f12; font-weight: bold;"
                    elif 7 <= missing <= 10:
                        bg_style = "background-color: #fed7aa; color: #7c2d12; font-weight: bold;"
                    else:
                        bg_style = "background-color: #fca5a5; color: #7f1d1d; font-weight: bold;"
                    canvas.loc[idx, col] = bg_style
            return canvas

        st.dataframe(summary_df.style.apply(style_missing_counters, axis=None), use_container_width=True, hide_index=True)

        # 6. DETAILED SENSOR AUDIT
        st.divider()
        st.subheader("🔍 Detailed Sensor Audit")
        
        selected_loc = st.selectbox("Filter Audit by Location:", ["--- All ---"] + sorted(df['Location'].unique()))
        audit_df = df.copy() if selected_loc == "--- All ---" else df[df['Location'] == selected_loc]
        
        rows = []
        for _, r in audit_df.sort_values(['Location', 'Depth', 'Bank']).iterrows():
            rows.append({
                "Node": r['NodeNum'],
                "Location": r['Location'],
                "Position": f"{r['Depth']}ft" if pd.notnull(r['Depth']) else f"Bank {r['Bank']}",
                "Last Seen": get_status_icon(r['last_seen_hrs']),
                "24 hour coverage": f"{r['coverage_24h']:.1f}%",
                "Current Temp": fmt_t(r['current_temp']),
                "Change for 1 hr": get_arrow(r['current_temp'], r['avg_1h']),
                "Change for 24 hr": get_arrow(r['current_temp'], r['avg_24h']),
                "24 hr high": fmt_t(r['high_24h']),
                "24 hour low": fmt_t(r['low_24h'])
            })
        
        st.dataframe(rows, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Sensor Status Error: {e}")
            
# =============================================================================
# 6. SENSOR STATUS WORKSPACE & PERFORMANCE TABLES
# =============================================================================
def fmt_temp(val, unit_mode, unit_label):
    """Unified helper to format raw database values into active unit scales."""
    if pd.isnull(val): return "N/A"
    v = (val - 32) * 5/9 if unit_mode == "Celsius" else val
    return f"{v:.1f}{unit_label}"

def render_project_status_dashboard(client, selected_project, unit_label, unit_mode):
    """Compiles segmented, horizontal metric summaries tracking structural sensor types."""
    st.subheader("📊 Project Status Summary")
    if client is None: return

    query = f"""
        SELECT n.NodeNum, n.Bank, n.Location, n.Depth,
            CASE 
                WHEN (n.Bank LIKE 'S%' OR n.Location LIKE 'S%') AND (n.Bank NOT LIKE '%Amb%' AND n.Location NOT LIKE '%Amb%') THEN 'Supply'
                WHEN (n.Bank LIKE 'R%' OR n.Location LIKE 'R%') AND (n.Bank NOT LIKE '%Amb%' AND n.Location NOT LIKE '%Amb%') THEN 'Return'
                WHEN (n.Bank LIKE '%Amb%' OR n.Location LIKE '%Amb%') THEN 'Ambient'
                WHEN n.Depth IS NOT NULL THEN 'TempPipes' ELSE 'Other'
            END as hardware_type,
            AVG(CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN m.temperature END) as avg_now,
            MIN(CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN m.temperature END) as min_now,
            MAX(CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN m.temperature END) as max_now,
            MIN(CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN m.temperature END) as min_24h,
            MAX(CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN m.temperature END) as max_24h,
            AVG(CASE WHEN m.timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN m.temperature END) as avg_1h_prev,
            AVG(CASE WHEN m.timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR) THEN m.temperature END) as avg_6h_prev,
            COUNTIF(m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) as pings_24h,
            ARRAY_AGG(m.temperature ORDER BY m.timestamp DESC LIMIT 1)[OFFSET(0)] as latest_temp,
            MAX(m.timestamp) as latest_ts
        FROM `{NODE_REGISTRY_TABLE}` n
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.master_data_view` m ON n.NodeNum = m.NodeNum
        WHERE n.Project = @proj_id AND n.End_Date IS NULL
        GROUP BY 1, 2, 3, 4, 5
    """
    try:
        df = client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)])).to_dataframe()
    except Exception as e:
        st.error(f"Dashboard Query Failed: {e}"); return

    if df.empty:
        st.info("No active nodes found for dashboard summary."); return

    cols = st.columns(4)
    type_map = {"Supply": (cols[0], "📥"), "Return": (cols[1], "📤"), "TempPipes": (cols[2], "📏"), "Ambient": (cols[3], "☁️")}
    now_utc = pd.Timestamp.now(tz='UTC')

    for h_type, (col, icon) in type_map.items():
        g_df = df[df['hardware_type'] == h_type]
        with col:
            st.markdown(f"#### {icon} {h_type}")
            if g_df.empty or g_df['latest_ts'].isna().all():
                st.caption("No recent data"); continue
            
            latest_time = g_df['latest_ts'].max()
            latest_time = latest_time.tz_localize('UTC') if latest_time.tzinfo is None else latest_time.tz_convert('UTC')
            lag_hrs = (now_utc - latest_time).total_seconds() / 3600
            
            if lag_hrs > 1.1: 
                st.subheader(f"⚠️ Offline {int(lag_hrs)}h")
            else: 
                val = g_df['avg_now'].mean() if pd.notnull(g_df['avg_now'].mean()) else g_df['latest_temp'].mean()
                st.title(f"{fmt_temp(val, unit_mode, unit_label)}")
            
            st.write(f"**{int(g_df['avg_now'].notnull().sum())}/{len(g_df)}** (1h) | **{int((g_df['pings_24h'] > 0).sum())}/{len(g_df)}** (24h)")
            st.caption(f"Cur: {fmt_temp(g_df['min_now'].min(), unit_mode, unit_label)} to {fmt_temp(g_df['max_now'].max(), unit_mode, unit_label)}")
            st.caption(f"24h: {fmt_temp(g_df['min_24h'].min(), unit_mode, unit_label)} to {fmt_temp(g_df['max_24h'].max(), unit_mode, unit_label)}")
            
            t_col = st.columns(2)
            cur_val = g_df['avg_now'].mean() if pd.notnull(g_df['avg_now'].mean()) else g_df['latest_temp'].mean()
            t_col[0].caption(f"1h\n{get_trend_arrow(cur_val, g_df['avg_1h_prev'].mean())}")
            t_col[1].caption(f"6h\n{get_trend_arrow(cur_val, g_df['avg_6h_prev'].mean())}")


def render_hardware_integrity_table(client, selected_project, unit_mode, unit_label):
    """Generates precise hardware diagnostic grids displaying system latency color heatmaps."""
    st.subheader("📋 Hardware Integrity & Connectivity")
    if client is None: return

    query = f"""
        SELECT n.NodeNum, n.Location, n.Bank, n.Depth, n.SensorStatus, MAX(m.timestamp) as last_ping,
            ARRAY_AGG(m.temperature ORDER BY m.timestamp DESC LIMIT 1)[OFFSET(0)] as last_temp,
            COUNTIF(m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) as pings_1h,
            COUNTIF(m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)) as pings_6h,
            COUNTIF(m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) as pings_24h,
            (COUNT(DISTINCT CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN TIMESTAMP_TRUNC(m.timestamp, HOUR) END) / 24.0) * 100 as coverage_24h,
            AVG(CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN m.temperature END) as avg_now,
            AVG(CASE WHEN m.timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN m.temperature END) as avg_1h_prev
        FROM `{NODE_REGISTRY_TABLE}` n
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.master_data_view` m ON n.NodeNum = m.NodeNum
        WHERE n.Project = @proj_id AND n.End_Date IS NULL
        GROUP BY 1, 2, 3, 4, 5
    """
    try:
        df = client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)])).to_dataframe()
    except Exception as e:
        st.error(f"Hardware Table Query Failed: {e}"); return

    if df.empty: 
        st.info("No active nodes configured inside this project scope."); return

    now_utc = pd.Timestamp.now(tz='UTC')

    def row_processor(row):
        ping = row['last_ping']
        if pd.isnull(ping):
            return pd.Series(["❌ Never", "background-color: #d1d5db; color: #1f2937;", f"{row['Depth']}ft" if (pd.notnull(row['Depth']) and row['Depth'] != 0) else f"Bank {row['Bank']}", "N/A", float('inf')])
        
        ts = ping if ping.tzinfo else ping.tz_localize('UTC')
        diff_mins = (now_utc - ts).total_seconds() / 60.0
        hours_hidden = diff_mins / 60.0
        
        if hours_hidden < 1.0:
            txt, style = "Just now" if diff_mins < 1.0 else f"{int(diff_mins)}m ago", "background-color: #d1fae5; color: #065f46;"
        elif hours_hidden <= 6.0:
            txt, style = f"{hours_hidden:.1f}h ago", "background-color: #fef08a; color: #854d0e;"
        elif hours_hidden <= 12.0:
            txt, style = f"{hours_hidden:.1f}h ago", "background-color: #fed7aa; color: #9a3412;"
        elif hours_hidden <= 24.0:
            txt, style = f"{hours_hidden:.1f}h ago", "background-color: #fca5a5; color: #991b1b;"
        else:
            txt, style = f"{hours_hidden:.1f}h ago", "background-color: #d1d5db; color: #1f2937;"
        
        pos = f"{row['Depth']}ft" if (pd.notnull(row['Depth']) and row['Depth'] != 0) else f"Bank {row['Bank']}"
        return pd.Series([txt, style, pos, get_trend_arrow(row['avg_now'], row['avg_1h_prev']), hours_hidden])

    df[['Seen_Text', 'Seen_Style', 'Pos_Label', 'Trend', 'hours_hidden']] = df.apply(row_processor, axis=1)
    df['hours_hidden'] = pd.to_numeric(df['hours_hidden'], errors='coerce').fillna(float('inf'))
    df = df.sort_values(by='hours_hidden', ascending=True).reset_index(drop=True)

    display_df = pd.DataFrame({
        "Node ID": df['NodeNum'], "Location": df['Location'], "Position": df['Pos_Label'], "Last Seen": df['Seen_Text'],
        "24h Coverage": df['coverage_24h'], "1h Change": df['Trend'], "Last Temp": df['last_temp'].apply(lambda x: fmt_temp(x, unit_mode, unit_label)),
        "1h Pings": df['pings_1h'], "6h Pings": df['pings_6h'], "24h Pings": df['pings_24h']
    })

    st.dataframe(
        display_df.style.apply(lambda d: pd.DataFrame([['' if c != 'Last Seen' else df.loc[i, 'Seen_Style'] for c in d.columns] for i in d.index], index=d.index, columns=d.columns), axis=None), 
        use_container_width=True, hide_index=True,
        column_config={
            "24h Coverage": st.column_config.ProgressColumn("24h Coverage", format="%.1f%%", min_value=0, max_value=100),
            "1h Pings": st.column_config.NumberColumn("1h Pings", format="%d"), "6h Pings": st.column_config.NumberColumn("6h Pings", format="%d"), "24h Pings": st.column_config.NumberColumn("24h Pings", format="%d")
        }
    )

# =============================================================================
# WORKSPACE PAGE 4: ACTIVE ASSET SELECTION REGISTRY
# =============================================================================
def render_node_selector(reg_df, proj_list):
    """Renders a filtered overview tracking real-time asset distributions across projects."""
    st.subheader("🎯 Active Node Registry")
    hide_archived = st.checkbox("Hide Archived Records", value=True, key="ns_hide_archived_toggle")
    
    df = reg_df.copy()
    if hide_archived and 'SensorStatus' in df.columns:
        df = df[(df['SensorStatus'].str.lower() != "archived") & (~df['Location'].str.contains("Archive", case=False, na=False))]

    c1, c2, c3 = st.columns(3)
    f_proj = c1.selectbox("Filter by Project Space", ["All", "Unassigned"] + proj_list, key="ns_proj_f")
    
    loc_opts = (
        df['Location'].dropna().unique().tolist() if f_proj == "All" else
        df[df['Project'].isna() | (df['Project'] == "") | (df['Project'].str.upper() == "OFFICE") | (df['Location'].str.upper() == "OFFICE")]['Location'].dropna().unique().tolist() if f_proj == "Unassigned" else
        df[df['Project'] == f_proj]['Location'].dropna().unique().tolist()
    )
    f_loc = c2.selectbox("Filter by Physical Location", ["All"] + sorted(loc_opts), key="ns_loc_f")
    search_term = c3.text_input("Global Search (Node ID)", "", key="ns_search_f")

    # Implement filtering steps
    if f_proj == "Unassigned": df = df[df['Project'].isna() | (df['Project'] == "") | (df['Project'].str.upper() == "OFFICE")]
    elif f_proj != "All": df = df[df['Project'] == f_proj]
    if f_loc != "All": df = df[df['Location'] == f_loc]
    if search_term: df = df[df['NodeNum'].str.contains(search_term, case=False, na=False)]

    if df.empty:
        st.info("No matching nodes located under current filter parameters."); return None

    df = df.reset_index(drop=True)
    st.markdown("### 📡 Hardware Inventory Fleet Breakdown")
    
    def classify_family(node):
        return "Lord" if "-ch" in str(node).lower() else "SP" if str(node).lower().startswith("sp") else "TP" if str(node).lower().startswith("tp") else "Other"

    summary_df = reg_df.copy()
    summary_df['Hardware Family'] = summary_df['NodeNum'].apply(classify_family)
    summary_df['Parent ID'] = summary_df['NodeNum'].apply(lambda x: re.split(r'(?i)-ch', str(x))[0] if "-ch" in str(x).lower() else x)
    summary_df['is_active'] = summary_df['End_Date'].isna() if 'End_Date' in summary_df.columns else True
    
    try:
        fleet_pivot = summary_df.drop_duplicates(subset=['Parent ID']).groupby(['Hardware Family', 'SensorStatus']).size().unstack(fill_value=0)
        fleet_pivot = fleet_pivot.reindex(["TP", "SP", "Lord", "Other"], fill_value=0)
        fleet_pivot['Total Units'] = fleet_pivot.sum(axis=1)
        st.dataframe(fleet_pivot, use_container_width=True)
    except Exception:
        st.info("💡 Assign status values inside your source Google Sheet to generate hardware breakdown matrices.")
        
    st.markdown("---")
    st.markdown("### 📋 Current Asset Allocation Matrix")

    unit_mode, unit_label = st.session_state.get("unit_mode", "Fahrenheit"), st.session_state.get("unit_label", "°F")
    df['Position'] = df.apply(lambda r: f"{r['Depth']}ft" if (pd.notnull(r.get('Depth')) and r.get('Depth') != 0) else f"Bank {r['Bank']}" if (pd.notnull(r.get('Bank')) and str(r.get('Bank')).strip() != "") else "-", axis=1)
    df['Current Temp'] = df['last_temp'].apply(lambda x: fmt_temp(x, unit_mode, unit_label))

    st.dataframe(
        df, use_container_width=True, hide_index=True,
        column_order=["Project", "Location", "NodeNum", "Position", "Last Seen", "Current Temp"]
    )
    return None

# =============================================================================
# WORKSPACE PAGE 5: COMMISSIONING & DIAGNOSTICS AUDIT
# =============================================================================
def render_node_diagnostics(selected_project, display_tz, unit_label):
    """Executes a deep audit tracking signal ratios, pings, and telemetric packet efficiencies."""
    st.header("📡 Commissioning & Diagnostics Audit")
    client = get_bq_client()
    if client is None: st.error("Database connection lost."); return

    diag_q = f"""
        WITH Stats AS (
            SELECT NodeNum, MAX(timestamp) as last_ping,
                ARRAY_AGG(temperature ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] as last_temp,
                COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) as count_1h,
                COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)) as count_6h,
                COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) as count_24h,
                ARRAY_AGG(rssi ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] as rssi_last_val,
                AVG(rssi) as rssi_avg_val
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` GROUP BY NodeNum
        )
        SELECT n.Project, n.Location, n.NodeNum, n.Bank, n.Depth, n.SensorStatus, 
               s.last_ping, s.last_temp, COALESCE(s.count_1h, 0) as count_1h, COALESCE(s.count_6h, 0) as count_6h, COALESCE(s.count_24h, 0) as count_24h,
               s.rssi_last_val as rssi_last, s.rssi_avg_val as rssi_avg
        FROM `{NODE_REGISTRY_TABLE}` n LEFT JOIN Stats s ON n.NodeNum = s.NodeNum WHERE n.End_Date IS NULL
    """
    try:
        df = client.query(diag_q).to_dataframe()
        if df.empty: st.warning("No tracking nodes located across the active system registries."); return

        st.markdown("### 🔍 Filter Fleet Scope")
        f1, f2, f3 = st.columns(3)
        proj_opts = ["--- All Projects ---"] + sorted(df['Project'].dropna().unique().tolist())
        filter_proj = f1.selectbox("Scope Project Context:", proj_opts, index=proj_opts.index(selected_project) if selected_project in proj_opts else 0)
        
        sub_df = df.copy() if filter_proj == "--- All Projects ---" else df[df['Project'] == filter_proj]
        filter_loc = f2.selectbox("Scope Physical Location:", ["--- All Locations ---"] + sorted(sub_df['Location'].dropna().unique().tolist()))
        filter_stat = f3.selectbox("Scope Hardware Status:", ["--- All Statuses ---"] + sorted(sub_df['SensorStatus'].dropna().unique().tolist()))

        if filter_proj != "--- All Projects ---": df = df[df['Project'] == filter_proj]
        if filter_loc != "--- All Locations ---": df = df[df['Location'] == filter_loc]
        if filter_stat != "--- All Statuses ---": df = df[df['SensorStatus'] == filter_stat]

        if df.empty: st.info("No matching hardware arrays found under current parameters."); return

        now_utc = pd.Timestamp.now(tz='UTC')
        def process_latency(row):
            ping = row['last_ping']
            if pd.isnull(ping): return pd.Series(["❌ Never", "background-color: #d1d5db; color: #1f2937;", float('inf')])
            ts = ping if ping.tzinfo else ping.tz_localize('UTC')
            hours_hidden = (now_utc - ts).total_seconds() / 3600.0
            
            style = (
                "background-color: #d1fae5; color: #065f46;" if hours_hidden < 1.0 else
                "background-color: #fef08a; color: #854d0e;" if hours_hidden <= 6.0 else
                "background-color: #fed7aa; color: #9a3412;" if hours_hidden <= 12.0 else
                "background-color: #fca5a5; color: #991b1b;" if hours_hidden <= 24.0 else
                "background-color: #d1d5db; color: #1f2937;"
            )
            return pd.Series([f"{hours_hidden:.1f}h", style, hours_hidden])

        df[['Seen_Text', 'Seen_Style', 'hours_hidden']] = df.apply(process_latency, axis=1)
        df = df.sort_values(by='hours_hidden', ascending=True).reset_index(drop=True)

        unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
        df['Clean_Pos'] = df.apply(lambda r: f"{r['Depth']}ft" if (pd.notnull(r.get('Depth')) and r.get('Depth') != 0) else re.sub(r'(?i)bank\s*', '', str(r['Bank'])).strip() if (pd.notnull(r.get('Bank')) and str(r.get('Bank')).strip() != "") else "-", axis=1)
        df['efficiency_pct'] = ((df['count_24h'] / 96.0) * 100.0).clip(upper=100.0)

        display_df = pd.DataFrame({
            "Node ID": df['NodeNum'], "Location": df['Location'].apply(lambda x: str(x).strip()[:5]), "Position": df['Clean_Pos'],
            "Current Temp": df['last_temp'].apply(lambda x: fmt_temp(x, unit_mode, unit_label)), "Last Seen": df['Seen_Text'],
            "Pings (1h)": df['count_1h'].astype(int), "Pings (6h)": df['count_6h'].astype(int), "Pings (24h)": df['count_24h'].astype(int),
            "RSSI Last": df['rssi_last'].apply(lambda x: f"{int(x)} dBm" if pd.notnull(x) else "N/A"),
            "RSSI Avg": df['rssi_avg'].apply(lambda x: f"{int(x)} dBm" if pd.notnull(x) else "N/A"),
            "Reporting Efficiency": df['efficiency_pct']
        })

        st.dataframe(
            display_df.style.apply(lambda d: pd.DataFrame([['' if c != 'Last Seen' else df.loc[i, 'Seen_Style'] for c in d.columns] for i in d.index], index=d.index, columns=d.columns), axis=None),
            use_container_width=True, hide_index=True,
            column_config={"Reporting Efficiency": st.column_config.ProgressColumn("Reporting Efficiency", format="%.0f%%", min_value=0, max_value=100)}
        )
    except Exception as e:
        st.error(f"Diagnostics Audit Failed: {e}")
            
# =============================================================================
# Function: Hardware integrity table (Setup Node Tool - Left Unchanged)
# =============================================================================
def render_hardware_integrity_table(client, selected_project, unit_mode, unit_label, target_registry):
    """
    Renders a detailed table showing connectivity, coverage, and recent activity.
    Sorted chronologically by data latency (minutes first, then hours).
    """
    st.subheader("📋 Hardware Integrity & Connectivity")
    
    query = f"""
        SELECT 
            n.NodeNum, n.Location, n.Bank, n.Depth, n.SensorStatus,
            MAX(m.timestamp) as last_ping,
            ARRAY_AGG(m.temperature ORDER BY m.timestamp DESC LIMIT 1)[OFFSET(0)] as last_temp,
            COUNTIF(m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) as pings_1h,
            COUNTIF(m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)) as pings_6h,
            COUNTIF(m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) as pings_24h,
            (COUNT(DISTINCT CASE 
                WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) 
                THEN TIMESTAMP_TRUNC(m.timestamp, HOUR) 
             END) / 24.0) * 100 as coverage_24h,
            AVG(CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN m.temperature END) as avg_now,
            AVG(CASE WHEN m.timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN m.temperature END) as avg_1h_prev
        FROM `{target_registry}` n
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.master_data_view` m ON n.NodeNum = m.NodeNum
        WHERE n.Project = @proj_id AND n.End_Date IS NULL
        GROUP BY 1, 2, 3, 4, 5
    """
    
    try:
        df = client.query(query, job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)]
        )).to_dataframe()
    except Exception as e:
        st.error(f"Hardware Table Query Failed: {e}")
        return

    if df.empty: 
        st.info("No active nodes found for connectivity table.")
        return

    now_utc = pd.Timestamp.now(tz='UTC')

    def row_processor(row):
        ping = row['last_ping']
        
        if pd.isnull(ping):
            hours_hidden = float('inf')
            txt = "❌ Never"
            style = "background-color: #d1d5db; color: #1f2937;" 
        else:
            ts = ping if ping.tzinfo else ping.tz_localize('UTC')
            diff_mins = (now_utc - ts).total_seconds() / 60.0
            hours_hidden = diff_mins / 60.0
            
            if hours_hidden < 1.0:
                txt = f"{int(diff_mins)}m ago" if diff_mins >= 1.0 else "Just now"
                style = "background-color: #d1fae5; color: #065f46;" 
            elif 1.0 <= hours_hidden <= 6.0:
                txt = f"{hours_hidden:.1f}h ago"
                style = "background-color: #fef08a; color: #854d0e;" 
            elif 6.0 < hours_hidden <= 12.0:
                txt = f"{hours_hidden:.1f}h ago"
                style = "background-color: #fed7aa; color: #9a3412;" 
            elif 12.0 < hours_hidden <= 24.0:
                txt = f"{hours_hidden:.1f}h ago"
                style = "background-color: #fca5a5; color: #991b1b;" 
            else:
                txt = f"{hours_hidden:.1f}h ago"
                style = "background-color: #d1d5db; color: #1f2937;" 
        
        pos = f"{row['Depth']}ft" if (pd.notnull(row['Depth']) and row['Depth'] != 0) else f"Bank {row['Bank']}"
        trend = get_trend_arrow(row['avg_now'], row['avg_1h_prev'])
        
        return pd.Series([txt, style, pos, trend, hours_hidden])

    df[['Seen_Text', 'Seen_Style', 'Pos_Label', 'Trend', 'hours_hidden']] = df.apply(row_processor, axis=1)

    df['hours_hidden'] = pd.to_numeric(df['hours_hidden'], errors='coerce').fillna(float('inf'))
    df = df.sort_values(by='hours_hidden', ascending=True).reset_index(drop=True)

    display_df = pd.DataFrame({
        "Node ID": df['NodeNum'],
        "Location": df['Location'],
        "Position": df['Pos_Label'],
        "Last Seen": df['Seen_Text'],
        "24h Coverage": df['coverage_24h'], 
        "1h Change": df['Trend'],
        "Last Temp": df['last_temp'].apply(lambda x: fmt_temp(x, unit_mode, unit_label)),
        "1h Pings": df['pings_1h'],
        "6h Pings": df['pings_6h'],
        "24h Pings": df['pings_24h']
    })
# =============================================================================
# WORKSPACE PAGE 4: ACTIVE ASSET SELECTION REGISTRY
# =============================================================================
def render_node_selector(reg_df, proj_list):
    """Renders a filtered inventory overview tracking active fleet distributions across projects."""
    st.subheader("🎯 Active Node Registry")
    hide_archived = st.checkbox("Hide Archived Records", value=True, key="ns_hide_archived_toggle")
    
    df = reg_df.copy()
    if hide_archived and 'SensorStatus' in df.columns:
        df = df[(df['SensorStatus'].str.lower() != "archived") & (~df['Location'].str.contains("Archive", case=False, na=False))]

    c1, c2, c3 = st.columns(3)
    f_proj = c1.selectbox("Filter by Project Space", ["All", "Unassigned"] + proj_list, key="ns_proj_f")
    
    loc_opts = (
        df['Location'].dropna().unique().tolist() if f_proj == "All" else
        df[df['Project'].isna() | (df['Project'] == "") | (df['Project'].str.upper() == "OFFICE") | (df['Location'].str.upper() == "OFFICE")]['Location'].dropna().unique().tolist() if f_proj == "Unassigned" else
        df[df['Project'] == f_proj]['Location'].dropna().unique().tolist()
    )
    f_loc = c2.selectbox("Filter by Physical Location", ["All"] + sorted(loc_opts), key="ns_loc_f")
    search_term = c3.text_input("Global Search (Node ID)", "", key="ns_search_f")

    # Cascading Data Filtering
    if f_proj == "Unassigned": df = df[df['Project'].isna() | (df['Project'] == "") | (df['Project'].str.upper() == "OFFICE")]
    elif f_proj != "All": df = df[df['Project'] == f_proj]
    if f_loc != "All": df = df[df['Location'] == f_loc]
    if search_term: df = df[df['NodeNum'].str.contains(search_term, case=False, na=False)]

    if df.empty:
        st.info("No matching nodes located under current filter parameters."); return None

    df = df.reset_index(drop=True)
    st.markdown("### 📡 Hardware Inventory Fleet Breakdown")
    
    def classify_hardware_family(node):
        node_str = str(node).lower()
        return "Lord" if "-ch" in node_str else "SP" if node_str.startswith("sp") else "TP" if node_str.startswith("tp") else "Other"

    summary_df = reg_df.copy()
    summary_df['Hardware Family'] = summary_df['NodeNum'].apply(classify_hardware_family)
    summary_df['Parent ID'] = summary_df['NodeNum'].apply(lambda x: re.split(r'(?i)-ch', str(x))[0] if "-ch" in str(x).lower() else x)
    
    try:
        fleet_pivot = summary_df.drop_duplicates(subset=['Parent ID']).groupby(['Hardware Family', 'SensorStatus']).size().unstack(fill_value=0)
        fleet_pivot = fleet_pivot.reindex(["TP", "SP", "Lord", "Other"], fill_value=0)
        fleet_pivot['Total Units'] = fleet_pivot.sum(axis=1)
        st.dataframe(fleet_pivot, use_container_width=True)
    except Exception:
        st.info("💡 Fleet inventory matrix summary is optimizing. Assign status properties inside Google Sheets to generate totals.")
        
    st.markdown("---")
    st.markdown("### 📋 Current Asset Allocation Matrix")

    unit_mode, unit_label = st.session_state.get("unit_mode", "Fahrenheit"), st.session_state.get("unit_label", "°F")
    df['Position'] = df.apply(lambda r: f"{r['Depth']}ft" if (pd.notnull(r.get('Depth')) and r.get('Depth') != 0) else f"Bank {r['Bank']}" if (pd.notnull(r.get('Bank')) and str(r.get('Bank')).strip() != "") else "-", axis=1)
    df['Current Temp'] = df['last_temp'].apply(lambda x: fmt_temp(x, unit_mode, unit_label))

    st.dataframe(
        df, use_container_width=True, hide_index=True,
        column_order=["Project", "Location", "NodeNum", "Position", "Last Seen", "Current Temp"]
    )
    return None

# =============================================================================
# WORKSPACE PAGE 5: DATA PROCESSING & REFERENCE ENGINE
# =============================================================================
def render_data_processing_page(selected_project):
    """Handles parsing inbound telemetry files, wide pivot exports, and database asset inventory tracking."""
    st.header("⚙️ Data Processing & Reference Engine")
    
    client = get_bq_client()
    if client is None: st.error("Database connection unavailable."); return
        
    tab_upload, tab_export, tab_ref_library, tab_event_log, tab_chiller_reg = st.tabs([
        "📄 Upload Telemetry", "📥 Export Report", "📈 Ref Curve Library", "🚨 Log Site Event", "❄️ Register Chiller"
    ])
    
    CHILLER_REG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.chiller_registry"
    EVENTS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.chiller_events"
    
    # --- TAB 1: HIGH-SPEED LOG INGESTION PROCESSING ---
    with tab_upload:
        st.subheader("📄 Manual File Ingestion")
        st.caption("💡 Rule: Lord hardware IDs utilize a dash character string separator (e.g. 58014-ch1). SensorPush nodes are strictly numeric.")
        u_file = st.file_uploader("Select CSV or Excel file", type=['csv', 'xlsx'], key="manual_upload_main")
        
        if u_file is not None:
            try:
                is_sensorconnect, skip_rows = False, 0
                if u_file.name.endswith('.csv'):
                    u_file.seek(0)
                    for i, line in enumerate(u_file):
                        if b"DATA_START" in line: is_sensorconnect, skip_rows = True, i + 1; break
                    u_file.seek(0)

                df_raw = (
                    pd.read_csv(u_file, encoding='latin1', skiprows=skip_rows, dtype=str) if is_sensorconnect else
                    pd.read_csv(u_file, encoding='latin1', dtype=str) if u_file.name.endswith('.csv') else
                    pd.read_excel(u_file, dtype=str)
                )

                if not df_raw.empty:
                    df_processed = pd.DataFrame()
                    headers = [str(h).strip().lower() for h in df_raw.columns]
                    
                    # Branch A: Lord SensorConnect (Wide-to-Long Melting Pipeline)
                    if is_sensorconnect:
                        t_col = [h for h in df_raw.columns if 'time' in h.lower()][0]
                        df_melted = df_raw.melt(id_vars=[t_col], value_vars=[h for h in df_raw.columns if h != t_col], var_name='NodeNum', value_name='temperature')
                        df_processed['timestamp'] = pd.to_datetime(df_melted[t_col], errors='coerce', utc=True)
                        df_processed['NodeNum'] = df_melted['NodeNum'].str.strip().str.replace(':', '-')
                        df_processed['temperature'] = pd.to_numeric(df_melted['temperature'], errors='coerce')

                    # Branch B: Lord SensorCloud Standard Log Structures
                    elif any(k in headers for k in ['channel', 'node']) and any('time' in h for h in headers):
                        t_h = df_raw.columns[next(i for i, h in enumerate(headers) if 'time' in h)]
                        n_h = df_raw.columns[next(i for i, h in enumerate(headers) if 'channel' in h or 'node' in h)]
                        df_processed['timestamp'] = pd.to_datetime(df_raw[t_h], errors='coerce', utc=True)
                        df_processed['NodeNum'] = df_raw[n_h].str.strip().str.replace(':', '-')
                        df_processed['temperature'] = pd.to_numeric(df_raw[[h for h in df_raw.columns if 'temp' in h.lower()][0]], errors='coerce')

                    # Branch C: SensorPush Standard Structure
                    else:
                        t_h, v_h = [h for h in df_raw.columns if 'timestamp' in h.lower()][0], [h for h in df_raw.columns if 'temp' in h.lower()][0]
                        df_processed['timestamp'] = pd.to_datetime(df_raw[t_h], errors='coerce', utc=True)
                        df_processed['temperature'] = pd.to_numeric(df_raw[v_h], errors='coerce')
                        match = re.search(r'^([^ \(\)]+)', u_file.name.replace(".csv", "").replace(".xlsx", ""))
                        df_processed['NodeNum'] = match.group(1).strip() if match else "Unknown"

                    if not df_processed.empty:
                        df_processed = df_processed.dropna(subset=['timestamp', 'temperature'])
                        df_processed = apply_sanity_filter(df_processed) # Drops points outside physical threshold boundary bounds [-30°F, 120°F]
                        
                        st.success(f"✅ Clean Ingest Prepared: {len(df_processed)} records found for Node(s): {', '.join(df_processed['NodeNum'].unique())}")
                        is_lord = "-" in str(df_processed['NodeNum'].iloc[0])
                        target_table = "raw_lord" if is_lord else "raw_sensorpush"
                        
                        if st.button(f"🚀 Push Verified Data to {target_table}"):
                            with st.spinner("Streaming arrays to BigQuery destination layers..."):
                                if is_lord:
                                    from decimal import Decimal
                                    df_processed['temperature'] = df_processed['temperature'].apply(lambda x: Decimal(str(round(x, 1))) if pd.notnull(x) else None)
                                
                                load_config = bigquery.LoadJobConfig(
                                    schema=[bigquery.SchemaField("timestamp", "TIMESTAMP"), bigquery.SchemaField("NodeNum", "STRING"), bigquery.SchemaField("temperature", "NUMERIC" if is_lord else "FLOAT")],
                                    write_disposition="WRITE_APPEND"
                                )
                                client.load_table_from_dataframe(df_processed[['timestamp', 'NodeNum', 'temperature']], f"{PROJECT_ID}.{DATASET_ID}.{target_table}", job_config=load_config).result()
                                st.success("🎉 Ingestion complete!"); st.cache_data.clear(); time.sleep(0.5); st.rerun()
            except Exception as e: st.error(f"Ingestion structural processing sequence broken: {e}")

    # --- TAB 2: PIVOTED MULTI-COLUMN WIDE ENGINEERING EXPORT PORTAL ---
    with tab_export:
        st.subheader("📥 Wide-Format Data Export")
        if not selected_project or selected_project == "All Projects":
            st.warning("⚠️ Select a specific project in the sidebar framework to generate report exports.")
        else:
            c1, c2 = st.columns(2)
            e_start, e_end = c1.date_input("Start Horizon Horizon", value=datetime.now() - timedelta(days=30)), c2.date_input("End Horizon Horizon", value=datetime.now())
            
            with st.spinner("Processing historical telemetry charts..."):
                full_df = get_universal_portal_data(selected_project, (datetime.now().date() - e_start).days)
            
            if not full_df.empty:
                selected_locs = st.multiselect("Filter by Structural Location String (Leave empty for ALL):", options=sorted(full_df['Location'].unique().tolist()))
                mask = (full_df['timestamp'].dt.date >= e_start) & (full_df['timestamp'].dt.date <= e_end)
                if selected_locs: mask = mask & (full_df['Location'].isin(selected_locs))
                
                export_df = full_df.loc[mask].copy()
                if export_df.empty: st.warning("No telemetric metrics matched this configuration pattern.")
                else:
                    export_df['Sensor'] = export_df['Location'] + " (" + export_df['NodeNum'].astype(str) + ")"
                    wide_df = export_df.pivot_table(index='timestamp', columns='Sensor', values='temperature', aggfunc='first').reset_index()
                    wide_df['timestamp'] = wide_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

                    st.success(f"Custom report matrix constructed cleanly: {len(wide_df.columns)-1} active nodes parsed.")
                    st.download_button(
                        label="💾 Download Custom Pivot Matrix Sheet Export (.CSV)", data=wide_df.to_csv(index=False).encode('utf-8'),
                        file_name=f"SF-Job-{selected_project}_MatrixExport_{datetime.now().strftime('%Y%m%d')}.csv", mime="text/csv", use_container_width=True
                    )

    # --- TAB 3: READ-ONLY REGEX TARGET VISUALIZATION CURVES INVENTORY ---
    with tab_ref_library:
        st.subheader("📚 Theoretical Curve Inventory Profile")
        try:
            inventory_df = client.query(f"SELECT CurveID, COUNT(*) as Total_Coordinates, MIN(Day) as Day_Start, MAX(Day) as Day_End FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` GROUP BY CurveID ORDER BY CurveID").to_dataframe()
            if not inventory_df.empty: st.dataframe(inventory_df, use_container_width=True, hide_index=True)
            else: st.info("The reference curves repository metadata database is currently empty.")
        except Exception: st.warning("⚠️ Core table link `reference_curves` not discovered on BigQuery instance cluster.")

    # --- TAB 4: READ-ONLY AUDIT FOR THERMAL PROJECT SITE EVENTS OVERLAYS ---
    with tab_event_log:
        st.subheader("🚨 Historical Project Operational Events Registry Log")
        try:
            logs_q = f"""
                SELECT e.StartDate as Date, COALESCE(e.StartTime, '—') as Start_Time, COALESCE(e.EndDate, CAST(NULL AS DATE)) as Resolution_Date, 
                       CAST(e.Project AS STRING) as Project, COALESCE(e.Chiller, '—') as Chiller_Unit, e.TypeofEvent as Event_Classification, 
                       COALESCE(e.Notes, '—') as Operations_Summary, COALESCE(e.Cost, 0) as Logged_Cost
                FROM `{EVENTS_TABLE}` e ORDER BY e.StartDate DESC LIMIT 150
            """
            logs_df = client.query(logs_q).to_dataframe()
            if not logs_df.empty:
                st.dataframe(logs_df, use_container_width=True, hide_index=True, column_config={"Logged_Cost": st.column_config.NumberColumn("Cost ($)", format="$%.2f")})
            else: st.info("No active operations log files matched this context space.")
        except Exception as e: st.error(f"⚠️ Event Registry Link Fault: {e}")

    # --- TAB 5: READ-ONLY PIPELINE LOGISTICS CHILLER FLEET METRICS REGISTRY ---
    with tab_chiller_reg:
        st.subheader("❄️ Fleet Chiller Infrastructure Log Registry Stores")
        try:
            inventory_q = f"""
                WITH TimelineState AS (
                    SELECT CAST(Project AS STRING) as project_id, Chiller as chiller_id, StartDate as event_timestamp, TypeofEvent, Cost,
                           LEAD(StartDate) OVER(PARTITION BY Chiller ORDER BY StartDate ASC) as next_evt
                    FROM `{EVENTS_TABLE}` WHERE Chiller IS NOT NULL
                ),
                Durations AS (
                    SELECT chiller_id, MAX_BY(project_id, event_timestamp) as location_context, SUM(COALESCE(Cost, 0)) as accumulated_costs
                    FROM TimelineState GROUP BY chiller_id
                )
                SELECT c.Chiller_ID, c.chiller_type, c.purchase_date, c.initial_price, c.acquired_status,
                       COALESCE(d.location_context, 'Yard / Shop Staging') as active_deployment_site,
                       COALESCE(d.accumulated_costs, 0.0) as total_logged_maintenance_costs
                FROM `{CHILLER_REG_TABLE}` c LEFT JOIN Durations d ON c.Chiller_ID = d.chiller_id ORDER BY c.Chiller_ID ASC
            """
            inv_raw_df = client.query(inventory_q).to_dataframe()
            if not inv_raw_df.empty:
                st.dataframe(
                    inv_raw_df, use_container_width=True, hide_index=True,
                    column_config={
                        "initial_price": st.column_config.NumberColumn("Purchase Cost ($)", format="$%.2f"),
                        "total_logged_maintenance_costs": st.column_config.NumberColumn("Accrued Repairs ($)", format="$%.2f"),
                        "purchase_date": st.column_config.DateColumn("Date Purchased", format="MM/DD/YYYY")
                    }
                )
            else: st.info("ℹ️ Chiller infrastructure array catalogs contain zero active tracking logs.")
        except Exception as e: st.error(f"⚠️ Asset Catalog Core Failure: {e}")
# =============================================================================
# ADMIN HELPERS: ADVANCED MAINTENANCE & BULK APPROVAL WORKSPACE
# =============================================================================
def render_bulk_approval_controls():
    """Renders the top-level scope selection, filter parameters, and target status inputs."""
    c1, c2, c3 = st.columns(3)
    target_scope = c1.radio("Target Scope", ["Project Wide", "Specific Location", "Specific Node"], horizontal=True, key="blk_mgmt_target_scope")
    current_status_filter = c2.selectbox("Filter Current Designation Status:", options=["all", "all but null", "true", "null (streaming / unreviewed)", "masked", "office", "baddata"], key="blk_mgmt_current_status_filter", help="Limits updates only to data points that match this selected classification.")
    new_status = c3.selectbox("Set Approval Status To:", ["true", "masked", "office", "baddata"], key="blk_mgmt_new_status")
    return target_scope, current_status_filter, new_status


def build_bulk_approval_where_clause(reg_df, selected_project, target_scope, current_status_filter, f):
    """Constructs dynamic BigQuery SQL logical condition statements parsing telemetric data footprints."""
    where_clauses = []

    if selected_project != "All Projects":
        if target_scope == "Specific Node":
            where_clauses.append(f"NodeNum = '{f['scope_val']}'")
        elif target_scope == "Specific Location":
            loc_nodes = reg_df[(reg_df['Project'] == selected_project) & (reg_df['Location'] == f['scope_val'])]['NodeNum'].dropna().unique().tolist()
            where_clauses.append(f"NodeNum IN ({', '.join([f'\' {n} \'' for n in loc_nodes])})" if loc_nodes else "NodeNum = 'NONE'")
        else:
            proj_nodes = reg_df[reg_df['Project'] == selected_project]['NodeNum'].dropna().unique().tolist()
            where_clauses.append(f"NodeNum IN ({', '.join([f'\' {n} \'' for n in proj_nodes])})" if proj_nodes else "NodeNum = 'NONE'")
        where_clauses.append(f"Project = '{selected_project}'")
    else:
        where_clauses.append("Project IS NOT NULL")

    start_ts_str = f"{f['s_date'].strftime('%Y-%m-%d')} {f['s_time'].strftime('%H:%M:%S')}"

    if f["temporal_dir"] == "Between Range":
        where_clauses.append(f"timestamp BETWEEN '{start_ts_str}' AND '{f['e_date'].strftime('%Y-%m-%d')} {f['e_time'].strftime('%H:%M:%S')}'")
    elif f["temporal_dir"] in ["Older Than", "Newer Than"]:
        where_clauses.append(f"timestamp {'<' if f['temporal_dir'] == 'Older Than' else '>'} '{start_ts_str}'")
    
    if f["val_filter"] == "Above Threshold": where_clauses.append(f"temperature > {f['threshold']}")
    elif f["val_filter"] == "Below Threshold": where_clauses.append(f"temperature < {f['threshold']}")

    if current_status_filter != "all":
        if current_status_filter == "all but null": where_clauses.append("r.approve IS NOT NULL")
        elif current_status_filter == "null (streaming / unreviewed)": where_clauses.append("r.approve IS NULL")
        elif current_status_filter == "true": where_clauses.append("r.approve IS NULL")
        else: where_clauses.append(f"LOWER(CAST(r.approve AS STRING)) = '{str(current_status_filter).lower()}'")

    return " AND ".join(where_clauses)


def render_bulk_approval_filters(reg_df, selected_project, target_scope):
    """Renders temporal query range selectors alongside numeric threshold control blocks."""
    col_f1, col_f2, col_f3 = st.columns(3)
    
    with col_f1:
        temporal_dir = st.selectbox("Temporal Direction", ["Between Range", "Older Than", "Newer Than"], key="blk_mgmt_temp_dir")
        if temporal_dir == "Between Range":
            c_start, c_end = st.columns(2)
            s_date = c_start.date_input("Start Date", value=datetime.now().date() - timedelta(days=7), key="blk_mgmt_s_date")
            s_time = c_start.time_input("Start Time", value=datetime.min.time(), key="blk_mgmt_s_time")
            e_date = c_end.date_input("End Date", value=datetime.now().date(), key="blk_mgmt_e_date")
            e_time = c_end.time_input("End Time", value=datetime.max.time(), key="blk_mgmt_e_time")
        else:
            s_date = st.date_input("Target Date", value=datetime.now().date() - timedelta(days=7), key="blk_mgmt_single_date")
            s_time = st.time_input("Target Time", value=datetime.min.time(), key="blk_mgmt_single_time")
            e_date, e_time = None, None

    with col_f2:
        val_filter = st.selectbox("Value Filter", ["No Threshold", "Above Threshold", "Below Threshold"], key="blk_mgmt_val_filter")
        threshold = st.number_input("Threshold Value (°F)", value=100.0, key="blk_mgmt_threshold")

    with col_f3:
        if selected_project == "All Projects":
            st.info("Targeting **Global Scope Context** (All Active Deployment Zones)")
            scope_val = "ALL_PROJECTS"
        else:
            if target_scope == "Project Wide":
                st.info(f"Targeting all active strings in **{selected_project}**"); scope_val = selected_project
            elif target_scope == "Specific Location":
                scope_val = st.selectbox("Select Location", sorted(reg_df[reg_df['Project'] == selected_project]['Location'].dropna().unique().tolist()), key="blk_mgmt_loc_select")
            elif target_scope == "Specific Node":
                sel_loc = st.selectbox("First, Select Location Space", sorted(reg_df[reg_df['Project'] == selected_project]['Location'].dropna().unique().tolist()), key="blk_mgmt_loc_node_select")
                scope_val = st.selectbox("Then, Select Target Hardware Node ID", sorted(reg_df[(reg_df['Project'] == selected_project) & (reg_df['Location'] == sel_loc)]['NodeNum'].dropna().unique().tolist()), key="blk_mgmt_node_select")
            
    return {"temporal_dir": temporal_dir, "s_date": s_date, "s_time": s_time, "e_date": e_date, "e_time": e_time, "val_filter": val_filter, "threshold": threshold, "scope_val": scope_val}


def execute_bulk_approval_workspace(client, full_reg_df, selected_project, tab_logistics):
    """Main execution backend executing macro cleanups, flattening tables to 1h frequencies, and processing updates."""
    target_table, telemetry_table = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections", f"{PROJECT_ID}.{DATASET_ID}.master_data_view"

    st.title("⚡ Bulk Approval and Database Maintenance")
    st.divider()

    if "blk_mgmt_profile_df" not in st.session_state: st.session_state.blk_mgmt_profile_df = None
    if "blk_mgmt_total_points" not in st.session_state: st.session_state.blk_mgmt_total_points = 0

    # --- PART A: GLOBAL TELEMETRY OPTIMIZATION ENGINE ---
    st.header("🧹 Global Database Cleanup")
    st.write("Consolidate raw continuous records into compressed **1-decimal hourly averages**. Executing this system cleanup systematically purges rogue telemetric hardware anomalies outside ground freezing thresholds (-30°F and 120°F).")
    
    clean_col1, clean_col2 = st.columns(2)
    run_telemetry_cleanup = clean_col1.button("⚡ Run Global Database Cleanup & Hourly Consolidation", use_container_width=True)
    run_string_cleanup = clean_col2.button("🧹 Clean Approval Text 'true' to 'TRUE'", use_container_width=True)

    if run_telemetry_cleanup and client is not None:
        status_box = st.empty()
        try:
            status_box.markdown("⏳ **[1/3] Mapping current timeline data densities...**")
            sp_b = client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`").to_dataframe().iloc[0, 0]
            lord_b = client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`").to_dataframe().iloc[0, 0]
            
            status_box.markdown("🧹 **[2/3] Flattening SensorPush high-frequency streaming logs...**")
            client.query(f"CREATE OR REPLACE TEMP TABLE tmp_sp AS SELECT TIMESTAMP_TRUNC(timestamp, HOUR) as timestamp, NodeNum, ROUND(AVG(CAST(temperature AS NUMERIC)), 1) as temperature, MAX(rssi) as rssi FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` WHERE temperature >= -30.0 AND temperature <= 120.0 GROUP BY 1, 2; CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` AS SELECT timestamp, NodeNum, CAST(temperature AS FLOAT64) as temperature, rssi FROM tmp_sp;").result()
            
            status_box.markdown("🛰️ **[3/3] Flattening Lord Wireless logger dataset vectors...**")
            client.query(f"CREATE OR REPLACE TEMP TABLE tmp_ld AS SELECT TIMESTAMP_TRUNC(timestamp, HOUR) as timestamp, NodeNum, ROUND(AVG(CAST(temperature AS NUMERIC)), 1) as temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord` WHERE CAST(temperature AS NUMERIC) >= -30.0 AND CAST(temperature AS NUMERIC) <= 120.0 GROUP BY 1, 2; CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.raw_lord` AS SELECT timestamp, NodeNum, CAST(temperature AS FLOAT64) as temperature FROM tmp_ld;").result()
            
            st.cache_data.clear()
            sp_a = client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`").to_dataframe().iloc[0, 0]
            lord_a = client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`").to_dataframe().iloc[0, 0]
            
            status_box.empty(); st.success("🎉 Global Database Consolidation successfully completed!")
            st.dataframe(pd.DataFrame([
                {"Dataset Layer": "SensorPush Logs (raw_sensorpush)", "Before Tally": f"{sp_b:,}", "After Tally": f"{sp_a:,}", "Purged Over-Samples": f"{sp_b-sp_a:,}"},
                {"Dataset Layer": "Lord Array Pipes (raw_lord)", "Before Tally": f"{lord_b:,}", "After Tally": f"{lord_a:,}", "Purged Over-Samples": f"{lord_b-lord_a:,}"},
                {"Dataset Layer": "Combined Data Pool", "Before Tally": f"{sp_b+lord_b:,}", "After Tally": f"{sp_a+lord_a:,}", "Purged Over-Samples": f"{(sp_b+lord_b)-(sp_a+lord_a):,}"}
            ]), use_container_width=True, hide_index=True)
        except Exception as e: status_box.empty(); st.error(f"Database Consolidation Halted: {e}")

    if run_string_cleanup and client is not None:
        try:
            # Converts string variants like 'false' to standardized uppercase or 'masked' based on system guidelines
            job = client.query(f"UPDATE `{target_table}` SET approve = CASE WHEN LOWER(TRIM(approve)) = 'false' THEN 'MASKED' ELSE UPPER(TRIM(approve)) END WHERE LOWER(TRIM(approve)) IN ('true', 'false')")
            job.result()
            st.success(f"🎉 Standardization complete! Cleaned and mapped {job.num_dml_affected_rows:,} records inside the manual overrides database ledger.")
            st.cache_data.clear(); time.sleep(0.5); st.rerun()
        except Exception as e: st.error(f"String Alignment Operation Failed: {e}")

    # --- PART B: 2-STEP BULK COMPLIANCE & RECLASSIFICATION SYSTEM ---
    st.divider(); st.header("⚡ Bulk Approval and Data Status Change")
    st.info("💡 **Pre-flight Alert:** Please check that your context selection inside the sidebar panel matches your target parameters before launching batch commands.")
    
    target_scope, current_status_filter, new_status = render_bulk_approval_controls()
    st.divider()
    
    filters = render_bulk_approval_filters(full_reg_df, selected_project, target_scope)
    where_str = build_bulk_approval_where_clause(full_reg_df, selected_project, target_scope, current_status_filter, filters)
    aliased_where = where_str.replace("NodeNum", "t.NodeNum").replace("timestamp", "t.timestamp").replace("temperature", "t.temperature").replace("r.approve", "t.approval_status")
    
    def run_profile_audit():
        with st.spinner("Analyzing target metadata data counts..."):
            res = client.query(f"SELECT COALESCE(t.approval_status, 'NULL (Streaming / Unreviewed)') as Current_Designation_Status, COUNT(*) as Total_Captured_Points, FORMAT_TIMESTAMP('%m/%d/%Y', MIN(t.timestamp)) as Oldest_Log_Entry, FORMAT_TIMESTAMP('%m/%d/%Y', MAX(t.timestamp)) as Newest_Log_Entry FROM `{telemetry_table}` t WHERE {aliased_where} GROUP BY 1 ORDER BY 2 DESC").to_dataframe()
            if not res.empty:
                st.session_state.blk_mgmt_profile_df, st.session_state.blk_mgmt_total_points = res, res['Total_Captured_Points'].sum()
            else:
                st.session_state.blk_mgmt_profile_df, st.session_state.blk_mgmt_total_points = pd.DataFrame(), 0

    if st.button("🔍 Step 1: Verify Match Count & Current Status Profiles", key="blk_mgmt_verify_btn", use_container_width=True):
        try: run_profile_audit()
        except Exception as e: st.error(f"Verification Matrix Compilation Failed: {e}")

    if st.session_state.blk_mgmt_profile_df is not None:
        if not st.session_state.blk_mgmt_profile_df.empty:
            st.subheader("📊 Current Target Coordinate Status Profiles")
            st.dataframe(st.session_state.blk_mgmt_profile_df, use_container_width=True, hide_index=True)
            st.metric("Total Match Point Pool in Selection Scope Context", f"{st.session_state.blk_mgmt_total_points:,}")
        else: st.warning("Zero logged telemetric records discovered inside this configuration scope.")

    st.divider(); st.info(f"Target Designation Mask for matched points: **{new_status.upper()}**")
    
    if st.checkbox("I authorize updating these specific data coordinates to the target status variables specified.", key="confirm_blk_mgmt"):
        if st.button(f"🚀 Step 2: Execute Status Override to {new_status.upper()}", key="exec_blk_mgmt_btn", use_container_width=True):
            sql = (
                f"DELETE FROM `{target_table}` WHERE STRUCT(NodeNum, timestamp) IN (SELECT AS STRUCT t.NodeNum, t.timestamp FROM `{telemetry_table}` t WHERE {aliased_where})"
                if new_status.upper() == "TRUE" else
                f"MERGE `{target_table}` T USING (SELECT DISTINCT t.NodeNum, t.timestamp FROM `{telemetry_table}` t WHERE {aliased_where}) S ON T.NodeNum = S.NodeNum AND T.timestamp = S.timestamp WHEN MATCHED THEN UPDATE SET approve = '{new_status.upper()}' WHEN NOT MATCHED THEN INSERT (NodeNum, timestamp, approve) VALUES (S.NodeNum, S.timestamp, '{new_status.upper()}');"
            )
            try:
                with st.spinner("Executing structural batch status overrides..."):
                    job = client.query(sql); job.result()
                st.success(f"✅ Batch override completed successfully! Updated {job.num_dml_affected_rows:,} record streams inside manual_rejections.")
                st.cache_data.clear(); run_profile_audit(); st.balloons(); time.sleep(0.5); st.rerun()
            except Exception as e: st.error(f"Transaction execution failed: {e}"); st.code(sql, language="sql")


def render_data_checker(client, full_reg_df):
    """Quality assurance diagnostics engine highlighting sensor config mismatches or orphan device risks."""
    st.divider(); st.markdown("### 🔍 System Registry Diagnostics Audit")
    with st.expander("📊 View Discovered Inventory Conflict Logs", expanded=False):
        try:
            orphan_df = client.query(f"SELECT DISTINCT r.NodeNum, r.Project, r.Location FROM `{PROJECT_ID}.{DATASET_ID}.node_registry` r LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.hardware_inventory` i ON TRIM(r.NodeNum) = TRIM(i.NodeNum) WHERE i.NodeNum IS NULL AND r.NodeNum IS NOT NULL ORDER BY r.NodeNum ASC").to_dataframe()
            if not orphan_df.empty:
                st.warning("⚠️ **Orphan Sensor Alert:** The following active field nodes are mapping telemetry inside data views but do not exist in the hardware catalog:")
                st.dataframe(orphan_df, use_container_width=True, hide_index=True)
            else: st.success("✅ Complete Data Alignment: All registered active node loops match safely with the hardware warehouse directories.")
        except Exception as e: st.caption(f"Integrity script matrix is loading or offline: {e}")

# =============================================================================
# DATA RECOVERY REQUISITE ENGINE HELPERS
# =============================================================================
def render_recovery_filters(sp_reg):
    """Renders read-only hierarchical dropdown selections and returns targeted Node arrays."""
    st.subheader("🔍 Select Target Hardware Path")
    c1, c2, c3 = st.columns(3)
    
    u_projects = ["All"] + sorted(sp_reg['Project'].dropna().unique().tolist())
    rec_proj = c1.selectbox("Select Project Space Context:", u_projects, key="rec_proj_sel_isolated")
    
    proj_filtered = sp_reg if rec_proj == "All" else sp_reg[sp_reg['Project'] == rec_proj]
    u_locs = ["All"] + sorted(proj_filtered['Location'].dropna().unique().tolist(), key=natural_sort_key)
    rec_loc = c2.selectbox("Select Physical Location Context:", u_locs, key="rec_loc_sel_isolated")
    
    loc_filtered = proj_filtered if rec_loc == "All" else proj_filtered[proj_filtered['Location'] == rec_loc]
    return c3.multiselect("Select Target Node Numbers", sorted(loc_filtered['NodeNum'].dropna().unique().tolist(), key=natural_sort_key), default=None, key="rec_nodes_multiselect_isolated")

# =============================================================================
# MAIN INTERFACE MODULE: CENTRAL ADMINISTRATIVE COMMAND CENTER
# =============================================================================
def render_admin_page(selected_project, display_tz, unit_mode, unit_label, active_refs):
    """Central analytical administrative supervisor console streaming clean Google Sheets source records."""
    st.header("🛠️ Admin Tools")
    client = get_bq_client()
    if client is None: st.error("Database connection unavailable."); return

    # Core Read-Only Matrix Data Pull
    try:
        full_reg_df = load_lab_node_registry_data(NODE_REGISTRY_TABLE)
        available_projects_list = sorted(client.query(f"SELECT Project FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE ProjectStatus != 'Archived'").to_dataframe()['Project'].dropna().unique().tolist())
    except Exception as e: st.error(f"Registry Link Offline: {e}"); return

    # Standardized 7-Tab Layout Schema Paths
    tab_admin_sum, tab_bulk_app, tab_logistics, tab_recovery, tab_proj_master, tab_bulk_config, tab_chillers = st.tabs([
        "📋 Admin Summary", "⚡ Bulk Approval", "📋 Node Master", "📡 Data Recovery", "⚙️ Project Master", "📦 Bulk Uploads", "❄️ Chiller Operations"
    ])
    
    # --- SUB-TAB 1: ADMIN HARDWARE AND MATRIX SUMMARY ---
    with tab_admin_sum:
        st.subheader("📋 Centralized Infrastructure Status Overview")
        st.markdown("### 📡 Hardware Inventory Fleet Breakdown")
        try:
            def classify_family(node): return "Lord" if "-ch" in str(node).lower() else "SP" if str(node).lower().startswith("sp") else "TP" if str(node).lower().startswith("tp") else "Other"
            fleet_df = full_reg_df.copy()
            fleet_df['Hardware Family'] = fleet_df['NodeNum'].apply(classify_family)
            fleet_df['Parent ID'] = fleet_df['NodeNum'].apply(lambda x: re.split(r'(?i)-ch', str(x))[0] if "-ch" in str(x).lower() else x)
            fleet_df['is_active'] = fleet_df['End_Date'].isna()
            
            deduped = fleet_df.sort_values(by=['Parent ID', 'is_active'], ascending=[True, False]).drop_duplicates(subset=['Parent ID']).copy()
            pivot = deduped.groupby(['Hardware Family', 'SensorStatus']).size().unstack(fill_value=0).reindex(["TP", "SP", "Lord", "Other"], fill_value=0)
            for col in ["Available", "Dead", "Diagnostic", "On Project"]: 
                if col not in pivot.columns: pivot[col] = 0
            pivot = pivot[["Available", "Dead", "Diagnostic", "On Project"]]
            pivot['Total Units'] = pivot.sum(axis=1)
            st.dataframe(pivot.reset_index(), use_container_width=True, hide_index=True)
        except Exception as e: st.caption(f"Inventory matrix loading: {e}")

        st.divider(); st.markdown("### 🏗️ Active Deployment Overview Matrix")
        try:
            sum_q = f"SELECT p.Project, p.ProjectName, p.ProjectStatus, p.Date_Freezedown, COUNT(DISTINCT n.NodeNum) as Mapped_Sensors, COUNT(DISTINCT CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR) THEN n.NodeNum END) as Active_6h, COUNT(DISTINCT CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN n.NodeNum END) as Active_24h FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` p LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.node_registry` n ON p.Project = n.Project LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.master_data_view` m ON n.NodeNum = m.NodeNum WHERE n.End_Date IS NULL AND p.ProjectStatus IN ('Freezedown', 'Maintenance', 'Pre-freeze') AND UPPER(p.Project) NOT LIKE '%OFFICE%' GROUP BY 1,2,3,4 ORDER BY p.Project ASC"
            rows = []
            for _, r in client.query(sum_q).to_dataframe().iterrows():
                elapsed = max(0, (pd.Timestamp.now(tz=display_tz).date() - pd.to_datetime(r['Date_Freezedown']).date()).days) if pd.notnull(r['Date_Freezedown']) else 0
                rows.append({"Project ID": r['Project'], "Project Name": r['ProjectName'] or r['Project'], "Mapped Sensors": int(r['Mapped_Sensors']), "Active (6h)": int(r['Active_6h']), "Active (24h)": int(r['Active_24h']), "Project Status Timeline": f"Day {elapsed} of {str(r['ProjectStatus']).title()}" if pd.notnull(r['Date_Freezedown']) else "Not Freezing"})
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        except Exception as e: st.error(f"Upgraded overview fault: {e}")

    # --- SUB-TAB 2: BULK APPROVAL DESK ROUTING ---
    with tab_bulk_app:
        execute_bulk_approval_workspace(client, full_reg_df, selected_project, tab_logistics)
        
    # --- SUB-TAB 3: READ-ONLY ASSET TRACKING NODE MASTER ---
    with tab_logistics:
        st.title("📋 Real-Time Asset Configuration Matrix")
        c1, c2, c3 = st.columns(3)
        sel_p = c1.selectbox("Filter Project Context:", sorted(list(set(["Office"] + full_reg_df['Project'].dropna().unique().tolist()))), key="node_master_p")
        p_filtered = full_reg_df[full_reg_df['Project'] == sel_p]
        sel_l = c2.selectbox("Filter Location Context:", sorted(p_filtered['Location'].dropna().unique().tolist(), key=natural_sort_key) if not p_filtered.empty else ["Office"], key="node_master_l")
        l_filtered = p_filtered[p_filtered['Location'] == sel_l] if not p_filtered.empty else pd.DataFrame()
        sel_n = c3.selectbox("Select Target Sensor ID:", sorted(l_filtered['NodeNum'].dropna().unique().tolist(), key=natural_sort_key) if not l_filtered.empty else [], key="node_master_n")
        
        if sel_n:
            st.markdown(f"### 🕒 Timeline History: **{sel_n}**")
            st.dataframe(client.query(f"SELECT Project, Location, Bank, Depth, CAST(Start_Date AS STRING) as Deployment_Date, COALESCE(CAST(End_Date AS STRING), 'Active') as Cutoff_Date, SensorStatus FROM `{NODE_REGISTRY_TABLE}` WHERE NodeNum = '{sel_n}' ORDER BY Start_Date DESC").to_dataframe(), use_container_width=True, hide_index=True)
            render_data_checker(client, full_reg_df)

    # --- SUB-TAB 4: SENSORPUSH API CLOUD RECOVERY BACKFILL ENGINE ---
    with tab_recovery:
        st.title("📡 Data Recovery Engine")
        st.write("Extract missing data matrices from the SensorPush API framework and execute batch appends straight to raw tables.")
        tgt_nodes = render_recovery_filters(full_reg_df)
        
        c_rec1, c_rec2 = st.columns(2)
        r_start = c_rec1.date_input("Extraction Window Start Date", value=datetime.now().date() - timedelta(days=2), key="dt_rec_start")
        r_end = c_rec2.date_input("Extraction Window End Date", value=datetime.now().date(), key="dt_rec_end")
        
        final_nodes = tgt_nodes if tgt_nodes else sorted((full_reg_df[(full_reg_df['Project'] == st.session_state.get('rec_proj_sel_isolated', 'All')) & (full_reg_df['Location'] == st.session_state.get('rec_loc_sel_isolated', 'All'))] if st.session_state.get('rec_proj_sel_isolated', 'All') != "All" else full_reg_df)['NodeNum'].dropna().unique().tolist())
        st.warning(f"⚠️ Backfill Action Plan: Querying cloud servers for {len(final_nodes)} nodes from {r_start} to {r_end}.")

        if st.button("🚀 Execute Cloud Backfill Ingestion Pipeline Run", use_container_width=True, key="btn_trigger_recovery_run"):
            all_rows, h_map, rev_map, b_marks, account_stats = [], {}, {}, {}, {}
            ACCOUNTS = [{'email': 'ldunham@soilfreeze.com', 'password': 'Freeze123!!'}, {'email': 'tsteele@soilfreeze.com', 'password': 'Freeze123!!'}, {'email': 'soilfreeze98072@gmail.com', 'password': 'Freeze123!!'}]
            
            with st.status("Executing API extraction pipelines...", expanded=True) as status_box:
                try:
                    for row in client.query(f"SELECT RawID, NodeNum FROM `{PROJECT_ID}.{DATASET_ID}.hardware_inventory` WHERE RawID IS NOT NULL"):
                        rid, fn = str(row.RawID).split('.')[0].strip(), str(row.NodeNum).strip()
                        h_map[rid] = fn; rev_map[fn] = rid
                except Exception as e: st.error(f"Hardware inventory lookup failed: {e}"); st.stop()
                
                for row in client.query(f"SELECT NodeNum, FORMAT_TIMESTAMP('%m/%d/%Y %H:%M UTC', MAX(timestamp)) as max_time FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` GROUP BY NodeNum"): b_marks[str(row.NodeNum)] = str(row.max_time)
                
                iso_s = datetime.combine(r_start, datetime.min.time()).strftime('%Y-%m-%dT%H:%M:%SZ')
                iso_e = datetime.combine(r_end, datetime.max.time()).strftime('%Y-%m-%dT%H:%M:%SZ')
                
                for acc in ACCOUNTS:
                    account_stats[acc['email']] = 0
                    try:
                        auth_r = requests.post("https://api.sensorpush.com/api/v1/oauth/authorize", json=acc, timeout=15).json()
                        token = requests.post("https://api.sensorpush.com/api/v1/oauth/accesstoken", json={"authorization": auth_r['authorization']}, timeout=15).json().get('accesstoken')
                        s_resp = requests.post("https://api.sensorpush.com/api/v1/devices/sensors", headers={"Authorization": token}, json={}, timeout=20).json()
                        rssi_map = {str(sid).strip(): sm.get('rssi') for sid, sm in s_resp.items() if isinstance(sm, dict)}
                        
                        r_samples = requests.post("https://api.sensorpush.com/api/v1/samples", headers={"Authorization": token}, json={"startTime": iso_s, "endTime": iso_e, "limit": 100000}, timeout=60).json()
                        for sid, samples in r_samples.get('sensors', {}).items():
                            fn = h_map.get(str(sid).split('.')[0].strip()) or next((n for n in final_nodes if rev_map.get(n) == str(sid).split('.')[0].strip()), None)
                            if fn in final_nodes:
                                current_rssi = rssi_map.get(str(sid).strip())
                                for s in samples:
                                    temp = s.get('temp_f') or s.get('temperature')
                                    if temp is not None:
                                        account_stats[acc['email']] += 1
                                        all_rows.append({"timestamp": pd.to_datetime(s['observed']), "NodeNum": str(fn), "temperature": float(temp), "rssi": float(current_rssi) if current_rssi is not None else None})
                    except Exception: continue

                if all_rows:
                    up_df = pd.DataFrame(all_rows)
                    up_df['timestamp'] = pd.to_datetime(up_df['timestamp'], utc=True)
                    load_config = bigquery.LoadJobConfig(schema=[bigquery.SchemaField("timestamp", "TIMESTAMP"), bigquery.SchemaField("NodeNum", "STRING"), bigquery.SchemaField("temperature", "FLOAT"), bigquery.SchemaField("rssi", "FLOAT")], write_disposition="WRITE_APPEND")
                    client.load_table_from_dataframe(up_df, f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush", job_config=load_config).result()
                    status_box.update(label="Recovery Append Complete!", state="complete")
                    st.success(f"🎉 Success! Extracted and appended {len(up_df):,} lines."); st.cache_data.clear()
                else: status_box.update(label="Cloud sync found 0 rows for selected constraints.", state="complete")

    # --- SUB-TAB 5: READ-ONLY PROJECT TIMELINE DIRECTORY ---
    with tab_proj_master:
        st.subheader("🗄️ Complete Master Project Lifecycle Directory")
        st.dataframe(client.query(f"SELECT Project as `Project ID`, ProjectName as `Friendly Name`, ProjectStatus as `Operational Phase`, Date_Freezedown as `Freezedown Date`, City, Timezone FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` ORDER BY Project ASC").to_dataframe(), use_container_width=True, hide_index=True)

    # --- SUB-TAB 6: INBOUND BULK ACCELERATION ENGINE VIA DATA SPREADSHEETS ---
    with tab_bulk_config:
        st.subheader("📦 Centralized Bulk Ingestion Engine")
        cfg_mode = st.radio("Select Allocation Ingestion Target Engine:", ["Update Hardware Inventory", "Update Node Registry"], horizontal=True, key="bulk_uploads_engine_radio")
        
        if cfg_mode == "Update Hardware Inventory":
            st.info("Ingest spreadsheet data maps to append fresh units onto your hardware inventory directory table. Required Columns: `RawID`, `NodeNum`.")
            u_file = st.file_uploader("Upload Inventory Dataset File", type=["csv", "xlsx"], key="bulk_inv_file_uploader")
            if u_file and st.button("🚀 Commit Inventory Changes", use_container_width=True):
                df_upload = pd.read_csv(u_file, dtype=str) if u_file.name.endswith('.csv') else pd.read_excel(u_file, dtype=str)
                cols_map = {str(c).strip().lower(): str(c) for c in df_upload.columns}
                if 'rawid' in cols_map and 'nodenum' in cols_map:
                    clean_u = pd.DataFrame({'RawID': df_upload[cols_map['rawid']].astype(str).str.strip().str.split('.').str[0], 'NodeNum': df_upload[cols_map['nodenum']].astype(str).str.strip()}).dropna()
                    client.load_table_from_dataframe(clean_u, f"{PROJECT_ID}.{DATASET_ID}.hardware_inventory", job_config=bigquery.LoadJobConfig(schema=[bigquery.SchemaField("RawID", "STRING"), bigquery.SchemaField("NodeNum", "STRING")], write_disposition="WRITE_APPEND")).result()
                    st.success("🎉 Inventory catalog updated successfully!"); st.cache_data.clear(); time.sleep(0.5); st.rerun()
                else: st.error("Spreadsheet layout missing required columns.")

        elif cfg_mode == "Update Node Registry":
            st.info("Mass register field loops or append configuration settings to production registry timelines using data spreadsheets.")
            u_csv = st.file_uploader("Upload Registry Deployment Map File", type="csv", key="bulk_reg_csv_uploader")
            if u_csv and st.button("🚀 Commit Registry Changes", use_container_width=True):
                df_u = pd.read_csv(u_csv)
                if {'NodeNum', 'Project', 'Location'}.issubset(df_u.columns):
                    df_u['Start_Date'] = pd.to_datetime(df_u['Start_Date'], errors='coerce').dt.strftime('%Y-%m-%d') if 'Start_Date' in df_u.columns else datetime.now().strftime('%Y-%m-%d')
                    df_u['SensorStatus'] = df_u['SensorStatus'].fillna('On Project') if 'SensorStatus' in df_u.columns else 'On Project'
                    for c in ['NodeNum', 'Project', 'Location']: df_u[c] = df_u[c].astype(str).str.strip()
                    if 'Bank' in df_u.columns: df_u['Bank'] = df_u['Bank'].fillna('').astype(str).str.strip()
                    if 'Depth' in df_u.columns: df_u['Depth'] = pd.to_numeric(df_u['Depth'], errors='coerce').fillna(0.0)
                    client.load_table_from_dataframe(df_u[[c for c in df_u.columns if c != 'PhysicalID']], NODE_REGISTRY_TABLE, job_config=bigquery.LoadJobConfig(schema=[bigquery.SchemaField("NodeNum", "STRING"), bigquery.SchemaField("Project", "STRING"), bigquery.SchemaField("Location", "STRING"), bigquery.SchemaField("Bank", "STRING"), bigquery.SchemaField("Depth", "FLOAT"), bigquery.SchemaField("SensorStatus", "STRING"), bigquery.SchemaField("Start_Date", "DATE")], write_disposition="WRITE_APPEND")).result()
                    st.success("🎉 Matrix allocations appended safely onto timeline tracking logs!"); st.cache_data.clear(); time.sleep(0.5); st.rerun()

    # --- SUB-TAB 7: READ-ONLY MECHANICAL PLANT CHILLERS OVERVIEW ---
    with tab_chillers:
        st.subheader("❄️ Mechanical Chiller Fleet Deployment Manifest")
        try:
            st.dataframe(client.query(f"SELECT c.Chiller_ID as `Chiller Loop ID`, c.chiller_type as Specifications, c.status as Status, COALESCE(STRING_AGG(m.Location, ', '), 'Staging / Shop') as `Deployment Location Context` FROM `{PROJECT_ID}.{DATASET_ID}.chiller_registry` c LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.project_systems_map` m ON c.Chiller_ID = m.Chiller GROUP BY 1,2,3 ORDER BY 1 ASC").to_dataframe(), use_container_width=True, hide_index=True)
        except Exception as e: st.error(f"Asset Manifest Offline: {e}")
# =============================================================================
# 🛠️ REUSABLE LAB ENGINE ASSIGNMENT PIPELINES
# =============================================================================
@st.cache_data(ttl=300)
def load_lab_node_registry_data(target_table):
    """Safely assembles asset inventories with matching real-time ping lag windows and configurations."""
    client = get_bq_client()
    if client is None: return pd.DataFrame()
    try:
        master_query = f"""
            WITH LatestTelemetry AS (
                SELECT NodeNum, MAX(timestamp) as last_ping,
                       ARRAY_AGG(temperature ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] as last_temp
                FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` GROUP BY NodeNum
            ),
            AssignmentWindows AS (
                SELECT NodeNum, Start_Date, COALESCE(End_Date, CURRENT_DATE()) AS Effective_End,
                       DATE_DIFF(COALESCE(End_Date, CURRENT_DATE()), Start_Date, DAY) * 24 AS Expected_Hours
                FROM `{target_table}` WHERE Project != 'Dead'
            ),
            ActualProjectPings AS (
                SELECT m.NodeNum, a.Start_Date, COUNT(DISTINCT TIMESTAMP_TRUNC(m.timestamp, HOUR)) AS Actual_Pings_Logged
                FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
                INNER JOIN AssignmentWindows a ON m.NodeNum = a.NodeNum 
                    AND EXTRACT(DATE FROM m.timestamp) BETWEEN a.Start_Date AND a.Effective_End
                GROUP BY m.NodeNum, a.Start_Date
            )
            SELECT R.*, T.last_ping, T.last_temp, A.Expected_Hours, COALESCE(P.Actual_Pings_Logged, 0) AS Actual_Pings_Logged
            FROM `{target_table}` R
            LEFT JOIN LatestTelemetry T ON R.NodeNum = T.NodeNum
            LEFT JOIN AssignmentWindows A ON R.NodeNum = A.NodeNum AND R.Start_Date = A.Start_Date
            LEFT JOIN ActualProjectPings P ON R.NodeNum = P.NodeNum AND R.Start_Date = P.Start_Date
        """
        df = client.query(master_query).to_dataframe()
        now_utc = pd.Timestamp.now(tz='UTC')
        
        if not df.empty and 'last_ping' in df.columns:
            df['hours_hidden'] = df['last_ping'].apply(lambda x: (now_utc - pd.to_datetime(x).tz_convert('UTC')).total_seconds() / 3600.0 if pd.notnull(x) else float('inf'))
            df['hours_hidden'] = pd.to_numeric(df['hours_hidden'], errors='coerce').fillna(float('inf'))
            
            def format_last_seen(hours):
                if pd.isna(hours) or hours == float('inf'): return "❌ Never"
                if hours < 1.0:
                    mins = int(hours * 60)
                    return f"{mins}m ago" if mins > 0 else "Just now"
                return f"{hours:.1f}h ago"
            df['Last Seen'] = df['hours_hidden'].apply(format_last_seen)
        else:
            df['hours_hidden'], df['Last Seen'] = float('inf'), "❌ Never"
            
        if not df.empty and 'Expected_Hours' in df.columns:
            exp_hours = pd.to_numeric(df['Expected_Hours'], errors='coerce').fillna(0)
            act_pings = pd.to_numeric(df['Actual_Pings_Logged'], errors='coerce').fillna(0)
            raw_efficiency = np.where(exp_hours <= 0, 0.0, np.minimum(100.0, np.round((act_pings / exp_hours) * 100, 1)))
            df['Reporting Efficiency'] = [f"{x:.1f}%" for x in raw_efficiency]
        else:
            df['Reporting Efficiency'] = "0.0%"
            
        cols_to_drop = ['physicalID', 'PhysicalID', 'last_ping', 'Expected_Hours', 'Actual_Pings_Logged']
        df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors='ignore')
        return df.sort_values(by='hours_hidden', ascending=True).reset_index(drop=True)
    except Exception as e:
        st.error(f"Error compiling registry: {e}"); return pd.DataFrame()


def render_lab_node_selector(reg_df, proj_list):
    """Renders hierarchical dropdown filters and read-only allocation tables."""
    st.subheader("🎯 Active Node Registry")
    hide_archived = st.checkbox("Hide Archived Records", value=True, key="lab_ns_hide_archived_toggle")
    
    df = reg_df.copy()
    if hide_archived and 'SensorStatus' in df.columns:
        df = df[(df['SensorStatus'].str.lower() != "archived") & (~df['Location'].str.contains("Archive", case=False, na=False))]

    c1, c2, c3 = st.columns(3)
    f_proj = c1.selectbox("Filter by Project Space", ["All", "Unassigned"] + proj_list, key="lab_ns_proj_f")
    loc_opts = df['Location'].dropna().unique().tolist() if f_proj == "All" else df[df['Project'] == f_proj]['Location'].dropna().unique().tolist()
    f_loc = c2.selectbox("Filter by Physical Location", ["All"] + sorted(loc_opts), key="lab_ns_loc_f")
    search_term = c3.text_input("Global Search (Node ID)", "", key="lab_ns_search_f")

    if f_proj == "Unassigned": 
        df = df[df['Project'].isna() | (df['Project'] == "") | (df['Project'] == "Office")]
    elif f_proj != "All": 
        df = df[df['Project'] == f_proj]
        
    if f_loc != "All": df = df[df['Location'] == f_loc]
    if search_term: df = df[df['NodeNum'].str.contains(search_term, case=False, na=False)]

    if df.empty:
        st.info("No matching nodes located under current filter parameters."); return None

    df = df.reset_index(drop=True)
    st.markdown("### 📋 Current Asset Allocation Matrix")
    
    unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
    unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"
    
    df['Position'] = df.apply(lambda r: f"{r['Depth']}ft" if (pd.notnull(r.get('Depth')) and r.get('Depth') != 0) else f"Bank {r.get('Bank', '-')}", axis=1)
    df['Current Temp'] = df['last_temp'].apply(lambda x: f"{x:.1f}{unit_label}" if pd.notnull(x) else "N/A")

    # Clean display configuration containing Phase and System variables
    st.dataframe(
        df, hide_index=True, use_container_width=True,
        column_order=["Project", "Phase", "System", "Location", "NodeNum", "Position", "Last Seen", "Current Temp"]
    )
    return None


def render_lab_data_checker(client, reg_df):
    """Calculates and monitors systemic data conflicts and timeline overlaps across the grid table rows."""
    st.markdown("### 🔍 Data Checker Diagnostics")
    if reg_df.empty:
        st.info("The system node registry is unpopulated. Skipping automated integrity scans."); return

    active_registry_table = f"{PROJECT_ID}.{DATASET_ID}.node_registry"
    c1, c2, c3, c4 = st.tabs(["⏱️ Gaps in Data", "🚨 Open End Windows", "🚨 Overlapping Timelines", "🚨 Coordinate Collisions"])
    
    # --- TAB 1: DATA GAPS ---
    with c1:
        st.markdown("#### ⏱️ Chronological Gap Analysis")
        try:
            gap_df = client.query(f"WITH OrderedAssignments AS (SELECT NodeNum, Project, Start_Date, End_Date, LEAD(Start_Date) OVER (PARTITION BY NodeNum ORDER BY Start_Date ASC) as next_start FROM `{active_registry_table}`) SELECT NodeNum as `Node ID`, Project as `Ended Project ID`, End_Date as `Decommission Date`, next_start as `Next Deployment Date`, DATE_DIFF(next_start, End_Date, DAY) as `Unmonitored Gap (Days)` FROM OrderedAssignments WHERE End_Date IS NOT NULL AND next_start IS NOT NULL AND DATE_DIFF(next_start, End_Date, DAY) > 1 ORDER BY `Unmonitored Gap (Days)` DESC").to_dataframe()
            if not gap_df.empty:
                st.error("⚠️ **Timeline Discontinuity Warning:** Hardware sensors contain unmonitored gaps between active structural allocations without an intermediate 'Office' window:")
                st.dataframe(gap_df, use_container_width=True, hide_index=True)
            else: st.success("✅ **Chronological Integrity Verified:** No unmonitored calendar gaps located.")
        except Exception as e: st.caption(f"Integrity metrics processing: {e}")

    # --- TAB 2: OPEN ENDS ---
    with c2:
        st.markdown("#### 🚨 Open-Ended Terminations Checklist")
        try:
            orphan_df = client.query(f"WITH ActiveCounts AS (SELECT NodeNum, COUNT(*) as open_windows FROM `{active_registry_table}` WHERE End_Date IS NULL GROUP BY NodeNum) SELECT r.NodeNum as `Node ID`, r.Project as `Project ID`, r.Location as `Location / Borehole`, r.Start_Date as `Deployment Start` FROM `{active_registry_table}` r JOIN ActiveCounts a ON r.NodeNum = a.NodeNum WHERE r.End_Date IS NULL AND a.open_windows > 1 ORDER BY r.NodeNum ASC, r.Start_Date ASC").to_dataframe()
            if not orphan_df.empty:
                st.error("⚠️ **Orphaned Open-End Alert:** The following tracking rows have multiple active deployment windows without an End_Date clamp down:")
                st.dataframe(orphan_df, use_container_width=True, hide_index=True)
            else: st.success("✅ **Clean Terminations Verified:** No historical boundary overlap risks found.")
        except Exception as e: st.caption(f"Orphan scan component offline: {e}")

    # --- TAB 3: TIMELINE OVERLAPS ---
    with c3:
        st.markdown("#### 🚨 Timeline Window Overlap Scans")
        try:
            overlap_df = client.query(f"SELECT t1.NodeNum as `Node ID`, t1.Project as `Proj A`, t1.Start_Date as `Start A`, t1.End_Date as `End A`, t2.Project as `Proj B`, t2.Start_Date as `Start B`, t2.End_Date as `End B` FROM `{active_registry_table}` t1 JOIN `{active_registry_table}` t2 ON t1.NodeNum = t2.NodeNum AND t1.Start_Date < t2.Start_Date AND (t1.End_Date IS NULL OR t1.End_Date > t2.Start_Date) ORDER BY t1.NodeNum ASC").to_dataframe()
            if not overlap_df.empty:
                st.error("⚠️ **Simultaneous Allocation Conflict:** Telemetry streams show parallel assignments sharing identical date ranges:")
                st.dataframe(overlap_df, use_container_width=True, hide_index=True)
            else: st.success("✅ **Clean Database Entries:** No overlapping operational schedule windows found.")
        except Exception as e: st.caption(f"Window scan engine initializing: {e}")

    # --- TAB 4: COORDINATE COLLISIONS ---
    with c4:
        st.markdown("#### 🚨 Position and Coordinate Collision Check")
        try:
            spatial_df = client.query(f"SELECT Project as `Project ID`, Location, COALESCE(CAST(Depth AS STRING), CONCAT('Bank ', Bank)) as `Coordinate Position`, STRING_AGG(NodeNum, ' ↔️ ') as `Conflicting Node Group`, COUNT(*) as `Active Hardware Count` FROM `{active_registry_table}` WHERE End_Date IS NULL AND Project != 'Office' AND Location != 'Office' GROUP BY Project, Location, Bank, Depth HAVING COUNT(*) > 1 ORDER BY Project ASC, Location ASC").to_dataframe()
            if not spatial_df.empty:
                st.error("⚠️ **Spatial Grid Collision Detected:** Multiple hardware sensors are assigned to the exact same physical borehole coordinate position simultaneously:")
                st.dataframe(spatial_df, use_container_width=True, hide_index=True)
            else: st.success("✅ **Perfect Grid Alignment:** Every position coordinate safely holds exactly one active sensor asset mapping line.")
        except Exception as e: st.caption(f"Spatial proximity audit processing: {e}")


# =============================================================================
# 12. CENTRALIZED APPLICATION ROUTING SYSTEM
# =============================================================================
display_tz = st.session_state.get("display_tz", "UTC")
unit_label = st.session_state.get("unit_label", "°F")
unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
active_refs = st.session_state.get("active_refs", [])

client = get_bq_client() 

# FIXED: Aligned "Summary" to match the sidebar selection string exactly
if page == "Summary":
    render_summary_dashboard(unit_label, unit_mode, display_tz)

elif page == "Time vs Temp":
    render_global_overview(selected_project, st.session_state.get('project_metadata'), display_tz) 

elif page == "Depth Charts":
    render_depth_charts(selected_project, unit_label, display_tz)

elif page == "Sensor Status":
    render_sensor_status(client, selected_project, unit_label, unit_mode, display_tz)

elif page == "Node Diagnostics":
    render_node_diagnostics(selected_project, display_tz, unit_label)

elif page in ["Data Processing", "Admin Tools"]:
    if st.session_state.get('authenticated', False):
        if page == "Data Processing":
            render_data_processing_page(selected_project)
        elif page == "Admin Tools":
            render_admin_page(selected_project, display_tz, unit_mode, unit_label, active_refs)
    else:
        st.divider()
        c1, c2, c3 = st.columns([1, 2, 1])
        with c2:
            st.subheader("🔐 Restricted Admin Access")
            pwd = st.text_input("Enter Admin Password", type="password")
            if st.button("Unlock Dashboard", use_container_width=True):
                if pwd == st.secrets.get("admin_password", "Freeze123!!"):
                    st.session_state['authenticated'] = True
                    st.rerun()
                else:
                    st.error("Invalid Password. Access Denied.")
