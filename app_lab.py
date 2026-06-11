import streamlit as st
import pandas as pd
import time
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta, timezone, date, time as dt_time
import pytz
import traceback
import io
import re
import numpy as np

# 1. CONFIGURATION & STYLING
st.set_page_config(
    page_title="SoilFreeze Data Lab", 
    page_icon="❄️", 
    layout="wide"
)

# Global Database Constants
DATASET_ID = "Temperature" 
PROJECT_ID = "sensorpush-export"
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"

# Redirection pointed directly to live federated Google Sheet tables
PROJECT_REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.project_registry"
NODE_REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.node_registry"

@st.cache_resource
def get_bq_client():
    """
    Initializes and caches the BigQuery connection.
    Prioritizes Service Account info from st.secrets for Streamlit Cloud.
    """
    try:
        SCOPES = [
            "https://www.googleapis.com/auth/bigquery", 
            "https://www.googleapis.com/auth/drive" 
        ]
        
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(
                info, 
                scopes=SCOPES
            )
            return bigquery.Client(credentials=credentials, project=info["project_id"])
        
        return bigquery.Client(project=PROJECT_ID)

    except Exception as e:
        st.error(f"❌ BigQuery Authentication Failed: {e}")
        return None
        
############################
# - 2. DATA ENGINE LOGIC - #
############################

@st.cache_data(ttl=600)
def get_universal_portal_data(project_id):
    """
    Unified Direct Data Engine.
    - Strips punctuation (hyphens and colons) to seamlessly match Lord vs SensorPush strings.
    - Links project names directly to project_registry production sheets.
    """
    client = get_bq_client()
    if client is None: return pd.DataFrame()
    
    clean_token = str(project_id).replace("'", "''").strip()
    base_job_num = clean_token.split('-')[0].strip()
    is_office = "OFFICE" in clean_token.upper()

    # Visibility filtering conditions
    if is_office:
        filter_sql = "AND UPPER(COALESCE(CAST(m.approval_status AS STRING), 'PENDING')) != 'BADDATA'"
    else:
        filter_sql = "AND UPPER(COALESCE(CAST(m.approval_status AS STRING), 'PENDING')) NOT IN ('BADDATA', 'FALSE', '0')"

    # Upgraded Query: Uses REGEXP_REPLACE to normalize Node ID strings dynamically
    query = f"""
        WITH target_projects AS (
            SELECT Project FROM `{PROJECT_REGISTRY_TABLE}`
            WHERE Project = @project_id 
               OR ProjectName = @project_id
        ),
        raw_telemetry AS (
            SELECT 
                m.Project,
                m.NodeNum as RawNode,
                -- Creates a standardized alphanumeric string (e.g., "5720CH2")
                REGEXP_REPLACE(UPPER(TRIM(CAST(m.NodeNum AS STRING))), r'[:-]', '') as CleanTelemetryNode,
                m.temperature,
                m.timestamp,
                m.approval_status,
                COALESCE(m.Location, 'Unassigned Code') as Location,
                COALESCE(m.Bank, '—') as Bank,
                m.Depth
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
            WHERE m.temperature >= -30.0 AND m.temperature <= 120.0
              {filter_sql}
        )
        SELECT 
            t.Project,
            t.RawNode as NodeNum,
            t.temperature,
            t.timestamp,
            t.approval_status,
            t.Location,
            t.Bank,
            t.Depth
        FROM raw_telemetry t
        WHERE (
            t.Project = @project_id 
            OR t.Project = '{clean_token}'
            OR t.Project = '{base_job_num}' 
            OR t.Project LIKE '{base_job_num}%'
            OR t.Project IN (SELECT Project FROM target_projects)
        )
        ORDER BY t.timestamp ASC
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("project_id", "STRING", project_id)
        ]
    )
    
    try:
        query_job = client.query(query, job_config=job_config)
        return query_job.to_dataframe()
    except Exception as e:
        st.error(f"⚠️ Data Sync Error: {e}")
        return pd.DataFrame()
        
###########################
# - SIDEBAR NAVIGATION -  #
###########################

st.sidebar.title("❄️ SoilFreeze Lab")

# 1. PAGE NAVIGATION
page = st.sidebar.selectbox(
    "Navigation", 
    [
        "Summary",              
        "Time vs Temp",        
        "Depth Charts", 
        "Sensor Status",       
        "Node Diagnostics", 
        "Data Processing", 
        "Admin Tools"
    ],
    key="nav_page"
)

st.sidebar.divider()

# 2. PROJECT SELECTION
selected_project = "All Projects"
project_metadata = None  

sidebar_client = get_bq_client()

if sidebar_client is not None:
    try:
        # Fixed: Evaluates dynamic list based strictly on new Google Sheet 'ShowActive = Yes' parameter checks
        proj_q = f"""
            SELECT CAST(Project AS STRING) as Project, ProjectName, Timezone, ProjectStatus, Date_Freezedown, SoilType 
            FROM `{PROJECT_REGISTRY_TABLE}` 
            WHERE Project IS NOT NULL 
              AND TRIM(CAST(Project AS STRING)) != ''
              AND (UPPER(TRIM(CAST(ShowActive AS STRING))) = 'YES' OR UPPER(CAST(Project AS STRING)) LIKE '%OFFICE%')
        """
        proj_df = sidebar_client.query(proj_q).to_dataframe()
        
        # Python fix: Strip whitespace and filter out non-values to kill "No Project"
        proj_list = sorted([
            str(p).strip() for p in proj_df['Project'].unique() 
            if p and str(p).strip().lower() not in ['none', 'nan', 'null', '']
        ])
        
        selected_project = st.sidebar.selectbox(
            "🎯 Active Project", 
            ["All Projects"] + proj_list, 
            key="sidebar_proj_picker_global"
        )
        
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

# =============================================================================
# CURRENT DATA AGES & DYNAMIC REFRESH ENGINE
# =============================================================================
st.sidebar.subheader("⏱️ Current Data Ages")

if sidebar_client is not None:
    try:
        # Contextual switching logic based on sidebar dropdown choice
        if selected_project == "All Projects":
            pulse_q = f"""
                SELECT FORMAT_TIMESTAMP('%m/%d/%Y %H:%M UTC', MAX(timestamp)) as last_sync
                FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view`
            """
            scope_label = "Last Data"
        else:
            pulse_q = f"""
                SELECT FORMAT_TIMESTAMP('%m/%d/%Y %H:%M UTC', MAX(timestamp)) as last_sync
                FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view`
                WHERE Project = '{selected_project}' OR Project LIKE '{selected_project.split('-')[0]}%'
            """
            scope_label = f"Job {selected_project.split('-')[0]} Age"

        pulse_df = sidebar_client.query(pulse_q).to_dataframe()
        
        if not pulse_df.empty and pulse_df['last_sync'].iloc[0]:
            last_sync_str = str(pulse_df['last_sync'].iloc[0])
            
            last_sync_ts = pd.to_datetime(last_sync_str, utc=True)
            now_utc = pd.Timestamp.now(tz='UTC')
            elapsed_mins = int((now_utc - last_sync_ts).total_seconds() / 60)
            
            if elapsed_mins <= 60:
                pulse_status = f"🟢 **Live** ({elapsed_mins}m ago)"
            elif elapsed_mins <= 180:
                pulse_status = f"🟠 **Delayed** ({elapsed_mins}m ago)"
            else:
                pulse_status = f"🔴 **Stale** ({elapsed_mins // 60}h ago)"
                
            st.sidebar.markdown(f"**{scope_label}:** {pulse_status}")
            st.sidebar.caption(f"Last Entry: `{last_sync_str}`")
        else:
            st.sidebar.markdown(f"**{scope_label}:** ❌ No Sync Records")
            
    except Exception as pulse_err:
        st.sidebar.caption(f"Pulse tracking suspended: {pulse_err}")

# INTERACTIVE REFRESH TRIGGER
if st.sidebar.button("🔄 Refresh Data", use_container_width=True):
    with st.sidebar.spinner("Purging cache maps..."):
        st.cache_data.clear()
        st.toast("System cache completely cleared!", icon="🔄")
        time.sleep(0.5)
        st.rerun()
        
st.sidebar.divider()

# 3. GLOBAL VIEW TOGGLES & INTERACTIVE LOOKBACK
st.sidebar.subheader("👁️ Visibility Controls")

st.sidebar.toggle(
    "Show Theoretical Curves", 
    value=True, 
    key="global_show_ref",
    help="Superimpose goal curves on Time vs Temp charts."
)

st.sidebar.toggle(
    "Show Masked Data", 
    value=False, 
    key="global_show_masked",
    help="Display data points manually hidden by admins."
)

st.sidebar.toggle(
    "Mobile Layout", 
    value=False, 
    key="mobile_optimized_toggle"
)

st.sidebar.divider()

st.sidebar.subheader("⏳ Timeline Navigation")

selected_weeks = st.sidebar.slider(
    "Select History Window (Weeks)",
    min_value=1,
    max_value=12,
    value=5,  
    step=1,
    key="global_lookback_weeks_slider",
    help="Slide the point to change how many weeks of history pull into your charts."
)

lookback_days = selected_weeks * 7
st.session_state["global_lookback_days"] = lookback_days

st.sidebar.markdown(
    """
    <style>
        /* Target the slider track line */
        div[data-baseweb="slider"] > div > div {
            background: linear-gradient(to right, rgb(214, 39, 40) 0%, rgb(214, 39, 40) var(--slider-progress, 100%), rgb(230, 230, 230) var(--slider-progress, 100%)) !important;
        }
        /* Target the interactive thumb dot handle */
        div[role="slider"] {
            background-color: rgb(214, 39, 40) !important;
            border: 2px solid rgb(214, 39, 40) !important;
            box-shadow: 0px 0px 4px rgba(214, 39, 40, 0.5) !important;
        }
    </style>
    """,
    unsafe_allow_html=True
)
# --- CSS TO FORCE DATA TABLE PROGRESS COLUMNS RED ---
st.sidebar.markdown(
    """
    <style>
        /* Target the progress bar fill indicators inside Streamlit data grids */
        div[data-testid="stDataFrame"] div[role="progressbar"] > div {
            background-color: rgb(214, 39, 40) !important;
        }
        /* Target alternative HTML5 fallback elements if utilized by the matrix view */
        progress::-webkit-progress-value {
            background: rgb(214, 39, 40) !important;
        }
        progress::-moz-progress-bar {
            background: rgb(214, 39, 40) !important;
        }
    </style>
    """,
    unsafe_allow_html=True
)

# 4. MEASUREMENT & UNITS
st.sidebar.subheader("🌡️ Units")
unit_mode = st.sidebar.radio(
    "Temperature Scale", 
    ["Fahrenheit", "Celsius"], 
    horizontal=True,
    key="unit_toggle"
)
unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"
st.session_state["unit_mode"] = unit_mode
st.session_state["unit_label"] = unit_label

st.sidebar.divider()

# 5. TIMEZONE & DISPLAY
st.sidebar.subheader("📱 Display & Time")

default_tz_index = 2 
if project_metadata and project_metadata.get('Timezone') == "US/Eastern":
    default_tz_index = 1

tz_lookup = {
    "UTC": "UTC", 
    "Local (US/Eastern)": "US/Eastern", 
    "Local (US/Pacific)": "US/Pacific"
}

tz_mode = st.sidebar.selectbox(
    "Timezone Display", 
    list(tz_lookup.keys()), 
    index=default_tz_index,
    key="tz_picker"
)

st.session_state["display_tz"] = tz_lookup[tz_mode]

st.sidebar.divider()

# 6. REFERENCE LINES (Static Constants)
st.sidebar.subheader("📏 Reference Lines")
active_refs = [] 

if st.sidebar.checkbox("Freezing (32°F)", value=True, key="ref_freezing"): 
    active_refs.append((32.0, "Freezing"))
if st.sidebar.checkbox("Type B (26.6°F)", value=False, key="ref_type_b"): 
    active_refs.append((26.6, "Type B"))
if st.sidebar.checkbox("Type A (10.2°F)", value=False, key="ref_type_a"): 
    active_refs.append((10.2, "Type A"))

st.session_state["active_refs"] = tuple(active_refs)

unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
unit_label = st.session_state.get("unit_label", "°F")
display_tz = st.session_state.get("display_tz", "UTC")
active_refs = st.session_state.get("active_refs", [])

#############
# - Graph - #
#############

def natural_sort_key(s):
    """
    Splits strings into chunks of text and numbers to allow natural sorting.
    e.g., "10ft (SP32)" -> [10, "ft (sp", 32, ")"]
    """
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', str(s))]

def build_high_speed_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label, 
                           display_tz="UTC", mobile_mode=False, f_start_date=None, curve_id=None):
    """
    Engineering-grade Trend Graph.
    - Legend: Naturally sorted by logical numerical order (1, 2, ... 10).
    - Hover: Date at top, Time only on entries.
    - Gaps: Lines break if data is missing for > 6 hours.
    - Style: 15-Color Palette, RoyalBlue Freeze Line, Bold Monday Grids.
    """

    if df.empty: return go.Figure().update_layout(title="No data available")

    client = get_bq_client()
    plot_df = df.copy() 

    # 1. TIMEZONE & UNITS
    if plot_df['timestamp'].dt.tz is None:
        plot_df['timestamp'] = plot_df['timestamp'].dt.tz_localize('UTC')
    plot_df['timestamp'] = plot_df['timestamp'].dt.tz_convert(display_tz)
    
    freeze_pt = 0 if unit_mode == "Celsius" else 32
    y_range = [-30, 30] if unit_mode == "Celsius" else [-20, 80]

    fig = go.Figure()

    # 2. GLOBAL TIMELINE SYNC
    final_end_view, final_start_view = end_view, start_view
    proj_str = str(st.session_state.get('selected_project', ''))
    proj_match = re.findall(r'\d+', proj_str)
    proj_num = proj_match[0] if proj_match else ""
    loc_part = str(curve_id).split('-')[-1] if curve_id else ""

    # 3. THEORETICAL REFERENCE CURVES (Granular Phase/System Regex Fallbacks)
    if curve_id and curve_id != "None" and f_start_date:
        try:
            proj_str = str(st.session_state.get('selected_project', ''))
            proj_match = re.findall(r'\d+', proj_str)
            proj_num = proj_match[0] if proj_match else ""
            loc_part = str(curve_id).split('-')[-1].strip() if curve_id else ""

            if proj_num and loc_part:
                # Upgraded query using regex to look inside complex text strings
                target_q = f"""
                    SELECT CurveID, Day, Temp 
                    FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` 
                    WHERE REGEXP_CONTAINS(CurveID, r'^{proj_num}.*{loc_part}$')
                    ORDER BY Day
                """
                target_df = client.query(target_q).to_dataframe()
                if not target_df.empty:
                    dash_styles = ['dashdot', 'dash', 'dot']
                    gray_shades = ['rgba(30,30,30,0.8)', 'rgba(70,70,70,0.75)', 'rgba(110,110,110,0.7)']
                    
                    for c_idx, (cid, c_df) in enumerate(target_df.groupby('CurveID')):
                        c_df['timestamp'] = c_df['Day'].apply(lambda d: pd.Timestamp(f_start_date) + pd.Timedelta(days=d))
                        c_df['timestamp'] = c_df['timestamp'].dt.tz_localize('UTC').dt.tz_convert(display_tz)
                        ref_y = c_df['Temp'] if unit_mode == "Fahrenheit" else (c_df['Temp'] - 32) * 5/9
                        
                        # Extract the descriptive midsection (Phase/System info) for the graph label
                        label_clean = str(cid).replace(f"{proj_num}-", "").replace(f"-{loc_part}", "")
                        display_label = f"Goal: {label_clean}" if label_clean != loc_part else f"Goal: {loc_part}"
                        
                        selected_dash = dash_styles[c_idx % len(dash_styles)]
                        selected_gray = gray_shades[c_idx % len(gray_shades)]
                        
                        fig.add_trace(go.Scatter(
                            x=c_df['timestamp'], y=ref_y, 
                            name=f"<b>{display_label}</b>", 
                            mode='lines',
                            line=dict(color=selected_gray, width=3.5, dash=selected_dash, shape='spline', smoothing=1.3),
                            legendrank=1 
                        ))
        except Exception as e:
            pass

    # 4. SENSOR DATA (Naturally Sorted Group Loops)
    sf_15_palette = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf', '#FF1493', '#00CED1', '#FFD700', '#8A2BE2', '#32CD32']
    
    node_metadata = []
    for sn in plot_df['NodeNum'].unique():
        node_df = plot_df[plot_df['NodeNum'] == sn]
        depth_val = node_df['Depth'].iloc[0]
        bank_val = node_df['Bank'].iloc[0]
        loc_val = node_df['Location'].iloc[0]

        if pd.notnull(bank_val) and any(x in str(bank_val).upper() for x in ['S', 'R']):
            display_name = f"{bank_val} ({sn})"
            sort_val = str(bank_val)  
        elif pd.notnull(depth_val) and not pd.isna(depth_val): 
            display_name = f"{depth_val}ft ({sn})"
            sort_val = f"depth_{float(depth_val):05.1f}" 
        else: 
            display_name = f"{loc_val} ({sn})"
            sort_val = str(display_name)

        node_metadata.append({
            'node_num': sn,
            'display_name': display_name,
            'sort_key': sort_val
        })

    sorted_node_configs = sorted(node_metadata, key=lambda x: natural_sort_key(x['sort_key']))

    for i, config in enumerate(sorted_node_configs):
        sn = config['node_num']
        display_name = config['display_name']
        
        s_df = plot_df[plot_df['NodeNum'] == sn].sort_values('timestamp')
        s_df = s_df.set_index('timestamp').resample('1h').first().reset_index()
        
        fig.add_trace(go.Scatter(
            x=s_df['timestamp'], 
            y=s_df['temperature'],
            name=display_name, 
            mode='lines',
            connectgaps=False, 
            line=dict(shape='spline', smoothing=1.3, width=2, color=sf_15_palette[i % 15]),
            hovertemplate="<b>%{fullData.name}</b><br>Time: %{x|%H:%M}<br>Temp: %{y:.1f}" + unit_label + "<extra></extra>"
        ))

    # 5. REFERENCE LINES
    fig.add_hline(y=freeze_pt, line_width=2, line_dash="dash", line_color="RoyalBlue", annotation_text="32°F FREEZE", layer="above")
    
    now_ts = pd.Timestamp.now(tz=display_tz)
    fig.add_vline(x=now_ts.to_pydatetime(), line_width=2, line_color="red", line_dash="dash", layer='above')
    
    m_range = pd.date_range(start=final_start_view, end=final_end_view, freq='W-MON')
    for m_dt in m_range:
        fig.add_vline(x=m_dt, line_width=1.5, line_color="black", opacity=0.4)

    # 6. LAYOUT & TITLING
    p_name = st.session_state.get('selected_project', 'Project')
    fig.update_layout(
        title=dict(text=f"<b>{p_name} - Thermal Trend - {title}</b>", x=0.02, y=0.98, font=dict(size=18)),
        plot_bgcolor='white', 
        hovermode="x unified", 
        height=650,
        xaxis=dict(
            range=[final_start_view, final_end_view], 
            showgrid=True, gridcolor='Gainsboro',
            showline=True, mirror=True, linecolor='black', linewidth=2,
            hoverformat='%A, %b %d, %Y', 
            tickformat='%b %d',
            minor=dict(dtick=1000*60*60*24, showgrid=True, gridcolor='#f8f8f8')
        ),
        yaxis=dict(
            title=f"Temperature ({unit_label})", 
            range=y_range, 
            dtick=10,
            showgrid=True, gridcolor='Gainsboro', 
            showline=True, mirror=True, linecolor='black', linewidth=2,
            minor=dict(dtick=2, showgrid=True, gridcolor='#f8f8f8')
        ),
        legend=dict(orientation="v", x=1.02, y=1, xanchor="left", yanchor="top")
    )
    return fig
                                
def get_soil_reference_curves(soil_type, start_date, unit_mode):
    """
    Fallback function for hardcoded soil types.
    """
    references = {
        "Silty Sand": [(0, 50), (5, 32), (14, 20), (30, 10), (60, 5)],
        "Clay":       [(0, 50), (10, 32), (25, 25), (45, 15), (90, 10)]
    }
    
    curve = references.get(soil_type, [])
    if not curve:
        return None, None
        
    x_times = [pd.Timestamp(start_date) + pd.Timedelta(days=d) for d, t in curve]
    y_temps = [t if unit_mode == "Fahrenheit" else (t - 32) * 5/9 for d, t in curve]
    
    return x_times, y_temps

def run_office_auto_assignment():
    """
    Surgically assigns 'OFFICE' status to all telemetry 
    where the node is currently assigned to the Office project.
    """
    client = get_bq_client()
    
    sql = f"""
        MERGE `{OVERRIDE_TABLE}` T
        USING (
            SELECT DISTINCT r.NodeNum, TIMESTAMP_TRUNC(r.timestamp, HOUR) as ts
            FROM (
                SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` 
                UNION ALL 
                SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
            ) AS r
            INNER JOIN `{NODE_REGISTRY_TABLE}` AS n ON r.NodeNum = n.NodeNum
            WHERE n.Project LIKE '%OFFICE%' 
        ) S ON T.NodeNum = S.NodeNum AND T.timestamp = S.ts
        WHEN MATCHED THEN UPDATE SET approve = 'OFFICE'
        WHEN NOT MATCHED THEN INSERT (NodeNum, timestamp, approve) VALUES (S.NodeNum, S.ts, 'OFFICE')
    """
    try:
        client.query(sql).result()
        st.success("✅ Successfully assigned 'OFFICE' status to all relevant telemetry.")
    except Exception as e:
        st.error(f"Auto-assignment failed: {e}")

##################
# High temp mask #
##################
def apply_sanity_filter(df):
    """
    Automated filter for rogue data points.
    Flags anything outside the absolute limits of -30°F and 120°F as BADDATA.
    """
    if df.empty:
        return df

    # Logic: Mark records outside of strict industrial physical limits [-30, 120]
    bad_condition = (df['temperature'] > 120) | (df['temperature'] < -30)
    
    # If your view or data holds an approval column, mark it in-memory
    if 'approve' in df.columns:
        df.loc[bad_condition, 'approve'] = 'BADDATA'
    elif 'approval_status' in df.columns:
        df.loc[bad_condition, 'approval_status'] = 'BADDATA'

    return df

##############################
# Page 1 - Dashboard Summary #
##############################
def render_summary_dashboard(unit_label, unit_mode, display_tz):
    """
    The main Global Project Summary dashboard.
    - Fixed English translations for ranges.
    - Robust timezone-aware sensor check-in counters.
    - Automated link directory for active external client portals.
    - FIXED: Outlier filter raised to 120°F to accommodate warm pre-freeze zones.
    - FIXED: Re-aligned grouping hierarchy to prioritize numeric Depth for TempPipes.
    - FIXED: Linked to live Google Sheet tables and filtered by ShowActive = 'Yes'.
    """
    st.header("🌐 Global Project Summary")
    
    client = get_bq_client()
    if client is None: return

    mobile_mode = st.session_state.get("mobile_optimized_toggle", False)

    # SQL QUERY: Balanced approach showing active field data while pulling straight from Google Sheet tables
    summary_q = f"""
        WITH active_projects AS (
            SELECT 
                CAST(Project AS STRING) as Project, 
                ProjectName, 
                ProjectStatus, 
                Date_Freezedown,
                -- Slices out numeric base prefixes (e.g. '2538') to handle trailing descriptive text mismatches
                REGEXP_EXTRACT(TRIM(CAST(Project AS STRING)), r'^\\d+') as base_prefix
            FROM `{PROJECT_REGISTRY_TABLE}`
            WHERE UPPER(TRIM(CAST(ShowActive AS STRING))) = 'YES'
              AND UPPER(CAST(Project AS STRING)) NOT LIKE '%OFFICE%'
        ),
        raw_data AS (
            SELECT 
                p.Project, n.Bank, n.Location, n.Depth, m.temperature, m.timestamp, m.NodeNum
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
            -- Cross-reference against our spreadsheet metadata base prefixes to map groups accurately
            INNER JOIN active_projects p 
                ON REGEXP_EXTRACT(TRIM(CAST(m.Project AS STRING)), r'^\\d+') = p.base_prefix
            LEFT JOIN `{NODE_REGISTRY_TABLE}` n 
              -- HARDENED JOIN: Strips formatting punctuation to cleanly match Lord vs SensorPush strings
              ON REGEXP_REPLACE(UPPER(TRIM(CAST(m.NodeNum AS STRING))), r'[:-]', '') = 
                 REGEXP_REPLACE(UPPER(TRIM(CAST(n.NodeNum AS STRING))), r'[:-]', '')
            WHERE m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
              -- BALANCED RULE: Show verified AND streaming real-time data, but block bad data
              AND UPPER(COALESCE(CAST(m.approval_status AS STRING), 'PENDING')) NOT IN ('BADDATA', 'FALSE', '0')
              -- Outlier Shield: Aligned with the physical boundary limit of 120°F to handle high-ambient ground zones
              AND NOT (m.temperature > 120.0 AND NOT STARTS_WITH(m.NodeNum, 'SP'))
        ),
        MaxTime AS (
            SELECT MAX(timestamp) as max_ts FROM raw_data
        ),
        LatestStats AS (
            SELECT 
                r.Project, r.Bank, r.Location, r.Depth, r.NodeNum,
                AVG(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR) THEN r.temperature END) as avg_now,
                AVG(CASE WHEN r.timestamp BETWEEN TIMESTAMP_SUB(m.max_ts, INTERVAL 2 HOUR) AND TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR) THEN r.temperature END) as avg_1h,
                AVG(CASE WHEN r.timestamp BETWEEN TIMESTAMP_SUB(m.max_ts, INTERVAL 7 HOUR) AND TIMESTAMP_SUB(m.max_ts, INTERVAL 6 HOUR) THEN r.temperature END) as avg_6h,
                AVG(CASE WHEN r.timestamp BETWEEN TIMESTAMP_SUB(m.max_ts, INTERVAL 25 HOUR) AND TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR) THEN r.temperature END) as avg_24h,
                MIN(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR) THEN r.temperature END) as min_now,
                MAX(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR) THEN r.temperature END) as max_now,
                MIN(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR) THEN r.temperature END) as min_24h,
                MAX(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR) THEN r.temperature END) as max_24h,
                
                COUNTIF(r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR)) as checkins_1h,
                COUNTIF(r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR)) as checkins_24h,
                
                ARRAY_AGG(r.temperature ORDER BY r.timestamp DESC LIMIT 1)[OFFSET(0)] as latest_temp,
                MAX(r.timestamp) as latest_ts
            FROM raw_data r
            CROSS JOIN MaxTime m
            GROUP BY 1, 2, 3, 4, 5
        )
        SELECT 
            p.*, ls.*,
            (COUNTIF(ls.Bank LIKE 'S%' AND ls.latest_temp <= -10) OVER(PARTITION BY p.Project) / NULLIF(COUNTIF(ls.Bank LIKE 'S%') OVER(PARTITION BY p.Project), 0)) * 100 as supply_kpi,
            (COUNTIF(ls.Bank LIKE 'R%' AND ls.latest_temp <= 0) OVER(PARTITION BY p.Project) / NULLIF(COUNTIF(ls.Bank LIKE 'R%') OVER(PARTITION BY p.Project), 0)) * 100 as return_kpi,
            (COUNTIF(ls.Depth IS NOT NULL AND ls.latest_temp <= 32) OVER(PARTITION BY p.Project) / NULLIF(COUNTIF(ls.Depth IS NOT NULL) OVER(PARTITION BY p.Project), 0)) * 100 as freeze_kpi
        FROM active_projects p
        LEFT JOIN LatestStats ls ON p.Project = ls.Project
    """
    
    try:
        df = client.query(summary_q).to_dataframe()
        df[['Bank', 'Location']] = df[['Bank', 'Location']].fillna('')
    except Exception as e:
        st.error(f"Dashboard Query Failed: {e}")
        return

    if df.empty:
        st.info("No active projects found matching your 'ShowActive = Yes' parameter checks.")
        return

    for project in sorted(df['Project'].unique()):
        p_df = df[df['Project'] == project]
        p_name = p_df['ProjectName'].iloc[0] or project
        f_date = p_df['Date_Freezedown'].iloc[0]
        
        day_text, f_date_display = "", "Not Set"
        if pd.notnull(f_date):
            f_date_display = pd.to_datetime(f_date).strftime('%b %d, %Y')
            days_elapsed = (pd.Timestamp.now(tz=display_tz).date() - pd.to_datetime(f_date).date()).days
            day_text = f"🗓️ **Day {max(0, days_elapsed)}**"
        
        with st.container(border=True):
            h1, h2 = st.columns([2, 1])
            h1.subheader(f"🏗️ {p_name}")
            h2.markdown(f"<div style='text-align: right;'>{day_text}<br><small>ID: {project}</small></div>", unsafe_allow_html=True)
            
            # --- CLIENT PORTAL LINK INJECTION ENGINE ---
            proj_match = re.search(r'\b(\d{4})\b', str(project))
            if proj_match:
                job_number = proj_match.group(1)
                portal_url = f"https://sf{job_number}.streamlit.app"
                st.markdown(f"🔗 **External Client Portal:** [{p_name} Portal Site Link]({portal_url})")
            
            # --- ACCURATE CHECK-IN COUNTERS ---
            active_1h = p_df[p_df['checkins_1h'] > 0]['NodeNum'].nunique()
            active_24h = p_df[p_df['checkins_24h'] > 0]['NodeNum'].nunique()
            total_nodes = p_df['NodeNum'].dropna().nunique()
            
            st.markdown(
                f"📡 **Hardware Status:** `{active_1h}` nodes pinged in the last hour | "
                f"`{active_24h}` nodes pinged in the last 24h (Total Pool: `{total_nodes}` registered)"
            )
            st.divider() 

            # Data isolation
            is_amb = p_df['Bank'].str.contains('Amb', case=False) | p_df['Location'].str.contains('Amb', case=False)
            
            # PRIORITIZE NUMERIC DEPTHS FOR TEMPPIPES: Prevents multi-channel Lord nodes from mis-allocating to loop arrays
            is_tp = p_df['Depth'].notnull() & (p_df['Depth'].astype(str).str.strip() != '') & ~is_amb
            is_s = (p_df['Bank'].str.startswith('S') | p_df['Location'].str.startswith('S')) & ~is_amb & ~is_tp
            is_r = (p_df['Bank'].str.startswith('R') | p_df['Location'].str.startswith('R')) & ~is_amb & ~is_tp

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
                col_mappings = [0, 2, 4, 6]
                spacer_mappings = [1, 3, 5]
                
                for s_idx in spacer_mappings:
                    cols[s_idx].markdown("<div style='border-left: 1px solid #ddd; height: 320px; margin: auto;'></div>", unsafe_allow_html=True)
                
                for idx, (title, g_df, kpi_col, kpi_val) in enumerate(groups_data):
                    with cols[col_mappings[idx]]:
                        render_dashboard_column(title, g_df, kpi_col, kpi_val, unit_mode, unit_label)

def render_dashboard_column(title, g_df, kpi_col, kpi_val, unit_mode, unit_label):
    """Helper layout compiler to handle repeating column metric sets."""
    st.markdown(f"**{title}**")
    if g_df.empty or g_df['latest_temp'].isnull().all():
        st.caption("No recent data")
        return
    
    latest_val = g_df['latest_temp'].mean()
    c_min, c_max = g_df['min_now'].min(), g_df['max_now'].max()
    m24, x24 = g_df['min_24h'].min(), g_df['max_24h'].max()

    def convert(v):
        if pd.isnull(v) or pd.isna(v): return None
        return (v - 32) * 5/9 if unit_mode == "Celsius" else v

    l_conv, c_min, c_max, m24, x24 = map(convert, [latest_val, c_min, c_max, m24, x24])

    st.metric("Avg (Latest)", f"{l_conv:.1f}{unit_label}")
    
    if kpi_col:
        pct = g_df[kpi_col].iloc[0]
        color = "green" if pct == 100 else "#FF8C00" if pct > 0 else "gray"
        st.markdown(f"<p style='font-size:0.85rem; color:{color};'><b>{pct:.0f}%</b> Nodes ≤ {kpi_val}°F</p>", unsafe_allow_html=True)

    range_html = "<div style='font-size: 0.8rem; line-height: 1.2; margin-bottom: 10px;'><b>Normal Ranges:</b><br>"
    if c_min is not None and c_max is not None:
        range_html += f"Current: {c_min:.1f} to {c_max:.1f}{unit_label}<br>"
    else:
        range_html += "Current: No Data<br>"
    
    if m24 is not None and x24 is not None:
        range_html += f"24h Range: {m24:.1f} to {x24:.1f}{unit_label}"
    else:
        range_html += "24h Range: No Data"
    range_html += "</div>"
    st.markdown(range_html, unsafe_allow_html=True)
    st.markdown("<div style='font-size: 0.75rem; border-top: 1px solid #eee; padding-top: 5px;'>", unsafe_allow_html=True)


def get_trend_arrow(current, previous):
    """Helper to generate trend icons with updated blue downward arrow."""
    if pd.isnull(current) or pd.isnull(previous): return "N/A"
    delta = current - previous
    if delta > 0.1: return f"🔺 +{delta:.1f}"
    if delta < -0.1: return f"🔹 {delta:.1f}"
    return "➡️ 0.0"

# =============================================================================
# WORKSPACE PAGE 2: TIME VS TEMP SCROLLING OVERVIEW
# =============================================================================
def render_global_overview(selected_project, project_metadata, display_tz):
    """
    Shows all pipes/banks for a selected project in one scrolling view.
    Standardized: Normalizes variations like TP2 vs T2 for seamless plotting.
    """
    # 1. INITIALIZE UI STATE VARIABLES FROM SIDEBAR KEYS
    show_ref = st.session_state.get("global_show_ref", True)
    show_masked = st.session_state.get("global_show_masked", False)
    unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
    unit_label = st.session_state.get("unit_label", "°F")
    active_refs = st.session_state.get("active_refs", [])

    # 2. EXTRACT PROJECT METADATA
    p_name = selected_project
    status = "Active"
    f_start_date = None

    if project_metadata:
        p_name = project_metadata.get('ProjectName', selected_project)
        status = project_metadata.get('ProjectStatus', 'Active')
        raw_f_date = project_metadata.get('Date_Freezedown')
        if pd.notnull(raw_f_date):
            f_start_date = pd.to_datetime(raw_f_date).date()

    # 3. HEADER
    st.header(f"📈 Time vs Temp: {p_name} [{status}]")
    
    if f_start_date:
        today = pd.Timestamp.now(tz=display_tz).date()
        days_since = (today - f_start_date).days
        st.markdown(f"### 🗓️ Day **{max(0, days_since)}** of Freezedown")

    # 4. DATA PRE-FLIGHT
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Select a project in the sidebar to view engineering trends.")
        return

    with st.spinner(f"Syncing {p_name} telemetry..."):
        p_df = get_universal_portal_data(selected_project)

    if p_df.empty:
        st.warning(f"No data found for '{p_name}'.")
        return

    # 5. DYNAMIC UI FILTERING
    mask_col = 'approval_status' if 'approval_status' in p_df.columns else 'approve'
    if not show_masked and mask_col in p_df.columns:
        p_df = p_df[p_df[mask_col].astype(str).str.upper() != 'MASKED'].copy()

    # --- 6. TIMELINE CONFIG (CONNECTED TO GLOBAL RED SLIDER) ---
    lookback_weeks = st.session_state.get("global_lookback_weeks_slider", 5)
    now_local = pd.Timestamp.now(tz=display_tz)
    end_view = (now_local + pd.Timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    start_view = end_view - pd.Timedelta(weeks=lookback_weeks)

    # 7. LOCATION-BASED PLOTTING LOOP
    locations = sorted(
        [str(loc) for loc in p_df['Location'].dropna().unique()], 
        key=natural_sort_key
    )

    for loc in locations:
        with st.expander(f"📍 Location: {loc}", expanded=True):
            loc_df = p_df[p_df['Location'] == loc].copy()
            
            clean_proj_id = str(selected_project).split('-')[0]
            
            # NORMALIZATION EXTRACTION: Converts "TP2", "TP-2", or "T2" into a clean "T2" pattern for the curve lookup
            clean_loc_num = "".join(re.findall(r'\d+', loc))
            normalized_loc = f"T{clean_loc_num}" if clean_loc_num else loc
            search_id = f"{clean_proj_id}-{normalized_loc}"
            
            # Exclude supply/return loops from being flagged as vertical ground temperature boreholes
            is_temp_pipe = not any(x in loc.upper() for x in ["SUPPLY", "RETURN", "BANK S", "BANK R", "AMB"])

            fig = build_high_speed_graph(
                df=loc_df, 
                title=f"Thermal Trends: {loc}", 
                start_view=start_view, 
                end_view=end_view, 
                active_refs=active_refs, 
                unit_mode=unit_mode, 
                unit_label=unit_label, 
                display_tz=display_tz,
                f_start_date=f_start_date,
                curve_id=search_id if (show_ref and is_temp_pipe) else None
            )
            
            st.plotly_chart(fig, use_container_width=True, key=f"tvt_{selected_project}_{loc}")

# =============================================================================
# WORKSPACE PAGE 3: TEMPERATURE VS DEPTH CHARTS
# =============================================================================
def render_depth_charts(selected_project, unit_label, display_tz):
    """
    Vertical Temperature Profiles.
    Maps arrays dynamically based on native view Depth allocations.
    """
    st.header(f"📏 Depth Profile Analysis: {selected_project}")
    
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to view depth profiles.")
        return

    st.sidebar.subheader("📐 Profile Settings")
    lookback_weeks = st.sidebar.slider("Historical Snapshots (Weeks)", 1, 24, 8, key="depth_lookback")

    with st.spinner("Fetching historical telemetry..."):
        p_df = get_universal_portal_data(selected_project)

    if p_df is None or p_df.empty:
        st.warning("No data found for this project.")
        return

    # Convert native view Depth values straight into a graph-safe float coordinate
    p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
    p_df = p_df[p_df['temperature'] <= 120.0]
    
    depth_df = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
    
    if depth_df.empty:
        st.info("No sensors with valid numeric 'Depth' entries found in the data stream.")
        return

    unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
    freeze_pt = 0 if unit_mode == "Celsius" else 32
    
    now_utc = pd.Timestamp.now(tz='UTC')
    mondays = pd.date_range(end=now_utc, periods=lookback_weeks, freq='W-MON')
    locations = sorted(depth_df['Location'].unique())
    
    for loc in locations:
        with st.expander(f"📍 Temp vs Depth - {loc}", expanded=True):
            loc_data = depth_df[depth_df['Location'] == loc].copy()
            
            if loc_data['timestamp'].dt.tz is None:
                loc_data['timestamp'] = loc_data['timestamp'].dt.tz_localize('UTC')
            loc_data['timestamp_local'] = loc_data['timestamp'].dt.tz_convert(display_tz)
            
            fig = go.Figure()

            # --- A. BASELINE Snapshots ---
            baseline_ts = loc_data['timestamp_local'].min()
            b_window = loc_data[
                (loc_data['timestamp_local'] >= baseline_ts - pd.Timedelta(hours=12)) & 
                (loc_data['timestamp_local'] <= baseline_ts + pd.Timedelta(hours=12))
            ]
            
            baseline_date_str = ""
            snap_base = pd.DataFrame()
            if not b_window.empty:
                baseline_date_str = baseline_ts.strftime('%Y-%m-%d')
                snap_base = (
                    b_window.assign(diff=(b_window['timestamp_local'] - baseline_ts).abs())
                    .sort_values(['NodeNum', 'diff'])
                    .drop_duplicates('NodeNum')
                    .sort_values('Depth_Num')
                )

            # --- B. RECENT 6 AM Snapshots ---
            loc_data['date_str'] = loc_data['timestamp_local'].dt.strftime('%Y-%m-%d')
            loc_data['hour_int'] = loc_data['timestamp_local'].dt.hour
            
            recent_6am_date_str = ""
            recent_profile_rows = []
            
            if not loc_data.empty:
                sorted_all_dates = sorted(loc_data['date_str'].unique(), reverse=True)
                
                for candidate_date in sorted_all_dates:
                    if candidate_date == baseline_date_str:
                        continue
                    
                    day_pool = loc_data[loc_data['date_str'] == candidate_date]
                    if day_pool.empty:
                        continue
                        
                    recent_6am_date_str = candidate_date
                    
                    for node_id, node_group in day_pool.groupby('NodeNum'):
                        exact_6am = node_group[node_group['hour_int'] == 6]
                        if not exact_6am.empty:
                            recent_profile_rows.append(exact_6am.sort_values('timestamp_local').iloc[-1])
                        else:
                            node_group = node_group.assign(hour_dist=(node_group['hour_int'] - 6).abs())
                            best_fallback_row = node_group.sort_values(by=['hour_dist', 'timestamp_local']).iloc[0]
                            recent_profile_rows.append(best_fallback_row)
                    break

            snap_recent = pd.DataFrame(recent_profile_rows).sort_values('Depth_Num') if recent_profile_rows else pd.DataFrame()

            # --- C. HISTORICAL SNAPSHOTS ---
            for m_date in mondays:
                target_ts = m_date.replace(hour=6, minute=0, second=0)
                current_loop_date = target_ts.strftime('%Y-%m-%d')
                
                if current_loop_date == baseline_date_str or current_loop_date == recent_6am_date_str:
                    continue
                    
                window = loc_data[
                    (loc_data['timestamp_local'] >= target_ts - pd.Timedelta(hours=12)) & 
                    (loc_data['timestamp_local'] <= target_ts + pd.Timedelta(hours=12))
                ]
                
                if not window.empty:
                    snap_week = (
                        window.assign(diff=(window['timestamp_local'] - target_ts).abs())
                        .sort_values(['NodeNum', 'diff'])
                        .drop_duplicates('NodeNum')
                        .sort_values('Depth_Num')
                    )
                    
                    if snap_week.empty:
                        snap_week = (
                            window.assign(hour_dist=(window['timestamp_local'].dt.hour - 6).abs())
                            .sort_values(by=['hour_dist', 'timestamp_local'])
                            .drop_duplicates('NodeNum')
                            .sort_values('Depth_Num')
                        )
                    
                    temps = snap_week['temperature']
                    if unit_mode == "Celsius": temps = (temps - 32) * 5/9
                    
                    fig.add_trace(go.Scatter(
                        x=temps, y=snap_week['Depth_Num'], 
                        mode='lines+markers', 
                        name=current_loop_date,
                        line=dict(shape='spline', smoothing=1.1, width=1.5),
                        marker=dict(size=4),
                        hovertemplate=f"Date: {current_loop_date}<br>Depth: %{{y}}ft<br>Temp: %{{x:.1f}}{unit_label}<extra></extra>"
                    ))

            # --- D. INJECT THE MOST RECENT LINE ---
            if not snap_recent.empty:
                recent_temps = snap_recent['temperature']
                if unit_mode == "Celsius": recent_temps = (recent_temps - 32) * 5/9
                
                fig.add_trace(go.Scatter(
                    x=recent_temps, y=snap_recent['Depth_Num'],
                    mode='lines+markers',
                    name=f'<b>Most Recent ({recent_6am_date_str} 6AM*)</b>',
                    line=dict(color='#ff7f0e', width=3.5, shape='spline', smoothing=1.1),
                    marker=dict(size=6, color='#ff7f0e'),
                    hovertemplate="Most Recent: %{text}<br>Depth: %{y}ft<br>Temp: %{x:.1f}" + unit_label + "<extra></extra>",
                    text=snap_recent['timestamp_local'].dt.strftime('%b %d, %H:%M')
                ))

            # --- E. INJECT BASELINE ---
            if not snap_base.empty:
                b_temps = snap_base['temperature']
                if unit_mode == "Celsius": b_temps = (b_temps - 32) * 5/9
                
                fig.add_trace(go.Scatter(
                    x=b_temps, y=snap_base['Depth_Num'], 
                    mode='lines+markers', 
                    name=f'<b>Baseline ({baseline_date_str})</b>',
                    line=dict(color='black', width=3, dash='dash'),
                    marker=dict(size=5, color='black'),
                    hovertemplate=f"Baseline: {baseline_date_str}<br>Depth: %{{y}}ft<br>Temp: %{{x:.1f}}{unit_label}<extra></extra>"
                ))

            fig.add_vline(x=freeze_pt, line_width=2, line_dash="solid", line_color="#ADD8E6")

            max_depth = loc_data['Depth_Num'].max()
            y_limit = int(((max_depth // 10) + 1) * 10) if pd.notnull(max_depth) else 50

            fig.update_layout(
                title=f"<b>Temp vs Depth - {loc}</b>",
                plot_bgcolor='white', 
                height=800,
                xaxis=dict(
                    title=f"Temperature ({unit_label})", 
                    range=[-20, 80], dtick=10,
                    minor=dict(dtick=2, showgrid=True, gridcolor='#f8f8f8'),
                    gridcolor='Gainsboro', showline=True, linewidth=2, linecolor='black', mirror=True
                ),
                yaxis=dict(
                    title="Depth (ft)", 
                    range=[y_limit, 0], dtick=10,
                    minor=dict(dtick=2, showgrid=True, gridcolor='#f8f8f8'),
                    gridcolor='Silver', showline=True, linewidth=2, linecolor='black', mirror=True
                ),
                legend=dict(orientation="h", y=-0.1, xanchor="center", x=0.5)
            )
            
            st.plotly_chart(fig, use_container_width=True, key=f"depth_cht_{selected_project}_{loc}")
            
##################
# High temp mask #
##################
def apply_sanity_filter(df):
    """
    Automated filter for rogue data points.
    Flags anything outside the absolute limits of -30°F and 120°F as BADDATA.
    """
    if df.empty:
        return df

    # Logic: Mark records outside of strict industrial physical limits [-30, 120]
    bad_condition = (df['temperature'] > 120) | (df['temperature'] < -30)
    
    # If your view or data holds an approval column, mark it in-memory
    if 'approve' in df.columns:
        df.loc[bad_condition, 'approve'] = 'BADDATA'
    elif 'approval_status' in df.columns:
        df.loc[bad_condition, 'approval_status'] = 'BADDATA'

    return df

##############################
# Page 1 - Dashboard Summary #
##############################
def render_summary_dashboard(unit_label, unit_mode, display_tz):
    """
    The main Global Project Summary dashboard.
    - Fixed English translations for ranges.
    - Robust timezone-aware sensor check-in counters.
    - Automated link directory for active external client portals.
    - FIXED: Outlier filter raised to 120°F to accommodate warm pre-freeze zones.
    - FIXED: Re-aligned grouping hierarchy to prioritize numeric Depth for TempPipes.
    """
    st.header("🌐 Global Project Summary")
    
    client = get_bq_client()
    if client is None: return

    mobile_mode = st.session_state.get("mobile_optimized_toggle", False)

    # SQL QUERY: Balanced approach showing active field data while purging bad data
    summary_q = f"""
        WITH active_projects AS (
            SELECT CAST(Project AS STRING) as Project, ProjectName, ProjectStatus, Date_Freezedown
            FROM `{PROJECT_REGISTRY_TABLE}`
            WHERE UPPER(TRIM(CAST(ShowActive AS STRING))) = 'YES'
              AND UPPER(CAST(Project AS STRING)) NOT LIKE '%OFFICE%'
        ),
        raw_data AS (
            SELECT 
                p.Project, n.Bank, n.Location, n.Depth, m.temperature, m.timestamp, m.NodeNum
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
            INNER JOIN active_projects p 
                ON REGEXP_EXTRACT(TRIM(CAST(m.Project AS STRING)), r'^\\d+') = REGEXP_EXTRACT(TRIM(CAST(p.Project AS STRING)), r'^\\d+')
            LEFT JOIN `{NODE_REGISTRY_TABLE}` n 
              -- HARDENED JOIN: Strips formatting punctuation to cleanly match Lord vs SensorPush strings
              ON REGEXP_REPLACE(UPPER(TRIM(CAST(m.NodeNum AS STRING))), r'[:-]', '') = 
                 REGEXP_REPLACE(UPPER(TRIM(CAST(n.NodeNum AS STRING))), r'[:-]', '')
            WHERE m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
              -- BALANCED RULE: Show verified AND streaming real-time data, but block bad data
              AND UPPER(COALESCE(CAST(m.approval_status AS STRING), 'PENDING')) NOT IN ('BADDATA', 'FALSE', '0')
              -- Outlier Shield: Aligned with the physical boundary limit of 120°F to handle high-ambient ground zones
              AND NOT (m.temperature > 120.0 AND NOT STARTS_WITH(m.NodeNum, 'SP'))
        ),
        MaxTime AS (
            SELECT MAX(timestamp) as max_ts FROM raw_data
        ),
        LatestStats AS (
            SELECT 
                r.Project, r.Bank, r.Location, r.Depth, r.NodeNum,
                AVG(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR) THEN r.temperature END) as avg_now,
                AVG(CASE WHEN r.timestamp BETWEEN TIMESTAMP_SUB(m.max_ts, INTERVAL 2 HOUR) AND TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR) THEN r.temperature END) as avg_1h,
                AVG(CASE WHEN r.timestamp BETWEEN TIMESTAMP_SUB(m.max_ts, INTERVAL 7 HOUR) AND TIMESTAMP_SUB(m.max_ts, INTERVAL 6 HOUR) THEN r.temperature END) as avg_6h,
                AVG(CASE WHEN r.timestamp BETWEEN TIMESTAMP_SUB(m.max_ts, INTERVAL 25 HOUR) AND TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR) THEN r.temperature END) as avg_24h,
                MIN(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR) THEN r.temperature END) as min_now,
                MAX(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR) THEN r.temperature END) as max_now,
                MIN(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR) THEN r.temperature END) as min_24h,
                MAX(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR) THEN r.temperature END) as max_24h,
                
                COUNTIF(r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR)) as checkins_1h,
                COUNTIF(r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR)) as checkins_24h,
                
                ARRAY_AGG(r.temperature ORDER BY r.timestamp DESC LIMIT 1)[OFFSET(0)] as latest_temp,
                MAX(r.timestamp) as latest_ts
            FROM raw_data r
            CROSS JOIN MaxTime m
            GROUP BY 1, 2, 3, 4, 5
        )
        SELECT 
            p.*, ls.*,
            (COUNTIF(ls.Bank LIKE 'S%' AND ls.latest_temp <= -10) OVER(PARTITION BY p.Project) / NULLIF(COUNTIF(ls.Bank LIKE 'S%') OVER(PARTITION BY p.Project), 0)) * 100 as supply_kpi,
            (COUNTIF(ls.Bank LIKE 'R%' AND ls.latest_temp <= 0) OVER(PARTITION BY p.Project) / NULLIF(COUNTIF(ls.Bank LIKE 'R%') OVER(PARTITION BY p.Project), 0)) * 100 as return_kpi,
            (COUNTIF(ls.Depth IS NOT NULL AND ls.latest_temp <= 32) OVER(PARTITION BY p.Project) / NULLIF(COUNTIF(ls.Depth IS NOT NULL) OVER(PARTITION BY p.Project), 0)) * 100 as freeze_kpi
        FROM active_projects p
        LEFT JOIN LatestStats ls ON p.Project = ls.Project
    """
    
    try:
        df = client.query(summary_q).to_dataframe()
        df[['Bank', 'Location']] = df[['Bank', 'Location']].fillna('')
    except Exception as e:
        st.error(f"Dashboard Query Failed: {e}")
        return

    if df.empty:
        st.info("No active projects found matching your 'ShowActive = Yes' parameter checks.")
        return

    for project in sorted(df['Project'].unique()):
        p_df = df[df['Project'] == project]
        p_name = p_df['ProjectName'].iloc[0] or project
        f_date = p_df['Date_Freezedown'].iloc[0]
        
        day_text, f_date_display = "", "Not Set"
        if pd.notnull(f_date):
            f_date_display = pd.to_datetime(f_date).strftime('%b %d, %Y')
            days_elapsed = (pd.Timestamp.now(tz=display_tz).date() - pd.to_datetime(f_date).date()).days
            day_text = f"🗓️ **Day {max(0, days_elapsed)}**"
        
        with st.container(border=True):
            h1, h2 = st.columns([2, 1])
            h1.subheader(f"🏗️ {p_name}")
            h2.markdown(f"<div style='text-align: right;'>{day_text}<br><small>Start: {f_date_display}</small></div>", unsafe_allow_html=True)
            
            # --- CLIENT PORTAL LINK INJECTION ENGINE ---
            proj_match = re.search(r'\b(\d{4})\b', str(project))
            if proj_match:
                job_number = proj_match.group(1)
                portal_url = f"https://sf{job_number}.streamlit.app"
                st.markdown(f"🔗 **External Client Portal:** [{p_name} Portal Site Link]({portal_url})")
            
            # --- ACCURATE CHECK-IN COUNTERS ---
            active_1h = p_df[p_df['checkins_1h'] > 0]['NodeNum'].nunique()
            active_24h = p_df[p_df['checkins_24h'] > 0]['NodeNum'].nunique()
            total_nodes = p_df['NodeNum'].dropna().nunique()
            
            st.markdown(
                f"📡 **Hardware Status:** `{active_1h}` nodes pinged in the last hour | "
                f"`{active_24h}` nodes pinged in the last 24h (Total Pool: `{total_nodes}` registered)"
            )
            st.divider() 

            # Data isolation
            is_amb = p_df['Bank'].str.contains('Amb', case=False) | p_df['Location'].str.contains('Amb', case=False)
            
            # CRITICAL FIX: Prioritize valid numeric Depths for TempPipes first to stop multi-channel Lord nodes from slipping into brines
            is_tp = p_df['Depth'].notnull() & (p_df['Depth'].astype(str).str.strip() != '') & ~is_amb
            is_s = (p_df['Bank'].str.startswith('S') | p_df['Location'].str.startswith('S')) & ~is_amb & ~is_tp
            is_r = (p_df['Bank'].str.startswith('R') | p_df['Location'].str.startswith('R')) & ~is_amb & ~is_tp

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
                col_mappings = [0, 2, 4, 6]
                spacer_mappings = [1, 3, 5]
                
                for s_idx in spacer_mappings:
                    cols[s_idx].markdown("<div style='border-left: 1px solid #ddd; height: 320px; margin: auto;'></div>", unsafe_allow_html=True)
                
                for idx, (title, g_df, kpi_col, kpi_val) in enumerate(groups_data):
                    with cols[col_mappings[idx]]:
                        render_dashboard_column(title, g_df, kpi_col, kpi_val, unit_mode, unit_label)

def render_dashboard_column(title, g_df, kpi_col, kpi_val, unit_mode, unit_label):
    """Helper layout compiler to handle repeating column metric sets."""
    st.markdown(f"**{title}**")
    if g_df.empty or g_df['latest_temp'].isnull().all():
        st.caption("No recent data")
        return
    
    latest_val = g_df['latest_temp'].mean()
    c_min, c_max = g_df['min_now'].min(), g_df['max_now'].max()
    m24, x24 = g_df['min_24h'].min(), g_df['max_24h'].max()

    def convert(v):
        if pd.isnull(v) or pd.isna(v): return None
        return (v - 32) * 5/9 if unit_mode == "Celsius" else v

    l_conv, c_min, c_max, m24, x24 = map(convert, [latest_val, c_min, c_max, m24, x24])

    st.metric("Avg (Latest)", f"{l_conv:.1f}{unit_label}")
    
    if kpi_col:
        pct = g_df[kpi_col].iloc[0]
        color = "green" if pct == 100 else "#FF8C00" if pct > 0 else "gray"
        st.markdown(f"<p style='font-size:0.85rem; color:{color};'><b>{pct:.0f}%</b> Nodes ≤ {kpi_val}°F</p>", unsafe_allow_html=True)

    range_html = "<div style='font-size: 0.8rem; line-height: 1.2; margin-bottom: 10px;'><b>Normal Ranges:</b><br>"
    if c_min is not None and c_max is not None:
        range_html += f"Current: {c_min:.1f} to {c_max:.1f}{unit_label}<br>"
    else:
        range_html += "Current: No Data<br>"
    
    if m24 is not None and x24 is not None:
        range_html += f"24h Range: {m24:.1f} to {x24:.1f}{unit_label}"
    else:
        range_html += "24h Range: No Data"
    range_html += "</div>"
    st.markdown(range_html, unsafe_allow_html=True)

    st.markdown("<div style='font-size: 0.75rem; border-top: 1px solid #eee; padding-top: 5px;'>", unsafe_allow_html=True)


def get_trend_arrow(current, previous):
    """Helper to generate trend icons with updated blue downward arrow."""
    if pd.isnull(current) or pd.isnull(previous): return "N/A"
    delta = current - previous
    if delta > 0.1: return f"🔺 +{delta:.1f}"
    if delta < -0.1: return f"🔹 {delta:.1f}"
    return "➡️ 0.0"

# =============================================================================
# WORKSPACE PAGE 4: SENSOR STATUS COMPONENT LIST
# =============================================================================
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
    Strictly locked to: project_registry, master_data_view, and manual_rejections.
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
            return (now_local - ts_aware.tz_convert(display_tz)).total_seconds() / 3600

        df['last_seen_hrs'] = df['last_ping'].apply(get_lag)

        # 4. FORMATTING HELPERS
        def get_status_icon(hrs):
            if hrs == float('inf') or hrs >= 999.0: return "❌ Never"
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
            min_hours_lag = loc_group['last_seen_hrs'].min()
            max_hours_lag = loc_group['last_seen_hrs'].max()
            
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
                'Best Seen': get_status_icon(min_hours_lag),
                'Worst Seen': get_status_icon(max_hours_lag)
            })
            
        summary_df = pd.DataFrame(summary_rows)

        def style_missing_counters(val_df):
            canvas = pd.DataFrame('', index=val_df.index, columns=val_df.columns)
            target_cols = ['Seen 1h', 'Seen 6h', 'Seen 24h']
            
            for idx in val_df.index:
                total = val_df.loc[idx, 'Total Nodes']
                for col in target_cols:
                    seen = val_df.loc[idx, col]
                    missing = total - seen
                    
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


def render_project_status_dashboard(client, selected_project, unit_label, target_registry):
    """Renders high-level project summaries segmented by structural hardware groupings."""
    st.subheader("📊 Project Status Summary")
    
    query = f"""
        SELECT 
            n.NodeNum, n.Bank, n.Location, n.Depth,
            CASE 
                WHEN (n.Bank LIKE 'S%' OR n.Location LIKE 'S%') AND (n.Bank NOT LIKE '%Amb%' AND n.Location NOT LIKE '%Amb%') THEN 'Supply'
                WHEN (n.Bank LIKE 'R%' OR n.Location LIKE 'R%') AND (n.Bank NOT LIKE '%Amb%' AND n.Location NOT LIKE '%Amb%') THEN 'Return'
                WHEN (n.Bank LIKE '%Amb%' OR n.Location LIKE '%Amb%') THEN 'Ambient'
                WHEN n.Depth IS NOT NULL THEN 'TempPipes'
                ELSE 'Other'
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
        st.error(f"Dashboard Query Failed: {e}")
        return

    if df.empty:
        st.info("No active nodes found for dashboard summary.")
        return

    cols = st.columns(4)
    type_map = {"Supply": (cols[0], "📥"), "Return": (cols[1], "📤"), "TempPipes": (cols[2], "📏"), "Ambient": (cols[3], "☁️")}
    now_utc = pd.Timestamp.now(tz='UTC')

    for h_type, (col, icon) in type_map.items():
        g_df = df[df['hardware_type'] == h_type]
        with col:
            st.markdown(f"#### {icon} {h_type}")
            if g_df.empty or g_df['latest_ts'].isna().all():
                st.caption("No recent data")
                continue
            
            latest_time = g_df['latest_ts'].max()
            if latest_time.tzinfo is None:
                latest_time = latest_time.tz_localize('UTC')
            else:
                latest_time = latest_time.tz_convert('UTC')
            
            lag_hrs = (now_utc - latest_time).total_seconds() / 3600
            val = g_df['avg_now'].mean() if pd.notnull(g_df['avg_now'].mean()) else g_df['latest_temp'].mean()
            
            if lag_hrs > 1.1: 
                st.subheader(f"⚠️ Offline {int(lag_hrs)}h")
            else: 
                unit_mode = st.session_state.get('unit_mode', 'Fahrenheit')
                display_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
                st.title(f"{display_val:.1f}{unit_label}")
            
            active_1h = int(g_df['avg_now'].notnull().sum())
            active_24h = int((g_df['pings_24h'] > 0).sum())
            st.write(f"**{active_1h}/{len(g_df)}** (1h) | **{active_24h}/{len(g_df)}** (24h)")
            
            min_now_val = g_df['min_now'].min()
            max_now_val = g_df['max_now'].max()
            min_24h_val = g_df['min_24h'].min()
            max_24h_val = g_df['max_24h'].max()
            
            if pd.notnull(min_now_val) and pd.notnull(max_now_val):
                mn = (min_now_val - 32) * 5/9 if st.session_state.get('unit_mode') == "Celsius" else min_now_val
                mx = (max_now_val - 32) * 5/9 if st.session_state.get('unit_mode') == "Celsius" else max_now_val
                st.caption(f"Cur: {mn:.1f} to {mx:.1f}{unit_label}")
            else:
                st.caption(f"Cur: N/A to N/A")
                
            if pd.notnull(min_24h_val) and pd.notnull(max_24h_val):
                mn24 = (min_24h_val - 32) * 5/9 if st.session_state.get('unit_mode') == "Celsius" else min_24h_val
                mx24 = (max_24h_val - 32) * 5/9 if st.session_state.get('unit_mode') == "Celsius" else max_24h_val
                st.caption(f"24h: {mn24:.1f} to {mx24:.1f}{unit_label}")
            else:
                st.caption(f"24h: N/A to N/A")
            
            t_row = st.columns(2)
            try:
                prev_1h = g_df['avg_1h_prev'].mean()
                arrow_1h = get_trend_arrow(val, prev_1h) if pd.notnull(prev_1h) else "➡️ N/A"
                t_row[0].caption(f"1h\n{arrow_1h}")
            except Exception:
                t_row[0].caption("1h\n➡️ N/A")
                
            try:
                prev_6h = g_df['avg_6h_prev'].mean()
                arrow_6h = get_trend_arrow(val, prev_6h) if pd.notnull(prev_6h) else "➡️ N/A"
                t_row[1].caption(f"6h\n{arrow_6h}")
            except Exception:
                t_row[1].caption("6h\n➡️ N/A")


def render_hardware_integrity_table(client, selected_project, unit_mode, unit_label, target_registry):
    """Renders a detailed table showing connectivity, coverage, and recent activity sorted by latency."""
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
                -- standard hour truncations used inside manual rejection tables
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

    def diagnostic_styler(data):
        style_df = pd.DataFrame('', index=data.index, columns=data.columns)
        for i in data.index:
            style_df.loc[i, 'Last Seen'] = df.loc[i, 'Seen_Style']
            if df.loc[i, 'SensorStatus'] == 'Diagnostic':
                style_df.loc[i, 'Node ID'] = 'background-color: #ff4b4b; color: white; font-weight: bold;'
        return style_df

    st.dataframe(
        display_df.style.apply(diagnostic_styler, axis=None), 
        use_container_width=True, 
        hide_index=True,
        column_config={
            "24h Coverage": st.column_config.ProgressColumn(
                "24h Coverage", 
                format="%.1f%%", 
                min_value=0, 
                max_value=100
            ),
            "1h Pings": st.column_config.NumberColumn("1h Pings", format="%d"),
            "6h Pings": st.column_config.NumberColumn("6h Pings", format="%d"),
            "24h Pings": st.column_config.NumberColumn("24h Pings", format="%d"),
        }
    )
##################
# High temp mask #
##################
def apply_sanity_filter(df):
    """
    Automated filter for rogue data points.
    Flags anything outside the absolute limits of -30°F and 120°F as BADDATA.
    """
    if df.empty:
        return df

    # Logic: Mark records outside of strict industrial physical limits [-30, 120]
    bad_condition = (df['temperature'] > 120) | (df['temperature'] < -30)
    
    # If your view or data holds an approval column, mark it in-memory
    if 'approve' in df.columns:
        df.loc[bad_condition, 'approve'] = 'BADDATA'
    elif 'approval_status' in df.columns:
        df.loc[bad_condition, 'approval_status'] = 'BADDATA'

    return df

##############################
# Page 1 - Dashboard Summary #
##############################
def render_summary_dashboard(unit_label, unit_mode, display_tz):
    """
    The main Global Project Summary dashboard.
    - Fixed English translations for ranges.
    - Robust timezone-aware sensor check-in counters.
    - Automated link directory for active external client portals.
    - FIXED: Outlier filter raised to 120°F to accommodate warm pre-freeze zones.
    - FIXED: Re-aligned grouping hierarchy to prioritize numeric Depth for TempPipes.
    """
    st.header("🌐 Global Project Summary")
    
    client = get_bq_client()
    if client is None: return

    mobile_mode = st.session_state.get("mobile_optimized_toggle", False)

    # SQL QUERY: Balanced approach showing active field data while purging bad data
    summary_q = f"""
        WITH active_projects AS (
            SELECT CAST(Project AS STRING) as Project, ProjectName, ProjectStatus, Date_Freezedown
            FROM `{PROJECT_REGISTRY_TABLE}`
            WHERE UPPER(TRIM(CAST(ShowActive AS STRING))) = 'YES'
              AND UPPER(CAST(Project AS STRING)) NOT LIKE '%OFFICE%'
        ),
        raw_data AS (
            SELECT 
                p.Project, n.Bank, n.Location, n.Depth, m.temperature, m.timestamp, n.NodeNum
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
            INNER JOIN active_projects p 
                ON REGEXP_EXTRACT(TRIM(CAST(m.Project AS STRING)), r'^\\d+') = REGEXP_EXTRACT(TRIM(CAST(p.Project AS STRING)), r'^\\d+')
            LEFT JOIN `{NODE_REGISTRY_TABLE}` n 
              -- HARDENED JOIN: Strips formatting punctuation to cleanly match Lord vs SensorPush strings
              ON REGEXP_REPLACE(UPPER(TRIM(CAST(m.NodeNum AS STRING))), r'[:-]', '') = 
                 REGEXP_REPLACE(UPPER(TRIM(CAST(n.NodeNum AS STRING))), r'[:-]', '')
            WHERE m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
              -- BALANCED RULE: Show verified AND streaming real-time data, but block bad data
              AND UPPER(COALESCE(CAST(m.approval_status AS STRING), 'PENDING')) NOT IN ('BADDATA', 'FALSE', '0')
              -- Outlier Shield: Aligned with the physical boundary limit of 120°F to handle high-ambient ground zones
              AND NOT (m.temperature > 120.0 AND NOT STARTS_WITH(m.NodeNum, 'SP'))
        ),
        MaxTime AS (
            SELECT MAX(timestamp) as max_ts FROM raw_data
        ),
        LatestStats AS (
            SELECT 
                r.Project, r.Bank, r.Location, r.Depth, r.NodeNum,
                AVG(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR) THEN r.temperature END) as avg_now,
                AVG(CASE WHEN r.timestamp BETWEEN TIMESTAMP_SUB(m.max_ts, INTERVAL 2 HOUR) AND TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR) THEN r.temperature END) as avg_1h,
                AVG(CASE WHEN r.timestamp BETWEEN TIMESTAMP_SUB(m.max_ts, INTERVAL 7 HOUR) AND TIMESTAMP_SUB(m.max_ts, INTERVAL 6 HOUR) THEN r.temperature END) as avg_6h,
                AVG(CASE WHEN r.timestamp BETWEEN TIMESTAMP_SUB(m.max_ts, INTERVAL 25 HOUR) AND TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR) THEN r.temperature END) as avg_24h,
                MIN(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR) THEN r.temperature END) as min_now,
                MAX(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR) THEN r.temperature END) as max_now,
                MIN(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR) THEN r.temperature END) as min_24h,
                MAX(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR) THEN r.temperature END) as max_24h,
                
                COUNTIF(r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR)) as checkins_1h,
                COUNTIF(r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR)) as checkins_24h,
                
                ARRAY_AGG(r.temperature ORDER BY r.timestamp DESC LIMIT 1)[OFFSET(0)] as latest_temp,
                MAX(r.timestamp) as latest_ts
            FROM raw_data r
            CROSS JOIN MaxTime m
            GROUP BY 1, 2, 3, 4, 5
        )
        SELECT 
            p.*, ls.*,
            (COUNTIF(ls.Bank LIKE 'S%' AND ls.latest_temp <= -10) OVER(PARTITION BY p.Project) / NULLIF(COUNTIF(ls.Bank LIKE 'S%') OVER(PARTITION BY p.Project), 0)) * 100 as supply_kpi,
            (COUNTIF(ls.Bank LIKE 'R%' AND ls.latest_temp <= 0) OVER(PARTITION BY p.Project) / NULLIF(COUNTIF(ls.Bank LIKE 'R%') OVER(PARTITION BY p.Project), 0)) * 100 as return_kpi,
            (COUNTIF(ls.Depth IS NOT NULL AND ls.latest_temp <= 32) OVER(PARTITION BY p.Project) / NULLIF(COUNTIF(ls.Depth IS NOT NULL) OVER(PARTITION BY p.Project), 0)) * 100 as freeze_kpi
        FROM active_projects p
        LEFT JOIN LatestStats ls ON p.Project = ls.Project
    """
    
    try:
        df = client.query(summary_q).to_dataframe()
        df[['Bank', 'Location']] = df[['Bank', 'Location']].fillna('')
    except Exception as e:
        st.error(f"Dashboard Query Failed: {e}")
        return

    if df.empty:
        st.info("No active projects found matching your 'ShowActive = Yes' parameter checks.")
        return

    for project in sorted(df['Project'].unique()):
        p_df = df[df['Project'] == project]
        p_name = p_df['ProjectName'].iloc[0] or project
        f_date = p_df['Date_Freezedown'].iloc[0]
        
        day_text, f_date_display = "", "Not Set"
        if pd.notnull(f_date):
            f_date_display = pd.to_datetime(f_date).strftime('%b %d, %Y')
            days_elapsed = (pd.Timestamp.now(tz=display_tz).date() - pd.to_datetime(f_date).date()).days
            day_text = f"🗓️ **Day {max(0, days_elapsed)}**"
        
        with st.container(border=True):
            h1, h2 = st.columns([2, 1])
            h1.subheader(f"🏗️ {p_name}")
            h2.markdown(f"<div style='text-align: right;'>{day_text}<br><small>ID: {project}</small></div>", unsafe_allow_html=True)
            
            # --- CLIENT PORTAL LINK INJECTION ENGINE ---
            proj_match = re.search(r'\b(\d{4})\b', str(project))
            if proj_match:
                job_number = proj_match.group(1)
                portal_url = f"https://sf{job_number}.streamlit.app"
                st.markdown(f"🔗 **External Client Portal:** [{p_name} Portal Site Link]({portal_url})")
            
            # --- ACCURATE CHECK-IN COUNTERS ---
            active_1h = p_df[p_df['checkins_1h'] > 0]['NodeNum'].nunique()
            active_24h = p_df[p_df['checkins_24h'] > 0]['NodeNum'].nunique()
            total_nodes = p_df['NodeNum'].dropna().nunique()
            
            st.markdown(
                f"📡 **Hardware Status:** `{active_1h}` nodes pinged in the last hour | "
                f"`{active_24h}` nodes pinged in the last 24h (Total Pool: `{total_nodes}` registered)"
            )
            st.divider() 

            # Data isolation
            is_amb = p_df['Bank'].str.contains('Amb', case=False) | p_df['Location'].str.contains('Amb', case=False)
            
            # CRITICAL FIX: Prioritize valid numeric Depths for TempPipes first to stop multi-channel Lord nodes from slipping into brines
            is_tp = p_df['Depth'].notnull() & (p_df['Depth'].astype(str).str.strip() != '') & ~is_amb
            is_s = (p_df['Bank'].str.startswith('S') | p_df['Location'].str.startswith('S')) & ~is_amb & ~is_tp
            is_r = (p_df['Bank'].str.startswith('R') | p_df['Location'].str.startswith('R')) & ~is_amb & ~is_tp

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
                col_mappings = [0, 2, 4, 6]
                spacer_mappings = [1, 3, 5]
                
                for s_idx in spacer_mappings:
                    cols[s_idx].markdown("<div style='border-left: 1px solid #ddd; height: 320px; margin: auto;'></div>", unsafe_allow_html=True)
                
                for idx, (title, g_df, kpi_col, kpi_val) in enumerate(groups_data):
                    with cols[col_mappings[idx]]:
                        render_dashboard_column(title, g_df, kpi_col, kpi_val, unit_mode, unit_label)

def render_dashboard_column(title, g_df, kpi_col, kpi_val, unit_mode, unit_label):
    """Helper layout compiler to handle repeating column metric sets."""
    st.markdown(f"**{title}**")
    if g_df.empty or g_df['latest_temp'].isnull().all():
        st.caption("No recent data")
        return
    
    latest_val = g_df['latest_temp'].mean()
    c_min, c_max = g_df['min_now'].min(), g_df['max_now'].max()
    m24, x24 = g_df['min_24h'].min(), g_df['max_24h'].max()

    def convert(v):
        if pd.isnull(v) or pd.isna(v): return None
        return (v - 32) * 5/9 if unit_mode == "Celsius" else v

    l_conv, c_min, c_max, m24, x24 = map(convert, [latest_val, c_min, c_max, m24, x24])

    st.metric("Avg (Latest)", f"{l_conv:.1f}{unit_label}")
    
    if kpi_col:
        pct = g_df[kpi_col].iloc[0]
        color = "green" if pct == 100 else "#FF8C00" if pct > 0 else "gray"
        st.markdown(f"<p style='font-size:0.85rem; color:{color};'><b>{pct:.0f}%</b> Nodes ≤ {kpi_val}°F</p>", unsafe_allow_html=True)

    range_html = "<div style='font-size: 0.8rem; line-height: 1.2; margin-bottom: 10px;'><b>Normal Ranges:</b><br>"
    if c_min is not None and c_max is not None:
        range_html += f"Current: {c_min:.1f} to {c_max:.1f}{unit_label}<br>"
    else:
        range_html += "Current: No Data<br>"
    
    if m24 is not None and x24 is not None:
        range_html += f"24h Range: {m24:.1f} to {x24:.1f}{unit_label}"
    else:
        range_html += "24h Range: No Data"
    range_html += "</div>"
    st.markdown(range_html, unsafe_allow_html=True)
    st.markdown("<div style='font-size: 0.75rem; border-top: 1px solid #eee; padding-top: 5px;'>", unsafe_allow_html=True)


def get_trend_arrow(current, previous):
    """Helper to generate trend icons with updated blue downward arrow."""
    if pd.isnull(current) or pd.isnull(previous): return "N/A"
    delta = current - previous
    if delta > 0.1: return f"🔺 +{delta:.1f}"
    if delta < -0.1: return f"🔹 {delta:.1f}"
    return "➡️ 0.0"

# =============================================================================
# PAGE 4: SENSOR STATUS -
# =============================================================================

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
    Strictly locked to: project_registry, master_data_view, and manual_rejections.
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
            return (now_local - ts_aware.tz_convert(display_tz)).total_seconds() / 3600

        df['last_seen_hrs'] = df['last_ping'].apply(get_lag)

        # 4. FORMATTING HELPERS
        def get_status_icon(hrs):
            if hrs == float('inf') or hrs >= 999.0: return "❌ Never"
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
            min_hours_lag = loc_group['last_seen_hrs'].min()
            max_hours_lag = loc_group['last_seen_hrs'].max()
            
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
                'Best Seen': get_status_icon(min_hours_lag),
                'Worst Seen': get_status_icon(max_hours_lag)
            })
            
        summary_df = pd.DataFrame(summary_rows)

        def style_missing_counters(val_df):
            canvas = pd.DataFrame('', index=val_df.index, columns=val_df.columns)
            target_cols = ['Seen 1h', 'Seen 6h', 'Seen 24h']
            
            for idx in val_df.index:
                total = val_df.loc[idx, 'Total Nodes']
                for col in target_cols:
                    seen = val_df.loc[idx, col]
                    missing = total - seen
                    
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


def render_project_status_dashboard(client, selected_project, unit_label, target_registry):
    """Renders high-level project summaries segmented by structural hardware groupings."""
    st.subheader("📊 Project Status Summary")
    
    query = f"""
        SELECT 
            n.NodeNum, n.Bank, n.Location, n.Depth,
            CASE 
                WHEN (n.Bank LIKE 'S%' OR n.Location LIKE 'S%') AND (n.Bank NOT LIKE '%Amb%' AND n.Location NOT LIKE '%Amb%') THEN 'Supply'
                WHEN (n.Bank LIKE 'R%' OR n.Location LIKE 'R%') AND (n.Bank NOT LIKE '%Amb%' AND n.Location NOT LIKE '%Amb%') THEN 'Return'
                WHEN (n.Bank LIKE '%Amb%' OR n.Location LIKE '%Amb%') THEN 'Ambient'
                WHEN n.Depth IS NOT NULL THEN 'TempPipes'
                ELSE 'Other'
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
        st.error(f"Dashboard Query Failed: {e}")
        return

    if df.empty:
        st.info("No active nodes found for dashboard summary.")
        return

    cols = st.columns(4)
    type_map = {"Supply": (cols[0], "📥"), "Return": (cols[1], "📤"), "TempPipes": (cols[2], "📏"), "Ambient": (cols[3], "☁️")}
    now_utc = pd.Timestamp.now(tz='UTC')

    for h_type, (col, icon) in type_map.items():
        g_df = df[df['hardware_type'] == h_type]
        with col:
            st.markdown(f"#### {icon} {h_type}")
            if g_df.empty or g_df['latest_ts'].isna().all():
                st.caption("No recent data")
                continue
            
            latest_time = g_df['latest_ts'].max()
            if latest_time.tzinfo is None:
                latest_time = latest_time.tz_localize('UTC')
            else:
                latest_time = latest_time.tz_convert('UTC')
            
            lag_hrs = (now_utc - latest_time).total_seconds() / 3600
            val = g_df['avg_now'].mean() if pd.notnull(g_df['avg_now'].mean()) else g_df['latest_temp'].mean()
            
            if lag_hrs > 1.1: 
                st.subheader(f"⚠️ Offline {int(lag_hrs)}h")
            else: 
                unit_mode = st.session_state.get('unit_mode', 'Fahrenheit')
                display_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
                st.title(f"{display_val:.1f}{unit_label}")
            
            active_1h = int(g_df['avg_now'].notnull().sum())
            active_24h = int((g_df['pings_24h'] > 0).sum())
            st.write(f"**{active_1h}/{len(g_df)}** (1h) | **{active_24h}/{len(g_df)}** (24h)")
            
            min_now_val = g_df['min_now'].min()
            max_now_val = g_df['max_now'].max()
            min_24h_val = g_df['min_24h'].min()
            max_24h_val = g_df['max_24h'].max()
            
            if pd.notnull(min_now_val) and pd.notnull(max_now_val):
                mn = (min_now_val - 32) * 5/9 if st.session_state.get('unit_mode') == "Celsius" else min_now_val
                mx = (max_now_val - 32) * 5/9 if st.session_state.get('unit_mode') == "Celsius" else max_now_val
                st.caption(f"Cur: {mn:.1f} to {mx:.1f}{unit_label}")
            else:
                st.caption(f"Cur: N/A to N/A")
                
            if pd.notnull(min_24h_val) and pd.notnull(max_24h_val):
                mn24 = (min_24h_val - 32) * 5/9 if st.session_state.get('unit_mode') == "Celsius" else min_24h_val
                mx24 = (max_24h_val - 32) * 5/9 if st.session_state.get('unit_mode') == "Celsius" else max_24h_val
                st.caption(f"24h: {mn24:.1f} to {mx24:.1f}{unit_label}")
            else:
                st.caption(f"24h: N/A to N/A")
            
            t_row = st.columns(2)
            try:
                prev_1h = g_df['avg_1h_prev'].mean()
                arrow_1h = get_trend_arrow(val, prev_1h) if pd.notnull(prev_1h) else "➡️ N/A"
                t_row[0].caption(f"1h\n{arrow_1h}")
            except Exception:
                t_row[0].caption("1h\n➡️ N/A")
                
            try:
                prev_6h = g_df['avg_6h_prev'].mean()
                arrow_6h = get_trend_arrow(val, prev_6h) if pd.notnull(prev_6h) else "➡️ N/A"
                t_row[1].caption(f"6h\n{arrow_6h}")
            except Exception:
                t_row[1].caption("6h\n➡️ N/A")

# =============================================================================
# HARDWARE INTEGRITY MATRIX SUMMARY
# =============================================================================
def render_hardware_integrity_table(client, selected_project, unit_mode, unit_label, target_registry):
    """Renders a detailed table showing connectivity, coverage, and recent activity sorted by latency."""
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

    def diagnostic_styler(data):
        style_df = pd.DataFrame('', index=data.index, columns=data.columns)
        for i in data.index:
            style_df.loc[i, 'Last Seen'] = df.loc[i, 'Seen_Style']
            if df.loc[i, 'SensorStatus'] == 'Diagnostic':
                style_df.loc[i, 'Node ID'] = 'background-color: #ff4b4b; color: white; font-weight: bold;'
        return style_df

    st.dataframe(
        display_df.style.apply(diagnostic_styler, axis=None), 
        use_container_width=True, 
        hide_index=True,
        column_config={
            "24h Coverage": st.column_config.ProgressColumn(
                "24h Coverage", 
                format="%.1f%%", 
                min_value=0, 
                max_value=100
            ),
            "1h Pings": st.column_config.NumberColumn("1h Pings", format="%d"),
            "6h Pings": st.column_config.NumberColumn("6h Pings", format="%d"),
            "24h Pings": st.column_config.NumberColumn("24h Pings", format="%d"),
        }
    )
# =============================================================================
# PAGE MODULE: 🛠️ NODE MANAGER (ARCHIVED STAGING BLOCK)
# =============================================================================

def render_node_selector(reg_df, proj_list):
    """Renders a filtered fleet hardware configuration status matrix view."""
    st.subheader("🎯 Active Node Registry")
    hide_archived = st.checkbox("Hide Archived Records", value=True, key="ns_hide_archived_toggle")
    
    df = reg_df.copy()
    if hide_archived and 'SensorStatus' in df.columns:
        df = df[
            (df['SensorStatus'].str.lower() != "archived") & 
            (df['Location'].str.contains("Archive", case=False, na=False) == False)
        ]

    c1, c2, c3 = st.columns(3)
    with c1:
        f_proj = st.selectbox("Filter by Project Space", ["All", "Unassigned"] + proj_list, key="ns_proj_f")
    with c2:
        if f_proj == "All":
            loc_opts = df['Location'].dropna().unique().tolist()
        elif f_proj == "Unassigned":
            loc_opts = df[df['Project'].isna() | (df['Project'] == "") | (df['Project'] == "Office") | (df['Location'] == "Office")]['Location'].dropna().unique().tolist()
        else:
            loc_opts = df[df['Project'] == f_proj]['Location'].dropna().unique().tolist()
            
        f_loc = st.selectbox("Filter by Physical Location", ["All"] + sorted(loc_opts), key="ns_loc_f")
    with c3:
        search_term = st.text_input("Global Search (Node ID)", "", key="ns_search_f")

    if f_proj == "Unassigned":
        df = df[df['Project'].isna() | (df['Project'] == "") | (df['Project'] == "Office")]
    elif f_proj != "All":
        df = df[df['Project'] == f_proj]
        
    if f_loc != "All":
        df = df[df['Location'] == f_loc]
        
    if search_term:
        df = df[df['NodeNum'].str.contains(search_term, case=False, na=False)]

    if df.empty:
        st.info("No matching nodes located under current filter parameters.")
        return None

    # Recalculate physical positions to avoid row selection drift anomalies inside standard layouts
    df = df.reset_index(drop=True)

    if 'hours_hidden' in df.columns:
        df['hours_hidden'] = pd.to_numeric(df['hours_hidden'], errors='coerce').fillna(float('inf'))
        df = df.sort_values(by='hours_hidden', ascending=True).reset_index(drop=True)
    else:
        df['hours_hidden'] = float('inf')

    st.markdown("### 📡 Hardware Inventory Fleet Breakdown")
    
    def classify_hardware_family(node):
        node_str = str(node).lower()
        if "-ch" in node_str: return "Lord"
        if node_str.startswith("sp"): return "SP"
        if node_str.startswith("tp"): return "TP"
        return "None of the Above"

    summary_df = reg_df.copy()
    summary_df['Hardware Family'] = summary_df['NodeNum'].apply(classify_hardware_family)
    summary_df['Parent ID'] = summary_df['NodeNum'].apply(
        lambda x: re.split(r'(?i)-ch', str(x))[0] if "-ch" in str(x).lower() else x
    )
    
    if 'End_Date' in summary_df.columns:
        summary_df['is_active'] = summary_df['End_Date'].isna()
    else:
        summary_df['is_active'] = True
        
    sort_keys = ['Parent ID', 'is_active']
    sort_asc = [True, False]
    if 'Start_Date' in summary_df.columns:
        sort_keys.append('Start_Date')
        sort_asc.append(False)
        
    summary_df = summary_df.sort_values(by=sort_keys, ascending=sort_asc)
    deduped_units = summary_df.drop_duplicates(subset=['Parent ID']).copy()
    
    try:
        fleet_pivot = deduped_units.groupby(['Hardware Family', 'SensorStatus']).size().unstack(fill_value=0)
        desired_order = ["TP", "SP", "Lord", "None of the Above"]
        fleet_pivot = fleet_pivot.reindex(desired_order, fill_value=0)
        fleet_pivot['Total Units'] = fleet_pivot.sum(axis=1)
        st.dataframe(fleet_pivot, use_container_width=True)
    except Exception:
        st.info("💡 Inventory matrix is populating. Assign statuses to your hardware to generate totals.")
        
    st.markdown("---")
    st.markdown("### 📋 Current Asset Allocation Matrix")

    if "last_selected_node" not in st.session_state: st.session_state["last_selected_node"] = None
    if "active_selected_node_record" not in st.session_state: st.session_state["active_selected_node_record"] = None

    ed_key = "node_registry_editor"
    if ed_key in st.session_state and "edited_rows" in st.session_state[ed_key]:
        changed_rows = st.session_state[ed_key]["edited_rows"]
        newly_checked = [int(idx) for idx, changes in changed_rows.items() if changes.get("Select") == True]
        
        if newly_checked and not df.empty:
            latest_idx = newly_checked[-1]
            if latest_idx != st.session_state["last_selected_node"]:
                st.session_state["last_selected_node"] = latest_idx
                rec_dict = df.iloc[latest_idx].drop(["hours_hidden"], errors='ignore').to_dict()
                rec_dict["Select"] = True
                st.session_state["active_selected_node_record"] = rec_dict
                st.session_state[ed_key]["edited_rows"] = {}
                st.rerun()
        
        elif any(changes.get("Select") == False for idx, changes in changed_rows.items()):
            st.session_state["last_selected_node"] = None
            st.session_state["active_selected_node_record"] = None
            st.session_state[ed_key]["edited_rows"] = {}
            st.rerun()

    df.insert(0, "Select", False)
    if st.session_state["last_selected_node"] is not None and st.session_state["last_selected_node"] < len(df):
        df.loc[st.session_state["last_selected_node"], "Select"] = True

    def node_selector_styler(data):
        style_canvas = pd.DataFrame('', index=data.index, columns=data.columns)
        for i in data.index:
            try:
                val = data.loc[i, 'hours_hidden']
                hours_val = None if (val == float('inf') or pd.isnull(val)) else float(val)
                color_style = assign_row_color(hours_val)
            except Exception:
                color_style = "background-color: transparent;"
            
            for col in data.columns:
                if col != "Select": style_canvas.loc[i, col] = color_style
        return style_canvas

    unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
    unit_label = st.session_state.get("unit_label", "°F")
    
    def get_pos_label(row):
        if pd.notnull(row.get('Depth')) and row.get('Depth') != 0: return f"{row['Depth']}ft"
        return f"Bank {row['Bank']}" if pd.notnull(row.get('Bank')) and str(row.get('Bank')).strip() != "" else "-"

    df['Position'] = df.apply(get_pos_label, axis=1)
    df['Current Temp'] = df['last_temp'].apply(lambda x: fmt_temp(x, unit_mode, unit_label))

    edited_df = st.data_editor(
        df.style.apply(node_selector_styler, axis=None) if not df.empty else df,
        hide_index=True,
        use_container_width=True,
        column_config={
            "Select": st.column_config.CheckboxColumn("Select", default=False, required=True),
            "Project": "Project", "Location": "Location", "NodeNum": "Node ID",
            "Position": "Depth/Bank", "Last Seen": st.column_config.TextColumn("Last Seen"), "Current Temp": "Current Temp",
        },
        disabled=[col for col in df.columns if col != "Select"],
        column_order=["Select", "Project", "Location", "NodeNum", "Position", "Last Seen", "Current Temp"], 
        key=ed_key
    )

    if st.session_state["active_selected_node_record"] is not None:
        selected_returned_row = st.session_state["active_selected_node_record"].copy()
        if "Select" in selected_returned_row: del selected_returned_row["Select"]
    else:
        selected_returned_row = None
            
    st.markdown("---")
    with st.expander("🧨 Danger Zone: Sync Playground Staging Table Directly to Production"):
        st.error("⚠️ CRITICAL WARNING: This action will completely erase ALL records in your live production `node_registry` and overwrite them with an exact copy of your staging table.")
        confirm_token = st.text_input("Type out 'OVERWRITE' to authorize replacing production logs:", value="", key="force_production_overwrite_token_input")
        
        if st.button("💥 Wipe Production & Clone Playground Table", type="primary", use_container_width=True):
            if confirm_token.strip() != "OVERWRITE":
                st.error("Authorization token verification failed. Action aborted.")
            else:
                prod_table = f"{PROJECT_ID}.{DATASET_ID}.node_registry"
                dummy_table = f"{PROJECT_ID}.{DATASET_ID}.node_registry_dummy"
                job_config = bigquery.QueryJobConfig(write_disposition="WRITE_TRUNCATE", destination=prod_table)
                sql = f"SELECT * FROM `{dummy_table}`"
                try:
                    with st.spinner("Executing complete environment teardown and reconstruction workflows..."):
                        client.query(sql, job_config=job_config).result()
                    st.success("🔥 Production registry completely reset and replaced with dummy playground snapshot!")
                    st.cache_data.clear()
                    time.sleep(1.5)
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to copy staging parameters: {e}")
                    st.code(sql, language="sql")
                    
    return selected_returned_row

# =============================================================================
# Page: Data Processing
# =============================================================================

def render_data_processing_page(selected_project):
    """
    Page Name: Data Processing
    Handles manual file ingestion, data masking limits filters, wide-format engineering exports,
    Theoretical Reference Curve Library, Chiller Asset Inventory, and Site Event entries.
    """
    st.header("⚙️ Data Processing & Reference Engine")
    
    client = get_bq_client()
    if client is None:
        st.error("Database connection unavailable.")
        return
        
    # Standardized 5-tab layout order matching blueprint specifications
    tab_upload, tab_export, tab_ref_library, tab_event_log, tab_chiller_reg = st.tabs([
        "📄 Upload Telemetry", 
        "📥 Export Report",
        "📈 Ref Curve Library", 
        "🚨 Log Site Event",
        "❄️ Register Chiller"
    ])
    
    CHILLER_REG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.chiller_registry"
    CHILLER_MAP_TABLE = f"{PROJECT_ID}.{DATASET_ID}.chiller_sensor_mapping"
    EVENTS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.freezedown_events"
    
    # --- TAB 1: UPLOAD LOGIC ---
    with tab_upload:
        st.subheader("📄 Manual File Ingestion")
        st.info("Rule: Lord IDs use '-' (58014-ch1). SensorPush IDs are numeric.")
        
        u_file = st.file_uploader("Select CSV or Excel file", type=['csv', 'xlsx'], key="manual_upload_main")
        
        if u_file is not None:
            try:
                # 1. FORMAT DETECTION
                is_sensorconnect, skip_rows = False, 0
                if u_file.name.endswith('.csv'):
                    u_file.seek(0)
                    for i, line in enumerate(u_file):
                        if b"DATA_START" in line:
                            is_sensorconnect, skip_rows = True, i + 1
                            break
                    u_file.seek(0)

                # 2. DATA READING
                if is_sensorconnect:
                    st.info("Detected Format: Lord SensorConnect (Wide)")
                    df_raw = pd.read_csv(u_file, encoding='latin1', skiprows=skip_rows, dtype=str)
                elif u_file.name.endswith('.csv'):
                    df_raw = pd.read_csv(u_file, encoding='latin1', dtype=str)
                else:
                    df_raw = pd.read_excel(u_file, dtype=str)

                if not df_raw.empty:
                    df_processed = pd.DataFrame()
                    actual_headers = list(df_raw.columns)
                    clean_headers = [str(h).strip().lower() for h in actual_headers]
                    
                    # BRANCH A: Lord SensorConnect (Melt Wide to Long)
                    if is_sensorconnect:
                        time_col = [h for h in actual_headers if 'time' in h.lower()][0]
                        value_vars = [h for h in actual_headers if h != time_col]
                        df_melted = df_raw.melt(id_vars=[time_col], value_vars=value_vars, var_name='NodeNum', value_name='temperature')
                        
                        df_processed['timestamp'] = pd.to_datetime(df_melted[time_col], errors='coerce', utc=True)
                        df_processed['NodeNum'] = df_melted['NodeNum'].str.strip().str.replace(':', '-')
                        df_processed['temperature'] = pd.to_numeric(df_melted['temperature'], errors='coerce')

                    # BRANCH B: Lord SensorCloud (Standard Long Format)
                    elif any(k in clean_headers for k in ['channel', 'node']) and any('time' in h for h in clean_headers):
                        st.info("Detected Format: Lord (Standard Long)")
                        time_h = actual_headers[next(i for i, h in enumerate(clean_headers) if 'time' in h)]
                        node_h = actual_headers[next(i for i, h in enumerate(clean_headers) if 'channel' in h or 'node' in h)]
                        temp_h = [h for h in actual_headers if 'temp' in h.lower()][0]
                        
                        df_processed['timestamp'] = pd.to_datetime(df_raw[time_h], errors='coerce', utc=True)
                        df_processed['NodeNum'] = df_raw[node_h].str.strip().str.replace(':', '-')
                        df_processed['temperature'] = pd.to_numeric(df_raw[temp_h], errors='coerce')

                    # BRANCH C: SensorPush
                    else:
                        st.info("Detected Format: SensorPush")
                        t_match = [h for h in actual_headers if 'timestamp' in h.lower()][0]
                        v_match = [h for h in actual_headers if 'temp' in h.lower()][0]
                        
                        clean_name = u_file.name.replace(".csv", "").replace(".xlsx", "")
                        match = re.search(r'^([^ \(\)]+)', clean_name)
                        
                        df_processed['timestamp'] = pd.to_datetime(df_raw[t_match], errors='coerce', utc=True)
                        df_processed['temperature'] = pd.to_numeric(df_raw[v_match], errors='coerce')
                        df_processed['NodeNum'] = match.group(1).strip() if match else "Unknown"

                    # 3. AUTOMATED LIMITS FILTER RUNROOM
                    if not df_processed.empty:
                        df_processed = df_processed.dropna(subset=['timestamp', 'temperature'])
                        
                        bad_mask = (df_processed['temperature'] > 120) | (df_processed['temperature'] < -30)
                        
                        bad_count = bad_mask.sum()
                        if bad_count > 0:
                            st.warning(f"⚠️ Sanity Filter: Flagged {bad_count} records exceeding -30°F to 120°F boundary lines as BADDATA.")
                        
                        st.success(f"✅ Prepared {len(df_processed)} records for Node(s): {', '.join(df_processed['NodeNum'].unique())}")
                        
                        is_lord = "-" in str(df_processed['NodeNum'].iloc[0])
                        target_table = "raw_lord" if is_lord else "raw_sensorpush"
                        
                        if st.button(f"🚀 Upload to {target_table}"):
                            with st.spinner("Writing to BigQuery..."):
                                table_id = f"{PROJECT_ID}.{DATASET_ID}.{target_table}"
                                
                                if is_lord:
                                    from decimal import Decimal
                                    df_processed['temperature'] = df_processed['temperature'].apply(lambda x: Decimal(str(round(x, 1))) if pd.notnull(x) else None)
                                
                                columns_to_upload = ['timestamp', 'NodeNum', 'temperature']
                                upload_payload_df = df_processed[columns_to_upload].copy()
                                
                                job_config = bigquery.QueryJobConfig(
                                    schema=[
                                        bigquery.SchemaField("timestamp", "TIMESTAMP"),
                                        bigquery.SchemaField("NodeNum", "STRING"),
                                        bigquery.SchemaField("temperature", "NUMERIC" if is_lord else "FLOAT"),
                                    ],
                                    write_disposition="WRITE_APPEND"
                                )
                                client.load_table_from_dataframe(upload_payload_df, table_id, job_config=job_config).result()
                                st.success("Upload Complete!")
                                st.cache_data.clear() 

            except Exception as e:
                st.error(f"Ingestion Failed: {e}")

    # --- TAB 2: EXPORT LOGIC ---
    with tab_export:
        st.subheader("📥 Wide-Format Data Export")
        if not selected_project or selected_project == "All Projects":
            st.warning("⚠️ Select a specific project in the sidebar to export data.")
        else:
            c1, c2 = st.columns(2)
            e_start = c1.date_input("Start Date", value=datetime.now() - timedelta(days=30))
            e_end = c2.date_input("End Date", value=datetime.now())
            
            with st.spinner("Processing dashboard records..."):
                full_df = get_universal_portal_data(selected_project)
            
            if not full_df.empty:
                all_locs = sorted(full_df['Location'].unique().tolist())
                selected_locs = st.multiselect("Filter by Location (Leave empty for ALL)", options=all_locs)

                mask = (full_df['timestamp'].dt.date >= e_start) & (full_df['timestamp'].dt.date <= e_end)
                if selected_locs:
                    mask = mask & (full_df['Location'].isin(selected_locs))
                
                export_df = full_df.loc[mask].copy()
                
                if export_df.empty:
                    st.warning("No data found for the selected criteria.")
                else:
                    export_df['Sensor'] = export_df['Location'] + " (" + export_df['NodeNum'].astype(str) + ")"
                    
                    wide_df = export_df.pivot_table(
                        index='timestamp', columns='Sensor', values='temperature', aggfunc='first'
                    ).reset_index()

                    wide_df['timestamp'] = wide_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

                    st.success(f"Report Ready: {len(wide_df.columns)-1} columns generated.")
                    csv_data = wide_df.to_csv(index=False).encode('utf-8')
                    
                    st.download_button(
                        label="💾 Download Custom CSV Export",
                        data=csv_data,
                        file_name=f"{selected_project}_Export_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )

    # --- TAB 3: REFERENCE CURVE LIBRARY ---
    with tab_ref_library:
        st.subheader("📚 Theoretical Curve Library")
        st.write("Manage the target temperature curves used for visual goal-tracking on graphs.")
        
        with st.expander("🗑️ Library Management (Delete/Purge)", expanded=False):
            st.warning("Action is permanent. Purging will remove curves from all graphs.")
            
            try:
                lib_df = client.query(f"SELECT DISTINCT CurveID FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves`").to_dataframe()
                if not lib_df.empty:
                    to_delete = st.selectbox("Select Curve to Remove", sorted(lib_df['CurveID'].tolist()), key="delete_curve_picker")
                    if st.button(f"🗑️ Delete {to_delete}", type="secondary", key="delete_single_curve_btn"):
                        client.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` WHERE CurveID='{to_delete}'").result()
                        st.success(f"Removed {to_delete} from library.")
                        st.cache_data.clear()
                        time.sleep(0.5)
                        st.rerun()
                else:
                    st.info("No curves available to delete.")
            except Exception:
                st.info("Reference table is empty or not yet initialized.")

            st.divider()

            st.error("Danger: This wipes the entire reference database.")
            confirm_purge = st.checkbox("I confirm I want to DELETE ALL curves in the library.", key="confirm_purge_check")
            if st.button("🧨 PURGE ENTIRE LIBRARY", type="primary", disabled=not confirm_purge, key="nuclear_purge_btn"):
                try:
                    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.reference_curves`").result()
                    st.success("Library has been completely purged.")
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    st.error(f"Purge failed: {e}")

        st.divider()

        st.write("### 📤 Upload New Curves")
        st.caption("Expected Format: CSV files (e.g., `2527-TP1.csv`). Data should start on Row 3. Col 1: Day, Col 2: Temp.")
        
        u_files = st.file_uploader(
            "Select CSV Files", 
            type="csv", 
            accept_multiple_files=True, 
            key="ref_uploader_v6" 
        )
        
        if u_files:
            if st.button("💾 Commit Files to BigQuery", key="commit_ref_btn_final", use_container_width=True):
                progress_bar = st.progress(0)
                
                for idx, f in enumerate(u_files):
                    try:
                        curve_id = f.name.replace(".csv", "")
                        try:
                            f.seek(0)
                            ref_df = pd.read_csv(f, skiprows=2, names=['Day', 'Temp'], encoding='utf-8')
                        except Exception:
                            f.seek(0)
                            ref_df = pd.read_csv(f, skiprows=2, names=['Day', 'Temp'], encoding='latin-1')

                        ref_df['Day'] = pd.to_numeric(ref_df['Day'], errors='coerce')
                        ref_df['Temp'] = pd.to_numeric(ref_df['Temp'], errors='coerce')
                        ref_df = ref_df.dropna(subset=['Day', 'Temp'])
                        ref_df['CurveID'] = curve_id

                        if not ref_df.empty:
                            client.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` WHERE CurveID='{curve_id}'").result()
                            table_ref = f"{PROJECT_ID}.{DATASET_ID}.reference_curves"
                            client.load_table_from_dataframe(ref_df, table_ref).result()
                            st.toast(f"Success: {curve_id}", icon="✅")
                        else:
                            st.error(f"❌ {f.name} contained no valid numeric data after row 2.")
                            
                        progress_bar.progress((idx + 1) / len(u_files))
                        
                    except Exception as e:
                        st.error(f"❌ Error processing {f.name}: {e}")
                
                st.success("Library Processing Complete.")
                st.cache_data.clear()
                time.sleep(1)
                st.rerun()

        st.divider()
        st.write("### 📂 Current Library Inventory")
        try:
            inventory_df = client.query(
                f"SELECT CurveID, COUNT(*) as Data_Points, MIN(Day) as Start_Day, MAX(Day) as End_Day "
                f"FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` "
                f"GROUP BY CurveID ORDER BY CurveID"
            ).to_dataframe()
            
            if not inventory_df.empty:
                st.dataframe(inventory_df, use_container_width=True, hide_index=True)
            else:
                st.info("The library table is currently empty.")
        except Exception:
            st.warning("⚠️ Reference table (`reference_curves`) not found in BigQuery.")

    # --- TAB 4: SITE EVENT LOGGING ENGINE & INTEGRATED HISTORY LOG ---
    with tab_event_log:
        st.subheader("🚨 Log New Site Event Entry")
        st.write("Track power transitions, compressor cycles, repair costs, and generator behaviors relative to active freeze down operations.")
        
        try:
            proj_reg_q = f"SELECT Project FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE UPPER(TRIM(CAST(ShowActive AS STRING))) = 'YES' ORDER BY Project"
            active_projects_list = sorted(client.query(proj_reg_q).to_dataframe()['Project'].dropna().unique().tolist())
        except Exception:
            active_projects_list = ["Office"]
            
        try:
            active_chillers_list = sorted(client.query(f"SELECT chiller_id FROM `{CHILLER_REG_TABLE}`").to_dataframe()['chiller_id'].tolist())
        except Exception:
            active_chillers_list = []

        import uuid
        with st.form("comprehensive_site_event_logger_form"):
            col_el1, col_el2, col_el3 = st.columns(3)
            target_proj = col_el1.selectbox("Assign Event to Project Space Phase*", active_projects_list, key="input_ev_proj")
            event_date = col_el2.date_input("Event Log Start Date*", value=datetime.now().date(), key="input_ev_date")
            event_time = col_el3.time_input("Event Log Start Time (UTC)*", value=datetime.now().time(), key="input_ev_time")
            
            col_el4, col_el5, col_el6 = st.columns(3)
            event_type = col_el4.selectbox("Type of Event*", ["Chiller Turn On", "Chiller Turn Off", "Power Source Transition", "Generator Fault / Outage", "Equipment Repair / Maintenance", "Other Site Anomaly"], key="input_ev_type")
            power_type = col_el5.selectbox("Active Power Type Source*", ["Line Power", "Generator", "None / Outage State"], key="input_ev_power")
            assoc_chiller = col_el6.selectbox("Associated Chiller Loop (Optional)", ["None"] + active_chillers_list, key="input_ev_chiller")
            
            col_el7, col_el8 = st.columns(2)
            proj_system = col_el7.text_input("Project System Loop Identifier (Optional)", placeholder="e.g., Loop A, Loop B")
            event_cost_val = col_el8.number_input("Associated Event / Repair Cost ($)", min_value=0.0, value=0.0, step=50.0, format="%.2f")
            
            event_desc = st.text_input("Operational Event Description / Detailed Log Alert Notes*", placeholder="e.g., Emergency technician callout to replace blown primary fuse block.")
            root_cause = st.text_input("Determined Root Cause Analysis / Notes", placeholder="e.g., Electrical spike from main line transformer grid drop.")
            
            st.write("##### ⏱️ Duration / Resolution Tracking")
            c_has_end = st.checkbox("Toggle this to specify when the event ended / resolved", value=False)
            
            end_col1, end_col2 = st.columns(2)
            if c_has_end:
                end_date = end_col1.date_input("Resolution Date", value=event_date)
                end_time = end_col2.time_input("Resolution Time (UTC)", value=event_time)
            
            c_bool1 = st.checkbox("Timestamp registration is approximate (Estimated time block record flag)", value=False)
            
            if st.form_submit_button("💾 Save Event Entry to Database", use_container_width=True):
                if not event_desc.strip():
                    st.error("❌ Submission Rejected: Event Description tracking summary text notes are required.")
                else:
                    generated_uuid = str(uuid.uuid4())
                    combined_start = datetime.combine(event_date, event_time).strftime('%Y-%m-%d %H:%M:%S')
                    
                    if c_has_end:
                        combined_end = f"TIMESTAMP('{datetime.combine(end_date, end_time).strftime('%Y-%m-%d %H:%M:%S')}')"
                    else:
                        combined_end = "NULL"
                    
                    system_prefix = f"[System: {proj_system.strip()}] " if proj_system.strip() else ""
                    safe_desc = f"{system_prefix}[{event_type} | Power: {power_type}] " + event_desc.strip().replace("'", "''")
                    safe_cause = root_cause.strip().replace("'", "''")
                    chiller_val_str = f"'{assoc_chiller}'" if assoc_chiller != "None" else "NULL"
                    
                    insert_sql = f"""
                        INSERT INTO `{EVENTS_TABLE}` (event_id, project_id, chiller_id, event_timestamp, resolution_timestamp, event_description, root_cause, is_time_approximate, event_cost)
                        VALUES ('{generated_uuid}', '{target_proj}', {chiller_val_str}, TIMESTAMP('{combined_start}'), {combined_end}, '{safe_desc}', '{safe_cause}', {str(c_bool1).upper()}, {float(event_cost_val)})
                    """
                    try:
                        with st.spinner("Streaming event logging data to BigQuery table..."):
                            client.query(insert_sql).result()
                        st.success(f"🎉 Success! Event tracking record committed cleanly under code: `{generated_uuid[:8]}`")
                        st.cache_data.clear()
                        time.sleep(0.5)
                        st.rerun()
                    except Exception as err:
                        st.error(f"Database insertion failed: {err}")
                        st.code(insert_sql, language="sql")

        st.divider()
        
        # --- EVENT REGISTRY MANAGEMENT LEDGER WITH ASSET & PROJECT FILTERS ---
        st.write("#### 📂 Event Registry")
        st.caption("💡 **Tip:** To clear out double postings or bad copies, select the row(s) and click the **Remove Selected Entries** button below.")
        
        f_col1, f_col2 = st.columns(2)
        filter_proj = f_col1.selectbox("Filter Logs by Project Space Context:", ["All"] + active_projects_list, key="evt_log_filter_project")
        filter_chiller = f_col2.selectbox("Filter Logs by Associated Chiller Asset:", ["All"] + active_chillers_list, key="evt_log_filter_chiller")
        
        try:
            where_clauses = []
            if filter_proj != "All":
                where_clauses.append(f"e.project_id = '{filter_proj}'")
            if filter_chiller != "All":
                where_clauses.append(f"e.chiller_id = '{filter_chiller}'")
                
            where_stmt = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
            
            logs_q = f"""
                SELECT e.event_id,
                       FORMAT_TIMESTAMP('%m/%d/%Y %H:%M', e.event_timestamp) as Start_Time,
                       COALESCE(FORMAT_TIMESTAMP('%m/%d/%Y %H:%M', e.resolution_timestamp), 'Ongoing / N/A') as End_Time,
                       e.project_id as Project,
                       COALESCE(c.chiller_id, '—') as Chiller_Name,
                       e.event_description as Chiller_Event,
                       COALESCE(e.event_cost, 0.0) as event_cost
                FROM `{EVENTS_TABLE}` e
                LEFT JOIN `{CHILLER_REG_TABLE}` c ON e.chiller_id = c.chiller_id
                {where_stmt}
                ORDER BY e.event_timestamp DESC
                LIMIT 200
            """
            logs_df = client.query(logs_q).to_dataframe()
            
            if not logs_df.empty:
                logs_df.insert(0, "Select to Remove", False)
                ev_key = "live_event_registry_interactive_editor"
                
                edited_logs_df = st.data_editor(
                    logs_df, 
                    use_container_width=True, 
                    hide_index=True,
                    disabled=["event_id", "Start_Time", "End_Time", "Project", "Chiller_Name", "Chiller_Event", "event_cost"],
                    column_config={
                        "Select to Remove": st.column_config.CheckboxColumn("Remove?"),
                        "event_id": None, 
                        "Start_Time": st.column_config.TextColumn("Start Time"),
                        "End_Time": st.column_config.TextColumn("End / Resolution Time"),
                        "Project": st.column_config.TextColumn("Project"),
                        "Chiller_Name": st.column_config.TextColumn("Chiller Name"),
                        "Chiller_Event": st.column_config.TextColumn("Chiller Event"),
                        "event_cost": st.column_config.NumberColumn("Cost ($)", format="$%.2f")
                    },
                    key=ev_key
                )
                
                targeted_deletions = edited_logs_df[edited_logs_df["Select to Remove"] == True]
                
                if not targeted_deletions.empty:
                    st.warning(f"⚠️ Warning: You have targeted {len(targeted_deletions)} event log record(s) for extraction.")
                    if st.button("🗑️ Remove Selected Entries From Registry", use_container_width=True, type="primary"):
                        with st.spinner("Purging records from BigQuery array storage..."):
                            for _, target_row in targeted_deletions.iterrows():
                                target_uuid = target_row["event_id"]
                                purge_sql = f"DELETE FROM `{EVENTS_TABLE}` WHERE event_id = '{target_uuid}'"
                                client.query(purge_sql).result()
                                
                        st.success("🎉 Registry cleaned up! Duplicate items removed successfully.")
                        st.cache_data.clear()
                        time.sleep(0.5)
                        st.rerun()
            else:
                st.info("No historical event entries log files match your selected parameter options.")
        except Exception as e:
            st.error(f"⚠️ Event Registry Fault: {e}")

    # --- TAB 5: REGISTER CHILLER CONTROLS ---
    with tab_chiller_reg:
        st.subheader("❄️ Chiller Infrastructure Master Control")
        try:
            inventory_q = f"""
                WITH TimelineState AS (
                    SELECT 
                        project_id, chiller_id, event_timestamp, event_description, event_cost,
                        REGEXP_CONTAINS(UPPER(event_description), 'CHILLER TURN ON') as is_on,
                        REGEXP_CONTAINS(UPPER(event_description), 'CHILLER TURN OFF') as is_off,
                        LEAD(event_timestamp) OVER(PARTITION BY chiller_id ORDER BY event_timestamp ASC) as next_evt
                    FROM `{EVENTS_TABLE}`
                    WHERE chiller_id IS NOT NULL
                ),
                Durations AS (
                    SELECT 
                        chiller_id,
                        MAX_BY(project_id, event_timestamp) as current_location,
                        ARRAY_AGG(is_on ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] as currently_chilling,
                        SUM(CASE WHEN is_on THEN TIMESTAMP_DIFF(COALESCE(next_evt, CURRENT_TIMESTAMP()), event_timestamp, HOUR) ELSE 0 END) as total_chill_hours,
                        MAX(CASE WHEN is_on AND next_evt IS NULL THEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), event_timestamp, HOUR) END) as active_run_hours,
                        SUM(COALESCE(event_cost, 0.0)) as total_logged_costs
                    FROM TimelineState
                    GROUP BY chiller_id
                )
                SELECT 
                    c.chiller_id, c.chiller_type, c.purchase_date, c.initial_price, c.acquired_status, COALESCE(c.status, 'Yard') as status,
                    COALESCE(d.current_location, 'Unassigned (Shop)') as current_location,
                    COALESCE(d.currently_chilling, FALSE) as is_chilling,
                    COALESCE(d.total_chill_hours, 0) as cumulative_hours,
                    COALESCE(d.active_run_hours, 0) as current_run_hours,
                    COALESCE(d.total_logged_costs, 0.0) as total_operating_costs
                FROM `{CHILLER_REG_TABLE}` c
                LEFT JOIN Durations d ON c.chiller_id = d.chiller_id
                ORDER BY c.chiller_id ASC
            """
            inv_raw_df = client.query(inventory_q).to_dataframe()
            
            if not inv_raw_df.empty:
                editable_rows = []
                for _, r in inv_raw_df.iterrows():
                    status_text = "🔵 Active Chilling" if r['is_chilling'] else "⚪ Standby / Off"
                    duration_text = f"{int(r['current_run_hours'])}h ongoing" if r['is_chilling'] else f"{int(r['cumulative_hours'])}h total runtime"
                    
                    editable_rows.append({
                        "Chiller Name": r['chiller_id'],
                        "Asset Status": str(r['status']).upper(),
                        "Current Location": r['current_location'],
                        "Operational Status": status_text,
                        "Chill Duration": duration_text,
                        "Equipment Type": r['chiller_type'],
                        "Date Acquired": pd.to_datetime(r['purchase_date']).date() if pd.notnull(r['purchase_date']) else None,
                        "Condition When Acquired": str(r['acquired_status']).upper() if pd.notnull(r['acquired_status']) else "NEW",
                        "Initial Cost": float(r['initial_price']) if pd.notnull(r['initial_price']) else 0.0,
                        "Accumulated Operating Costs": f"${r['total_operating_costs']:,.2f}"
                    })
                
                base_edit_df = pd.DataFrame(editable_rows)
                ed_key = "chiller_live_inventory_editor"
                
                st.data_editor(
                    base_edit_df,
                    use_container_width=True,
                    hide_index=True,
                    disabled=["Chiller Name", "Asset Status", "Current Location", "Operational Status", "Chill Duration", "Accumulated Operating Costs"],
                    column_config={
                        "Initial Cost": st.column_config.NumberColumn("Initial Cost", format="$%.2f", min_value=0.0),
                        "Condition When Acquired": st.column_config.SelectboxColumn("Condition When Acquired", options=["NEW", "USED"]),
                        "Date Acquired": st.column_config.DateColumn("Date Acquired", format="MM/DD/YYYY")
                    },
                    key=ed_key
                )
                
                if ed_key in st.session_state and "edited_rows" in st.session_state[ed_key] and st.session_state[ed_key]["edited_rows"]:
                    if st.button("💾 Save Inventory Modifications", use_container_width=True, key="save_inventory_mods_btn", type="secondary"):
                        with st.spinner("Overwriting registry values..."):
                            for row_idx_str, col_deltas in st.session_state[ed_key]["edited_rows"].items():
                                row_idx = int(row_idx_str)
                                target_cid = base_edit_df.loc[row_idx, "Chiller Name"]
                                
                                set_clauses = []
                                if "Equipment Type" in col_deltas:
                                    set_clauses.append(f"chiller_type = '{col_deltas['Equipment Type'].replace("'", "''")}'")
                                if "Initial Cost" in col_deltas:
                                    set_clauses.append(f"initial_price = {float(col_deltas['Initial Cost'])}")
                                if "Condition When Acquired" in col_deltas:
                                    set_clauses.append(f"acquired_status = '{col_deltas['Condition When Acquired'].lower()}'")
                                if "Date Acquired" in col_deltas:
                                    set_clauses.append(f"purchase_date = DATE('{col_deltas['Date Acquired']}')")
                                    
                                if set_clauses:
                                    update_sql = f"UPDATE `{CHILLER_REG_TABLE}` SET {', '.join(set_clauses)} WHERE chiller_id = '{target_cid}'"
                                    client.query(update_sql).result()
                                    
                        st.success("✅ Fleet inventory data logs updated successfully!")
                        st.cache_data.clear()
                        time.sleep(0.5)
                        st.rerun()
            else:
                st.info("ℹ️ Chiller registry metadata catalog stores are currently unpopulated.")
        except Exception as e:
            st.error(f"⚠️ Inventory Ledger Render Fault: {e}")

        st.divider()

        # Section B: Registration Entry Form
        st.write("#### ➕ Update Chiller Status & Asset Records")
        is_brand_new_asset = st.checkbox("➕ Check this box to register a completely NEW chiller asset to the fleet", value=False)
        fleet_options = sorted(inv_raw_df['chiller_id'].tolist()) if not inv_raw_df.empty else []
        
        with st.form("hardened_unified_chiller_asset_management_form"):
            if is_brand_new_asset or not fleet_options:
                st.info("Form mode: Registering a brand new hardware asset block to BigQuery.")
                target_c_name = st.text_input("Chiller Name / Unique Serial ID*", placeholder="e.g., CH-53-03")
                current_type_val = ""
                current_date_val = datetime.now().date()
                current_cost_val = 0.0
                current_cond_idx = 0
                current_stat_idx = 1
            else:
                target_c_name = st.selectbox("Select Target Chiller to Edit/Update*", options=fleet_options)
                matched_record = inv_raw_df[inv_raw_df['chiller_id'] == target_c_name].iloc[0]
                
                current_type_val = matched_record['chiller_type'] if pd.notnull(matched_record['chiller_type']) else ""
                current_date_val = pd.to_datetime(matched_record['purchase_date']).date() if pd.notnull(matched_record['purchase_date']) else datetime.now().date()
                current_cost_val = float(matched_record['initial_price']) if pd.notnull(matched_record['initial_price']) else 0.0
                
                cond_str = str(matched_record['acquired_status']).upper()
                current_cond_idx = 1 if cond_str == "USED" else 0
                
                stat_str = str(matched_record['status']).strip().title()
                stat_options_list = ["On Project", "Yard", "Need Repair", "Scrap"]
                current_stat_idx = stat_options_list.index(stat_str) if stat_str in stat_options_list else 1

            form_col1, form_col2, form_col3 = st.columns(3)
            f_type = form_col1.text_input("Chiller Mechanical Type / Spec", value=current_type_val, placeholder="e.g., 53-Ton Logue")
            f_acquired = form_col2.date_input("Date Acquired", value=current_date_val, min_value=datetime.now().date() - timedelta(days=365*30))
            f_status = form_col3.selectbox("Asset Management Status*", ["On Project", "Yard", "Need Repair", "Scrap"], index=current_stat_idx)
            
            form_col4, form_col5 = st.columns(2)
            f_price = form_col4.number_input("Initial Purchase Cost ($)", min_value=0.0, value=current_cost_val, step=1000.0, format="%.2f")
            f_condition = form_col5.selectbox("Hardware Condition Status When Acquired", ["New", "Used"], index=current_cond_idx)
            
            submit_action_label = "🚀 Register Brand New Chiller Asset" if is_brand_new_asset else "💾 Save Chiller Record Updates"
            
            if st.form_submit_button(submit_action_label, use_container_width=True, type="primary"):
                if not target_c_name.strip():
                    st.error("❌ Action Rejected: Chiller identifier name string cannot be blank.")
                else:
                    safe_cid = target_c_name.strip().replace("'", "''")
                    safe_type = f_type.strip().replace("'", "''")
                    
                    if is_brand_new_asset:
                        execution_sql = f"""
                            INSERT INTO `{CHILLER_REG_TABLE}` (chiller_id, chiller_type, purchase_date, initial_price, acquired_status, status)
                            VALUES ('{safe_cid}', '{safe_type}', DATE('{f_acquired.strftime('%Y-%m-%d')}'), {float(f_price)}, '{f_condition.lower()}', '{f_status}')
                        """
                        success_alert = f"🎉 Success! Asset block **{safe_cid}** has been written to the production registry."
                    else:
                        execution_sql = f"""
                            UPDATE `{CHILLER_REG_TABLE}`
                            SET chiller_type = '{safe_type}',
                                purchase_date = DATE('{f_acquired.strftime('%Y-%m-%d')}'),
                                initial_price = {float(f_price)},
                                acquired_status = '{f_condition.lower()}',
                                status = '{f_status}'
                            WHERE chiller_id = '{safe_cid}'
                        """
                        success_alert = f"✅ Success! Asset parameter values for **{safe_cid}** updated cleanly."
                        
                    try:
                        with st.spinner("Streaming data transactions to BigQuery storage arrays..."):
                            client.query(execution_sql).result()
                        st.success(success_alert)
                        st.cache_data.clear()
                        time.sleep(0.5)
                        st.rerun()
                    except Exception as bq_fault:
                        st.error(f"❌ BigQuery Database Rejected Entry: {bq_fault}")
                        st.code(execution_sql, language="sql")

######################
# Page: Admin Tool Helpers   #
######################
# =============================================================================
# SUB-TAB WORKSPACE HELPERS: ADVANCED MAINTENANCE & BULK APPROVAL WORKSPACE
# =============================================================================

def render_bulk_approval_controls():
    """Renders the top-level scope selection, filter parameters, and target flag status inputs."""
    c1, c2, c3 = st.columns(3)
    with c1:
        target_scope = st.radio(
            "Target Scope", 
            ["Project Wide", "Specific Location", "Specific Node"], 
            horizontal=True, 
            key="blk_mgmt_target_scope"
        )
    with c2:
        current_status_filter = st.selectbox(
            "Filter Current Designation Status:",
            options=["all", "all but null", "true", "null (streaming / unreviewed)", "masked", "office", "baddata"],
            key="blk_mgmt_current_status_filter",
            help="Limits modifications only to data points that currently match this selected classification."
        )
    with c3:
        new_status = st.selectbox(
            "Set Approval Status To:", 
            ["true", "masked", "office", "baddata"], 
            key="blk_mgmt_new_status"
        )
    return target_scope, current_status_filter, new_status


def build_bulk_approval_where_clause(reg_df, selected_project, target_scope, current_status_filter, f):
    """Constructs analytical logical statements parsing historical coordinates."""
    where_clauses = []

    if selected_project != "All Projects":
        if target_scope == "Specific Node":
            where_clauses.append(f"NodeNum = '{f['scope_val']}'")
        elif target_scope == "Specific Location":
            loc_nodes = reg_df[(reg_df['Project'] == selected_project) & (reg_df['Location'] == f['scope_val'])]['NodeNum'].dropna().unique().tolist()
            nodes_str = ", ".join([f"'{n}'" for n in loc_nodes])
            where_clauses.append(f"NodeNum IN ({nodes_str})")
        else:
            proj_nodes = reg_df[reg_df['Project'] == selected_project]['NodeNum'].dropna().unique().tolist()
            if proj_nodes:
                nodes_str = ", ".join([f"'{n}'" for n in proj_nodes])
                where_clauses.append(f"NodeNum IN ({nodes_str})")
            else:
                where_clauses.append("NodeNum = 'NONE'")
        where_clauses.append(f"Project = '{selected_project}'")
    else:
        where_clauses.append("Project IS NOT NULL")

    start_ts_str = f"{f['s_date'].strftime('%Y-%m-%d')} {f['s_time'].strftime('%H:%M:%S')}"

    if f["temporal_dir"] == "Between Range":
        end_ts_str = f"{f['e_date'].strftime('%Y-%m-%d')} {f['e_time'].strftime('%H:%M:%S')}"
        where_clauses.append(f"timestamp BETWEEN '{start_ts_str}' AND '{end_ts_str}'")
    elif f["temporal_dir"] in ["Older Than", "Newer Than"]:
        op = "<" if f["temporal_dir"] == "Older Than" else ">"
        where_clauses.append(f"timestamp {op} '{start_ts_str}'")
    
    if f["val_filter"] == "Above Threshold":
        where_clauses.append(f"temperature > {f['threshold']}")
    elif f["val_filter"] == "Below Threshold":
        where_clauses.append(f"temperature < {f['threshold']}")

    if current_status_filter != "all":
        if current_status_filter == "all but null":
            where_clauses.append("r.approve IS NOT NULL")
        elif current_status_filter == "null (streaming / unreviewed)":
            where_clauses.append("r.approve IS NULL")
        elif current_status_filter == "true":
            where_clauses.append("r.approve IS NULL")
        else:
            where_clauses.append(f"LOWER(CAST(r.approve AS STRING)) = '{str(current_status_filter).lower()}'")

    return " AND ".join(where_clauses)


def render_bulk_approval_filters(reg_df, selected_project, target_scope):
    """Renders temporal filter vectors alongside numeric sensor value threshold blocks."""
    col_f1, col_f2, col_f3 = st.columns(3)
    
    with col_f1:
        temporal_dir = st.selectbox("Temporal Direction", ["Between Range", "Older Than", "Newer Than"], key="blk_mgmt_temp_dir")
        
        if temporal_dir == "Between Range":
            c_start, c_end = st.columns(2)
            with c_start:
                s_date = st.date_input("Start Date", value=datetime.now().date() - timedelta(days=7), key="blk_mgmt_s_date")
                s_time = st.time_input("Start Time (Exact)", value=datetime.min.time(), key="blk_mgmt_s_time")
            with c_end:
                e_date = st.date_input("End Date", value=datetime.now().date(), key="blk_mgmt_e_date")
                e_time = st.time_input("End Time (Exact)", value=datetime.max.time(), key="blk_mgmt_e_time")
        else:
            s_date = st.date_input("Target Date", value=datetime.now().date() - timedelta(days=7), key="blk_mgmt_single_date")
            s_time = st.time_input("Target Time (Exact)", value=datetime.min.time(), key="blk_mgmt_single_time")
            e_date, e_time = None, None

    with col_f2:
        val_filter = st.selectbox("Value Filter", ["No Threshold", "Above Threshold", "Below Threshold"], key="blk_mgmt_val_filter")
        threshold = st.number_input("Threshold Value (°F)", value=100.0, key="blk_mgmt_threshold")

    with col_f3:
        scope_val = None
        if selected_project == "All Projects":
            st.info("Targeting **Global Registry Scope** (All Active Projects)")
            scope_val = "ALL_PROJECTS"
        else:
            if target_scope == "Project Wide":
                st.info(f"Targeting all nodes in **{selected_project}**")
                scope_val = selected_project
            elif target_scope == "Specific Location":
                u_locs = sorted(reg_df[reg_df['Project'] == selected_project]['Location'].dropna().unique().tolist())
                scope_val = st.selectbox("Select Location", u_locs, key="blk_mgmt_loc_select")
            elif target_scope == "Specific Node":
                u_locs = sorted(reg_df[reg_df['Project'] == selected_project]['Location'].dropna().unique().tolist())
                selected_loc = st.selectbox("First, Select Location", u_locs, key="blk_mgmt_loc_node_select")
                u_nodes = sorted(
                    reg_df[(reg_df['Project'] == selected_project) & (reg_df['Location'] == selected_loc)]['NodeNum'].dropna().unique().tolist()
                )
                scope_val = st.selectbox("Then, Select Node", u_nodes, key="blk_mgmt_node_select")
            
    return {
        "temporal_dir": temporal_dir, 
        "s_date": s_date, "s_time": s_time,
        "e_date": e_date, "e_time": e_time,
        "val_filter": val_filter, "threshold": threshold, "scope_val": scope_val
    }


def execute_bulk_approval_workspace(client, full_reg_df, selected_project, tab_logistics):
    """
    Main administrative execution module managing bulk data approval modification routines,
    hourly table consolidation aggregates, and manual rejection string standardization.
    """
    # Establish explicit table paths mapped directly out of your data view catalog
    target_table = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections" 
    telemetry_table = f"{PROJECT_ID}.{DATASET_ID}.master_data_view" 

    st.title("⚡ Bulk Approval and Database Maintenance")
    st.divider()

    # Initialize application state memory footprints to prevent unintended app re-runs during data scans
    if "blk_mgmt_profile_df" not in st.session_state: 
        st.session_state.blk_mgmt_profile_df = None
    if "blk_mgmt_total_points" not in st.session_state: 
        st.session_state.blk_mgmt_total_points = 0

    # =========================================================================
    # UTILITY A: GLOBAL DATABASE CLEANUP ENGINE
    # =========================================================================
    st.header("🧹 Global Database Cleanup")
    st.write(
        "Consolidate raw datasets into **1-decimal hourly averages** and safely remove all high-frequency "
        "and duplicate records system-wide. "
        "**Note:** Running this cleanup automatically drops any rogue data points outside the physical bounds of -30°F and 120°F."
    )
    
    # Split utilities into clean side-by-side management columns
    clean_col1, clean_col2 = st.columns(2)
    
    with clean_col1:
        st.write("##### 📊 Telemetry Aggregation & Hourly Flattening")
        st.caption("Truncates raw timestamps to the hour, filters bad logs, and collapses records to an average value.")
        run_telemetry_cleanup = st.button("⚡ Run Global Database Cleanup & Hourly Consolidation", use_container_width=True)
        
    with clean_col2:
        st.write("##### 🧼 Approval String Casing Standardization")
        st.caption("Scans the rejections table to convert any lowercase 'true/false' strings to standard 'TRUE/FALSE'.")
        run_string_cleanup = st.button("🧹 Clean Approval Text 'true' to 'TRUE'", use_container_width=True)

    # --- PATHWAY A: COMPREHENSIVE HOURLY HOOD CONSOLIDATION ENGINE ---
    if run_telemetry_cleanup:
        status_box = st.empty()
        try:
            # 1. Audit active data rows before applying modifications to map the exact purge count
            status_box.markdown("⏳ **[1/4] Calculating initial database row baselines...**")
            count_sp_before = client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`").to_dataframe().iloc[0, 0]
            count_lord_before = client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`").to_dataframe().iloc[0, 0]
            
            # 2. Upgraded SensorPush: Groups by Node & Truncated Hour, filtering outliers and calculating clean averages
            status_box.markdown("🧹 **[2/4] Consolidating and averaging SensorPush timelines to the hour...**")
            sp_cleanup_sql = f"""
                CREATE OR REPLACE TEMP TABLE tmp_clean_sensorpush AS
                SELECT 
                    TIMESTAMP_TRUNC(timestamp, HOUR) as timestamp, 
                    NodeNum, 
                    ROUND(AVG(CAST(temperature AS NUMERIC)), 1) as temperature,
                    MAX(rssi) as rssi
                FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
                WHERE temperature >= -30.0 AND temperature <= 120.0
                GROUP BY TIMESTAMP_TRUNC(timestamp, HOUR), NodeNum;

                CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` AS
                SELECT timestamp, NodeNum, CAST(temperature AS FLOAT64) as temperature, rssi FROM tmp_clean_sensorpush;
            """
            client.query(sp_cleanup_sql).result()
            
            # 3. Upgraded Lord: Groups by Node & Truncated Hour, filtering outliers and calculating clean averages
            status_box.markdown("🛰️ **[3/4] Consolidating and averaging Lord Wireless timelines to the hour...**")
            lord_cleanup_sql = f"""
                CREATE OR REPLACE TEMP TABLE tmp_clean_lord AS
                SELECT 
                    TIMESTAMP_TRUNC(timestamp, HOUR) as timestamp, 
                    NodeNum, 
                    ROUND(AVG(CAST(temperature AS NUMERIC)), 1) as temperature
                FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
                WHERE CAST(temperature AS NUMERIC) >= -30.0 AND CAST(temperature AS NUMERIC) <= 120.0
                GROUP BY TIMESTAMP_TRUNC(timestamp, HOUR), NodeNum;

                CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.raw_lord` AS
                SELECT timestamp, NodeNum, CAST(temperature AS FLOAT64) as temperature FROM tmp_clean_lord;
            """
            client.query(lord_cleanup_sql).result()
            st.cache_data.clear()

            # 4. Pull database row summaries to document the data cleanup audit trail
            status_box.markdown("📊 **[4/4] Finalizing database overwrites and pulling consolidated tallies...**")
            count_sp_after = client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`").to_dataframe().iloc[0, 0]
            count_lord_after = client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`").to_dataframe().iloc[0, 0]

            sp_removed = count_sp_before - count_sp_after
            lord_removed = count_lord_before - count_lord_after
            total_removed = sp_removed + lord_removed
            
            status_box.empty()
            st.success("🎉 Global Database Consolidation successfully completed!")
            
            # Print comparative ledger results matrix
            report_data = [
                {"Data Table": "SensorPush (raw_sensorpush)", "Before Count": f"{count_sp_before:,}", "After Count": f"{count_sp_after:,}", "Purged High-Freq Points": f"{sp_removed:,}"},
                {"Data Table": "Lord Wireless (raw_lord)", "Before Count": f"{count_lord_before:,}", "After Count": f"{count_lord_after:,}", "Purged High-Freq Points": f"{lord_removed:,}"},
                {"Data Table": "Combined Total Pool", "Before Count": f"{count_sp_before + count_lord_before:,}", "After Count": f"{count_sp_after + count_lord_after:,}", "Purged High-Freq Points": f"{total_removed:,}"}
            ]
            st.dataframe(pd.DataFrame(report_data), use_container_width=True, hide_index=True)
            
        except Exception as e:
            status_box.empty()
            st.error(f"Global Database Consolidation Failed: {e}")

    # --- PATHWAY B: REJECTIONS ENGINE STRING CASING CLEANUP ---
    if run_string_cleanup:
        status_box_str = st.empty()
        try:
            status_box_str.markdown("🧼 **Standardizing mixed-case manual override parameters...**")
            
            # Scans rejections table to convert false/lower case values to standardized uppercase or masked states
            str_cleanup_sql = f"""
                UPDATE `{target_table}`
                SET approve = CASE 
                    WHEN LOWER(TRIM(approve)) = 'false' THEN 'MASKED' 
                    ELSE UPPER(TRIM(approve)) 
                END
                WHERE LOWER(TRIM(approve)) IN ('true', 'false')
            """
            job = client.query(str_cleanup_sql)
            job.result()
            
            status_box_str.empty()
            st.success(f"🎉 Text standardization complete! Successfully cleaned {job.num_dml_affected_rows:,} records inside the rejections ledger.")
            st.cache_data.clear()
            time.sleep(0.5)
            st.rerun()
        except Exception as e:
            status_box_str.empty()
            st.error(f"Text String Cleanup Operation Failed: {e}")

    st.divider()

    # =========================================================================
    # UTILITY B: BULK APPROVAL AND DATA STATUS CHANGE SYSTEM CONTROLS
    # =========================================================================
    st.header("⚡ Bulk Approval and Data Status Change")
    st.info("💡 **Important:** Please ensure you have selected your targeted project framework or 'All Projects' in the sidebar menu before applying any status overrides.")
    
    # Render user selection widgets to grab Target Scope (Project/All), Filtering Criteria, and New Status Value
    target_scope, current_status_filter, new_status = render_bulk_approval_controls()
    st.divider()

    # Build active project logic constraints by pulling down matching query string blocks
    filters = render_bulk_approval_filters(full_reg_df, selected_project, target_scope)
    where_str = build_bulk_approval_where_clause(full_reg_df, selected_project, target_scope, current_status_filter, filters)
    
    # Map raw field strings to match the proper table aliases used inside the Master analytical query view
    aliased_where = (where_str.replace("NodeNum", "t.NodeNum")
                              .replace("timestamp", "t.timestamp")
                              .replace("temperature", "t.temperature")
                              .replace("r.approve", "t.approval_status"))
    
    # Internal function to map and verify exactly how many data rows will be changed before saving
    def run_profile_audit():
        status_q = f"""
            SELECT  
                COALESCE(t.approval_status, 'NULL (Streaming / Unreviewed)') as Current_Designation_Status,
                COUNT(*) as Total_Captured_Points,
                FORMAT_TIMESTAMP('%m/%d/%Y', MIN(t.timestamp)) as Oldest_Log_Entry,
                FORMAT_TIMESTAMP('%m/%d/%Y', MAX(t.timestamp)) as Newest_Log_Entry
            FROM `{telemetry_table}` t
            WHERE {aliased_where}
            GROUP BY Current_Designation_Status
            ORDER BY Total_Captured_Points DESC
        """
        with st.spinner("Auditing active database designation profiles..."):
            res = client.query(status_q).to_dataframe()
            if not res.empty:
                st.session_state.blk_mgmt_profile_df = res
                st.session_state.blk_mgmt_total_points = res['Total_Captured_Points'].sum()
            else:
                st.session_state.blk_mgmt_profile_df = pd.DataFrame()
                st.session_state.blk_mgmt_total_points = 0

    # Step 1 Button: Verification Routine
    if st.button("🔍 Step 1: Verify Match Count & Current Status Profiles", key="blk_mgmt_verify_btn", use_container_width=True):
        try:
            run_profile_audit()
        except Exception as e:
            st.error(f"Verification Matrix Compilation Failed: {e}")

    # Render results grid if data profile calculations are actively held in app cache states
    if st.session_state.blk_mgmt_profile_df is not None:
        if not st.session_state.blk_mgmt_profile_df.empty:
            st.subheader("📊 Current Node Status")
            st.dataframe(st.session_state.blk_mgmt_profile_df, use_container_width=True, hide_index=True)
            st.metric("Total Consolidated Points in Selection Scope", f"{st.session_state.blk_mgmt_total_points:,}")
        else:
            st.warning("No telemetry data points found matching this configuration window.")

    st.divider()
    st.info(f"Target Designation Status for selected coordinates: **{new_status.upper()}**")
    
    # Step 2: Form Checkbox and Execution Engine Block
    if st.checkbox("I authorize updating these data markers to the target parameters specified.", key="confirm_blk_mgmt"):
        if st.button(f"🚀 Step 2: Execute Status Override to {new_status.upper()}", key="exec_blk_mgmt_btn", use_container_width=True):
            
            # PATH A: If target override is TRUE, drop tracking tokens entirely out of the rejections table so they re-approve
            if new_status.upper() == "TRUE":
                sql = f"""
                    DELETE FROM `{target_table}`
                    WHERE STRUCT(NodeNum, timestamp) IN (
                        SELECT AS STRUCT t.NodeNum, t.timestamp 
                        FROM `{telemetry_table}` t
                        WHERE {aliased_where}
                    )
                """
            # PATH B: If target override is a custom flag (BADDATA, MASKED, OFFICE), merge row coordinates into manual_rejections
            else:
                sql = f"""
                    MERGE `{target_table}` T
                    USING (
                        SELECT DISTINCT t.NodeNum, t.timestamp 
                        FROM `{telemetry_table}` t 
                        WHERE {aliased_where}
                    ) S
                    ON T.NodeNum = S.NodeNum AND T.timestamp = S.timestamp
                    WHEN MATCHED THEN
                        UPDATE SET approve = '{new_status.upper()}'
                    WHEN NOT MATCHED THEN
                        INSERT (NodeNum, timestamp, approve) 
                        VALUES (S.NodeNum, S.timestamp, '{new_status.upper()}')
                """
            try:
                with st.spinner("Processing database status reclassifications..."):
                    job = client.query(sql)
                    job.result()
                
                st.success(f"✅ Reclassification successful! Updated {job.num_dml_affected_rows:,} records inside the registry ledger.")
                st.cache_data.clear()
                run_profile_audit() # Refresh data metrics locally
                st.balloons()
                time.sleep(1.0)
                st.rerun()
            except Exception as e:
                st.error(f"Execution Error: {e}")
                st.code(sql, language="sql")

def save_status_to_bigquery(project_id, node_num, timestamp, new_status):
    """Executes a proper database commit to write approvals, rejections, or BADDATA flags."""
    client = get_bq_client()
    if client is None: return False
        
    if isinstance(timestamp, pd.Timestamp):
        ts_str = timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')
    else:
        ts_str = str(timestamp)

    write_q = f"""
        MERGE `{PROJECT_ID}.{DATASET_ID}.manual_rejections` T
        USING (SELECT '{node_num}' as NodeNum, TIMESTAMP('{ts_str}') as timestamp) S
        ON T.NodeNum = S.NodeNum AND T.timestamp = S.timestamp
        WHEN MATCHED THEN
          UPDATE SET approve = '{new_status.upper()}'
        WHEN NOT MATCHED THEN
          INSERT (NodeNum, timestamp, approve) 
          VALUES (S.NodeNum, S.timestamp, '{new_status.upper()}')
    """
    try:
        client.query(write_q).result()
        return True
    except Exception as e:
        st.error(f"⚠️ Cloud DB Commit Failed: {e}")
        return False

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
            else: st.success("✅ **Perfect Grid Alignment:** Every position coordinate safely holds exactly one distinct active hardware asset mapping line.")
        except Exception as e: st.caption(f"Spatial proximity engine initializing: {e}")

# =============================================================================
# Page: Admin Tools 
# =============================================================================

def render_admin_page(selected_project, display_tz, unit_mode, unit_label, active_refs):
    """Central analytical administrative supervisor console streaming clean Google Sheets source records."""
    st.header("🛠️ Admin Tools")
    client = get_bq_client()
    if client is None: st.error("Database connection unavailable."); return

    # Core Read-Only Matrix Data Pull
    try:
        proj_q = f"SELECT CAST(Project AS STRING) as Project, ProjectName, Timezone, ProjectStatus, Date_Freezedown FROM `{PROJECT_REGISTRY_TABLE}` WHERE UPPER(TRIM(CAST(ShowActive AS STRING))) = 'YES'"
        full_reg_df = client.query(f"SELECT * FROM `{NODE_REGISTRY_TABLE}` WHERE End_Date IS NULL").to_dataframe()
        available_projects_list = sorted(client.query(proj_q).to_dataframe()['Project'].dropna().unique().tolist())
    except Exception as e: st.error(f"Registry Link Offline: {e}"); return

    # Standardized Navigation Tabs Layout Schema Paths
    tab_admin_sum, tab_bulk_app, tab_logistics, tab_recovery, tab_proj_master, tab_chillers = st.tabs([
        "📋 Admin Summary", "⚡ Bulk Approval", "📋 Node Master", "📡 Data Recovery", "⚙️ Project Master", "❄️ Chiller Operations"
    ])
    
    # --- SUB-TAB 1: ADMIN HARDWARE AND DIRECTORY SUMMARY ---
    with tab_admin_sum:
        st.subheader("📋 Centralized Infrastructure Status Overview")
        st.markdown("### 📡 Hardware Inventory Fleet Breakdown")
        try:
            def classify_family(node): return "Lord" if "-ch" in str(node).lower() else "SP" if str(node).lower().startswith("sp") else "TP" if str(node).lower().startswith("tp") else "Other"
            fleet_df = full_reg_df.copy()
            fleet_df['Hardware Family'] = fleet_df['NodeNum'].apply(classify_family)
            fleet_df['Parent ID'] = fleet_df['NodeNum'].apply(lambda x: re.split(r'(?i)-ch', str(x))[0] if "-ch" in str(x).lower() else x)
            fleet_df['is_active'] = True
            
            deduped = fleet_df.sort_values(by=['Parent ID']).drop_duplicates(subset=['Parent ID']).copy()
            pivot = deduped.groupby(['Hardware Family', 'SensorStatus']).size().unstack(fill_value=0).reindex(["TP", "SP", "Lord", "Other"], fill_value=0)
            for col in ["Available", "Dead", "Diagnostic", "On Project"]: 
                if col not in pivot.columns: pivot[col] = 0
            pivot = pivot[["Available", "Dead", "Diagnostic", "On Project"]]
            pivot['Total Units'] = pivot.sum(axis=1)
            st.dataframe(pivot.reset_index(), use_container_width=True, hide_index=True)
        except Exception as e: st.caption(f"Inventory matrix loading: {e}")

        st.divider(); st.markdown("### 🏗️ Active Deployment Overview Matrix")
        try:
            sum_q = f"SELECT p.Project, p.ProjectName, p.ProjectStatus, p.Date_Freezedown, COUNT(DISTINCT n.NodeNum) as Mapped_Sensors, COUNT(DISTINCT CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR) THEN n.NodeNum END) as Active_6h, COUNT(DISTINCT CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN n.NodeNum END) as Active_24h FROM `{PROJECT_REGISTRY_TABLE}` p LEFT JOIN `{NODE_REGISTRY_TABLE}` n ON p.Project = n.Project LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.master_data_view` m ON n.NodeNum = m.NodeNum WHERE n.End_Date IS NULL AND UPPER(TRIM(CAST(p.ShowActive AS STRING))) = 'YES' AND UPPER(p.Project) NOT LIKE '%OFFICE%' GROUP BY 1,2,3,4 ORDER BY p.Project ASC"
            rows = []
            for _, r in client.query(sum_q).to_dataframe().iterrows():
                elapsed = max(0, (pd.Timestamp.now(tz=display_tz).date() - pd.to_datetime(r['Date_Freezedown']).date()).days) if pd.notnull(r['Date_Freezedown']) else 0
                rows.append({"Project ID": r['Project'], "Project Name": r['ProjectName'] or r['Project'], "Mapped Sensors": int(r['Mapped_Sensors']), "Active (6h)": int(r['Active_6h']), "Active (24h)": int(r['Active_24h']), "Project Status Timeline": f"Day {elapsed} of {str(r['ProjectStatus']).title()}" if pd.notnull(r['Date_Freezedown']) else "Not Freezing"})
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        except Exception as e: st.error(f"Overview compilation fault: {e}")

    # --- SUB-TAB 2: BULK APPROVAL SYSTEM RUNROOM ---
    with tab_bulk_app:
        execute_bulk_approval_workspace(client, full_reg_df, selected_project, tab_logistics)
        
    # --- SUB-TAB 3: NODE MASTER ASSIGNMENTS VIEWER ---
    with tab_logistics:
        st.title("📋 Node Status Timeline assignments Viewer")
        c1, c2, c3 = st.columns(3)
        sel_p = c1.selectbox("Filter Project Context:", sorted(list(set(["Office"] + full_reg_df['Project'].dropna().unique().tolist()))), key="node_master_p")
        p_filtered = full_reg_df[full_reg_df['Project'] == sel_p]
        sel_l = c2.selectbox("Filter Location Context:", sorted(p_filtered['Location'].dropna().unique().tolist(), key=natural_sort_key) if not p_filtered.empty else ["Office"], key="node_master_l")
        l_filtered = p_filtered[p_filtered['Location'] == sel_l] if not p_filtered.empty else pd.DataFrame()
        sel_n = c3.selectbox("Select Target Sensor ID ID:", sorted(l_filtered['NodeNum'].dropna().unique().tolist(), key=natural_sort_key) if not l_filtered.empty else [], key="node_master_n")
        
        if sel_n:
            st.markdown(f"### 🕒 Timeline Assignment Logging: **{sel_n}**")
            st.dataframe(client.query(f"SELECT Project, Location, Bank, Depth, CAST(Start_Date AS STRING) as Deployment_Date, COALESCE(CAST(End_Date AS STRING), 'Active') as Cutoff_Date, SensorStatus FROM `{NODE_REGISTRY_TABLE}` WHERE NodeNum = '{sel_n}' ORDER BY Start_Date DESC").to_dataframe(), use_container_width=True, hide_index=True)
            render_lab_data_checker(client, full_reg_df)

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

    # --- SUB-TAB 5: PROJECT LIFECYCLE HISTORY DIRECTORY ---
    with tab_proj_master:
        st.subheader("🗄️ Complete Master Project Lifecycle Directory")
        st.dataframe(client.query(f"SELECT Project as `Project ID`, ProjectName as `Friendly Name`, ProjectStatus as `Operational Phase`, Date_Freezedown as `Freezedown Date`, City, Timezone FROM `{PROJECT_REGISTRY_TABLE}` ORDER BY Project ASC").to_dataframe(), use_container_width=True, hide_index=True)

    # --- SUB-TAB 6: READ-ONLY INFRASTRUCTURE CHILLERS OVERVIEW ---
    with tab_chillers:
        st.subheader("❄️ Mechanical Chiller Fleet Deployment Manifest")
        try:
            st.dataframe(client.query(f"SELECT c.chiller_id as `Chiller Loop ID`, c.chiller_type as Specifications, c.status as Status, COALESCE(d.current_location, 'Yard / Shop Staging') as `Deployment Location Context` FROM `{PROJECT_ID}.{DATASET_ID}.chiller_registry` c LEFT JOIN (SELECT Chiller, MAX_BY(Location, StartDate) as current_location FROM `{PROJECT_ID}.{DATASET_ID}.project_systems_map` GROUP BY Chiller) d ON c.chiller_id = d.Chiller ORDER BY 1 ASC").to_dataframe(), use_container_width=True, hide_index=True)
        except Exception as e: st.error(f"Asset Manifest Offline: {e}")

# =============================================================================
# 12. CENTRALIZED APPLICATION ROUTING ROUTINES
# =============================================================================
display_tz = st.session_state.get("display_tz", "UTC")
unit_label = st.session_state.get("unit_label", "°F")
unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
active_refs = st.session_state.get("active_refs", [])

client = get_bq_client() 

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
            pwd = st.text_input("Enter Admin Password", type="password", key="admin_password_input_field")
            if st.button("Unlock Dashboard", use_container_width=True):
                if pwd == st.secrets.get("admin_password", "Freeze123!!"):
                    st.session_state['authenticated'] = True
                    st.rerun()
                else:
                    st.error("Invalid Password. Access Denied.")
   # =============================================================================
# DATA RECOVERY REQUISITE ENGINE HELPERS
# =============================================================================

def render_recovery_filters(sp_reg):
    """Renders read-only hierarchical dropdown blocks and returns selected Node IDs."""
    st.subheader("🔍 Select Target Hardware")
    col_f1, col_f2, col_f3 = st.columns(3)
    
    with col_f1:
        u_projects = ["All"] + sorted(sp_reg['Project'].dropna().unique().tolist())
        rec_proj = st.selectbox("Select Project Space Context:", u_projects, key="rec_proj_sel_isolated")
    
    proj_filtered = sp_reg if rec_proj == "All" else sp_reg[sp_reg['Project'] == rec_proj]
    
    with col_f2:
        u_locs = ["All"] + sorted(proj_filtered['Location'].dropna().unique().tolist(), key=natural_sort_key)
        rec_loc = st.selectbox("Select Physical Location Context:", u_locs, key="rec_loc_sel_isolated")
        
    with col_f3:
        loc_filtered = proj_filtered if rec_loc == "All" else proj_filtered[proj_filtered['Location'] == rec_loc]
        available_nodes = sorted(loc_filtered['NodeNum'].dropna().unique().tolist(), key=natural_sort_key)
        
        selected_nodes = st.multiselect(
            "Select Target Node Numbers", 
            available_nodes, 
            default=None,
            key="rec_nodes_multiselect_isolated",
            help="Choose the specific sensors to backfill. Leave empty to pull all filtered assets."
        )
    return selected_nodes

######################
# Page: Admin Tools  #
######################

def render_admin_page(selected_project, display_tz, unit_mode, unit_label, active_refs):
    """
    Advanced Admin Tools: Centralized administrative command center.
    Integrated with the 📋 Node Master interface as Sub-Tab 3.
    """
    import re
    import numpy as np
    import plotly.graph_objects as go
    from datetime import datetime, timedelta
    import time
    import requests
    import pandas as pd
    
    st.header("🛠️ Admin Tools")
    
    client = get_bq_client()
    if client is None: 
        st.error("Database connection unavailable.")
        return

    # 1. CENTRAL TRANSACTIONAL DATA FETCH     
    target_registry_path = NODE_REGISTRY_TABLE
    try:
        proj_q = f"SELECT CAST(Project AS STRING) as Project, ProjectName, Timezone, ProjectStatus, Date_Freezedown FROM `{PROJECT_REGISTRY_TABLE}` WHERE UPPER(TRIM(CAST(ShowActive AS STRING))) = 'YES'"
        full_reg_df = client.query(f"SELECT * FROM `{NODE_REGISTRY_TABLE}` WHERE End_Date IS NULL").to_dataframe()
        available_projects_list = sorted(client.query(proj_q).to_dataframe()['Project'].dropna().unique().tolist())
    except Exception as e:
        st.error(f"Registry Link Offline: {e}")
        return

    # 2. NAVIGATION TABS (Aligned matching your exact blueprint)
    tab_admin_sum, tab_bulk_app, tab_logistics, tab_recovery, tab_proj_master, tab_chillers = st.tabs([
        "📋 Admin Summary", 
        "⚡ Bulk Approval", 
        "📋 Node Master",  
        "📡 Data Recovery", 
        "⚙️ Project Master", 
        "❄️ Chiller Operations"
    ])

    # --- SUB-TAB 1: ADMIN SUMMARY ---
    with tab_admin_sum:
        st.subheader("📋 Centralized Infrastructure Status Overview")
        
        # Table 1: Hardware Inventory Fleet Breakdown
        st.markdown("### 📡 Hardware Inventory Fleet Breakdown")
        try:
            def classify_hardware_family(node):
                node_str = str(node).lower()
                if "-ch" in node_str: return "Lord"
                elif node_str.startswith("sp"): return "SP"
                elif node_str.startswith("tp"): return "TP"
                return "None of the Above"

            fleet_df = full_reg_df.copy()
            fleet_df['Hardware Family'] = fleet_df['NodeNum'].apply(classify_hardware_family)
            fleet_df['Parent ID'] = fleet_df['NodeNum'].apply(lambda x: re.split(r'(?i)-ch', str(x))[0] if "-ch" in str(x).lower() else x)
            fleet_df['is_active'] = True
            
            deduped_units = fleet_df.sort_values(by=['Parent ID']).drop_duplicates(subset=['Parent ID']).copy()
            
            fleet_pivot = deduped_units.groupby(['Hardware Family', 'SensorStatus']).size().unstack(fill_value=0)
            desired_order = ["TP", "SP", "Lord", "None of the Above"]
            fleet_pivot = fleet_pivot.reindex(desired_order, fill_value=0)
            
            for stat_col in ["Available", "Dead", "Diagnostic", "On Project"]:
                if stat_col not in fleet_pivot.columns:
                    fleet_pivot[stat_col] = 0
            
            fleet_pivot = fleet_pivot[["Available", "Dead", "Diagnostic", "On Project"]]
            fleet_pivot['Total Units'] = fleet_pivot.sum(axis=1)
            st.dataframe(fleet_pivot.reset_index(), use_container_width=True, hide_index=True)
        except Exception as e:
            st.caption(f"Inventory matrix loading: {e}")

        st.divider()

        # Table 2: Upgraded Project Overview Matrix (Active Projects only)
        st.markdown("### 🏗️ Active Deployment Overview Matrix")
        summary_summary_q = f"""
            WITH Metrics AS (
                SELECT 
                    n.Project,
                    COUNT(DISTINCT n.NodeNum) as Mapped_Sensors,
                    COUNT(DISTINCT CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR) THEN n.NodeNum END) as Active_in_last_6_hours,
                    COUNT(DISTINCT CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN n.NodeNum END) as Active_in_last_24_hours
                FROM `{NODE_REGISTRY_TABLE}` n
                LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.master_data_view` m ON n.NodeNum = m.NodeNum
                WHERE n.End_Date IS NULL
                GROUP BY n.Project
            )
            SELECT 
                p.Project,
                p.ProjectName,
                p.ProjectStatus,
                p.Date_Freezedown,
                COALESCE(m.Mapped_Sensors, 0) as Mapped_Sensors,
                COALESCE(m.Active_in_last_6_hours, 0) as Active_in_last_6_hours,
                COALESCE(m.Active_in_last_24_hours, 0) as Active_in_last_24_hours
            FROM `{PROJECT_REGISTRY_TABLE}` p
            LEFT JOIN Metrics m ON p.Project = m.Project
            WHERE UPPER(TRIM(CAST(p.ShowActive AS STRING))) = 'YES'
              AND UPPER(p.Project) NOT LIKE '%OFFICE%'
            ORDER BY p.Project ASC
        """
        
        try:
            sum_summary_df = client.query(summary_summary_q).to_dataframe()
            
            rows = []
            for _, r in sum_summary_df.iterrows():
                p_status = str(r['ProjectStatus']).strip()
                f_date = r['Date_Freezedown']
                status_tracking_text = "Not Freezing"
                
                if pd.notnull(f_date):
                    days_elapsed = (pd.Timestamp.now(tz=display_tz).date() - pd.to_datetime(f_date).date()).days
                    days_count = max(0, days_elapsed)
                    
                    if p_status.lower() == "freezedown":
                        status_tracking_text = f"Day {days_count} of Freezedown"
                    elif p_status.lower() == "maintenance":
                        status_tracking_text = f"Day {days_count} of Maintenance"
                    elif p_status.lower() == "pre-freeze":
                        status_tracking_text = f"Pre-freeze (Day {days_count})"
                
                rows.append({
                    "Project ID": r['Project'],
                    "Project Name": r['ProjectName'] if pd.notnull(r['ProjectName']) else r['Project'],
                    "Mapped Sensors": int(r['Mapped_Sensors']),
                    "Active in last 6 hours": int(r['Active_in_last_6_hours']),
                    "Active in last 24 hours": int(r['Active_in_last_24_hours']),
                    "Project Status Timeline": status_tracking_text
                })
                
            display_summary_df = pd.DataFrame(rows)
            st.dataframe(display_summary_df, use_container_width=True, hide_index=True)
            
        except Exception as e:
            st.error(f"Failed to generate upgraded overview matrix: {e}")

        st.divider()

        # Table 3: System-Wide Master Project Directory (All Projects)
        st.markdown("### 🗄️ Master Project Historical Directory")
        all_projects_q = f"""
            WITH NodeCounts AS (
                SELECT Project, COUNT(DISTINCT NodeNum) as Nodes_Assigned
                FROM `{NODE_REGISTRY_TABLE}`
                WHERE End_Date IS NULL
                GROUP BY Project
            )
            SELECT 
                p.Project as `Project ID`,
                COALESCE(p.ProjectName, p.Project) as `Project Name`,
                p.ProjectStatus as `Project Status`,
                COALESCE(n.Nodes_Assigned, 0) as `Sensors Assigned`
            FROM `{PROJECT_REGISTRY_TABLE}` p
            LEFT JOIN NodeCounts n ON p.Project = n.Project
            ORDER BY p.ProjectStatus ASC, p.Project ASC
        """
        
        try:
            with st.spinner("Extracting master systemic tracking history profiles..."):
                all_proj_df = client.query(all_projects_q).to_dataframe()
                
            if not all_proj_df.empty:
                st.dataframe(all_proj_df, use_container_width=True, hide_index=True)
                st.caption(f"Total historical lifecycle tracking configurations mapped in system: {len(all_proj_df)}")
            else:
                st.info("The system project registry data store is unpopulated.")
        except Exception as master_err:
            st.error(f"Failed to build master data log directory: {master_err}")

    # --- SUB-TAB 2: BULK APPROVAL SYSTEM RUNROOM ---
    with tab_bulk_app:
        execute_bulk_approval_workspace(client, full_reg_df, selected_project, tab_logistics)
        
    # --- SUB-TAB 3: NODE MASTER ASSIGNMENTS VIEWER ---
    with tab_logistics:
        st.title("📋 Node Status Timeline Assignments Viewer")
        st.write("Review active field loops or examine chronological allocation parameters tracked across structural nodes.")
        st.divider()
        
        col_l1, col_l2, col_l3 = st.columns(3)
        with col_l1:
            raw_projects = full_reg_df['Project'].dropna().unique().tolist() if not full_reg_df.empty else []
            u_projects = sorted(list(set(["Office"] + raw_projects)))
            selected_log_proj = st.selectbox("Select Project Space Context:", u_projects, key="node_log_project_filter")
        
        proj_filtered_df = full_reg_df[full_reg_df['Project'] == selected_log_proj] if not full_reg_df.empty else pd.DataFrame()
        
        with col_l2:
            u_locations = sorted(proj_filtered_df['Location'].dropna().unique().tolist(), key=natural_sort_key) if not proj_filtered_df.empty else []
            if not u_locations: u_locations = ["Office"]
            selected_log_loc = st.selectbox("Select Physical Location Context:", u_locations, key="node_log_location_filter")
            
        loc_filtered_df = proj_filtered_df[proj_filtered_df['Location'] == selected_log_loc] if not proj_filtered_df.empty else pd.DataFrame()
        
        with col_l3:
            u_nodes = sorted(loc_filtered_df['NodeNum'].dropna().unique().tolist(), key=natural_sort_key) if not loc_filtered_df.empty else []
            selected_log_node = st.selectbox("Select Target Node Number ID:", u_nodes, index=0 if u_nodes else None, key="node_log_node_filter")

        st.divider()

        if selected_log_node:
            st.markdown(f"### 🕒 Assignment History Timeline: **{selected_log_node}**")
            history_df = client.query(f"SELECT Project, Location, Bank, Depth, CAST(Start_Date AS STRING) as Deployment_Date, COALESCE(CAST(End_Date AS STRING), 'Active') as Cutoff_Date, SensorStatus FROM `{NODE_REGISTRY_TABLE}` WHERE NodeNum = '{selected_log_node}' ORDER BY Start_Date DESC").to_dataframe()
            st.dataframe(history_df, use_container_width=True, hide_index=True)

    # --- SUB-TAB 4: SENSORPUSH API CLOUD RECOVERY BACKFILL ENGINE ---
    with tab_recovery:
        st.title("📡 Data Recovery Engine")
        st.write("Extract missing data matrices from the SensorPush API framework and execute batch appends straight to raw tables.")
        dropdown_selected_nodes = render_recovery_filters(full_reg_df)

        st.divider()
        st.subheader("📅 Define Recovery Timeline Parameters")
        rec_c1, rec_c2 = st.columns(2)
        with rec_c1:
            rec_start_date = st.date_input("Extraction Window Start Date", value=datetime.now().date() - timedelta(days=2), key="dt_rec_start")
        with rec_c2:
            rec_end_date = st.date_input("Extraction Window End Date", value=datetime.now().date(), key="dt_rec_end")

        st.divider()

        if dropdown_selected_nodes:
            final_target_nodes = dropdown_selected_nodes
        else:
            active_proj_context = st.session_state.get('rec_proj_sel_isolated', 'All')
            active_loc_context = st.session_state.get('rec_loc_sel_isolated', 'All')
            slice_df = full_reg_df.copy()
            if active_proj_context != "All": slice_df = slice_df[slice_df['Project'] == active_proj_context]
            if active_loc_context != "All": slice_df = slice_df[slice_df['Location'] == active_loc_context]
            final_target_nodes = sorted(slice_df['NodeNum'].dropna().unique().tolist())

        scope_text = f"{len(final_target_nodes)} selected nodes" if final_target_nodes else "ALL registered fleet nodes"
        st.warning(f"⚠️ **Action Required:** Initiating backfill protocol for {scope_text} from **{rec_start_date}** through **{rec_end_date}**.")

        if 'recovery_run_complete' not in st.session_state: st.session_state['recovery_run_complete'] = False
        if 'recovery_cached_rows' not in st.session_state: st.session_state['recovery_cached_rows'] = []
        if 'recovery_cached_stats' not in st.session_state: st.session_state['recovery_cached_stats'] = {}

        if st.button("🚀 Execute Cloud Backfill Ingestion Pipeline Run", use_container_width=True, key="btn_trigger_recovery_run"):
            all_rows, hardware_map, reverse_hardware_map, db_max_timestamps, account_stats = [], {}, {}, {}, {}
            LOCAL_REC_TABLE, LOCAL_INV_TABLE, LOCAL_API_URL = "raw_sensorpush", "hardware_inventory", "https://api.sensorpush.com/api/v1"
            ACCOUNTS = [{'email': 'ldunham@soilfreeze.com', 'password': 'Freeze123!!'}, {'email': 'tsteele@soilfreeze.com', 'password': 'Freeze123!!'}, {'email': 'soilfreeze98072@gmail.com', 'password': 'Freeze123!!'}]
            
            start_time_iso = datetime.combine(rec_start_date, datetime.min.time()).strftime('%Y-%m-%dT%H:%M:%SZ')
            end_time_iso = datetime.combine(rec_end_date, datetime.max.time()).strftime('%Y-%m-%dT%H:%M:%SZ')

            with st.status("Executing Cloud Backfill Ingestion Pipeline Run...", expanded=True) as status_box:
                st.write("🔍 Extracting Translation Mappings from Hardware Inventory...")
                try:
                    for row in client.query(f"SELECT RawID, NodeNum FROM `{PROJECT_ID}.{DATASET_ID}.{LOCAL_INV_TABLE}` WHERE RawID IS NOT NULL"):
                        clean_db_id = str(row.RawID).split('.')[0].strip()
                        friendly_name = str(row.NodeNum).strip()
                        hardware_map[clean_db_id] = friendly_name; reverse_hardware_map[friendly_name] = clean_db_id
                except Exception as e: st.error(f"Failed to query inventory map tables: {e}"); st.stop()
                
                try:
                    for row in client.query(f"SELECT NodeNum, FORMAT_TIMESTAMP('%m/%d/%Y %H:%M UTC', MAX(timestamp)) as max_time FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` GROUP BY NodeNum"):
                        db_max_timestamps[str(row.NodeNum)] = str(row.max_time)
                except Exception as e: st.warning(f"Could not calculate maximum timelines: {e}")

                for acc in ACCOUNTS:
                    account_stats[acc['email']] = 0
                    try:
                        auth_r = requests.post(f"{LOCAL_API_URL}/oauth/authorize", json=acc, timeout=15).json()
                        token = requests.post(f"{LOCAL_API_URL}/oauth/accesstoken", json={"authorization": auth_r['authorization']}, timeout=15).json().get('accesstoken')
                        s_resp = requests.post(f"{LOCAL_API_URL}/devices/sensors", headers={"Authorization": token}, json={}, timeout=20).json()
                        device_rssi_map = {str(sid).strip(): sm.get('rssi') for sid, sm in s_resp.items() if isinstance(sm, dict) and 'rssi' in sm}
                        
                        r_samples = requests.post(f"{LOCAL_API_URL}/samples", headers={"Authorization": token}, json={"startTime": start_time_iso, "endTime": end_time_iso, "limit": 100000}, timeout=60).json()
                        for sid, samples in r_samples.get('sensors', {}).items():
                            fn = hardware_map.get(str(sid).split('.')[0].strip()) or next((tn for dt in final_target_nodes if reverse_hardware_map.get(tn) == str(sid).split('.')[0].strip()), None)
                            if fn in final_target_nodes:
                                current_device_rssi = device_rssi_map.get(str(sid).strip())
                                for s in samples:
                                    temp = s.get('temp_f') or s.get('temperature')
                                    if temp is not None:
                                        account_stats[acc['email']] += 1
                                        all_rows.append({"timestamp": pd.to_datetime(s['observed']), "NodeNum": str(fn), "temperature": float(temp), "rssi": float(current_device_rssi) if current_device_rssi is not None else None})
                    except Exception: continue

                if all_rows:
                    upload_df = pd.DataFrame(all_rows)
                    upload_df['timestamp'] = pd.to_datetime(upload_df['timestamp'], utc=True)
                    client.load_table_from_dataframe(upload_df, f"{PROJECT_ID}.{DATASET_ID}.{LOCAL_REC_TABLE}", job_config=bigquery.LoadJobConfig(schema=[bigquery.SchemaField("timestamp", "TIMESTAMP"), bigquery.SchemaField("NodeNum", "STRING"), bigquery.SchemaField("temperature", "FLOAT"), bigquery.SchemaField("rssi", "FLOAT")], write_disposition="WRITE_APPEND")).result()
                    st.success(f"🎉 Success! Appended {len(upload_df):,} raw rows onto production storage views.")
                    st.session_state['recovery_cached_rows'], st.session_state['recovery_cached_stats'], st.session_state['recovery_run_complete'] = all_rows, db_max_timestamps, True
                    st.cache_data.clear(); st.rerun()
                else: status_box.update(label="Cloud sync found 0 missing rows for selected variables.", state="complete")

        if st.session_state.get('recovery_run_complete'):
            summary_records = [{"Node Table Number": node, "Last Database Check-In": st.session_state['recovery_cached_stats'].get(node, "❌ No Historical Mapped Records Found"), "Points Extracted & Appended": sum(1 for r in st.session_state['recovery_cached_rows'] if r["NodeNum"] == node)} for node in final_target_nodes]
            summary_df = pd.DataFrame(summary_records).sort_values(by="Node Table Number")
            st.dataframe(pd.concat([summary_df, pd.DataFrame([{"Node Table Number": "🧮 Combined Total Pool", "Last Database Check-In": "—", "Points Extracted & Appended": len(st.session_state['recovery_cached_rows'])}])], ignore_index=True), use_container_width=True, hide_index=True)
            if len(st.session_state['recovery_cached_rows']) > 0: st.balloons()

    # --- SUB-TAB 5: PROJECT LIFECYCLE HISTORY DIRECTORY ---
    with tab_proj_master:
        st.subheader("🗄️ Complete Master Project Lifecycle Directory")
        st.dataframe(client.query(f"SELECT Project as `Project ID`, ProjectName as `Friendly Name`, ProjectStatus as `Operational Phase`, Date_Freezedown as `Freezedown Date`, City, Timezone FROM `{PROJECT_REGISTRY_TABLE}` ORDER BY Project ASC").to_dataframe(), use_container_width=True, hide_index=True)

    # --- SUB-TAB 6: MECHANICAL CHILLER FLEET OPERATIONS ---
    with tab_chillers:
        st.subheader("❄️ Mechanical Chiller Fleet Deployment Manifest")
        try:
            st.dataframe(client.query(f"SELECT c.chiller_id as `Chiller Loop ID`, c.chiller_type as Specifications, c.status as Status, COALESCE(d.current_location, 'Yard / Shop Staging') as `Deployment Location Context` FROM `{PROJECT_ID}.{DATASET_ID}.chiller_registry` c LEFT JOIN (SELECT Chiller, MAX_BY(Location, StartDate) as current_location FROM `{PROJECT_ID}.{DATASET_ID}.project_systems_map` GROUP BY Chiller) d ON c.chiller_id = d.Chiller ORDER BY 1 ASC").to_dataframe(), use_container_width=True, hide_index=True)
        except Exception as e: st.error(f"Asset Manifest Offline: {e}")

# =============================================================================
# 12. MASTER LAYOUT FRAMEWORK PAGE ROUTER
# =============================================================================
display_tz = st.session_state.get("display_tz", "UTC")
unit_label = st.session_state.get("unit_label", "°F")
unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
active_refs = st.session_state.get("active_refs", [])

client = get_bq_client() 

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
            pwd = st.text_input("Enter Admin Password", type="password", key="admin_password_input_field")
            if st.button("Unlock Dashboard", use_container_width=True):
                if pwd == st.secrets.get("admin_password", "Freeze123!!"):
                    st.session_state['authenticated'] = True
                    st.rerun()
                else:
                    st.error("Invalid Password. Access Denied.")

                c2.error("Invalid Password. Access Denied.")
