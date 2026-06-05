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
def get_universal_portal_data(project_id, view_mode="engineering"):
    client = get_bq_client()
    if client is None: return pd.DataFrame()

    is_office = "OFFICE" in str(project_id).upper()

    if view_mode == "client":
        filter_sql = "AND UPPER(CAST(m.approval_status AS STRING)) IN ('TRUE', '1')"
    else:
        # If Office, show everything except BadData. If regular project, hide False/0.
        if is_office:
            filter_sql = "AND UPPER(COALESCE(CAST(m.approval_status AS STRING), 'PENDING')) != 'BADDATA'"
        else:
            filter_sql = "AND UPPER(COALESCE(CAST(m.approval_status AS STRING), 'PENDING')) NOT IN ('BADDATA', 'FALSE', '0')"

    query = f"""
        SELECT m.* FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
        JOIN `{PROJECT_ID}.{DATASET_ID}.project_registry` p ON m.Project = p.Project
        WHERE m.Project = @project_id
        {filter_sql}
        ORDER BY m.timestamp ASC
    """
    # ... rest of function
    
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
        # SQL fix: Exclude empty strings and force inclusion of 'Office'
        proj_q = f"""
            SELECT Project, ProjectName, Timezone, ProjectStatus, Date_Freezedown, SoilType 
            FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` 
            WHERE Project IS NOT NULL 
              AND TRIM(Project) != ''
              AND (ProjectStatus != 'Archived' OR UPPER(Project) LIKE '%OFFICE%')
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
                WHERE Project = '{selected_project}'
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
# --- NEW INSERTION: CSS TO FORCE DATA TABLE PROGRESS COLUMNS RED ---
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

    # 3. THEORETICAL REFERENCE CURVES
    if curve_id and curve_id != "None" and f_start_date:
        try:
            # Extract the raw project number (e.g., 2538)
            proj_str = str(st.session_state.get('selected_project', ''))
            proj_match = re.findall(r'\d+', proj_str)
            proj_num = proj_match[0] if proj_match else ""
            
            # Extract the exact pipe identifier (e.g., T1, T10)
            loc_part = str(curve_id).split('-')[-1].strip() if curve_id else ""

            if proj_num and loc_part:
                # Rigid string comparison blocks to eliminate multi-channel cross-talk leaks
                target_q = f"""
                    SELECT CurveID, Day, Temp 
                    FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` 
                    WHERE (CurveID = '{proj_num}-{loc_part}' 
                       OR CurveID = '{proj_num}_{loc_part}'
                       OR UPPER(CurveID) LIKE UPPER('{proj_num}-%{loc_part}')
                       OR (UPPER(CurveID) LIKE UPPER('%{proj_num}%') AND ENDS_WITH(UPPER(CurveID), UPPER('-{loc_part}'))))
                    ORDER BY Day
                """
                target_df = client.query(target_q).to_dataframe()
                if not target_df.empty:
                    dash_styles = ['dashdot', 'dash', 'dot']
                    gray_shades = [
                        'rgba(30, 30, 30, 0.8)',   
                        'rgba(70, 70, 70, 0.75)',  
                        'rgba(110, 110, 110, 0.7)' 
                    ]
                    
                    for c_idx, (cid, c_df) in enumerate(target_df.groupby('CurveID')):
                        c_df['timestamp'] = c_df['Day'].apply(lambda d: pd.Timestamp(f_start_date) + pd.Timedelta(days=d))
                        c_df['timestamp'] = c_df['timestamp'].dt.tz_localize('UTC').dt.tz_convert(display_tz)
                        ref_y = c_df['Temp'] if unit_mode == "Fahrenheit" else (c_df['Temp'] - 32) * 5/9
                        soil_label = str(cid).split('-')[-1].strip()
                        
                        selected_dash = dash_styles[c_idx % len(dash_styles)]
                        selected_gray = gray_shades[c_idx % len(gray_shades)]
                        
                        fig.add_trace(go.Scatter(
                            x=c_df['timestamp'], 
                            y=ref_y, 
                            name=f"<b>Goal: {soil_label}</b>", 
                            mode='lines',
                            line=dict(
                                color=selected_gray, 
                                width=3.5, 
                                dash=selected_dash, 
                                shape='spline', 
                                smoothing=1.3
                            ),
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
            INNER JOIN `{PROJECT_ID}.{DATASET_ID}.node_registry` AS n ON r.NodeNum = n.NodeNum
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
    """
    st.header("🌐 Global Project Summary")
    
    client = get_bq_client()
    if client is None: return

    mobile_mode = st.session_state.get("mobile_optimized_toggle", False)

    # SQL QUERY: Balanced approach showing active field data while purging bad data
    summary_q = f"""
        WITH active_projects AS (
            SELECT Project, ProjectName, ProjectStatus, Date_Freezedown
            FROM `{PROJECT_ID}.{DATASET_ID}.project_registry`
            WHERE ProjectStatus IN ('Freezedown', 'Maintenance', 'Pre-freeze')
              AND UPPER(Project) NOT LIKE '%OFFICE%'
        ),
        raw_data AS (
            SELECT 
                n.Project, n.Bank, n.Location, n.Depth, m.temperature, m.timestamp, n.NodeNum
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
            JOIN `{PROJECT_ID}.{DATASET_ID}.node_registry` n ON m.NodeNum = n.NodeNum
            WHERE m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
              -- BALANCED RULE: Show verified AND streaming real-time data, but block bad data
              AND UPPER(COALESCE(CAST(m.approval_status AS STRING), 'PENDING')) NOT IN ('BADDATA', 'FALSE', '0')
              -- Outlier Shield: Ignore hardware spikes above boiling point
              AND NOT (m.temperature > 100 AND NOT STARTS_WITH(n.NodeNum, 'SP'))
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
            
            # --- FIXED: ACCURATE CHECK-IN COUNTERS ---
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

#############################
# - 2. PAGE: TIME vs TEMP - #
#############################

def render_global_overview(selected_project, project_metadata, display_tz):
    """
    Shows all pipes/banks for a selected project in one scrolling view.
    Updated: Connected to global red lookback slider state.
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
        p_df = get_universal_portal_data(selected_project, view_mode="engineering")

    if p_df.empty:
        st.warning(f"No engineering data found for '{p_name}'.")
        return

    # 5. DYNAMIC UI FILTERING
    mask_col = 'approval_status' if 'approval_status' in p_df.columns else 'approve'
    
    if not show_masked and mask_col in p_df.columns:
        p_df = p_df[p_df[mask_col].astype(str).str.upper() != 'MASKED'].copy()

    # --- 6. TIMELINE CONFIG (CONNECTED TO GLOBAL RED SLIDER) ---
    lookback_weeks = st.session_state.get("global_lookback_weeks_slider", 5)
    
    now_local = pd.Timestamp.now(tz=display_tz)
    end_view = (now_local + pd.Timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Calculate start view dynamically based on the slider value
    start_view = end_view - pd.Timedelta(weeks=lookback_weeks)

    # 7. LOCATION-BASED PLOTTING LOOP
    locations = sorted(
        [str(loc) for loc in p_df['Location'].dropna().unique()], 
        key=natural_sort_key
    )

    for loc in locations:
        with st.expander(f"📍 Location: {loc}", expanded=True):
            loc_df = p_df[p_df['Location'] == loc].copy()
            
            # Extract ID and Location for Curve Matching
            clean_proj_id = str(selected_project).split('-')[0]
            search_id = f"{clean_proj_id}-{loc}"
            
            is_temp_pipe = any(x in loc.upper() for x in ["TP", "T", "PIPE", "TEMP"])
            
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

#########################
# Page 3 - Depth Charts #
#########################

def render_depth_charts(selected_project, unit_label, display_tz):
    """
    Engineering-grade Vertical Temperature Profiles.
    - Empirical data only (no theoretical lines).
    - Recent Line: Most recent day's 6:00 AM snapshot (Bright Orange Solid Line).
    - Fallback Engine: If a specific pipe lacks a 6 AM reading on the most recent day,
      it scans that same day to find its closest available reading as a fallback substitute.
    - Baseline: First Monday at 06:00 AM (Black Dashed Line) forced to sit on top layer.
    - Outlier Filter: Explicitly excludes any rogue sensor pings reading above 50°F.
    - Freezing Line: Light Blue (Hex #ADD8E6).
    - Scale: Fixed -20 to 80.
    - Frame: Full 4-sided black box.
    """
    # 1. HEADER
    st.header(f"📏 Depth Profile Analysis: {selected_project}")
    
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to view depth profiles.")
        return

    # 2. SIDEBAR SETTINGS
    st.sidebar.subheader("📐 Profile Settings")
    lookback_weeks = st.sidebar.slider("Historical Snapshots (Weeks)", 1, 24, 8, key="depth_lookback")

    with st.spinner("Fetching historical telemetry..."):
        p_df = get_universal_portal_data(selected_project, view_mode="engineering")

    if p_df is None or p_df.empty:
        st.warning("No data found for this project.")
        return

    # 3. PRE-PROCESS DATA & APPLY 50°F OUTLIER MASK FILTER
    p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
    
    # HARD FILTER CRITERIA: Ignore any weird readings above 50°F before building charts
    p_df = p_df[p_df['temperature'] <= 50.0]
    
    depth_df = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
    
    if depth_df.empty:
        st.info("No sensors with valid 'Depth' values under 50°F found in the registry.")
        return

    unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
    freeze_pt = 0 if unit_mode == "Celsius" else 32
    
    # 4. TIMELINE REFERENCE SYSTEM CONTROLS
    now_utc = pd.Timestamp.now(tz='UTC')
    mondays = pd.date_range(end=now_utc, periods=lookback_weeks, freq='W-MON')
    locations = sorted(depth_df['Location'].unique())
    
    for loc in locations:
        with st.expander(f"📍 Temp vs Depth - {loc}", expanded=True):
            loc_data = depth_df[depth_df['Location'] == loc].copy()
            
            # Ensure timestamps are localized matching display preferences
            if loc_data['timestamp'].dt.tz is None:
                loc_data['timestamp'] = loc_data['timestamp'].dt.tz_localize('UTC')
            loc_data['timestamp_local'] = loc_data['timestamp'].dt.tz_convert(display_tz)
            
            fig = go.Figure()

            # --- A. CALCULATE BASELINE RAW HOOKS ---
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

            # --- B. HARDENED MOST RECENT LINE SEARCH ENGINE (WITH PIPE-LEVEL FALLBACKS) ---
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
                            # Symmetrical dynamic fallback checking window
                            node_group = node_group.assign(hour_dist=(node_group['hour_int'] - 6).abs())
                            best_fallback_row = node_group.sort_values(by=['hour_dist', 'timestamp_local']).iloc[0]
                            recent_profile_rows.append(best_fallback_row)
                    break

            snap_recent = pd.DataFrame(recent_profile_rows).sort_values('Depth_Num') if recent_profile_rows else pd.DataFrame()

            # --- C. PLOT WEEKLY HISTORICAL SNAPSHOTS (Mondays) ---
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
                    
                    # Hard fallback window processing check
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

            # --- D. INJECT THE BRIGHT ORANGE HARDENED MOST RECENT PROFILE ---
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

            # --- E. LAYER OVERRIDE: INJECT BLACK DASHED BASELINE AT THE VERY END ---
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

            # --- F. FREEZING REFERENCE LINE ---
            fig.add_vline(x=freeze_pt, line_width=2, line_dash="solid", line_color="#ADD8E6")

            # --- G. STANDARDIZED SCALING & BOX FRAME ---
            max_depth = loc_data['Depth_Num'].max()
            y_limit = int(((max_depth // 10) + 1) * 10) if pd.notnull(max_depth) else 50

            fig.update_layout(
                title=f"<b>Temp vs Depth - {loc}</b>",
                plot_bgcolor='white', 
                height=800,
                xaxis=dict(
                    title=f"Temperature ({unit_label})", 
                    range=[-20, 80], 
                    dtick=10,
                    minor=dict(dtick=2, showgrid=True, gridcolor='#f8f8f8'),
                    gridcolor='Gainsboro', 
                    showline=True, linewidth=2, linecolor='black', mirror=True
                ),
                yaxis=dict(
                    title="Depth (ft)", 
                    range=[y_limit, 0], 
                    dtick=10,
                    minor=dict(dtick=2, showgrid=True, gridcolor='#f8f8f8'),
                    gridcolor='Silver', 
                    showline=True, linewidth=2, linecolor='black', mirror=True
                ),
                legend=dict(orientation="h", y=-0.1, xanchor="center", x=0.5)
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
                # FIXED: Structural patch using positional extraction to avoid tracking mismatches
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

    # FIXED: Replaced non-existent tracking hook call with explicit session variables
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

###########
# - 8. PAGE: NODE DIAGNOSTICS - #
###########

def render_node_diagnostics(selected_project, display_tz, unit_label):
    """
    Page Name: Node Diagnostics
    Live connectivity audit, signal quality metrics, and operational performance efficiency matrix.
    """
    st.header("📡 Commissioning & Diagnostics Audit")
    st.write("Advanced real-time audit of hardware packet logs, signal tracking metrics, and data reporting streams.")

    client = get_bq_client()
    if client is None: 
        st.error("Database connection lost.")
        return

    # 1. ENHANCED DIAGNOSTIC ENGINE QUERY
    diag_q = f"""
        WITH Stats AS (
            SELECT 
                NodeNum,
                MAX(timestamp) as last_ping,
                ARRAY_AGG(temperature ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] as last_temp,
                COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) as count_1h,
                COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)) as count_6h,
                COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) as count_24h,
                
                -- Live hardware signal quality hooks
                ARRAY_AGG(rssi ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] as rssi_last_val,
                AVG(rssi) as rssi_avg_val
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view`
            GROUP BY NodeNum
        )
        SELECT 
            n.Project,
            n.Location, 
            n.NodeNum, 
            n.Bank, 
            n.Depth,
            n.SensorStatus, 
            s.last_ping,
            s.last_temp,
            COALESCE(s.count_1h, 0) as count_1h,
            COALESCE(s.count_6h, 0) as count_6h,
            COALESCE(s.count_24h, 0) as count_24h,
            s.rssi_last_val as rssi_last,
            s.rssi_avg_val as rssi_avg
        FROM `{PROJECT_ID}.{DATASET_ID}.node_registry` n
        LEFT JOIN Stats s ON n.NodeNum = s.NodeNum
        WHERE n.End_Date IS NULL
    """
    
    try:
        df = client.query(diag_q).to_dataframe()
        if df.empty:
            st.warning("No nodes found in the system registry.")
            return

        now_utc = pd.Timestamp.now(tz='UTC')

        # 2. INTERACTIVE DRILLDOWN FILTERS
        st.markdown("### 🔍 Filter Fleet Scope")
        f_col1, f_col2, f_col3 = st.columns(3)
        
        with f_col1:
            proj_opts = ["--- All Projects ---"] + sorted(df['Project'].dropna().unique().tolist())
            init_proj_idx = proj_opts.index(selected_project) if selected_project in proj_opts else 0
            filter_proj = st.selectbox("Scope Project Context:", proj_opts, index=init_proj_idx)
            
        with f_col2:
            sub_df = df.copy() if filter_proj == "--- All Projects ---" else df[df['Project'] == filter_proj]
            filter_loc = st.selectbox("Scope Physical Location:", ["--- All Locations ---"] + sorted(sub_df['Location'].dropna().unique().tolist()))
            
        with f_col3:
            filter_stat = st.selectbox("Scope Hardware Status:", ["--- All Statuses ---"] + sorted(sub_df['SensorStatus'].dropna().unique().tolist()))

        # Execute Cascading Truncations
        if filter_proj != "--- All Projects ---":
            df = df[df['Project'] == filter_proj]
        if filter_loc != "--- All Locations ---":
            df = df[df['Location'] == filter_loc]
        if filter_stat != "--- All Statuses ---":
            df = df[df['SensorStatus'] == filter_stat]

        if df.empty:
            st.info("No matching hardware entries found for current selected filters.")
            return

        # 3. CONVERT LATENCY TO PURE HOURS & CALCULATE STYLE COLOR MATRICES
        def process_latency_metrics(row):
            ping = row['last_ping']
            if pd.isnull(ping):
                return pd.Series(["❌ Never", "background-color: #d1d5db; color: #1f2937;", float('inf')])
            
            ts = ping if ping.tzinfo else ping.tz_localize('UTC')
            hours_hidden = (now_utc - ts).total_seconds() / 3600.0
            txt = f"{hours_hidden:.1f}h"
            
            if hours_hidden < 1.0:
                style = "background-color: #d1fae5; color: #065f46;"
            elif 1.0 <= hours_hidden <= 6.0:
                style = "background-color: #fef08a; color: #854d0e;"
            elif 6.0 < hours_hidden <= 12.0:
                style = "background-color: #fed7aa; color: #9a3412;"
            elif 12.0 < hours_hidden <= 24.0:
                style = "background-color: #fca5a5; color: #991b1b;"
            else:
                style = "background-color: #d1d5db; color: #1f2937;"
                
            return pd.Series([txt, style, hours_hidden])

        df[['Seen_Text', 'Seen_Style', 'hours_hidden']] = df.apply(process_latency_metrics, axis=1)

        # 4. CHRONOLOGICAL DATA FRAME PRE-SORT
        df['hours_hidden'] = pd.to_numeric(df['hours_hidden'], errors='coerce').fillna(float('inf'))
        df = df.sort_values(by='hours_hidden', ascending=True).reset_index(drop=True)

        # 5. MAX 5-CHARACTER LOCATION CLIPPER
        def compress_location(loc_val):
            loc_str = str(loc_val).strip()
            if len(loc_str) > 5:
                return f"{loc_str[:5]}"
            return loc_str

        df['Compact_Loc'] = df['Location'].apply(compress_location)

        # 6. FIXED CLEAN POSITION LABELS (Removes "Bank" prefix string)
        def resolve_clean_position(row):
            if pd.notnull(row.get('Depth')) and row.get('Depth') != 0:
                return f"{row['Depth']}ft"
            if pd.notnull(row.get('Bank')) and str(row.get('Bank')).strip() != "":
                # Strips out any loose structural occurrences of word variations safely
                return re.sub(r'(?i)bank\s*', '', str(row['Bank'])).strip()
            return "-"

        df['Clean_Pos'] = df.apply(resolve_clean_position, axis=1)

        # 7. TEMPERATURE AND REPORTING EFFICIENCY PARSERS
        unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
        def format_temperatures(val):
            if pd.isnull(val): return "N/A"
            c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
            return f"{round(c_val, 1)}{unit_label}"

        df['efficiency_pct'] = ((df['count_24h'] / 96.0) * 100.0).clip(upper=100.0)

        # 8. MATRIX PRESENTATION FRAME COMPILE
        display_df = pd.DataFrame({
            "Node ID": df['NodeNum'],
            "Location": df['Compact_Loc'],
            "Position": df['Clean_Pos'],
            "Current Temp": df['last_temp'].apply(format_temperatures),
            "Last Seen": df['Seen_Text'],
            "Last Temp": df['last_temp'].apply(format_temperatures),
            "Pings (1h)": df['count_1h'].astype(int),
            "Pings (6h)": df['count_6h'].astype(int),
            "Pings (24h)": df['count_24h'].astype(int),
            "RSSI Last": df['rssi_last'].apply(lambda x: f"{int(x)} dBm" if pd.notnull(x) and not pd.isna(x) else "N/A"),
            "RSSI Avg": df['rssi_avg'].apply(lambda x: f"{int(x)} dBm" if pd.notnull(x) and not pd.isna(x) else "N/A"),
            "Reporting Efficiency": df['efficiency_pct']
        })

        # 9. CELL COLOR INJECTION MATRIX OVERRIDE
        def diagnostic_styler(data):
            style_canvas = pd.DataFrame('', index=data.index, columns=data.columns)
            for i in data.index:
                style_canvas.loc[i, 'Last Seen'] = df.loc[i, 'Seen_Style']
            return style_canvas

        st.dataframe(
            display_df.style.apply(diagnostic_styler, axis=None),
            use_container_width=True,
            hide_index=True,
            column_config={
                "Reporting Efficiency": st.column_config.ProgressColumn(
                    "Reporting Efficiency",
                    format="%.0f%%",
                    min_value=0,
                    max_value=100
                )
            }
        )
        
    except Exception as e:
        st.error(f"Diagnostics Audit Failed: {e}")
        
# ===============================================================
# Function: Status Dashboard (Setup Node Tool) - Left Unchanged
# ===============================================================
def render_project_status_dashboard(client, selected_project, unit_label, target_registry):
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
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
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

###########################
# Page: Data Processing   #
###########################

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
                                
                                job_config = bigquery.LoadJobConfig(
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
            
            with st.spinner("Processing engineering records..."):
                full_df = get_universal_portal_data(selected_project, view_mode="engineering")
            
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

    # --- TAB 4: SITE EVENT LOGGING ENGINE ---
    with tab_event_log:
        st.subheader("🚨 Log New Site Event Entry")
        st.write("Track power transitions, compressor cycles, and generator behaviors relative to active freeze down operations.")
        
        try:
            proj_reg_q = f"SELECT Project FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE ProjectStatus != 'Archived' ORDER BY Project"
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
            event_date = col_el2.date_input("Event Log Date Entry", value=datetime.now().date(), key="input_ev_date")
            event_time = col_el3.time_input("Event Log Time Entry (UTC)", value=datetime.now().time(), key="input_ev_time")
            
            col_el4, col_el5, col_el6 = st.columns(3)
            # Custom event types added matching requirements
            event_type = col_el4.selectbox("Type of Event*", ["Chiller Turn On", "Chiller Turn Off", "Power Source Transition", "Generator Fault / Outage", "Other Site Anomaly"], key="input_ev_type")
            power_type = col_el5.selectbox("Active Power Type Source*", ["Line Power", "Generator", "None / Outage State"], key="input_ev_power")
            assoc_chiller = col_el6.selectbox("Associated Chiller Loop (Optional)", ["None"] + active_chillers_list, key="input_ev_chiller")
            
            proj_system = st.text_input("Project System Loop Identifier (Optional)", placeholder="e.g., Loop A, Loop B (Leave blank if project uses only one system)")
            
            event_desc = st.text_input("Operational Event Description / Detailed Log Alert Notes*", placeholder="e.g., Generator 2 ran out of fuel causing Chiller unit A power dropout for 45 minutes.")
            root_cause = st.text_input("Determined Root Cause Analysis / Notes", placeholder="e.g., Refueling delivery delay window shift.")
            
            c_bool1 = st.checkbox("Timestamp registration is approximate (Estimated time block record flag)", value=False)
            
            if st.form_submit_button("💾 Save Event Entry to Database", use_container_width=True):
                if not event_desc.strip():
                    st.error("❌ Submission Rejected: Event Description tracking summary text notes are required.")
                else:
                    generated_uuid = str(uuid.uuid4())
                    combined_ts = datetime.combine(event_date, event_time).strftime('%Y-%m-%d %H:%M:%S')
                    
                    system_prefix = f"[System: {proj_system.strip()}] " if proj_system.strip() else ""
                    safe_desc = f"{system_prefix}[{event_type} | Power: {power_type}] " + event_desc.strip().replace("'", "''")
                    safe_cause = root_cause.strip().replace("'", "''")
                    chiller_val_str = f"'{assoc_chiller}'" if assoc_chiller != "None" else "NULL"
                    
                    insert_sql = f"""
                        INSERT INTO `{EVENTS_TABLE}` (event_id, project_id, chiller_id, event_timestamp, resolution_timestamp, event_description, root_cause, is_time_approximate)
                        VALUES ('{generated_uuid}', '{target_proj}', {chiller_val_str}, TIMESTAMP('{combined_ts}'), NULL, '{safe_desc}', '{safe_cause}', {str(c_bool1).upper()})
                    """
                    try:
                        with st.spinner("Streaming event logging data to BigQuery table..."):
                            client.query(insert_sql).result()
                        st.success(f"🎉 Success! Event tracking record committed cleanly under transaction code snapshot: `{generated_uuid[:8]}`")
                        st.cache_data.clear()
                        time.sleep(0.5)
                        st.rerun()
                    except Exception as err:
                        st.error(f"Database insertion failed: {err}")
                        st.code(insert_sql, language="sql")

        st.divider()
        
        # --- DYNAMIC RECONFIGURED HISTORICAL ENTRIES TABLE ---
        st.write("#### 📂 Historical Site Log Registry")
        
        f_col1, f_col2 = st.columns(2)
        filter_proj = f_col1.selectbox("Filter Logs by Project Space Context:", ["All"] + active_projects_list, key="evt_log_filter_project")
        filter_chiller = f_col2.selectbox("Filter Logs by Associated Chiller Asset:", ["All"] + active_chillers_list, key="evt_log_filter_chiller")
        
        try:
            where_clauses = []
            if filter_proj != "All":
                where_clauses.append(f"project_id = '{filter_proj}'")
            if filter_chiller != "All":
                where_clauses.append(f"chiller_id = '{filter_chiller}'")
                
            where_stmt = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
            
            # Simplified columns structure layout pulling strictly: Time, Project, Chiller Event
            logs_q = f"""
                SELECT FORMAT_TIMESTAMP('%m/%d/%Y %H:%M', event_timestamp) as Time,
                       project_id as Project,
                       event_description as `Chiller Event`
                FROM `{EVENTS_TABLE}`
                {where_stmt}
                ORDER BY event_timestamp DESC
                LIMIT 200
            """
            logs_df = client.query(logs_q).to_dataframe()
            if not logs_df.empty:
                st.dataframe(logs_df, use_container_width=True, hide_index=True)
            else:
                st.info("No historical event entries log files match your selected parameter options.")
        except Exception as e:
            st.caption(f"Log viewer pipeline suspended: {e}")

    # --- TAB 5: MASTER CHILLER INTERFACE CONTROLS ---
    with tab_chiller_reg:
        st.subheader("❄️ Chiller Infrastructure Master Control")
        
        # =====================================================================
        # SECTION 1: GLOBAL FLEET INVENTORY STATUS DISPLAY
        # =====================================================================
        st.write("#### 📂 Current Inventory of Chillers")
        
        try:
            # Enhanced query dynamically unpacking your newly introduced 'status' attribute
            inventory_q = f"""
                WITH TimelineState AS (
                    SELECT 
                        project_id, chiller_id, event_timestamp, event_description,
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
                        MAX(CASE WHEN is_on AND next_evt IS NULL THEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), event_timestamp, HOUR) END) as active_run_hours
                    FROM TimelineState
                    GROUP BY chiller_id
                )
                SELECT 
                    c.chiller_id, c.chiller_type, c.purchase_date, c.initial_price, c.acquired_status, COALESCE(c.status, 'Yard') as status,
                    COALESCE(d.current_location, 'Unassigned (Shop)') as current_location,
                    COALESCE(d.currently_chilling, FALSE) as is_chilling,
                    COALESCE(d.total_chill_hours, 0) as cumulative_hours,
                    COALESCE(d.active_run_hours, 0) as current_run_hours
                FROM `{CHILLER_REG_TABLE}` c
                LEFT JOIN Durations d ON c.chiller_id = d.chiller_id
                ORDER BY c.chiller_id ASC
            """
            inv_raw_df = client.query(inventory_q).to_dataframe()
            
            if not inv_raw_df.empty:
                inv_raw_df['Associated Costs'] = inv_raw_df['cumulative_hours'] * 4.50
                
                display_rows = []
                for _, r in inv_raw_df.iterrows():
                    status_flag = "🔵 Actively Running" if r['is_chilling'] else "⚪ Off/Standby"
                    duration_label = f"{int(r['current_run_hours'])}h ongoing" if r['is_chilling'] else f"{int(r['cumulative_hours'])}h total"
                    
                    display_rows.append({
                        "Chiller Name": r['chiller_id'],
                        "Asset Status": str(r['status']).upper(),
                        "Location Deployment": r['current_location'],
                        "Operational State": status_flag,
                        "Chill Duration": duration_label,
                        "Equipment Type": r['chiller_type'] if pd.notnull(r['chiller_type']) else "—",
                        "Date Acquired": pd.to_datetime(r['purchase_date']).date() if pd.notnull(r['purchase_date']) else "—",
                        "Condition": str(r['acquired_status']).upper() if pd.notnull(r['acquired_status']) else "NEW",
                        "Initial Cost": f"${float(r['initial_price']):,.2f}" if pd.notnull(r['initial_price']) else "$0.00",
                        "Operating Costs": f"${r['Associated Costs']:,.2f}"
                    })
                
                st.dataframe(pd.DataFrame(display_rows), use_container_width=True, hide_index=True)
            else:
                st.info("ℹ️ Fleet infrastructure database registry is currently unpopulated.")
        except Exception as e:
            st.error(f"⚠️ Inventory View Synchronization Error: {e}")

        st.divider()

        # =====================================================================
        # SECTION 2: DYNAMIC UPDATE STATUS & REGISTRATION CONTROL CONSOLE
        # =====================================================================
        st.write("#### ⚙️ Update Chiller Status & Asset Records")
        
        # Checkbox mode routing logic to easily swap form context
        is_brand_new_asset = st.checkbox("➕ Check this box to register a completely NEW chiller asset to the fleet", value=False)
        
        # Build master fleet arrays to fuel update dropdown targets
        fleet_options = sorted(inv_raw_df['chiller_id'].tolist()) if not inv_raw_df.empty else []
        
        with st.form("hardened_unified_chiller_asset_management_form"):
            if is_brand_new_asset or not fleet_options:
                # Fresh entry registration display mode
                st.info("Form mode: Registering a brand new hardware asset block to BigQuery.")
                target_c_name = st.text_input("Chiller Name / Unique Serial ID*", placeholder="e.g., CH-53-03")
                
                # Setup default base fields for fresh objects
                current_type_val = ""
                current_date_val = datetime.now().date()
                current_cost_val = 0.0
                current_cond_idx = 0
                current_stat_idx = 1  # Defaults new items to 'Yard'
            else:
                # Dynamic update selection menu mode
                target_c_name = st.selectbox("Select Target Chiller to Edit/Update*", options=fleet_options)
                
                # Fetch target record array variables directly from memory frame to populate context fields
                matched_record = inv_raw_df[inv_raw_df['chiller_id'] == target_c_name].iloc[0]
                
                current_type_val = matched_record['chiller_type'] if pd.notnull(matched_record['chiller_type']) else ""
                current_date_val = pd.to_datetime(matched_record['purchase_date']).date() if pd.notnull(matched_record['purchase_date']) else datetime.now().date()
                current_cost_val = float(matched_record['initial_price']) if pd.notnull(matched_record['initial_price']) else 0.0
                
                cond_str = str(matched_record['acquired_status']).upper()
                current_cond_idx = 1 if cond_str == "USED" else 0
                
                stat_str = str(matched_record['status']).strip().title()
                stat_options_list = ["On Project", "Yard", "Need Repair", "Scrap"]
                current_stat_idx = stat_options_list.index(stat_str) if stat_str in stat_options_list else 1

            # Input form matrix layout fields
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
                    st.error("❌ Action Rejected: Chiller identifier name/token string cannot be blank.")
                else:
                    safe_cid = target_c_name.strip().replace("'", "''")
                    safe_type = f_type.strip().replace("'", "''")
                    
                    if is_brand_new_asset:
                        # Transaction block routing pathway A: Insert fresh records
                        execution_sql = f"""
                            INSERT INTO `{CHILLER_REG_TABLE}` (chiller_id, chiller_type, purchase_date, initial_price, acquired_status, status)
                            VALUES ('{safe_cid}', '{safe_type}', DATE('{f_acquired.strftime('%Y-%m-%d')}'), {float(f_price)}, '{f_condition.lower()}', '{f_status}')
                        """
                        success_alert = f"🎉 Success! Asset block **{safe_cid}** has been written to the production registry."
                    else:
                        # Transaction block routing pathway B: Rewrite existing fields
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
# Page: Admin Tool Helpers  #
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
    """Main administrative execution module managing bulk data approval modification routines."""
    target_table = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections" 
    telemetry_table = f"{PROJECT_ID}.{DATASET_ID}.master_data_view" 

    st.title("⚡ Bulk Approval and Database Maintenance")
    st.divider()

    if "blk_mgmt_profile_df" not in st.session_state: st.session_state.blk_mgmt_profile_df = None
    if "blk_mgmt_total_points" not in st.session_state: st.session_state.blk_mgmt_total_points = 0

    # =========================================================================
    # UTILITY A: GLOBAL DATABASE CLEANUP ENGINE
    # =========================================================================
    st.header("🧹 Global Database Cleanup")
    st.write(
        "Consolidate raw datasets into 1-decimal hourly averages and safely remove all duplicate records system-wide. "
        "**Note:** Running this cleanup automatically marks any rogue data points outside the physical bounds of -30°F and 120°F as `BadData`."
    )
    
    if st.button("⚡ Run Global Database Cleanup & Duplicate Purge", use_container_width=True):
        status_box = st.empty()
        try:
            status_box.markdown("⏳ **[1/4] Calculating initial database row baselines...**")
            count_sp_before = client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`").to_dataframe().iloc[0, 0]
            count_lord_before = client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`").to_dataframe().iloc[0, 0]
            
            status_box.markdown("🧹 **[2/4] Initializing temporary staging pools and filtering SensorPush duplicates...**")
            sp_cleanup_sql = f"""
                CREATE OR REPLACE TEMP TABLE tmp_clean_sensorpush AS
                SELECT timestamp, NodeNum, ROUND(CAST(temperature AS NUMERIC), 1) as temperature, rssi
                FROM (
                    SELECT *, ROW_NUMBER() OVER(PARTITION BY NodeNum, timestamp ORDER BY timestamp DESC) as rn
                    FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
                    WHERE temperature >= -30.0 AND temperature <= 120.0
                )
                WHERE rn = 1;

                CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` AS
                SELECT * FROM tmp_clean_sensorpush;
            """
            client.query(sp_cleanup_sql).result()
            
            status_box.markdown("🛰️ **[3/4] Running row-deduplication matrices on Lord Wireless tables...**")
            lord_cleanup_sql = f"""
                CREATE OR REPLACE TEMP TABLE tmp_clean_lord AS
                SELECT timestamp, NodeNum, ROUND(CAST(temperature AS NUMERIC), 1) as temperature
                FROM (
                    SELECT timestamp, NodeNum, temperature,
                           ROW_NUMBER() OVER(PARTITION BY NodeNum, timestamp ORDER BY timestamp DESC) as rn
                    FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
                    WHERE CAST(temperature AS NUMERIC) >= -30.0 AND CAST(temperature AS NUMERIC) <= 120.0
                )
                WHERE rn = 1;

                CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.raw_lord` AS
                SELECT * FROM tmp_clean_lord;
            """
            client.query(lord_cleanup_sql).result()
            st.cache_data.clear()

            status_box.markdown("📊 **[4/4] Finalizing database overwrites and pulling post-cleanup tallies...**")
            count_sp_after = client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`").to_dataframe().iloc[0, 0]
            count_lord_after = client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`").to_dataframe().iloc[0, 0]

            sp_removed = count_sp_before - count_sp_after
            lord_removed = count_lord_before - count_lord_after
            total_removed = sp_removed + lord_removed
            
            status_box.empty()
            st.success("🎉 Global Database Cleanup successfully completed!")
            
            st.markdown("### 📊 Before vs. After Summary Ledger")
            report_data = [
                {"Data Table": "SensorPush (raw_sensorpush)", "Before Count": f"{count_sp_before:,}", "After Count": f"{count_sp_after:,}", "Purged Points": f"{sp_removed:,}"},
                {"Data Table": "Lord Wireless (raw_lord)", "Before Count": f"{count_lord_before:,}", "After Count": f"{count_lord_after:,}", "Purged Points": f"{lord_removed:,}"},
                {"Data Table": "Combined Total Pool", "Before Count": f"{count_sp_before + count_lord_before:,}", "After Count": f"{count_sp_after + count_lord_after:,}", "Purged Points": f"{total_removed:,}"}
            ]
            st.dataframe(pd.DataFrame(report_data), use_container_width=True, hide_index=True)
            
        except Exception as e:
            status_box.empty()
            st.error(f"Global Database Cleanup Failed: {e}")

    st.divider()

    # =========================================================================
    # UTILITY B: BULK APPROVAL AND DATA STATUS CHANGE SYSTEM CONTROLS
    # =========================================================================
    st.header("⚡ Bulk Approval and Data Status Change")
    st.info("💡 **Important:** Please ensure you have selected your targeted project framework or 'All Projects' in the sidebar menu before applying any status overrides.")
    
    target_scope, current_status_filter, new_status = render_bulk_approval_controls()
    st.divider()

    filters = render_bulk_approval_filters(full_reg_df, selected_project, target_scope)
    where_str = build_bulk_approval_where_clause(full_reg_df, selected_project, target_scope, current_status_filter, filters)
    
    aliased_where = (where_str.replace("NodeNum", "t.NodeNum")
                              .replace("timestamp", "t.timestamp")
                              .replace("temperature", "t.temperature")
                              .replace("r.approve", "t.approval_status"))
    
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

    if st.button("🔍 Step 1: Verify Match Count & Current Status Profiles", key="blk_mgmt_verify_btn", use_container_width=True):
        try:
            run_profile_audit()
        except Exception as e:
            st.error(f"Verification Matrix Compilation Failed: {e}")

    if st.session_state.blk_mgmt_profile_df is not None:
        if not st.session_state.blk_mgmt_profile_df.empty:
            st.subheader("📊 Current Node Status")
            st.dataframe(st.session_state.blk_mgmt_profile_df, use_container_width=True, hide_index=True)
            st.metric("Total Consolidated Points in Selection Scope", f"{st.session_state.blk_mgmt_total_points:,}")
        else:
            st.warning("No telemetry data points found matching this configuration window.")

    st.divider()
    st.info(f"Target Designation Status for selected coordinates: **{new_status}**")
    if st.checkbox("I authorize updating these data markers to the target parameters specified.", key="confirm_blk_mgmt"):
        if st.button(f"🚀 Step 2: Execute Status Override to {new_status}", key="exec_blk_mgmt_btn", use_container_width=True):
            if new_status == "TRUE":
                sql = f"""
                    DELETE FROM `{target_table}`
                    WHERE STRUCT(NodeNum, timestamp) IN (
                        SELECT AS STRUCT t.NodeNum, t.timestamp 
                        FROM `{telemetry_table}` t
                        WHERE {aliased_where}
                    )
                """
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
                        UPDATE SET approve = '{new_status}'
                    WHEN NOT MATCHED THEN
                        INSERT (NodeNum, timestamp, approve) 
                        VALUES (S.NodeNum, S.timestamp, '{new_status}')
                """
            try:
                with st.spinner("Processing database status reclassifications..."):
                    job = client.query(sql)
                    job.result()
                
                st.success(f"✅ Reclassification successful! Updated {job.num_dml_affected_rows:,} records inside the registry ledger.")
                st.cache_data.clear()
                run_profile_audit()
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
          UPDATE SET approve = '{new_status}'
        WHEN NOT MATCHED THEN
          INSERT (NodeNum, timestamp, approve) 
          VALUES (S.NodeNum, S.timestamp, '{new_status}')
    """
    try:
        client.query(write_q).result()
        return True
    except Exception as e:
        st.error(f"⚠️ Cloud DB Commit Failed: {e}")
        return False

# =============================================================================
# WORKER UTILITIES: NODE LOGISTICS ENGINE (FROM TOOLS)
# =============================================================================

def render_node_action_manager(client, selected_node_data, reg_df, proj_list, target_registry):
    """Renders editing form panels and handles transactional database appends."""
    st.markdown(f"### ⚙️ Operational Settings Editor: **{selected_node_data.get('NodeNum')}**")
    
    current_project = str(selected_node_data.get('Project', 'Office'))
    current_location = str(selected_node_data.get('Location', ''))
    current_bank = str(selected_node_data.get('Bank', 'A'))
    current_depth = float(selected_node_data.get('Depth', 0.0))
    current_status = str(selected_node_data.get('SensorStatus', 'On Project'))
    
    raw_date = selected_node_data.get('Start_Date')
    if isinstance(raw_date, (datetime, date)):
        current_start_date = raw_date
    else:
        try: current_start_date = pd.to_datetime(raw_date).date()
        except Exception: current_start_date = datetime.now().date()

    edit_c1, edit_c2, edit_c3 = st.columns(3)
    
    with edit_c1:
        raw_projects = reg_df['Project'].dropna().unique().tolist() if 'reg_df' in locals() else []
        u_projects = sorted(list(set(["Office"] + raw_projects)))
        new_node_project = st.selectbox(
            "Target Allocation Project:", 
            options=u_projects, 
            index=u_projects.index(current_project) if current_project in u_projects else 0
        )
        new_node_location = st.text_input("Target Allocation Location / Borehole:", value=current_location)

    with edit_c2:
        bank_options = ["A", "B", "C", "D", "E", "X"]
        new_node_bank = st.selectbox(
            "Bank Designation String:", 
            options=bank_options, 
            index=bank_options.index(current_bank) if current_bank in bank_options else 0
        )
        new_node_depth = st.number_input(
            "Sensor Vertical Placement Depth (Feet):", 
            value=float(current_depth), step=1.0, format="%.2f"
        )

    with edit_c3:
        status_options = ["On Project", "In Office/Shop", "Decommissioned", "Spare/Storage"]
        new_node_status = st.selectbox(
            "Operational Tracking Status:", 
            options=status_options, 
            index=status_options.index(current_status) if current_status in status_options else 0
        )
        new_node_start_date = st.date_input("Deployment Modification Effective Date:", value=current_start_date)

    st.markdown("#### 🚀 Step 3: Authorize Change Record")
    with st.expander("⚠️ View Registry Transaction Script Actions"):
        st.write(
            f"Executing this deployment write adds a tracking line into `{target_registry}` mapping "
            f"**{selected_node_data.get('NodeNum')}** to Project **{new_node_project}** at depth **{new_node_depth} ft** "
            f"effective **{new_node_start_date.strftime('%m/%d/%Y')}**."
        )
        
        if st.checkbox("I verify that these field allocation parameters match our physical sensor logs.", key="confirm_node_logistics_action_write"):
            if st.button(f"💾 Append Deployment Update for {selected_node_data.get('NodeNum')}", type="primary", use_container_width=True):
                new_logistics_payload = [{
                    "NodeNum": str(selected_node_data.get('NodeNum')).strip(),
                    "Project": str(new_node_project).strip(),
                    "Location": str(new_node_location).strip(),
                    "Bank": str(new_node_bank).strip().upper(),
                    "Depth": float(new_node_depth),
                    "Start_Date": str(new_node_start_date.strftime('%Y-%m-%d')), # Standardized date format string pass
                    "SensorStatus": str(new_node_status).strip()
                }]
                
                try:
                    with st.spinner("Appending tracking metrics to node registry matrix..."):
                        job_config = bigquery.LoadJobConfig(
                            schema=[
                                bigquery.SchemaField("NodeNum", "STRING"),
                                bigquery.SchemaField("Project", "STRING"),
                                bigquery.SchemaField("Location", "STRING"),
                                bigquery.SchemaField("Bank", "STRING"),
                                bigquery.SchemaField("Depth", "FLOAT"),
                                bigquery.SchemaField("Start_Date", "DATE"),
                                bigquery.SchemaField("SensorStatus", "STRING"),
                            ],
                            write_disposition="WRITE_APPEND"
                        )
                        log_df = pd.DataFrame(new_logistics_payload)
                        client.load_table_from_dataframe(log_df, target_registry, job_config=job_config).result()
                        
                    st.success(f"🎉 Success! Asset registry mapping updated for node {selected_node_data.get('NodeNum')}.")
                    st.cache_data.clear()
                    time.sleep(1.0)
                    st.rerun()
                except Exception as log_err:
                    st.error(f"❌ Failed to commit node asset updates to registry table: {log_err}")


def render_data_checker(client, full_reg_df):
    """Renders a quality assurance diagnostics matrix highlighting configuration or orphan risks."""
    st.divider()
    st.markdown("### 🔍 System Registry Diagnostics Audit")
    
    with st.expander("📊 View Discovered Inventory Conflict Logs", expanded=False):
        try:
            orphan_q = f"""
                SELECT DISTINCT r.NodeNum, r.Project, r.Location 
                FROM `{PROJECT_ID}.{DATASET_ID}.node_registry` r
                LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.hardware_inventory` i 
                  ON TRIM(r.NodeNum) = TRIM(i.NodeNum)
                WHERE i.NodeNum IS NULL AND r.NodeNum IS NOT NULL
                ORDER BY r.NodeNum ASC
            """
            orphan_df = client.query(orphan_q).to_dataframe()
            if not orphan_df.empty:
                st.warning("⚠️ **Orphan Sensor Alert:** The following node tags exist in deployment schedules but lack hardware index keys:")
                st.dataframe(orphan_df, use_container_width=True, hide_index=True)
            else:
                st.success("✅ All registered node mappings align cleanly with the Hardware Inventory catalog.")
        except Exception as e:
            st.caption(f"Integrity check skipped or loading: {e}")


# =============================================================================
# DATA RECOVERY REQUISITE ENGINE HELPERS
# =============================================================================

def render_recovery_filters(sp_reg):
    """Renders hierarchical dropdown blocks and returns selected Node IDs."""
    st.subheader("🔍 Select Target Hardware")
    col_f1, col_f2, col_f3 = st.columns(3)
    
    with col_f1:
        u_projects = ["All"] + sorted(sp_reg['Project'].dropna().unique().tolist())
        rec_proj = st.selectbox("Filter by Project Space Context:", u_projects, key="rec_proj_sel_isolated")
    
    proj_filtered = sp_reg if rec_proj == "All" else sp_reg[sp_reg['Project'] == rec_proj]
    
    with col_f2:
        u_locs = ["All"] + sorted(proj_filtered['Location'].dropna().unique().tolist(), key=natural_sort_key)
        rec_loc = st.selectbox("Filter by Physical Location Context:", u_locs, key="rec_loc_sel_isolated")
        
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


def handle_recovery_trigger(selected_nodes, start_date, end_date):
    """Manages the cloud pipeline to execute a raw data recovery dump into the database table via fast batch loads."""
    import requests
    import numpy as np
    
    all_rows = []
    hardware_map = {}
    db_max_timestamps = {}
    node_stats = {}
    account_stats = {}

    LOCAL_REC_TABLE = "raw_sensorpush"
    LOCAL_INV_TABLE = "hardware_inventory"
    LOCAL_API_URL = "https://api.sensorpush.com/api/v1"

    ACCOUNTS = [
        {'email': 'ldunham@soilfreeze.com', 'password': 'Freeze123!!'},
        {'email': 'tsteele@soilfreeze.com', 'password': 'Freeze123!!'},
        {'email': 'soilfreeze98072@gmail.com', 'password': 'Freeze123!!'}
    ]

    start_time_iso = datetime.combine(start_date, datetime.min.time()).strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time_iso = datetime.combine(end_date, datetime.max.time()).strftime('%Y-%m-%dT%H:%M:%SZ')

    # Seed stats for tracking visual layout fields
    if selected_nodes:
        for node in selected_nodes:
            node_stats[node] = 0

    with st.status("Executing Cloud Backfill Ingestion Pipeline Run...", expanded=True) as status_box:
        st.write("🔍 Extracting Translation Mappings from Hardware Inventory...")
        try:
            inv_q = f"SELECT RawID, NodeNum FROM `{PROJECT_ID}.{DATASET_ID}.{LOCAL_INV_TABLE}` WHERE RawID IS NOT NULL"
            db_client = get_bq_client()
            for row in db_client.query(inv_q):
                clean_db_id = str(row.RawID).split('.')[0].strip()
                friendly_name = str(row.NodeNum).strip()
                hardware_map[clean_db_id] = friendly_name
                if not selected_nodes:
                    node_stats[friendly_name] = 0
        except Exception as e:
            st.error(f"Failed to query inventory map tables: {e}")
            st.stop()

        # --- PRE-FLIGHT CHECK: EXTRACT LAST SEEN TIMESTAMPS FROM MASTER VIEW ---
        st.write("📅 Checking historical system check-in history benchmarks...")
        try:
            time_q = f"""
                SELECT NodeNum, FORMAT_TIMESTAMP('%m/%d/%Y %H:%M UTC', MAX(timestamp)) as max_time 
                FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` 
                GROUP BY NodeNum
            """
            for row in db_client.query(time_q):
                if row.max_time:
                    db_max_timestamps[str(row.NodeNum)] = str(row.max_time)
        except Exception as e:
            st.warning(f"Could not calculate maximum timelines: {e}")

        for acc in ACCOUNTS:
            st.write(f"🔐 Authenticating token profile for `{acc['email']}`...")
            account_stats[acc['email']] = 0
            
            try:
                auth_r = requests.post(f"{LOCAL_API_URL}/oauth/authorize", json=acc, timeout=15).json()
                token = requests.post(f"{LOCAL_API_URL}/oauth/accesstoken", json={"authorization": auth_r['authorization']}, timeout=15).json().get('accesstoken')
                
                s_resp = requests.post(f"{LOCAL_API_URL}/devices/sensors", headers={"Authorization": token}, json={}, timeout=20).json()
                device_rssi_map = {}
                if isinstance(s_resp, dict):
                    for s_id, s_meta in s_resp.items():
                        if isinstance(s_meta, dict) and 'rssi' in s_meta:
                            device_rssi_map[str(s_id).strip()] = s_meta.get('rssi')

                st.write(f"📥 Pulling raw cloud payload matrix for `{acc['email']}`...")
                samples_payload = {"startTime": start_time_iso, "endTime": end_time_iso, "limit": 10000}
                r_samples = requests.post(f"{LOCAL_API_URL}/samples", headers={"Authorization": token}, json=samples_payload, timeout=60).json()
                
                sensors_data = r_samples.get('sensors', {})
                if not sensors_data:
                    continue

                for s_id, samples in sensors_data.items():
                    api_root_id = str(s_id).split('.')[0].strip()
                    friendly_name = hardware_map.get(api_root_id)
                    
                    if not friendly_name:
                        friendly_name = f"UNMAPPED-{api_root_id}"
                        if friendly_name not in node_stats:
                            node_stats[friendly_name] = 0
                        
                    if selected_nodes and friendly_name not in selected_nodes:
                        continue
                        
                    current_device_rssi = device_rssi_map.get(str(s_id).strip())
                    
                    for s in samples:
                        temp = s.get('temp_f') or s.get('temperature') or s.get('thermocouple_temperature')
                        if temp is not None:
                            account_stats[acc['email']] += 1
                            
                            all_rows.append({
                                "timestamp": pd.to_datetime(s['observed']),
                                "NodeNum": str(friendly_name),
                                "temperature": float(temp),
                                "rssi": float(current_device_rssi) if current_device_rssi is not None else None
                            })
            except Exception:
                continue

        # Unified Batch Ingestion Layer (Safely placed outside account loops)
        # Unified Batch Ingestion Layer (Safely placed outside account loops)
        total_recovered_appends = len(all_rows)
        if total_recovered_appends == 0:
            st.info("🔒 Cloud accounts returned 0 points for this window context.")
            status_box.update(label="Run Finalized (0 Points Found)", state="complete")
        else:
            st.write(f"📥 Batch loading rows straight into `{LOCAL_REC_TABLE}`...")
            try:
                upload_df = pd.DataFrame(all_rows)
                
                # 🛡️ HARDENED FIX: Convert to explicit timezone-aware datetimes so pyarrow passes accurate offsets to BigQuery
                upload_df['timestamp'] = pd.to_datetime(upload_df['timestamp'], utc=True)
                
                # Force numerical data types to match schema layouts exactly
                if 'rssi' in upload_df.columns:
                    upload_df['rssi'] = pd.to_numeric(upload_df['rssi'], errors='coerce').astype(object).where(upload_df['rssi'].notnull(), None)
                if 'temperature' in upload_df.columns:
                    upload_df['temperature'] = pd.to_numeric(upload_df['temperature'], errors='coerce')
                
                upload_df['NodeNum'] = upload_df['NodeNum'].astype(str).str.strip()

                real_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{LOCAL_REC_TABLE}"
                
                # 🛡️ HARDENED FIX: Explicitly define schema constraints for the load job configuration
                job_config = bigquery.LoadJobConfig(
                    schema=[
                        bigquery.SchemaField("timestamp", "TIMESTAMP"),
                        bigquery.SchemaField("NodeNum", "STRING"),
                        bigquery.SchemaField("temperature", "FLOAT"),
                        bigquery.SchemaField("rssi", "FLOAT"),
                    ],
                    write_disposition="WRITE_APPEND"
                )
                
                client.load_table_from_dataframe(upload_df, real_table_ref, job_config=job_config).result()
                
                st.success(f"🎉 Success! Appended {total_recovered_appends:,} raw rows to storage.")
                summary_line = " | ".join([f"**{email}**: {count:,} pts" for email, count in account_stats.items()])
                st.markdown(f"📥 **Account Run Summary Logs:** {summary_line}")
                status_box.update(label="Recovery Dump Complete!", state="complete")
                st.cache_data.clear()
            except Exception as bq_err:
                st.error(f"Batch loading Ingestion pipeline failure: {bq_err}")
                status_box.update(state="error")

    
    # --- RENDER STATISTICAL BREAKDOWN GRID WITH LAST SEEN BENCHMARKS ---
    if node_stats:
        st.write("### 📊 Data Recovery Tally Distribution:")
        summary_records = []
        grand_total_tally = 0
        
        nodes_to_report = selected_nodes if selected_nodes else sorted(list(node_stats.keys()))
        
        for node in nodes_to_report:
            if node not in node_stats:
                continue
                
            true_node_count = sum(1 for row in all_rows if row["NodeNum"] == node) if total_recovered_appends > 0 else 0
            grand_total_tally += true_node_count
            last_checked_in = db_max_timestamps.get(node, "❌ No Historical Records Found")
            
            summary_records.append({
                "Node Number": node,
                "Last Database Check-In": last_checked_in,
                "Points Extracted & Appended": true_node_count
            })
            
        summary_df = pd.DataFrame(summary_records).sort_values(by="Node Number")
        
        total_row = pd.DataFrame([{
            "Node Number": "🧮 Combined Total Pool",
            "Last Database Check-In": "—",
            "Points Extracted & Appended": grand_total_tally
        }])
        summary_df = pd.concat([summary_df, total_row], ignore_index=True)
        
        st.dataframe(summary_df, use_container_width=True, hide_index=True)
        if total_recovered_appends > 0:
            st.balloons()

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
    target_registry_path = f"{PROJECT_ID}.{DATASET_ID}.node_registry"
    try:
        # Single-pass database join with real-time latency calculations for the grids
        full_reg_df = load_lab_node_registry_data(target_registry_path)
        
        proj_reg_q = f"SELECT Project FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE ProjectStatus != 'Archived'"
        available_projects_list = sorted(client.query(proj_reg_q).to_dataframe()['Project'].dropna().unique().tolist())
    except Exception as e:
        st.error(f"Registry Link Offline: {e}")
        return

    # 2. NAVIGATION TABS (Aligned matching your exact blueprint)
    tab_admin_sum, tab_bulk_app, tab_logistics, tab_recovery, tab_proj_master, tab_bulk_config, tab_chillers = st.tabs([
        "📋 Admin Summary", 
        "⚡ Bulk Approval", 
        "📋 Node Master",  
        "📡 Data Recovery", 
        "⚙️ Project Master", 
        "📦 Bulk Uploads",
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
            fleet_df['is_active'] = fleet_df['End_Date'].isna()
            
            deduped_units = fleet_df.sort_values(by=['Parent ID', 'is_active'], ascending=[True, False]).drop_duplicates(subset=['Parent ID']).copy()
            
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
                FROM `{PROJECT_ID}.{DATASET_ID}.node_registry` n
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
            FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` p
            LEFT JOIN Metrics m ON p.Project = m.Project
            WHERE p.ProjectStatus IN ('Freezedown', 'Maintenance', 'Pre-freeze')
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
                FROM `{PROJECT_ID}.{DATASET_ID}.node_registry`
                WHERE End_Date IS NULL
                GROUP BY Project
            )
            SELECT 
                p.Project as `Project ID`,
                COALESCE(p.ProjectName, p.Project) as `Project Name`,
                p.ProjectStatus as `Project Status`,
                COALESCE(n.Nodes_Assigned, 0) as `Sensors Assigned`
            FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` p
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

    # --- SUB-TAB 2: BULK APPROVAL ---
    with tab_bulk_app:
        execute_bulk_approval_workspace(client, full_reg_df, selected_project, tab_logistics)
        
    # =========================================================================
    # SUB-TAB 3: NODE MASTER
    # =========================================================================
    with tab_logistics:
        st.title("📋 Node Status and Changes")
        st.write("Manage active asset configurations, update field deployment depths, or reassign operational node locations.")
        st.divider()
        
        st.subheader("🔍 Select Target Hardware Path")
        
        # 1. CASCADING SELECTBOX CONTROLS
        col_l1, col_l2, col_l3 = st.columns(3)
        
        with col_l1:
            raw_projects = full_reg_df['Project'].dropna().unique().tolist() if not full_reg_df.empty else []
            u_projects = sorted(list(set(["Office"] + raw_projects)))
            selected_log_proj = st.selectbox("Select Project Space Context:", u_projects, key="node_log_project_filter")
        
        # Isolate rows matching project context
        proj_filtered_df = full_reg_df[full_reg_df['Project'] == selected_log_proj] if not full_reg_df.empty else pd.DataFrame()
        
        with col_l2:
            u_locations = sorted(proj_filtered_df['Location'].dropna().unique().tolist(), key=natural_sort_key) if not proj_filtered_df.empty else []
            if not u_locations:
                u_locations = ["Office"]
                
            location_options = u_locations + ["➕ Add New Location..."]
            selected_log_loc = st.selectbox("Select Physical Location Context:", location_options, key="node_log_location_filter")
            
        loc_filtered_df = proj_filtered_df[proj_filtered_df['Location'] == selected_log_loc] if not proj_filtered_df.empty else pd.DataFrame()
        
        with col_l3:
            node_lookup_df = loc_filtered_df if selected_log_loc != "➕ Add New Location..." else proj_filtered_df
            u_nodes = sorted(node_lookup_df['NodeNum'].dropna().unique().tolist(), key=natural_sort_key) if not node_lookup_df.empty else []
            selected_log_node = st.selectbox(
                "Select Target Node Number ID:", 
                u_nodes, 
                index=0 if u_nodes else None,
                key="node_log_node_filter"
            )

        st.divider()

        # 2. RENDER THE HISTORICAL TIMELINE EXTRACTION GRID
        if selected_log_node:
            history_query = f"""
                SELECT *, 
                       CAST(Start_Date AS STRING) as start_date_str,
                       COALESCE(CAST(End_Date AS STRING), 'Active') as end_date_str
                FROM `{target_registry_path}`
                WHERE NodeNum = '{selected_log_node}'
                ORDER BY Start_Date DESC
            """
            try:
                raw_node_history_df = client.query(history_query).to_dataframe()
            except Exception as e:
                st.error(f"Error reading asset timeline lines: {e}")
                raw_node_history_df = pd.DataFrame()

            if not raw_node_history_df.empty:
                st.markdown(f"### 🕒 Assignment History Timeline: **{selected_log_node}**")
                st.caption("💡 **Tip:** Use the checkbox selector in the tracking table below to force loading an archived historical row entry context into the editor panel form matrix.")
                
                hist_ed_key = f"hist_ed_{selected_log_node}"
                if f"active_hist_idx_{selected_log_node}" not in st.session_state:
                    st.session_state[f"active_hist_idx_{selected_log_node}"] = 0

                display_history_df = raw_node_history_df.copy()
                display_history_df.insert(0, "Select", False)
                
                curr_active_idx = st.session_state[f"active_hist_idx_{selected_log_node}"]
                if curr_active_idx < len(display_history_df):
                    display_history_df.loc[curr_active_idx, "Select"] = True

                if hist_ed_key in st.session_state and "edited_rows" in st.session_state[hist_ed_key]:
                    user_changes = st.session_state[hist_ed_key]["edited_rows"]
                    clicked_indices = [int(idx) for idx, changes in user_changes.items() if changes.get("Select") == True]
                    if clicked_indices:
                        st.session_state[f"active_hist_idx_{selected_log_node}"] = clicked_indices[-1]
                        st.session_state[hist_ed_key]["edited_rows"] = {}
                        st.rerun()

                st.data_editor(
                    display_history_df,
                    hide_index=True,
                    use_container_width=True,
                    column_config={
                        "Select": st.column_config.CheckboxColumn("Select", default=False),
                        "start_date_str": "Start Date",
                        "end_date_str": "End Date",
                        "Depth": "Depth",
                        "SensorStatus": "Sensor Status"
                    },
                    disabled=[c for c in display_history_df.columns if c != "Select"],
                    column_order=["Select", "Project", "Location", "Bank", "Depth", "start_date_str", "end_date_str", "SensorStatus"],
                    key=hist_ed_key
                )

                target_selected_idx = st.session_state[f"active_hist_idx_{selected_log_node}"]
                if target_selected_idx >= len(raw_node_history_df):
                    target_selected_idx = 0
                
                chosen_target_record = raw_node_history_df.iloc[target_selected_idx].to_dict()

                st.divider()
                
                # 3. CALL ACTION FORM HANDLER COMPONENT WITH CASCADING VARIABLES
                try:
                    render_lab_node_action_manager(
                        client=client,
                        selected_node_data=chosen_target_record,
                        reg_df=full_reg_df,
                        proj_list=u_projects,
                        known_project_locations=u_locations, 
                        target_registry=target_registry_path
                    )
                    render_lab_data_checker(client, full_reg_df)
                except Exception as routing_err:
                    st.error(f"Internal workspace linkage failed: {routing_err}")
            else:
                st.info("No prior deployment entries found tracked for this hardware tracking context.")
        else:
            st.info("💡 Please specify a valid Project, Location, and Node path above to populate management components.")
            
    # -------------------------------------------------------------------------
    # SUB-TAB 4: DATA RECOVERY PIPELINE ENGINE
    # -------------------------------------------------------------------------
    with tab_recovery:
        st.title("📡 Data Recovery Engine")
        st.write(
            "Extract raw chronological data streams directly from the SensorPush Cloud API architecture "
            "and execute a direct batch-load insert into your primary production table layers."
        )
        st.divider()

        # 1. RENDER STREAMLINED HIERARCHICAL SEARCH DROPDOWNS
        # Collects chosen node indices contextually from Project Space and Location selections
        dropdown_selected_nodes = render_recovery_filters(full_reg_df)

        st.divider()

        # 2. DEFINE TIMELINE RECOVERY CONTROLS
        st.subheader("📅 Define Recovery Timeline Parameters")
        rec_c1, rec_c2 = st.columns(2)
        with rec_c1:
            rec_start_date = st.date_input("Extraction Window Start Date", value=datetime.now().date() - timedelta(days=2), key="dt_rec_start")
        with rec_c2:
            rec_end_date = st.date_input("Extraction Window End Date", value=datetime.now().date(), key="dt_rec_end")

        st.divider()

        # 3. CONTEXTUAL DETERMINATION OF TARGET HARDWARE SCOPE
        # If specific nodes aren't selected in the multiselect box, fallback to all nodes matching the dropdown choices
        if dropdown_selected_nodes:
            final_target_nodes = dropdown_selected_nodes
        else:
            # Reconstruct dropdown slice filter criteria dynamically to prevent extraction drops
            active_proj_context = st.session_state.get('rec_proj_sel_isolated', 'All')
            active_loc_context = st.session_state.get('rec_loc_sel_isolated', 'All')
            
            slice_df = full_reg_df.copy()
            if active_proj_context != "All":
                slice_df = slice_df[slice_df['Project'] == active_proj_context]
            if active_loc_context != "All":
                slice_df = slice_df[slice_df['Location'] == active_loc_context]
                
            final_target_nodes = sorted(slice_df['NodeNum'].dropna().unique().tolist())

        # 4. SELECTION METRIC WARNING BANNER
        scope_text = f"{len(final_target_nodes)} selected nodes" if final_target_nodes else "ALL registered fleet nodes"
        st.warning(f"⚠️ **Action Required:** Initiating backfill protocol for {scope_text} from **{rec_start_date}** through **{rec_end_date}**.")

        # 5. TRIGGER EXECUTION PIPELINE BUTTON
        if st.button("🚀 Execute Cloud Backfill Ingestion Pipeline Run", use_container_width=True, key="btn_trigger_recovery_run"):
            
            # --- START HARDENED WORKER LOGIC ---
            import requests
            import numpy as np
            
            all_rows = []
            hardware_map = {}
            db_max_timestamps = {}
            node_stats = {}
            account_stats = {}

            LOCAL_REC_TABLE = "raw_sensorpush"
            LOCAL_INV_TABLE = "hardware_inventory"
            LOCAL_API_URL = "https://api.sensorpush.com/api/v1"

            ACCOUNTS = [
                {'email': 'ldunham@soilfreeze.com', 'password': 'Freeze123!!'},
                {'email': 'tsteele@soilfreeze.com', 'password': 'Freeze123!!'},
                {'email': 'soilfreeze98072@gmail.com', 'password': 'Freeze123!!'}
            ]

            start_time_iso = datetime.combine(rec_start_date, datetime.min.time()).strftime('%Y-%m-%dT%H:%M:%SZ')
            end_time_iso = datetime.combine(rec_end_date, datetime.max.time()).strftime('%Y-%m-%dT%H:%M:%SZ')

            # Seed data status tracker parameters
            if final_target_nodes:
                for node in final_target_nodes:
                    node_stats[node] = 0

            with st.status("Executing Cloud Backfill Ingestion Pipeline Run...", expanded=True) as status_box:
                st.write("🔍 Extracting Translation Mappings from Hardware Inventory...")
                try:
                    inv_q = f"SELECT RawID, NodeNum FROM `{PROJECT_ID}.{DATASET_ID}.{LOCAL_INV_TABLE}` WHERE RawID IS NOT NULL"
                    for row in client.query(inv_q):
                        clean_db_id = str(row.RawID).split('.')[0].strip()
                        friendly_name = str(row.NodeNum).strip()
                        hardware_map[clean_db_id] = friendly_name
                        if not final_target_nodes:
                            node_stats[friendly_name] = 0
                except Exception as e:
                    st.error(f"Failed to query inventory map tables: {e}")
                    st.stop()

                # --- PRE-FLIGHT CHECK: EXTRACT LAST SEEN TIMESTAMPS ---
                st.write("📅 Checking historical system check-in history benchmarks...")
                try:
                    time_q = f"SELECT NodeNum, FORMAT_TIMESTAMP('%m/%d/%Y %H:%M UTC', MAX(timestamp)) as max_time FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` GROUP BY NodeNum"
                    for row in client.query(time_q):
                        if row.max_time:
                            db_max_timestamps[str(row.NodeNum)] = str(row.max_time)
                except Exception as e:
                    st.warning(f"Could not calculate maximum timelines: {e}")

                for acc in ACCOUNTS:
                    st.write(f"🔐 Authenticating token profile for `{acc['email']}`...")
                    account_stats[acc['email']] = 0
                    
                    try:
                        auth_r = requests.post(f"{LOCAL_API_URL}/oauth/authorize", json=acc, timeout=15).json()
                        token = requests.post(f"{LOCAL_API_URL}/oauth/accesstoken", json={"authorization": auth_r['authorization']}, timeout=15).json().get('accesstoken')
                        
                        s_resp = requests.post(f"{LOCAL_API_URL}/devices/sensors", headers={"Authorization": token}, json={}, timeout=20).json()
                        device_rssi_map = {}
                        if isinstance(s_resp, dict):
                            for s_id, s_meta in s_resp.items():
                                if isinstance(s_meta, dict) and 'rssi' in s_meta:
                                    device_rssi_map[str(s_id).strip()] = s_meta.get('rssi')

                        st.write(f"📥 Pulling raw cloud payload matrix for `{acc['email']}`...")
                        samples_payload = {"startTime": start_time_iso, "endTime": end_time_iso, "limit": 10000}
                        r_samples = requests.post(f"{LOCAL_API_URL}/samples", headers={"Authorization": token}, json=samples_payload, timeout=60).json()
                        
                        sensors_data = r_samples.get('sensors', {})
                        if not sensors_data:
                            continue

                        for s_id, samples in sensors_data.items():
                            api_root_id = str(s_id).split('.')[0].strip()
                            friendly_name = hardware_map.get(api_root_id)
                            
                            if not friendly_name:
                                friendly_name = f"UNMAPPED-{api_root_id}"
                                if friendly_name not in node_stats:
                                    node_stats[friendly_name] = 0
                                
                            if final_target_nodes and friendly_name not in final_target_nodes:
                                continue
                                
                            current_device_rssi = device_rssi_map.get(str(s_id).strip())
                            
                            for s in samples:
                                temp = s.get('temp_f') or s.get('temperature') or s.get('thermocouple_temperature')
                                if temp is not None:
                                    account_stats[acc['email']] += 1
                                    
                                    all_rows.append({
                                        "timestamp": pd.to_datetime(s['observed']),
                                        "NodeNum": str(friendly_name),
                                        "temperature": float(temp),
                                        "rssi": float(current_device_rssi) if current_device_rssi is not None else None
                                    })
                    except Exception:
                        continue

                total_recovered_appends = len(all_rows)
                if total_recovered_appends == 0:
                    st.info("🔒 Cloud accounts returned 0 points for this window context.")
                    status_box.update(label="Run Finalized (0 Points Found)", state="complete")
                else:
                    st.write(f"📥 Batch loading rows straight into `{LOCAL_REC_TABLE}`...")
                    try:
                        upload_df = pd.DataFrame(all_rows)
                        if 'rssi' in upload_df.columns:
                            upload_df['rssi'] = upload_df['rssi'].astype(object).where(upload_df['rssi'].notnull(), None)

                        real_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{LOCAL_REC_TABLE}"
                        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                        client.load_table_from_dataframe(upload_df, real_table_ref, job_config=job_config).result()
                        
                        st.success(f"🎉 Success! Appended {total_recovered_appends:,} raw rows to storage.")
                        summary_line = " | ".join([f"**{email}**: {count:,} pts" for email, count in account_stats.items()])
                        st.markdown(f"📥 **Account Run Summary Logs:** {summary_line}")
                        status_box.update(label="Recovery Dump Complete!", state="complete")
                        st.cache_data.clear()
                    except Exception as bq_err:
                        st.error(f"Batch loading ingestion pipeline failure: {bq_err}")
                        status_box.update(state="error")

            # --- 6. RENDER STATISTICAL BREAKDOWN SUMMARY LEDGER ---
            if node_stats:
                st.write("### 📊 Data Recovery Tally Distribution:")
                summary_records = []
                grand_total_tally = 0
                
                nodes_to_report = final_target_nodes if final_target_nodes else sorted(list(node_stats.keys()))
                
                for node in nodes_to_report:
                    if node not in node_stats:
                        continue
                        
                    true_node_count = sum(1 for row in all_rows if row["NodeNum"] == node) if total_recovered_appends > 0 else 0
                    grand_total_tally += true_node_count
                    last_checked_in = db_max_timestamps.get(node, "❌ No Historical Records Found")
                    
                    summary_records.append({
                        "Node Number": node,
                        "Last Database Check-In": last_checked_in,
                        "Points Extracted & Appended": true_node_count
                    })
                    
                summary_df = pd.DataFrame(summary_records).sort_values(by="Node Number")
                
                total_row = pd.DataFrame([{
                    "Node Number": "🧮 Combined Total Pool",
                    "Last Database Check-In": "—",
                    "Points Extracted & Appended": grand_total_tally
                }])
                summary_df = pd.concat([summary_df, total_row], ignore_index=True)
                
                st.dataframe(summary_df, use_container_width=True, hide_index=True)
                if total_recovered_appends > 0:
                    st.balloons()
                    
    # --- SUB-TAB 5: PROJECT MASTER ---
    with tab_proj_master:
        st.subheader("⚙️ Project Lifecycle Management")
        
        # Navigation actions row
        action = st.radio("Action", ["📋 Project List", "🏗️ New Project", "🔧 Edit Project Metadata"], horizontal=True, key="admin_pm_action_radio")
        table_projects = f"{PROJECT_ID}.{DATASET_ID}.project_registry"
    
        if action == "📋 Project List":
            st.subheader("📋 Complete Project Registry Table")
            query = f"SELECT * FROM `{table_projects}` ORDER BY Project ASC"
            try:
                with st.spinner("Extracting structural project lists..."):
                    df = client.query(query).to_dataframe()
                if not df.empty:
                    for col in ['Date_Freezedown', 'Date_Completion']:
                        if col in df.columns:
                            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else:
                    st.info("The central project tracking configuration registry is currently empty.")
            except Exception as e:
                st.error(f"Failed to extract project records: {e}")
    
        elif action == "🏗️ New Project":
            st.subheader("🏗️ Initialize New Project Profile")
            try:
                all_p_q = f"SELECT Project FROM `{table_projects}` ORDER BY Project ASC"
                existing_p_list = client.query(all_p_q).to_dataframe()['Project'].tolist()
            except Exception:
                existing_p_list = []
    
            use_template = st.checkbox("📋 Clone settings from an existing project template?", key="pm_clone_toggle")
            template_data = {}
            if use_template and existing_p_list:
                template_source = st.selectbox("Select Project to Clone From", existing_p_list, key="pm_clone_source")
                if template_source:
                    try:
                        t_res = client.query(f"SELECT * FROM `{table_projects}` WHERE Project = '{template_source}'").to_dataframe()
                        if not t_res.empty:
                            template_data = t_res.iloc[0].to_dict()
                    except Exception as e:
                        st.error(f"Error reading template parameters: {e}")
    
            with st.form("new_project_form_pm"):
                col1, col2 = st.columns(2)
                n_code = col1.text_input("Project ID / Job # (e.g., 2541-Phase 2)*")
                n_name = col2.text_input("Friendly Project Name", value=template_data.get('ProjectName', ''))
                
                c_g1, c_g2 = st.columns(2)
                n_city = c_g1.text_input("City Deployment Field", value=template_data.get('City', ''))
                n_tz = c_g2.text_input("Operational Timezone Reference", value=template_data.get('Timezone', 'America/Los_Angeles'))
                
                n_up_notes = st.text_input("Automated Pipeline Sync Notes (UploadNote)", value=template_data.get('UploadNote', 'Data will be uploaded once per business day.'))
                n_as_built = st.text_input("Engineering Archive ID (AsBuiltFile)", value=template_data.get('AsBuiltFile', ''))
                n_notes = st.text_area("Initial Site Engineering Field Notes", value=template_data.get('EngNotes', ''))
                
                if st.form_submit_button("🚀 Commit New Project Entry"):
                    if not n_code.strip():
                        st.error("Unique Internal Project Identifier reference required.")
                    else:
                        safe_n_code = n_code.strip().replace("'", "''")
                        safe_n_name = n_name.strip().replace("'", "''")
                        safe_n_city = n_city.strip().replace("'", "''")
                        safe_n_tz = n_tz.strip().replace("'", "''")
                        safe_n_up_notes = n_up_notes.strip().replace("'", "''")
                        safe_n_as_built = n_as_built.strip().replace("'", "''")
                        safe_n_notes = n_notes.strip().replace("'", "''")
    
                        insert_q = f"""
                            INSERT INTO `{table_projects}` (Project, ProjectName, ProjectStatus, City, Timezone, UploadNote, AsBuiltFile, EngNotes)
                            VALUES ('{safe_n_code}', '{safe_n_name}', 'Initialized', '{safe_n_city}', '{safe_n_tz}', '{safe_n_up_notes}', '{safe_n_as_built}', '{safe_n_notes}')
                        """
                        try:
                            client.query(insert_q).result()
                            st.success(f"Registered **{safe_n_code}** successfully.")
                            st.cache_data.clear()
                            time.sleep(1)
                            st.rerun()
                        except Exception as ins_err:
                            st.error(f"Database insertion failed: {ins_err}")
    
        # =========================================================================
        # SUB-TAB 5: PROJECT MASTER
        # =========================================================================
        elif action == "🔧 Edit Project Metadata":
            st.subheader(f"🔧 Configuration Editor: {selected_project}")
            proj_q = f"SELECT * FROM `{table_projects}` WHERE Project = '{selected_project}'"
            try:
                p_res = client.query(proj_q).to_dataframe()
            except Exception as e:
                p_res = pd.DataFrame()
                st.error(f"Error querying table metadata: {e}")
            
            if p_res.empty:
                st.error("Please pick an active project in the sidebar to modify metadata metrics.")
            else:
                p_data = p_res.iloc[0].to_dict()
                with st.form("comprehensive_edit_project_pm"):
                    col1, col2 = st.columns(2)
                    u_project_id = col1.text_input("Project ID", value=p_data.get('Project', ''), disabled=True)
                    u_project_name = col2.text_input("Friendly Project Name", value=p_data.get('ProjectName', ''))
    
                    c3, c4 = st.columns(2)
                    u_city = c3.text_input("City", value=p_data.get('City', ''))
                    u_tz = c4.text_input("Timezone", value=p_data.get('Timezone', 'America/Los_Angeles'))
                    
                    u_up_notes = st.text_input("Upload Notes", value=p_data.get('UploadNote', ''))
                    u_as_built = st.text_input("As-Built File Tracking ID", value=p_data.get('AsBuiltFile', ''))
    
                    st.divider()
                    st.markdown("#### 🔄 Lifecycle Status & Target Phase Date")
                    col_status, col_date = st.columns(2)
                    
                    status_options = ["Initialized", "Pre-freeze", "Freezedown", "Maintenance", "Archived"]
                    curr_status = p_data.get('ProjectStatus', 'Initialized')
                    s_idx = status_options.index(curr_status) if curr_status in status_options else 0
                    u_status = col_status.selectbox("Lifecycle Status Tier", status_options, index=s_idx)
                    
                    # Dynamic mapping dictionary linking choices to your real database columns
                    status_date_mappings = {
                        "Initialized": "Date_Initialized",
                        "Pre-freeze": "Date_PreFreeze",
                        "Freezedown": "Date_Freezedown",
                        "Maintenance": "Date_Maintenance",
                        "Archived": "Date_Archived"
                    }
                    
                    target_date_column = status_date_mappings.get(u_status, "Date_Freezedown")
                    
                    def safe_date(d): return pd.to_datetime(d).date() if pd.notnull(d) and str(d) != 'NaT' else None
                    
                    # Automatically pull the existing date for whatever status phase is currently selected
                    u_phase_date = col_date.date_input(
                        f"Set Date for Phase: {u_status}", 
                        value=safe_date(p_data.get(target_date_column))
                    )
    
                    st.divider()
                    u_notes = st.text_area("Engineering & Site Notes Logs", value=p_data.get('EngNotes', ''))
    
                    if st.form_submit_button("💾 Overwrite Project Registry Information"):
                        # Format our calculated date cleanly to prevent 'DATE(None)' syntax errors
                        formatted_date_clause = f"DATE('{u_phase_date}')" if (u_phase_date and str(u_phase_date) != 'None') else "NULL"
                        
                        # Escape text strings safely to shield against embedded single quotes
                        safe_name = u_project_name.strip().replace("'", "''")
                        safe_city = u_city.strip().replace("'", "''")
                        safe_tz = u_tz.strip().replace("'", "''")
                        safe_up_notes = u_up_notes.strip().replace("'", "''")
                        safe_as_built = u_as_built.strip().replace("'", "''")
                        safe_notes = u_notes.strip().replace("'", "''")
                        
                        # Dynamically updates both ProjectStatus and the exact matching Phase Date column from your schema
                        update_q = f"""
                            UPDATE `{table_projects}` SET 
                                ProjectName = '{safe_name}', 
                                ProjectStatus = '{u_status}', 
                                City = '{safe_city}',
                                Timezone = '{safe_tz}', 
                                UploadNote = '{safe_up_notes}', 
                                AsBuiltFile = '{safe_as_built}',
                                EngNotes = '{safe_notes}', 
                                {target_date_column} = {formatted_date_clause}
                            WHERE Project = '{selected_project}'
                        """
                        try:
                            client.query(update_q).result()
                            st.success(f"✅ Configuration and {target_date_column} modified for: {selected_project}")
                            st.cache_data.clear()
                            time.sleep(1)
                            st.rerun()
                        except Exception as query_err:
                            st.error(f"❌ BigQuery update rejected: {query_err}")
                            st.code(update_q, language="sql")

    # =========================================================================
    # SUB-TAB 6: BULK UPLOADS (NOW CLEANLY REDUCED TO SPREADSHEETS)
    # =========================================================================
    with tab_bulk_config:
        st.subheader("📦 Centralized Bulk Ingestion Engine")
        
        cfg_mode = st.radio(
            "Select Allocation Ingestion Target Engine:", 
            ["Update Hardware Inventory", "Update Node Registry"], 
            horizontal=True, 
            key="bulk_uploads_engine_radio"
        )
        target_registry_path = f"{PROJECT_ID}.{DATASET_ID}.node_registry"
        target_inventory_path = f"{PROJECT_ID}.{DATASET_ID}.hardware_inventory"
        target_curves_path = f"{PROJECT_ID}.{DATASET_ID}.reference_curves"
        
        # --- ENGINE A: UPDATE HARDWARE INVENTORY ---
        if cfg_mode == "Update Hardware Inventory":
            st.markdown("### 📡 Update Hardware Inventory")
            st.info("Ingest spreadsheet data to append fresh units onto your inventory master table. Required Fields: `RawID`, `NodeNum`.")
            
            u_file = st.file_uploader("Upload Inventory Dataset File", type=["csv", "xlsx"], key="bulk_inv_file_uploader")
            if u_file:
                try:
                    if u_file.name.endswith('.csv'):
                        df_upload = pd.read_csv(u_file, dtype=str)
                    else:
                        df_upload = pd.read_excel(u_file, dtype=str)
                        
                    st.write("### Preview Staged Inventory Matrix")
                    st.dataframe(df_upload.head(), use_container_width=True, hide_index=True)
                    
                    if st.button("🚀 Commit Inventory Changes", key="bulk_inv_upload_commit_btn", use_container_width=True):
                        actual_cols = {str(c).strip().lower(): str(c) for c in df_upload.columns}
                        if 'rawid' not in actual_cols or 'nodenum' not in actual_cols:
                            st.error("❌ Ingestion Rejected: Missing required spreadsheet target fields.")
                        else:
                            with st.spinner("Analyzing delta thresholds and updating inventory catalog..."):
                                clean_upload_df = pd.DataFrame({
                                    'RawID': df_upload[actual_cols['rawid']].astype(str).str.strip().str.split('.').str[0],
                                    'NodeNum': df_upload[actual_cols['nodenum']].astype(str).str.strip()
                                }).dropna()
                                staging_table = f"{PROJECT_ID}.{DATASET_ID}.temp_staged_inventory_import"
                                
                                # 🛡️ HARDENED FIX: Explicitly enforce string formatting types on the staging environment
                                load_job_config = bigquery.LoadJobConfig(
                                    schema=[
                                        bigquery.SchemaField("RawID", "STRING"),
                                        bigquery.SchemaField("NodeNum", "STRING"),
                                    ],
                                    write_disposition="WRITE_TRUNCATE"
                                )
                                client.load_table_from_dataframe(clean_upload_df, staging_table, job_config=load_job_config).result()
                                
                                merge_upsert_sql = f"""
                                    INSERT INTO `{target_inventory_path}` (RawID, NodeNum)
                                    SELECT DISTINCT s.RawID, s.NodeNum FROM `{staging_table}` s
                                    WHERE NOT EXISTS (SELECT 1 FROM `{target_inventory_path}` i WHERE TRIM(CAST(i.RawID AS STRING)) = TRIM(s.RawID))
                                """
                                query_job = client.query(merge_upsert_sql)
                                query_job.result()
                                client.delete_table(staging_table, not_found_ok=True)
                                
                            st.success(f"🎉 Inventory synchronization complete! Appended {query_job.num_dml_affected_rows:,} fresh rows.")
                            st.cache_data.clear()
                            time.sleep(1)
                            st.rerun()
                except Exception as e:
                    st.error(f"Failed parsing inventory load batch matrix: {e}")
                    
        # --- ENGINE B: UPDATE NODE REGISTRY ---
        elif cfg_mode == "Update Node Registry":
            st.markdown("### 📋 Update Node Registry Maps")
            st.info("Mass register multi-sensor arrays or shift batch deployment settings across timelines using configuration maps.")
            u_csv = st.file_uploader("Upload Registry Deployment Map File", type="csv", key="bulk_reg_csv_uploader")
            if u_csv:
                df_upload = pd.read_csv(u_csv)
                st.write("### Preview Staged Registry Matrix")
                st.dataframe(df_upload.head(), use_container_width=True, hide_index=True)
                
                if st.button("🚀 Commit Registry Changes", key="bulk_reg_upload_commit_btn", use_container_width=True):
                    try:
                        required = {'NodeNum', 'Project', 'Location'}
                        if not required.issubset(df_upload.columns):
                            st.error(f"Missing required allocation column labels: {required - set(df_upload.columns)}")
                        else:
                            with st.spinner("Streaming spatial allocations into active registry view..."):
                                # 🛡️ HARDENED FIX: Force explicit string formatting to guarantee 16-byte API safety
                                if 'Start_Date' in df_upload.columns:
                                    df_upload['Start_Date'] = pd.to_datetime(df_upload['Start_Date'], errors='coerce').dt.strftime('%Y-%m-%d')
                                else:
                                    df_upload['Start_Date'] = datetime.now().strftime('%Y-%m-%d')
                                    
                                if 'SensorStatus' not in df_upload.columns:
                                    df_upload['SensorStatus'] = 'On Project'
                                    
                                if 'PhysicalID' in df_upload.columns:
                                    df_upload = df_upload.drop(columns=['PhysicalID'])
                                    
                                # Standardize field types to strings to avoid floating-point conversion errors
                                df_upload['NodeNum'] = df_upload['NodeNum'].astype(str).str.strip()
                                df_upload['Project'] = df_upload['Project'].astype(str).str.strip()
                                df_upload['Location'] = df_upload['Location'].astype(str).str.strip()
                                if 'Bank' in df_upload.columns:
                                    df_upload['Bank'] = df_upload['Bank'].fillna('').astype(str).str.strip()
                                if 'Depth' in df_upload.columns:
                                    df_upload['Depth'] = pd.to_numeric(df_upload['Depth'], errors='coerce').fillna(0.0)

                                # 🛡️ HARDENED FIX: Explicitly enforce the table layout schema configuration
                                job_config = bigquery.LoadJobConfig(
                                    schema=[
                                        bigquery.SchemaField("NodeNum", "STRING"),
                                        bigquery.SchemaField("Project", "STRING"),
                                        bigquery.SchemaField("Location", "STRING"),
                                        bigquery.SchemaField("Bank", "STRING"),
                                        bigquery.SchemaField("Depth", "FLOAT"),
                                        bigquery.SchemaField("SensorStatus", "STRING"),
                                        bigquery.SchemaField("Start_Date", "DATE"),
                                    ],
                                    write_disposition="WRITE_APPEND"
                                )
                                client.load_table_from_dataframe(df_upload, target_registry_path, job_config=job_config).result()
                                
                            st.success(f"🎉 Success! Appended {len(df_upload)} nodes onto your asset deployment matrix timeline safely.")
                            st.cache_data.clear()
                            time.sleep(1)
                            st.rerun()
                    except Exception as upload_err:
                        st.error(f"Bulk hardware deployment logging operation failed: {upload_err}")
                        

# =========================================================================
    # SUB-TAB 7: CHILLER OPERATIONS & SYSTEM MANIFEST
    # =========================================================================
    with tab_chillers:
        st.subheader("❄️ Chiller Plant Infrastructure & Event Logging")
        
        c_mode = st.radio("Management Context", ["📋 Plant Manifest", "🚨 Log Chiller Event"], horizontal=True, key="chiller_mgmt_mode_radio")
        
        CHILLER_REG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.chiller_registry"
        CHILLER_MAP_TABLE = f"{PROJECT_ID}.{DATASET_ID}.chiller_sensor_mapping"
        EVENTS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.freezedown_events"
        
        # --- VIEW MANIFESTS ---
        if c_mode == "📋 Plant Manifest":
            st.markdown("### 📡 Registered Cooling Infrastructure")
            try:
                manifest_q = f"""
                    SELECT c.chiller_id, c.project_id, c.chiller_type,
                           STRING_AGG(m.Location, ', ' ORDER BY m.Location) as mapped_locations
                    FROM `{CHILLER_REG_TABLE}` c
                    LEFT JOIN `{CHILLER_MAP_TABLE}` m ON c.chiller_id = m.chiller_id AND c.project_id = m.project_id
                    GROUP BY c.chiller_id, c.project_id, c.chiller_type
                    ORDER BY c.project_id ASC, c.chiller_id ASC
                """
                manifest_df = client.query(manifest_q).to_dataframe()
                if not manifest_df.empty:
                    st.dataframe(manifest_df, use_container_width=True, hide_index=True, column_config={
                        "chiller_id": "Chiller ID", "project_id": "Assigned Phase ID", 
                        "chiller_type": "Chiller Specifications", "mapped_locations": "Chilled Ground Assets"
                    })
                else:
                    st.info("No mechanical chiller plant assets registered in system catalog yet.")
            except Exception as e:
                st.caption(f"Manifest offline or initializing: {e}")
                
            # Quick Infrastructure Insertion Form
            with st.expander("➕ Register New Mechanical Chiller Unit"):
                with st.form("register_chiller_unit_form"):
                    col_c1, col_c2, col_c3 = st.columns(3)
                    new_c_id = col_c1.text_input("Chiller Serial ID (Unique)*", placeholder="e.g., CH-2541-A")
                    new_c_proj = col_c2.selectbox("Assign to Project Space Phase*", available_projects_list)
                    new_c_type = col_c3.text_input("Chiller Type Specifications", placeholder="e.g., 53-Ton Logue")
                    
                    # Pull current available unique locations for that specific project phase
                    possible_locations = sorted(full_reg_df[full_reg_df['Project'] == new_c_proj]['Location'].dropna().unique().tolist())
                    selected_assigned_locs = st.multiselect("Chilled Pipe Group Assignments", options=possible_locations)
                    
                    if st.form_submit_button("🚀 Save Unit Settings"):
                        if not new_c_id.strip():
                            st.error("Unique Chiller Serial ID validation token required.")
                        else:
                            safe_cid = new_c_id.strip().replace("'", "''")
                            safe_ctype = new_c_type.strip().replace("'", "''")
                            
                            insert_chiller_sql = f"""
                                INSERT INTO `{CHILLER_REG_TABLE}` (chiller_id, project_id, chiller_type, purchase_date)
                                VALUES ('{safe_cid}', '{new_c_proj}', '{safe_ctype}', CURRENT_DATE());
                            """
                            try:
                                client.query(insert_chiller_sql).result()
                                
                                if selected_assigned_locs:
                                    map_rows = [f"('{new_c_proj}', '{safe_cid}', '{loc.replace("'", "''")}')" for loc in selected_assigned_locs]
                                    insert_maps_sql = f"INSERT INTO `{CHILLER_MAP_TABLE}` (project_id, chiller_id, Location) VALUES {', '.join(map_rows)};"
                                    client.query(insert_maps_sql).result()
                                    
                                st.success(f"Successfully registered machine asset {safe_cid} system-wide.")
                                st.cache_data.clear()
                                time.sleep(0.5)
                                st.rerun()
                            except Exception as err:
                                st.error(f"Database insertion failed: {err}")

        # --- LOG FREEZEDOWN EVENT HANDLER ---
        elif c_mode == "🚨 Log Chiller Event":
            st.markdown("### 🚨 Append Diagnostic Event Record")
            
            # Fetch active chillers to populate picker dropdown dynamically
            try:
                active_chillers_list = sorted(client.query(f"SELECT chiller_id FROM `{CHILLER_REG_TABLE}`").to_dataframe()['chiller_id'].tolist())
            except Exception:
                active_chillers_list = []
                
            if not active_chillers_list:
                st.warning("⚠️ No active chillers registered. Please add a mechanical plant unit under the manifest tab first.")
            else:
                import uuid
                with st.form("manual_chiller_event_logger_form"):
                    e_col1, e_col2, e_col3 = st.columns(3)
                    chosen_chiller = e_col1.selectbox("Select Target Chiller Loop ID*", active_chillers_list)
                    
                    # Fetch corresponding project_id dynamically
                    try:
                        chosen_proj = client.query(f"SELECT project_id FROM `{CHILLER_REG_TABLE}` WHERE chiller_id='{chosen_chiller}' LIMIT 1").to_dataframe()['project_id'].iloc[0]
                    except Exception:
                        chosen_proj = selected_project
                        
                    e_date = e_col2.date_input("Event Log Date Entry", value=datetime.now().date())
                    e_time = e_col3.time_input("Event Log Time Entry (UTC/Local)", value=datetime.now().time())
                    
                    e_desc = st.text_input("Operational Event Description / Alert Message*", placeholder="e.g., Compressor trip down due to unexpected mechanical oil pressure bypass fault.")
                    e_cause = st.text_input("Determined Root Cause Diagnostics", placeholder="e.g., Blocked primary filter screen.")
                    
                    is_approx = st.checkbox("Timestamp registration is approximate?", value=False)
                    is_resolved = st.checkbox("Event issue was resolved immediately?", value=False)
                    
                    if st.form_submit_button("💾 Commit Diagnostic Event to Storage"):
                        if not e_desc.strip():
                            st.error("Operational Event Description summary strings are required.")
                        else:
                            generated_uuid = str(uuid.uuid4())
                            combined_event_ts = datetime.combine(e_date, e_time).strftime('%Y-%m-%d %H:%M:%S')
                            resolution_ts_val = f"TIMESTAMP('{combined_event_ts}')" if is_resolved else "NULL"
                            
                            safe_desc = e_desc.strip().replace("'", "''")
                            safe_cause = e_cause.strip().replace("'", "''")
                            
                            event_insert_sql = f"""
                                INSERT INTO `{EVENTS_TABLE}` (event_id, project_id, chiller_id, event_timestamp, resolution_timestamp, event_description, root_cause, is_time_approximate)
                                VALUES ('{generated_uuid}', '{chosen_proj}', '{chosen_chiller}', TIMESTAMP('{combined_event_ts}'), {resolution_ts_val}, '{safe_desc}', '{safe_cause}', {str(is_approx).upper()})
                            """
                            try:
                                client.query(event_insert_sql).result()
                                st.success("🎉 Event successfully injected into production analytics dataset tables.")
                                st.cache_data.clear()
                                time.sleep(0.5)
                                st.rerun()
                            except Exception as bq_err:
                                st.error(f"BigQuery tracking write rejected: {bq_err}")
                                st.code(event_insert_sql, language="sql")

# =============================================================================
# 🛠️ REUSABLE LAB ENGINE ASSIGNMENT PIPELINES
# =============================================================================

@st.cache_data(ttl=300)
def load_lab_node_registry_data(target_table):
    """Safely assembles asset inventories with matching real-time ping lag windows."""
    client = get_bq_client()
    if client is None: return pd.DataFrame()
    try:
        master_query = f"""
            WITH LatestTelemetry AS (
                SELECT 
                    NodeNum, 
                    MAX(timestamp) as last_ping,
                    ARRAY_AGG(temperature ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] as last_temp
                FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view`
                GROUP BY NodeNum
            ),
            AssignmentWindows AS (
                SELECT 
                    NodeNum, Start_Date, COALESCE(End_Date, CURRENT_DATE()) AS Effective_End,
                    DATE_DIFF(COALESCE(End_Date, CURRENT_DATE()), Start_Date, DAY) * 24 AS Expected_Hours
                FROM `{target_table}` WHERE Project != 'Dead'
            ),
            ActualProjectPings AS (
                SELECT 
                    m.NodeNum, a.Start_Date,
                    COUNT(DISTINCT TIMESTAMP_TRUNC(m.timestamp, HOUR)) AS Actual_Pings_Logged
                FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
                INNER JOIN AssignmentWindows a ON m.NodeNum = a.NodeNum 
                  AND EXTRACT(DATE FROM m.timestamp) BETWEEN a.Start_Date AND a.Effective_End
                GROUP BY m.NodeNum, a.Start_Date
            )
            SELECT 
                R.*, T.last_ping, T.last_temp, A.Expected_Hours,
                COALESCE(P.Actual_Pings_Logged, 0) AS Actual_Pings_Logged
            FROM `{target_table}` R
            LEFT JOIN LatestTelemetry T ON R.NodeNum = T.NodeNum
            LEFT JOIN AssignmentWindows A ON R.NodeNum = A.NodeNum AND R.Start_Date = A.Start_Date
            LEFT JOIN ActualProjectPings P ON R.NodeNum = P.NodeNum AND R.Start_Date = P.Start_Date
        """
        df = client.query(master_query).to_dataframe()
        now_utc = pd.Timestamp.now(tz='UTC')
        
        if not df.empty and 'last_ping' in df.columns:
            df['hours_hidden'] = df['last_ping'].apply(
                lambda x: (now_utc - pd.to_datetime(x).tz_convert('UTC')).total_seconds() / 3600.0
                if pd.notnull(x) else float('inf')
            )
            df['hours_hidden'] = pd.to_numeric(df['hours_hidden'], errors='coerce').fillna(float('inf'))
            
            def format_last_seen(hours):
                if pd.isna(hours) or hours == float('inf'): return "❌ Never"
                if hours < 1.0:
                    mins = int(hours * 60)
                    return f"{mins}m ago" if mins > 0 else "Just now"
                return f"{hours:.1f}h ago"
            df['Last Seen'] = df['hours_hidden'].apply(format_last_seen)
        else:
            df['hours_hidden'] = float('inf')
            df['Last Seen'] = "❌ Never"
            
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
        st.error(f"Error compiling registry: {e}")
        return pd.DataFrame()

def render_lab_node_selector(reg_df, proj_list):
    """Renders hierarchical dropdown filters and selection matrix tables."""
    st.subheader("🎯 Active Node Registry")
    hide_archived = st.checkbox("Hide Archived Records", value=True, key="lab_ns_hide_archived_toggle")
    
    df = reg_df.copy()
    if hide_archived and 'SensorStatus' in df.columns:
        df = df[(df['SensorStatus'].str.lower() != "archived") & (~df['Location'].str.contains("Archive", case=False, na=False))]

    c1, c2, c3 = st.columns(3)
    with c1:
        f_proj = st.selectbox("Filter by Project Space", ["All", "Unassigned"] + proj_list, key="lab_ns_proj_f")
    with c2:
        loc_opts = df['Location'].dropna().unique().tolist() if f_proj == "All" else df[df['Project'] == f_proj]['Location'].dropna().unique().tolist()
        f_loc = st.selectbox("Filter by Physical Location", ["All"] + sorted(loc_opts), key="lab_ns_loc_f")
    with c3:
        search_term = st.text_input("Global Search (Node ID)", "", key="lab_ns_search_f")

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

    # Reset index so that position line up perfectly with st.data_editor keys
    df = df.reset_index(drop=True)

    st.markdown("### 📋 Current Asset Allocation Matrix")
    if "lab_last_selected_node" not in st.session_state: 
        st.session_state["lab_last_selected_node"] = None
    if "lab_active_selected_record" not in st.session_state: 
        st.session_state["lab_active_selected_record"] = None

    ed_key = "lab_node_registry_editor"
    if ed_key in st.session_state and "edited_rows" in st.session_state[ed_key]:
        changed_rows = st.session_state[ed_key]["edited_rows"]
        newly_checked = [int(idx) for idx, changes in changed_rows.items() if changes.get("Select") == True]
        
        if newly_checked and not df.empty:
            latest_idx = newly_checked[-1]
            if latest_idx != st.session_state["lab_last_selected_node"]:
                st.session_state["lab_last_selected_node"] = latest_idx
                # FIXED: Change .loc to .iloc to safely fetch via positional editor index array
                rec_dict = df.iloc[latest_idx].drop(["hours_hidden"], errors='ignore').to_dict()
                rec_dict["Select"] = True
                st.session_state["lab_active_selected_record"] = rec_dict
                st.session_state[ed_key]["edited_rows"] = {}
                st.rerun()
        elif any(changes.get("Select") == False for idx, changes in changed_rows.items()):
            st.session_state["lab_last_selected_node"] = None
            st.session_state["lab_active_selected_record"] = None
            st.session_state[ed_key]["edited_rows"] = {}
            st.rerun()

    df.insert(0, "Select", False)
    if st.session_state["lab_last_selected_node"] is not None and st.session_state["lab_last_selected_node"] < len(df):
        df.loc[st.session_state["lab_last_selected_node"], "Select"] = True

    unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
    unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"
    
    def resolve_pos(row):
        return f"{row['Depth']}ft" if (pd.notnull(row.get('Depth')) and row.get('Depth') != 0) else f"Bank {row.get('Bank', '-')}"
    
    df['Position'] = df.apply(resolve_pos, axis=1)
    df['Current Temp'] = df['last_temp'].apply(lambda x: f"{x:.1f}{unit_label}" if pd.notnull(x) else "N/A")

    edited_df = st.data_editor(
        df, hide_index=True, use_container_width=True,
        column_config={
            "Select": st.column_config.CheckboxColumn("Select", default=False, required=True),
            "NodeNum": "Node ID", "Position": "Depth/Bank", "Last Seen": "Last Seen", "Current Temp": "Current Temp"
        },
        disabled=[col for col in df.columns if col != "Select"],
        column_order=["Select", "Project", "Location", "NodeNum", "Position", "Last Seen", "Current Temp"], 
        key=ed_key
    )
    return st.session_state["lab_active_selected_record"]


def render_lab_node_action_manager(client, selected_node_data, reg_df, proj_list, known_project_locations, target_registry):
    """Displays transactional metadata forms supporting historical time-series manipulation with cascading locations lists."""
    import time
    from datetime import datetime
    
    node_id = str(selected_node_data['NodeNum']).strip()
    origin_start_str = str(selected_node_data.get('Start_Date'))
    end_label_text = str(selected_node_data.get('End_Date')) if pd.notnull(selected_node_data.get('End_Date')) else "Current Active Window"
    
    st.markdown(f"### 🛠️ Modify Assignment Attributes")
    st.caption(f"📝 Currently Editing Configuration Path for Node: **{node_id}** | Window Timeline: `({origin_start_str})` ➡️ `({end_label_text})`")
    
    # 1. ATTRIBUTE FORM ENGINE BLOCK
    with st.form("lab_attribute_form_historical"):
        col1, col2, col3 = st.columns(3)
        edit_proj = col1.selectbox("Project", proj_list, index=proj_list.index(selected_node_data['Project']) if selected_node_data['Project'] in proj_list else 0)
        
        # LOCATION ARCHITECTURE OVERRIDE
        current_loc_val = str(selected_node_data.get('Location', ''))
        
        # Build dropdown options containing current assigned options pool
        form_loc_options = sorted(list(set(known_project_locations)))
        if current_loc_val not in form_loc_options and current_loc_val.strip() != "":
            form_loc_options.append(current_loc_val)
        form_loc_options.append("➕ Add Custom Location...")
        
        default_loc_idx = form_loc_options.index(current_loc_val) if current_loc_val in form_loc_options else 0
        
        chosen_form_loc = col2.selectbox(
            "Location", 
            options=form_loc_options, 
            index=default_loc_idx,
            help="Select an existing project location from the drop-down menu, or choose Add Custom Location to enter a new one."
        )
        
        custom_loc_input = ""
        if chosen_form_loc == "➕ Add Custom Location...":
            custom_loc_input = st.text_input("Enter New Custom Location name:", placeholder="e.g., Borehole-12")
            
        status_options = ["On Project", "Available", "Diagnostic", "Dead", "Archived"]
        curr_status_str = str(selected_node_data.get('SensorStatus', 'On Project'))
        status_idx = status_options.index(curr_status_str) if curr_status_str in status_options else 0
        edit_status = col3.selectbox("Sensor Status", status_options, index=status_idx)
        
        c4, c5, c6, c7 = st.columns(4)
        edit_bank = c4.text_input("Bank", value=str(selected_node_data.get('Bank', '')) if pd.notnull(selected_node_data.get('Bank')) else "")
        edit_depth = c5.number_input("Depth", value=float(selected_node_data.get('Depth', 0.0)))
        edit_start = c6.date_input("Start Date", value=pd.to_datetime(selected_node_data.get('Start_Date')).date())
        
        has_end_date = pd.notnull(selected_node_data.get('End_Date'))
        default_end_date = pd.to_datetime(selected_node_data.get('End_Date')).date() if has_end_date else datetime.now().date()
        
        use_end_date_toggle = c7.checkbox("Apply Terminated End Date Constraints?", value=has_end_date, key=f"end_dt_toggle_{node_id}_{origin_start_str}")
        edit_end = c7.date_input("End Date", value=default_end_date, disabled=not use_end_date_toggle)

        if st.form_submit_button("💾 Overwrite Targeted Assignment Attributes Configuration Row Line", use_container_width=True):
            raw_loc_str = custom_loc_input.strip() if chosen_form_loc == "➕ Add Custom Location..." else chosen_form_loc
            
            if chosen_form_loc == "➕ Add Custom Location..." and not raw_loc_str:
                st.error("❌ Action Rejected: Custom location field string value cannot be blank.")
                return

            # Sanitize character inputs to defend against script string tears
            final_loc_str = str(raw_loc_str).replace("'", "''").strip()
            safe_proj = str(edit_proj).replace("'", "''").strip()
            safe_status = str(edit_status).replace("'", "''").strip()
            safe_bank = str(edit_bank).strip().replace("'", "''")

            sql_bank_clause = f"'{safe_bank}'" if safe_bank != "" else "NULL"
            sql_depth_clause = "NULL" if edit_depth == 0.0 else f"{edit_depth}"
            sql_end_clause = f"DATE('{edit_end}')" if use_end_date_toggle else "NULL"
            
            update_sql = f"""
                BEGIN TRANSACTION;
                DELETE FROM `{target_registry}` 
                WHERE NodeNum = '{node_id}' AND Start_Date = DATE('{selected_node_data['Start_Date']}');
                INSERT INTO `{target_registry}` (NodeNum, Project, Location, Bank, Depth, SensorStatus, Start_Date, End_Date)
                VALUES ('{node_id}', '{safe_proj}', '{final_loc_str}', {sql_bank_clause}, {sql_depth_clause}, '{safe_status}', DATE('{edit_start}'), {sql_end_clause});
                COMMIT;
            """
            try:
                client.query(update_sql).result()
                st.success("✅ Entry updated within the registry layout maps successfully!")
                st.cache_data.clear()
                time.sleep(0.5)
                st.rerun()
            except Exception as bq_exec_err:
                st.error(f"❌ Database Transaction Failed: {bq_exec_err}")
                st.code(update_sql, language="sql")

    # 2. QUICK TASKS FOOTER MATRICES
    st.markdown("##### Quick Operational Tasks")
    o1, o2, o3, o4 = st.columns(4)
    
    with o1:
        with st.expander("🔚 End Assignment"):
            st.caption("Surgically stamp a termination date onto this deployment window.")
            if st.button("Confirm End Assignment", key="btn_end_task_hist", use_container_width=True):
                end_sql = f"""
                    UPDATE `{target_registry}` 
                    SET End_Date = CURRENT_DATE() 
                    WHERE NodeNum='{node_id}' AND Start_Date = DATE('{selected_node_data['Start_Date']}')
                """
                client.query(end_sql).result()
                st.success("🏁 Target assignment window finalized successfully!")
                st.cache_data.clear()
                time.sleep(0.5)
                st.rerun()
                
    with o2:
        with st.expander("🔄 Change Sensor ID"):
            swap_target = st.text_input("Replacement Node Tag ID string:", placeholder="e.g., TP-0105")
            if st.button("Execute Change Sensor Protocol", key="btn_swap_task_hist") and swap_target:
                safe_swap = str(swap_target).strip().replace("'", "''")
                client.query(f"UPDATE `{target_registry}` SET NodeNum='{safe_swap}' WHERE NodeNum='{node_id}' AND Start_Date=DATE('{selected_node_data['Start_Date']}')").result()
                st.cache_data.clear()
                st.rerun()
                
    with o3:
        with st.expander("➕ Add New Manual Assignment"):
            st.caption("Insert manual lines tracking log entries.")
            
    with o4:
        with st.expander("🗑️ Permanent Hard Delete Row Line"):
            if st.checkbox("Authorize permanent line termination"):
                if st.button("Delete Selected Historical Row Line Block", type="primary"):
                    client.query(f"DELETE FROM `{target_registry}` WHERE NodeNum='{node_id}' AND Start_Date=DATE('{selected_node_data['Start_Date']}')").result()
                    st.cache_data.clear()
                    st.rerun()

def render_lab_data_checker(client, reg_df):
    """
    Calculates and monitors systemic data conflicts, chronological alignment 
    gaps, timeline skips, and hardware assignment overlaps across the fleet.
    """
    st.markdown("### 🔍 Data Checker Diagnostics")
    
    if reg_df.empty:
        st.info("The system node registry is unpopulated. Skipping automated integrity scans.")
        return

    # Extract clean baseline reference structures
    active_registry_table = f"{PROJECT_ID}.{DATASET_ID}.node_registry"
    master_telemetry_view = f"{PROJECT_ID}.{DATASET_ID}.master_data_view"
    
    c1, c2, c3, c4 = st.tabs([
        "⏱️ Gaps in Data (Missing Office Time)", 
        "🚨 Orphaned Nodes (Missing Next Assignment)", 
        "🚨 Multiple / Duplicate Assignments", 
        "🚨 Location & Position Overlaps"
    ])
    
    # =========================================================================
    # TAB 1: CHRONOLOGICAL TIMELINE GAPS (MISSING OFFICE TIME)
    # =========================================================================
    with c1:
        st.markdown("#### ⏱️ Chronological Gap Analysis")
        gap_query = f"""
            WITH OrderedAssignments AS (
                SELECT 
                    NodeNum, Project, Start_Date, End_Date,
                    LEAD(Start_Date) OVER (PARTITION BY NodeNum ORDER BY Start_Date ASC) as next_start
                FROM `{active_registry_table}`
            )
            SELECT 
                NodeNum as `Node ID`,
                Project as `Ended Project ID`,
                End_Date as `Decommission Date`,
                next_start as `Next Deployment Date`,
                DATE_DIFF(next_start, End_Date, DAY) as `Unmonitored Gap (Days)`
            FROM OrderedAssignments
            WHERE End_Date IS NOT NULL 
              AND next_start IS NOT NULL 
              AND DATE_DIFF(next_start, End_Date, DAY) > 1
            ORDER BY `Unmonitored Gap (Days)` DESC
        """
        try:
            gap_df = client.query(gap_query).to_dataframe()
            if not gap_df.empty:
                st.error("⚠️ **Timeline Discontinuity Warning:** The following hardware sensors contain unmonitored tracking gaps between historical assignments without an intermediate 'Office' log record:")
                st.dataframe(gap_df, use_container_width=True, hide_index=True)
            else:
                st.success("✅ **Chronological Integrity Verified:** No timeline gaps or missing 'Office' storage windows detected across node history logs.")
        except Exception as e:
            st.caption(f"Timeline gap engine initializing: {e}")

    # =========================================================================
    # TAB 2: ORPHANED SENSORS (MISSING END DATES ON PREVIOUS DEPLOYMENTS)
    # =========================================================================
    with c2:
        st.markdown("#### 🚨 Open-Ended Terminations Checklist")
        orphan_query = f"""
            WITH ActiveCounts AS (
                SELECT NodeNum, COUNT(*) as open_windows
                FROM `{active_registry_table}`
                WHERE End_Date IS NULL
                GROUP BY NodeNum
            )
            SELECT 
                r.NodeNum as `Node ID`,
                r.Project as `Project ID`,
                r.Location as `Location / Borehole`,
                r.Start_Date as `Deployment Start`
            FROM `{active_registry_table}` r
            JOIN ActiveCounts a ON r.NodeNum = a.NodeNum
            WHERE r.End_Date IS NULL AND a.open_windows > 1
            ORDER BY r.NodeNum ASC, r.Start_Date ASC
        """
        try:
            orphan_df = client.query(orphan_query).to_dataframe()
            if not orphan_df.empty:
                st.error("⚠️ **Orphaned Open-End Alert:** The following nodes have been reassigned to new lines but their older historical assignments were never closed out with an `End_Date`:")
                st.dataframe(orphan_df, use_container_width=True, hide_index=True)
            else:
                st.success("✅ **Clean Terminations Verified:** All decommissioned hardware records successfully close prior windows when moving to a new deployment channel.")

        except Exception as e:
            st.caption(f"Orphan scan engine initializing: {e}")

    # =========================================================================
    # TAB 3: DUAL / MULTIPLE ASSIGNMENTS OVERLAPPING TIMELINES
    # =========================================================================
    with c3:
        st.markdown("#### 🚨 Timeline Window Overlap Scans")
        overlap_query = f"""
            SELECT 
                t1.NodeNum as `Node ID`,
                t1.Project as `Proj A`,
                t1.Start_Date as `Start A`,
                t1.End_Date as `End A`,
                t2.Project as `Proj B`,
                t2.Start_Date as `Start B`,
                t2.End_Date as `End B`
            FROM `{active_registry_table}` t1
            JOIN `{active_registry_table}` t2 
              ON t1.NodeNum = t2.NodeNum 
             AND t1.Start_Date < t2.Start_Date
             AND (t1.End_Date IS NULL OR t1.End_Date > t2.Start_Date)
            ORDER BY t1.NodeNum ASC
        """
        try:
            overlap_df = client.query(overlap_query).to_dataframe()
            if not overlap_df.empty:
                st.error("⚠️ **Simultaneous Allocation Conflict:** The following sensors are registered to multiple distinct physical configurations with overlapping operational timelines:")
                st.dataframe(overlap_df, use_container_width=True, hide_index=True)
            else:
                st.success("✅ **Clean Database Entries:** No concurrent duplicate records with overlapping calendar windows identified.")
        except Exception as e:
            st.caption(f"Overlap cross-scan metrics initializing: {e}")

    # =========================================================================
    # TAB 4: SPATIAL OVERLAPS (MULTIPLE HARDWARE IN ONE SPATIAL COORDINATE)
    # =========================================================================
    with c4:
        st.markdown("#### 🚨 Position and Coordinate Collision Check")
        spatial_query = f"""
            SELECT 
                Project as `Project ID`,
                Location as `Location`,
                COALESCE(CAST(Depth AS STRING), CONCAT('Bank ', Bank)) as `Coordinate Position`,
                STRING_AGG(NodeNum, ' ↔️ ') as `Conflicting Node Group`,
                COUNT(*) as `Active Hardware Count`
            FROM `{active_registry_table}`
            WHERE End_Date IS NULL 
              AND Project != 'Office' 
              AND Location != 'Office'
            GROUP BY Project, Location, Bank, Depth
            HAVING COUNT(*) > 1
            ORDER BY Project ASC, Location ASC
        """
        try:
            spatial_df = client.query(spatial_query).to_dataframe()
            if not spatial_df.empty:
                st.error("⚠️ **Spatial Grid Collision Detected:** The following borehole coordinates currently house more than one active telemetry asset simultaneously:")
                st.dataframe(spatial_df, use_container_width=True, hide_index=True)
            else:
                st.success("✅ **Perfect Grid Alignment:** Every active coordinate holds exactly one distinct active hardware asset mapping layout.")
        except Exception as e:
            st.caption(f"Spatial proximity engine initializing: {e}")


###################
# 12. MAIN ROUTER #
###################

# 1. RETRIEVE GLOBAL STATE
display_tz = st.session_state.get("display_tz", "UTC")
unit_label = st.session_state.get("unit_label", "°F")
unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
active_refs = st.session_state.get("active_refs", [])

# 2. INITIALIZE DB CLIENT
client = get_bq_client() 

# 3. PAGE ROUTING
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

# --- PASSWORD PROTECTED ADMINISTRATIVE SECTIONS ---
elif page in ["Data Processing", "Admin Tools"]:
    if st.session_state.get('authenticated', False):
        
        if page == "Data Processing":
            render_data_processing_page(selected_project)
                
        elif page == "Admin Tools":
            # FIXED: Execution is now cleanly routed entirely to the centralized admin page layout engine
            render_admin_page(
                selected_project, 
                display_tz, 
                unit_mode, 
                unit_label, 
                active_refs
            )
                
    else:
        st.divider()
        c1, c2, c3 = st.columns([1, 2, 1])
        with c2:
            st.subheader("🔐 Restricted Admin Access")
            pwd = st.text_input("Enter Admin Password", type="password")
            if st.button("Unlock Dashboard", use_container_width=True):
                if pwd == st.secrets["admin_password"]:
                    st.session_state['authenticated'] = True
                    st.rerun()
                else:
                    st.error("Invalid Password. Access Denied.")
