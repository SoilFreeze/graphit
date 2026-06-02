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
            target_q = f"""
                SELECT CurveID, Day, Temp FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` 
                WHERE UPPER(CurveID) LIKE UPPER('%{proj_num}%') 
                AND UPPER(CurveID) LIKE UPPER('%{loc_part}%')
                ORDER BY Day
            """
            target_df = client.query(target_q).to_dataframe()
            if not target_df.empty:
                
                # --- NEW: DESIGN VARIATION MATRICES ---
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
        except: pass

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
    # Read lookback parameters directly out of the global sidebar slider key
    # Defaulting to 5 weeks to match your main slider fallback state
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
    - Baseline: First Monday at 06:00 AM (Black Dashed Line).
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

    # 3. PRE-PROCESS DATA
    p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
    depth_df = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
    
    if depth_df.empty:
        st.info("No sensors with valid 'Depth' values found in the Node Registry.")
        return

    unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
    freeze_pt = 0 if unit_mode == "Celsius" else 32
    
    # 4. GENERATE SNAPSHOTS
    now_utc = pd.Timestamp.now(tz='UTC')
    mondays = pd.date_range(end=now_utc, periods=lookback_weeks, freq='W-MON')
    locations = sorted(depth_df['Location'].unique())
    
    for loc in locations:
        with st.expander(f"📍 Temp vs Depth - {loc}", expanded=True):
            loc_data = depth_df[depth_df['Location'] == loc].copy()
            fig = go.Figure()

            # --- A. CALCULATE BASELINE (True First Week Data - No hardcoded offset shift) ---
            baseline_ts = loc_data['timestamp'].min()
            
            # Create an exact 24-hour window around that first timestamp to grab the profile
            b_window = loc_data[
                (loc_data['timestamp'] >= baseline_ts - pd.Timedelta(hours=12)) & 
                (loc_data['timestamp'] <= baseline_ts + pd.Timedelta(hours=12))
            ]
            
            # Store the exact baseline date string so we can block it from the weekly loop
            baseline_date_str = ""
            
            if not b_window.empty:
                # Standardize to a date string (e.g., '2026-04-20')
                baseline_date_str = baseline_ts.strftime('%Y-%m-%d')
                
                snap = (
                    b_window.assign(diff=(b_window['timestamp'] - baseline_ts).abs())
                    .sort_values(['NodeNum', 'diff'])
                    .drop_duplicates('NodeNum')
                    .sort_values('Depth_Num')
                )
                
                b_temps = snap['temperature']
                if unit_mode == "Celsius": b_temps = (b_temps - 32) * 5/9
                
                # Plot the clean, single Black Dashed Baseline
                fig.add_trace(go.Scatter(
                    x=b_temps, y=snap['Depth_Num'], 
                    mode='lines', 
                    name=f'Baseline ({baseline_date_str})',
                    line=dict(color='black', width=2.5, dash='dash'),
                    hovertemplate=f"Baseline: {baseline_date_str}<br>Depth: %{{y}}ft<br>Temp: %{{x:.1f}}{unit_label}<extra></extra>"
                ))
            
            # --- B. PLOT WEEKLY SNAPSHOTS (Deduplicated) ---
            for m_date in mondays:
                target_ts = m_date.replace(hour=6, minute=0, second=0)
                current_loop_date = target_ts.strftime('%Y-%m-%d')
                
                # CRITICAL CRITERIA: If this week matches the baseline date, SKIP IT 
                if current_loop_date == baseline_date_str:
                    continue
                    
                window = loc_data[
                    (loc_data['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & 
                    (loc_data['timestamp'] <= target_ts + pd.Timedelta(hours=12))
                ]
                
                if not window.empty:
                    snap = (
                        window.assign(diff=(window['timestamp'] - target_ts).abs())
                        .sort_values(['NodeNum', 'diff'])
                        .drop_duplicates('NodeNum')
                        .sort_values('Depth_Num')
                    )
                    
                    temps = snap['temperature']
                    if unit_mode == "Celsius": temps = (temps - 32) * 5/9
                    
                    fig.add_trace(go.Scatter(
                        x=temps, y=snap['Depth_Num'], 
                        mode='lines+markers', 
                        name=current_loop_date,
                        line=dict(shape='spline', smoothing=1.1, width=1.5),
                        marker=dict(size=4),
                        hovertemplate=f"Date: {current_loop_date}<br>Depth: %{{y}}ft<br>Temp: %{{x:.1f}}{unit_label}<extra></extra>"
                    ))

            # --- C. FREEZING REFERENCE LINE ---
            fig.add_vline(x=freeze_pt, line_width=2, line_dash="solid", line_color="#ADD8E6")

            # --- D. STANDARDIZED SCALING & BOX FRAME ---
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

        # 5. LOCATION SUMMARY (High-Resolution Spread with Conditional Color Styler)
        st.subheader("📍 Location Performance Summary")
        
        summary_df = df.groupby('Location').apply(lambda x: pd.Series({
            'Total Nodes': int(len(x)),
            'Seen 1h': int(x['seen_1h_f'].sum()),
            'Seen 6h': int(x['seen_6h_f'].sum()),
            'Seen 24h': int(x['seen_24h_f'].sum()),
            '24h Coverage': f"{x['coverage_24h'].mean():.1f}%",
            '7d Coverage': f"{x['coverage_7d'].mean():.1f}%",
            'Avg Temp': fmt_t(x['current_temp'].mean()),
            'Low 24h': fmt_t(x['low_24h'].min()),
            'High 24h': fmt_t(x['high_24h'].max()),
            'Best Seen': get_status_icon(x['last_seen_hrs'].min()),
            'Worst Seen': get_status_icon(x['last_seen_hrs'].max())
        })).reset_index()

        # Custom Cell Color Matrix Engine for Hardware Availability Toggles
        def style_missing_counters(val_df):
            canvas = pd.DataFrame('', index=val_df.index, columns=val_df.columns)
            target_cols = ['Seen 1h', 'Seen 6h', 'Seen 24h']
            
            for idx in val_df.index:
                total = val_df.loc[idx, 'Total Nodes']
                for col in target_cols:
                    seen = val_df.loc[idx, col]
                    missing = total - seen
                    
                    if missing == 0:
                        bg_style = "background-color: #d1fae5; color: #065f46; font-weight: bold;" # Green
                    elif 1 <= missing <= 3:
                        bg_style = "background-color: #bbf7d0; color: #14532d; font-weight: bold;" # Light Green
                    elif 4 <= missing <= 6:
                        bg_style = "background-color: #fef08a; color: #713f12; font-weight: bold;" # Yellow
                    elif 7 <= missing <= 10:
                        bg_style = "background-color: #fed7aa; color: #7c2d12; font-weight: bold;" # Orange
                    else:
                        bg_style = "background-color: #fca5a5; color: #7f1d1d; font-weight: bold;" # Red
                        
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
# PAGE MODULE: 🛠️ NODE MANAGER
# =============================================================================

def render_node_selector(reg_df, proj_list):
    """
    Renders an active inventory node selection engine with integrated 
    Last Seen reporting, project uptime efficiencies, and a fleet hardware status matrix.
    """
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

    if 'hours_hidden' in df.columns:
        df['hours_hidden'] = pd.to_numeric(df['hours_hidden'], errors='coerce').fillna(float('inf'))
        df = df.sort_values(by='hours_hidden', ascending=True).reset_index(drop=True)
    else:
        df['hours_hidden'] = float('inf')

    st.markdown("### 📡 Hardware Inventory Fleet Breakdown")
    
    def classify_hardware_family(node):
        node_str = str(node).lower()
        if "-ch" in node_str:
            return "Lord"
        elif node_str.startswith("sp"):
            return "SP"
        elif node_str.startswith("tp"):
            return "TP"
        else:
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
    except Exception as pivot_err:
        st.info("💡 Inventory matrix is populating. Assign statuses to your hardware to generate totals.")
        
    st.markdown("---")

    st.markdown("### 📋 Current Asset Allocation Matrix")

    if "last_selected_node" not in st.session_state:
        st.session_state["last_selected_node"] = None
    if "active_selected_node_record" not in st.session_state:
        st.session_state["active_selected_node_record"] = None

    ed_key = "node_registry_editor"
    if ed_key in st.session_state and "edited_rows" in st.session_state[ed_key]:
        changed_rows = st.session_state[ed_key]["edited_rows"]
        newly_checked = [idx for idx, changes in changed_rows.items() if changes.get("Select") == True]
        
        if newly_checked and not df.empty:
            latest_idx = newly_checked[-1]
            if latest_idx != st.session_state["last_selected_node"]:
                st.session_state["last_selected_node"] = latest_idx
                
                rec_dict = df.loc[latest_idx].drop(["hours_hidden"], errors='ignore').to_dict()
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
                if col != "Select":
                    style_canvas.loc[i, col] = color_style
        return style_canvas

    unit_mode, unit_label = get_unit_labels()
    
    def get_pos_label(row):
        if pd.notnull(row.get('Depth')) and row.get('Depth') != 0:
            return f"{row['Depth']}ft"
        return f"Bank {row['Bank']}" if pd.notnull(row.get('Bank')) and str(row.get('Bank')).strip() != "" else "-"

    df['Position'] = df.apply(get_pos_label, axis=1)
    df['Current Temp'] = df['last_temp'].apply(lambda x: fmt_temp(x, unit_mode, unit_label))

    edited_df = st.data_editor(
        df.style.apply(node_selector_styler, axis=None) if not df.empty else df,
        hide_index=True,
        use_container_width=True,
        column_config={
            "Select": st.column_config.CheckboxColumn("Select", default=False, required=True),
            "Project": "Project",
            "Location": "Location",
            "NodeNum": "Node ID",
            "Position": "Depth/Bank",
            "Last Seen": st.column_config.TextColumn("Last Seen", help="Hours since last server telemetry ping"),
            "Current Temp": "Current Temp",
        },
        disabled=[col for col in df.columns if col != "Select"],
        column_order=["Select", "Project", "Location", "NodeNum", "Position", "Last Seen", "Current Temp"], 
        key=ed_key
    )

    if st.session_state["active_selected_node_record"] is not None:
        selected_returned_row = st.session_state["active_selected_node_record"].copy()
        if "Select" in selected_returned_row:
            del selected_returned_row["Select"]
    else:
        selected_returned_row = None
            
    st.markdown("---")
    with st.expander("🧨 Danger Zone: Sync Playground Staging Table Directly to Production"):
        st.error("⚠️ CRITICAL WARNING: This action will completely erase ALL records in your live production `node_registry` and overwrite them with an exact snapshot copy of your `node_registry_dummy` table.")
        
        confirm_token = st.text_input(
            "Type out 'OVERWRITE' to authorize replacing your production environment data models:", 
            value="", 
            key="force_production_overwrite_token_input"
        )
        
        if st.button("💥 Wipe Production & Clone Playground Table", type="primary", use_container_width=True):
            if confirm_token.strip() != "OVERWRITE":
                st.error("Authorization token verification failed. Action aborted.")
            else:
                prod_table = f"{PROJECT_ID}.{DATASET_ID}.node_registry"
                dummy_table = f"{PROJECT_ID}.{DATASET_ID}.node_registry_dummy"
                
                job_config = bigquery.QueryJobConfig(
                    write_disposition="WRITE_TRUNCATE",
                    destination=prod_table
                )
                
                sql = f"SELECT * FROM `{dummy_table}`"
                
                try:
                    with st.spinner("Executing complete environment teardown and reconstruction workflows..."):
                        query_job = client.query(sql, job_config=job_config)
                        query_job.result()
                        
                    st.success("🔥 Production registry completely reset and replaced with dummy playground snapshot!")
                    st.cache_data.clear()
                    time.sleep(1.5)
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to copy staging parameters: {e}")
                    st.code(sql, language="sql")
                    
    return selected_returned_row



##############################            
# - 7. PAGE: CLIENT PORTAL - #
##############################

def render_client_portal(selected_project, project_metadata, display_tz, unit_mode, unit_label, active_refs):
    """
    Client-facing portal with approved thermal trends and vertical profiles.
    Includes Theoretical Goal overlays and professional chart borders.
    """
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to view client data.")
        return

    # 1. METADATA & HEADER
    meta = project_metadata if isinstance(project_metadata, dict) else {}
    display_name = meta.get('ProjectName', selected_project)
    project_status = meta.get('ProjectStatus', 'Active')
    f_start_date = pd.to_datetime(meta.get('Date_Freezedown')).date() if pd.notnull(meta.get('Date_Freezedown')) else None
    
    asbuilt_filename = meta.get('AsBuiltFile')
    registry_disclaimer = meta.get('ClientDisclaimer') 

    st.markdown(f"## 📊 {display_name}")
    st.markdown(f"<p style='color: #6d6d6d; font-size: 18px; margin-top: -15px;'>Status: {project_status}</p>", unsafe_allow_html=True)

    if pd.notnull(registry_disclaimer) and str(registry_disclaimer).strip() != "":
        st.info(f"ℹ️ {registry_disclaimer}")

    # 2. DATA FETCHING (CLIENT MODE)
    with st.spinner("Synchronizing official records..."):
        p_df = get_universal_portal_data(selected_project, view_mode="client")
    
    if p_df.empty:
        st.warning(f"⚠️ No approved data records available for {display_name} yet.")
        return

    # 3. TABS
    tab_time, tab_depth, tab_table, tab_built = st.tabs([
        "📈 Timeline Analysis", "📏 Depth Profile", "📋 Summary Table", "🗺️ As-Built Plan"
    ])

    # --- TAB 1: TIMELINE ANALYSIS ---
    with tab_time:
        st.sidebar.subheader("📅 Portal View Options")
        weeks_view = st.sidebar.slider("Timeline Span (Weeks)", 1, 12, 6, key="client_weeks_slider")
        show_ref = st.sidebar.toggle("Show Progress Goals", value=True)
        
        # Calculate viewport
        now_utc = pd.Timestamp.now(tz='UTC')
        start_view = now_utc - timedelta(weeks=weeks_view)
        
        locations = sorted(
            [str(loc) for loc in p_df['Location'].dropna().unique()], 
            key=natural_sort_key
        )
        for loc in locations:
            with st.expander(f"📍 {loc} Thermal Trend", expanded=True):
                loc_data = p_df[p_df['Location'] == loc].copy()
                
                # --- CRITICAL FIX: Build the search ID and pass f_start_date ---
                clean_proj_id = str(selected_project).split('-')[0]
                cid = f"{clean_proj_id}-{loc}" if show_ref else None

                fig = build_high_speed_graph(
                    df=loc_data, 
                    title=f"{loc}: {weeks_view}-Week Trend", 
                    start_view=start_view, 
                    end_view=now_utc, 
                    active_refs=active_refs, 
                    unit_mode=unit_mode, 
                    unit_label=unit_label, 
                    display_tz=display_tz,
                    f_start_date=f_start_date, # Passed from metadata
                    curve_id=cid
                )
                st.plotly_chart(fig, use_container_width=True, key=f"portal_grid_{loc}")
                
    # --- TAB 2: DEPTH PROFILE ---
    with tab_depth:
        st.subheader("📏 Vertical Temperature Profile")
        p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
        depth_only = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
        
        if depth_only.empty:
            st.info("Vertical profile data is not available for this project.")
        else:
            x_range = [-20, 40] if unit_mode == "Celsius" else [-10, 80]
            
            for loc in sorted(depth_only['Location'].unique()):
                with st.expander(f"📏 Temp vs Depth - {loc}", expanded=True):
                    loc_data = depth_only[depth_only['Location'] == loc].copy()
                    fig_d = go.Figure()
                    
                    # Weekly Snapshots
                    mondays = pd.date_range(end=pd.Timestamp.now(tz='UTC'), periods=4, freq='W-MON')
                    for m_date in mondays:
                        target_ts = m_date.replace(hour=6, minute=0, second=0)
                        window = loc_data[(loc_data['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & 
                                         (loc_data['timestamp'] <= target_ts + pd.Timedelta(hours=12))]
                        
                        if not window.empty:
                            snap_df = window.assign(diff=(window['timestamp'] - target_ts).abs()).sort_values(['NodeNum', 'diff']).drop_duplicates('NodeNum').sort_values('Depth_Num')
                            c_temps = snap_df['temperature'] if unit_mode == "Fahrenheit" else (snap_df['temperature'] - 32) * 5/9
                            
                            fig_d.add_trace(go.Scatter(
                                x=c_temps, y=snap_df['Depth_Num'], 
                                mode='lines+markers', name=target_ts.strftime('%m/%d/%y'),
                                line=dict(shape='spline', smoothing=0.5)
                            ))

                    # --- ADD THEORETICAL GOAL TO DEPTH CHART ---
                    if show_ref and f_start_date:
                        try:
                            client = get_bq_client()
                            today_day = (pd.Timestamp.now().date() - f_start_date).days
                            ref_q = f"SELECT Temp FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` WHERE UPPER(CurveID) LIKE UPPER('%{selected_project}-{loc}%') AND Day <= {today_day} ORDER BY Day DESC LIMIT 1"
                            res = client.query(ref_q).to_dataframe()
                            if not res.empty:
                                goal_temp = res.iloc[0]['Temp'] if unit_mode == "Fahrenheit" else (res.iloc[0]['Temp'] - 32) * 5/9
                                fig_d.add_vline(x=goal_temp, line_dash="dot", line_color="Red", annotation_text="Target Goal")
                        except: pass

                    max_d = depth_only['Depth_Num'].max()
                    y_limit = int(((max_d // 10) + 1) * 10) if pd.notnull(max_d) else 50
                    
                    fig_d.update_layout(
                        plot_bgcolor='white', height=600,
                        # FULL BOARDER
                        xaxis=dict(title=f"Temp ({unit_label})", range=x_range, showline=True, mirror=True, linecolor='black'),
                        yaxis=dict(title="Depth (ft)", range=[y_limit, 0], showline=True, mirror=True, linecolor='black'),
                        legend=dict(orientation="h", y=-0.2, xanchor="center", x=0.5)
                    )
                    st.plotly_chart(fig_d, use_container_width=True, key=f"portal_depth_{loc}")

    # --- TAB 3: SUMMARY TABLE ---
    with tab_table:
        latest = p_df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        latest['Position'] = latest.apply(lambda r: f"{r['Depth']} ft" if pd.notnull(r.get('Depth')) else (f"Bank {r['Bank']}" if pd.notnull(r.get('Bank')) else "Surface"), axis=1)
        st.dataframe(latest[['Location', 'Position', 'temperature', 'timestamp']].sort_values(['Location', 'Position']), use_container_width=True, hide_index=True)

    # --- TAB 4: AS-BUILT PLAN ---
    with tab_built:
        if pd.notnull(asbuilt_filename):
            st.image(f"assets/asbuilts/{asbuilt_filename}", caption=f"Site Plan: {display_name}")
        else:
            st.info("The as-built site plan is currently being processed.")
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
                
                -- Fallbacks for hardware specific signal logging if present in schemas
                AVG(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN CAST(NULL AS FLOAT64) END) as rssi_last_val,
                AVG(CAST(NULL AS FLOAT64)) as rssi_avg_val
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
            COALESCE(s.rssi_last_val, -99.0) as rssi_last,
            COALESCE(s.rssi_avg_val, -99.0) as rssi_avg
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

        # 2. INTERACTIVE SIDEBAR/PAGE DRILLDOWN FILTERS
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

        # 3. CALCULATE LATENCY METRICS
        def get_latency_strings(row):
            ping = row['last_ping']
            if pd.isnull(ping): 
                return "❌ Never", "Never"
            
            ping_utc = ping if ping.tzinfo else ping.tz_localize('UTC')
            diff_mins = (now_utc - ping_utc).total_seconds() / 60.0
            
            if diff_mins <= 15: cat = "🟢 0-15 Mins"
            elif diff_mins <= 60: cat = "🟡 15-60 Mins"
            elif diff_mins <= 1440: cat = "⏳ < 24 Hours"
            else: cat = "🔴 > 24 Hours"
            
            if diff_mins < 60: time_str = f"{int(diff_mins)}m ago"
            elif diff_mins < 1440: time_str = f"{round(diff_mins/60, 1)}h ago"
            else: time_str = f"{int(diff_mins/1440)}d ago"
            
            return cat, time_str

        df[['Latency_Cat', 'Time_Ago']] = df.apply(lambda x: pd.Series(get_latency_strings(x)), axis=1)
        
        unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
        def format_temperatures(val):
            if pd.isnull(val): return "N/A"
            c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
            return f"{round(c_val, 1)}{unit_label}"

        # 4. CALCULATE Reporting Performance Percentages
        # Baseline target rates: 4 pings/hour ideal for standard intervals
        df['efficiency_pct'] = (df['count_24h'] / 96.0) * 100.0
        df['efficiency_pct'] = df['efficiency_pct'].clip(upper=100.0)

        # 5. MATRIX COMPILATION
        display_df = pd.DataFrame({
            "Node ID": df['NodeNum'],
            "Location": df['Location'],
            "Position": df.apply(lambda r: f"{r['Depth']}ft" if pd.notnull(r['Depth']) else f"Bank {r['Bank']}", axis=1),
            "Current Temp": df['last_temp'].apply(format_temperatures),
            "Status": df['SensorStatus'],
            "Last Seen": df['Time_Ago'],
            "Last Temp": df['last_temp'].apply(format_temperatures),
            "Pings (1h)": df['count_1h'].astype(int),
            "Pings (6h)": df['count_6h'].astype(int),
            "Pings (24h)": df['count_24h'].astype(int),
            "RSSI Last": df['rssi_last'].apply(lambda x: f"{int(x)} dBm" if x != -99.0 else "N/A"),
            "RSSI Avg": df['rssi_avg'].apply(lambda x: f"{int(x)} dBm" if x != -99.0 else "N/A"),
            "Performance": df.apply(lambda r: "Stable" if r['count_1h'] >= 2 else "Intermittent" if r['count_1h'] > 0 else "Stale", axis=1),
            "Reporting Efficiency": df['efficiency_pct']
        })

        # Sort Order Rules
        order = ["❌ Never", "🔴 > 24 Hours", "⏳ < 24 Hours", "🟡 15-60 Mins", "🟢 0-15 Mins"]
        df['Latency_Cat'] = pd.Categorical(df['Latency_Cat'], categories=order, ordered=True)
        display_df = display_df.iloc[df.sort_values('Latency_Cat').index].reset_index(drop=True)

        st.dataframe(
            display_df,
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
# PAGE MODULE: 🛠️ NODE MANAGER
# =============================================================================

def render_node_selector(reg_df, proj_list):
    """
    Renders an active inventory node selection engine with integrated 
    Last Seen reporting, project uptime efficiencies, and a fleet hardware status matrix.
    """
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

    if 'hours_hidden' in df.columns:
        df['hours_hidden'] = pd.to_numeric(df['hours_hidden'], errors='coerce').fillna(float('inf'))
        df = df.sort_values(by='hours_hidden', ascending=True).reset_index(drop=True)
    else:
        df['hours_hidden'] = float('inf')

    st.markdown("### 📡 Hardware Inventory Fleet Breakdown")
    
    def classify_hardware_family(node):
        node_str = str(node).lower()
        if "-ch" in node_str:
            return "Lord"
        elif node_str.startswith("sp"):
            return "SP"
        elif node_str.startswith("tp"):
            return "TP"
        else:
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
    except Exception as pivot_err:
        st.info("💡 Inventory matrix is populating. Assign statuses to your hardware to generate totals.")
        
    st.markdown("---")

    st.markdown("### 📋 Current Asset Allocation Matrix")

    if "last_selected_node" not in st.session_state:
        st.session_state["last_selected_node"] = None
    if "active_selected_node_record" not in st.session_state:
        st.session_state["active_selected_node_record"] = None

    ed_key = "node_registry_editor"
    if ed_key in st.session_state and "edited_rows" in st.session_state[ed_key]:
        changed_rows = st.session_state[ed_key]["edited_rows"]
        newly_checked = [idx for idx, changes in changed_rows.items() if changes.get("Select") == True]
        
        if newly_checked and not df.empty:
            latest_idx = newly_checked[-1]
            if latest_idx != st.session_state["last_selected_node"]:
                st.session_state["last_selected_node"] = latest_idx
                
                rec_dict = df.loc[latest_idx].drop(["hours_hidden"], errors='ignore').to_dict()
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
                if col != "Select":
                    style_canvas.loc[i, col] = color_style
        return style_canvas

    unit_mode, unit_label = get_unit_labels()
    
    def get_pos_label(row):
        if pd.notnull(row.get('Depth')) and row.get('Depth') != 0:
            return f"{row['Depth']}ft"
        return f"Bank {row['Bank']}" if pd.notnull(row.get('Bank')) and str(row.get('Bank')).strip() != "" else "-"

    df['Position'] = df.apply(get_pos_label, axis=1)
    df['Current Temp'] = df['last_temp'].apply(lambda x: fmt_temp(x, unit_mode, unit_label))

    edited_df = st.data_editor(
        df.style.apply(node_selector_styler, axis=None) if not df.empty else df,
        hide_index=True,
        use_container_width=True,
        column_config={
            "Select": st.column_config.CheckboxColumn("Select", default=False, required=True),
            "Project": "Project",
            "Location": "Location",
            "NodeNum": "Node ID",
            "Position": "Depth/Bank",
            "Last Seen": st.column_config.TextColumn("Last Seen", help="Hours since last server telemetry ping"),
            "Current Temp": "Current Temp",
        },
        disabled=[col for col in df.columns if col != "Select"],
        column_order=["Select", "Project", "Location", "NodeNum", "Position", "Last Seen", "Current Temp"], 
        key=ed_key
    )

    if st.session_state["active_selected_node_record"] is not None:
        selected_returned_row = st.session_state["active_selected_node_record"].copy()
        if "Select" in selected_returned_row:
            del selected_returned_row["Select"]
    else:
        selected_returned_row = None
            
    st.markdown("---")
    with st.expander("🧨 Danger Zone: Sync Playground Staging Table Directly to Production"):
        st.error("⚠️ CRITICAL WARNING: This action will completely erase ALL records in your live production `node_registry` and overwrite them with an exact snapshot copy of your `node_registry_dummy` table.")
        
        confirm_token = st.text_input(
            "Type out 'OVERWRITE' to authorize replacing your production environment data models:", 
            value="", 
            key="force_production_overwrite_token_input"
        )
        
        if st.button("💥 Wipe Production & Clone Playground Table", type="primary", use_container_width=True):
            if confirm_token.strip() != "OVERWRITE":
                st.error("Authorization token verification failed. Action aborted.")
            else:
                prod_table = f"{PROJECT_ID}.{DATASET_ID}.node_registry"
                dummy_table = f"{PROJECT_ID}.{DATASET_ID}.node_registry_dummy"
                
                job_config = bigquery.QueryJobConfig(
                    write_disposition="WRITE_TRUNCATE",
                    destination=prod_table
                )
                
                sql = f"SELECT * FROM `{dummy_table}`"
                
                try:
                    with st.spinner("Executing complete environment teardown and reconstruction workflows..."):
                        query_job = client.query(sql, job_config=job_config)
                        query_job.result()
                        
                    st.success("🔥 Production registry completely reset and replaced with dummy playground snapshot!")
                    st.cache_data.clear()
                    time.sleep(1.5)
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to copy staging parameters: {e}")
                    st.code(sql, language="sql")
                    
    return selected_returned_row
    
#######################
# Page: Data Processing #
#######################

def render_data_processing_page(selected_project):
    """
    Page Name: Data Processing
    Handles manual file ingestion, data masking limits filters, wide-format engineering exports,
    and houses the Theoretical Reference Curve Library engine components.
    """
    st.header("⚙️ Data Processing & Reference Engine")
    
    client = get_bq_client()
    if client is None:
        st.error("Database connection unavailable.")
        return
        
    tab_upload, tab_ref_library, tab_export = st.tabs(["📄 Upload Telemetry", "📈 Ref Curve Library", "📥 Export Report"])
    
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
                        df_processed['timestamp'] = pd.to_datetime(df_melted[time_col], format='mixed')
                        df_processed['NodeNum'] = df_melted['NodeNum'].str.strip().str.replace(':', '-')
                        df_processed['temperature'] = pd.to_numeric(df_melted['temperature'], errors='coerce')

                    # BRANCH B: Lord SensorCloud (Standard Long Format)
                    elif any(k in clean_headers for k in ['channel', 'node']) and any('time' in h for h in clean_headers):
                        st.info("Detected Format: Lord (Standard Long)")
                        time_h = actual_headers[next(i for i, h in enumerate(clean_headers) if 'time' in h)]
                        node_h = actual_headers[next(i for i, h in enumerate(clean_headers) if 'channel' in h or 'node' in h)]
                        temp_h = [h for h in actual_headers if 'temp' in h.lower()][0]
                        df_processed['timestamp'] = pd.to_datetime(df_raw[time_h], format='mixed')
                        df_processed['NodeNum'] = df_raw[node_h].str.strip().str.replace(':', '-')
                        df_processed['temperature'] = pd.to_numeric(df_raw[temp_h], errors='coerce')

                    # BRANCH C: SensorPush
                    else:
                        st.info("Detected Format: SensorPush")
                        t_match = [h for h in actual_headers if 'timestamp' in h.lower()][0]
                        v_match = [h for h in actual_headers if 'temp' in h.lower()][0]
                        
                        clean_name = u_file.name.replace(".csv", "").replace(".xlsx", "")
                        match = re.search(r'^([^ \(\)]+)', clean_name)
                        
                        df_processed['timestamp'] = pd.to_datetime(df_raw[t_match], format='mixed')
                        df_processed['temperature'] = pd.to_numeric(df_raw[v_match], errors='coerce')
                        df_processed['NodeNum'] = match.group(1).strip() if match else "Unknown"

                    # 3. AUTOMATED LIMITS FILTER RUNROOM
                    if not df_processed.empty:
                        df_processed = df_processed.dropna(subset=['timestamp', 'temperature'])
                        
                        # Apply strict industrial limit filtering rules right during ingestion
                        bad_mask = (df_processed['temperature'] > 120) | (df_processed['temperature'] < -30)
                        df_processed['approve'] = 'TRUE'
                        df_processed.loc[bad_mask, 'approve'] = 'BADDATA'
                        
                        bad_count = bad_mask.sum()
                        if bad_count > 0:
                            st.warning(f"⚠️ Sanity Filter: Flagged {bad_count} records exceeding -30°F to 120°F boundary lines as BADDATA.")
                        
                        st.success(f"✅ Prepared {len(df_processed)} records for Node(s): {', '.join(df_processed['NodeNum'].unique())}")
                        
                        is_lord = "-" in str(df_processed['NodeNum'].iloc[0])
                        target_table = "raw_lord" if is_lord else "raw_sensorpush"
                        
                        if st.button(f"🚀 Upload to {target_table}"):
                            with st.spinner("Writing to BigQuery..."):
                                table_id = f"{PROJECT_ID}.{DATASET_ID}.{target_table}"
                                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                                client.load_table_from_dataframe(df_processed, table_id, job_config=job_config).result()
                                st.success("Upload Complete!")
                                st.cache_data.clear() 

            except Exception as e:
                st.error(f"Ingestion Failed: {e}")

    # --- TAB 2: REFERENCE CURVE LIBRARY ---
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

    # --- TAB 3: EXPORT LOGIC ---
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
######################
# Page: Admin Tools  #
######################

def render_admin_page(selected_project, display_tz, unit_mode, unit_label, active_refs):
    """
    Advanced Admin Tools: Centralized administrative command center.
    All sidebar sub-navigation radio buttons have been removed. Layout routing 
    is handled cleanly via the core multi-page navigation selectbox.
    """
    st.header("🛠️ Admin Tools")
    
    client = get_bq_client()
    if client is None: 
        st.error("Database connection unavailable.")
        return

    # 1. CENTRAL TRANSACTIONAL DATA FETCH
    try:
        reg_q = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.node_registry`"
        full_reg_df = client.query(reg_q).to_dataframe()
        
        proj_reg_q = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.project_registry`"
        proj_reg_df = client.query(proj_reg_q).to_dataframe()
    except Exception as e:
        st.error(f"Registry Link Offline: {e}")
        return

    # 2. SEAMLESS SUB-TAB CONTAINER LAYOUT
    tab_admin_sum, tab_bulk_app, tab_recovery, tab_proj_master, tab_bulk_config = st.tabs([
        "📋 Admin Summary", 
        "⚡ Bulk Approval", 
        "📡 Data Recovery", 
        "⚙️ Project Master", 
        "📦 Bulk Updates"
    ])

    # --- SUB-TAB 1: ADMIN SUMMARY ---
    with tab_admin_sum:
        st.subheader("📋 Centralized Infrastructure Status Overview")
        
        active_nodes_df = full_reg_df[full_reg_df['End_Date'].isna()].copy()
        total_live_pool = active_nodes_df['NodeNum'].nunique()
        
        m_col1, m_col2 = st.columns(2)
        m_col1.metric("Total Active Sensors Currently in Use", f"{total_live_pool} Units")
        
        st.markdown("### 🏗️ Active Deployment Overview Matrix")
        try:
            summary_summary_q = f"""
                SELECT 
                    n.Project,
                    COUNT(DISTINCT n.NodeNum) as Total_Mapped_Sensors,
                    COUNT(DISTINCT CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN n.NodeNum END) as Active_Seen_24h
                FROM `{PROJECT_ID}.{DATASET_ID}.node_registry` n
                LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.master_data_view` m ON n.NodeNum = m.NodeNum
                WHERE n.End_Date IS NULL
                GROUP BY n.Project
                ORDER BY n.Project ASC
            """
            sum_summary_df = client.query(summary_summary_q).to_dataframe()
            st.dataframe(sum_summary_df, use_container_width=True, hide_index=True)
        except Exception as e:
            st.caption(f"Asset runtime metrics loading: {e}")

    # --- SUB-TAB 2: BULK APPROVAL ---
    with tab_bulk_app:
        st.subheader("⚡ Bulk Approval & System Maintenance")
        
        m_mode = st.radio("Select Maintenance Action:", ["Range-Based Bulk Approval", "Targeted Dataset Masking (Soft Hide)", "Database Cleanup Compression"], horizontal=True)
        
        if m_mode == "Range-Based Bulk Approval":
            st.markdown("##### ✅ Range-Based Mass Data Verification")
            active_locs = sorted(full_reg_df[full_reg_df['Project'] == selected_project]['Location'].unique())
            sel_loc = st.selectbox("Target Site Location Scope", ["All Locations"] + active_locs, key="bulk_app_loc_sel")
            
            c1, c2 = st.columns(2)
            b_s = c1.date_input("Approval Window Start Date", value=datetime.now() - timedelta(days=7), key="bulk_app_s_date")
            b_e = c2.date_input("Approval Window End Date", value=datetime.now(), key="bulk_app_e_date")
            
            if st.button("🚀 Execute Bulk Validation Append", use_container_width=True):
                loc_f = f"AND n.Location = '{sel_loc}'" if sel_loc != "All Locations" else ""
                sql = f"""
                    INSERT INTO `{OVERRIDE_TABLE}` (NodeNum, timestamp, approve)
                    SELECT DISTINCT r.NodeNum, TIMESTAMP_TRUNC(r.timestamp, HOUR), 'TRUE'
                    FROM (
                        SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` 
                        UNION ALL 
                        SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
                    ) AS r
                    INNER JOIN `{PROJECT_ID}.{DATASET_ID}.node_registry` AS n ON r.NodeNum = n.NodeNum
                    WHERE n.Project = '{selected_project}' {loc_f} 
                    AND r.timestamp BETWEEN '{b_s}' AND '{b_e}'
                    AND NOT EXISTS (
                        SELECT 1 FROM `{OVERRIDE_TABLE}` x 
                        WHERE x.NodeNum = r.NodeNum AND x.timestamp = TIMESTAMP_TRUNC(r.timestamp, HOUR)
                    )
                """
                client.query(sql).result()
                st.success("Batch approval modification set successfully completed.")
                st.cache_data.clear()

        elif m_mode == "Targeted Dataset Masking (Soft Hide)":
            st.markdown("##### 🚫 Targeted Core Overrides Manager")
            
            mc1, mc2, mc3 = st.columns(3)
            target_scope = mc1.radio("Target Scope Selection", ["Project Wide", "Specific Location", "Specific Node"], horizontal=True)
            current_status_filter = mc2.selectbox("Filter Current Designation:", ["All Records", "TRUE", "BadData", "Masked", "Office"])
            new_status = mc3.selectbox("Set Approval Field Overwrite Status To:", ["TRUE", "BadData", "Masked", "Office"])
            
            st.divider()
            
            fc1, fc2, fc3 = st.columns(3)
            temporal_dir = fc1.selectbox("Temporal Filtering Direction", ["Between Range", "Older Than", "Newer Than"])
            if temporal_dir == "Between Range":
                s_date = fc2.date_input("Start Filter Date", value=datetime.now().date() - timedelta(days=7), key="mask_s_date")
                s_time = fc2.time_input("Start Time (Exact Mins)", value=datetime.min.time(), key="mask_s_time")
                e_date = fc3.date_input("End Filter Date", value=datetime.now().date() - timedelta(days=7), key="mask_e_date")
                e_time = fc3.time_input("End Time (Exact Mins)", value=datetime.max.time(), key="mask_e_time")
            else:
                s_date = fc2.date_input("Anchor Point Date", value=datetime.now().date() - timedelta(days=7), key="mask_single_date")
                s_time = fc2.time_input("Anchor Point Time", value=datetime.min.time(), key="mask_single_time")
                e_date, e_time = None, None

            v_filter = fc1.selectbox("Thermal Boundaries Constraint Value Filter", ["No Threshold", "Above Threshold", "Below Threshold"], key="mask_v_filter")
            threshold = fc2.number_input("Threshold Temperature Value Field (°F)", value=100.0, key="mask_threshold_input")

            scope_val = None
            if target_scope == "Project Wide":
                scope_val = selected_project
            elif target_scope == "Specific Location":
                u_locs = sorted(full_reg_df[full_reg_df['Project'] == selected_project]['Location'].unique().tolist(), key=natural_sort_key)
                scope_val = fc3.selectbox("Target Location Scope Dropdown", u_locs, key="mask_loc_scoped_dropdown")
            elif target_scope == "Specific Node":
                u_locs = sorted(full_reg_df[full_reg_df['Project'] == selected_project]['Location'].unique().tolist(), key=natural_sort_key)
                selected_loc = fc3.selectbox("Target Location Scope Dropdown", u_locs, key="mask_sub_loc_picker")
                u_nodes = sorted(full_reg_df[(full_reg_df['Project'] == selected_project) & (full_reg_df['Location'] == selected_loc)]['NodeNum'].unique().tolist(), key=natural_sort_key)
                scope_val = fc3.selectbox("Target Node Scope Dropdown", u_nodes, key="mask_node_scoped_dropdown")

            f_bundle = {"temporal_dir": temporal_dir, "s_date": s_date, "s_time": s_time, "e_date": e_date, "e_time": e_time, "val_filter": v_filter, "threshold": threshold, "scope_val": scope_val}
            where_clause_str = build_management_where_clause(full_reg_df, selected_project, target_scope, current_status_filter, f_bundle)

            st.divider()
            
            if st.button("🔍 Step 1: Verify Matching Point Count Indicators", use_container_width=True, key="mask_verify_trigger_btn"):
                aliased_where = where_clause_str.replace("NodeNum", "t.NodeNum").replace("timestamp", "t.timestamp").replace("temperature", "t.temperature")
                status_q = f"""
                    SELECT COALESCE(r.approve, 'TRUE') as Designation, COUNT(*) as Point_Count
                    FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` t
                    LEFT JOIN `{OVERRIDE_TABLE}` r ON t.NodeNum = r.NodeNum AND t.timestamp = r.timestamp
                    WHERE {aliased_where} GROUP BY Designation
                """
                try:
                    res_df = client.query(status_q).to_dataframe()
                    if not res_df.empty:
                        st.dataframe(res_df.rename(columns={"Designation": "Current Status", "Point_Count": "Count"}), use_container_width=True, hide_index=True)
                        st.metric("Total Points Staged for Override", f"{res_df['Point_Count'].sum():,}")
                    else:
                        st.info("0 rows found matching current filtering combinations.")
                except Exception as ex:
                    st.error(f"Analysis Routine Interrupted: {ex}")

            if st.checkbox("Confirm database write execution parameters authorization statement check.", key="mask_write_auth_toggle"):
                if st.button(f"🚀 Step 2: Execute Override Modifications to '{new_status}'", use_container_width=True, key="mask_execute_run_btn"):
                    aliased_where = where_clause_str.replace("NodeNum", "t.NodeNum").replace("timestamp", "t.timestamp").replace("temperature", "t.temperature")
                    if new_status == "TRUE":
                        exec_sql = f"""
                            DELETE FROM `{OVERRIDE_TABLE}` WHERE STRUCT(NodeNum, timestamp) IN (
                                SELECT AS STRUCT t.NodeNum, t.timestamp FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` t
                                LEFT JOIN `{OVERRIDE_TABLE}` r ON t.NodeNum = r.NodeNum AND t.timestamp = r.timestamp WHERE {aliased_where}
                            )
                        """
                    else:
                        exec_sql = f"""
                            MERGE `{OVERRIDE_TABLE}` T
                            USING (
                                SELECT DISTINCT t.NodeNum, t.timestamp FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` t 
                                LEFT JOIN `{OVERRIDE_TABLE}` r ON t.NodeNum = r.NodeNum AND t.timestamp = r.timestamp WHERE {aliased_where}
                            ) S ON T.NodeNum = S.NodeNum AND T.timestamp = S.timestamp
                            WHEN MATCHED THEN UPDATE SET approve = '{new_status}'
                            WHEN NOT MATCHED THEN INSERT (NodeNum, timestamp, approve) VALUES (S.NodeNum, S.timestamp, '{new_status}')
                        """
                    try:
                        job_run = client.query(exec_sql)
                        job_run.result()
                        st.success(f"Successfully processed {job_run.num_dml_affected_rows:,} change sets inside database overrides catalog.")
                        st.cache_data.clear()
                    except Exception as err:
                        st.error(f"Transaction Rejected: {err}")

        elif m_mode == "Database Cleanup Compression":
            st.markdown("##### 🧹 Database Table Compression Toolkit")
            target_tbl = st.radio("Select Target Infrastructure Table Stream Source:", ["SensorPush", "Lord"], horizontal=True, key="maint_compression_radio")
            if st.button("🧨 Execute Hourly Index Compression Workflow", use_container_width=True, key="maint_compression_run_btn"):
                path = f"{PROJECT_ID}.{DATASET_ID}.raw_{target_tbl.lower()}"
                if target_tbl.lower() == "sensorpush":
                    sql = f"""
                        CREATE OR REPLACE TABLE `{path}` AS 
                        SELECT TIMESTAMP_TRUNC(timestamp, HOUR) as timestamp, NodeNum, ROUND(AVG(temperature), 1) as temperature, AVG(rssi) as rssi 
                        FROM `{path}` GROUP BY 1, 2
                    """
                else:
                    sql = f"""
                        CREATE OR REPLACE TABLE `{path}` AS 
                        SELECT TIMESTAMP_TRUNC(timestamp, HOUR) as timestamp, NodeNum, ROUND(AVG(temperature), 1) as temperature 
                        FROM `{path}` GROUP BY 1, 2
                    """
                try:
                    client.query(sql).result()
                    st.success("Table compression logic executed. Cleanup successful.")
                    st.cache_data.clear()
                except Exception as ex:
                    st.error(f"Compression sequence failure: {ex}")

    # --- SUB-TAB 3: DATA RECOVERY ---
    with tab_recovery:
        st.subheader("📡 Remote Data Recovery Service Hub")
        
        sp_reg = full_reg_df[(full_reg_df['NodeNum'].str.startswith('TP', na=False)) & (full_reg_df['End_Date'].isna())].copy()
        selected_nodes = render_recovery_filters(sp_reg)
        
        st.divider()
        rc1, rc2 = st.columns(2)
        start_date = rc1.date_input("Recovery Window Start Date", value=datetime.now() - timedelta(days=3), key="rec_window_s_date")
        end_date = rc2.date_input("Recovery Window End Date", value=datetime.now(), key="rec_window_e_date")
        
        if st.button("🚀 Trigger Smart Delta Recovery Pipeline Ingestion", use_container_width=True, key="recovery_pipeline_trigger_btn"):
            global INVENTORY_TABLE, TABLE_ID, BASE_URL
            INVENTORY_TABLE = "hardware_inventory"
            TABLE_ID = "raw_sensorpush"
            BASE_URL = "https://api.sensorpush.com/api/v1"
            handle_recovery_trigger(selected_nodes, start_date, end_date)

    # --- SUB-TAB 4: PROJECT MASTER ---
    with tab_proj_master:
        render_project_master_page(client, selected_project)

    # --- SUB-TAB 5: BULK UPDATES ---
    with tab_bulk_config:
        st.subheader("📦 Bulk Configuration Engine Workspace")
        
        cfg_mode = st.radio("Select Allocation Configuration Target Engine:", ["Register/Provision Batch Hardware Entries", "Batch Update Position/Depth Fields"], horizontal=True, key="bulk_cfg_engine_radio")
        target_registry_path = f"{PROJECT_ID}.{DATASET_ID}.node_registry"
        
        if cfg_mode == "Register/Provision Batch Hardware Entries":
            render_bulk_deployment_tab(client, target_registry_path)
        elif cfg_mode == "Batch Update Position/Depth Fields":
            st.markdown("##### 📋 Direct Configuration Allocation Matrix")
            render_node_selector(full_reg_df, sorted(proj_reg_df['Project'].dropna().unique().tolist()))

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
    # FIXED: Reduced arguments to match the function definition precisely
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
            tab_io, tab_ref_curves = st.tabs(["Upload & Export", "Ref Curve Library"])
            with tab_io:
                render_data_intake_page(selected_project)
            with tab_ref_curves:
                render_ref_curve_library(client, selected_project)
                
        elif page == "Admin Tools":
            st.title("🛠️ Admin Tools")
            tab_admin_sum, tab_bulk_app, tab_recovery, tab_proj_master, tab_bulk_config = st.tabs([
                "📋 Admin Summary", 
                "⚡ Bulk Approval", 
                "📡 Data Recovery", 
                "⚙️ Project Master", 
                "📦 Bulk Updates"
            ])
            
            with tab_admin_sum:
                render_admin_summary_dashboard(client, selected_project)
            with tab_bulk_app:
                render_bulk_approval_maintenance(client, selected_project)
            with tab_recovery:
                render_data_recovery_tool(client, selected_project)
            with tab_proj_master:
                render_project_master_tool(client, selected_project)
            with tab_bulk_config:
                render_bulk_config_engine(client, selected_project)
                
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
