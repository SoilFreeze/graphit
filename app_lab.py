import streamlit as st
import pandas as pd
import time
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import re
import numpy as np

# 1. CONFIGURATION & STYLING
st.set_page_config(
    page_title="SoilFreeze Data Lab", 
    page_icon="❄️", 
    layout="wide"
)

# Global Database Constants - Linked to Read-Only Infrastructure
DATASET_ID = "Temperature" 
PROJECT_ID = "sensorpush-export"

# Schema-Aligned Table References
PROJECT_REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.project_registry"
NODE_REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.node_registry"
MASTER_VIEW = f"{PROJECT_ID}.{DATASET_ID}.master_data_view"
REF_CURVE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.reference_curves"

@st.cache_resource
def get_bq_client():
    """
    Initializes and caches the BigQuery connection.
    Includes mandatory Google Drive scopes for federated Google Sheet tables.
    """
    try:
        # THE FIX: Both BigQuery and Drive scopes are required for external tables
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
# - 2. READ-ONLY DATA ENGINE - #
############################

@st.cache_data(ttl=600)
def get_universal_portal_data(project_id):
    """
    Unified Data Fetcher.
    Pulls directly from master_data_view. Write operations have been deprecated.
    """
    client = get_bq_client()
    if client is None: return pd.DataFrame()
    
    clean_token = str(project_id).replace("'", "''").strip()
    base_job_num = clean_token.split('-')[0].strip()

    # Sanitized query pulling only from the consolidated view
    query = f"""
        SELECT 
            m.Project,
            m.NodeNum,
            m.temperature,
            m.timestamp,
            m.approval_status,
            COALESCE(m.Location, 'Unassigned') as Location,
            COALESCE(m.Bank, '—') as Bank,
            m.Depth
        FROM `{MASTER_VIEW}` m
        WHERE m.temperature >= -30.0 AND m.temperature <= 120.0
          AND (m.Project = @project_id 
               OR m.Project LIKE '{base_job_num}%')
        ORDER BY m.timestamp ASC
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("project_id", "STRING", project_id)]
    )
    
    try:
        return client.query(query, job_config=job_config).to_dataframe()
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
        proj_q = f"""
            SELECT 
                CAST(Project AS STRING) as Project, 
                ProjectName, 
                Timezone, 
                ProjectStatus, 
                Date_Freezedown
            FROM `{PROJECT_REGISTRY_TABLE}` 
            WHERE Project IS NOT NULL 
              AND TRIM(CAST(Project AS STRING)) != ''
              AND (
                  UPPER(TRIM(CAST(ShowActive AS STRING))) IN ('TRUE', 'YES', '1') 
                  OR UPPER(CAST(Project AS STRING)) LIKE '%OFFICE%'
              )
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
                           display_tz="UTC", mobile_mode=False, f_start_date=None, curve_id=None, 
                           allowed_nodes=None): # <-- 1. Add allowed_nodes parameter
    """
    Engineering-grade Trend Graph.
    - Added: 'allowed_nodes' filter to prevent cross-phase data contamination.
    """
    if df.empty: return go.Figure().update_layout(title="No data available")

    # 2. FILTER DATA IMMEDIATELY
    plot_df = df.copy()
    if allowed_nodes is not None:
        plot_df = plot_df[plot_df['NodeNum'].isin(allowed_nodes)]
    
    if plot_df.empty: return go.Figure().update_layout(title="No nodes in this project phase")                  
                               
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
    # Only iterate through nodes that survived the filter
    for sn in sorted(plot_df['NodeNum'].unique(), key=natural_sort_key): 
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

    # This line must be aligned with the 'for sn in plot_df...' line above
    sorted_node_configs = sorted(node_metadata, key=lambda x: natural_sort_key(x['sort_key']))

    # This for loop must also be aligned with the 'for sn...' line
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
    
    # Implementing the inverted Y-axis preference dynamically if required by project scope
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

##################
# High temp mask #
##################
def apply_sanity_filter(df):
    """
    Automated filter for rogue data points.
    - Removes entries with null sensor names to ensure integrity.
    - Flags anything outside physical limits [-30°F, 120°F] as BADDATA.
    - Masks dynamic outliers +/- 20°F from the sensor line's average.
    """
    if df.empty:
        return df

    # Enforce strict data integrity before processing averages
    if 'NodeNum' in df.columns:
        df = df.dropna(subset=['NodeNum']).copy()

    if df.empty:
        return df

    # 1. Absolute Physical Limits -> BADDATA
    bad_condition = (df['temperature'] > 120) | (df['temperature'] < -30)
    
    # 2. Dynamic Relative Outliers -> MASKED
    if 'NodeNum' in df.columns:
        node_means = df.groupby('NodeNum')['temperature'].transform('mean')
        outlier_condition = (df['temperature'] > node_means + 20) | (df['temperature'] < node_means - 20)
    else:
        avg_temp = df['temperature'].mean()
        outlier_condition = (df['temperature'] > avg_temp + 20) | (df['temperature'] < avg_temp - 20)

    # Determine the active status column in the current view
    mask_col = 'approve' if 'approve' in df.columns else 'approval_status' if 'approval_status' in df.columns else None
    
    if mask_col:
        # Apply dynamic line outliers first
        df.loc[outlier_condition, mask_col] = 'MASKED'
        # Overwrite with absolute physical failures as a higher priority flag
        df.loc[bad_condition, mask_col] = 'BADDATA'

    return df


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
    # --- ADDED: Filter p_df to ONLY contain nodes assigned to the selected_project ---
    # We retrieve the specific list of nodes assigned to this project
    valid_nodes_for_project = full_reg_df[full_reg_df['Project'] == selected_project]['NodeNum'].unique().tolist()
    p_df = p_df[p_df['NodeNum'].isin(valid_nodes_for_project)].copy()
    
    # Now sort the locations based on the valid nodes for this specific project
    locations = sorted(
        [str(loc) for loc in p_df['Location'].dropna().unique()], 
        key=natural_sort_key
    )

    for loc in locations:
        # We define loc_nodes to ensure this expander only handles nodes for this location in THIS project
        loc_nodes = p_df[(p_df['Location'] == loc)]['NodeNum'].unique().tolist()
        
        with st.expander(f"📍 Location: {loc}", expanded=True):
            loc_df = p_df[(p_df['Location'] == loc) & (p_df['NodeNum'].isin(loc_nodes))].copy()
            
            clean_proj_id = str(selected_project).split('-')[0]
            
            # NORMALIZATION EXTRACTION: Converts "TP2", "TP-2", or "T2" into clean "T2"
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
            
##############################
# Page 1 - Dashboard Summary #
##############################
def render_summary_dashboard(unit_label, unit_mode, display_tz):
    """
    Renders Global Project Summary.
    Updated: Points to live Google Sheet tables and ensures column names match production schema.
    """
    st.header("🌐 Global Project Summary")
    
    client = get_bq_client()
    if client is None: return

    mobile_mode = st.session_state.get("mobile_optimized_toggle", False)

    summary_q = f"""
            WITH active_projects AS (
                SELECT 
                    CAST(Project AS STRING) as Project, 
                    ProjectName, 
                    ProjectStatus, 
                    Date_Freezedown,
                    REGEXP_EXTRACT(TRIM(CAST(Project AS STRING)), r'^\\d+') as base_prefix
                FROM `{PROJECT_REGISTRY_TABLE}`
                WHERE UPPER(TRIM(CAST(ShowActive AS STRING))) IN ('TRUE', 'YES', '1')
                  AND UPPER(CAST(Project AS STRING)) NOT LIKE '%OFFICE%'
            ),
        raw_data AS (
            SELECT 
                p.Project, n.Bank, n.Location, n.Depth, m.temperature, m.timestamp, m.NodeNum
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
            INNER JOIN active_projects p 
                ON REGEXP_EXTRACT(TRIM(CAST(m.Project AS STRING)), r'^\\d+') = p.base_prefix
            LEFT JOIN `{NODE_REGISTRY_TABLE}` n 
              ON REGEXP_REPLACE(UPPER(TRIM(CAST(m.NodeNum AS STRING))), r'[:-]', '') = 
                 REGEXP_REPLACE(UPPER(TRIM(CAST(n.NodeNum AS STRING))), r'[:-]', '')
            WHERE m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
              AND m.NodeNum IS NOT NULL
              AND UPPER(COALESCE(CAST(m.approval_status AS STRING), 'PENDING')) NOT IN ('BADDATA', 'FALSE', '0')
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
        st.info("No active projects found matching your active parameter checks.")
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
            
            # Prioritize valid numeric Depths for TempPipes first to stop multi-channel Lord nodes from slipping into brines
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
              AND m.NodeNum IS NOT NULL
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
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.master_data_view` m 
          ON n.NodeNum = m.NodeNum AND m.NodeNum IS NOT NULL
        WHERE n.Project = @proj_id AND (n.End_Date IS NULL OR TRIM(CAST(n.End_Date AS STRING)) = '')
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
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.master_data_view` m 
          ON n.NodeNum = m.NodeNum AND m.NodeNum IS NOT NULL
        WHERE n.Project = @proj_id AND (n.End_Date IS NULL OR TRIM(CAST(n.End_Date AS STRING)) = '')
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
                    
    return selected_returned_row
# =============================================================================
# Page: Data Processing
# =============================================================================

def render_data_processing_page(selected_project):
    """
    Page Name: Data Processing
    Handles manual file ingestion, data masking limits filters, wide-format engineering exports,
    and Theoretical Reference Curve Library.
    Write operations to external Google Sheet tables (Events, Chiller Registry) have been deprecated.
    """
    st.header("⚙️ Data Processing & Reference Engine")
    
    client = get_bq_client()
    if client is None:
        st.error("Database connection unavailable.")
        return
        
    # Standardized 5-tab layout order matching blueprint specifications
    tab_upload, tab_export, tab_ref_library = st.tabs([
        "📄 Upload Telemetry", 
        "📥 Export Report",
        "📈 Ref Curve Library"
    ])
    
    # --- TAB 1: UPLOAD LOGIC ---
    with tab_upload:
        st.subheader("📄 Manual File Ingestion")
        st.info("Supports: Lord SensorConnect (Wide), Lord SensorCloud (Long), and Native SensorPush formats.")
        
        u_files = st.file_uploader("Select CSV or Excel files", type=['csv', 'xlsx'], key="manual_upload_main", accept_multiple_files=True) 
    
        if u_files:
            all_processed_dfs = []
            target_table = None
    
            # 1. PROCESS ALL FILES
            for f in u_files:
                try:
                    # Format Detection
                    is_sensorconnect, skip_rows = False, 0
                    if f.name.endswith('.csv'):
                        f.seek(0)
                        for i, line in enumerate(f):
                            if b"DATA_START" in line:
                                is_sensorconnect, skip_rows = True, i + 1
                                break
                        f.seek(0)
                    
                    # Reading
                    if is_sensorconnect:
                        df_raw = pd.read_csv(f, encoding='latin1', skiprows=skip_rows, dtype=str)
                    elif f.name.endswith('.csv'):
                        df_raw = pd.read_csv(f, encoding='latin1', dtype=str)
                    else:
                        df_raw = pd.read_excel(f, dtype=str)
    
                    # Processing
                    if not df_raw.empty:
                        df_processed = pd.DataFrame()
                        actual_headers = list(df_raw.columns)
                        clean_headers = [str(h).strip().lower() for h in actual_headers]
                        
                        # Branching Logic
                        if is_sensorconnect:
                            time_col = [h for h in actual_headers if 'time' in h.lower()][0]
                            value_vars = [h for h in actual_headers if h != time_col]
                            df_melted = df_raw.melt(id_vars=[time_col], value_vars=value_vars, var_name='NodeNum', value_name='temperature')
                            df_processed['timestamp'] = pd.to_datetime(df_melted[time_col], errors='coerce', utc=True)
                            df_processed['NodeNum'] = df_melted['NodeNum'].str.strip().str.replace(':', '-')
                            df_processed['temperature'] = pd.to_numeric(df_melted['temperature'], errors='coerce')
                            target_table = "raw_lord"
                        
                        elif any(k in clean_headers for k in ['channel', 'node']) and any('time' in h for h in clean_headers):
                            time_h = actual_headers[next(i for i, h in enumerate(clean_headers) if 'time' in h)]
                            node_h = actual_headers[next(i for i, h in enumerate(clean_headers) if 'channel' in h or 'node' in h)]
                            temp_h = [h for h in actual_headers if 'temp' in h.lower()][0]
                            df_processed['timestamp'] = pd.to_datetime(df_raw[time_h], errors='coerce', utc=True)
                            df_processed['NodeNum'] = df_raw[node_h].str.strip().str.replace(':', '-')
                            df_processed['temperature'] = pd.to_numeric(df_raw[temp_h], errors='coerce')
                            target_table = "raw_lord"
                            
                        else:
                            t_match = next((h for h in actual_headers if 'timestamp' in h.lower()), None)
                            v_match = next((h for h in actual_headers if 'temp' in h.lower()), None)
                            if t_match and v_match:
                                clean_name = f.name.replace(".csv", "").replace(".xlsx", "")
                                match = re.search(r'^([^ \(\)]+)', clean_name)
                                df_processed['timestamp'] = pd.to_datetime(df_raw[t_match], errors='coerce', utc=True)
                                df_processed['temperature'] = pd.to_numeric(df_raw[v_match], errors='coerce')
                                df_processed['NodeNum'] = match.group(1).strip() if match else clean_name
                                target_table = "raw_sensorpush"
                        
                        # Sanity & Storage
                        if not df_processed.empty:
                            df_processed = df_processed.dropna(subset=['timestamp', 'temperature'])
                            all_processed_dfs.append(df_processed)
                            st.write(f"✅ Prepared {f.name}: {len(df_processed)} records.")
    
                except Exception as e:
                    st.error(f"❌ Error processing {f.name}: {e}")

            # 2. BATCH UPLOAD (Outside the loop)
            if all_processed_dfs and target_table:
                combined_df = pd.concat(all_processed_dfs, ignore_index=True)
                combined_df['temperature'] = combined_df['temperature'].round(1)
                
                if st.button(f"🚀 Commit {len(combined_df)} records to {target_table}"):
                    with st.spinner("Writing to BigQuery..."):
                        table_id = f"{PROJECT_ID}.{DATASET_ID}.{target_table}"
                        job_config = bigquery.LoadJobConfig(
                            schema=[
                                bigquery.SchemaField("timestamp", "TIMESTAMP"),
                                bigquery.SchemaField("NodeNum", "STRING"),
                                bigquery.SchemaField("temperature", "FLOAT"), 
                            ],
                            write_disposition="WRITE_APPEND"
                        )
                        client.load_table_from_dataframe(combined_df[['timestamp', 'NodeNum', 'temperature']], table_id, job_config=job_config).result()
                        st.success("Batch Upload Complete!")
                        st.cache_data.clear()

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
                table_ref = f"{PROJECT_ID}.{DATASET_ID}.reference_curves"
        
                for idx, f in enumerate(u_files):
                    try:
                        curve_id = f.name.replace(".csv", "")
                        
                        # Simplified encoding handling
                        f.seek(0)
                        try:
                            ref_df = pd.read_csv(f, skiprows=2, names=['Day', 'Temp'], encoding='utf-8')
                        except UnicodeDecodeError:
                            f.seek(0)
                            ref_df = pd.read_csv(f, skiprows=2, names=['Day', 'Temp'], encoding='latin-1')
        
                        # Data validation
                        ref_df['Day'] = pd.to_numeric(ref_df['Day'], errors='coerce')
                        ref_df['Temp'] = pd.to_numeric(ref_df['Temp'], errors='coerce')
                        ref_df = ref_df.dropna(subset=['Day', 'Temp'])
                        
                        if ref_df.empty:
                            st.error(f"❌ {f.name} contained no valid numeric data.")
                            continue
        
                        ref_df['CurveID'] = curve_id
        
                        # Atomic Update: Delete old and Load new
                        client.query(f"DELETE FROM `{table_ref}` WHERE CurveID='{curve_id}'").result()
                        
                        job_config = bigquery.LoadJobConfig(
                            schema=[
                                bigquery.SchemaField("Day", "INTEGER"),
                                bigquery.SchemaField("Temp", "FLOAT"),
                                bigquery.SchemaField("CurveID", "STRING"),
                            ],
                            write_disposition="WRITE_APPEND"
                        )
                        
                        client.load_table_from_dataframe(ref_df, table_ref, job_config=job_config).result()
                        st.toast(f"Success: {curve_id}", icon="✅")
                                    
                    except Exception as e:
                        st.error(f"❌ Error processing {f.name}: {e}")
                    
                    progress_bar.progress((idx + 1) / len(u_files))
                
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


def execute_bulk_approval_workspace(client, full_reg_df, selected_project):
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
        proj_q = f"SELECT CAST(Project AS STRING) as Project, ProjectName, Timezone, ProjectStatus, Date_Freezedown FROM `{PROJECT_REGISTRY_TABLE}` WHERE ShowActive IS TRUE"
        full_reg_df = client.query(f"SELECT * FROM `{NODE_REGISTRY_TABLE}` WHERE End_Date IS NULL OR TRIM(CAST(End_Date AS STRING)) = ''").to_dataframe()
        available_projects_list = sorted(client.query(proj_q).to_dataframe()['Project'].dropna().unique().tolist())
    except Exception as e: st.error(f"Registry Link Offline: {e}"); return

    # Standardized Navigation Tabs Layout Schema Paths (Registry & Chiller Tabs Removed)
    tab_admin_sum, tab_bulk_app, tab_recovery, tab_proj_master = st.tabs([
        "📋 Admin Summary", "⚡ Bulk Approval", "📡 Data Recovery", "⚙️ Project Master"
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
            sum_q = f"SELECT p.Project, p.ProjectName, p.ProjectStatus, p.Date_Freezedown, COUNT(DISTINCT n.NodeNum) as Mapped_Sensors, COUNT(DISTINCT CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR) THEN n.NodeNum END) as Active_6h, COUNT(DISTINCT CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN n.NodeNum END) as Active_24h FROM `{PROJECT_REGISTRY_TABLE}` p LEFT JOIN `{NODE_REGISTRY_TABLE}` n ON p.Project = n.Project LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.master_data_view` m ON n.NodeNum = m.NodeNum WHERE (n.End_Date IS NULL OR TRIM(CAST(n.End_Date AS STRING)) = '') AND p.ShowActive IS TRUE AND UPPER(p.Project) NOT LIKE '%OFFICE%' GROUP BY 1,2,3,4 ORDER BY p.Project ASC"
            rows = []
            for _, r in client.query(sum_q).to_dataframe().iterrows():
                elapsed = max(0, (pd.Timestamp.now(tz=display_tz).date() - pd.to_datetime(r['Date_Freezedown']).date()).days) if pd.notnull(r['Date_Freezedown']) else 0
                rows.append({"Project ID": r['Project'], "Project Name": r['ProjectName'] or r['Project'], "Mapped Sensors": int(r['Mapped_Sensors']), "Active (6h)": int(r['Active_6h']), "Active (24h)": int(r['Active_24h']), "Project Status Timeline": f"Day {elapsed} of {str(r['ProjectStatus']).title()}" if pd.notnull(r['Date_Freezedown']) else "Not Freezing"})
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        except Exception as e: st.error(f"Overview compilation fault: {e}")

    # --- SUB-TAB 2: BULK APPROVAL SYSTEM RUNROOM ---
    with tab_bulk_app:
        execute_bulk_approval_workspace(client, full_reg_df, selected_project)
        
    # --- SUB-TAB 3: SENSORPUSH API CLOUD RECOVERY BACKFILL ENGINE ---
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

    # --- SUB-TAB 4: PROJECT LIFECYCLE HISTORY DIRECTORY ---
    with tab_proj_master:
        st.subheader("🗄️ Complete Master Project Lifecycle Directory")
        st.dataframe(client.query(f"SELECT Project as `Project ID`, ProjectName as `Friendly Name`, ProjectStatus as `Operational Phase`, Date_Freezedown as `Freezedown Date`, City, Timezone FROM `{PROJECT_REGISTRY_TABLE}` ORDER BY Project ASC").to_dataframe(), use_container_width=True, hide_index=True)

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
    # Ensure this function exists in your code or is removed if deprecated
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
