import streamlit as st
import pandas as pd
import time
import os
import re
from app.utils import config
from app.data.processor import get_universal_portal_data, apply_sanity_filter, get_bq_client
from app.components.charts import build_high_speed_graph

# =============================================================================
# IMPORTANT: Import your other page functions here based on your file structure
# =============================================================================
from app.pages.summary import render_summary_dashboard
from app.pages.depth import render_depth_charts
from app.pages.sensors import render_sensor_status
from app.pages.diagnostics import render_node_diagnostics
from app.pages.processing import render_data_processing_page
from app.pages.admin import render_admin_page


# 1. UI SETUP
st.set_page_config(page_title="SoilFreeze Data Lab", page_icon="❄️", layout="wide")

# 2. SIDEBAR NAVIGATION
st.sidebar.title("❄️ SoilFreeze Lab")

# ---------------------------------------------------------
# 1. NAVIGATION
# ---------------------------------------------------------
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

# ---------------------------------------------------------
# 2. PROJECT SELECTION (Using original setup, patched for missing projects)
# ---------------------------------------------------------
selected_project = "All Projects"
project_metadata = None  

sidebar_client = get_bq_client()

if sidebar_client is not None:
    try:
        # We fetch all projects and the active status to avoid BigQuery casting errors hiding your list
        proj_q = f"""
            SELECT 
                CAST(Project AS STRING) as Project, 
                ProjectName, 
                Timezone, 
                ProjectStatus, 
                Date_Freezedown,
                CAST(ShowActive AS STRING) as ShowActive_Str
            FROM `{config.PROJECT_REGISTRY_TABLE}` 
            WHERE Project IS NOT NULL 
              AND TRIM(CAST(Project AS STRING)) != ''
        """
        proj_df = sidebar_client.query(proj_q).to_dataframe()
        
        # Original logic applied via Python to guarantee the dropdown isn't empty
        if not st.session_state.get('global_show_archived', False):
            valid_flags = ['TRUE', 'YES', '1', 'T']
            proj_df = proj_df[proj_df['ShowActive_Str'].fillna('').str.upper().str.strip().isin(valid_flags)]
        
        # Python fix from your original: Strip whitespace and filter out non-values
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
        # Added a fallback so the box doesn't vanish on an error
        selected_project = st.sidebar.selectbox("🎯 Active Project", ["All Projects"], key="sidebar_proj_picker_fallback")


# ---------------------------------------------------------
# 3. INTERACTIVE REFRESH TRIGGER
# ---------------------------------------------------------
if st.sidebar.button("🔄 Refresh Data", width="stretch"):
    with st.sidebar.spinner("Purging cache maps..."):
        st.cache_data.clear()
        st.toast("System cache completely cleared!", icon="🔄")
        time.sleep(0.5)
        st.rerun()


# ---------------------------------------------------------
# 4. VISIBILITY CONTROLS
# ---------------------------------------------------------
st.sidebar.header("👁️ Visibility Controls")

# 1. Archived Projects Toggle
st.session_state['global_show_archived'] = st.sidebar.checkbox(
    "Show Archived Projects", 
    value=st.session_state.get('global_show_archived', False)
)

# 2. Ambient Temp Toggle
st.session_state['global_show_ambient'] = st.sidebar.checkbox(
    "Show Ambient Temp", 
    value=st.session_state.get('global_show_ambient', True)
)

# 3. Theoretical Curve (Auto-toggles based on Project Status)
p_meta = st.session_state.get('project_metadata')
p_status = ""
try:
    if p_meta is not None:
        if hasattr(p_meta, 'get'):
            p_status = str(p_meta.get('ProjectStatus', '')).lower()
        else:
            p_status = str(p_meta['ProjectStatus']).lower()
except Exception:
    p_status = ""

default_curve = False if 'maintenance' in p_status else True 

st.session_state['global_show_ref'] = st.sidebar.checkbox(
    "Show Theoretical Curves", 
    value=st.session_state.get('global_show_ref', default_curve)
)

# 4 & 5. Independent Data Auditing Controls
st.session_state['global_show_masked'] = st.sidebar.checkbox(
    "Show Masked Data", 
    value=st.session_state.get('global_show_masked', False)
)

st.session_state['global_show_baddata'] = st.sidebar.checkbox(
    "Show Bad Data", 
    value=st.session_state.get('global_show_baddata', False)
)


# ---------------------------------------------------------
# 5. CURRENT DATA AGES
# ---------------------------------------------------------
st.sidebar.subheader("⏱️ Current Data Ages")

if sidebar_client is not None:
    try:
        if selected_project == "All Projects":
            pulse_q = f"""
                SELECT FORMAT_TIMESTAMP('%m/%d/%Y %H:%M UTC', MAX(timestamp)) as last_sync
                FROM `{config.MASTER_VIEW}`
            """
            scope_label = "Last Data"
        else:
            job_num = selected_project.split('-')[0].strip()
            
            phase_sql = ""
            if "Phase 1" in selected_project:
                phase_sql = " AND Phase = '1' "
            elif "Phase 2" in selected_project or "Phase2" in selected_project:
                phase_sql = " AND Phase = '2' "

            pulse_q = f"""
                SELECT FORMAT_TIMESTAMP('%m/%d/%Y %H:%M UTC', MAX(timestamp)) as last_sync
                FROM `{config.MASTER_VIEW}`
                WHERE Project LIKE '{job_num}%' {phase_sql}
            """
            scope_label = f"Job {job_num} Age"

        pulse_df = sidebar_client.query(pulse_q).to_dataframe()
        
        if not pulse_df.empty and pulse_df['last_sync'].iloc[0] is not None and pd.notna(pulse_df['last_sync'].iloc[0]):
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
            st.sidebar.markdown(f"**{scope_label}:** ⚠️ No Recent Sync")
            st.sidebar.write("Raw Sync Data:", pulse_df['last_sync'].iloc[0])
            
    except Exception as pulse_err:
        st.sidebar.caption(f"Pulse tracking suspended: {pulse_err}")


# ---------------------------------------------------------
# 6. TIMELINE NAVIGATION
# ---------------------------------------------------------
st.sidebar.subheader("⏳ Timeline Navigation")

show_full_dataset = st.sidebar.checkbox("🌍 See Full Data (Since Freezedown)", value=False, key="full_data_toggle")

if show_full_dataset:
    p_meta = st.session_state.get('project_metadata') or {}
    real_f_date = p_meta.get('Date_Freezedown')
    parsed_date = pd.to_datetime(real_f_date, errors='coerce')
    
    if pd.notnull(parsed_date):
        if parsed_date.tzinfo is not None:
            parsed_date = parsed_date.tz_localize(None)
            
        days_since = (pd.Timestamp.now() - parsed_date).days
        lookback = max(7, days_since + 2) 
        
        st.sidebar.caption(f"Showing ~{lookback} days of data since freezedown.")
        st.session_state["global_lookback_days"] = lookback
    else:
        st.sidebar.caption("No freezedown date set for this project. Defaulting to 90 days.")
        st.session_state["global_lookback_days"] = 90
else:
    selected_weeks = st.sidebar.slider(
        "Select History Window (Weeks)",
        min_value=1,
        max_value=12,
        value=5,  
        step=1,
        key="global_lookback_weeks_slider",
        help="Slide the point to change how many weeks of history pull into your charts."
    )
    st.session_state["global_lookback_days"] = selected_weeks * 7

# CSS customizations
st.sidebar.markdown(
    """
    <style>
        div[data-baseweb="slider"] > div > div {
            background: linear-gradient(to right, rgb(214, 39, 40) 0%, rgb(214, 39, 40) var(--slider-progress, 100%), rgb(230, 230, 230) var(--slider-progress, 100%)) !important;
        }
        div[role="slider"] {
            background-color: rgb(214, 39, 40) !important;
            border: 2px solid rgb(214, 39, 40) !important;
            box-shadow: 0px 0px 4px rgba(214, 39, 40, 0.5) !important;
        }
        div[data-testid="stDataFrame"] div[role="progressbar"] > div {
            background-color: rgb(214, 39, 40) !important;
        }
        progress::-webkit-progress-value { background: rgb(214, 39, 40) !important; }
        progress::-moz-progress-bar { background: rgb(214, 39, 40) !important; }
    </style>
    """,
    unsafe_allow_html=True
)

# ---------------------------------------------------------
# 7. UNITS
# ---------------------------------------------------------
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


# ---------------------------------------------------------
# 8. DISPLAY & TIME
# ---------------------------------------------------------
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


# ---------------------------------------------------------
# 9. REFERENCE LINES
# ---------------------------------------------------------
st.sidebar.subheader("📏 Reference Lines")
active_refs = [] 

if st.sidebar.checkbox("Freezing (32°F)", value=True, key="ref_freezing"): 
    active_refs.append((32.0, "Freezing"))
if st.sidebar.checkbox("Type B (26.6°F)", value=False, key="ref_type_b"): 
    active_refs.append((26.6, "Type B"))
if st.sidebar.checkbox("Type A (10.2°F)", value=False, key="ref_type_a"): 
    active_refs.append((10.2, "Type A"))

st.session_state["active_refs"] = tuple(active_refs)

display_tz = st.session_state.get("display_tz", "UTC")


# =============================================================================
# MASTER LAYOUT FRAMEWORK PAGE ROUTER
# =============================================================================

def natural_sort_key(text):
    return [int(c) if c.isdigit() else str(c).lower() for c in re.split(r'(\d+)', str(text))]

GLOBAL_PAGES = ["Summary", "Data Processing", "Admin Tools"]

if page in GLOBAL_PAGES:
    if page == "Summary":
        project_arg = None if selected_project == "All Projects" else selected_project
        render_summary_dashboard(project_arg, unit_label, unit_mode, display_tz)
        
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
                if st.button("Unlock Dashboard", width="stretch"):
                    if pwd == st.secrets.get("admin_password", "freeze123"):
                        st.session_state['authenticated'] = True
                        st.rerun()
                    else:
                        st.error("Invalid Password. Access Denied.")

elif selected_project != "All Projects":
    lookback_days = st.session_state.get("global_lookback_days", 35)
    end_date = pd.Timestamp.now()  
    start_date = end_date - pd.Timedelta(days=lookback_days)
    
    freeze_start_ts = start_date 
    p_meta = st.session_state.get('project_metadata') or {}
    real_f_date = p_meta.get('Date_Freezedown')
    
    parsed_date = pd.to_datetime(real_f_date, errors='coerce')
    if pd.notnull(parsed_date):
        if parsed_date.tzinfo is not None:
            freeze_start_ts = parsed_date.tz_localize(None)
        else:
            freeze_start_ts = parsed_date
            
    raw_data = get_universal_portal_data(
        selected_project, 
        lookback_days=lookback_days, 
        is_summary_page=False,
        show_masked=st.session_state.get('global_show_masked', False),
        show_baddata=st.session_state.get('global_show_baddata', False)
    )
    clean_data = apply_sanity_filter(raw_data)

    if page == "Time vs Temp":
        st.write("### 📈 Time vs Temperature Tracking")
        
        tab1, tab2 = st.tabs(["Telemetry Charts", "Site As-Builts"])
        
        with tab1:
            available_systems = sorted(
                [str(s) for s in clean_data['System'].dropna().unique() if str(s).strip().upper() not in ['NAN', 'NONE', '']], 
                key=natural_sort_key
            )
            
            selected_systems = []
            
            if len(available_systems) > 1:
                selected_systems = st.multiselect(
                    "⚙️ Filter by System (Leave blank to show all systems):", 
                    options=available_systems, 
                    default=[]  
                )
                
            display_data = clean_data.copy()
            if selected_systems:
                display_data = display_data[display_data['System'].astype(str).isin(selected_systems)]

            unique_locations = display_data['Location'].dropna().unique()
            valid_locations = [loc for loc in unique_locations if str(loc).strip().upper() != 'UNASSIGNED']
            sorted_locations = sorted(valid_locations, key=natural_sort_key)

            for loc in sorted_locations:
                loc_data = display_data[display_data['Location'] == loc]
                
                if loc_data.empty:
                    continue

                fig = build_high_speed_graph(
                    client=sidebar_client,  
                    df=loc_data, 
                    title=f"Thermal Trends: {loc}",
                    start_view=start_date, 
                    end_view=end_date, 
                    active_refs=active_refs,
                    unit_mode=unit_mode,
                    unit_label=unit_label,
                    display_tz=display_tz,
                    f_start_date=freeze_start_ts, 
                    curve_id=selected_project
                )
                
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
                    st.markdown("---")

        with tab2:
            st.subheader(f"Site As-Builts: {selected_project}")
            
            job_num = selected_project.split('-')[0].strip()
            as_builts_dir = "as_builts"
            
            if not os.path.exists(as_builts_dir):
                st.warning(f"Please create an `{as_builts_dir}` folder in your main directory.")
            else:
                found_images = []
                for file_name in os.listdir(as_builts_dir):
                    if file_name.startswith(job_num) and file_name.lower().endswith(('.png', '.jpg', '.jpeg')):
                        found_images.append(os.path.join(as_builts_dir, file_name))
                
                found_images = sorted(found_images, key=natural_sort_key)
                
                if found_images:
                    for img_path in found_images:
                        st.image(img_path, caption=os.path.basename(img_path), use_container_width=True)
                        st.markdown("---")
                else:
                    st.info(f"No as-built images found for Job {job_num}. Add them to the `{as_builts_dir}` folder using the naming convention (e.g., {job_num}.jpg).")

    elif page == "Depth Charts":
        render_depth_charts(selected_project, unit_label, display_tz)

    elif page == "Sensor Status":
        render_sensor_status(sidebar_client, selected_project, unit_label, unit_mode, display_tz)

    elif page == "Node Diagnostics":
        render_node_diagnostics(selected_project, display_tz, unit_label)

else:
    st.info(f"👈 Please select a specific project from the sidebar to view the **{page}** dashboard.")
