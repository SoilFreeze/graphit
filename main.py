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
# Example paths provided below, adjust as needed!
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

# PAGE NAVIGATION
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

# PROJECT SELECTION
selected_project = "All Projects"
project_metadata = None  

sidebar_client = get_bq_client()

if sidebar_client is not None:
    try:
        # Determine the filter based on the toggle
        status_filter = "" if st.session_state.get('global_show_archived', False) else "AND UPPER(TRIM(CAST(ShowActive AS STRING))) IN ('TRUE', 'YES', '1')"

        proj_q = f"""
            SELECT 
                CAST(Project AS STRING) as Project, 
                ProjectName, 
                Timezone, 
                ProjectStatus, 
                Date_Freezedown
            FROM `{config.PROJECT_REGISTRY_TABLE}` 
            WHERE Project IS NOT NULL 
              AND TRIM(CAST(Project AS STRING)) != ''
              {status_filter}
        """
        proj_df = sidebar_client.query(proj_q).to_dataframe()
        
        # Python fix: Strip whitespace and filter out non-values
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

# INTERACTIVE REFRESH TRIGGER
if st.sidebar.button("🔄 Refresh Data", use_container_width=True):
    with st.sidebar.spinner("Purging cache maps..."):
        st.cache_data.clear()
        st.toast("System cache completely cleared!", icon="🔄")
        time.sleep(0.5)
        st.rerun()

# 3. GLOBAL VIEW TOGGLES & INTERACTIVE LOOKBACK
st.sidebar.subheader("👁️ Visibility Controls")

show_archived = st.sidebar.toggle(
    "Show Archived Projects", 
    value=False, 
    key="global_show_archived",
    help="Display all historic projects in the project selection menu."
)

st.sidebar.toggle(
    "Show Ambient Temp", 
    value=True, 
    key="global_show_ambient",
    help="Overlay ambient air temperature on the charts."
)

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

display_tz = st.session_state.get("display_tz", "UTC")

# =============================================================================
# MASTER LAYOUT FRAMEWORK PAGE ROUTER
# =============================================================================

# Define a sorting helper to ensure proper numerical sequencing (T1, T2, T3... instead of T1, T10, T2)
def natural_sort_key(text):
    return [int(c) if c.isdigit() else str(c).lower() for c in re.split(r'(\d+)', str(text))]

# 1. DEFINE GLOBAL PAGES
GLOBAL_PAGES = ["Summary", "Data Processing", "Admin Tools"]

# 2. RENDER GLOBAL PAGES (Load regardless of project selection)
if page in GLOBAL_PAGES:
    if page == "Summary":
        # Pass None as selected_project if it's "All Projects"
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
                if st.button("Unlock Dashboard", use_container_width=True):
                    if pwd == st.secrets.get("admin_password", "Freeze123!!"):
                        st.session_state['authenticated'] = True
                        st.rerun()
                    else:
                        st.error("Invalid Password. Access Denied.")

# 3. RENDER PROJECT-SPECIFIC PAGES (Only load if a project is selected)
elif selected_project != "All Projects":
    # Calculate dates once for project pages
    lookback_days = st.session_state.get("global_lookback_days", 35)
    end_date = pd.Timestamp.now()
    start_date = end_date - pd.Timedelta(days=lookback_days)
    
    # Fetch and process the data for the selected project
    raw_data = get_universal_portal_data(selected_project)
    clean_data = apply_sanity_filter(raw_data)

    if page == "Time vs Temp":
        unique_locations = clean_data['Location'].dropna().unique()
        sorted_locations = sorted(unique_locations, key=natural_sort_key)

        # Loop through each location and build its own graph
        for loc in sorted_locations:
            if str(loc).strip().upper() == 'UNASSIGNED':
                continue
                
            loc_data = clean_data[clean_data['Location'] == loc]
            
            if loc_data.empty:
                continue

            fig = build_high_speed_graph(
                df=loc_data, 
                title=f"Thermal Trends: {loc}",
                start_view=start_date, 
                end_view=end_date, 
                active_refs=active_refs,
                unit_mode=unit_mode,
                unit_label=unit_label,
                display_tz=display_tz,
                f_start_date=start_date,
                curve_id=selected_project
            )
            
            if fig:
                st.plotly_chart(fig, use_container_width=True)
                st.markdown("---") 

    elif page == "Depth Charts":
        render_depth_charts(selected_project, unit_label, display_tz)

    elif page == "Sensor Status":
        render_sensor_status(sidebar_client, selected_project, unit_label, unit_mode, display_tz)

    elif page == "Node Diagnostics":
        render_node_diagnostics(selected_project, display_tz, unit_label)

# 4. FALLBACK
else:
    st.info(f"👈 Please select a specific project from the sidebar to view the **{page}** dashboard.")
