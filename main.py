import streamlit as st
import pandas as pd
import time
import os
from app.utils import config
from app.data.processor import get_universal_portal_data, apply_sanity_filter, get_bq_client
from app.components.charts import build_high_speed_graph

# 1. UI SETUP
st.set_page_config(page_title="SoilFreeze Data Lab", page_icon="❄️", layout="wide")

# 2. SIDEBAR NAVIGATION
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

# 2. PROJECT SELECTION
selected_project = "All Projects"
project_metadata = None  

sidebar_client = get_bq_client()

if sidebar_client is not None:
    try:
        # Determine the filter based on the new toggle
        # Assumes your boolean column is named 'ShowActive'
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
            # Extract just the '2541'
            job_num = selected_project.split('-')[0].strip()
            
            # Map the dropdown selection to the raw database integer
            phase_sql = ""
            if "Phase 1" in selected_project:
                phase_sql = " AND Phase = '1' "
            elif "Phase 2" in selected_project or "Phase2" in selected_project:
                phase_sql = " AND Phase = '2' "

            # Search by job number and phase integer, ignoring the messy project strings
            pulse_q = f"""
                SELECT FORMAT_TIMESTAMP('%m/%d/%Y %H:%M UTC', MAX(timestamp)) as last_sync
                FROM `{config.MASTER_VIEW}`
                WHERE Project LIKE '{job_num}%' {phase_sql}
            """
            scope_label = f"Job {job_num} Age"

        pulse_df = sidebar_client.query(pulse_q).to_dataframe()
        
        # Guard: Check if the result is valid and not null
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

# NEW: The Ambient Toggle
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

# ... (all your existing sidebar code ends here) ...

# 3. APP LOGIC (Add this at the very bottom)
if selected_project and selected_project != "All Projects":
    
    # Calculate dates based on the lookback slider
    lookback_days = st.session_state.get("global_lookback_days", 35)
    end_date = pd.Timestamp.now()
    start_date = end_date - pd.Timedelta(days=lookback_days)
    
    # Fetch and process
    raw_data = get_universal_portal_data(selected_project)
    clean_data = apply_sanity_filter(raw_data)
    
    # 4. RENDER CHARTS
    if page == "Time vs Temp":
        # Pass your session state variables into the graph builder
        fig = build_high_speed_graph(
            df=clean_data, 
            title=selected_project, 
            start_view=start_date, 
            end_view=end_date, 
            active_refs=st.session_state.get("active_refs"),
            unit_mode=st.session_state.get("unit_mode"),
            unit_label=st.session_state.get("unit_label"),
            display_tz=st.session_state.get("display_tz"),
            f_start_date=start_date,
            curve_id=selected_project
        )
        
        if fig:
            st.plotly_chart(fig, use_container_width=True)

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

