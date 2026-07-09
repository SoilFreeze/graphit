
#############################
# - 2. PAGE: TIME vs TEMP - #
#############################

def render_global_overview(selected_project, project_metadata, display_tz):
    """
    Shows all pipes/banks for a selected project in one scrolling view.
    Fixed: Uses enumerate(locations) to resolve NameError and DuplicateKey errors.
    """
    # 1. UI STATE
    show_ref = st.session_state.get("global_show_ref", True)
    show_masked = st.session_state.get("global_show_masked", False)
    unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
    unit_label = st.session_state.get("unit_label", "°F")
    active_refs = st.session_state.get("active_refs", [])

    # 2. METADATA
    p_name = selected_project
    status = "Active"
    f_start_date = None
    if project_metadata:
        p_name = project_metadata.get('ProjectName', selected_project)
        status = project_metadata.get('ProjectStatus', 'Active')
        raw_f_date = project_metadata.get('Date_Freezedown')
        if pd.notnull(raw_f_date):
            f_start_date = pd.to_datetime(raw_f_date).date()

    st.header(f"📈 Time vs Temp: {p_name} [{status}]")
    
    # 3. SYNC
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Select a project in the sidebar.")
        return

    p_df = get_universal_portal_data(selected_project)
    if p_df.empty:
        st.warning(f"No data found for '{p_name}'.")
        return

    # --- AUTO-FILTER BY PHASE FROM PROJECT TITLE ---
    # We strip any whitespace and handle the Phase as a STRING to match the schema
    import re
    phase_match = re.search(r'(?i)Phase\s*(\d+)', selected_project)
    
    if phase_match:
        target_phase = phase_match.group(1)
        # Using string matching explicitly since the schema defines Phase as STRING
        p_df = p_df[p_df['Phase'].astype(str).str.strip() == target_phase]
        st.caption(f"🎯 Auto-filtered to **Phase {target_phase}** based on project selection.")
    
    # --- MANUAL SYSTEM FILTER ---
    st.markdown("### 🎛️ System Filters")
    avail_systems = sorted([str(s) for s in p_df['System'].dropna().unique() if str(s).strip()])
    
    if len(avail_systems) > 1:
        sel_systems = st.multiselect("Filter by System", avail_systems, default=avail_systems)
        if sel_systems:
            p_df = p_df[p_df['System'].astype(str).isin(sel_systems)]
    elif len(avail_systems) == 1:
        st.caption(f"Showing data for System: **{avail_systems[0]}**")

    # 4. FILTERING & TIMING WINDOW
    mask_col = 'approval_status' if 'approval_status' in p_df.columns else 'approve'
    if not show_masked and mask_col in p_df.columns:
        p_df = p_df[p_df[mask_col].astype(str).str.upper() != 'MASKED'].copy()

    # Re-establishing the missing view variables here!
    lookback_weeks = st.session_state.get("global_lookback_weeks_slider", 5)
    now_local = pd.Timestamp.now(tz=display_tz)
    end_view = (now_local + pd.Timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    start_view = end_view - pd.Timedelta(weeks=lookback_weeks)

    # 5. LOCATION-BASED PLOTTING LOOP
    # Purge trash locations and Ambient entirely before building the container list
    trash_locations = ['Dead Stock', 'Elizabeth', 'Office', 'Ambient', 'AMBIENT']
    p_df = p_df[~p_df['Location'].isin(trash_locations)].copy()
    
    # One more aggressive scrub to catch any strange casing variations
    p_df = p_df[~p_df['Location'].astype(str).str.upper().str.contains('AMBIENT', na=False)]

    # Force string types, and drop any pure garbage string anomalies (Fixes the Ghost Graphs)
    p_df['Location'] = p_df['Location'].astype(str).str.strip()
    valid_locations = [loc for loc in p_df['Location'].unique() if loc.lower() not in ['nan', 'none', '', 'unassigned']]
    locations = sorted(valid_locations, key=natural_sort_key)

    for i, loc in enumerate(locations):
        loc_df = p_df[p_df['Location'] == loc].copy()
        
        if loc_df.empty:
            continue
            
        clean_proj_id = str(selected_project).split('-')[0]
        clean_loc_num = "".join(re.findall(r'\d+', loc))
        normalized_loc = f"T{clean_loc_num}" if clean_loc_num else loc
        search_id = f"{clean_proj_id}-{normalized_loc}"
        is_temp_pipe = not any(x in loc.upper() for x in ["SUPPLY", "RETURN", "BANK S", "BANK R", "AMB"])

        # Generate the figure in memory FIRST
        fig = build_high_speed_graph(
            df=loc_df, 
            title=f"Thermal Trends: {loc}", 
            start_view=start_view, 
            end_view=end_view, 
            active_refs=active_refs, 
            unit_mode=unit_mode, 
            unit_label=unit_label, 
            display_tz=display_tz,
            mobile_mode=False, 
            f_start_date=f_start_date,
            curve_id=search_id if (show_ref and is_temp_pipe) else None
        )
        
        # Only draw the expander UI if the graph actually successfully generated valid data lines
        if fig is not None and hasattr(fig, 'data') and len(fig.data) > 0:
            with st.expander(f"📍 Location: {loc}", expanded=True):
                st.plotly_chart(
                    fig, 
                    use_container_width=True, 
                    key=f"tvt_{selected_project}_{loc}_{i}"
                )
