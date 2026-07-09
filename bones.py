
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
    
    Parameters:
    -----------
    client : bigquery.Client
        Authenticated Google Cloud BigQuery client instance.
    full_reg_df : pandas.DataFrame
        The full sensor node registry dataset mapping nodes to active hardware configurations.
    selected_project : str
        The current active project context token filtered out of the sidebar app menu.
    tab_logistics : streamlit.tabs
        Bubble handle routing to pass downstream context states across layouts.
    """
    # Establish explicit table paths mapped directly out of your data view catalog
    target_table = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections" 
    telemetry_table = f"{PROJECT_ID}.{DATASET_ID}.master_data_view_v2" 

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
    clean_col1, clean_col2, clean_col3 = st.columns(3)
    
    with clean_col1:
        st.write("##### 📊 Telemetry Aggregation & Hourly Flattening")
        st.caption("Truncates raw timestamps to the hour, filters bad logs, and collapses records to an average value.")
        run_telemetry_cleanup = st.button("⚡ Run Global Database Cleanup & Hourly Consolidation", use_container_width=True)
        
    with clean_col2:
        st.write("##### 🧼 Approval String Casing Standardization")
        st.caption("Scans the rejections table to convert any lowercase 'true/false' strings to standard 'TRUE/FALSE'.")
        run_string_cleanup = st.button("🧹 Clean Approval Text 'true' to 'TRUE'", use_container_width=True)

    with clean_col3:
        st.write("##### 🧠 Smart TempPipe Spike Filter")
        st.caption("Auto-masks TempPipe readings that jump >5°F from adjacent records.")
        run_smart_filter = st.button("🤖 Run TempPipe Smart Masking", use_container_width=True)

    

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
            
            # Targets the data override source table directly (`manual_rejections`)
            # Converts lower or mixed-case string variants safely into standard uppercase 'TRUE' or 'FALSE'
            str_cleanup_sql = f"""
                UPDATE `{target_table}`
                SET approve = UPPER(TRIM(approve))
                WHERE LOWER(approve) IN ('true', 'false')
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

    # --- PATHWAY C: SMART FILTER ANOMALY MASKING ---
    if run_smart_filter:
        status_box_smart = st.empty()
        try:
            status_box_smart.markdown("🧠 **Scanning TempPipes for >5°F anomalies...**")
            
            # Uses LAG (previous) and LEAD (next) to compare chronological neighbors
            spike_sql = f"""
                MERGE `{PROJECT_ID}.{DATASET_ID}.manual_rejections` T
                USING (
                    WITH OrderedData AS (
                        SELECT 
                            NodeNum, 
                            timestamp, 
                            temperature,
                            LAG(temperature) OVER(PARTITION BY NodeNum ORDER BY timestamp) as prev_temp,
                            LEAD(temperature) OVER(PARTITION BY NodeNum ORDER BY timestamp) as next_temp
                        FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view_v2`
                        -- Isolate TempPipes: Has depth, not ambient, not a bank
                        WHERE Depth IS NOT NULL 
                          AND TRIM(CAST(Depth AS STRING)) != '' 
                          AND UPPER(CAST(Location AS STRING)) NOT LIKE '%AMB%'
                    )
                    SELECT DISTINCT NodeNum, timestamp
                    FROM OrderedData
                    WHERE (prev_temp IS NOT NULL AND ABS(temperature - prev_temp) > 5.0)
                       OR (next_temp IS NOT NULL AND ABS(temperature - next_temp) > 5.0)
                ) S
                ON T.NodeNum = S.NodeNum AND T.timestamp = S.timestamp
                WHEN MATCHED THEN UPDATE SET approve = 'MASKED'
                WHEN NOT MATCHED THEN INSERT (NodeNum, timestamp, approve) VALUES (S.NodeNum, S.timestamp, 'MASKED')
            """
            
            job = client.query(spike_sql)
            job.result()
            
            status_box_smart.empty()
            st.success(f"🎉 Smart Filter applied! Successfully masked {job.num_dml_affected_rows:,} anomalous TempPipe records.")
            st.cache_data.clear()
            time.sleep(0.5)
            st.rerun()
            
        except Exception as e:
            status_box_smart.empty()
            st.error(f"Smart Filter Operation Failed: {e}")
    
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
    st.info(f"Target Designation Status for selected coordinates: **{new_status}**")
    
    # Step 2: Form Checkbox and Execution Engine Block
    if st.checkbox("I authorize updating these data markers to the target parameters specified.", key="confirm_blk_mgmt"):
        if st.button(f"🚀 Step 2: Execute Status Override to {new_status}", key="exec_blk_mgmt_btn", use_container_width=True):
            
            # PATH A: If target override is TRUE, drop tracking tokens entirely out of the rejections table so they re-approve
            if new_status == "TRUE":
                sql = f"""
                    DELETE FROM `{target_table}`
                    WHERE STRUCT(NodeNum, timestamp) IN (
                        SELECT AS STRUCT t.NodeNum, t.timestamp 
                        FROM `{telemetry_table}` t
                        WHERE {aliased_where}
                    )
                """
            # PATH B: If target override is a custom flag (FALSE, BADDATA, MASK), merge row coordinates into manual_rejections
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
            sum_q = f"SELECT p.Project, p.ProjectName, p.ProjectStatus, p.Date_Freezedown, COUNT(DISTINCT n.NodeNum) as Mapped_Sensors, COUNT(DISTINCT CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR) THEN n.NodeNum END) as Active_6h, COUNT(DISTINCT CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN n.NodeNum END) as Active_24h FROM `{PROJECT_REGISTRY_TABLE}` p LEFT JOIN `{NODE_REGISTRY_TABLE}` n ON p.Project = n.Project LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.master_data_view_v2` m ON n.NodeNum = m.NodeNum WHERE (n.End_Date IS NULL OR TRIM(CAST(n.End_Date AS STRING)) = '') AND p.ShowActive IS TRUE AND UPPER(p.Project) NOT LIKE '%OFFICE%' GROUP BY 1,2,3,4 ORDER BY p.Project ASC"
            rows = []
            for _, r in client.query(sum_q).to_dataframe().iterrows():
                elapsed = max(0, (pd.Timestamp.now(tz=display_tz).date() - pd.to_datetime(r['Date_Freezedown']).date()).days) if pd.notnull(r['Date_Freezedown']) else 0
                rows.append({"Project ID": r['Project'], "Project Name": r['ProjectName'] or r['Project'], "Mapped Sensors": int(r['Mapped_Sensors']), "Active (6h)": int(r['Active_6h']), "Active (24h)": int(r['Active_24h']), "Project Status Timeline": f"Day {elapsed} of {str(r['ProjectStatus']).title()}" if pd.notnull(r['Date_Freezedown']) else "Not Freezing"})
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        except Exception as e: st.error(f"Overview compilation fault: {e}")

    # --- SUB-TAB 2: BULK APPROVAL SYSTEM RUNROOM ---
    with tab_bulk_app:
        execute_bulk_approval_workspace(client, full_reg_df, selected_project)
        
    # -------------------------------------------------------------------------
    # --- SUB-TAB 3: SENSORPUSH API CLOUD RECOVERY BACKFILL ENGINE ---
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    with tab_recovery:
        st.title("📡 Data Recovery Engine")
        st.write(
            "Extract raw chronological data streams directly from the SensorPush Cloud API architecture "
            "and execute a direct batch-load insert into your primary production table layers."
        )
        st.divider()

        # 1. RENDER STREAMLINED HIERARCHICAL SEARCH DROPDOWNS
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
        if dropdown_selected_nodes:
            final_target_nodes = dropdown_selected_nodes
        else:
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

        # Initialize tracking flags inside session state to survive reruns safely
        if 'recovery_run_complete' not in st.session_state:
            st.session_state['recovery_run_complete'] = False
        if 'recovery_cached_rows' not in st.session_state:
            st.session_state['recovery_cached_rows'] = []
        if 'recovery_cached_stats' not in st.session_state:
            st.session_state['recovery_cached_stats'] = {}

        # 5. TRIGGER EXECUTION PIPELINE BUTTON
        if st.button("🚀 Execute Cloud Backfill Ingestion Pipeline Run", use_container_width=True, key="btn_trigger_recovery_run"):
            import requests
            import numpy as np
            
            all_rows = []
            hardware_map = {}
            reverse_hardware_map = {}
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
                        reverse_hardware_map[friendly_name] = clean_db_id
                        if friendly_name in node_stats:
                            node_stats[friendly_name] = 0
                except Exception as e:
                    st.error(f"Failed to query inventory map tables: {e}")
                    st.stop()

                st.write("📅 Checking historical system check-in history benchmarks...")
                try:
                    time_q = f"SELECT NodeNum, FORMAT_TIMESTAMP('%m/%d/%Y %H:%M UTC', MAX(timestamp)) as max_time FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view_v2` GROUP BY NodeNum"
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
                        samples_payload = {"startTime": start_time_iso, "endTime": end_time_iso, "limit": 100000}
                        r_samples = requests.post(f"{LOCAL_API_URL}/samples", headers={"Authorization": token}, json=samples_payload, timeout=60).json()

                        st.write(f"DEBUG [{acc['email']}]: Found {len(r_samples.get('sensors', {}))} raw sensor payloads in API response.")
                        
                        sensors_data = r_samples.get('sensors', {})
                        if not sensors_data:
                            continue

                        for s_id, samples in sensors_data.items():
                            api_root_id = str(s_id).split('.')[0].strip()
                            friendly_name = hardware_map.get(api_root_id)
                            
                            # 🛡️ HARDENED MATCH GUARD FIX: Check both friendly name maps and Raw ID listings
                            is_target_match = False
                            if friendly_name and friendly_name in final_target_nodes:
                                is_target_match = True
                            else:
                                # Fallback check: look up if the raw API tracking reference maps back to our targeted assets list
                                for target_node in final_target_nodes:
                                    if reverse_hardware_map.get(target_node) == api_root_id:
                                        friendly_name = target_node
                                        is_target_match = True
                                        break
                                        
                            if not is_target_match:
                                continue
                                
                            if friendly_name not in node_stats:
                                node_stats[friendly_name] = 0
                                
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
                    st.session_state['recovery_run_complete'] = False
                else:
                    st.write(f"📥 Batch loading rows straight into `{LOCAL_REC_TABLE}`...")
                    try:
                        upload_df = pd.DataFrame(all_rows)
                        upload_df['timestamp'] = pd.to_datetime(upload_df['timestamp'], utc=True)
                        
                        if 'rssi' in upload_df.columns:
                            upload_df['rssi'] = pd.to_numeric(upload_df['rssi'], errors='coerce').astype(object).where(upload_df['rssi'].notnull(), None)
                        if 'temperature' in upload_df.columns:
                            upload_df['temperature'] = pd.to_numeric(upload_df['temperature'], errors='coerce')
                        
                        upload_df['NodeNum'] = upload_df['NodeNum'].astype(str).str.strip()

                        real_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{LOCAL_REC_TABLE}"
                        
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
                        
                        st.session_state['recovery_cached_rows'] = all_rows
                        st.session_state['recovery_cached_stats'] = db_max_timestamps
                        st.session_state['recovery_run_complete'] = True
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as bq_err:
                        st.error(f"Batch loading Ingestion pipeline failure: {bq_err}")
                        status_box.update(state="error")

        if st.session_state.get('recovery_run_complete'):
            st.write("### 📊 Data Recovery Tally Distribution:")
            summary_records = []
            grand_total_tally = 0
            
            cached_rows = st.session_state['recovery_cached_rows']
            cached_benchmarks = st.session_state['recovery_cached_stats']
            nodes_to_report = final_target_nodes
            
            for node in nodes_to_report:
                true_node_count = sum(1 for row in cached_rows if row["NodeNum"] == node)
                grand_total_tally += true_node_count
                last_checked_in = cached_benchmarks.get(node, "❌ No Historical Records Found")
                
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
            if grand_total_tally > 0:
                st.balloons()

# -------------------------------------------------------------------------
        # SUB-SECTION: HARDWARE AUDIT & STATUS LOOKUP
        # -------------------------------------------------------------------------
        st.divider()
        st.subheader("📋 Account Hardware Audit & Status Lookup")
        st.write("Scan all connected SensorPush cloud accounts to generate a comprehensive list of mapped hardware, physical IDs, and the last time they successfully logged data.")

        if st.button("📊 Run Fleet Account Audit", use_container_width=True, key="btn_run_account_audit"):
            import requests
            import pandas as pd

            audit_records = []
            hardware_map = {}
            db_max_timestamps = {}

            LOCAL_INV_TABLE = "hardware_inventory"
            LOCAL_API_URL = "https://api.sensorpush.com/api/v1"
            ACCOUNTS = [
                {'email': 'ldunham@soilfreeze.com', 'password': 'Freeze123!!'},
                {'email': 'tsteele@soilfreeze.com', 'password': 'Freeze123!!'},
                {'email': 'soilfreeze98072@gmail.com', 'password': 'Freeze123!!'}
            ]

            with st.status("Gathering Fleet Intelligence...", expanded=True) as audit_status:
                st.write("🔍 Building Hardware Translation Maps from Database...")
                try:
                    inv_q = f"SELECT RawID, NodeNum FROM `{PROJECT_ID}.{DATASET_ID}.{LOCAL_INV_TABLE}` WHERE RawID IS NOT NULL"
                    for row in client.query(inv_q):
                        clean_id = str(row.RawID).split('.')[0].strip()
                        hardware_map[clean_id] = str(row.NodeNum).strip()
                except Exception as e:
                    st.warning(f"Could not load inventory: {e}")

                st.write("📅 Checking Database For Last Known Data Points...")
                try:
                    time_q = f"SELECT NodeNum, FORMAT_TIMESTAMP('%m/%d/%Y %H:%M UTC', MAX(timestamp)) as max_time FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view_v2` GROUP BY NodeNum"
                    for row in client.query(time_q):
                        if row.max_time:
                            db_max_timestamps[str(row.NodeNum)] = str(row.max_time)
                except Exception as e:
                    st.warning(f"Could not load timestamps: {e}")

                st.write("☁️ Polling Cloud APIs for Registered Devices...")
                for acc in ACCOUNTS:
                    acc_email = acc['email']
                    try:
                        auth_r = requests.post(f"{LOCAL_API_URL}/oauth/authorize", json=acc, timeout=15).json()
                        token = requests.post(f"{LOCAL_API_URL}/oauth/accesstoken", json={"authorization": auth_r['authorization']}, timeout=15).json().get('accesstoken')

                        s_resp = requests.post(f"{LOCAL_API_URL}/devices/sensors", headers={"Authorization": token}, json={}, timeout=20).json()

                        if isinstance(s_resp, dict):
                            for s_id, s_meta in s_resp.items():
                                raw_physical_id = str(s_id).split('.')[0].strip()
                                node_num = hardware_map.get(raw_physical_id, "⚠️ Unmapped/Unknown")
                                last_seen = db_max_timestamps.get(node_num, "❌ No Database Records")

                                audit_records.append({
                                    "Account Email": acc_email,
                                    "Node Number": node_num,
                                    "Physical ID (RawID)": raw_physical_id,
                                    "App Friendly Name": s_meta.get('name', 'Unknown'), 
                                    "Last Database Check-In": last_seen
                                })
                    except Exception as e:
                        st.error(f"Failed to poll account {acc_email}: {e}")

                audit_status.update(label="Audit Complete!", state="complete")

            # -------------------------------------------------------------------------
            # RENDER THE AUDIT RESULTS
            # -------------------------------------------------------------------------
            if audit_records:
                st.write("### 🗄️ Fleet Audit Results")
                
                # Convert to dataframe and sort it logically
                audit_df = pd.DataFrame(audit_records).sort_values(by=["Account Email", "Node Number"])
                st.dataframe(audit_df, use_container_width=True, hide_index=True)

                # Generate CSV payload for download
                csv_payload = audit_df.to_csv(index=False).encode('utf-8')
                
                st.download_button(
                    label="⬇️ Download Audit Report as CSV",
                    data=csv_payload,
                    file_name="sensorpush_fleet_audit.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            else:
                st.info("No devices found across any accounts.")
    
    # --- SUB-TAB 4: PROJECT LIFECYCLE HISTORY DIRECTORY ---
    with tab_proj_master:
        st.subheader("🗄️ Complete Master Project Lifecycle Directory")
        st.dataframe(client.query(f"SELECT Project as `Project ID`, ProjectName as `Friendly Name`, ProjectStatus as `Operational Phase`, Date_Freezedown as `Freezedown Date`, City, Timezone FROM `{PROJECT_REGISTRY_TABLE}` ORDER BY Project ASC").to_dataframe(), use_container_width=True, hide_index=True)
##################################
# Page: Node Diagnostics (New)   #
##################################
def render_node_diagnostics(selected_project, display_tz, unit_label):
    """
    Diagnostic supervisor console for deep device investigation.
    Provides three distinct tabs: raw history inspection, phase performance metrics, 
    and real-time exception alerting.
    """
    st.header("🔬 Node Diagnostics Workspace")
    
    client = get_bq_client()
    if client is None:
        st.error("Database link offline.")
        return

    # Load down node inventory definitions for filter mappings
    try:
        # Change this in your app.py:
        reg_df = client.query("SELECT * FROM `sensorpush-export.Temperature.node_registry_synced`").to_dataframe()
    except Exception as e:
        st.error(f"Failed to fetch active registry for dropdown paths: {e}")
        return

    # Establish the three core tabs
    tab_lookup, tab_performance, tab_alerts = st.tabs([
        "🔍 Data Lookup", 
        "📊 Thermal Performance Metrics", 
        "⚠️ Node Alerts"
    ])

    # =========================================================================
    # TAB 1: DATA LOOKUP ENGINE
    # =========================================================================
    with tab_lookup:
        st.subheader("🔍 Individual Node Telemetry Inspection")
        
        # 1. Tie Project Scope to the Sidebar Context
        scope_label = "Global Fleet" if selected_project == "All Projects" else selected_project
        st.info(f"🎯 **Search Scope:** {scope_label} (Change in sidebar)")
        
        c1, c2 = st.columns([1, 1])
        with c1:
            search_mode = st.radio("Search Method", ["Filter Mappings", "Search by Node ID"], horizontal=True)
            
        target_node = None
        
        # Filter registry based on the sidebar selection
        if selected_project == "All Projects":
            proj_filtered = reg_df 
        else:
            job_num = str(selected_project).split('-')[0].strip()
            proj_filtered = reg_df[reg_df['Project'].astype(str).str.startswith(job_num)]
            
        if search_mode == "Filter Mappings":
            with c2:
                avail_locs = sorted(proj_filtered['Location'].dropna().unique().tolist(), key=natural_sort_key)
                f_loc = st.selectbox("Physical Location Context", avail_locs, key="diag_f_loc")
                
            matching_nodes = sorted(proj_filtered[proj_filtered['Location'] == f_loc]['NodeNum'].dropna().unique().tolist(), key=natural_sort_key)
            if matching_nodes:
                target_node = st.selectbox("Select Target Node to Inspect", matching_nodes, key="diag_node_select_dropdown")
            else:
                st.warning("No nodes match this configuration.")
                
        else:
            with c2:
                all_active_nodes = sorted(proj_filtered['NodeNum'].dropna().astype(str).unique().tolist(), key=natural_sort_key)
                selected_search_node = st.selectbox(
                    "Type Node ID to Search:", 
                    options=[""] + all_active_nodes,
                    index=0,
                    key="diag_direct_node_search"
                )
                if selected_search_node != "":
                    target_node = selected_search_node

        if target_node:
            st.divider()
            
            c_header, c_time = st.columns([3, 1])
            with c_header:
                st.markdown(f"##### 📈 Telemetry History for Node: `{target_node}`")
            with c_time:
                # Add dynamic timeline amounts
                time_opt = st.selectbox("Historical Window:", ["30 Days", "60 Days", "90 Days", "1 Year", "All Time"], index=0)
                
            days_map = {"30 Days": 30, "60 Days": 60, "90 Days": 90, "1 Year": 365, "All Time": 5000}
            lookback_days = days_map[time_opt]
            
            # Master read query pulling localized node history down
            node_q = f"""
                SELECT timestamp, temperature, Location, Bank, Depth, Project, SensorStatus
                FROM `{MASTER_VIEW}`
                WHERE NodeNum = @target_node
                  AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)
                ORDER BY timestamp DESC
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("target_node", "STRING", target_node),
                    bigquery.ScalarQueryParameter("lookback_days", "INTEGER", int(lookback_days))
                ]
            )
            
            with st.spinner(f"Fetching {time_opt} of node history..."):
                node_history = client.query(node_q, job_config=job_config).to_dataframe()
            
            if node_history.empty:
                st.warning(f"No telemetry data found for Node `{target_node}` in the past {time_opt}.")
            else:
                # Localize and convert time for entire dataframe first so aggregation works cleanly
                if node_history['timestamp'].dt.tz is None:
                    node_history['timestamp'] = node_history['timestamp'].dt.tz_localize('UTC')
                node_history['timestamp'] = node_history['timestamp'].dt.tz_convert(display_tz)

                # Meta overview statistics boxes
                meta_row = node_history.iloc[0]
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Current Temp", f"{meta_row['temperature']:.1f}{unit_label}")
                m2.metric("Latest Location", str(meta_row['Location']))
                m3.metric("Latest Project", str(meta_row['Project']))
                m4.metric("Scanned Records", f"{len(node_history):,}")

                # Compile the Historical Placements Table
                st.markdown("#### 🗺️ Historical Placements")
                
                # Copy and fill NA to ensure GroupBy works without dropping records
                hist_df = node_history.copy()
                hist_df[['Project', 'Location', 'Bank', 'Depth']] = hist_df[['Project', 'Location', 'Bank', 'Depth']].fillna('')
                
                placements = hist_df.groupby(['Project', 'Location', 'Bank', 'Depth']).agg(
                    First_Seen=('timestamp', 'min'),
                    Last_Seen=('timestamp', 'max'),
                    Records=('timestamp', 'count')
                ).reset_index().sort_values('Last_Seen', ascending=False)
                
                # Format coordinates and timestamps for display
                def format_pos(r):
                    if r['Depth']: return f"{r['Depth']}ft"
                    if r['Bank']: return f"Bank {r['Bank']}"
                    return "-"
                    
                placements['Position'] = placements.apply(format_pos, axis=1)
                placements['First Seen'] = placements['First_Seen'].dt.strftime('%m/%d/%Y %H:%M')
                placements['Last Seen'] = placements['Last_Seen'].dt.strftime('%m/%d/%Y %H:%M')
                
                # Reorder and display the clean matrix
                disp_placements = placements[['Project', 'Location', 'Position', 'First Seen', 'Last Seen', 'Records']]
                st.dataframe(disp_placements, use_container_width=True, hide_index=True)

                # ==========================================
                # TEMPERATURE TREND & AMBIENT TOGGLE
                # ==========================================
                st.markdown("#### 📉 Temperature Trend")
                
                # The checkbox is placed directly above the graph
                show_ambient = st.checkbox("Show Ambient Office Temperature", value=False, key="toggle_ambient_temp")
                
                # Calculate exact bounds for the chart's X-axis to force the view window
                now_ts = pd.Timestamp.now(tz=display_tz)
                start_ts = now_ts - pd.Timedelta(days=lookback_days)
                
                fig = px.line(
                    node_history, x='timestamp', y='temperature',
                    labels={'timestamp': 'Time', 'temperature': f'Temperature ({unit_label})'},
                    color_discrete_sequence=['#1f77b4']
                )

                # Fetch and Append the Ambient data to the figure if checked
                if show_ambient:
                    ambient_q = f"""
                        SELECT timestamp, temperature
                        FROM `{MASTER_VIEW}`
                        WHERE Project = 'Office' 
                          AND Location = 'Ambient'
                          AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)
                        ORDER BY timestamp DESC
                    """
                    amb_job_config = bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("lookback_days", "INTEGER", int(lookback_days))
                        ]
                    )
                    
                    with st.spinner("Fetching ambient office data..."):
                        ambient_df = client.query(ambient_q, job_config=amb_job_config).to_dataframe()
                    
                    if not ambient_df.empty:
                        # Localize timezone to match the node_history so the graph aligns perfectly
                        if ambient_df['timestamp'].dt.tz is None:
                            ambient_df['timestamp'] = ambient_df['timestamp'].dt.tz_localize('UTC')
                        ambient_df['timestamp'] = ambient_df['timestamp'].dt.tz_convert(display_tz)
                        
                        # Add the trace to the Plotly figure
                        fig.add_scatter(
                            x=ambient_df['timestamp'], 
                            y=ambient_df['temperature'],
                            mode='lines', 
                            name="Ambient Office",
                            line=dict(color='orange', dash='dot')
                        )
                    else:
                        st.toast("No ambient data found for 'Office/Ambient' in this timeframe.", icon="⚠️")
                
                fig.update_layout(plot_bgcolor='white', hovermode='x unified', height=400, margin=dict(l=0, r=0, t=20, b=0))
                
                # Force the x-axis range to strictly match the selected time window
                fig.update_xaxes(
                    range=[start_ts, now_ts],
                    showgrid=True, gridcolor='Gainsboro', showline=True, linecolor='black', mirror=True
                )
                
                fig.update_yaxes(showgrid=True, gridcolor='Gainsboro', showline=True, linecolor='black', mirror=True)
                
                # Overlay standard freezing marker reference point bounds
                freeze_pt = 0 if st.session_state.get("unit_mode") == "Celsius" else 32
                fig.add_hline(y=freeze_pt, line_width=2, line_dash="dash", line_color="RoyalBlue")
                
                st.plotly_chart(fig, use_container_width=True)

    # =========================================================================
    # TAB 2: THERMAL PERFORMANCE METRICS (UPDATED)
    # =========================================================================
    with tab_performance:
        st.subheader("📊 Ground Freezing System Performance")
        
        if selected_project == "All Projects":
            st.info("💡 Please select a specific project in the sidebar.")
        else:
            job_num = str(selected_project).split('-')[0].strip()

            st.markdown("### 🎛️ Dashboard Filters")
            
            # --- 1. TIME WINDOW FILTERS ---
            st.markdown("##### ⏳ Timeline & Baselines")
            
            t1, t2 = st.columns(2)
            with t1:
                history_weeks = st.slider(
                    "Select History Window (Weeks)", 
                    min_value=1, max_value=12, value=2
                )
            with t2:
                baseline_days = st.slider(
                    "Cluster Baseline Window (Days)", 
                    min_value=1, max_value=14, value=1,
                    help="How many days back should the baseline comparison look?"
                )
            
            lookback_days = history_weeks * 7
            baseline_seconds = baseline_days * 86400
            time_opt = f"{history_weeks} Week{'s' if history_weeks > 1 else ''}"
            
            # We must pull extra historical data so the window function has data 
            # to calculate the baseline for the very first day of your visual graph.
            total_fetch_days = lookback_days + baseline_days

            # --- 2. DYNAMIC BIGQUERY FETCH ---
            perf_q = f"""
                WITH BaseData AS (
                    SELECT 
                        NodeNum, Location, Depth, temperature AS current_temp, timestamp,
                        CASE 
                            WHEN Depth IS NOT NULL AND TRIM(CAST(Depth AS STRING)) != '' AND UPPER(CAST(Location AS STRING)) NOT LIKE '%AMB%' THEN 'TempPipe' 
                            ELSE 'Brine' 
                        END as PipeType
                    FROM `{MASTER_VIEW}`
                    WHERE Project LIKE CONCAT(@job_num, '%')
                      AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @total_fetch_days DAY)
                ),
                InstantDivergence AS (
                    SELECT 
                        *,
                        -- 1. Find the instantaneous median of the pipe at this exact second
                        PERCENTILE_CONT(current_temp, 0.5) OVER(PARTITION BY Location, PipeType, timestamp) AS peer_median
                    FROM BaseData
                ),
                RollingMetrics AS (
                    SELECT 
                        *,
                        -- 2. Calculate this node's raw distance from the median
                        current_temp - peer_median AS raw_divergence,
                        
                        -- 3. Calculate the average of that distance over the past X days
                        AVG(current_temp - peer_median) OVER(
                            PARTITION BY NodeNum 
                            ORDER BY UNIX_SECONDS(timestamp) 
                            RANGE BETWEEN @baseline_seconds PRECEDING AND CURRENT ROW
                        ) AS baseline_divergence_avg,
                        
                        -- 4. Keep the 24-hour thermal velocity for sudden spikes
                        AVG(current_temp) OVER(
                            PARTITION BY NodeNum 
                            ORDER BY UNIX_SECONDS(timestamp) 
                            RANGE BETWEEN 86400 PRECEDING AND 3600 PRECEDING
                        ) AS past_24h_avg
                    FROM InstantDivergence
                )
                SELECT 
                    *,
                    -- 5. Subtract the baseline average from the current divergence to flatten the line
                    raw_divergence - baseline_divergence_avg AS cluster_divergence,
                    
                    -- Velocity remains the same
                    current_temp - past_24h_avg AS thermal_velocity
                FROM RollingMetrics
                -- 6. Filter the final output so the graph matches the timeline slider exactly
                WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)
                ORDER BY timestamp DESC
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("job_num", "STRING", job_num),
                    bigquery.ScalarQueryParameter("total_fetch_days", "INTEGER", total_fetch_days),
                    bigquery.ScalarQueryParameter("lookback_days", "INTEGER", lookback_days),
                    bigquery.ScalarQueryParameter("baseline_seconds", "INTEGER", baseline_seconds)
                ]
            )
            
            with st.spinner(f"Fetching {time_opt} of thermodynamic arrays..."):
                try:
                    perf_df = client.query(perf_q, job_config=job_config).to_dataframe()
                    
                    if perf_df.empty:
                        st.warning(f"No telemetry samples found for this project in the past {time_opt}.")
                    else:
                        if perf_df['timestamp'].dt.tz is None:
                            perf_df['timestamp'] = perf_df['timestamp'].dt.tz_localize('UTC')
                        perf_df['timestamp'] = perf_df['timestamp'].dt.tz_convert(display_tz)
                        
                        perf_df['DisplayLabel'] = perf_df.apply(
                            lambda r: f"{r['NodeNum']} ({r['Depth']}ft)" if pd.notnull(r['Depth']) and str(r['Depth']).strip() else r['NodeNum'], 
                            axis=1
                        )

                        latest_df = perf_df.drop_duplicates(subset=['NodeNum'], keep='first').copy()
                        
                        def classify_performance_status(row):
                            if row['thermal_velocity'] >= 2.0: return "🔥 Rapid Warming (Urgent)"
                            if abs(row['cluster_divergence']) >= 4.0: return "⚠️ Thermal Drift"
                            if row['thermal_velocity'] <= -1.5: return "❄️ Freezing Active"
                            return "🟢 Stable Maintenance"
                            
                        latest_df['Operational Assessment'] = latest_df.apply(classify_performance_status, axis=1)

                        # --- 3. COMPONENT & LOCATION FILTERS ---
                        c_loc, c_node, c_pipe = st.columns([2, 3, 2])
                        
                        with c_loc:
                            unique_locations = sorted(perf_df['Location'].dropna().unique().tolist())
                            selected_location = st.selectbox("2. Select Location:", ["All Locations"] + unique_locations)
                        
                        if selected_location != "All Locations":
                            perf_filtered = perf_df[perf_df['Location'] == selected_location]
                            latest_filtered = latest_df[latest_df['Location'] == selected_location]
                        else:
                            perf_filtered = perf_df
                            latest_filtered = latest_df

                        with c_node:
                            available_nodes = sorted(latest_filtered['NodeNum'].unique().tolist())
                            default_nodes = available_nodes if selected_location != "All Locations" and available_nodes else []
                            
                            selected_nodes = st.multiselect(
                                "3. Select Specific Sensors:", 
                                options=available_nodes,
                                default=default_nodes,
                                placeholder="Select nodes to view drift charts..."
                            )

                        with c_pipe:
                            st.write("###") 
                            pipe_filter = st.radio("4. Component Type:", ["All", "Temp Pipes", "Brine Banks"], horizontal=True)

                        if selected_nodes:
                            perf_filtered = perf_filtered[perf_filtered['NodeNum'].isin(selected_nodes)]
                            latest_filtered = latest_filtered[latest_filtered['NodeNum'].isin(selected_nodes)]

                        if pipe_filter == "Temp Pipes":
                            latest_filtered = latest_filtered[latest_filtered['PipeType'] == 'TempPipe']
                        elif pipe_filter == "Brine Banks":
                            latest_filtered = latest_filtered[latest_filtered['PipeType'] == 'Brine']

                        st.divider()

                        # ==========================================
                        # GRAPHICAL SECTION: UNIFIED MASTER DASHBOARD
                        # ==========================================
                        if selected_nodes:
                            st.markdown(f"### 🌡️ {time_opt} Thermodynamic Master View")
                            
                            fig = make_subplots(
                                rows=3, cols=1, 
                                shared_xaxes=True,
                                vertical_spacing=0.08,
                                subplot_titles=(
                                    "1. Raw Temperature Telemetry", 
                                    "2. Data Spread (Distance from Pipe Baseline)", 
                                    "3. Thermal Velocity (24-Hour Rate of Change)"
                                )
                            )
                            
                            colors = px.colors.qualitative.Plotly
                            
                            for i, node in enumerate(selected_nodes):
                                node_data = perf_filtered[perf_filtered['NodeNum'] == node]
                                if node_data.empty: continue
                                    
                                label = node_data['DisplayLabel'].iloc[0]
                                line_color = colors[i % len(colors)]
                                
                                fig.add_trace(go.Scatter(x=node_data['timestamp'], y=node_data['current_temp'],
                                                         name=label, legendgroup=label, mode='lines',
                                                         line=dict(color=line_color, width=2)),
                                              row=1, col=1)
                                
                                fig.add_trace(go.Scatter(x=node_data['timestamp'], y=node_data['cluster_divergence'],
                                                         name=label, legendgroup=label, mode='lines', showlegend=False,
                                                         line=dict(color=line_color, width=2)),
                                              row=2, col=1)
                                              
                                fig.add_trace(go.Scatter(x=node_data['timestamp'], y=node_data['thermal_velocity'],
                                                         name=label, legendgroup=label, mode='lines', showlegend=False,
                                                         line=dict(color=line_color, width=2)),
                                              row=3, col=1)

                            freeze_pt = 0 if st.session_state.get("unit_mode") == "Celsius" else 32
                            fig.add_hline(y=freeze_pt, line_dash="dash", line_color="RoyalBlue", row=1, col=1)
                            
                            fig.add_hline(y=0, line_width=2, line_color="black", row=2, col=1)
                            fig.add_hline(y=4.0, line_dash="dot", line_color="orange", row=2, col=1)
                            fig.add_hline(y=-4.0, line_dash="dot", line_color="blue", row=2, col=1)
                            
                            fig.add_hline(y=0, line_width=2, line_color="black", row=3, col=1)
                            fig.add_hline(y=2.0, line_dash="dash", line_color="red", row=3, col=1)
                            fig.add_hline(y=-2.0, line_dash="dash", line_color="cyan", row=3, col=1)

                            fig.update_layout(
                                height=900, 
                                hovermode='x unified',
                                plot_bgcolor='white',
                                legend_title_text="Node (Depth)",
                                margin=dict(t=40, b=0, l=0, r=0)
                            )
                            fig.update_xaxes(showgrid=True, gridcolor='Gainsboro', showline=True, linecolor='black')
                            fig.update_yaxes(showgrid=True, gridcolor='Gainsboro', showline=True, linecolor='black')
                            
                            fig.update_xaxes(rangeslider_visible=True, row=3, col=1)
                            
                            st.plotly_chart(fig, use_container_width=True)

                        else:
                            st.info("👆 Please select at least one sensor from the filters above to view the thermodynamics.")
                            
                        # ==========================================
                        # SNAPSHOT SECTION: CURRENT FLEET STATUS
                        # ==========================================
                        if latest_filtered.empty:
                            st.warning("No sensors match your specific filter criteria.")
                        else:
                            st.markdown("### 📍 Array Summary (Latest Readings)")
                            
                            status_color_map = {
                                "🔥 Rapid Warming (Urgent)": "#8b0000",
                                "🚨 Cluster Divergence": "#d62728",
                                "⚠️ Thermal Drift": "#ff7f0e",
                                "❄️ Freezing Active": "#1f77b4",
                                "🟢 Stable Maintenance": "#2ca02c"
                            }
                            
                            summary_rows = []
                            for loc, loc_group in latest_filtered.groupby('Location'):
                                summary_rows.append({
                                    "Location": str(loc),
                                    "Total Nodes": len(loc_group),
                                    "🟢 Stable": len(loc_group[loc_group['Operational Assessment'] == "🟢 Stable Maintenance"]),
                                    "❄️ Freezing": len(loc_group[loc_group['Operational Assessment'] == "❄️ Freezing Active"]),
                                    "⚠️ Drift": len(loc_group[loc_group['Operational Assessment'].isin(["🚨 Cluster Divergence", "⚠️ Thermal Drift"])]),
                                    "🔥 Urgent": len(loc_group[loc_group['Operational Assessment'] == "🔥 Rapid Warming (Urgent)"])
                                })
                            st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, hide_index=True)

                            st.markdown("### 📈 Visual Thermodynamics")
                            g1, g2 = st.columns(2)
                            
                            with g1:
                                fig_scatter = px.scatter(
                                    latest_filtered, x="cluster_divergence", y="thermal_velocity", 
                                    color="Operational Assessment",
                                    color_discrete_map=status_color_map,
                                    hover_data=["DisplayLabel", "Location", "current_temp"],
                                    title="Velocity vs Data Spread",
                                    labels={"cluster_divergence": "Data Spread", "thermal_velocity": "24h Velocity"}
                                )
                                fig_scatter.add_hline(y=0, line_dash="dot", line_width=1, line_color="black")
                                fig_scatter.add_vline(x=0, line_dash="dot", line_width=1, line_color="black")
                                fig_scatter.update_layout(plot_bgcolor='white', margin=dict(t=40, b=0, l=0, r=0))
                                st.plotly_chart(fig_scatter, use_container_width=True)
                                
                            with g2:
                                fig_bar = px.histogram(
                                    latest_filtered, x="Location", color="Operational Assessment",
                                    color_discrete_map=status_color_map,
                                    title="Node Health Distribution by Location",
                                    barmode="stack"
                                )
                                fig_bar.update_layout(plot_bgcolor='white', margin=dict(t=40, b=0, l=0, r=0))
                                st.plotly_chart(fig_bar, use_container_width=True)

                            st.markdown("### 🗄️ Raw Mathematical Evaluation")
                            
                            output_cols = ["DisplayLabel", "Location", "PipeType", "current_temp", "cluster_divergence", "thermal_velocity", "Operational Assessment"]
                            
                            unit_label = "°C" if st.session_state.get("unit_mode") == "Celsius" else "°F"
                            st.dataframe(
                                latest_filtered[output_cols].style.format({
                                    "current_temp": f"{{:.1f}}{unit_label}",
                                    "cluster_divergence": f"{{:+.2f}}{unit_label}",
                                    "thermal_velocity": f"{{:+.2f}}{unit_label}/day"
                                }),
                                use_container_width=True, hide_index=True
                            )
                except Exception as e:
                    st.error(f"Performance Analysis Compiler Error: {e}")

    # =========================================================================
    # TAB 3: NODE ALERT
    # =========================================================================
    with tab_alerts:
        st.subheader("⚠️ Node Alert Dashboard")
        st.write("Real-time tracking for telemetry dropouts, extreme temperature limits, and anomalous data spikes.")
        
        # FIX: Explicitly pull units from session_state to avoid local scope errors
        unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
        unit_label = st.session_state.get("unit_label", "°F")
        
        archived_toggle = st.session_state.get('global_show_archived', False)
        
        # 1. Master Diagnostic Query
        active_sql = "1=1" if archived_toggle else "UPPER(TRIM(CAST(ShowActive AS STRING))) IN ('TRUE', 'YES', '1')"
        
        alert_q = f"""
            WITH ActiveJobs AS (
                SELECT CAST(Project AS STRING) as FullProjectID, TRIM(SPLIT(SPLIT(CAST(Project AS STRING), '-')[OFFSET(0)], ' ')[OFFSET(0)]) as RootJob
                FROM `{PROJECT_REGISTRY_TABLE}` WHERE {active_sql}
            ),
            BaseNodes AS (
                SELECT n.NodeNum, CAST(n.Project AS STRING) as RawProject, n.Phase, n.Location, n.Bank, n.Depth,
                CASE WHEN n.Depth IS NOT NULL AND TRIM(CAST(n.Depth AS STRING)) != '' AND UPPER(CAST(n.Location AS STRING)) NOT LIKE '%AMB%' THEN 'TempPipe' ELSE 'Brine' END as PipeType
                FROM `{NODE_REGISTRY_TABLE}` n
                WHERE (n.End_Date IS NULL OR TRIM(CAST(n.End_Date AS STRING)) = '') AND n.NodeNum IS NOT NULL
            ),
            MappedNodes AS (
                SELECT b.NodeNum, b.RawProject, b.Location, b.Bank, b.Depth, b.PipeType, a.FullProjectID,
                ROW_NUMBER() OVER(
                    PARTITION BY b.NodeNum ORDER BY CASE WHEN a.FullProjectID IS NULL THEN 99 WHEN b.Phase IS NULL OR TRIM(CAST(b.Phase AS STRING)) = '' THEN 1 WHEN UPPER(a.FullProjectID) LIKE CONCAT('%PHASE%', TRIM(CAST(b.Phase AS STRING))) THEN 1 WHEN UPPER(a.FullProjectID) LIKE CONCAT('%PHASE %', TRIM(CAST(b.Phase AS STRING))) THEN 1 ELSE 2 END ASC
                ) as rn
                FROM BaseNodes b LEFT JOIN ActiveJobs a ON TRIM(b.RawProject) LIKE CONCAT(a.RootJob, '%')
            ),
            RegisteredNodes AS (
                SELECT NodeNum, Location, Bank, Depth, PipeType, COALESCE(FullProjectID, RawProject) as FinalProjectLabel
                FROM MappedNodes WHERE rn = 1 AND (FullProjectID IS NOT NULL OR UPPER(RawProject) LIKE '%OFFICE%')
            ),
            NodeTimelineHistory AS (
                SELECT m.NodeNum, m.temperature, m.timestamp,
                LAG(m.temperature) OVER (PARTITION BY m.NodeNum ORDER BY m.timestamp ASC) as last_temp_val
                FROM `{MASTER_VIEW}` m WHERE m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
            ),
            NodeAggregates AS (
                SELECT h.NodeNum, MAX(h.timestamp) as last_seen_ts,
                ARRAY_AGG(h.temperature ORDER BY h.timestamp DESC LIMIT 1)[OFFSET(0)] as latest_temp,
                MAX(CASE WHEN h.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN ABS(h.temperature - h.last_temp_val) ELSE 0 END) as max_single_spike_24h,
                COUNT(DISTINCT CASE WHEN h.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN TIMESTAMP_TRUNC(h.timestamp, HOUR) END) as hours_with_data_24h
                FROM NodeTimelineHistory h GROUP BY h.NodeNum
            ),
            SpikeCounts AS (
                SELECT h.NodeNum, COUNTIF(h.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) AND h.last_temp_val IS NOT NULL AND ((r.PipeType = 'TempPipe' AND ABS(h.temperature - h.last_temp_val) > 1.0) OR (r.PipeType = 'Brine' AND ABS(h.temperature - h.last_temp_val) > 8.0))) as spike_count_24h
                FROM NodeTimelineHistory h JOIN RegisteredNodes r ON h.NodeNum = r.NodeNum GROUP BY h.NodeNum
            )
            SELECT r.FinalProjectLabel as Project, r.NodeNum, r.Location, r.Bank, r.Depth, r.PipeType,
            a.last_seen_ts, a.latest_temp, a.max_single_spike_24h, 
            COALESCE(a.hours_with_data_24h, 0) as hours_with_data_24h,
            COALESCE(s.spike_count_24h, 0) as spike_count_24h
            FROM RegisteredNodes r
            LEFT JOIN NodeAggregates a ON r.NodeNum = a.NodeNum
            LEFT JOIN SpikeCounts s ON r.NodeNum = s.NodeNum
            ORDER BY r.FinalProjectLabel ASC, r.Location ASC
        """
        
        with st.spinner("Scanning active arrays for node alerts..."):
            try:
                alert_df = client.query(alert_q).to_dataframe()
                
                if alert_df.empty:
                    st.info("No active registered nodes found matching the current active filters.")
                else:
                    missing_rows, extreme_rows, spiking_rows = [], [], []
                    project_summary = {}
                    now_utc = pd.Timestamp.now(tz='UTC')
                    
                    # Helper for temp conversion
                    def convert_t(val):
                        if pd.isnull(val): return val
                        return (val - 32) * 5/9 if unit_mode == "Celsius" else val

                    for _, r in alert_df.iterrows():
                        proj_label = str(r['Project']) 
                        if proj_label not in project_summary:
                            project_summary[proj_label] = {"Total": 0, "Working Fine": 0, "Missing": 0, "Extreme": 0, "Spiking": 0}
                        project_summary[proj_label]["Total"] += 1
                        
                        is_in_scope = (selected_project == "All Projects" or proj_label.strip().lower() == selected_project.strip().lower())
                        pos_lbl = f"{r['Depth']}ft" if (pd.notnull(r['Depth']) and str(r['Depth']).strip() != '') else f"Bank {r['Bank']}"
                        
                        last_seen_str, latency_hours = "❌ Never", 999.0
                        if pd.notnull(r['last_seen_ts']):
                            ts_aware = r['last_seen_ts'] if r['last_seen_ts'].tzinfo else r['last_seen_ts'].tz_localize('UTC')
                            latency_hours = (now_utc - ts_aware).total_seconds() / 3600.0
                            if latency_hours <= 1.0: last_seen_str = f"🟢 {latency_hours:.1f}h"
                            elif latency_hours <= 6.0: last_seen_str = f"🟠 {latency_hours:.1f}h"
                            else: last_seen_str = f"🔴 {latency_hours:.1f}h"

                        node_has_issue = False

                        if latency_hours > 24.0:
                            node_has_issue = True
                            project_summary[proj_label]["Missing"] += 1
                            if is_in_scope:
                                missing_rows.append({"Project": proj_label, "Location": str(r['Location']), "Node": str(r['NodeNum']), "Position": pos_lbl, "Last Seen": last_seen_str})
                            
                        if pd.notnull(r['latest_temp']) and (r['latest_temp'] < -25.0 or r['latest_temp'] > 105.0):
                            node_has_issue = True
                            project_summary[proj_label]["Extreme"] += 1
                            if is_in_scope:
                                extreme_rows.append({"Project": proj_label, "Location": str(r['Location']), "Node": str(r['NodeNum']), "Position": pos_lbl, "Last Seen": last_seen_str, "Current Temp": f"{convert_t(r['latest_temp']):.1f}{unit_label}"})
                            
                        spike_val, spike_count, hours_with_data = r['max_single_spike_24h'], int(r['spike_count_24h']), int(r['hours_with_data_24h'])
                        if pd.notnull(spike_val) and spike_count > 0:
                            node_has_issue = True
                            project_summary[proj_label]["Spiking"] += 1
                            if is_in_scope:
                                spiking_rows.append({"Project": proj_label, "Location": str(r['Location']), "Node": str(r['NodeNum']), "Position": pos_lbl, "Last Seen": last_seen_str, "Max Δ Temp": f"{convert_t(spike_val):.1f}{unit_label}", "Spike Count (24h)": f"{spike_count}x in {hours_with_data}h"})
                        
                        if not node_has_issue: project_summary[proj_label]["Working Fine"] += 1

                    # UI RENDER
                    st.markdown("#### 📊 Fleet Health Summary")
                    sum_df_rows = [{"Project": p, **stats} for p, stats in project_summary.items()]
                    st.dataframe(pd.DataFrame(sum_df_rows), use_container_width=True, hide_index=True)
                    st.divider()

                    scope_label = "Global Fleet" if selected_project == "All Projects" else selected_project
                    st.markdown(f"### 🔍 Alerts: {scope_label}")
                    
                    for title, data in [("📡 Missing Nodes", missing_rows), ("🌡️ Extreme Temps", extreme_rows), ("📈 Spiking Data", spiking_rows)]:
                        st.markdown(f"#### {title}")
                        if data: st.dataframe(pd.DataFrame(data), use_container_width=True, hide_index=True)
                        else: st.success(f"✅ Clear.")
                        st.divider()
            except Exception as e:
                st.error(f"Alert Parser Error: {e}")
