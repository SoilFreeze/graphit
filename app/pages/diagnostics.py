import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import re
from google.cloud import bigquery

# Import your custom app modules
from app.data.processor import get_bq_client
from app.utils.config import (
    PROJECT_ID, 
    DATASET_ID, 
    PROJECT_REGISTRY_TABLE,
    MASTER_VIEW,
    NODE_REGISTRY_TABLE
)
from app.pages.admin import natural_sort_key
from app.components.charts import build_high_speed_graph

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
    
##################################
# Page: Node Diagnostics (New)   #
##################################
def render_node_diagnostics(selected_project, display_tz, unit_label):
    """
    Diagnostic supervisor console for deep device investigation.
    Provides three distinct tabs: real-time exception alerting, 
    bad actor reliability, and raw history inspection.
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

    # Establish the core tabs
    tab_alerts, tab_bad_actors, tab_lookup = st.tabs([
        "⚠️ Node Alerts",
        "🚨 Bad Actor & Reliability",
        "🔍 Data Lookup"
    ])

    # =========================================================================
    # TAB 1: NODE ALERT
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
                SELECT m.NodeNum, m.temperature, m.rssi, m.timestamp,
                LAG(m.temperature) OVER (PARTITION BY m.NodeNum ORDER BY m.timestamp ASC) as last_temp_val
                FROM `{MASTER_VIEW}` m WHERE m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
            ),
            NodeAggregates AS (
                SELECT h.NodeNum, MAX(h.timestamp) as last_seen_ts,
                ARRAY_AGG(h.temperature ORDER BY h.timestamp DESC LIMIT 1)[OFFSET(0)] as latest_temp,
                MAX(CASE WHEN h.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN ABS(h.temperature - h.last_temp_val) ELSE 0 END) as max_single_spike_24h,
                
                -- NEW RSSI COLUMNS
                ARRAY_AGG(h.rssi ORDER BY h.timestamp DESC LIMIT 1)[OFFSET(0)] as latest_rssi,
                AVG(CASE WHEN h.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN h.rssi END) as avg_rssi_24h,
                
                COUNT(CASE WHEN h.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN h.timestamp END) as total_pings_24h,
                COUNT(DISTINCT CASE WHEN h.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN TIMESTAMP_TRUNC(h.timestamp, HOUR) END) as hours_with_data_24h
                FROM NodeTimelineHistory h GROUP BY h.NodeNum
            ),
            SpikeCounts AS (
                SELECT h.NodeNum, COUNTIF(h.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) AND h.last_temp_val IS NOT NULL AND ((r.PipeType = 'TempPipe' AND ABS(h.temperature - h.last_temp_val) > 1.0) OR (r.PipeType = 'Brine' AND ABS(h.temperature - h.last_temp_val) > 8.0))) as spike_count_24h
                FROM NodeTimelineHistory h JOIN RegisteredNodes r ON h.NodeNum = r.NodeNum GROUP BY h.NodeNum
            )
            SELECT r.FinalProjectLabel as Project, r.NodeNum, r.Location, r.Bank, r.Depth, r.PipeType,
            a.last_seen_ts, a.latest_temp, a.max_single_spike_24h, 
            a.latest_rssi, a.avg_rssi_24h,
            COALESCE(a.hours_with_data_24h, 0) as hours_with_data_24h,
            COALESCE(a.total_pings_24h, 0) as checkin_frequency_24h,
            ROUND((COALESCE(a.hours_with_data_24h, 0) / 24.0) * 100, 1) as Signal_Reliability_Pct,
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
                        
                        # UPDATED: Node must have 3 or more spikes in 24 hours to trigger the alert
                        if pd.notnull(spike_val) and spike_count >= 3:
                            node_has_issue = True
                            project_summary[proj_label]["Spiking"] += 1
                            if is_in_scope:
                                spiking_rows.append({
                                    "Project": proj_label, 
                                    "Location": str(r['Location']), 
                                    "Node": str(r['NodeNum']), 
                                    "Position": pos_lbl, 
                                    "Last Seen": last_seen_str, 
                                    "Max Δ Temp": f"{convert_t(spike_val):.1f}{unit_label}", 
                                    "Spike Count (24h)": f"{spike_count}x in {hours_with_data}h"
                                })
                        
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

    # =========================================================================
    # TAB 2: GLOBAL RELIABILITY & DRILL-DOWN OVERVIEWS
    # =========================================================================
    with tab_bad_actors:
        st.subheader("🚨 Global Network Reliability Overview")
        st.write("Complete node registry with drill-down overviews by location/pipe.")
        
        if 'alert_df' not in locals() or alert_df.empty:
            st.info("No network data available to evaluate.")
        else:
            perf_df = alert_df.copy()
            
            # Filter for in-scope project
            if selected_project != "All Projects":
                perf_df = perf_df[perf_df['Project'].astype(str).str.strip().str.lower() == selected_project.strip().lower()]

            if perf_df.empty:
                st.success(f"No active hardware found for {selected_project}.")
            else:
                # Global Fleet Metrics
                c1, c2, c3 = st.columns(3)
                c1.metric("Total Active Sensors", len(perf_df))
                
                avg_fleet_rel = perf_df['Signal_Reliability_Pct'].mean()
                c2.metric("Global Fleet Reliability", f"{avg_fleet_rel:.1f}%")
                c3.metric("Locations Monitored", perf_df['Location'].nunique())
                
                st.divider()
                
                # Sort completely by Location to create the drill-downs
                locations = sorted(perf_df['Location'].dropna().unique().tolist())
                
                st.markdown("#### 📍 Drill-Down by Location / Temp Pipe")
                
                # Setup display columns
                display_cols = ['NodeNum', 'Depth', 'Bank', 'Signal_Reliability_Pct', 'latest_rssi', 'avg_rssi_24h', 'spike_count_24h']
                
                for loc in locations:
                    # Filter to location and sort worst reliability to the top of each pipe
                    loc_df = perf_df[perf_df['Location'] == loc].sort_values(by='Signal_Reliability_Pct', ascending=True)
                    loc_avg_rel = loc_df['Signal_Reliability_Pct'].mean()
                    
                    # Create an expandable drill-down per pipe
                    with st.expander(f"Location: {loc}  |  Sensors: {len(loc_df)}  |  Average Reliability: {loc_avg_rel:.1f}%"):
                        st.dataframe(
                            loc_df[display_cols].style.format({
                                "Signal_Reliability_Pct": "{:.1f}%",
                                "latest_rssi": "{:.0f} dBm",
                                "avg_rssi_24h": "{:.0f} dBm",
                                "spike_count_24h": "{}"
                            }),
                            use_container_width=True, 
                            hide_index=True,
                            column_config={
                                "NodeNum": "Node ID",
                                "Depth": "Depth",
                                "Bank": "Bank",
                                "Signal_Reliability_Pct": "Reliability Score",
                                "latest_rssi": "Current RSSI",
                                "avg_rssi_24h": "24h Avg RSSI",
                                "spike_count_24h": "Erratic Spikes (24h)"
                            }
                        )

    # =========================================================================
    # TAB 3: DATA LOOKUP ENGINE
    # =========================================================================
    with tab_lookup:
        st.subheader("🔍 Node Telemetry Inspection (Multi-Node)")
        
        # 1. Tie Project Scope to the Sidebar Context
        scope_label = "Global Fleet" if selected_project == "All Projects" else selected_project
        st.info(f"🎯 **Search Scope:** {scope_label} (Change in sidebar)")
        
        c1, c2 = st.columns([1, 1])
        with c1:
            search_mode = st.radio("Search Method", ["Filter Mappings", "Search by Node ID"], horizontal=True)
            
        target_nodes = [] # Initialize as an empty list
        
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
                # CHANGED to multiselect
                target_nodes = st.multiselect("Select Target Node(s) to Inspect", matching_nodes, default=[matching_nodes[0]], key="diag_node_select_dropdown")
            else:
                st.warning("No nodes match this configuration.")
                
        else:
            with c2:
                all_active_nodes = sorted(proj_filtered['NodeNum'].dropna().astype(str).unique().tolist(), key=natural_sort_key)
                # CHANGED to multiselect
                target_nodes = st.multiselect(
                    "Search and Select Node ID(s):", 
                    options=all_active_nodes,
                    default=[],
                    key="diag_direct_node_search"
                )

        if target_nodes:
            st.divider()
            
            c_header, c_time = st.columns([3, 1])
            with c_header:
                st.markdown(f"##### 📈 Telemetry History for Selected Nodes")
            with c_time:
                # Add dynamic timeline amounts
                time_opt = st.selectbox("Historical Window:", ["30 Days", "60 Days", "90 Days", "1 Year", "All Time"], index=0)
                
            days_map = {"30 Days": 30, "60 Days": 60, "90 Days": 90, "1 Year": 365, "All Time": 5000}
            lookback_days = days_map[time_opt]
            
            # Master read query pulling localized node history down for ALL target nodes
            node_q = f"""
                SELECT timestamp, temperature, rssi, Location, Bank, Depth, Project, SensorStatus, NodeNum
                FROM `{MASTER_VIEW}`
                WHERE NodeNum IN UNNEST(@target_nodes)
                  AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)
                ORDER BY timestamp DESC
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("target_nodes", "STRING", target_nodes),
                    bigquery.ScalarQueryParameter("lookback_days", "INTEGER", int(lookback_days))
                ]
            )
            
            with st.spinner(f"Fetching {time_opt} of node history..."):
                node_history = client.query(node_q, job_config=job_config).to_dataframe()
            
            if node_history.empty:
                st.warning(f"No telemetry data found for the selected nodes in the past {time_opt}.")
            else:
                # Localize and convert time for entire dataframe first so aggregation works cleanly
                if node_history['timestamp'].dt.tz is None:
                    node_history['timestamp'] = node_history['timestamp'].dt.tz_localize('UTC')
                node_history['timestamp'] = node_history['timestamp'].dt.tz_convert(display_tz)

                # Fetch ONLY the distinct ping hours AND the average RSSI for ALL selected nodes
                ping_q = f"""
                    SELECT 
                        NodeNum,
                        TIMESTAMP_TRUNC(timestamp, HOUR) as ping_hour,
                        AVG(rssi) as hourly_rssi
                    FROM `{MASTER_VIEW}`
                    WHERE NodeNum IN UNNEST(@target_nodes)
                    GROUP BY 1, 2
                """
                job_config_ping = bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ArrayQueryParameter("target_nodes", "STRING", target_nodes)]
                )
                
                with st.spinner("Calculating reliability and RSSI scores..."):
                    try:
                        ping_df = client.query(ping_q, job_config=job_config_ping).to_dataframe()
                        if not ping_df.empty:
                            ping_df['ping_hour'] = pd.to_datetime(ping_df['ping_hour'], utc=True)
                    except Exception as e:
                        st.error(f"Data Fetch Error: {e}")
                        ping_df = pd.DataFrame()

                # ==========================================
                # LOOP THROUGH EACH SELECTED NODE FOR METRICS
                # ==========================================
                for node in target_nodes:
                    st.markdown(f"### 📡 Diagnostics: `{node}`")
                    
                    # Filter dataframes for just the current node in the loop
                    node_specific_history = node_history[node_history['NodeNum'] == node]
                    node_pings = ping_df[ping_df['NodeNum'] == node] if not ping_df.empty else pd.DataFrame()

                    target_reg = reg_df[reg_df['NodeNum'] == node].copy()
                    target_reg['Start_Date_DT'] = pd.to_datetime(target_reg['Start_Date'], errors='coerce')
                    target_reg['End_Date_DT'] = pd.to_datetime(target_reg['End_Date'], errors='coerce')
                    
                    # Sort newest assignments to the top
                    target_reg = target_reg.sort_values(by='Start_Date_DT', ascending=False)

                    overall_active_hrs = 0
                    overall_total_hrs = 0
                    target_reg['Project Reliability'] = 0.0
                    target_reg['Avg RSSI'] = pd.NA
                    
                    now_utc = pd.Timestamp.now(tz='UTC')

                    for idx, row in target_reg.iterrows():
                        # Standardize boundaries
                        start_ts = row['Start_Date_DT']
                        if pd.isnull(start_ts):
                            start_ts = pd.Timestamp('2000-01-01', tz='UTC')
                        elif start_ts.tzinfo is None:
                            start_ts = start_ts.tz_localize('UTC')
                            
                        end_ts = row['End_Date_DT']
                        if pd.isnull(end_ts):
                            end_ts = now_utc
                        elif end_ts.tzinfo is None:
                            end_ts = end_ts.tz_localize('UTC')
                            
                        # Bound calc_end_ts to 'now' so we don't penalize active assignments for future hours
                        calc_end_ts = min(end_ts, now_utc)

                        # Calculate total expected hours in this assignment window
                        total_assignment_hours = max(1.0, (calc_end_ts - start_ts).total_seconds() / 3600.0)

                        # Count actual pings within the bounds and average the RSSI
                        if not node_pings.empty:
                            pings_in_window = node_pings[(node_pings['ping_hour'] >= start_ts) & (node_pings['ping_hour'] <= end_ts)]
                            active_hrs = len(pings_in_window)
                            avg_rssi = pd.to_numeric(pings_in_window['hourly_rssi'], errors='coerce').mean()
                        else:
                            active_hrs = 0
                            avg_rssi = pd.NA
                            
                        # Assign row reliability and RSSI
                        rel_score = (active_hrs / total_assignment_hours) * 100.0
                        target_reg.at[idx, 'Project Reliability'] = min(100.0, rel_score)
                        target_reg.at[idx, 'Avg RSSI'] = avg_rssi
                        
                        # Accumulate overall score if NOT an office/desk assignment
                        proj_name = str(row['Project']).strip().lower()
                        loc_name = str(row['Location']).strip().lower()
                        
                        if "office" not in proj_name and "office" not in loc_name:
                            overall_active_hrs += active_hrs
                            overall_total_hrs += total_assignment_hours

                    # Calculate Final Top-Level Metric
                    if overall_total_hrs > 0:
                        overall_reliability = min(100.0, (overall_active_hrs / overall_total_hrs) * 100.0)
                    else:
                        overall_reliability = 0.0

                    if not node_specific_history.empty:
                        # Meta overview statistics boxes
                        meta_row = node_specific_history.iloc[0]
                        m1, m2, m3, m4, m5 = st.columns(5)
                        m1.metric("Current Temp", f"{meta_row['temperature']:.1f}{unit_label}")
                        m2.metric("Latest Location", str(meta_row['Location']))
                        m3.metric("Latest Project", str(meta_row['Project']))
                        m4.metric("Scanned Records", f"{len(node_specific_history):,}")
                        m5.metric("Overall Field Reliability", f"{overall_reliability:.1f}%")
                    else:
                        st.info("No recent telemetry found for this node in the timeframe.")

                    # Compile the Historical Placements Table strictly from the Registry
                    st.markdown(f"**Assignment History (Registry) - {node}**")
                    
                    def format_pos(r):
                        if pd.notnull(r.get('Depth')) and str(r.get('Depth')).strip(): return f"{r['Depth']}ft"
                        if pd.notnull(r.get('Bank')) and str(r.get('Bank')).strip(): return f"Bank {r['Bank']}"
                        return "-"
                        
                    target_reg['Position'] = target_reg.apply(format_pos, axis=1)
                    target_reg['Start Date'] = target_reg['Start_Date_DT'].dt.strftime('%m/%d/%Y %H:%M').fillna("Unknown")
                    target_reg['End Date'] = target_reg['End_Date_DT'].dt.strftime('%m/%d/%Y %H:%M').fillna("Active")
                    target_reg['Project Reliability'] = target_reg['Project Reliability'].fillna(0).apply(lambda x: f"{x:.1f}%")
                    
                    # Format the new RSSI column safely for the table ONLY
                    target_reg['Avg RSSI'] = target_reg['Avg RSSI'].apply(lambda x: f"{x:.0f} dBm" if pd.notnull(x) else "-")
                    
                    # Add Avg RSSI to the display columns
                    disp_placements = target_reg[['Project', 'Location', 'Position', 'Start Date', 'End Date', 'Project Reliability', 'Avg RSSI', 'SensorStatus']]
                    st.dataframe(disp_placements, use_container_width=True, hide_index=True)
                    st.divider()

                # ==========================================
                # TEMPERATURE TREND & AMBIENT TOGGLE
                # ==========================================

                st.markdown("#### 📉 Temperature Trend")
                
                # The checkbox is placed directly above the graph
                show_ambient = st.checkbox("Show Ambient Office Temperature", value=False, key="toggle_ambient_temp")
                
                # Calculate exact bounds for the chart's X-axis to force the view window
                now_ts = pd.Timestamp.now(tz=display_tz)
                start_ts = now_ts - pd.Timedelta(days=lookback_days)
                
                # Removed color_discrete_sequence so Plotly assigns different colors to different NodeNums
                fig = px.line(
                    node_history, x='timestamp', y='temperature', color='NodeNum',
                    labels={'timestamp': 'Time', 'temperature': f'Temperature ({unit_label})'}
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
