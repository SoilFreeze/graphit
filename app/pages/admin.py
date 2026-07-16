import streamlit as st
import pandas as pd
import time
import re
import requests
import numpy as np
from datetime import datetime, timedelta
from google.cloud import bigquery

# Internal Config & Data connections
from app.utils.config import (
    PROJECT_ID, 
    DATASET_ID, 
    PROJECT_REGISTRY_TABLE, 
    NODE_REGISTRY_TABLE
)
from app.data.processor import get_bq_client

def natural_sort_key(s):
    """Sorts strings containing numbers logically (e.g., T1, T2, T10 instead of T1, T10, T2)"""
    if pd.isnull(s):
        return []
    return [int(text) if text.isdigit() else str(text).lower() for text in re.split(r'(\d+)', str(s))]

def get_project_mask(df, selected_project):
    """Helper to match projects that have multi-phase suffixes."""
    if selected_project == "All Projects":
        return pd.Series([True] * len(df), index=df.index)
        
    job_num = str(selected_project).split('-')[0].strip()
    is_job = df['Project'].astype(str).str.startswith(job_num)
    
    phase_match = re.search(r'(?i)Phase\s*(\d+)', selected_project)
    if phase_match and 'Phase' in df.columns:
        target_phase = phase_match.group(1)
        return is_job & (df['Phase'].astype(str).str.strip() == target_phase)
        
    return is_job

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
            # THE FIX: Force these to read in all caps!
            options=["ALL", "ALL BUT NULL", "TRUE", "NULL (STREAMING / UNREVIEWED)", "MASKED", "OFFICE", "BADDATA"],
            key="blk_mgmt_current_status_filter",
            help="Limits modifications only to data points that currently match this selected classification."
        )
    with c3:
        new_status = st.selectbox(
            "Set Approval Status To:", 
            # THE FIX: Force these to write in all caps!
            ["TRUE", "MASKED", "OFFICE", "BADDATA"], 
            key="blk_mgmt_new_status"
        )
    return target_scope, current_status_filter, new_status


def build_bulk_approval_where_clause(reg_df, selected_project, target_scope, current_status_filter, f):
    """Constructs analytical logical statements parsing historical coordinates."""
    where_clauses = []

    if selected_project != "All Projects":
        proj_mask = get_project_mask(reg_df, selected_project)
        proj_filtered = reg_df[proj_mask]
        
        # 1. Handle Node ID targeting (FIXED: UPPER() for case-insensitive matching)
        if target_scope == "Specific Node":
            safe_node = str(f['scope_val']).strip().upper()
            where_clauses.append(f"UPPER(TRIM(CAST(t.NodeNum AS STRING))) = '{safe_node}'")
        elif target_scope == "Specific Location":
            loc_nodes = proj_filtered[proj_filtered['Location'] == f['scope_val']]['NodeNum'].dropna().unique().tolist()
            nodes_str = ", ".join([f"'{str(n).strip().upper()}'" for n in loc_nodes])
            where_clauses.append(f"UPPER(TRIM(CAST(t.NodeNum AS STRING))) IN ({nodes_str})" if nodes_str else "t.NodeNum = 'NONE'")
        elif target_scope == "Project Wide":
            proj_nodes = proj_filtered['NodeNum'].dropna().unique().tolist()
            if proj_nodes:
                nodes_str = ", ".join([f"'{str(n).strip().upper()}'" for n in proj_nodes])
                where_clauses.append(f"UPPER(TRIM(CAST(t.NodeNum AS STRING))) IN ({nodes_str})")
            else:
                where_clauses.append("t.NodeNum = 'NONE'")
        
        # 2. Handle Job and Phase targeting for the SQL view (FIXED: UPPER() applied)
        job_num = str(selected_project).split('-')[0].strip().upper()
        where_clauses.append(f"UPPER(CAST(t.Project AS STRING)) LIKE '{job_num}%'")
        
        phase_match = re.search(r'(?i)Phase\s*(\d+)', selected_project)
        if phase_match:
            target_phase = phase_match.group(1).upper()
            where_clauses.append(f"UPPER(TRIM(CAST(t.Phase AS STRING))) = '{target_phase}'")
    else:
        where_clauses.append("t.Project IS NOT NULL")

    # 3. Handle Timestamps (safely cast to BigQuery TIMESTAMP format)
    start_ts_str = f"{f['s_date'].strftime('%Y-%m-%d')} {f['s_time'].strftime('%H:%M:%S')}"

    if f["temporal_dir"] == "Between Range":
        end_ts_str = f"{f['e_date'].strftime('%Y-%m-%d')} {f['e_time'].strftime('%H:%M:%S')}"
        where_clauses.append(f"t.timestamp BETWEEN TIMESTAMP('{start_ts_str}') AND TIMESTAMP('{end_ts_str}')")
    elif f["temporal_dir"] in ["Older Than", "Newer Than"]:
        op = "<" if f["temporal_dir"] == "Older Than" else ">"
        where_clauses.append(f"t.timestamp {op} TIMESTAMP('{start_ts_str}')")
    
    # 4. Handle Temperature Thresholds
    if f["val_filter"] == "Above Threshold":
        where_clauses.append(f"t.temperature > {f['threshold']}")
    elif f["val_filter"] == "Below Threshold":
        where_clauses.append(f"t.temperature < {f['threshold']}")

    # 5. Handle Status Filters (FIXED: Explicitly mimic the processor.py logic by stripping inner spaces)
    safe_status = str(current_status_filter).upper().replace(" ", "")
    
    if safe_status != "ALL":
        if safe_status == "ALLBUTNULL":
            where_clauses.append("t.approval_status IS NOT NULL")
        elif safe_status == "NULL(STREAMING/UNREVIEWED)":
            where_clauses.append("t.approval_status IS NULL")
        elif safe_status == "TRUE":
            where_clauses.append("(t.approval_status IS NULL OR REPLACE(UPPER(TRIM(CAST(t.approval_status AS STRING))), ' ', '') = 'TRUE')")
        else:
            where_clauses.append(f"REPLACE(UPPER(TRIM(CAST(t.approval_status AS STRING))), ' ', '') = '{safe_status}'")

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
            # Apply our new smart mask
            proj_mask = get_project_mask(reg_df, selected_project)
            proj_filtered = reg_df[proj_mask]
            
            if target_scope == "Project Wide":
                st.info(f"Targeting all nodes in **{selected_project}**")
                scope_val = selected_project
            elif target_scope == "Specific Location":
                u_locs = sorted(proj_filtered['Location'].dropna().unique().tolist(), key=natural_sort_key)
                scope_val = st.selectbox("Select Location", u_locs if u_locs else ["No Locations Found"], key="blk_mgmt_loc_select")
            elif target_scope == "Specific Node":
                u_locs = sorted(proj_filtered['Location'].dropna().unique().tolist(), key=natural_sort_key)
                selected_loc = st.selectbox("First, Select Location", u_locs if u_locs else ["No Locations Found"], key="blk_mgmt_loc_node_select")
                
                u_nodes = sorted(
                    proj_filtered[proj_filtered['Location'] == selected_loc]['NodeNum'].dropna().unique().tolist(), key=natural_sort_key
                )
                scope_val = st.selectbox("Then, Select Node", u_nodes if u_nodes else ["No Nodes Found"], key="blk_mgmt_node_select")
        
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
    target_table = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections" 
    telemetry_table = f"{PROJECT_ID}.{DATASET_ID}.master_data_view_v2" 

    st.title("⚡ Bulk Approval and Database Maintenance")
    st.divider()

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
        "**Note:** Running this automatically drops rogue data points outside the physical bounds of -30°F and 120°F."
    )
    
    if "cleanup_audit_df" not in st.session_state:
        st.session_state.cleanup_audit_df = None

    # --- STEP 1: AUDIT & PREVIEW ---
    if st.button("🔍 Step 1: Audit Database & Calculate Cleanup Impact", use_container_width=True):
        status_box = st.empty()
        status_box.info("Auditing massive raw tables... (This may take a few seconds)")
        try:
            def get_audit_query(table_name):
                return f"""
                    WITH RawStats AS (
                        SELECT COUNT(*) as Total_Points
                        FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`
                    ),
                    DistinctStats AS (
                        SELECT COUNT(*) as Distinct_Points
                        FROM (
                            SELECT DISTINCT 
                                timestamp, 
                                UPPER(TRIM(CAST(NodeNum AS STRING))) as NodeNum, 
                                CAST(temperature AS STRING) as temp
                            FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`
                        )
                    ),
                    HourlyStats AS (
                        SELECT COUNT(*) as Final_Points
                        FROM (
                            SELECT 1 
                            FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`
                            WHERE CAST(temperature AS NUMERIC) >= -30.0 AND CAST(temperature AS NUMERIC) <= 120.0
                            GROUP BY TIMESTAMP_TRUNC(timestamp, HOUR), UPPER(TRIM(CAST(NodeNum AS STRING)))
                        )
                    )
                    SELECT 
                        r.Total_Points, 
                        (r.Total_Points - d.Distinct_Points) as Exact_Doubles,
                        (d.Distinct_Points - h.Final_Points) as Merged_Points,
                        h.Final_Points
                    FROM RawStats r 
                    CROSS JOIN DistinctStats d 
                    CROSS JOIN HourlyStats h
                """
            
            sp_res = client.query(get_audit_query("raw_sensorpush")).to_dataframe().iloc[0]
            lord_res = client.query(get_audit_query("raw_lord")).to_dataframe().iloc[0]
            
            audit_data = [
                {
                    "Table": "SensorPush", 
                    "Total Points": f"{sp_res['Total_Points']:,}", 
                    "Doubles to Delete": f"{sp_res['Exact_Doubles']:,}", 
                    "Points to Merge": f"{sp_res['Merged_Points']:,}", 
                    "Final Points": f"{sp_res['Final_Points']:,}"
                },
                {
                    "Table": "Lord Wireless", 
                    "Total Points": f"{lord_res['Total_Points']:,}", 
                    "Doubles to Delete": f"{lord_res['Exact_Doubles']:,}", 
                    "Points to Merge": f"{lord_res['Merged_Points']:,}", 
                    "Final Points": f"{lord_res['Final_Points']:,}"
                },
                {
                    "Table": "Combined Total", 
                    "Total Points": f"{(sp_res['Total_Points'] + lord_res['Total_Points']):,}", 
                    "Doubles to Delete": f"{(sp_res['Exact_Doubles'] + lord_res['Exact_Doubles']):,}", 
                    "Points to Merge": f"{(sp_res['Merged_Points'] + lord_res['Merged_Points']):,}", 
                    "Final Points": f"{(sp_res['Final_Points'] + lord_res['Final_Points']):,}"
                }
            ]
            st.session_state.cleanup_audit_df = pd.DataFrame(audit_data)
            status_box.empty()
        except Exception as e:
            status_box.empty()
            st.error(f"Audit compilation failed: {e}")

    # --- STEP 2: REVIEW & EXECUTE ---
    if st.session_state.cleanup_audit_df is not None:
        st.write("### 📊 Cleanup Impact Matrix")
        st.dataframe(st.session_state.cleanup_audit_df, use_container_width=True, hide_index=True)
        
        if st.checkbox("I authorize permanently merging and deleting these records.", key="confirm_global_cleanup"):
            if st.button("🚀 Step 2: Execute Database Cleanup", use_container_width=True):
                status_box2 = st.empty()
                try:
                    status_box2.markdown("🧹 **[1/2] Consolidating SensorPush timelines...**")
                    sp_cleanup_sql = f"""
                        CREATE OR REPLACE TEMP TABLE tmp_clean_sensorpush AS
                        SELECT 
                            TIMESTAMP_TRUNC(timestamp, HOUR) as timestamp, 
                            UPPER(TRIM(CAST(NodeNum AS STRING))) as NodeNum, 
                            ROUND(AVG(CAST(temperature AS NUMERIC)), 1) as temperature,
                            MAX(rssi) as rssi
                        FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
                        WHERE CAST(temperature AS NUMERIC) >= -30.0 AND CAST(temperature AS NUMERIC) <= 120.0
                        GROUP BY TIMESTAMP_TRUNC(timestamp, HOUR), UPPER(TRIM(CAST(NodeNum AS STRING)));

                        CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` AS
                        SELECT timestamp, NodeNum, CAST(temperature AS FLOAT64) as temperature, rssi FROM tmp_clean_sensorpush;
                    """
                    client.query(sp_cleanup_sql).result()
                    
                    status_box2.markdown("🧹 **[1/2] Consolidating SensorPush timelines...**")
                    sp_cleanup_sql = f"""
                        CREATE OR REPLACE TEMP TABLE tmp_clean_sensorpush AS
                        SELECT 
                            TIMESTAMP_TRUNC(timestamp, HOUR) as timestamp, 
                            -- Strip floating-point decimals from raw hardware IDs before grouping
                            UPPER(TRIM(SPLIT(CAST(NodeNum AS STRING), '.')[OFFSET(0)])) as NodeNum, 
                            ROUND(AVG(CAST(temperature AS NUMERIC)), 1) as temperature,
                            MAX(rssi) as rssi
                        FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
                        WHERE CAST(temperature AS NUMERIC) >= -30.0 AND CAST(temperature AS NUMERIC) <= 120.0
                        GROUP BY 1, 2;

                        CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` AS
                        SELECT timestamp, NodeNum, CAST(temperature AS FLOAT64) as temperature, rssi FROM tmp_clean_sensorpush;
                    """
                    client.query(sp_cleanup_sql).result()
                    
                    status_box2.markdown("🛰️ **[2/2] Consolidating Lord Wireless timelines...**")
                    lord_cleanup_sql = f"""
                        CREATE OR REPLACE TEMP TABLE tmp_clean_lord AS
                        SELECT 
                            TIMESTAMP_TRUNC(timestamp, HOUR) as timestamp, 
                            -- Strip floating-point decimals from raw hardware IDs before grouping
                            UPPER(TRIM(SPLIT(CAST(NodeNum AS STRING), '.')[OFFSET(0)])) as NodeNum, 
                            ROUND(AVG(CAST(temperature AS NUMERIC)), 1) as temperature
                        FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
                        WHERE CAST(temperature AS NUMERIC) >= -30.0 AND CAST(temperature AS NUMERIC) <= 120.0
                        GROUP BY 1, 2;

                        CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.raw_lord` AS
                        SELECT timestamp, NodeNum, CAST(temperature AS FLOAT64) as temperature FROM tmp_clean_lord;
                    """
                    client.query(lord_cleanup_sql).result()
                                     
                    st.cache_data.clear()
                    status_box2.empty()
                    st.success("🎉 Global Database Consolidation successfully completed!")
                    st.balloons()
                    
                    st.session_state.cleanup_audit_df = None
                    time.sleep(1.5)
                    st.rerun()
                except Exception as e:
                    status_box2.empty()
                    st.error(f"Global Database Consolidation Failed: {e}")
                    
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
    
    # THE FIX: Assign the where clause directly. DO NOT use .replace() here anymore!
    aliased_where = where_str
    
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
            
            # We explicitly write every status into the table so the system registers it
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

                affected_rows = job.num_dml_affected_rows
                st.warning(f"🚨 DEBUG: BigQuery physically modified {affected_rows} rows.")
                
                st.success(f"✅ Reclassification successful! Explicitly stamped '{new_status}' on {affected_rows:,} records.")
                
                st.cache_data.clear()
                run_profile_audit() # Refresh data metrics locally
                st.balloons()
                
                # 2. TEMPORARILY disable the rerun so you can actually read the screen
                # time.sleep(1.0)
                # st.rerun()
                
            except Exception as e:
                st.error(f"Execution Error: {e}")
                st.code(sql, language="sql")
                
                st.success(f"✅ Reclassification successful! Explicitly stamped '{new_status}' on {job.num_dml_affected_rows:,} records.")
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

@st.cache_data(ttl=600)
def get_cached_registry():
    client = get_bq_client()
    if client is None: return pd.DataFrame(), []
    
    full_reg_df = client.query(f"SELECT * FROM `{NODE_REGISTRY_TABLE}` WHERE End_Date IS NULL OR TRIM(CAST(End_Date AS STRING)) = ''").to_dataframe()
    full_reg_df['Project'] = full_reg_df['Project'].astype(str).str.split('.').str[0].str.strip()
    
    proj_q = f"SELECT CAST(Project AS STRING) as Project, ProjectName, Timezone, ProjectStatus, Date_Freezedown FROM `{PROJECT_REGISTRY_TABLE}` WHERE ShowActive IS TRUE"
    available_projects_list = sorted(client.query(proj_q).to_dataframe()['Project'].dropna().unique().tolist())
    
    return full_reg_df, available_projects_list

@st.cache_data(ttl=600)
def get_cached_fleet_matrix():
    client = get_bq_client()
    if client is None: return pd.DataFrame()
    
    sum_q = f"""
        WITH ProjectBase AS (
          SELECT 
            Project, ProjectName, ProjectStatus, Date_Freezedown,
            TRIM(SPLIT(CAST(Project AS STRING), '-')[OFFSET(0)]) as RootJob,
            REGEXP_EXTRACT(CAST(Project AS STRING), r'(?i)Phase\\s*(\\d+)') as ProjectPhase
          FROM `{PROJECT_REGISTRY_TABLE}`
          WHERE ShowActive IS TRUE AND UPPER(CAST(Project AS STRING)) NOT LIKE '%OFFICE%'
        ),
        ActiveNodes AS (
          SELECT 
            NodeNum, CAST(Phase AS STRING) as Phase,
            TRIM(SPLIT(CAST(Project AS STRING), '-')[OFFSET(0)]) as NodeRootJob
          FROM `{NODE_REGISTRY_TABLE}`
          WHERE (End_Date IS NULL OR TRIM(CAST(End_Date AS STRING)) = '')
        )
        SELECT 
            p.Project, p.ProjectName, p.ProjectStatus, p.Date_Freezedown, 
            COUNT(DISTINCT n.NodeNum) as Mapped_Sensors, 
            COUNT(DISTINCT CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR) THEN n.NodeNum END) as Active_6h, 
            COUNT(DISTINCT CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN n.NodeNum END) as Active_24h 
        FROM ProjectBase p
        LEFT JOIN ActiveNodes n 
          ON n.NodeRootJob = p.RootJob AND (p.ProjectPhase IS NULL OR TRIM(n.Phase) = p.ProjectPhase)
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.master_data_view_v2` m 
          ON UPPER(TRIM(CAST(n.NodeNum AS STRING))) = UPPER(TRIM(CAST(m.NodeNum AS STRING)))
          AND m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        GROUP BY 1,2,3,4 
        ORDER BY p.Project ASC
    """
    return client.query(sum_q).to_dataframe()
    
# =============================================================================
# Page: Admin Tools 
# =============================================================================

def render_admin_page(selected_project, display_tz, unit_mode, unit_label, active_refs):
    """Central analytical administrative supervisor console streaming clean Google Sheets source records."""
    st.header("🛠️ Admin Tools")
    client = get_bq_client()
    if client is None: st.error("Database connection unavailable."); return

    # Core Read-Only Matrix Data Pull (Now Cached!)
    try:
        full_reg_df, available_projects_list = get_cached_registry()
    except Exception as e: 
        st.error(f"Registry Link Offline: {e}"); return

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
            matrix_df = get_cached_fleet_matrix()
            rows = []
            for _, r in matrix_df.iterrows():
                elapsed = max(0, (pd.Timestamp.now(tz=display_tz).date() - pd.to_datetime(r['Date_Freezedown']).date()).days) if pd.notnull(r['Date_Freezedown']) else 0
                rows.append({
                    "Project ID": r['Project'], 
                    "Project Name": r['ProjectName'] or r['Project'], 
                    "Mapped Sensors": int(r['Mapped_Sensors']), 
                    "Active (6h)": int(r['Active_6h']), 
                    "Active (24h)": int(r['Active_24h']), 
                    "Project Status Timeline": f"Day {elapsed} of {str(r['ProjectStatus']).title()}" if pd.notnull(r['Date_Freezedown']) else "Not Freezing"
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        except Exception as e: 
            st.error(f"Overview compilation fault: {e}")

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
        
        # Expanded query with Maintenance/EndFreeze AND a filter to drop empty spreadsheet rows
        directory_q = f"""
            SELECT 
                Project as `Project ID`, 
                ProjectName as `Friendly Name`, 
                ProjectStatus as `Operational Phase`, 
                Date_Freezedown as `Freezedown Date`, 
                Date_Maintenance as `Maintenance Date`,
                Date_EndFreeze as `End Freeze Date`,
                City, 
                Timezone 
            FROM `{PROJECT_REGISTRY_TABLE}` 
            WHERE Project IS NOT NULL 
              AND TRIM(CAST(Project AS STRING)) != ''
            ORDER BY Project ASC
        """
        
        try:
            st.dataframe(
                client.query(directory_q).to_dataframe(), 
                use_container_width=True, 
                hide_index=True
            )
        except Exception as e:
            st.error(f"Failed to load directory: {e}")
