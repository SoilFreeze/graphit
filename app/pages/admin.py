import streamlit as st
import pandas as pd
import time
import re
import requests
import numpy as np
import os
from PIL import Image
from streamlit_image_coordinates import streamlit_image_coordinates
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
            
            # We explicitly write every status into the table so the system registers it.
            # FIX: Removed the HOUR grouping so we match on exact, down-to-the-second timestamps.
            sql = f"""
                MERGE `{target_table}` T
                USING (
                    SELECT 
                        UPPER(TRIM(CAST(t.NodeNum AS STRING))) as NodeNum, 
                        t.timestamp as exact_timestamp
                    FROM `{telemetry_table}` t 
                    WHERE {aliased_where}
                ) S
                ON UPPER(TRIM(CAST(T.NodeNum AS STRING))) = S.NodeNum 
                   AND T.timestamp = S.exact_timestamp
                WHEN MATCHED THEN
                    UPDATE SET approve = '{new_status}'
                WHEN NOT MATCHED THEN
                    INSERT (NodeNum, timestamp, approve) 
                    VALUES (S.NodeNum, S.exact_timestamp, '{new_status}')
            """
            
            # FIX: Cleaned up the try/except block to stop triggering fake success messages when the query fails.
            try:
                with st.spinner("Processing database status reclassifications..."):
                    job = client.query(sql)
                    job.result()

                affected_rows = job.num_dml_affected_rows
                
                st.success(f"✅ Reclassification successful! Explicitly stamped '{new_status}' on {affected_rows:,} records.")
                
                st.cache_data.clear()
                run_profile_audit() # Refresh data metrics locally
                st.balloons()
                
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

    # Standardized Navigation Tabs Layout Schema Paths
    tab_bulk_app, tab_admin_sum, tab_pipe_mapper = st.tabs([
        "⚡ Bulk Approval", "📋 Admin Summary", "🗺️ Pipe Mapper"
    ])
    
    # --- SUB-TAB 1: BULK APPROVAL SYSTEM RUNROOM ---
    with tab_bulk_app:
        execute_bulk_approval_workspace(client, full_reg_df, selected_project)

    # --- SUB-TAB 2: ADMIN SUMMARY & PROJECT MASTER ---
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

        st.divider()
        st.markdown("### 🏗️ Active Deployment Overview Matrix")
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

        st.divider()
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

    # --- SUB-TAB 3: AS-BUILT PIPE MAPPER ---
    with tab_pipe_mapper:
        st.subheader("🗺️ As-Built Pipe Mapper")
        st.markdown("Select a site plan to log X/Y pixel coordinates for physical locations. Download the CSV when finished to paste into your Google Sheet.")
        
        # Initialize session memory (Added a safety check to reset it if it still has the old NodeNum column)
        if 'mapped_pipes' not in st.session_state or 'Location' not in st.session_state.mapped_pipes.columns:
            st.session_state.mapped_pipes = pd.DataFrame(columns=['Location', 'Map_X', 'Map_Y'])

        col_map1, col_map2 = st.columns([3, 1])
        AS_BUILT_DIR = "as_builts" 
        
        with col_map2:
            # 1. Scan the folder for images
            available_images = []
            if os.path.exists(AS_BUILT_DIR):
                available_images = sorted([f for f in os.listdir(AS_BUILT_DIR) if f.lower().endswith(('.png', '.jpg', '.jpeg'))])
            
            if not available_images:
                st.error(f"No image files found in the '{AS_BUILT_DIR}' folder.")
                selected_image = "(None)"
            else:
                selected_image = st.selectbox("1. Select As-Built Image:", ["(None)"] + available_images)
            
            if selected_image != "(None)":
                all_projects = ["(None)"] + sorted(full_reg_df['Project'].dropna().unique().tolist())
                selected_mapper_proj = st.selectbox("2. Link to Project Database:", all_projects)
                
                pipe_options = []
                if selected_mapper_proj != "(None)":
                    proj_df = full_reg_df[full_reg_df['Project'] == selected_mapper_proj]
                    # CHANGED: Now pulling unique Locations (T1, T2, etc.) instead of NodeNums
                    pipe_options = sorted(proj_df['Location'].dropna().astype(str).unique().tolist(), key=natural_sort_key)
                
                if not pipe_options:
                    pipe_name = st.text_input("3. Location Name (Manual Entry):", key="mapper_pipe_input").upper().strip()
                else:
                    # Auto-Advancing Queue Logic for Locations
                    mapped_list = st.session_state.mapped_pipes['Location'].tolist()
                    unmapped_pipes = [p for p in pipe_options if p not in mapped_list]
                    
                    default_idx = 0
                    if unmapped_pipes:
                        default_idx = pipe_options.index(unmapped_pipes[0])
                        
                    pipe_name = st.selectbox("3. Select Location to Map (Auto-advances):", pipe_options, index=default_idx)

                st.dataframe(st.session_state.mapped_pipes, use_container_width=True, hide_index=True)
                
                if not st.session_state.mapped_pipes.empty:
                    csv = st.session_state.mapped_pipes.to_csv(index=False)
                    st.download_button("⬇️ Download CSV", data=csv, file_name=f"{selected_image}_coordinates.csv", mime="text/csv", use_container_width=True)
                    
                    if st.button("Clear All Data", use_container_width=True):
                        st.session_state.mapped_pipes = pd.DataFrame(columns=['Location', 'Map_X', 'Map_Y'])
                        st.session_state.pop('last_click', None) 
                        st.rerun()

        with col_map1:
            if selected_image != "(None)":
                img_path = os.path.join(AS_BUILT_DIR, selected_image)
                
                try:
                    # 1. Open the massive original image
                    raw_img = Image.open(img_path)
                    orig_width, orig_height = raw_img.size
                    
                    # 2. Calculate a scale factor to shrink it to fit the screen (~900px wide)
                    MAX_DISPLAY_WIDTH = 900
                    scale_factor = 1.0
                    
                    if orig_width > MAX_DISPLAY_WIDTH:
                        scale_factor = orig_width / MAX_DISPLAY_WIDTH
                        new_height = int(orig_height / scale_factor)
                        display_img = raw_img.resize((MAX_DISPLAY_WIDTH, new_height))
                    else:
                        display_img = raw_img
                    
                    if pipe_name:
                        st.info(f"👆 Click on the map to log coordinates for **{pipe_name}**.")
                    else:
                        st.warning("⚠️ Enter a Location on the right before clicking!")
                        
                    # 3. Render the SHRUNKEN image on the screen
                    click_data = streamlit_image_coordinates(display_img, key="site_map")
                    
                    if click_data is not None and pipe_name:
                        click_hash = f"{click_data['x']}-{click_data['y']}"
                        
                        if st.session_state.get('last_click') != click_hash:
                            st.session_state['last_click'] = click_hash 
                            
                            # 4. Math Magic: Multiply the click by the scale factor to get the TRUE original coordinates!
                            true_x = int(click_data['x'] * scale_factor)
                            true_y = int(click_data['y'] * scale_factor)
                            
                            if pipe_name in st.session_state.mapped_pipes['Location'].values:
                                st.session_state.mapped_pipes.loc[st.session_state.mapped_pipes['Location'] == pipe_name, ['Map_X', 'Map_Y']] = [true_x, true_y]
                            else:
                                new_row = pd.DataFrame({'Location': [pipe_name], 'Map_X': [true_x], 'Map_Y': [true_y]})
                                st.session_state.mapped_pipes = pd.concat([st.session_state.mapped_pipes, new_row], ignore_index=True)
                            
                            st.rerun()
                            
                except Exception as e:
                    st.error(f"Could not load image {selected_image}. Error: {e}")
