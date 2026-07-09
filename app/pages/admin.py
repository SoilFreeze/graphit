
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
