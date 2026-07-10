import streamlit as st
import pandas as pd
import zipfile
import time
from datetime import datetime, timedelta
from google.cloud import bigquery

# Import your custom app modules
from app.data.processor import get_bq_client, get_universal_portal_data
from app.utils.config import PROJECT_ID, DATASET_ID

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
        
        # This line must be indented exactly 8 spaces under 'with tab_upload:'
        u_files = st.file_uploader(
            "Select CSV or Excel files", 
            type=['csv', 'xlsx', 'zip'], 
            key="manual_upload_main", 
            accept_multiple_files=True
        ) 
    
        if u_files:
            # All lines under 'if u_files:' must be indented at least 12 spaces
            all_processed_dfs = []
            target_table = None
    
            for f in u_files:
                try:
                    df_raw = None
                    f_identifier = f.name
                    is_sensorconnect = False
                    skip_rows = 0

                    # 1. READ FILE INTO df_raw
                    if f.name.endswith('.zip'):
                        with zipfile.ZipFile(f, 'r') as z:
                            csv_name = [name for name in z.namelist() if name.endswith('.csv')][0]
                            with z.open(csv_name) as zf:
                                df_raw = pd.read_csv(zf, encoding='utf-8', dtype=str)
                                f_identifier = csv_name
                    elif f.name.endswith('.csv'):
                        f.seek(0)
                        # Check for SensorConnect header
                        for i, line in enumerate(f):
                            if b"DATA_START" in line:
                                is_sensorconnect, skip_rows = True, i + 1
                                break
                        f.seek(0)
                        df_raw = pd.read_csv(f, encoding='latin1', skiprows=skip_rows, dtype=str)
                    else:
                        df_raw = pd.read_excel(f, dtype=str)

                    # 2. PROCESS df_raw -> df_processed
                    if df_raw is not None and not df_raw.empty:
                        df_processed = pd.DataFrame()
                        actual_headers = list(df_raw.columns)
                        clean_headers = [str(h).strip().lower() for h in actual_headers]
                        
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
                            
                        else: # Generic SensorPush or similar CSV
                            t_match = next((h for h in actual_headers if 'timestamp' in h.lower() or 'time' in h.lower()), None)
                            v_match = next((h for h in actual_headers if 'temp' in h.lower() or 'probe' in h.lower()), None)
                            if t_match and v_match:
                                df_processed['timestamp'] = pd.to_datetime(df_raw[t_match], errors='coerce', utc=True)
                                df_processed['temperature'] = pd.to_numeric(df_raw[v_match], errors='coerce')
                                
                                # --- THE FIX: Clean SensorPush Export Filenames ---
                                raw_filename = f_identifier.split('/')[-1].replace('.csv', '').strip()
                                
                                # If the filename contains '-starts-', split it and only keep the first part (the Node ID)
                                if '-starts-' in raw_filename:
                                    clean_node_num = raw_filename.split('-starts-')[0].strip()
                                else:
                                    clean_node_num = raw_filename
                                    
                                df_processed['NodeNum'] = clean_node_num
                                target_table = "raw_sensorpush"
                        
                        if not df_processed.empty:
                            df_processed = df_processed.dropna(subset=['timestamp', 'temperature'])
                            all_processed_dfs.append(df_processed)
                            
                            # Grab the actual parsed node number for the UI message
                            display_name = df_processed['NodeNum'].iloc[0] if 'NodeNum' in df_processed.columns else f_identifier
                            
                            st.write(f"✅ {display_name}: {len(df_processed)} records.")
                
                except Exception as e:
                    st.error(f"❌ Error processing {f.name}: {e}")

            # 3. BATCH UPLOAD
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
