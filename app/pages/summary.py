import streamlit as st
import pandas as pd

# Internal Data & Config
from app.data.processor import get_bq_client
from app.utils.config import PROJECT_REGISTRY_TABLE, NODE_REGISTRY_TABLE, MASTER_VIEW

##############################
# Page 1 - Dashboard Summary #
##############################
def render_summary_dashboard(unit_label, unit_mode, display_tz):
    """
    Renders Global Active Project Summary.
    Driven by the Project Registry to ensure active projects show up even if offline.
    Properly counts 'Ambient' nodes in the 'Total Assigned' pool.
    """
    st.header("🌐 Global Active Project Summary")
    
    client = get_bq_client()
    if client is None: return

    # --- 1. THE CONTROL LIST: Active Projects Only ---
    proj_q = f"""
        SELECT CAST(Project AS STRING) as Project, ProjectName, Date_Freezedown, Date_Maintenance
        FROM `{PROJECT_REGISTRY_TABLE}`
        WHERE UPPER(TRIM(CAST(ShowActive AS STRING))) IN ('TRUE', 'YES', '1')
          AND UPPER(Project) NOT LIKE '%OFFICE%'
        ORDER BY Project
    """
    
    try: active_projs = client.query(proj_q).to_dataframe()
    except Exception as e: return st.error(f"Project Registry failed: {e}")

    if active_projs.empty:
        return st.info("No active projects found in registry.")

    # --- 2. INVENTORY POOL: Total assigned hardware ---
    pool_q = f"""
        SELECT CAST(Project AS STRING) as Project, Phase, System, UPPER(CAST(Location AS STRING)) as Location, COUNT(DISTINCT NodeNum) as total_assigned
        FROM `{NODE_REGISTRY_TABLE}`
        WHERE UPPER(Project) NOT LIKE '%OFFICE%'
          -- Ensure we only count the currently active hardware in the hole
          AND (End_Date IS NULL OR TRIM(CAST(End_Date AS STRING)) = '')
        GROUP BY 1, 2, 3, 4
    """
    pool_df = client.query(pool_q).to_dataframe()
    pool_df[['Phase', 'System', 'Location']] = pool_df[['Phase', 'System', 'Location']].fillna('')

    # --- 3. TELEMETRY: Last 48 hours of data ---
    summary_q = f"""
        WITH raw_data AS (
            SELECT Project, Phase, System, Bank, Location, Depth, temperature, timestamp, NodeNum
            FROM `{MASTER_VIEW}`
            WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
              AND Project IS NOT NULL
              AND UPPER(Project) NOT LIKE '%OFFICE%'
        ),
        MaxTime AS (
            SELECT MAX(timestamp) as max_ts FROM raw_data
        )
        SELECT 
            r.Project, r.Phase, r.System, r.Bank, r.Location, r.Depth, r.NodeNum,
            MIN(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR) THEN r.temperature END) as min_now,
            MAX(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR) THEN r.temperature END) as max_now,
            MIN(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR) THEN r.temperature END) as min_24h,
            MAX(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR) THEN r.temperature END) as max_24h,
            COUNTIF(r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR)) as checkins_1h,
            COUNTIF(r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 6 HOUR)) as checkins_6h,
            COUNTIF(r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR)) as checkins_24h,
            ARRAY_AGG(r.temperature ORDER BY r.timestamp DESC LIMIT 1)[OFFSET(0)] as latest_temp,
            MAX(r.timestamp) as latest_ts
        FROM raw_data r CROSS JOIN MaxTime m
        GROUP BY 1, 2, 3, 4, 5, 6, 7
    """
    tel_df = client.query(summary_q).to_dataframe()
    if not tel_df.empty:
        tel_df[['Phase', 'System', 'Bank', 'Location']] = tel_df[['Phase', 'System', 'Bank', 'Location']].fillna('')

    # --- 4. RENDER ENGINE: Iterate over the exact control list ---
    for _, row in active_projs.iterrows():
        p_project = str(row['Project']).strip()
        p_name = row['ProjectName'] if pd.notnull(row['ProjectName']) else p_project
        f_date = row['Date_Freezedown']
        
        job_num = p_project.split('-')[0].strip()
        
        target_phase = ""
        if "Phase 1" in p_project or "Phase1" in p_project: target_phase = "1"
        elif "Phase 2" in p_project or "Phase2" in p_project: target_phase = "2"
        elif "Phase 3" in p_project or "Phase3" in p_project: target_phase = "3"

        pool_matches = pool_df[
            (pool_df['Project'].str.startswith(job_num)) & 
            ((pool_df['Phase'] == target_phase) | (target_phase == ""))
        ]
        
        raw_systems = [str(s).strip() for s in pool_matches['System'].unique() if str(s).strip()]
        systems = sorted(list(set(raw_systems)))
        if not systems:
            systems = [""] 

        if tel_df.empty:
            tel_matches = pd.DataFrame(columns=tel_df.columns)
        else:
            tel_matches = tel_df[
                (tel_df['Project'].str.startswith(job_num)) & 
                ((tel_df['Phase'] == target_phase) | (target_phase == ""))
            ]

        for sys in systems:
            # THE UPGRADE: Include Ambient sensors in the registry pool math for this block
            if sys == "":
                block_pool = pool_matches
            else:
                block_pool = pool_matches[(pool_matches['System'] == sys) | (pool_matches['Location'] == 'AMBIENT')]
                
            total_assigned = block_pool['total_assigned'].sum() if not block_pool.empty else 0
            
            if not tel_matches.empty:
                is_sys = tel_matches['System'] == sys
                is_amb = tel_matches['Location'].astype(str).str.upper() == 'AMBIENT'
                
                if sys == "": 
                    sys_tel = tel_matches
                else:
                    sys_tel = tel_matches[is_sys | is_amb]
            else:
                sys_tel = tel_matches 

            if total_assigned == 0 and sys_tel.empty:
                continue 

            title_ext = []
            if target_phase: title_ext.append(f"Phase {target_phase}")
            if sys: title_ext.append(f"System {sys}")
            title_suffix = f" ({', '.join(title_ext)})" if title_ext else ""

            # --- DATE CALCULATION LOGIC ---
            # --- SEAMLESS DATE LOGIC ---
            f_date = row.get('Date_Freezedown')
            m_date = row.get('Date_Maintenance') 

            def is_valid_date(val):
                if pd.isnull(val): return False
                val_str = str(val).strip().lower()
                if val_str in ['', 'nan', 'nat', 'none', '<na>']: return False
                return True

            header_html = "<div style='text-align: right;'><small>Start: Not Set</small></div>"

            if is_valid_date(f_date):
                f_date_dt = pd.to_datetime(f_date).date()
                f_date_display = f_date_dt.strftime('%b %d, %Y')
                
                # ALWAYS calculate the total time since the project started freezing
                total_freezedown_days = (pd.Timestamp.now(tz=display_tz).date() - f_date_dt).days
                
                if is_valid_date(m_date):
                    m_date_dt = pd.to_datetime(m_date).date()
                    m_date_display = m_date_dt.strftime('%b %d, %Y')
                    
                    # Calculate durations for the specific phases
                    time_to_freeze = (m_date_dt - f_date_dt).days
                    maintenance_days = (pd.Timestamp.now(tz=display_tz).date() - m_date_dt).days
                    
                    header_html = f"""
                        <div style='text-align: right; line-height: 1.3;'>
                            🗓️ <b>Freezedown: {max(0, total_freezedown_days)} Days</b><br>
                            <small style='color: #666;'>Start Freezedown: {f_date_display}</small><br>
                            <span style='color: #28a745; display: inline-block; margin-top: 4px;'>✅ <b>Full Freezedown Provided</b></span><br>
                            <small style='color: #666;'>
                                Start Maintenance: {m_date_display}<br>
                                Time to Freeze: {max(0, time_to_freeze)} Days | In Maintenance: {max(0, maintenance_days)} Days
                            </small>
                        </div>
                    """
                else:
                    # Active freezedown (no maintenance yet)
                    header_html = f"""
                        <div style='text-align: right; line-height: 1.3;'>
                            🗓️ <b>Freezedown: {max(0, total_freezedown_days)} Days</b><br>
                            <small style='color: #666;'>Start Freezedown: {f_date_display}</small>
                        </div>
                    """

            # ====================================================================
            # RENDER THE CONTAINER 
            # ====================================================================
            with st.container(border=True):
                h1, h2 = st.columns([2, 1])
                h1.subheader(f"🏗️ {p_name}{title_suffix}")
                
                # Inject the dynamic HTML we built above
                h2.markdown(header_html, unsafe_allow_html=True)
                
                st.markdown(f"🔗 **External Client Portal:** [{p_name} Portal Site Link](https://sf{job_num}.streamlit.app)")
                
                if not sys_tel.empty:
                    active_1h = sys_tel[sys_tel['checkins_1h'] > 0]['NodeNum'].nunique()
                    active_6h = sys_tel[sys_tel['checkins_6h'] > 0]['NodeNum'].nunique()
                    active_24h = sys_tel[sys_tel['checkins_24h'] > 0]['NodeNum'].nunique()
                else:
                    active_1h = active_6h = active_24h = 0
                
                status_color = "🟢" if active_24h >= total_assigned and total_assigned > 0 else "🔴" if active_24h == 0 else "🟠"
                st.markdown(
                    f"{status_color} **Hardware Status:** `{active_1h}` (1h) | "
                    f"`{active_6h}` (6h) | `{active_24h}` (24h) | "
                    f"Assigned Pool: `{total_assigned}`"
                )
                st.divider() 

                if sys_tel.empty:
                    st.info(f"No recent telemetry received for {p_project}{title_suffix}.")
                    continue

                is_amb_col = sys_tel['Location'].astype(str).str.upper() == 'AMBIENT'
                is_tp_col = sys_tel['Depth'].notnull() & (sys_tel['Depth'].astype(str).str.strip() != '') & ~is_amb_col
                is_s_col = (sys_tel['Bank'].astype(str).str.startswith('S') | sys_tel['Location'].astype(str).str.startswith('S')) & ~is_amb_col & ~is_tp_col
                is_r_col = (sys_tel['Bank'].astype(str).str.startswith('R') | sys_tel['Location'].astype(str).str.startswith('R')) & ~is_amb_col & ~is_tp_col

                groups_data = [
                    ("📥 Supply", sys_tel[is_s_col], -10), 
                    ("📤 Return", sys_tel[is_r_col], 0), 
                    ("📏 TempPipes", sys_tel[is_tp_col], 32)
                ]

                if st.session_state.get("global_show_ambient", True):
                    groups_data.append(("☁️ Ambient", sys_tel[is_amb_col], None))

                cols = st.columns(len(groups_data))
                for idx, (title, g_df, target_temp) in enumerate(groups_data):
                    with cols[idx]:
                        render_dashboard_column(title, g_df, target_temp, unit_mode, unit_label)

def render_dashboard_column(title, g_df, target_temp, unit_mode, unit_label):
    """Helper layout compiler to handle repeating column metric sets cleanly."""
    st.markdown(f"**{title}**")
    if g_df.empty or g_df['latest_temp'].isnull().all():
        st.caption("No recent data")
        return
    
    # Mathematical averages strictly use the dataframe passed in, zero cross-contamination
    latest_val = g_df['latest_temp'].mean()
    c_min, c_max = g_df['min_now'].min(), g_df['max_now'].max()
    m24, x24 = g_df['min_24h'].min(), g_df['max_24h'].max()

    def convert(v):
        if pd.isnull(v) or pd.isna(v): return None
        return (v - 32) * 5/9 if unit_mode == "Celsius" else v

    l_conv, c_min, c_max, m24, x24 = map(convert, [latest_val, c_min, c_max, m24, x24])

    st.metric("Avg (Latest)", f"{l_conv:.1f}{unit_label}")
    
    # Calculate % of nodes meeting target goal directly in Python
    if target_temp is not None:
        total_valid_nodes = g_df['NodeNum'].nunique()
        if total_valid_nodes > 0:
            nodes_meeting_target = g_df[g_df['latest_temp'] <= target_temp]['NodeNum'].nunique()
            pct = (nodes_meeting_target / total_valid_nodes) * 100
            color = "green" if pct == 100 else "#FF8C00" if pct > 0 else "gray"
            
            # --- NEW: Convert target limit for display if in Celsius ---
            display_target = target_temp
            if unit_mode == "Celsius":
                display_target = (target_temp - 32) * 5/9
                
            st.markdown(f"<p style='font-size:0.85rem; color:{color};'><b>{pct:.0f}%</b> Nodes ≤ {display_target:.1f}{unit_label}</p>", unsafe_allow_html=True)

    range_html = "<div style='font-size: 0.8rem; line-height: 1.2; margin-bottom: 10px;'><b>Normal Ranges:</b><br>"
    if c_min is not None and c_max is not None:
        range_html += f"Current: {c_min:.1f} to {c_max:.1f}{unit_label}<br>"
    else:
        range_html += "Current: No Data<br>"
    
    if m24 is not None and x24 is not None:
        range_html += f"24h Range: {m24:.1f} to {x24:.1f}{unit_label}"
    else:
        range_html += "24h Range: No Data"
    range_html += "</div>"
    st.markdown(range_html, unsafe_allow_html=True)
    st.markdown("<div style='font-size: 0.75rem; border-top: 1px solid #eee; padding-top: 5px;'>", unsafe_allow_html=True)

def get_trend_arrow(current, previous):
    """Helper to generate trend icons with updated blue downward arrow."""
    if pd.isnull(current) or pd.isnull(previous): return "N/A"
    delta = current - previous
    if delta > 0.1: return f"🔺 +{delta:.1f}"
    if delta < -0.1: return f"🔹 {delta:.1f}"
    return "➡️ 0.0"
