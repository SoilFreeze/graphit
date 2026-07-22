import streamlit as st
import pandas as pd
from google.cloud import bigquery

# Import your custom app modules
from app.utils.config import MASTER_VIEW, PROJECT_ID, DATASET_ID
from app.components.charts import build_high_speed_graph

# =============================================================================
# WORKSPACE PAGE 4: SENSOR STATUS COMPONENT LIST
# =============================================================================
def fmt_temp(val, unit_mode, unit_label):
    """Standalone helper utility to safely format raw float metrics into clean text values."""
    if pd.isnull(val) or pd.isna(val):
        return "N/A"
    v = (val - 32) * 5/9 if unit_mode == "Celsius" else val
    return f"{v:.1f}{unit_label}"


def assign_row_color(hours):
    """Standalone utility mapping data latency windows directly to CSS background colors."""
    if hours is None or pd.isna(hours) or hours == float('inf'):
        return "background-color: #d1d5db; color: #1f2937;"  # Gray / Offline
    if hours < 1.0:
        return "background-color: #d1fae5; color: #065f46;"  # Green / Online
    if 1.0 <= hours <= 6.0:
        return "background-color: #fef08a; color: #854d0e;"  # Yellow / Warning
    if 6.0 < hours <= 12.0:
        return "background-color: #fed7aa; color: #9a3412;"  # Orange / Stale
    return "background-color: #fca5a5; color: #991b1b;"      # Red / Critical


def render_sensor_status(client, selected_project, unit_label, unit_mode, display_tz):
    """
    Page Name: Sensor Status
    Strictly locked to: project_registry, master_data_view_v2, and manual_rejections.
    """
    # 1. HEADER LOGIC
    p_meta = st.session_state.get('project_metadata')
    if not p_meta or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to view sensor health.")
        return

    p_name = p_meta.get('ProjectName', selected_project)
    f_date = p_meta.get('Date_Freezedown')
    st.title(f"❄️ {p_name}")
    
    if pd.notnull(f_date):
        days = (pd.Timestamp.now(tz=display_tz).date() - pd.to_datetime(f_date).date()).days
        st.markdown(f"## 🗓️ Day **{max(0, days)}** of Freezedown")
    st.divider()

    # --- THE FIX: Extract Job Number and Phase for accurate v2 querying ---
    job_num = str(selected_project).split('-')[0].strip()
    
    import re
    phase_match = re.search(r'(?i)Phase\s*(\d+)', selected_project)
    phase_sql = ""
    if phase_match:
        target_phase = phase_match.group(1)
        # Safely cast to string and trim to match your v2 schema perfectly
        phase_sql = f"AND TRIM(CAST(m.Phase AS STRING)) = '{target_phase}'"
        st.caption(f"🎯 Auto-filtered to **Phase {target_phase}**")

    # --- NEW FIX: Dynamic Status Filter ---
    # If the active project is the Office, drop the filter to show ALL sensors
    if 'OFFICE' in job_num.upper():
        status_sql = "" 
    else:
        # Otherwise, only allow active field and diagnostic nodes
        status_sql = "AND UPPER(CAST(m.SensorStatus AS STRING)) IN ('ON PROJECT', 'DIAGNOSTIC')"

    # 2. TELEMETRY & COVERAGE QUERY (Uses updated master_data_view_v2)
    query = f"""
        WITH BaseReporting AS (
            SELECT m.NodeNum, m.timestamp, m.temperature, m.Location, m.Bank, m.Depth
            FROM `{MASTER_VIEW}` m
            WHERE m.Project LIKE CONCAT(@job_num, '%') 
              {phase_sql}
              AND m.NodeNum IS NOT NULL
              {status_sql}
        ),
        GapAnalysis AS (
            SELECT *, LAG(timestamp) OVER (PARTITION BY NodeNum ORDER BY timestamp) AS prev_ts
            FROM BaseReporting
        ),
        HistoricalStats AS (
            SELECT 
                NodeNum, Location, Bank, Depth,
                MAX(timestamp) AS last_ping,
                ARRAY_AGG(temperature ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS current_temp,
                AVG(CASE WHEN timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN temperature END) as avg_1h,
                AVG(CASE WHEN timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 25 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature END) as avg_24h,
                
                -- Pulse Check Flags
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN 1 ELSE 0 END) as seen_1h_f,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR) THEN 1 ELSE 0 END) as seen_6h_f,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN 1 ELSE 0 END) as seen_24h_f,

                -- Hourly Coverage Calculation
                (COUNT(DISTINCT CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN TIMESTAMP_TRUNC(timestamp, HOUR) END) / 24.0) * 100 as coverage_24h,
                (COUNT(DISTINCT CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 168 HOUR) THEN TIMESTAMP_TRUNC(timestamp, HOUR) END) / 168.0) * 100 as coverage_7d,

                -- Extremes & Gaps
                MIN(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature END) AS low_24h,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature END) AS high_24h,
                MAX(TIMESTAMP_DIFF(timestamp, prev_ts, HOUR)) AS max_gap_7d
            FROM GapAnalysis 
            GROUP BY NodeNum, Location, Bank, Depth
        )
        SELECT * FROM HistoricalStats
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("job_num", "STRING", job_num)]
    )
    try:
        df = client.query(query, job_config=job_config).to_dataframe()
        if df.empty:
            st.warning("No data found in master_data_view_v2 for this project.")
            return

        # 3. STATUS & LAG CALCULATIONS
        now_local = pd.Timestamp.now(tz=display_tz)
        def get_lag(ts):
            if pd.isnull(ts): return 999.0
            ts_aware = ts if ts.tzinfo else ts.tz_localize('UTC')
            return (now_local - ts_aware.tz_convert(display_tz)).total_seconds() / 3600

        df['last_seen_hrs'] = df['last_ping'].apply(get_lag)

        # 4. FORMATTING HELPERS
        def get_status_icon(hrs):
            if hrs == float('inf') or hrs >= 999.0: return "❌ Never"
            if hrs <= 1.0: return f"🟢 {hrs:.1f}h"
            if hrs <= 6.0: return f"🟠 {hrs:.1f}h"
            return f"🔴 {hrs:.1f}h"

        def fmt_t(val):
            if pd.isnull(val): return "N/A"
            v = (val - 32) * 5/9 if unit_mode == "Celsius" else val
            return f"{v:.1f}{unit_label}"

        def get_arrow(cur, prev):
            if pd.isnull(cur) or pd.isnull(prev): return "N/A"
            d = cur - prev
            return f"🔺 +{d:.1f}" if d > 0.1 else f"🔹 {d:.1f}" if d < -0.1 else "➡️ 0.0"

        # 5. LOCATION PERFORMANCE SUMMARY
        st.subheader("📍 Location Performance Summary")
        
        summary_rows = []
        for loc, loc_group in df.groupby('Location'):
            min_hours_lag = loc_group['last_seen_hrs'].min()
            max_hours_lag = loc_group['last_seen_hrs'].max()
            
            summary_rows.append({
                'Location': loc,
                'Total Nodes': int(len(loc_group)),
                'Seen 1h': int(loc_group['seen_1h_f'].sum()),
                'Seen 6h': int(loc_group['seen_6h_f'].sum()),
                'Seen 24h': int(loc_group['seen_24h_f'].sum()),
                '24h Coverage': f"{loc_group['coverage_24h'].mean():.1f}%",
                '7d Coverage': f"{loc_group['coverage_7d'].mean():.1f}%",
                'Avg Temp': fmt_t(loc_group['current_temp'].mean()),
                'Low 24h': fmt_t(loc_group['low_24h'].min()),
                'High 24h': fmt_t(loc_group['high_24h'].max()),
                'Best Seen': get_status_icon(min_hours_lag),
                'Worst Seen': get_status_icon(max_hours_lag)
            })
            
        summary_df = pd.DataFrame(summary_rows)

        def style_missing_counters(val_df):
            canvas = pd.DataFrame('', index=val_df.index, columns=val_df.columns)
            target_cols = ['Seen 1h', 'Seen 6h', 'Seen 24h']
            
            for idx in val_df.index:
                total = val_df.loc[idx, 'Total Nodes']
                for col in target_cols:
                    seen = val_df.loc[idx, col]
                    missing = total - seen
                    
                    if missing == 0:
                        bg_style = "background-color: #d1fae5; color: #065f46; font-weight: bold;"
                    elif 1 <= missing <= 3:
                        bg_style = "background-color: #bbf7d0; color: #14532d; font-weight: bold;"
                    elif 4 <= missing <= 6:
                        bg_style = "background-color: #fef08a; color: #713f12; font-weight: bold;"
                    elif 7 <= missing <= 10:
                        bg_style = "background-color: #fed7aa; color: #7c2d12; font-weight: bold;"
                    else:
                        bg_style = "background-color: #fca5a5; color: #7f1d1d; font-weight: bold;"
                        
                    canvas.loc[idx, col] = bg_style
            return canvas

        st.dataframe(summary_df.style.apply(style_missing_counters, axis=None), use_container_width=True, hide_index=True)

        # 6. DETAILED SENSOR AUDIT
        st.divider()
        st.subheader("🔍 Detailed Sensor Audit")
        
        selected_loc = st.selectbox("Filter Audit by Location:", ["--- All ---"] + sorted(df['Location'].unique()))
        audit_df = df.copy() if selected_loc == "--- All ---" else df[df['Location'] == selected_loc]
        
        rows = []
        for _, r in audit_df.sort_values(['Location', 'Depth', 'Bank']).iterrows():
            rows.append({
                "Node": r['NodeNum'],
                "Location": r['Location'],
                "Position": f"{r['Depth']}ft" if pd.notnull(r['Depth']) else f"Bank {r['Bank']}",
                "Last Seen": get_status_icon(r['last_seen_hrs']),
                "24 hour coverage": f"{r['coverage_24h']:.1f}%",
                "Current Temp": fmt_t(r['current_temp']),
                "Change for 1 hr": get_arrow(r['current_temp'], r['avg_1h']),
                "Change for 24 hr": get_arrow(r['current_temp'], r['avg_24h']),
                "24 hr high": fmt_t(r['high_24h']),
                "24 hour low": fmt_t(r['low_24h'])
            })
        
        st.dataframe(rows, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Sensor Status Error: {e}")

def render_hardware_integrity_table(client, selected_project, unit_mode, unit_label, target_registry):
    """Renders a detailed table showing connectivity, coverage, and recent activity sorted by latency."""
    st.subheader("📋 Hardware Integrity & Connectivity")
    
    query = f"""
        SELECT 
            n.NodeNum, n.Location, n.Bank, n.Depth, n.SensorStatus,
            MAX(m.timestamp) as last_ping,
            ARRAY_AGG(m.temperature ORDER BY m.timestamp DESC LIMIT 1)[OFFSET(0)] as last_temp,
            COUNTIF(m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) as pings_1h,
            COUNTIF(m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)) as pings_6h,
            COUNTIF(m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) as pings_24h,
            (COUNT(DISTINCT CASE 
                WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) 
                -- standard hour truncations used inside manual rejection tables
                THEN TIMESTAMP_TRUNC(m.timestamp, HOUR) 
             END) / 24.0) * 100 as coverage_24h,
            AVG(CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN m.temperature END) as avg_now,
            AVG(CASE WHEN m.timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN m.temperature END) as avg_1h_prev
        FROM `{target_registry}` n
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.master_data_view_v2` m 
          ON n.NodeNum = m.NodeNum AND m.NodeNum IS NOT NULL
        WHERE n.Project = @proj_id AND (n.End_Date IS NULL OR TRIM(CAST(n.End_Date AS STRING)) = '')
        GROUP BY 1, 2, 3, 4, 5
    """
    
    try:
        df = client.query(query, job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)]
        )).to_dataframe()
    except Exception as e:
        st.error(f"Hardware Table Query Failed: {e}")
        return

    if df.empty: 
        st.info("No active nodes found for connectivity table.")
        return

    now_utc = pd.Timestamp.now(tz='UTC')

    def row_processor(row):
        ping = row['last_ping']
        if pd.isnull(ping):
            hours_hidden = float('inf')
            txt = "❌ Never"
            style = "background-color: #d1d5db; color: #1f2937;" 
        else:
            ts = ping if ping.tzinfo else ping.tz_localize('UTC')
            diff_mins = (now_utc - ts).total_seconds() / 60.0
            hours_hidden = diff_mins / 60.0
            
            if hours_hidden < 1.0:
                txt = f"{int(diff_mins)}m ago" if diff_mins >= 1.0 else "Just now"
                style = "background-color: #d1fae5; color: #065f46;" 
            elif 1.0 <= hours_hidden <= 6.0:
                txt = f"{hours_hidden:.1f}h ago"
                style = "background-color: #fef08a; color: #854d0e;" 
            elif 6.0 < hours_hidden <= 12.0:
                txt = f"{hours_hidden:.1f}h ago"
                style = "background-color: #fed7aa; color: #9a3412;" 
            elif 12.0 < hours_hidden <= 24.0:
                txt = f"{hours_hidden:.1f}h ago"
                style = "background-color: #fca5a5; color: #991b1b;" 
            else:
                txt = f"{hours_hidden:.1f}h ago"
                style = "background-color: #d1d5db; color: #1f2937;" 
        
        pos = f"{row['Depth']}ft" if (pd.notnull(row['Depth']) and row['Depth'] != 0) else f"Bank {row['Bank']}"
        trend = get_trend_arrow(row['avg_now'], row['avg_1h_prev'])
        
        return pd.Series([txt, style, pos, trend, hours_hidden])

    df[['Seen_Text', 'Seen_Style', 'Pos_Label', 'Trend', 'hours_hidden']] = df.apply(row_processor, axis=1)
    df['hours_hidden'] = pd.to_numeric(df['hours_hidden'], errors='coerce').fillna(float('inf'))
    df = df.sort_values(by='hours_hidden', ascending=True).reset_index(drop=True)

    display_df = pd.DataFrame({
        "Node ID": df['NodeNum'],
        "Location": df['Location'],
        "Position": df['Pos_Label'],
        "Last Seen": df['Seen_Text'],
        "24h Coverage": df['coverage_24h'], 
        "1h Change": df['Trend'],
        "Last Temp": df['last_temp'].apply(lambda x: fmt_temp(x, unit_mode, unit_label)),
        "1h Pings": df['pings_1h'],
        "6h Pings": df['pings_6h'],
        "24h Pings": df['pings_24h']
    })

    def diagnostic_styler(data):
        style_df = pd.DataFrame('', index=data.index, columns=data.columns)
        for i in data.index:
            style_df.loc[i, 'Last Seen'] = df.loc[i, 'Seen_Style']
            if df.loc[i, 'SensorStatus'] == 'Diagnostic':
                style_df.loc[i, 'Node ID'] = 'background-color: #ff4b4b; color: white; font-weight: bold;'
        return style_df

    st.dataframe(
        display_df.style.apply(diagnostic_styler, axis=None), 
        use_container_width=True, 
        hide_index=True,
        column_config={
            "24h Coverage": st.column_config.ProgressColumn(
                "24h Coverage", 
                format="%.1f%%", 
                min_value=0, 
                max_value=100
            ),
            "1h Pings": st.column_config.NumberColumn("1h Pings", format="%d"),
            "6h Pings": st.column_config.NumberColumn("6h Pings", format="%d"),
            "24h Pings": st.column_config.NumberColumn("24h Pings", format="%d"),
        }
    )
