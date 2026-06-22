import streamlit as st
import pandas as pd
import time
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta, date, time as dt_time
import re
import numpy as np

# =============================================================================
# 1. CONFIGURATION, LAYOUT & STYLING
# =============================================================================
st.set_page_config(
    page_title="SoilFreeze Data Lab", 
    page_icon="❄️", 
    layout="wide"
)

# Global Database Constants - Linked to Read-Only Infrastructure
DATASET_ID = "Temperature" 
PROJECT_ID = "sensorpush-export"

# Schema-Aligned Table References
PROJECT_REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.project_registry"
NODE_REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.node_registry"
MASTER_VIEW = f"{PROJECT_ID}.{DATASET_ID}.master_data_view"
REF_CURVE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.reference_curves"
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"

@st.cache_resource
def get_bq_client():
    """
    Initializes and caches the BigQuery connection.
    Includes mandatory Google Drive scopes for federated Google Sheet tables.
    """
    try:
        SCOPES = [
            "https://www.googleapis.com/auth/bigquery", 
            "https://www.googleapis.com/auth/drive" 
        ]
        
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(
                info, 
                scopes=SCOPES
            )
            return bigquery.Client(credentials=credentials, project=info["project_id"])
        
        return bigquery.Client(project=PROJECT_ID)

    except Exception as e:
        st.error(f"❌ BigQuery Authentication Failed: {e}")
        return None

# =============================================================================
# 2. DATA PIPELINE UTILITIES & ENGINES
# =============================================================================
@st.cache_data(ttl=600)
def get_universal_portal_data(project_id, is_summary_page=False):
    """
    Unified Time-Aware Data Fetcher.
    Joins telemetry to registry by NodeNum AND valid active date range.
    Filters out locations specified in user preferences.
    """
    client = get_bq_client()
    if client is None: return pd.DataFrame()
    
    clean_token = str(project_id).replace("'", "''").strip()
    base_job_num = clean_token.split('-')[0].strip()

    # Apply strict physical filter boundaries
    filter_logic = "" if is_summary_page else """
        AND (UPPER(COALESCE(n.Location, m.Location)) LIKE 'BANK%' 
             OR REGEXP_CONTAINS(UPPER(COALESCE(n.Location, m.Location)), r'^T[0-9]+'))
    """

    query = f"""
        SELECT 
            m.Project,
            m.NodeNum,
            m.temperature,
            m.timestamp,
            m.approval_status,
            COALESCE(n.Location, m.Location, 'Unassigned') as Location,
            COALESCE(n.Bank, m.Bank, '—') as Bank,
            COALESCE(n.Depth, m.Depth) as Depth
        FROM `{MASTER_VIEW}` m
        LEFT JOIN `{NODE_REGISTRY_TABLE}` n 
            ON m.NodeNum = n.NodeNum
            -- TIME-BOUND LOCK: Filter out stale histories by validating active ranges
            AND m.timestamp >= CAST(n.Start_Date AS TIMESTAMP)
            AND (m.timestamp <= CAST(n.End_Date AS TIMESTAMP) OR n.End_Date IS NULL)
        WHERE m.temperature >= -30.0 AND m.temperature <= 120.0
          AND (m.Project = @project_id OR m.Project LIKE '{base_job_num}%')
          AND n.Project IS NOT NULL
          {filter_logic}
          -- Permanent asset tracking visibility rules
          AND UPPER(COALESCE(n.Location, m.Location)) NOT IN ('DEAD STOCK', 'ELIZABETH', 'OFFICE')
        ORDER BY m.timestamp ASC
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("project_id", "STRING", project_id)]
    )
    
    try:
        return client.query(query, job_config=job_config).to_dataframe()
    except Exception as e:
        st.error(f"⚠️ Data Sync Error: {e}")
        return pd.DataFrame()

def apply_sanity_filter(df):
    """
    Automated filter for anomalous data points.
    Flags anything outside the physical limit range of -30°C to 120°C as BADDATA.
    """
    if df.empty:
        return df
        
    if 'NodeNum' in df.columns:
        df = df.dropna(subset=['NodeNum']).copy()
        
    if df.empty:
        return df

    bad_condition = (df['temperature'] > 120.0) | (df['temperature'] < -30.0)
    mask_col = 'approve' if 'approve' in df.columns else 'approval_status' if 'approval_status' in df.columns else None
    
    if mask_col:
        df.loc[bad_condition, mask_col] = 'BADDATA'

    return df

def run_office_auto_assignment():
    """Surgically assigns 'OFFICE' status to staging records for office projects."""
    client = get_bq_client()
    if client is None: return
    sql = f"""
        MERGE `{OVERRIDE_TABLE}` T
        USING (
            SELECT DISTINCT r.NodeNum, TIMESTAMP_TRUNC(r.timestamp, HOUR) as ts
            FROM (
                SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` 
                UNION ALL 
                SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
            ) AS r
            INNER JOIN `{NODE_REGISTRY_TABLE}` AS n ON r.NodeNum = n.NodeNum
            WHERE n.Project LIKE '%OFFICE%' 
        ) S ON T.NodeNum = S.NodeNum AND T.timestamp = S.ts
        WHEN MATCHED THEN UPDATE SET approve = 'OFFICE'
        WHEN NOT MATCHED THEN INSERT (NodeNum, timestamp, approve) VALUES (S.NodeNum, S.ts, 'OFFICE')
    """
    try: 
        client.query(sql).result()
        st.success("✅ Success.")
    except Exception as e: 
        st.error(f"Failed: {e}")

# =============================================================================
# 3. INTERACTIVE VISUALIZATION COMPILERS
# =============================================================================
def natural_sort_key(s):
    """Splits strings into alphanumeric chunks to allow natural ordering lists."""
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', str(s))]

def fmt_temp(val, unit_mode, unit_label):
    """Formats raw float telemetry metrics into unit strings."""
    if pd.isnull(val) or pd.isna(val):
        return "N/A"
    v = (val - 32) * 5/9 if unit_mode == "Celsius" else val
    return f"{v:.1f}{unit_label}"

def assign_row_color(hours):
    """Maps data tracking latency windows straight to CSS background hex matrices."""
    if hours is None or pd.isna(hours) or hours == float('inf'):
        return "background-color: #d1d5db; color: #1f2937;"
    if hours < 1.0:
        return "background-color: #d1fae5; color: #065f46;"
    if 1.0 <= hours <= 6.0:
        return "background-color: #fef08a; color: #854d0e;"
    if 6.0 < hours <= 12.0:
        return "background-color: #fed7aa; color: #9a3412;"
    return "background-color: #fca5a5; color: #991b1b;"

def get_trend_arrow(current, previous):
    """Calculates directional rate-of-change metrics."""
    if pd.isnull(current) or pd.isnull(previous): return "N/A"
    delta = current - previous
    if delta > 0.1: return f"🔺 +{delta:.1f}"
    if delta < -0.1: return f"🔹 {delta:.1f}"
    return "➡️ 0.0"

def get_soil_reference_curves(soil_type, start_date, unit_mode):
    """Fallback compiler providing static hardcoded structural curve reference metrics."""
    references = {
        "Silty Sand": [(0, 50), (5, 32), (14, 20), (30, 10), (60, 5)], 
        "Clay": [(0, 50), (10, 32), (25, 25), (45, 15), (90, 10)]
    }
    curve = references.get(soil_type, [])
    if not curve: return None, None
    x_times = [pd.Timestamp(start_date) + pd.Timedelta(days=d) for d, t in curve]
    y_temps = [t if unit_mode == "Fahrenheit" else (t - 32) * 5/9 for d, t in curve]
    return x_times, y_temps

def build_high_speed_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label, 
                           display_tz="UTC", mobile_mode=False, f_start_date=None, curve_id=None):
    """
    Engineering-Grade Trend Graph.
    - Legend: Naturally sorted by numerical depth order.
    - Gaps: Line continuity breaks if data gaps exceed 6 hours.
    - Grids: Darkened gridlines on Monday timestamps for tracking.
    """
    if df.empty: return go.Figure().update_layout(title="No data available")

    client = get_bq_client()
    plot_df = df.copy() 

    if plot_df['timestamp'].dt.tz is None:
        plot_df['timestamp'] = plot_df['timestamp'].dt.tz_localize('UTC')
    plot_df['timestamp'] = plot_df['timestamp'].dt.tz_convert(display_tz)
    
    freeze_pt = 0 if unit_mode == "Celsius" else 32
    y_range = [-30, 30] if unit_mode == "Celsius" else [-20, 80]

    fig = go.Figure()

    # Render Historical reference Curves
    if curve_id and curve_id != "None" and f_start_date:
        try:
            proj_str = str(st.session_state.get('selected_project', ''))
            proj_match = re.findall(r'\d+', proj_str)
            proj_num = proj_match[0] if proj_match else ""
            loc_part = str(curve_id).split('-')[-1].strip() if curve_id else ""

            if proj_num and loc_part:
                target_q = f"""
                    SELECT CurveID, Day, Temp 
                    FROM `{REF_CURVE_TABLE}` 
                    WHERE REGEXP_CONTAINS(CurveID, r'^{proj_num}.*{loc_part}$')
                    ORDER BY Day
                """
                target_df = client.query(target_q).to_dataframe()
                if not target_df.empty:
                    dash_styles = ['dashdot', 'dash', 'dot']
                    gray_shades = ['rgba(30,30,30,0.8)', 'rgba(70,70,70,0.75)', 'rgba(110,110,110,0.7)']
                    
                    for c_idx, (cid, c_df) in enumerate(target_df.groupby('CurveID')):
                        c_df['timestamp'] = c_df['Day'].apply(lambda d: pd.Timestamp(f_start_date) + pd.Timedelta(days=d))
                        c_df['timestamp'] = c_df['timestamp'].dt.tz_localize('UTC').dt.tz_convert(display_tz)
                        ref_y = c_df['Temp'] if unit_mode == "Fahrenheit" else (c_df['Temp'] - 32) * 5/9
                        
                        label_clean = str(cid).replace(f"{proj_num}-", "").replace(f"-{loc_part}", "")
                        display_label = f"Goal: {label_clean}" if label_clean != loc_part else f"Goal: {loc_part}"
                        
                        fig.add_trace(go.Scatter(
                            x=c_df['timestamp'], y=ref_y, name=f"<b>{display_label}</b>", 
                            mode='lines',
                            line=dict(color=gray_shades[c_idx % len(gray_shades)], width=3.5, dash=dash_styles[c_idx % len(dash_styles)], shape='spline', smoothing=1.3),
                            legendrank=1 
                        ))
        except Exception: pass

    sf_15_palette = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf', '#FF1493', '#00CED1', '#FFD700', '#8A2BE2', '#32CD32']
    
    node_metadata = []
    for sn in plot_df['NodeNum'].unique():
        node_df = plot_df[plot_df['NodeNum'] == sn]
        depth_val = node_df['Depth'].iloc[0]
        bank_val = node_df['Bank'].iloc[0]
        loc_val = node_df['Location'].iloc[0]

        if pd.notnull(bank_val) and any(x in str(bank_val).upper() for x in ['S', 'R']):
            display_name = f"{bank_val} ({sn})"
            sort_val = str(bank_val)  
        elif pd.notnull(depth_val) and not pd.isna(depth_val): 
            display_name = f"{depth_val}ft ({sn})"
            sort_val = f"depth_{float(depth_val):05.1f}" 
        else: 
            display_name = f"{loc_val} ({sn})"
            sort_val = str(display_name)

        node_metadata.append({'node_num': sn, 'display_name': display_name, 'sort_key': sort_val})

    sorted_node_configs = sorted(node_metadata, key=lambda x: natural_sort_key(x['sort_key']))

    for i, config in enumerate(sorted_node_configs):
        sn = config['node_num']
        display_name = config['display_name']
        
        s_df = plot_df[plot_df['NodeNum'] == sn].sort_values('timestamp')
        s_df = s_df.set_index('timestamp').resample('1h').first().reset_index()
        
        fig.add_trace(go.Scatter(
            x=s_df['timestamp'], y=s_df['temperature'],
            name=display_name, mode='lines',
            connectgaps=False, 
            line=dict(shape='spline', smoothing=1.3, width=2, color=sf_15_palette[i % 15]),
            hovertemplate="<b>%{fullData.name}</b><br>Time: %{x|%H:%M}<br>Temp: %{y:.1f}" + unit_label + "<extra></extra>"
        ))

    fig.add_hline(y=freeze_pt, line_width=2, line_dash="dash", line_color="RoyalBlue", annotation_text="32°F FREEZE", layer="above")
    
    now_ts = pd.Timestamp.now(tz=display_tz)
    fig.add_vline(x=now_ts.to_pydatetime(), line_width=2, line_color="red", line_dash="dash", layer='above')
    
    m_range = pd.date_range(start=start_view, end=end_view, freq='W-MON')
    for m_dt in m_range:
        fig.add_vline(x=m_dt, line_width=1.5, line_color="black", opacity=0.4)

    fig.update_layout(
        title=dict(text=f"<b>{st.session_state.get('selected_project', 'Project')} - Thermal Trend - {title}</b>", x=0.02, y=0.98, font=dict(size=18)),
        plot_bgcolor='white', hovermode="x unified", height=650,
        xaxis=dict(range=[start_view, end_view], showgrid=True, gridcolor='Gainsboro', showline=True, mirror=True, linecolor='black', linewidth=2, hoverformat='%A, %b %d, %Y', tickformat='%b %d', minor=dict(dtick=1000*60*60*24, showgrid=True, gridcolor='#f8f8f8')),
        yaxis=dict(title=f"Temperature ({unit_label})", range=y_range, dtick=10, showgrid=True, gridcolor='Gainsboro', showline=True, mirror=True, linecolor='black', linewidth=2, minor=dict(dtick=2, showgrid=True, gridcolor='#f8f8f8')),
        legend=dict(orientation="v", x=1.02, y=1, xanchor="left", yanchor="top")
    )
    return fig

# =============================================================================
# 4. SIDEBAR SELECTION SYSTEM CONTROL ENVIRONMENT
# =============================================================================
if sidebar_client is not None:
    try:
        proj_q = f"""
            SELECT CAST(Project AS STRING) as Project, ProjectName, Timezone, ProjectStatus, Date_Freezedown
            FROM `{PROJECT_REGISTRY_TABLE}` 
            WHERE Project IS NOT NULL 
              AND TRIM(CAST(Project AS STRING)) != ''
              AND (
                  UPPER(TRIM(CAST(ShowActive AS STRING))) IN ('TRUE', 'YES', '1') 
                  OR UPPER(CAST(Project AS STRING)) LIKE '%OFFICE%'
              )
        """
        proj_df = sidebar_client.query(proj_q).to_dataframe()
        proj_list = sorted([str(p).strip() for p in proj_df['Project'].unique() if p and str(p).strip().lower() not in ['none', 'nan', 'null', '']])
        
        selected_project = st.sidebar.selectbox("🎯 Active Project", ["All Projects"] + proj_list, key="sidebar_proj_picker_global")
        st.session_state['selected_project'] = selected_project
        
        if selected_project != "All Projects":
            meta_row = proj_df[proj_df['Project'] == selected_project]
            if not meta_row.empty:
                st.session_state['project_metadata'] = meta_row.iloc[0].to_dict()
        else:
            st.session_state['project_metadata'] = None
            
    except Exception as e:
        st.sidebar.error(f"Registry Link Offline: {e}")

@st.cache_data(ttl=600)
def get_universal_portal_data(project_id):
    """
    Unified Time-Aware Data Fetcher.
    Joins telemetry to registry by NodeNum AND valid date range.
    """
    client = get_bq_client()
    if client is None: return pd.DataFrame()
    
    clean_token = str(project_id).replace("'", "''").strip()
    base_job_num = clean_token.split('-')[0].strip()

    # The JOIN logic below creates the "Time-Bound Lock"
    query = f"""
        SELECT 
            m.Project,
            m.NodeNum,
            m.temperature,
            m.timestamp,
            m.approval_status,
            COALESCE(n.Location, m.Location, 'Unassigned') as Location,
            COALESCE(n.Bank, m.Bank, '—') as Bank,
            COALESCE(n.Depth, m.Depth) as Depth
        FROM `{MASTER_VIEW}` m
        LEFT JOIN `{NODE_REGISTRY_TABLE}` n 
            ON m.NodeNum = n.NodeNum
            AND m.timestamp >= CAST(n.Start_Date AS TIMESTAMP)
            AND (m.timestamp <= CAST(n.End_Date AS TIMESTAMP) OR n.End_Date IS NULL)
        WHERE m.temperature >= -30.0 AND m.temperature <= 120.0
          AND (m.Project = @project_id OR m.Project LIKE '{base_job_num}%')
          AND n.Project IS NOT NULL
        ORDER BY m.timestamp ASC
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("project_id", "STRING", project_id)]
    )
    
    try:
        return client.query(query, job_config=job_config).to_dataframe()
    except Exception as e:
        st.error(f"⚠️ Data Sync Error: {e}")
        return pd.DataFrame()
    # Only iterate through nodes that survived the filter
    for sn in sorted(plot_df['NodeNum'].unique(), key=natural_sort_key): 
        node_df = plot_df[plot_df['NodeNum'] == sn]
        depth_val = node_df['Depth'].iloc[0]
        bank_val = node_df['Bank'].iloc[0]
        loc_val = node_df['Location'].iloc[0]

        if pd.notnull(bank_val) and any(x in str(bank_val).upper() for x in ['S', 'R']):
            display_name = f"{bank_val} ({sn})"
            sort_val = str(bank_val)  
        elif pd.notnull(depth_val) and not pd.isna(depth_val): 
            display_name = f"{depth_val}ft ({sn})"
            sort_val = f"depth_{float(depth_val):05.1f}" 
        else: 
            display_name = f"{loc_val} ({sn})"
            sort_val = str(display_name)

        node_metadata.append({
            'node_num': sn,
            'display_name': display_name,
            'sort_key': sort_val
        })

    # This line must be aligned with the 'for sn in plot_df...' line above
    sorted_node_configs = sorted(node_metadata, key=lambda x: natural_sort_key(x['sort_key']))

    # This for loop must also be aligned with the 'for sn...' line
    for i, config in enumerate(sorted_node_configs):
        sn = config['node_num']
        display_name = config['display_name']
        
        s_df = plot_df[plot_df['NodeNum'] == sn].sort_values('timestamp')
        s_df = s_df.set_index('timestamp').resample('1h').first().reset_index()
        
        fig.add_trace(go.Scatter(
            x=s_df['timestamp'], 
            y=s_df['temperature'],
            name=display_name, 
            mode='lines',
            connectgaps=False, 
            line=dict(shape='spline', smoothing=1.3, width=2, color=sf_15_palette[i % 15]),
            hovertemplate="<b>%{fullData.name}</b><br>Time: %{x|%H:%M}<br>Temp: %{y:.1f}" + unit_label + "<extra></extra>"
        ))

    # 5. REFERENCE LINES
    fig.add_hline(y=freeze_pt, line_width=2, line_dash="dash", line_color="RoyalBlue", annotation_text="32°F FREEZE", layer="above")
    
    now_ts = pd.Timestamp.now(tz=display_tz)
    fig.add_vline(x=now_ts.to_pydatetime(), line_width=2, line_color="red", line_dash="dash", layer='above')
    
    m_range = pd.date_range(start=final_start_view, end=final_end_view, freq='W-MON')
    for m_dt in m_range:
        fig.add_vline(x=m_dt, line_width=1.5, line_color="black", opacity=0.4)

    # 6. LAYOUT & TITLING
    p_name = st.session_state.get('selected_project', 'Project')
    
    # Implementing the inverted Y-axis preference dynamically if required by project scope
    fig.update_layout(
        title=dict(text=f"<b>{p_name} - Thermal Trend - {title}</b>", x=0.02, y=0.98, font=dict(size=18)),
        plot_bgcolor='white', 
        hovermode="x unified", 
        height=650,
        xaxis=dict(
            range=[final_start_view, final_end_view], 
            showgrid=True, gridcolor='Gainsboro',
            showline=True, mirror=True, linecolor='black', linewidth=2,
            hoverformat='%A, %b %d, %Y', 
            tickformat='%b %d',
            minor=dict(dtick=1000*60*60*24, showgrid=True, gridcolor='#f8f8f8')
        ),
        yaxis=dict(
            title=f"Temperature ({unit_label})", 
            range=y_range, 
            dtick=10,
            showgrid=True, gridcolor='Gainsboro', 
            showline=True, mirror=True, linecolor='black', linewidth=2,
            minor=dict(dtick=2, showgrid=True, gridcolor='#f8f8f8')
        ),
        legend=dict(orientation="v", x=1.02, y=1, xanchor="left", yanchor="top")
    )
    return fig
                                
def get_soil_reference_curves(soil_type, start_date, unit_mode):
    """
    Fallback function for hardcoded soil types.
    """
    references = {
        "Silty Sand": [(0, 50), (5, 32), (14, 20), (30, 10), (60, 5)],
        "Clay":       [(0, 50), (10, 32), (25, 25), (45, 15), (90, 10)]
    }
    
    curve = references.get(soil_type, [])
    if not curve:
        return None, None
        
    x_times = [pd.Timestamp(start_date) + pd.Timedelta(days=d) for d, t in curve]
    y_temps = [t if unit_mode == "Fahrenheit" else (t - 32) * 5/9 for d, t in curve]
    
    return x_times, y_temps

##################
# High temp mask #
##################
def apply_sanity_filter(df):
    """
    Automated filter for rogue data points.
    - Removes entries with null sensor names to ensure integrity.
    - Flags anything outside physical limits [-30°F, 120°F] as BADDATA.
    - Masks dynamic outliers +/- 20°F from the sensor line's average.
    """
    if df.empty:
        return df

    # Enforce strict data integrity before processing averages
    if 'NodeNum' in df.columns:
        df = df.dropna(subset=['NodeNum']).copy()

    if df.empty:
        return df

    # 1. Absolute Physical Limits -> BADDATA
    bad_condition = (df['temperature'] > 120) | (df['temperature'] < -30)
    
    # 2. Dynamic Relative Outliers -> MASKED
    if 'NodeNum' in df.columns:
        node_means = df.groupby('NodeNum')['temperature'].transform('mean')
        outlier_condition = (df['temperature'] > node_means + 20) | (df['temperature'] < node_means - 20)
    else:
        avg_temp = df['temperature'].mean()
        outlier_condition = (df['temperature'] > avg_temp + 20) | (df['temperature'] < avg_temp - 20)

    # Determine the active status column in the current view
    mask_col = 'approve' if 'approve' in df.columns else 'approval_status' if 'approval_status' in df.columns else None
    
    if mask_col:
        # Apply dynamic line outliers first
        df.loc[outlier_condition, mask_col] = 'MASKED'
        # Overwrite with absolute physical failures as a higher priority flag
        df.loc[bad_condition, mask_col] = 'BADDATA'

    return df


def render_dashboard_column(title, g_df, kpi_col, kpi_val, unit_mode, unit_label):
    """Helper layout compiler to handle repeating column metric sets."""
    st.markdown(f"**{title}**")
    if g_df.empty or g_df['latest_temp'].isnull().all():
        st.caption("No recent data")
        return
    
    latest_val = g_df['latest_temp'].mean()
    c_min, c_max = g_df['min_now'].min(), g_df['max_now'].max()
    m24, x24 = g_df['min_24h'].min(), g_df['max_24h'].max()

    def convert(v):
        if pd.isnull(v) or pd.isna(v): return None
        return (v - 32) * 5/9 if unit_mode == "Celsius" else v

    l_conv, c_min, c_max, m24, x24 = map(convert, [latest_val, c_min, c_max, m24, x24])

    st.metric("Avg (Latest)", f"{l_conv:.1f}{unit_label}")
    
    if kpi_col:
        pct = g_df[kpi_col].iloc[0]
        color = "green" if pct == 100 else "#FF8C00" if pct > 0 else "gray"
        st.markdown(f"<p style='font-size:0.85rem; color:{color};'><b>{pct:.0f}%</b> Nodes ≤ {kpi_val}°F</p>", unsafe_allow_html=True)

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

    # 4. FILTERING
    trash_locations = ['Dead Stock', 'Elizabeth', 'Office']
    p_df = p_df[~p_df['Location'].isin(trash_locations)].copy()
    
    mask_col = 'approval_status' if 'approval_status' in p_df.columns else 'approve'
    if not show_masked and mask_col in p_df.columns:
        p_df = p_df[p_df[mask_col].astype(str).str.upper() != 'MASKED'].copy()

    lookback_weeks = st.session_state.get("global_lookback_weeks_slider", 5)
    now_local = pd.Timestamp.now(tz=display_tz)
    end_view = (now_local + pd.Timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    start_view = end_view - pd.Timedelta(weeks=lookback_weeks)

    # 5. FIXED LOOP
    # 5. LOCATION-BASED PLOTTING LOOP
    # Filter trash locations before creating the list
    trash_locations = ['Dead Stock', 'Elizabeth', 'Office']
    p_df = p_df[~p_df['Location'].isin(trash_locations)].copy()
    
    locations = sorted([str(loc) for loc in p_df['Location'].dropna().unique()], key=natural_sort_key)

    for i, loc in enumerate(locations):
        with st.expander(f"📍 Location: {loc}", expanded=True):
            loc_df = p_df[p_df['Location'] == loc].copy()
            
            # Integrity guard
            if loc_df.empty or not isinstance(loc_df, pd.DataFrame):
                st.warning(f"No valid data found for {loc}.")
                continue
            
            clean_proj_id = str(selected_project).split('-')[0]
            clean_loc_num = "".join(re.findall(r'\d+', loc))
            normalized_loc = f"T{clean_loc_num}" if clean_loc_num else loc
            search_id = f"{clean_proj_id}-{normalized_loc}"
            is_temp_pipe = not any(x in loc.upper() for x in ["SUPPLY", "RETURN", "BANK S", "BANK R", "AMB"])

            # Full parameter mapping to prevent TypeError
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
            
            # Consolidated safety check
            if fig is not None and hasattr(fig, 'data') and len(fig.data) > 0:
                st.plotly_chart(
                    fig, 
                    use_container_width=True, 
                    key=f"tvt_{selected_project}_{loc}_{i}"
                )
            else:
                st.warning(f"⚠️ Could not generate graph for {loc}. Data may be missing or invalid.")
                
                

#########################
# Page 3 - Depth Charts #
#########################

def render_depth_charts(selected_project, unit_label, display_tz):
    """
    Vertical Temperature Profiles.
    Maps arrays dynamically based on native view Depth allocations.
    """
    st.header(f"📏 Depth Profile Analysis: {selected_project}")
    
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to view depth profiles.")
        return

    st.sidebar.subheader("📐 Profile Settings")
    lookback_weeks = st.sidebar.slider("Historical Snapshots (Weeks)", 1, 24, 8, key="depth_lookback")

    with st.spinner("Fetching historical telemetry..."):
        p_df = get_universal_portal_data(selected_project)

    if p_df is None or p_df.empty:
        st.warning("No data found for this project.")
        return

    # Convert native view Depth values straight into a graph-safe float coordinate
    p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
    p_df = p_df[p_df['temperature'] <= 120.0]
    
    depth_df = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
    
    if depth_df.empty:
        st.info("No sensors with valid numeric 'Depth' entries found in the data stream.")
        return

    unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
    freeze_pt = 0 if unit_mode == "Celsius" else 32
    
    now_utc = pd.Timestamp.now(tz='UTC')
    mondays = pd.date_range(end=now_utc, periods=lookback_weeks, freq='W-MON')
    locations = sorted(depth_df['Location'].unique())
    
    for loc in locations:
        with st.expander(f"📍 Temp vs Depth - {loc}", expanded=True):
            loc_data = depth_df[depth_df['Location'] == loc].copy()
            
            if loc_data['timestamp'].dt.tz is None:
                loc_data['timestamp'] = loc_data['timestamp'].dt.tz_localize('UTC')
            loc_data['timestamp_local'] = loc_data['timestamp'].dt.tz_convert(display_tz)
            
            fig = go.Figure()

            # --- A. BASELINE Snapshots ---
            baseline_ts = loc_data['timestamp_local'].min()
            b_window = loc_data[
                (loc_data['timestamp_local'] >= baseline_ts - pd.Timedelta(hours=12)) & 
                (loc_data['timestamp_local'] <= baseline_ts + pd.Timedelta(hours=12))
            ]
            
            baseline_date_str = ""
            snap_base = pd.DataFrame()
            if not b_window.empty:
                baseline_date_str = baseline_ts.strftime('%Y-%m-%d')
                snap_base = (
                    b_window.assign(diff=(b_window['timestamp_local'] - baseline_ts).abs())
                    .sort_values(['NodeNum', 'diff'])
                    .drop_duplicates('NodeNum')
                    .sort_values('Depth_Num')
                )

            # --- B. RECENT 6 AM Snapshots ---
            loc_data['date_str'] = loc_data['timestamp_local'].dt.strftime('%Y-%m-%d')
            loc_data['hour_int'] = loc_data['timestamp_local'].dt.hour
            
            recent_6am_date_str = ""
            recent_profile_rows = []
            
            if not loc_data.empty:
                sorted_all_dates = sorted(loc_data['date_str'].unique(), reverse=True)
                
                for candidate_date in sorted_all_dates:
                    if candidate_date == baseline_date_str:
                        continue
                    
                    day_pool = loc_data[loc_data['date_str'] == candidate_date]
                    if day_pool.empty:
                        continue
                        
                    recent_6am_date_str = candidate_date
                    
                    for node_id, node_group in day_pool.groupby('NodeNum'):
                        exact_6am = node_group[node_group['hour_int'] == 6]
                        if not exact_6am.empty:
                            recent_profile_rows.append(exact_6am.sort_values('timestamp_local').iloc[-1])
                        else:
                            node_group = node_group.assign(hour_dist=(node_group['hour_int'] - 6).abs())
                            best_fallback_row = node_group.sort_values(by=['hour_dist', 'timestamp_local']).iloc[0]
                            recent_profile_rows.append(best_fallback_row)
                    break

            snap_recent = pd.DataFrame(recent_profile_rows).sort_values('Depth_Num') if recent_profile_rows else pd.DataFrame()

            # --- C. HISTORICAL SNAPSHOTS ---
            for m_date in mondays:
                target_ts = m_date.replace(hour=6, minute=0, second=0)
                current_loop_date = target_ts.strftime('%Y-%m-%d')
                
                if current_loop_date == baseline_date_str or current_loop_date == recent_6am_date_str:
                    continue
                    
                window = loc_data[
                    (loc_data['timestamp_local'] >= target_ts - pd.Timedelta(hours=12)) & 
                    (loc_data['timestamp_local'] <= target_ts + pd.Timedelta(hours=12))
                ]
                
                if not window.empty:
                    snap_week = (
                        window.assign(diff=(window['timestamp_local'] - target_ts).abs())
                        .sort_values(['NodeNum', 'diff'])
                        .drop_duplicates('NodeNum')
                        .sort_values('Depth_Num')
                    )
                    
                    if snap_week.empty:
                        snap_week = (
                            window.assign(hour_dist=(window['timestamp_local'].dt.hour - 6).abs())
                            .sort_values(by=['hour_dist', 'timestamp_local'])
                            .drop_duplicates('NodeNum')
                            .sort_values('Depth_Num')
                        )
                    
                    temps = snap_week['temperature']
                    if unit_mode == "Celsius": temps = (temps - 32) * 5/9
                    
                    fig.add_trace(go.Scatter(
                        x=temps, y=snap_week['Depth_Num'], 
                        mode='lines+markers', 
                        name=current_loop_date,
                        line=dict(shape='spline', smoothing=1.1, width=1.5),
                        marker=dict(size=4),
                        hovertemplate=f"Date: {current_loop_date}<br>Depth: %{{y}}ft<br>Temp: %{{x:.1f}}{unit_label}<extra></extra>"
                    ))

            # --- D. INJECT THE MOST RECENT LINE ---
            if not snap_recent.empty:
                recent_temps = snap_recent['temperature']
                if unit_mode == "Celsius": recent_temps = (recent_temps - 32) * 5/9
                
                fig.add_trace(go.Scatter(
                    x=recent_temps, y=snap_recent['Depth_Num'],
                    mode='lines+markers',
                    name=f'<b>Most Recent ({recent_6am_date_str} 6AM*)</b>',
                    line=dict(color='#ff7f0e', width=3.5, shape='spline', smoothing=1.1),
                    marker=dict(size=6, color='#ff7f0e'),
                    hovertemplate="Most Recent: %{text}<br>Depth: %{y}ft<br>Temp: %{x:.1f}" + unit_label + "<extra></extra>",
                    text=snap_recent['timestamp_local'].dt.strftime('%b %d, %H:%M')
                ))

            # --- E. INJECT BASELINE ---
            if not snap_base.empty:
                b_temps = snap_base['temperature']
                if unit_mode == "Celsius": b_temps = (b_temps - 32) * 5/9
                
                fig.add_trace(go.Scatter(
                    x=b_temps, y=snap_base['Depth_Num'], 
                    mode='lines+markers', 
                    name=f'<b>Baseline ({baseline_date_str})</b>',
                    line=dict(color='black', width=3, dash='dash'),
                    marker=dict(size=5, color='black'),
                    hovertemplate=f"Baseline: {baseline_date_str}<br>Depth: %{{y}}ft<br>Temp: %{{x:.1f}}{unit_label}<extra></extra>"
                ))

            fig.add_vline(x=freeze_pt, line_width=2, line_dash="solid", line_color="#ADD8E6")

            max_depth = loc_data['Depth_Num'].max()
            y_limit = int(((max_depth // 10) + 1) * 10) if pd.notnull(max_depth) else 50

            fig.update_layout(
                title=f"<b>Temp vs Depth - {loc}</b>",
                plot_bgcolor='white', 
                height=800,
                xaxis=dict(
                    title=f"Temperature ({unit_label})", 
                    range=[-20, 80], dtick=10,
                    minor=dict(dtick=2, showgrid=True, gridcolor='#f8f8f8'),
                    gridcolor='Gainsboro', showline=True, linewidth=2, linecolor='black', mirror=True
                ),
                yaxis=dict(
                    title="Depth (ft)", 
                    range=[y_limit, 0], dtick=10,
                    minor=dict(dtick=2, showgrid=True, gridcolor='#f8f8f8'),
                    gridcolor='Silver', showline=True, linewidth=2, linecolor='black', mirror=True
                ),
                legend=dict(orientation="h", y=-0.1, xanchor="center", x=0.5)
            )
            
            st.plotly_chart(fig, use_container_width=True, key=f"depth_cht_{selected_project}_{loc}")
            
##############################
# Page 1 - Dashboard Summary #
##############################
def render_summary_dashboard(unit_label, unit_mode, display_tz):
    """
    The main Global Project Summary dashboard.
    - Accurately tracks active field metrics across operations.
    - Uses a robust timezone-aware sensor check-in calculation.
    - Outlier Shield: Aligned with physical boundaries up to 120°F to handle warm ambient ground zones.
    - Groups data cleanly by forcing numeric Depth tracking for TempPipes to prevent mix-ups.
    """
    st.header("🌐 Global Project Summary")
    
    client = get_bq_client()
    if client is None: return

    mobile_mode = st.session_state.get("mobile_optimized_toggle", False)

    # SQL QUERY: Optimized logic parsing active project groups while filtering stale/bad data
    summary_q = f"""
        WITH active_projects AS (
            SELECT 
                CAST(Project AS STRING) as Project, 
                ProjectName, 
                ProjectStatus, 
                Date_Freezedown,
                REGEXP_EXTRACT(TRIM(CAST(Project AS STRING)), r'^\\d+') as base_prefix
            FROM `{PROJECT_REGISTRY_TABLE}`
            WHERE UPPER(TRIM(CAST(ShowActive AS STRING))) = 'YES'
              AND UPPER(CAST(Project AS STRING)) NOT LIKE '%OFFICE%'
        ),
        raw_data AS (
            SELECT 
                p.Project, n.Bank, n.Location, n.Depth, m.temperature, m.timestamp, m.NodeNum
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
            INNER JOIN active_projects p 
                ON REGEXP_EXTRACT(TRIM(CAST(m.Project AS STRING)), r'^\\d+') = p.base_prefix
            INNER JOIN `{NODE_REGISTRY_TABLE}` n 
              ON m.NodeNum = n.NodeNum
              -- TIME-BOUND LOCK: Filter out stale sensor allocation records from project tracking
              AND m.timestamp >= CAST(n.Start_Date AS TIMESTAMP)
              AND (m.timestamp <= CAST(n.End_Date AS TIMESTAMP) OR n.End_Date IS NULL)
            WHERE m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
              AND UPPER(COALESCE(CAST(m.approval_status AS STRING), 'PENDING')) NOT IN ('BADDATA', 'FALSE', '0')
              AND NOT (m.temperature > 120.0 AND NOT STARTS_WITH(m.NodeNum, 'SP'))
        ),
        MaxTime AS (
            SELECT MAX(timestamp) as max_ts FROM raw_data
        ),
        LatestStats AS (
            SELECT 
                r.Project, r.Bank, r.Location, r.Depth, r.NodeNum,
                AVG(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR) THEN r.temperature END) as avg_now,
                AVG(CASE WHEN r.timestamp BETWEEN TIMESTAMP_SUB(m.max_ts, INTERVAL 2 HOUR) AND TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR) THEN r.temperature END) as avg_1h,
                AVG(CASE WHEN r.timestamp BETWEEN TIMESTAMP_SUB(m.max_ts, INTERVAL 7 HOUR) AND TIMESTAMP_SUB(m.max_ts, INTERVAL 6 HOUR) THEN r.temperature END) as avg_6h,
                AVG(CASE WHEN r.timestamp BETWEEN TIMESTAMP_SUB(m.max_ts, INTERVAL 25 HOUR) AND TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR) THEN r.temperature END) as avg_24h,
                MIN(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR) THEN r.temperature END) as min_now,
                MAX(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR) THEN r.temperature END) as max_now,
                MIN(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR) THEN r.temperature END) as min_24h,
                MAX(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR) THEN r.temperature END) as max_24h,
                
                COUNTIF(r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR)) as checkins_1h,
                COUNTIF(r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR)) as checkins_24h,
                
                ARRAY_AGG(r.temperature ORDER BY r.timestamp DESC LIMIT 1)[OFFSET(0)] as latest_temp,
                MAX(r.timestamp) as latest_ts
            FROM raw_data r
            CROSS JOIN MaxTime m
            GROUP BY 1, 2, 3, 4, 5
        )
        SELECT 
            p.*, ls.*,
            (COUNTIF(ls.Bank LIKE 'S%' AND ls.latest_temp <= -10) OVER(PARTITION BY p.Project) / NULLIF(COUNTIF(ls.Bank LIKE 'S%') OVER(PARTITION BY p.Project), 0)) * 100 as supply_kpi,
            (COUNTIF(ls.Bank LIKE 'R%' AND ls.latest_temp <= 0) OVER(PARTITION BY p.Project) / NULLIF(COUNTIF(ls.Bank LIKE 'R%') OVER(PARTITION BY p.Project), 0)) * 100 as return_kpi,
            (COUNTIF(ls.Depth IS NOT NULL AND ls.latest_temp <= 32) OVER(PARTITION BY p.Project) / NULLIF(COUNTIF(ls.Depth IS NOT NULL) OVER(PARTITION BY p.Project), 0)) * 100 as freeze_kpi
        FROM active_projects p
        LEFT JOIN LatestStats ls ON p.Project = ls.Project
    """
    
    try:
        df = client.query(summary_q).to_dataframe()
        df[['Bank', 'Location']] = df[['Bank', 'Location']].fillna('')
    except Exception as e:
        st.error(f"Dashboard Query Failed: {e}")
        return

    if df.empty:
        st.info("No active projects found matching your tracking parameter choices.")
        return

    for project in sorted(df['Project'].unique()):
        p_df = df[df['Project'] == project]
        p_name = p_df['ProjectName'].iloc[0] or project
        f_date = p_df['Date_Freezedown'].iloc[0]
        
        day_text, f_date_display = "", "Not Set"
        if pd.notnull(f_date):
            f_date_display = pd.to_datetime(f_date).strftime('%b %d, %Y')
            days_elapsed = (pd.Timestamp.now(tz=display_tz).date() - pd.to_datetime(f_date).date()).days
            day_text = f"🗓️ **Day {max(0, days_elapsed)}**"
        
        with st.container(border=True):
            h1, h2 = st.columns([2, 1])
            h1.subheader(f"🏗️ {p_name}")
            h2.markdown(f"<div style='text-align: right;'>{day_text}<br><small>Start: {f_date_display}</small></div>", unsafe_allow_html=True)
            
            # --- CLIENT PORTAL LINK INJECTION ---
            proj_match = re.search(r'\b(\d{4})\b', str(project))
            if proj_match:
                job_number = proj_match.group(1)
                portal_url = f"https://sf{job_number}.streamlit.app"
                st.markdown(f"🔗 **External Client Portal:** [{p_name} Portal Site Link]({portal_url})")
            
            # --- CONNECTIVITY CHECK-IN TALLIES ---
            active_1h = p_df[p_df['checkins_1h'] > 0]['NodeNum'].nunique()
            active_24h = p_df[p_df['checkins_24h'] > 0]['NodeNum'].nunique()
            total_nodes = p_df['NodeNum'].dropna().nunique()
            
            st.markdown(
                f"📡 **Hardware Status:** `{active_1h}` nodes pinged in the last hour | "
                f"`{active_24h}` nodes pinged in the last 24h (Total Pool: `{total_nodes}` registered)"
            )
            st.divider() 

            # Segregate systemic structural groups
            is_amb = p_df['Bank'].str.contains('Amb', case=False) | p_df['Location'].str.contains('Amb', case=False)
            is_tp = p_df['Depth'].notnull() & (p_df['Depth'].astype(str).str.strip() != '') & ~is_amb
            is_s = (p_df['Bank'].str.startswith('S') | p_df['Location'].str.startswith('S')) & ~is_amb & ~is_tp
            is_r = (p_df['Bank'].str.startswith('R') | p_df['Location'].str.startswith('R')) & ~is_amb & ~is_tp

            groups_data = [
                ("📥 Supply", p_df[is_s], "supply_kpi", -10), 
                ("📤 Return", p_df[is_r], "return_kpi", 0), 
                ("📏 TempPipes", p_df[is_tp], "freeze_kpi", 32), 
                ("☁️ Ambient", p_df[is_amb], None, None)
            ]

            if mobile_mode:
                for title, g_df, kpi_col, kpi_val in groups_data:
                    render_dashboard_column(title, g_df, kpi_col, kpi_val, unit_mode, unit_label)
                    st.markdown("<hr style='border: 1px dashed #ccc; margin: 15px 0;'>", unsafe_allow_html=True)
            else:
                cols = st.columns([1, 0.1, 1, 0.1, 1, 0.1, 1])
                col_mappings = [0, 2, 4, 6]
                spacer_mappings = [1, 3, 5]
                
                for s_idx in spacer_mappings:
                    cols[s_idx].markdown("<div style='border-left: 1px solid #ddd; height: 320px; margin: auto;'></div>", unsafe_allow_html=True)
                
                for idx, (title, g_df, kpi_col, kpi_val) in enumerate(groups_data):
                    with cols[col_mappings[idx]]:
                        render_dashboard_column(title, g_df, kpi_col, kpi_val, unit_mode, unit_label)

#############################
# - 2. PAGE: TIME vs TEMP - #
#############################
def render_global_overview(selected_project, project_metadata, display_tz):
    """
    Shows all pipes/banks for a selected project in one scrolling view.
    Filters out specified default and internal testing locations cleanly.
    """
    show_ref = st.session_state.get("global_show_ref", True)
    show_masked = st.session_state.get("global_show_masked", False)
    unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
    unit_label = st.session_state.get("unit_label", "°F")
    active_refs = st.session_state.get("active_refs", [])

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
    
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Select a project in the sidebar to populate runtime analytics trends.")
        return

    p_df = get_universal_portal_data(selected_project, is_summary_page=False)
    if p_df.empty:
        st.warning(f"No data found for '{p_name}'.")
        return

    mask_col = 'approval_status' if 'approval_status' in p_df.columns else 'approve'
    if not show_masked and mask_col in p_df.columns:
        p_df = p_df[p_df[mask_col].astype(str).str.upper() != 'MASKED'].copy()

    lookback_weeks = st.session_state.get("global_lookback_weeks_slider", 5)
    now_local = pd.Timestamp.now(tz=display_tz)
    end_view = (now_local + pd.Timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    start_view = end_view - pd.Timedelta(weeks=lookback_weeks)

    locations = sorted([str(loc) for loc in p_df['Location'].dropna().unique()], key=natural_sort_key)

    for i, loc in enumerate(locations):
        with st.expander(f"📍 Location: {loc}", expanded=True):
            loc_df = p_df[p_df['Location'] == loc].copy()
            
            if loc_df.empty or not isinstance(loc_df, pd.DataFrame):
                st.warning(f"No valid data found for {loc}.")
                continue
            
            clean_proj_id = str(selected_project).split('-')[0]
            clean_loc_num = "".join(re.findall(r'\d+', loc))
            normalized_loc = f"T{clean_loc_num}" if clean_loc_num else loc
            search_id = f"{clean_proj_id}-{normalized_loc}"
            is_temp_pipe = not any(x in loc.upper() for x in ["SUPPLY", "RETURN", "BANK S", "BANK R", "AMB"])

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
            
            if fig is not None and hasattr(fig, 'data') and len(fig.data) > 0:
                st.plotly_chart(fig, use_container_width=True, key=f"tvt_{selected_project}_{loc}_{i}")
            else:
                st.warning(f"⚠️ Could not generate graph for {loc}. Data may be missing or invalid.")

#########################
# Page 3 - Depth Charts #
#########################
def render_depth_charts(selected_project, unit_label, display_tz):
    """
    Vertical Temperature Profiles.
    Maps dimensional coordinates based on active node placement registries.
    """
    st.header(f"📏 Depth Profile Analysis: {selected_project}")
    
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to view depth profiles.")
        return

    st.sidebar.subheader("📐 Profile Settings")
    lookback_weeks = st.sidebar.slider("Historical Snapshots (Weeks)", 1, 24, 8, key="depth_lookback")

    with st.spinner("Fetching historical telemetry..."):
        p_df = get_universal_portal_data(selected_project, is_summary_page=False)

    if p_df is None or p_df.empty:
        st.warning("No data found for this project.")
        return

    p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
    p_df = p_df[p_df['temperature'] <= 120.0]
    depth_df = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
    
    if depth_df.empty:
        st.info("No sensors with valid numeric 'Depth' entries found in the data stream.")
        return

    unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
    freeze_pt = 0 if unit_mode == "Celsius" else 32
    
    now_utc = pd.Timestamp.now(tz='UTC')
    mondays = pd.date_range(end=now_utc, periods=lookback_weeks, freq='W-MON')
    locations = sorted(depth_df['Location'].unique())
    
    for loc in locations:
        with st.expander(f"📍 Temp vs Depth - {loc}", expanded=True):
            loc_data = depth_df[depth_df['Location'] == loc].copy()
            
            if loc_data['timestamp'].dt.tz is None:
                loc_data['timestamp'] = loc_data['timestamp'].dt.tz_localize('UTC')
            loc_data['timestamp_local'] = loc_data['timestamp'].dt.tz_convert(display_tz)
            
            fig = go.Figure()

            # --- A. BASELINE Snapshots ---
            baseline_ts = loc_data['timestamp_local'].min()
            b_window = loc_data[
                (loc_data['timestamp_local'] >= baseline_ts - pd.Timedelta(hours=12)) & 
                (loc_data['timestamp_local'] <= baseline_ts + pd.Timedelta(hours=12))
            ]
            
            baseline_date_str = ""
            snap_base = pd.DataFrame()
            if not b_window.empty:
                baseline_date_str = baseline_ts.strftime('%Y-%m-%d')
                snap_base = (
                    b_window.assign(diff=(b_window['timestamp_local'] - baseline_ts).abs())
                    .sort_values(['NodeNum', 'diff'])
                    .drop_duplicates('NodeNum')
                    .sort_values('Depth_Num')
                )

            # --- B. RECENT 6 AM Snapshots ---
            loc_data['date_str'] = loc_data['timestamp_local'].dt.strftime('%Y-%m-%d')
            loc_data['hour_int'] = loc_data['timestamp_local'].dt.hour
            
            recent_6am_date_str = ""
            recent_profile_rows = []
            
            if not loc_data.empty:
                sorted_all_dates = sorted(loc_data['date_str'].unique(), reverse=True)
                for candidate_date in sorted_all_dates:
                    if candidate_date == baseline_date_str: continue
                    day_pool = loc_data[loc_data['date_str'] == candidate_date]
                    if day_pool.empty: continue
                        
                    recent_6am_date_str = candidate_date
                    for node_id, node_group in day_pool.groupby('NodeNum'):
                        exact_6am = node_group[node_group['hour_int'] == 6]
                        if not exact_6am.empty:
                            recent_profile_rows.append(exact_6am.sort_values('timestamp_local').iloc[-1])
                        else:
                            node_group = node_group.assign(hour_dist=(node_group['hour_int'] - 6).abs())
                            best_fallback_row = node_group.sort_values(by=['hour_dist', 'timestamp_local']).iloc[0]
                            recent_profile_rows.append(best_fallback_row)
                    break

            snap_recent = pd.DataFrame(recent_profile_rows).sort_values('Depth_Num') if recent_profile_rows else pd.DataFrame()

            # --- C. HISTORICAL SNAPSHOTS ---
            for m_date in mondays:
                target_ts = m_date.replace(hour=6, minute=0, second=0)
                current_loop_date = target_ts.strftime('%Y-%m-%d')
                
                if current_loop_date == baseline_date_str or current_loop_date == recent_6am_date_str:
                    continue
                    
                window = loc_data[
                    (loc_data['timestamp_local'] >= target_ts - pd.Timedelta(hours=12)) & 
                    (loc_data['timestamp_local'] <= target_ts + pd.Timedelta(hours=12))
                ]
                
                if not window.empty:
                    snap_week = (
                        window.assign(diff=(window['timestamp_local'] - target_ts).abs())
                        .sort_values(['NodeNum', 'diff'])
                        .drop_duplicates('NodeNum')
                        .sort_values('Depth_Num')
                    )
                    if snap_week.empty:
                        snap_week = (
                            window.assign(hour_dist=(window['timestamp_local'].dt.hour - 6).abs())
                            .sort_values(by=['hour_dist', 'timestamp_local'])
                            .drop_duplicates('NodeNum')
                            .sort_values('Depth_Num')
                        )
                    
                    temps = snap_week['temperature']
                    if unit_mode == "Celsius": temps = (temps - 32) * 5/9
                    
                    fig.add_trace(go.Scatter(
                        x=temps, y=snap_week['Depth_Num'], 
                        mode='lines+markers', name=current_loop_date,
                        line=dict(shape='spline', smoothing=1.1, width=1.5),
                        marker=dict(size=4),
                        hovertemplate=f"Date: {current_loop_date}<br>Depth: %{{y}}ft<br>Temp: %{{x:.1f}}{unit_label}<extra></extra>"
                    ))

            # --- D. INJECT THE MOST RECENT LINE ---
            if not snap_recent.empty:
                recent_temps = snap_recent['temperature']
                if unit_mode == "Celsius": recent_temps = (recent_temps - 32) * 5/9
                
                fig.add_trace(go.Scatter(
                    x=recent_temps, y=snap_recent['Depth_Num'], mode='lines+markers',
                    name=f'<b>Most Recent ({recent_6am_date_str} 6AM*)</b>',
                    line=dict(color='#ff7f0e', width=3.5, shape='spline', smoothing=1.1),
                    marker=dict(size=6, color='#ff7f0e'),
                    hovertemplate="Most Recent: %{text}<br>Depth: %{y}ft<br>Temp: %{x:.1f}" + unit_label + "<extra></extra>",
                    text=snap_recent['timestamp_local'].dt.strftime('%b %d, %H:%M')
                ))

            # --- E. INJECT BASELINE ---
            if not snap_base.empty:
                b_temps = snap_base['temperature']
                if unit_mode == "Celsius": b_temps = (b_temps - 32) * 5/9
                
                fig.add_trace(go.Scatter(
                    x=b_temps, y=snap_base['Depth_Num'], mode='lines+markers', 
                    name=f'<b>Baseline ({baseline_date_str})</b>',
                    line=dict(color='black', width=3, dash='dash'),
                    marker=dict(size=5, color='black'),
                    hovertemplate=f"Baseline: {baseline_date_str}<br>Depth: %{{y}}ft<br>Temp: %{{x:.1f}}{unit_label}<extra></extra>"
                ))

            fig.add_vline(x=freeze_pt, line_width=2, line_dash="solid", line_color="#ADD8E6")
            max_depth = loc_data['Depth_Num'].max()
            y_limit = int(((max_depth // 10) + 1) * 10) if pd.notnull(max_depth) else 50

            fig.update_layout(
                title=f"<b>Temp vs Depth - {loc}</b>", plot_bgcolor='white', height=800,
                xaxis=dict(title=f"Temperature ({unit_label})", range=[-20, 80], dtick=10, minor=dict(dtick=2, showgrid=True, gridcolor='#f8f8f8'), gridcolor='Gainsboro', showline=True, linewidth=2, linecolor='black', mirror=True),
                yaxis=dict(title="Depth (ft)", range=[y_limit, 0], dtick=10, minor=dict(dtick=2, showgrid=True, gridcolor='#f8f8f8'), gridcolor='Silver', showline=True, linewidth=2, linecolor='black', mirror=True),
                legend=dict(orientation="h", y=-0.1, xanchor="center", x=0.5)
            )
            st.plotly_chart(fig, use_container_width=True, key=f"depth_cht_{selected_project}_{loc}")

###########################
# PAGE 4: SENSOR STATUS - #
###########################
def render_sensor_status(client, selected_project, unit_label, unit_mode, display_tz):
    """
    Page Name: Sensor Status
    Strictly isolated tracking logic parsing operational statuses and health diagnostics.
    """
    p_meta = st.session_state.get('project_metadata')
    if not p_meta or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to view sensor health records.")
        return

    p_name = p_meta.get('ProjectName', selected_project)
    f_date = p_meta.get('Date_Freezedown')
    st.title(f"❄️ {p_name}")
    
    if pd.notnull(f_date):
        days = (pd.Timestamp.now(tz=display_tz).date() - pd.to_datetime(f_date).date()).days
        st.markdown(f"## 🗓️ Day **{max(0, days)}** of Freezedown")
    st.divider()

    # TIME-BOUND LOCK: Filter out stale/decommissioned elements by joining telemetry to active range constraints
    query = f"""
        WITH BaseReporting AS (
            SELECT m.NodeNum, m.timestamp, m.temperature, COALESCE(n.Location, m.Location) as Location, COALESCE(n.Bank, m.Bank) as Bank, COALESCE(n.Depth, m.Depth) as Depth
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
            INNER JOIN `{NODE_REGISTRY_TABLE}` n 
              ON m.NodeNum = n.NodeNum 
              AND m.timestamp >= CAST(n.Start_Date AS TIMESTAMP)
              AND (m.timestamp <= CAST(n.End_Date AS TIMESTAMP) OR n.End_Date IS NULL)
            WHERE m.Project = @proj_id 
              AND m.NodeNum IS NOT NULL
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
                
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN 1 ELSE 0 END) as seen_1h_f,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR) THEN 1 ELSE 0 END) as seen_6h_f,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN 1 ELSE 0 END) as seen_24h_f,

                (COUNT(DISTINCT CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN TIMESTAMP_TRUNC(timestamp, HOUR) END) / 24.0) * 100 as coverage_24h,
                (COUNT(DISTINCT CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 168 HOUR) THEN TIMESTAMP_TRUNC(timestamp, HOUR) END) / 168.0) * 100 as coverage_7d,

                MIN(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature END) AS low_24h,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature END) AS high_24h,
                MAX(TIMESTAMP_DIFF(timestamp, prev_ts, HOUR)) AS max_gap_7d
            FROM GapAnalysis 
            GROUP BY NodeNum, Location, Bank, Depth
        )
        SELECT * FROM HistoricalStats
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)]
    )

    try:
        df = client.query(query, job_config=job_config).to_dataframe()
        if df.empty:
            st.warning("No active tracking rows found for this project layout specification.")
            return

        now_local = pd.Timestamp.now(tz=display_tz)
        def get_lag(ts):
            if pd.isnull(ts): return 999.0
            ts_aware = ts if ts.tzinfo else ts.tz_localize('UTC')
            return (now_local - ts_aware.tz_convert(display_tz)).total_seconds() / 3600

        df['last_seen_hrs'] = df['last_ping'].apply(get_lag)

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

        st.subheader("📍 Location Performance Summary")
        summary_rows = []
        for loc, loc_group in df.groupby('Location'):
            min_hours_lag = loc_group['last_seen_hrs'].min()
            max_hours_lag = loc_group['last_seen_hrs'].max()
            
            summary_rows.append({
                'Location': loc, 'Total Nodes': int(len(loc_group)),
                'Seen 1h': int(loc_group['seen_1h_f'].sum()), 'Seen 6h': int(loc_group['seen_6h_f'].sum()), 'Seen 24h': int(loc_group['seen_24h_f'].sum()),
                '24h Coverage': f"{loc_group['coverage_24h'].mean():.1f}%", '7d Coverage': f"{loc_group['coverage_7d'].mean():.1f}%",
                'Avg Temp': fmt_t(loc_group['current_temp'].mean()), 'Low 24h': fmt_t(loc_group['low_24h'].min()), 'High 24h': fmt_t(loc_group['high_24h'].max()),
                'Best Seen': get_status_icon(min_hours_lag), 'Worst Seen': get_status_icon(max_hours_lag)
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
                    if missing == 0: bg_style = "background-color: #d1fae5; color: #065f46; font-weight: bold;"
                    elif 1 <= missing <= 3: bg_style = "background-color: #bbf7d0; color: #14532d; font-weight: bold;"
                    elif 4 <= missing <= 6: bg_style = "background-color: #fef08a; color: #713f12; font-weight: bold;"
                    elif 7 <= missing <= 10: bg_style = "background-color: #fed7aa; color: #7c2d12; font-weight: bold;"
                    else: bg_style = "background-color: #fca5a5; color: #7f1d1d; font-weight: bold;"
                    canvas.loc[idx, col] = bg_style
            return canvas

        st.dataframe(summary_df.style.apply(style_missing_counters, axis=None), use_container_width=True, hide_index=True)

        st.divider()
        st.subheader("🔍 Detailed Sensor Audit")
        selected_loc = st.selectbox("Filter Audit by Location:", ["--- All ---"] + sorted(df['Location'].unique()))
        audit_df = df.copy() if selected_loc == "--- All ---" else df[df['Location'] == selected_loc]
        
        rows = []
        for _, r in audit_df.sort_values(['Location', 'Depth', 'Bank']).iterrows():
            rows.append({
                "Node": r['NodeNum'], "Location": r['Location'],
                "Position": f"{r['Depth']}ft" if pd.notnull(r['Depth']) else f"Bank {r['Bank']}",
                "Last Seen": get_status_icon(r['last_seen_hrs']), "24 hour coverage": f"{r['coverage_24h']:.1f}%",
                "Current Temp": fmt_t(r['current_temp']), "Change for 1 hr": get_arrow(r['current_temp'], r['avg_1h']),
                "Change for 24 hr": get_arrow(r['current_temp'], r['avg_24h']), "24 hr high": fmt_t(r['high_24h']), "24 hour low": fmt_t(r['low_24h'])
            })
        st.dataframe(rows, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Sensor Status Error: {e}")

###########
# - 5. PAGE: NODE DIAGNOSTICS - #
###########
def render_node_diagnostics(selected_project, display_tz, unit_label):
    """
    Page Name: Node Diagnostics
    Live signal auditing interface tracking packet logging frequencies and telemetries.
    """
    st.header("📡 Commissioning & Diagnostics Audit")
    client = get_bq_client()
    if client is None: return

    diag_q = f"""
        WITH Stats AS (
            SELECT 
                NodeNum,
                MAX(timestamp) as last_ping,
                ARRAY_AGG(temperature ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] as last_temp,
                COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) as count_1h,
                COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)) as count_6h,
                COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) as count_24h,
                ARRAY_AGG(rssi ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] as rssi_last_val,
                AVG(rssi) as rssi_avg_val
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view`
            GROUP BY NodeNum
        )
        SELECT 
            n.Project, n.Location, n.NodeNum, n.Bank, n.Depth, n.SensorStatus, 
            s.last_ping, s.last_temp,
            COALESCE(s.count_1h, 0) as count_1h, COALESCE(s.count_6h, 0) as count_6h, COALESCE(s.count_24h, 0) as count_24h,
            s.rssi_last_val as rssi_last, s.rssi_avg_val as rssi_avg
        FROM `{NODE_REGISTRY_TABLE}` n
        LEFT JOIN Stats s ON n.NodeNum = s.NodeNum
        WHERE n.End_Date IS NULL OR TRIM(CAST(n.End_Date AS STRING)) = ''
    """
    try:
        df = client.query(diag_q).to_dataframe()
        if df.empty:
            st.warning("No nodes found in system registry.")
            return

        now_utc = pd.Timestamp.now(tz='UTC')
        st.markdown("### 🔍 Filter Fleet Scope")
        f_col1, f_col2, f_col3 = st.columns(3)
        
        with f_col1:
            proj_opts = ["--- All Projects ---"] + sorted(df['Project'].dropna().unique().tolist())
            init_proj_idx = proj_opts.index(selected_project) if selected_project in proj_opts else 0
            filter_proj = st.selectbox("Scope Project Context:", proj_opts, index=init_proj_idx)
        with f_col2:
            sub_df = df.copy() if filter_proj == "--- All Projects ---" else df[df['Project'] == filter_proj]
            filter_loc = st.selectbox("Scope Physical Location:", ["--- All Locations ---"] + sorted(sub_df['Location'].dropna().unique().tolist()))
        with f_col3:
            filter_stat = st.selectbox("Scope Hardware Status:", ["--- All Statuses ---"] + sorted(sub_df['SensorStatus'].dropna().unique().tolist()))

        if filter_proj != "--- All Projects ---": df = df[df['Project'] == filter_proj]
        if filter_loc != "--- All Locations ---": df = df[df['Location'] == filter_loc]
        if filter_stat != "--- All Statuses ---": df = df[df['SensorStatus'] == filter_stat]

        if df.empty:
            st.info("No matching hardware rows found.")
            return

        def process_latency_metrics(row):
            ping = row['last_ping']
            if pd.isnull(ping): return pd.Series(["❌ Never", "background-color: #d1d5db; color: #1f2937;", float('inf')])
            ts = ping if ping.tzinfo else ping.tz_localize('UTC')
            hours_hidden = (now_utc - ts).total_seconds() / 3600.0
            txt = f"{hours_hidden:.1f}h"
            style = assign_row_color(hours_hidden)
            return pd.Series([txt, style, hours_hidden])

        df[['Seen_Text', 'Seen_Style', 'hours_hidden']] = df.apply(process_latency_metrics, axis=1)
        df['hours_hidden'] = pd.to_numeric(df['hours_hidden'], errors='coerce').fillna(float('inf'))
        df = df.sort_values(by='hours_hidden', ascending=True).reset_index(drop=True)

        df['Compact_Loc'] = df['Location'].apply(lambda x: str(x).strip()[:5] if len(str(x).strip()) > 5 else str(x).strip())
        df['Clean_Pos'] = df.apply(lambda r: f"{r['Depth']}ft" if (pd.notnull(r.get('Depth')) and r.get('Depth') != 0) else re.sub(r'(?i)bank\s*', '', str(r['Bank'])).strip() if pd.notnull(r.get('Bank')) else "-", axis=1)
        
        unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
        df['Formatted_Temp'] = df['last_temp'].apply(lambda x: fmt_temp(x, unit_mode, unit_label))
        df['efficiency_pct'] = ((df['count_24h'] / 96.0) * 100.0).clip(upper=100.0)

        display_df = pd.DataFrame({
            "Node ID": df['NodeNum'], "Location": df['Compact_Loc'], "Position": df['Clean_Pos'],
            "Current Temp": df['Formatted_Temp'], "Last Seen": df['Seen_Text'],
            "Pings (1h)": df['count_1h'].astype(int), "Pings (6h)": df['count_6h'].astype(int), "Pings (24h)": df['count_24h'].astype(int),
            "RSSI Last": df['rssi_last'].apply(lambda x: f"{int(x)} dBm" if pd.notnull(x) and not pd.isna(x) else "N/A"),
            "RSSI Avg": df['rssi_avg'].apply(lambda x: f"{int(x)} dBm" if pd.notnull(x) and not pd.isna(x) else "N/A"),
            "Reporting Efficiency": df['efficiency_pct']
        })

        st.dataframe(
            display_df.style.apply(lambda d: pd.DataFrame(df['Seen_Style'].values, index=d.index, columns=['Last Seen']).reindex(columns=d.columns, fill_value=''), axis=None),
            use_container_width=True, hide_index=True,
            column_config={"Reporting Efficiency": st.column_config.ProgressColumn("Reporting Efficiency", format="%.0f%%", min_value=0, max_value=100)}
        )
    except Exception as e:
        st.error(f"Diagnostics Audit Failed: {e}")

# =============================================================================
# Page: Data Processing
# =============================================================================
def render_data_processing_page(selected_project):
    """
    Page Name: Data Processing
    Handles manual file ingestion, raw limits check filters, and wide engineering spreadsheet exports.
    """
    st.header("⚙️ Data Processing & Reference Engine")
    client = get_bq_client()
    if client is None: return
        
    tab_upload, tab_export, tab_ref_library = st.tabs(["📄 Upload Telemetry", "📥 Export Report", "📈 Ref Curve Library"])
    
    with tab_upload:
        st.subheader("📄 Manual File Ingestion")
        u_files = st.file_uploader("Select CSV or Excel files", type=['csv', 'xlsx'], key="manual_upload_main", accept_multiple_files=True) 
    
        if u_files:
            all_processed_dfs = []
            target_table = None
            for f in u_files:
                try:
                    is_sensorconnect, skip_rows = False, 0
                    if f.name.endswith('.csv'):
                        f.seek(0)
                        for i, line in enumerate(f):
                            if b"DATA_START" in line:
                                is_sensorconnect, skip_rows = True, i + 1
                                break
                        f.seek(0)
                    
                    df_raw = pd.read_csv(f, encoding='latin1', skiprows=skip_rows, dtype=str) if f.name.endswith('.csv') else pd.read_excel(f, dtype=str)
                    if not df_raw.empty:
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
                        else:
                            t_match = next((h for h in actual_headers if 'timestamp' in h.lower()), None)
                            v_match = next((h for h in actual_headers if 'temp' in h.lower()), None)
                            if t_match and v_match:
                                clean_name = f.name.replace(".csv", "").replace(".xlsx", "")
                                match = re.search(r'^([^ \(\)]+)', clean_name)
                                df_processed['timestamp'] = pd.to_datetime(df_raw[t_match], errors='coerce', utc=True)
                                df_processed['temperature'] = pd.to_numeric(df_raw[v_match], errors='coerce')
                                df_processed['NodeNum'] = match.group(1).strip() if match else clean_name
                                target_table = "raw_sensorpush"
                        
                        if not df_processed.empty:
                            df_processed = df_processed.dropna(subset=['timestamp', 'temperature'])
                            all_processed_dfs.append(df_processed)
                            st.write(f"✅ Prepared {f.name}: {len(df_processed)} records.")
                except Exception as e:
                    st.error(f"❌ Error processing {f.name}: {e}")

            if all_processed_dfs and target_table:
                combined_df = pd.concat(all_processed_dfs, ignore_index=True)
                combined_df['temperature'] = combined_df['temperature'].round(1)
                if st.button(f"🚀 Commit {len(combined_df)} records to {target_table}"):
                    with st.spinner("Writing to BigQuery..."):
                        table_id = f"{PROJECT_ID}.{DATASET_ID}.{target_table}"
                        job_config = bigquery.LoadJobConfig(
                            schema=[bigquery.SchemaField("timestamp", "TIMESTAMP"), bigquery.SchemaField("NodeNum", "STRING"), bigquery.SchemaField("temperature", "FLOAT")],
                            write_disposition="WRITE_APPEND"
                        )
                        client.load_table_from_dataframe(combined_df[['timestamp', 'NodeNum', 'temperature']], table_id, job_config=job_config).result()
                        st.success("Batch Upload Complete!")
                        st.cache_data.clear()

    with tab_export:
        st.subheader("📥 Wide-Format Data Export")
        if not selected_project or selected_project == "All Projects":
            st.warning("⚠️ Select a specific project in the sidebar to export data.")
        else:
            c1, c2 = st.columns(2)
            e_start = c1.date_input("Start Date", value=datetime.now() - timedelta(days=30))
            e_end = c2.date_input("End Date", value=datetime.now())
            with st.spinner("Processing records..."):
                full_df = get_universal_portal_data(selected_project)
            if not full_df.empty:
                all_locs = sorted(full_df['Location'].unique().tolist())
                selected_locs = st.multiselect("Filter by Location (Leave empty for ALL)", options=all_locs)
                mask = (full_df['timestamp'].dt.date >= e_start) & (full_df['timestamp'].dt.date <= e_end)
                if selected_locs: mask = mask & (full_df['Location'].isin(selected_locs))
                export_df = full_df.loc[mask].copy()
                if export_df.empty: st.warning("No data found for the selected criteria.")
                else:
                    export_df['Sensor'] = export_df['Location'] + " (" + export_df['NodeNum'].astype(str) + ")"
                    wide_df = export_df.pivot_table(index='timestamp', columns='Sensor', values='temperature', aggfunc='first').reset_index()
                    wide_df['timestamp'] = wide_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    st.success(f"Report Ready: {len(wide_df.columns)-1} columns generated.")
                    st.download_button(label="💾 Download Custom CSV Export", data=wide_df.to_csv(index=False).encode('utf-8'), file_name=f"{selected_project}_Export_{datetime.now().strftime('%Y%m%d')}.csv", mime="text/csv", use_container_width=True)

    with tab_ref_library:
        st.subheader("📚 Theoretical Curve Library")
        with st.expander("🗑️ Library Management (Delete/Purge)", expanded=False):
            try:
                lib_df = client.query(f"SELECT DISTINCT CurveID FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves`").to_dataframe()
                if not lib_df.empty:
                    to_delete = st.selectbox("Select Curve to Remove", sorted(lib_df['CurveID'].tolist()), key="delete_curve_picker")
                    if st.button(f"🗑️ Delete {to_delete}", type="secondary"):
                        client.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` WHERE CurveID='{to_delete}'").result()
                        st.success(f"Removed {to_delete} from library.")
                        st.cache_data.clear()
                        time.sleep(0.5); st.rerun()
                else: st.info("No curves available to delete.")
            except Exception: st.info("Reference table is empty or initializing.")

            st.divider()
            confirm_purge = st.checkbox("I confirm I want to DELETE ALL curves in the library.", key="confirm_purge_check")
            if st.button("🧨 PURGE ENTIRE LIBRARY", type="primary", disabled=not confirm_purge):
                try:
                    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.reference_curves`").result()
                    st.success("Library completely purged.")
                    st.cache_data.clear()
                    time.sleep(1); st.rerun()
                except Exception as e: st.error(f"Purge failed: {e}")

        st.divider(); st.write("### 📤 Upload New Curves")
        u_files = st.file_uploader("Select CSV Files", type="csv", accept_multiple_files=True, key="ref_uploader_v6")
        if u_files:
            if st.button("💾 Commit Files to BigQuery", key="commit_ref_btn_final", use_container_width=True):
                progress_bar = st.progress(0)
                table_ref = f"{PROJECT_ID}.{DATASET_ID}.reference_curves"
                for idx, f in enumerate(u_files):
                    try:
                        curve_id = f.name.replace(".csv", "")
                        f.seek(0)
                        try: ref_df = pd.read_csv(f, skiprows=2, names=['Day', 'Temp'], encoding='utf-8')
                        except UnicodeDecodeError: f.seek(0); ref_df = pd.read_csv(f, skiprows=2, names=['Day', 'Temp'], encoding='latin-1')
                        
                        ref_df['Day'] = pd.to_numeric(ref_df['Day'], errors='coerce')
                        ref_df['Temp'] = pd.to_numeric(ref_df['Temp'], errors='coerce')
                        ref_df = ref_df.dropna(subset=['Day', 'Temp'])
                        if ref_df.empty: continue
                        ref_df['CurveID'] = curve_id
                        client.query(f"DELETE FROM `{table_ref}` WHERE CurveID='{curve_id}'").result()
                        job_config = bigquery.LoadJobConfig(
                            schema=[bigquery.SchemaField("Day", "INTEGER"), bigquery.SchemaField("Temp", "FLOAT"), bigquery.SchemaField("CurveID", "STRING")],
                            write_disposition="WRITE_APPEND"
                        )
                        client.load_table_from_dataframe(ref_df, table_ref, job_config=job_config).result()
                        st.toast(f"Success: {curve_id}", icon="✅")
                    except Exception as e: st.error(f"❌ Error processing {f.name}: {e}")
                    progress_bar.progress((idx + 1) / len(u_files))
                st.success("Library Processing Complete."); st.cache_data.clear()
                time.sleep(1); st.rerun()

        st.divider(); st.write("### 📂 Current Library Inventory")
        try:
            inventory_df = client.query(f"SELECT CurveID, COUNT(*) as Data_Points, MIN(Day) as Start_Day, MAX(Day) as End_Day FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` GROUP BY CurveID ORDER BY CurveID").to_dataframe()
            if not inventory_df.empty: st.dataframe(inventory_df, use_container_width=True, hide_index=True)
            else: st.info("The library table is currently empty.")
        except Exception: st.warning("Reference table not located in database.")

# =============================================================================
# Page: Admin Tools
# =============================================================================
def render_admin_page(selected_project, display_tz, unit_mode, unit_label, active_refs):
    """Central administrative command room architecture from Sandbox code base."""
    st.header("🛠️ Admin Tools")
    client = get_bq_client()
    if client is None: return

    try:
        proj_q = f"SELECT CAST(Project AS STRING) as Project, ProjectName, Timezone, ProjectStatus, Date_Freezedown FROM `{PROJECT_REGISTRY_TABLE}` WHERE UPPER(TRIM(CAST(ShowActive AS STRING))) IN ('TRUE', 'YES', '1')"
        full_reg_df = client.query(f"SELECT * FROM `{NODE_REGISTRY_TABLE}` WHERE End_Date IS NULL OR TRIM(CAST(End_Date AS STRING)) = ''").to_dataframe()
        available_projects_list = sorted(client.query(proj_q).to_dataframe()['Project'].dropna().unique().tolist())
    except Exception as e: st.error(f"Registry Link Offline: {e}"); return

    tab_admin_sum, tab_bulk_app, tab_recovery, tab_proj_master = st.tabs(["📋 Admin Summary", "⚡ Bulk Approval", "📡 Data Recovery", "⚙️ Project Master"])
    
    with tab_admin_sum:
        st.subheader("📋 Centralized Infrastructure Status Overview")
        st.markdown("### 📡 Hardware Inventory Fleet Breakdown")
        try:
            def classify_family(node): return "Lord" if "-ch" in str(node).lower() else "SP" if str(node).lower().startswith("sp") else "TP" if str(node).lower().startswith("tp") else "Other"
            fleet_df = full_reg_df.copy()
            fleet_df['Hardware Family'] = fleet_df['NodeNum'].apply(classify_family)
            fleet_df['Parent ID'] = fleet_df['NodeNum'].apply(lambda x: re.split(r'(?i)-ch', str(x))[0] if "-ch" in str(x).lower() else x)
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
            sum_q = f"SELECT p.Project, p.ProjectName, p.ProjectStatus, p.Date_Freezedown, COUNT(DISTINCT n.NodeNum) as Mapped_Sensors, COUNT(DISTINCT CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR) THEN n.NodeNum END) as Active_6h, COUNT(DISTINCT CASE WHEN m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN n.NodeNum END) as Active_24h FROM `{PROJECT_REGISTRY_TABLE}` p LEFT JOIN `{NODE_REGISTRY_TABLE}` n ON p.Project = n.Project LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.master_data_view` m ON n.NodeNum = m.NodeNum WHERE (n.End_Date IS NULL OR TRIM(CAST(n.End_Date AS STRING)) = '') AND UPPER(TRIM(CAST(p.ShowActive AS STRING))) IN ('TRUE', 'YES', '1') AND UPPER(p.Project) NOT LIKE '%OFFICE%' GROUP BY 1,2,3,4 ORDER BY p.Project ASC"
            rows = []
            for _, r in client.query(sum_q).to_dataframe().iterrows():
                elapsed = max(0, (pd.Timestamp.now(tz=display_tz).date() - pd.to_datetime(r['Date_Freezedown']).date()).days) if pd.notnull(r['Date_Freezedown']) else 0
                rows.append({"Project ID": r['Project'], "Project Name": r['ProjectName'] or r['Project'], "Mapped Sensors": int(r['Mapped_Sensors']), "Active (6h)": int(r['Active_6h']), "Active (24h)": int(r['Active_24h']), "Project Status Timeline": f"Day {elapsed} of {str(r['ProjectStatus']).title()}" if pd.notnull(r['Date_Freezedown']) else "Not Freezing"})
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        except Exception as e: st.error(f"Overview compilation fault: {e}")

    with tab_bulk_app:
        execute_bulk_approval_workspace(client, full_reg_df, selected_project)
        
    with tab_recovery:
        st.title("📡 Data Recovery Engine")
        dropdown_selected_nodes = render_recovery_filters(full_reg_df)
        st.divider(); st.subheader("📅 Define Recovery Timeline Parameters")
        rec_c1, rec_c2 = st.columns(2)
        rec_start_date = rec_c1.date_input("Extraction Window Start Date", value=datetime.now().date() - timedelta(days=2), key="dt_rec_start")
        rec_end_date = rec_c2.date_input("Extraction Window End Date", value=datetime.now().date(), key="dt_rec_end")

        if dropdown_selected_nodes: final_target_nodes = dropdown_selected_nodes
        else:
            active_proj_context = st.session_state.get('rec_proj_sel_isolated', 'All')
            active_loc_context = st.session_state.get('rec_loc_sel_isolated', 'All')
            slice_df = full_reg_df.copy()
            if active_proj_context != "All": slice_df = slice_df[slice_df['Project'] == active_proj_context]
            if active_loc_context != "All": slice_df = slice_df[slice_df['Location'] == active_loc_context]
            final_target_nodes = sorted(slice_df['NodeNum'].dropna().unique().tolist())

        scope_text = f"{len(final_target_nodes)} selected nodes" if final_target_nodes else "ALL registered fleet nodes"
        st.warning(f"⚠️ **Action Required:** Initiating backfill protocol for {scope_text} from {rec_start_date} through {rec_end_date}.")

        if 'recovery_run_complete' not in st.session_state: st.session_state['recovery_run_complete'] = False
        if 'recovery_cached_rows' not in st.session_state: st.session_state['recovery_cached_rows'] = []
        if 'recovery_cached_stats' not in st.session_state: st.session_state['recovery_cached_stats'] = {}

        if st.button("🚀 Execute Cloud Backfill Ingestion Pipeline Run", use_container_width=True):
            all_rows, hardware_map, reverse_hardware_map, db_max_timestamps = [], {}, {}, {}
            LOCAL_REC_TABLE, LOCAL_INV_TABLE, LOCAL_API_URL = "raw_sensorpush", "hardware_inventory", "https://api.sensorpush.com/api/v1"
            ACCOUNTS = [{'email': 'ldunham@soilfreeze.com', 'password': 'Freeze123!!'}, {'email': 'tsteele@soilfreeze.com', 'password': 'Freeze123!!'}, {'email': 'soilfreeze98072@gmail.com', 'password': 'Freeze123!!'}]
            start_time_iso = datetime.combine(rec_start_date, datetime.min.time()).strftime('%Y-%m-%dT%H:%M:%SZ')
            end_time_iso = datetime.combine(rec_end_date, datetime.max.time()).strftime('%Y-%m-%dT%H:%M:%SZ')

            with st.status("Executing Cloud Backfill Run...", expanded=True) as status_box:
                try:
                    for row in client.query(f"SELECT RawID, NodeNum FROM `{PROJECT_ID}.{DATASET_ID}.{LOCAL_INV_TABLE}` WHERE RawID IS NOT NULL"):
                        hardware_map[str(row.RawID).split('.')[0].strip()] = str(row.NodeNum).strip()
                        reverse_hardware_map[str(row.NodeNum).strip()] = str(row.RawID).split('.')[0].strip()
                    for row in client.query(f"SELECT NodeNum, FORMAT_TIMESTAMP('%m/%d/%Y %H:%M UTC', MAX(timestamp)) as max_time FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` GROUP BY NodeNum"):
                        if row.max_time: db_max_timestamps[str(row.NodeNum)] = str(row.max_time)
                except Exception as e: st.error(f"Mapping fetch error: {e}"); st.stop()

                for acc in ACCOUNTS:
                    try:
                        auth_r = requests.post(f"{LOCAL_API_URL}/oauth/authorize", json=acc, timeout=15).json()
                        token = requests.post(f"{LOCAL_API_URL}/oauth/accesstoken", json={"authorization": auth_r['authorization']}, timeout=15).json().get('accesstoken')
                        s_resp = requests.post(f"{LOCAL_API_URL}/devices/sensors", headers={"Authorization": token}, json={}, timeout=20).json()
                        device_rssi_map = {str(s_id).strip(): s_meta.get('rssi') for s_id, s_meta in s_resp.items() if isinstance(s_meta, dict) and 'rssi' in s_meta} if isinstance(s_resp, dict) else {}
                        
                        r_samples = requests.post(f"{LOCAL_API_URL}/samples", headers={"Authorization": token}, json={"startTime": start_time_iso, "endTime": end_time_iso, "limit": 100000}, timeout=60).json()
                        for s_id, samples in r_samples.get('sensors', {}).items():
                            api_root_id = str(s_id).split('.')[0].strip()
                            friendly_name = hardware_map.get(api_root_id)
                            if not (friendly_name and friendly_name in final_target_nodes): continue
                            current_rssi = device_rssi_map.get(str(s_id).strip())
                            
                            for s in samples:
                                temp = s.get('temp_f') or s.get('temperature') or s.get('thermocouple_temperature')
                                if temp is not None:
                                    all_rows.append({"timestamp": pd.to_datetime(s['observed']), "NodeNum": str(friendly_name), "temperature": float(temp), "rssi": float(current_rssi) if current_rssi is not None else None})
                    except Exception: continue

                if not all_rows:
                    status_box.update(label="Run Finalized (0 Points Found)", state="complete")
                else:
                    try:
                        upload_df = pd.DataFrame(all_rows)
                        upload_df['timestamp'] = pd.to_datetime(upload_df['timestamp'], utc=True)
                        client.load_table_from_dataframe(upload_df, f"{PROJECT_ID}.{DATASET_ID}.{LOCAL_REC_TABLE}", job_config=bigquery.LoadJobConfig(schema=[bigquery.SchemaField("timestamp", "TIMESTAMP"), bigquery.SchemaField("NodeNum", "STRING"), bigquery.SchemaField("temperature", "FLOAT"), bigquery.SchemaField("rssi", "FLOAT")], write_disposition="WRITE_APPEND")).result()
                        st.session_state['recovery_cached_rows'] = all_rows
                        st.session_state['recovery_cached_stats'] = db_max_timestamps
                        st.session_state['recovery_run_complete'] = True
                        status_box.update(label="Recovery Dump Complete!", state="complete")
                        st.cache_data.clear(); st.rerun()
                    except Exception as bq_err: st.error(f"Ingestion pipeline failure: {bq_err}")

        if st.session_state.get('recovery_run_complete'):
            st.write("### 📊 Data Recovery Tally Distribution:")
            summary_records = []
            grand_total_tally = 0
            for node in final_target_nodes:
                true_node_count = sum(1 for row in st.session_state['recovery_cached_rows'] if row["NodeNum"] == node)
                grand_total_tally += true_node_count
                summary_records.append({"Node Number": node, "Last Database Check-In": st.session_state['recovery_cached_stats'].get(node, "❌ No History Found"), "Points Extracted & Appended": true_node_count})
            st.dataframe(pd.concat([pd.DataFrame(summary_records), pd.DataFrame([{"Node Number": "🧮 Combined Total Pool", "Last Database Check-In": "—", "Points Extracted & Appended": grand_total_tally}])], ignore_index=True), use_container_width=True, hide_index=True)

    with tab_proj_master:
        st.subheader("🗄️ Master Project Lifecycle Directory")
        st.dataframe(client.query(f"SELECT Project as `Project ID`, ProjectName as `Friendly Name`, ProjectStatus as `Operational Phase`, Date_Freezedown as `Freezedown Date`, City, Timezone FROM `{PROJECT_REGISTRY_TABLE}` ORDER BY Project ASC").to_dataframe(), use_container_width=True, hide_index=True)

# =============================================================================
# 12. MASTER LAYOUT FRAMEWORK ROUTER
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
    render_node_diagnostics(selected_project, display_tz, unit_label)
elif page in ["Data Processing", "Admin Tools"]:
    if st.session_state.get('authenticated', False):
        if page == "Data Processing": render_data_processing_page(selected_project)
        elif page == "Admin Tools": render_admin_page(selected_project, display_tz, unit_mode, unit_label, active_refs)
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
                else: st.error("Invalid Password. Access Denied.")
