import streamlit as st
import pandas as pd
import time
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz
import requests
import json
import traceback
import re
import io
import plotly.io as pio

################################
# --- GET ALL PROJECT DATA --- #
################################
@st.cache_data(ttl=600)
def get_universal_portal_data(project_id, only_approved=True):
    """
    Standardizes NY and Pacific data to UTC and handles hourly scrubbing joins.
    """
    query = f"""
        WITH UnifiedRaw AS (
            SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
            UNION ALL
            SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
        ),
        JoinedData AS (
            SELECT 
                r.NodeNum, r.timestamp, r.temperature,
                m.Location, m.Bank, m.Depth, m.Project,
                # Join logic: Truncate both to the hour so the scrub works
                CASE WHEN rej.NodeNum IS NULL THEN 'TRUE' ELSE 'FALSE' END as is_currently_approved
            FROM UnifiedRaw r
            INNER JOIN `{PROJECT_ID}.{DATASET_ID}.metadata` m ON r.NodeNum = m.NodeNum
            LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.manual_rejections` rej 
                ON r.NodeNum = rej.NodeNum 
                AND TIMESTAMP_TRUNC(r.timestamp, HOUR) = TIMESTAMP_TRUNC(rej.timestamp, HOUR)
        )
        SELECT * FROM JoinedData
        WHERE Project = '{project_id}'
        { "AND is_currently_approved = 'TRUE'" if only_approved else "" }
        AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 84 DAY)
        ORDER BY Location ASC, timestamp ASC
    """
    try:
        df = client.query(query).to_dataframe()
        if not df.empty:
            # Force UTC alignment for all incoming data
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
            if 'Bank' not in df.columns:
                df['Bank'] = ""
        return df
    except Exception as e:
        st.error(f"BigQuery Error: {e}")
        return pd.DataFrame()
##############################
# --- CHECK ADMIN ACCESS --- #
##############################
def check_admin_access():
    if "admin_authenticated" not in st.session_state:
        st.session_state["admin_authenticated"] = False

    if st.session_state["admin_authenticated"]:
        return True

    # Check if the secret even exists before trying to compare it
    if "admin_password" not in st.secrets:
        st.error("Developer Error: 'admin_password' is not defined in Streamlit Secrets.")
        return False

    st.warning("🔒 This area is restricted to Engineering Admins.")
    pwd_input = st.text_input("Enter Admin Password", type="password")
    
    if st.button("Unlock Tools"):
        if pwd_input == st.secrets["admin_password"]:
            st.session_state["admin_authenticated"] = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    return False
###################################
# --- GET CASHED PROJECT DATA --- #
###################################
@st.cache_data(ttl=600) # Cache data for 10 minutes
def get_cached_project_data(project_id, days=84):
    """
    Centralized data fetcher. 
    Returns all approved data for a project in one batch.
    """
    query = f"""
        SELECT timestamp, temperature, Depth, Location, Bank, NodeNum
        FROM `{MASTER_TABLE}`
        WHERE Project = '{project_id}' 
        AND (approve = 'TRUE' OR approve = 'true')
        AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
        ORDER BY timestamp ASC
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def get_project_list():
    """Caches the project list to speed up sidebar loading."""
    proj_q = f"SELECT DISTINCT Project FROM `{MASTER_TABLE}` WHERE Project IS NOT NULL"
    return client.query(proj_q).to_dataframe()['Project'].dropna().unique()

def style_delta(val):
    """Global styling for temperature deltas."""
    if val is None or pd.isna(val): return ""
    bg, color = "", "black"
    if val >= 5: bg, color = "#FF0000", "white"     # Critical Rise
    elif val >= 2: bg = "#FFA500"                   # Warning Rise
    elif val >= 0.5: bg = "#FFFF00"                 # Slight Rise
    elif -0.5 <= val <= 0.5: bg, color = "#008000", "white" # Stable
    elif -2 < val < -0.5: bg = "#ADD8E6"            # Slight Cooling
    elif -5 < val <= -2: bg, color = "#4169E1", "white" # Strong Cooling
    elif val <= -5: bg, color = "#00008B", "white"  # Deep Freeze
    return f'background-color: {bg}; color: {color}'

#########################
# --- CONFIGURATION --- #
#########################
# --- 1. CONFIGURATION & STYLING ---
st.set_page_config(page_title="SoilFreeze Data Lab", layout="wide")

# UPDATED: Pointing to the new 'Temperature' dataset
DATASET_ID = "Temperature" 
PROJECT_ID = "sensorpush-export"
# The full table name is now sensorpush-export.Temperature.master_data
MASTER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.master_data"
METADATA_TABLE = "metadata"

@st.cache_resource
def get_bq_client():
    """Handles authentication with BigQuery and Drive scopes."""
    try:
        SCOPES = [
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/drive"
        ]
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
            return bigquery.Client(credentials=credentials, project=info["project_id"])
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"Authentication Failed: {e}")
        return None

client = get_bq_client()

#################
# --- Graph --- #
#################
def build_high_speed_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label, display_tz="UTC", is_report=False):
    """
    Unified Plotly engine. 
    Restores Dashboard grid hierarchy, Now line, and tooltips while supporting Report Mode.
    """
    if df.empty:
        return go.Figure()

    plot_df = df.copy()
    
    # 1. TIMEZONE CONVERSION
    plot_df['timestamp'] = plot_df['timestamp'].dt.tz_convert(display_tz)
    
    # Adjust windows and 'Now' line to match local zone
    start_local = start_view.astimezone(pytz.timezone(display_tz))
    end_local = end_view.astimezone(pytz.timezone(display_tz))
    now_local = pd.Timestamp.now(tz=display_tz)

    # 2. UNIT CONVERSION
    if unit_mode == "Celsius":
        plot_df['temperature'] = (plot_df['temperature'] - 32) * 5/9
        y_range, dt_minor = [-30, 30], 2
    else:
        y_range, dt_minor = [-20, 80], 5

    # 3. LABELING LOGIC (The Fix: Ensures 'label' column exists)
    if 'label' not in plot_df.columns:
        plot_df['label'] = plot_df.apply(
            lambda r: f"Bank {r['Bank']} ({r['NodeNum']})" if str(r.get('Bank')).strip().lower() not in ["", "none", "nan", "null"]
            else f"{r.get('Depth')}ft ({r.get('NodeNum')})", axis=1
        )
    
    # 4. PLOT MODE
    is_admin = "Scrubbing" in title or "Diag" in title
    plot_mode = 'markers' if is_admin else 'lines'
    marker_size = 7 if is_admin else 3

    fig = go.Figure()
    
    for lbl in sorted(plot_df['label'].unique()):
        s_df = plot_df[plot_df['label'] == lbl].sort_values('timestamp')
        hover_name = lbl.split('(')[0].strip()

        # 5. GAP DETECTION (Dashboard Only)
        if not is_admin and not is_report:
            s_df['gap_hrs'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
            gap_mask = s_df['gap_hrs'] > 6.0
            if gap_mask.any():
                gaps = s_df[gap_mask].copy()
                gaps['temperature'] = None
                gaps['timestamp'] = gaps['timestamp'] - pd.Timedelta(minutes=1)
                s_df = pd.concat([s_df, gaps]).sort_values('timestamp')

        # 6. ADD TRACE WITH CLEAN HOVER
        fig.add_trace(go.Scattergl(
            x=s_df['timestamp'], 
            y=s_df['temperature'], 
            name=lbl, 
            mode=plot_mode,
            marker=dict(size=marker_size, opacity=0.8 if is_admin else 1.0),
            line=dict(width=2.5 if is_report else 1.5), # Slightly thicker for PDF
            connectgaps=False,
            customdata=[hover_name] * len(s_df),
            hovertemplate=f"<b>%{{customdata}}</b>: %{{y:.1f}}{unit_label}<extra></extra>"
        ))

    # 7. GRID HIERARCHY (Monday=Black, Midnight=Gray, 6h=LightGray)
    grid_times = pd.date_range(start=start_local, end=end_local, freq='6h', tz=display_tz)
    for ts in grid_times:
        if ts.weekday() == 0 and ts.hour == 0:
            color, width = "Black", 1.2 
        elif ts.hour == 0:
            color, width = "Gray", 0.8  
        else:
            color, width = "LightGray", 0.3
        fig.add_vline(x=ts, line_width=width, line_color=color, layer='below')

    # 8. REFERENCE LINES & RED "NOW" LINE
    if is_report:
        # Report Mode: Default to 32°F / 0°C
        ref_val = 32 if unit_label == "°F" else 0
        fig.add_hline(y=ref_val, line_dash="dash", line_color="DeepSkyBlue", 
                      annotation_text="Freezing", annotation_position="top right")
    else:
        # Dashboard Mode: Standard active_refs + Red "Now" Line
        for val, ref_label in active_refs:
            c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
            fig.add_hline(y=c_val, line_dash="dash", line_color="maroon" if "Type A" in ref_label else "RoyalBlue", 
                          annotation_text=ref_label, annotation_position="top right")
        
        fig.add_vline(x=now_local, line_width=2, line_color="Red", layer='above', line_dash="dash")

    # 9. FINAL LAYOUT
    fig.update_layout(
        title=None if is_report else {'text': f"{title} ({display_tz})", 'x': 0},
        plot_bgcolor='white',
        hovermode="x unified",
        height=850 if is_report else 600,
        margin=dict(t=80, l=50, r=180, b=50),
        xaxis=dict(
            range=[start_local, end_local],
            showline=True, 
            linecolor='black', 
            mirror=True,
            tickformat='%b %d\n%H:%M'
        ),
        yaxis=dict(
            title=f"Temperature ({unit_label})",
            range=y_range,
            dtick=dt_minor,
            gridcolor='Gainsboro',
            showline=True,
            linecolor='black',
            mirror=True
        ),
        legend=dict(
            title="Sensors",
            orientation="v",
            x=1.02,
            y=1,
            xanchor="left",
            bordercolor="Black",
            borderwidth=1
        )
    )
    
    return fig
################
# Print graphs #
################
# Official SoilFreeze Color Palette
SOILFREEZE_COLORS = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"]

def apply_report_frame(fig, project_name, title, fig_num, date_str):
    """
    Standardizes the 11x8.5 frame. 
    Ensures Pipe Name is centered and Project Name is left-justified.
    """
    fig.update_layout(
        width=1100,
        height=850,
        margin=dict(t=200, l=80, r=220, b=100), 
        paper_bgcolor='white',
        plot_bgcolor='white',
        # Clear any existing title from the internal plotly engine
        title=None,
        shapes=[
            dict(type="rect", xref="paper", yref="paper", x0=-0.05, y0=-0.1, x1=1.05, y1=1.1, line=dict(color="black", width=2)),
            dict(type="line", xref="paper", yref="paper", x0=-0.05, y0=1.0, x1=1.05, y1=1.0, line=dict(color="black", width=1)),
            dict(type="rect", xref="paper", yref="paper", x0=0.25, y0=1.04, x1=0.75, y1=1.12, fillcolor="#F2F4F4", line=dict(color="black", width=1)),
        ],
        annotations=[
            # Left: Project Name
            dict(text=f"<b>PROJECT:</b><br>{project_name.upper()}", 
                 x=-0.03, y=1.08, xref="paper", yref="paper", showarrow=False, align="left", xanchor="left", font=dict(size=14)),
            # Center: Pipe Title (e.g. Temperature TP33-N)
            dict(text=f"<b>{title.upper()}</b>", 
                 x=0.5, y=1.08, xref="paper", yref="paper", showarrow=False, xanchor="center", font=dict(size=20, color="#003366")),
            # Right: Logo
            dict(text="<b>SoilFreeze</b><br><small>SOLID GROUND</small>", 
                 x=1.03, y=1.08, xref="paper", yref="paper", showarrow=False, align="right", xanchor="right", font=dict(color="#003366")),
            # Footer
            dict(text=f"<b>FIGURE {fig_num}</b>", x=0, y=-0.07, xref="paper", yref="paper", showarrow=False, font=dict(size=14)),
            dict(text=f"<b>DATE:</b> {date_str}", x=1, y=-0.07, xref="paper", yref="paper", showarrow=False, font=dict(size=12)),
        ]
    )
    return fig

def build_depth_report_graph(df, loc_name, unit_label):
    """
    Restored Depth Profile Engine.
    Matches the original vertical logic with updated report formatting.
    """
    fig = go.Figure()
    if df.empty: return fig
    
    # 1. SETUP DIMENSIONS & RANGES
    # Use 22ft as default range or dynamic based on data
    max_d = df['Depth_Num'].max() if not df['Depth_Num'].empty else 20
    y_limit = int(((max_d // 5) + 1) * 5)
    x_range = [-20, 80] if unit_label == "°F" else [(-20-32)*5/9, (80-32)*5/9]

    # 2. WEEKLY SNAPSHOT LOGIC (Monday 6AM)
    # This finds the state of the ground every Monday to show progress
    now = df['timestamp'].max()
    start_view = now - pd.Timedelta(weeks=4)
    mondays = pd.date_range(start=start_view, end=now, freq='W-MON')
    
    for m_date in mondays:
        target_ts = m_date.replace(hour=6, minute=0, second=0)
        # Search window to catch sensors that report at different times
        window = df[(df['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & 
                    (df['timestamp'] <= target_ts + pd.Timedelta(hours=12))]
        
        if not window.empty:
            window = window.copy()
            window['diff'] = (window['timestamp'] - target_ts).abs()
            snap_df = window.sort_values('diff').groupby('NodeNum').head(1).sort_values('Depth_Num')
            
            fig.add_trace(go.Scatter(
                x=snap_df['temperature'], 
                y=snap_df['Depth_Num'], 
                mode='lines+markers', 
                name=target_ts.strftime('%m/%d/%y'),
                line=dict(width=3),
                marker=dict(size=8)
            ))

    # 3. REFERENCE LINES (Default to Freezing Only)
    ref_val = 32 if unit_label == "°F" else 0
    fig.add_vline(x=ref_val, line_dash="dot", line_color="DeepSkyBlue", 
                  line_width=3, annotation_text="Freezing", annotation_position="top right")

    # 4. FINAL LAYOUT & GRID
    fig.update_layout(
        plot_bgcolor='white',
        height=850, # Matches 11x8.5 ratio
        xaxis=dict(
            title=f"Temperature ({unit_label})", 
            range=x_range, 
            showgrid=True, 
            gridcolor='Gainsboro',
            showline=True,
            linecolor='black',
            mirror=True
        ),
        yaxis=dict(
            title="Depth (ft)", 
            range=[y_limit, 0], 
            dtick=2, # Detailed 2ft increments
            showgrid=True, 
            gridcolor='Silver',
            showline=True,
            linecolor='black',
            mirror=True
        ),
        legend=dict(
            title="Weekly Snapshots (6AM)", 
            orientation="h", 
            y=-0.15, 
            x=0.5, 
            xanchor="center",
            bordercolor="black",
            borderwidth=1
        )
    )
    
    return fig

# ############################################################
# # --- DATA INITIALIZATION (Automated) ---                  #
# ############################################################

# 1. Fetch the project list automatically as you did before
try:
    # This calls your existing function that queries BigQuery/API
    project_list = get_project_list() 
    
    if not project_list:
        project_list = ["No Active Projects"]
except Exception as e:
    # Fallback to prevent the sidebar from disappearing if the API is down
    project_list = ["Error Fetching Projects"]
    st.error(f"Connection Error: {e}")

# ############################################################
# # --- SIDEBAR: GLOBAL CONTROLS & NAVIGATION ---           #
# ############################################################

with st.sidebar:
    st.title("❄️ SoilFreeze Lab")
    st.markdown("---")

    # 1. RESTORED NAVIGATION
    # Ensure these labels EXACTLY match the 'if service ==' lines in your main code
    with st.sidebar:
        st.title("❄️ SoilFreeze Lab")
        
        # 1. RESTORED NAVIGATION (Matching your previous labels)
        service = st.selectbox(
            "📂 Select Page", 
            ["🌐 Global Overview", "🏠 Executive Summary", "📊 Client Portal", "📉 Node Diagnostics", "📤 Data Intake Lab", "🛠️ Admin Tools"],
            index=0  
        )
        st.divider()
    
   # 2. DATA SOURCE: Automatic Project Selection
    selected_project = st.selectbox("📁 Select Project", options=project_list, index=0)

    # 3. UNIT CONTROLS
    unit_mode = st.toggle("🌡️ Display Celsius", value=False)
    unit_label = "°C" if unit_mode else "°F"

    st.divider()

    # 4. REFERENCE LINES: Checkboxes (Freezing = Default)
    st.markdown("### 📏 Reference Lines")
    
    # Values update dynamically based on Celsius/Fahrenheit toggle
    freeze_val = 0 if unit_mode else 32
    
    show_freeze = st.checkbox(f"Freezing ({freeze_val}{unit_label})", value=True)
    show_type_a = st.checkbox("Type A", value=False)
    show_type_b = st.checkbox("Type B", value=False)

    # Build the active_refs list for your graphing functions
    active_refs = []
    if show_freeze: active_refs.append((32, "Freezing"))
    if show_type_a: active_refs.append((12, "Type A"))
    if show_type_b: active_refs.append((30, "Type B"))

    st.divider()

    # 5. ADVANCED SETTINGS
    with st.expander("⚙️ Advanced Settings"):
        display_tz = st.selectbox(
            "Timezone",
            ["UTC", "US/Pacific", "US/Mountain", "US/Central", "US/Eastern"],
            index=1 
        )
        show_diagnostics = st.checkbox("Show Sensor Metadata", value=False)
    
#################
# --- PAGES --- #
#################
###########################
# --- GLOBAL OVERVIEW --- #
###########################
if service == "🌐 Global Overview":
    st.header("🌐 Project Overview")
    
    # 1. Project Selector
    project_list_query = f"SELECT DISTINCT Project FROM `{PROJECT_ID}.{DATASET_ID}.metadata` WHERE Project IS NOT NULL"
    available_projects = sorted(client.query(project_list_query).to_dataframe()['Project'].astype(str).tolist())
    
    target_project = st.selectbox("🏗️ Select a Project", available_projects, key="global_proj_picker")

    if target_project:
        with st.spinner("Loading site timeline..."):
            p_df = get_universal_portal_data(target_project, only_approved=True)

        if not p_df.empty:
            lookback = st.sidebar.slider("Lookback (Weeks)", 1, 12, 4)
            now_utc = pd.Timestamp.now(tz='UTC')
            end_view = (now_utc + pd.Timedelta(days=(7-now_utc.weekday())%7 or 7)).replace(hour=0, minute=0, second=0)
            start_view = end_view - timedelta(weeks=lookback)

            for loc in sorted(p_df['Location'].unique()):
                with st.expander(f"📍 {loc}", expanded=True):
                    loc_df = p_df[p_df['Location'] == loc]
                    fig = build_high_speed_graph(loc_df, f"📈 {target_project} - {loc}", start_view, end_view, tuple(active_refs), unit_mode, unit_label, display_tz=display_tz)
                    st.plotly_chart(fig, use_container_width=True, key=f"ov_{target_project}_{loc}")
#############################
# --- Executive Summary --- #
#############################
if service == "🏠 Executive Summary":
    st.header(f"🏠 Executive Summary: {selected_project if selected_project else 'All Projects'}")
    
    # 1. SORTING & CONTROLS
    st.write("### ↕️ Sorting & View Options")
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        sort_choice = st.selectbox("Sort By", ["None", "Hours Since Last Seen", "Delta Magnitude"])
    with c2:
        sort_order = st.radio("Order", ["Descending", "Ascending"], horizontal=True)
    
    # 2. BATCH DATA QUERY (Optimized to 1 Query instead of N queries)
    # Fetch all data for the last 24H for the entire project at once
    summary_q = f"""
        WITH RecentData AS (
            SELECT *,
                FIRST_VALUE(temperature) OVER(PARTITION BY NodeNum ORDER BY timestamp ASC) as first_temp_24h,
                ROW_NUMBER() OVER(PARTITION BY NodeNum ORDER BY timestamp DESC) as latest_rank
            FROM `{MASTER_TABLE}`
            WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            {"AND Project = '" + selected_project + "'" if selected_project else ""}
        )
        SELECT 
            NodeNum, Project, Location, Bank, Depth, timestamp, temperature,
            first_temp_24h,
            MIN(temperature) OVER(PARTITION BY NodeNum) as min_24h,
            MAX(temperature) OVER(PARTITION BY NodeNum) as max_24h
        FROM RecentData
        WHERE latest_rank = 1
    """
    
    try:
        with st.spinner("⚡ Syncing Command Center (Batch Processing)..."):
            raw_summary_df = client.query(summary_q).to_dataframe()
        
        if raw_summary_df.empty:
            st.warning("📡 No active sensors seen in the last 24 hours.")
        else:
            now = pd.Timestamp.now(tz=pytz.UTC)
            
            # 3. PROCESSING LOGIC (Pandas is faster than SQL for these calculations)
            def process_row(row):
                # Time handling
                ts = row['timestamp'].tz_localize(pytz.UTC) if row['timestamp'].tzinfo is None else row['timestamp']
                hrs_ago = int((now - ts).total_seconds() / 3600)
                
                # Delta Calculation
                raw_delta = row['temperature'] - row['first_temp_24h']
                
                # Status Icon Logic
                if hrs_ago > 24:
                    status_icon, delta_text, delta_val = "🔴", "-", None
                else:
                    status_icon = "🟢" if hrs_ago < 6 else ("🟡" if hrs_ago < 12 else "🟠")
                    delta_text = f"{round(raw_delta, 1)}°F"
                    delta_val = raw_delta

                # Position Labeling
                pos_label = f"Bank {row['Bank']}" if str(row['Bank']).strip().lower() not in ["","none","nan","null"] else f"{row['Depth']} ft"

                return pd.Series({
                    "Project": row['Project'],
                    "Node": row['NodeNum'],
                    "Pipe/Bank": row['Location'],
                    "Pos/Depth": pos_label,
                    "Min": f"{round(convert_val(row['min_24h']), 1)}{unit_label}",
                    "Max": f"{round(convert_val(row['max_24h']), 1)}{unit_label}",
                    "Delta_Val": delta_val, 
                    "Delta": delta_text,
                    "Hours_Ago": hrs_ago,
                    "Last Seen": f"{ts.strftime('%m/%d %H:%M')} ({hrs_ago}h) {status_icon}"
                })

            summary_df = raw_summary_df.apply(process_row, axis=1)

            # 4. APPLY SORTING
            asc = (sort_order == "Ascending")
            if sort_choice == "Hours Since Last Seen":
                summary_df = summary_df.sort_values(by="Hours_Ago", ascending=asc)
            elif sort_choice == "Delta Magnitude":
                summary_df['abs_d'] = summary_df['Delta_Val'].abs().fillna(-1)
                summary_df = summary_df.sort_values(by="abs_d", ascending=asc).drop(columns=['abs_d'])

            # 5. STYLING FUNCTION
            def style_delta(val):
                if val is None or pd.isna(val): return ""
                bg, color = "", "black"
                if val >= 5: bg, color = "#FF0000", "white"     # Critical Heat
                elif val >= 2: bg = "#FFA500"                   # Warning Heat
                elif val >= 0.5: bg = "#FFFF00"                 # Slight Rise
                elif -0.5 <= val <= 0.5: bg, color = "#008000", "white" # Stable
                elif -2 < val < -0.5: bg = "#ADD8E6"            # Slight Cooling
                elif -5 < val <= -2: bg, color = "#4169E1", "white" # Strong Cooling
                elif val <= -5: bg, color = "#00008B", "white"  # Deep Freeze
                return f'background-color: {bg}; color: {color}'

            # 6. DISPLAY
            st.subheader(f"📡 Engineering Command Center ({len(summary_df)} sensors)")
            
            st.dataframe(
                summary_df[["Project", "Node", "Pipe/Bank", "Pos/Depth", "Min", "Max", "Delta", "Last Seen"]].style.apply(
                    lambda x: [style_delta(rv) for rv in summary_df['Delta_Val']], axis=0, subset=['Delta']
                ),
                use_container_width=True,
                hide_index=True,
                height=600
            )
            
    except Exception as e: 
        st.error(f"Summary Error: {traceback.format_exc()}")
#################################
# --- END EXECUTIVE SUMMARY --- #
#################################
#########################
# --- CLIENT PORTAL --- #
#########################
elif service == "📊 Client Portal":
    if not selected_project:
        st.sidebar.warning("Please select a project.")
    else:
        st.header(f"📊 Project Status: {selected_project}")
        
        # 1. FETCH DATA (Uses cached function)
        p_df = get_universal_portal_data(selected_project)
        
        if p_df.empty:
            st.info(f"No approved data found for {selected_project}.")
        else:
            tab_time, tab_depth, tab_table = st.tabs(["📈 Timeline Analysis", "📏 Depth Profile", "📋 Project Data"])

            with tab_time:
                weeks_view = st.slider("Weeks to View", 1, 12, 6, key="cp_weeks")
                now = pd.Timestamp.now(tz=pytz.UTC)
                end_view = (now + pd.Timedelta(days=(7 - now.weekday()) % 7 or 7)).replace(hour=0, minute=0, second=0, microsecond=0)
                start_view = end_view - timedelta(weeks=weeks_view)
                
                for loc in sorted(p_df['Location'].dropna().unique()):
                    with st.expander(f"📈 {loc}", expanded=True):
                        loc_data = p_df[(p_df['Location'] == loc) & (p_df['timestamp'] >= start_view)]
                        # Uses the High-Speed Engine (ensure build_high_speed_graph is updated)
                        fig = build_high_speed_graph(loc_data, loc, start_view, end_view, tuple(active_refs), unit_mode, unit_label)
                        st.plotly_chart(fig, use_container_width=True, key=f"cht_{loc}", config={'displayModeBar': False})

            with tab_depth:
                p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
                depth_only = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
                
                for loc in sorted(depth_only['Location'].unique()):
                    with st.expander(f"📏 {loc} Depth Profile", expanded=True):
                        loc_data = depth_only[depth_only['Location'] == loc].copy()
                        fig_d = go.Figure()
                        
                        # Monday Snapshots logic
                        mondays = pd.date_range(start=start_view, end=now, freq='W-MON')
                        
                        for m_date in mondays:
                            target_ts = m_date.replace(hour=6, minute=0, second=0).tz_localize(pytz.UTC) if m_date.tzinfo is None else m_date.replace(hour=6)
                            window = loc_data[(loc_data['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & (loc_data['timestamp'] <= target_ts + pd.Timedelta(hours=12))]
                            
                            if not window.empty:
                                snap_list = []
                                for node in window['NodeNum'].unique():
                                    node_data = window[window['NodeNum'] == node].copy()
                                    node_data['diff'] = (node_data['timestamp'] - target_ts).abs()
                                    snap_list.append(node_data.sort_values('diff').iloc[0])
                                
                                snap_df = pd.DataFrame(snap_list).sort_values('Depth_Num')
                                fig_d.add_trace(go.Scattergl(x=snap_df['temperature'], y=snap_df['Depth_Num'], mode='lines+markers', name=target_ts.strftime('%m/%d/%y')))

                        # --- DYNAMIC GRID LOGIC ---
                        if unit_mode == "Celsius":
                            x_range = [( -20 - 32) * 5/9, (80 - 32) * 5/9]
                            x_major, x_minor = 10, 2
                        else:
                            x_range = [-20, 80]
                            x_major, x_minor = 20, 5

                        y_limit = int(((loc_data['Depth_Num'].max() // 5) + 1) * 5) if not loc_data.empty else 50
                        
                        fig_d.update_layout(
                            plot_bgcolor='white', height=700,
                            # X-AXIS: Configured for Minor 5° grid
                            xaxis=dict(
                                title=f"Temp ({unit_label})", 
                                range=x_range, 
                                dtick=x_minor,           # Set minor interval (5°)
                                showgrid=True,           # Explicitly show minor grid
                                gridcolor='Gainsboro',   # Light gray for minor lines
                                gridwidth=0.5,
                                showline=True, 
                                linecolor='black', 
                                mirror=True
                            ),
                            # Y-AXIS: 10ft grid
                            yaxis=dict(
                                title="Depth (ft)", 
                                range=[y_limit, 0], 
                                dtick=10, 
                                showgrid=True,
                                gridcolor='Silver',      # Slightly darker for depth lines
                                showline=True, 
                                linecolor='black', 
                                mirror=True
                            ),
                            legend=dict(title="Weekly Snapshots (6AM)", orientation="h", y=-0.2)
                        )

                        # ADD MAJOR TEMPERATURE LINES (Every 20°)
                        # We iterate through the range to add bold vertical markers
                        for x_v in range(-40, 101, x_major):
                            if x_range[0] <= x_v <= x_range[1]:
                                fig_d.add_vline(x=x_v, line_width=1.5, line_color="DimGray", layer='below')

                        # ADD REFERENCE THRESHOLDS (Freezing, Type A, Type B)
                        for val, label in active_refs:
                            c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
                            fig_d.add_vline(x=c_val, line_dash="dash", line_color="maroon" if "Type A" in label else "RoyalBlue", line_width=2.5, opacity=0.8)
                            
                        st.plotly_chart(fig_d, use_container_width=True, key=f"dep_{loc}", config={'displayModeBar': False})

            with tab_table:
                latest = p_df.sort_values('timestamp').groupby('NodeNum').tail(1).copy()
                latest['Current Temp'] = latest['temperature'].apply(lambda x: f"{round(convert_val(x), 1)}{unit_label}")
                latest['Position'] = latest.apply(lambda r: f"Bank {r['Bank']}" if pd.notnull(r['Bank']) and str(r['Bank']).strip() != "" else f"{r.get('Depth', '??')} ft", axis=1)
                st.dataframe(latest[['Location', 'Position', 'Current Temp', 'NodeNum']].sort_values(['Location', 'Position']), use_container_width=True, hide_index=True)
#############################
# --- END CLIENT PORTAL --- #
#############################  
###########################
# --- NODE DIAGNOSTIC --- #
###########################  
elif service == "📉 Node Diagnostics":
    st.header(f"📉 Node Diagnostics: {selected_project}")
    
    if not selected_project:
        st.warning("👈 Please select a project in the sidebar to begin analysis.")
    else:
        try:
            # 1. DATA ACCESS & TIMEFRAME
            # Fetch all data (including unapproved) for full engineering visibility
            with st.spinner("🔍 Syncing diagnostic streams..."):
                all_data = get_universal_portal_data(selected_project, only_approved=False)
            
            if all_data.empty:
                st.warning(f"No data found for project {selected_project}.")
            else:
                # Layout for Top Controls
                loc_options = sorted(all_data['Location'].dropna().unique())
                c1, c2 = st.columns([2, 1])
                with c1: 
                    sel_loc = st.selectbox("Select Pipe / Bank to Analyze", loc_options)
                with c2: 
                    weeks_view = st.slider("Lookback (Weeks)", 1, 12, 4, key="diag_lookback")

                # Date Calculations (Standardized UTC)
                now_utc = pd.Timestamp.now(tz=pytz.UTC)
                days_until_monday = (7 - now_utc.weekday()) % 7 or 7
                end_view = (now_utc + pd.Timedelta(days=days_until_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
                start_view = end_view - timedelta(weeks=weeks_view)

                # Filter data for the specific location
                df_diag = all_data[all_data['Location'] == sel_loc].copy()

                # --- 2. THE THREE ANALYSIS SECTIONS ---
                
                # SECTION A: TIMELINE ANALYSIS
                st.subheader("📈 Timeline Analysis")
                st.caption(f"Viewing historical trends in **{tz_mode}**.")
                fig_time = build_high_speed_graph(
                    df_diag, sel_loc, start_view, end_view, 
                    tuple(active_refs), unit_mode, unit_label, 
                    display_tz=display_tz
                )
                st.plotly_chart(fig_time, use_container_width=True, config={'displayModeBar': True}, key=f"diag_time_{sel_loc}")

                st.divider()

                # SECTION B: DEPTH PROFILE ANALYSIS
                st.subheader("📏 Depth Profile Analysis")
                df_diag['Depth_Num'] = pd.to_numeric(df_diag['Depth'], errors='coerce')
                depth_only_df = df_diag.dropna(subset=['Depth_Num', 'NodeNum']).copy()
                
                if depth_only_df.empty:
                    st.info("No depth-based sensors (ground sticks/strings) found for this location.")
                else:
                    fig_depth = go.Figure()
                    # Generate snapshots for Monday mornings at 6:00 AM UTC
                    mondays = pd.date_range(start=start_view, end=end_view, freq='W-MON')
                    
                    for m_date in mondays:
                        target_ts = m_date.replace(hour=6, minute=0, second=0, tzinfo=pytz.UTC)
                        
                        # Grab closest points within a 12-hour window of the target
                        window = depth_only_df[
                            (depth_only_df['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & 
                            (depth_only_df['timestamp'] <= target_ts + pd.Timedelta(hours=12))
                        ]
                        
                        if not window.empty:
                            snap_list = []
                            for node in window['NodeNum'].unique():
                                node_data = window[window['NodeNum'] == node].copy()
                                node_data['diff'] = (node_data['timestamp'] - target_ts).abs()
                                snap_list.append(node_data.sort_values('diff').iloc[0])
                            
                            snap_df = pd.DataFrame(snap_list).sort_values('Depth_Num')
                            fig_depth.add_trace(go.Scattergl(
                                x=snap_df['temperature'], y=snap_df['Depth_Num'],
                                mode='lines+markers', name=target_ts.strftime('%m/%d/%y')
                            ))

                    # Formatting the Vertical Profile
                    y_limit = int(((depth_only_df['Depth_Num'].max() // 5) + 1) * 5)
                    x_range = [-20, 80] if unit_mode == "Fahrenheit" else [(-20-32)*5/9, (80-32)*5/9]
                    
                    fig_depth.update_layout(
                        plot_bgcolor='white', height=700,
                        xaxis=dict(title=f"Temp ({unit_label})", range=x_range, showgrid=True, gridcolor='Gainsboro'),
                        yaxis=dict(title="Depth (ft)", range=[y_limit, 0], dtick=10, showgrid=True, gridcolor='Gray'),
                        legend=dict(title="Weekly Snapshots (6AM UTC)", x=1.02, y=1)
                    )
                    st.plotly_chart(fig_depth, use_container_width=True)

                st.divider()

                # SECTION C: ENGINEERING STATUS TABLE
                st.subheader(f"📋 Engineering Summary: {sel_loc}")
                latest_nodes = df_diag.sort_values('timestamp').groupby('NodeNum').tail(1).copy()
                summary_rows = []
                
                for _, row in latest_nodes.iterrows():
                    node_id = row['NodeNum']
                    # Calculate how long since the last packet in UTC
                    hrs_ago = int((now_utc - row['timestamp']).total_seconds() / 3600)
                    status_icon = "🔴" if hrs_ago > 24 else ("🟢" if hrs_ago < 6 else "🟡")
                    
                    # Position Labeling
                    pos_display = f"Bank {row['Bank']}" if str(row['Bank']).strip().lower() not in ["","none","nan","null"] else f"{row['Depth']} ft"

                    summary_rows.append({
                        "Node": node_id,
                        "Position": pos_display,
                        "Last Reading": f"{round(row['temperature'], 1)}{unit_label}",
                        "Last Seen (UTC)": f"{row['timestamp'].strftime('%m/%d %H:%M')} ({hrs_ago}h ago)",
                        "Health": status_icon,
                        "Approval": "✅ Approved" if row['is_currently_approved'] == 'TRUE' else "❌ Rejected"
                    })
                
                st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, hide_index=True)

        except Exception as e:
            st.error(f"Diagnostics Rendering Error: {traceback.format_exc()}")
###############################
# --- END NODE DIAGNOSTIC --- #
###############################
###############################
# --- DATA INTAKE LAB ---     #
###############################
elif service == "📤 Data Intake Lab":
    if check_admin_access():
        st.header("📤 Data Ingestion & Recovery")
        
        # 1. Define the tabs
        tab1, tab2, tab3 = st.tabs(["📄 Manual File Upload", "🖼️ Professional Report Export", "📥 Export Raw Data"])
        
        with tab1:
            st.subheader("📄 Manual File Ingestion")
            st.info("Upload Lord SensorConnect (Wide), Lord Desktop Log (Narrow), or SensorPush CSVs.")
            u_file = st.file_uploader("Upload CSV", type=['csv'], key="manual_upload_unified_fixed")
            
            if u_file is not None:
                import io
                filename = u_file.name.lower()
                raw_content = u_file.getvalue().decode('utf-8').splitlines()
                
                # --- DETECT FILE TYPE ---
                is_lord_wide = any("DATA_START" in line for line in raw_content[:100])

                # Updated detection logic to support 'Channel' header
                is_lord_narrow = ("nodenumber" in raw_content[0].lower() or "channel" in raw_content[0].lower()) and \
                                 "temperature" in raw_content[0].lower()
                                
                # --- CASE 1: LORD SENSORCONNECT (WIDE) ---
                if is_lord_wide:
                    try:
                        start_idx = next(i for i, line in enumerate(raw_content) if "DATA_START" in line)
                        df_wide = pd.read_csv(io.StringIO("\n".join(raw_content[start_idx+1:])))
                        # Rename 'Time' to 'timestamp' and melt columns into 'NodeNum'
                        df_long = df_wide.melt(id_vars=['Time'], var_name='NodeNum', value_name='temperature')
                        df_long['NodeNum'] = df_long['NodeNum'].str.replace(':', '-', regex=False)
                        df_long['timestamp'] = pd.to_datetime(df_long['Time'], format='mixed')
                        df_long = df_long.dropna(subset=['temperature'])
                        
                        st.success(f"✅ Lord Wide Format Parsed: {len(df_long)} readings.")
                        st.dataframe(df_long.head())
                        if st.button("🚀 UPLOAD LORD WIDE DATA"):
                            client.load_table_from_dataframe(df_long[['timestamp', 'NodeNum', 'temperature']], 
                                                             f"{PROJECT_ID}.{DATASET_ID}.raw_lord").result()
                            st.success("Uploaded successfully to raw_lord!")
                    except Exception as e: st.error(f"Lord Wide Error: {e}")
    
                # --- CASE 2: LORD DESKTOP LOG (NARROW) ---
                elif is_lord_narrow:
                    try:
                        df_ln = pd.read_csv(io.StringIO("\n".join(raw_content)))
                        # MAP TO BIGQUERY SCHEMA: Case-sensitive NodeNum and timestamp
                        df_ln = df_ln.rename(columns={
                            'Timestamp': 'timestamp', 
                            'Channel': 'NodeNum', 
                            'Temperature': 'temperature'
                        })
                        df_ln['timestamp'] = pd.to_datetime(df_ln['timestamp'], format='mixed')
                        df_ln['NodeNum'] = df_ln['NodeNum'].str.replace(':', '-', regex=False)
                        
                        st.success(f"✅ Lord Narrow Format Parsed: {len(df_ln)} readings.")
                        st.dataframe(df_ln.head())
                        if st.button("🚀 UPLOAD LORD NARROW DATA"):
                            client.load_table_from_dataframe(df_ln[['timestamp', 'NodeNum', 'temperature']], 
                                                             f"{PROJECT_ID}.{DATASET_ID}.raw_lord").result()
                            st.success("Uploaded successfully to raw_lord!")
                    except Exception as e: st.error(f"Lord Narrow Error: {e}")

                # --- CASE 3: SENSORPUSH ---
                else:
                    try:
                        header_idx = -1
                        for i, line in enumerate(raw_content[:50]):
                            if "SensorId" in line or "Observed" in line:
                                header_idx = i; break
                        
                        if header_idx != -1:
                            df_sp = pd.read_csv(io.StringIO("\n".join(raw_content[header_idx:])), dtype=str)
                            ts_col = "Observed" if "Observed" in df_sp.columns else df_sp.columns[1]
                            
                            df_up = pd.DataFrame()
                            # Mapping to the raw_sensorpush schema
                            df_up['sensor_id'] = df_sp['SensorId'].astype(str).str.strip()
                            df_up['timestamp'] = pd.to_datetime(df_sp[ts_col], format='mixed')
                            t_cols = [c for c in df_sp.columns if "Temperature" in c or "Thermocouple" in c]
                            df_up['temperature'] = pd.to_numeric(df_sp[t_cols].bfill(axis=1).iloc[:, 0], errors='coerce')
                            df_up = df_up.dropna(subset=['timestamp', 'temperature'])
    
                            st.success(f"✅ SensorPush Parsed: {len(df_up)} readings.")
                            if st.button("🚀 UPLOAD SENSORPUSH"):
                                client.load_table_from_dataframe(df_up, f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush").result()
                                st.success("Uploaded successfully to raw_sensorpush!")
                        else:
                            st.error("Format not recognized. Check CSV headers.")
                    except Exception as e: st.error(f"SensorPush Error: {e}")

# ############################################################
# # --- TAB 2: PROFESSIONAL CLIENT REPORT EXPORT (11x8.5) --- #
# ############################################################
        with tab2:
            st.subheader("📤 Professional Client Report Export")
            
            if not selected_project:
                st.warning("👈 Please select a project in the sidebar first.")
            else:
                # Project Title Input for the Header Box
                report_project_title = st.text_input("📝 Report Project Name / Number", value=selected_project)
                
                with st.spinner(f"Fetching approved data..."):
                    export_df = get_universal_portal_data(selected_project, only_approved=True)
                
                if export_df.empty:
                    st.info("No approved data found for this project.")
                else:
                    export_df['Depth_Num'] = pd.to_numeric(export_df['Depth'], errors='coerce')
                    
                    c1, c2 = st.columns(2)
                    with c1:
                        report_types = st.multiselect("Select Figure Types", 
                            ["Bank Trends (Fig 2)", "Time vs Temp (Fig 3)", "Depth vs Temp (Fig 4)"], 
                            default=["Time vs Temp (Fig 3)", "Depth vs Temp (Fig 4)"])
                    with c2:
                        export_format = st.selectbox("Export Format", ["png", "pdf"], index=1)
        
                    if st.button("🚀 Generate Numbered Report Bundle"):
                        date_str = datetime.now().strftime("%m/%d/%Y")
                        now_utc = pd.Timestamp.now(tz='UTC')
                        
                        # Standard 4-week window ending next Monday
                        end_view = (now_utc + pd.Timedelta(days=(7-now_utc.weekday())%7 or 7)).replace(hour=0, minute=0, second=0, microsecond=0)
                        start_view = end_view - timedelta(weeks=4)
                        
                        pipes = sorted(export_df['Location'].dropna().unique().tolist())
                        
                        # Set Global Color Template
                        try:
                            pio.templates[pio.templates.default].layout.colorway = SOILFREEZE_COLORS
                        except: pass
        
                        for i, loc in enumerate(pipes, start=1):
                            loc_df = export_df[export_df['Location'] == loc]
                            
                            # FIGURE 3.X: Time vs Temp
                            if "Time vs Temp (Fig 3)" in report_types:
                                fig3 = build_high_speed_graph(loc_df, f"Temperature {loc}", start_view, end_view, [], unit_mode, unit_label, display_tz=display_tz, is_report=True)
                                # Center the Pipe Name in the box, Project Name on the left
                                fig3 = apply_report_frame(fig3, report_project_title, f"Temperature {loc}", f"3.{i}", date_str)
                                st.plotly_chart(fig3, use_container_width=True)
                                
                                img3 = fig3.to_image(format=export_format, width=1100, height=850)
                                st.download_button(f"📥 Download Fig 3.{i} ({loc})", img3, f"Fig3.{i}_{loc}.{export_format}", key=f"dl_3_{i}")
        
                            # FIGURE 4.X: Depth vs Temp
                            if "Depth vs Temp (Fig 4)" in report_types:
                                fig4 = build_depth_report_graph(loc_df, loc, unit_label)
                                fig4 = apply_report_frame(fig4, report_project_title, f"Temperature vs Depth: {loc}", f"4.{i}", date_str)
                                st.plotly_chart(fig4, use_container_width=True)
                                
                                img4 = fig4.to_image(format=export_format, width=1100, height=850)
                                st.download_button(f"📥 Download Fig 4.{i} ({loc})", img4, f"Fig4.{i}_{loc}.{export_format}", key=f"dl_4_{i}")
        
        with tab3:
            st.subheader("📥 Export Project Data (SensorConnect Format)")
            
            # Project Selection
            all_projects_q = f"SELECT DISTINCT Project FROM `{PROJECT_ID}.{DATASET_ID}.metadata` WHERE Project IS NOT NULL"
            all_projs = client.query(all_projects_q).to_dataframe()['Project'].tolist()
            default_ix = all_projs.index(selected_project) if selected_project in all_projs else 0
            target_project = st.selectbox("1️⃣ Select Project to Export", sorted(all_projs), index=default_ix)
        
            if target_project:
                with st.spinner(f"Loading data for {target_project}..."):
                    export_df = get_universal_portal_data(target_project, only_approved=False)
        
                if not export_df.empty:
                    c1, c2 = st.columns(2)
                    with c1:
                        pipes = sorted(export_df['Location'].dropna().unique().tolist())
                        sel_pipe = st.selectbox("2️⃣ Select Pipe / Location", pipes)
                    with c2:
                        min_ts, max_ts = export_df['timestamp'].min().date(), export_df['timestamp'].max().date()
                        export_range = st.date_input("3️⃣ Select Date Range", value=(min_ts, max_ts))
        
                    # --- TRANSFORMATION TO WIDE FORMAT ---
                    df_final = export_df[export_df['Location'] == sel_pipe].copy()
                    
                    if isinstance(export_range, tuple) and len(export_range) == 2:
                        start, end = export_range
                        df_final = df_final[(df_final['timestamp'].dt.date >= start) & (df_final['timestamp'].dt.date <= end)]
        
                    if not df_final.empty:
                        # 1. Standardize Units
                        if unit_mode == "Celsius":
                            df_final['temperature'] = (df_final['temperature'] - 32) * 5/9
                        
                        # 2. Create Column Labels (e.g., "5ft")
                        df_final['Depth_Col'] = df_final['Depth'].astype(str) + "ft"
                        
                        # 3. PIVOT: Turns Depths into Columns
                        df_wide = df_final.pivot_table(
                            index='timestamp', 
                            columns='Depth_Col', 
                            values='temperature',
                            aggfunc='first' 
                        ).reset_index()

                        # 4. NATURAL SORT: Ensures 5ft < 10ft < 20ft
                        def depth_sort_key(col):
                            if col == 'timestamp': return -1
                            nums = re.findall(r'\d+', col)
                            return int(nums[0]) if nums else 999
                        
                        df_wide = df_wide.reindex(columns=sorted(df_wide.columns, key=depth_sort_key))

                        st.divider()
                        st.write(f"📊 **Preview:** {sel_pipe} ({len(df_wide)} rows)")
                        st.dataframe(df_wide.head(10), use_container_width=True)
                        
                        csv_bytes = df_wide.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label=f"💾 Download {sel_pipe} Wide CSV",
                            data=csv_bytes,
                            file_name=f"SensorConnect_{target_project}_{sel_pipe}.csv",
                            mime='text/csv'
                        )
                        
###############################
# --- END DATA INTAKE LAB --- #
###############################
#######################
# --- ADMIN TOOLS --- #
#######################             
elif service == "🛠️ Admin Tools":
    if check_admin_access():
        st.header("🛠️ Engineering Admin Tools")
    
        # 1. DEFINE TABS
        tab1, tab2, tab3 = st.tabs(["✅ Bulk Approval", "🧹 Deep Data Scrub", "🧨 Surgical Cleaner"])
    
        with tab1:
            st.subheader("✅ Bulk Approval")
            if st.button("🚀 Approve All Pending Data"):
                raw_tables = [f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush", f"{PROJECT_ID}.{DATASET_ID}.raw_lord"]
                for table in raw_tables:
                    client.query(f"UPDATE `{table}` SET approve = 'TRUE' WHERE approve IS NULL OR UPPER(CAST(approve AS STRING)) != 'FALSE'").result()
                st.success("Bulk approval complete.")
                st.cache_data.clear()

        with tab2:
            st.subheader("🧹 Deep Data Scrub & Final Purge")
            st.error("⚠️ WARNING: This permanently deletes data from RAW tables.")
            
            scrub_target = st.radio("Target Table", ["SensorPush", "Lord"], horizontal=True)
            target_table = f"{PROJECT_ID}.{DATASET_ID}.raw_sensorpush" if scrub_target == "SensorPush" else f"{PROJECT_ID}.{DATASET_ID}.raw_lord"
    
            if st.button(f"🧨 Permanently Purge & Dedup {scrub_target}"):
                with st.spinner("Executing hard delete and dedup..."):
                    scrub_sql = f"""
                    CREATE OR REPLACE TABLE `{target_table}` AS 
                    SELECT * EXCEPT(rn) FROM (
                        SELECT *, 
                               ROW_NUMBER() OVER(
                                   PARTITION BY NodeNum, TIMESTAMP_TRUNC(timestamp, HOUR) 
                                   ORDER BY timestamp DESC
                               ) as rn
                        FROM `{target_table}` 
                        WHERE (approve IS NULL OR UPPER(CAST(approve AS STRING)) != 'FALSE')
                        AND temperature IS NOT NULL
                    ) WHERE rn = 1
                    """
                    try:
                        client.query(scrub_sql).result()
                        st.success(f"{scrub_target} purged and deduped to 1-hour intervals.")
                        st.cache_data.clear()
                    except Exception as e:
                        st.error(f"Scrub Error: {e}")
            

        # 4. SURGICAL CLEANER (State-Locked Lasso)
        with tab3:
            st.subheader("🧨 Surgical Data Cleaner")
            if not selected_project:
                st.warning("👈 Please select a Project in the sidebar.")
            else:
                p_df = get_universal_portal_data(selected_project, only_approved=False)
        
                if not p_df.empty:
                    # Filter and RESET INDEX
                    loc_options = sorted(p_df['Location'].dropna().unique())
                    sel_loc = st.selectbox("Select Pipe", loc_options, key="admin_scrub_loc")
                    scrub_plot_df = p_df[p_df['Location'] == sel_loc].copy().reset_index(drop=True)

                    # --- THE STATE LOCK ---
                    # Use a 'confirmed' state that the widget cannot touch directly
                    if "locked_selection" not in st.session_state:
                        st.session_state.locked_selection = None

                    # 3. BUILD GRAPH
                    fig_scrub = build_high_speed_graph(
                        scrub_plot_df, f"Scrubbing: {sel_loc}", 
                        pd.Timestamp.now(tz='UTC') - timedelta(days=7), 
                        pd.Timestamp.now(tz='UTC') + timedelta(hours=6), 
                        tuple(active_refs), unit_mode, unit_label, display_tz=display_tz
                    )

                    # 4. FORCE DRAW THE LOCK
                    # If we have a locked selection, force Plotly to show it
                    if st.session_state.locked_selection:
                        selected_indices = [p['point_index'] for p in st.session_state.locked_selection]
                        fig_scrub.update_traces(
                            selectedpoints=selected_indices, 
                            unselected=dict(marker=dict(opacity=0.3))
                        )

                    # 5. RENDER CHART
                    # use_container_width=True is mandatory for your version
                    event_data = st.plotly_chart(
                        fig_scrub, 
                        use_container_width=True, 
                        on_select="rerun", 
                        key=f"scrub_chart_{sel_loc.replace(' ', '_')}"
                    )

                    # 6. LOCKING LOGIC (The One-Way Valve)
                    # We ONLY update the lock if there are actual points in the event.
                    # This prevents the 'reset' from wiping your selection.
                    if event_data and "selection" in event_data:
                        current_event_points = event_data["selection"].get("points", [])
                        if len(current_event_points) > 0:
                            st.session_state.locked_selection = current_event_points

                    # 7. UI FOR LOCKED SELECTION
                    if st.session_state.locked_selection:
                        points = st.session_state.locked_selection
                        st.success(f"📍 {len(points)} points locked in memory.")
                        
                        c1, c2 = st.columns(2)
                        if c1.button("🚨 HIDE SELECTED DATA", type="primary"):
                            rejection_records = []
                            for pt in points:
                                raw_ts = pd.to_datetime(pt['x'])
                                scrub_ts = raw_ts.tz_convert('UTC').floor('h')
                                node_id = scrub_plot_df.iloc[pt['point_index']]['NodeNum']
                                rejection_records.append({"NodeNum": str(node_id), "timestamp": scrub_ts, "reason": "Surgical Scrub", "Project": selected_project})
                            
                            if rejection_records:
                                rej_df = pd.DataFrame(rejection_records).drop_duplicates()
                                client.load_table_from_dataframe(rej_df, f"{PROJECT_ID}.{DATASET_ID}.manual_rejections").result()
                                st.session_state.locked_selection = None
                                st.cache_data.clear()
                                st.rerun()

                        if c2.button("🧹 Clear Selection"):
                            st.session_state.locked_selection = None
                            st.rerun()
                    else:
                        st.info("💡 Use the Lasso tool. Selection is now locked and will not disappear.")
                                
###########################
# --- END ADMIN TOOLS --- #
###########################
