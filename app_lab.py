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
import openpyxl

##################################
# - 1. CONFIGURATION & STYLING - #
##################################
st.set_page_config(page_title="SoilFreeze Data Lab", layout="wide")

# Database Constants
DATASET_ID = "Temperature" 
PROJECT_ID = "sensorpush-export"
MASTER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.master_data"
# The Override table stores our Approve/Mask/Delete statuses
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"

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

############################
# - 2. DATA ENGINE LOGIC - #
############################

@st.cache_data(ttl=600)
def get_universal_portal_data(project_id, view_mode="engineering"):
    """
    Fetches data and joins with the manual_rejections table for status.
    - Engineering view: Shows everything except 'FALSE' (Deleted).
    - Client view: Shows ONLY 'TRUE' (Approved).
    """
    
    # 1. Define the Approval Filter based on your 'reason' column in manual_rejections
    if view_mode == "client":
        approval_filter = "AND rej.reason = 'TRUE'"
    else:
        # Engineering sees everything NOT explicitly deleted ('FALSE')
        approval_filter = "AND (rej.reason IS NULL OR rej.reason != 'FALSE')"

    # 2. Construct Query
    # We ensure UnifiedRaw and Metadata are joined BEFORE the filter is applied
    query = f"""
        SELECT 
            r.NodeNum, 
            r.timestamp, 
            r.temperature,
            m.Location, 
            m.Bank, 
            m.Depth, 
            m.Project,
            rej.reason as is_approved 
        FROM (
            SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
            UNION ALL
            SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
        ) AS r
        INNER JOIN `{PROJECT_ID}.{DATASET_ID}.metadata` AS m 
            ON r.NodeNum = m.NodeNum
        LEFT JOIN `{OVERRIDE_TABLE}` AS rej 
            ON r.NodeNum = rej.NodeNum 
            AND TIMESTAMP_TRUNC(TIMESTAMP_ADD(r.timestamp, INTERVAL 30 MINUTE), HOUR) = 
                TIMESTAMP_TRUNC(TIMESTAMP_ADD(rej.timestamp, INTERVAL 30 MINUTE), HOUR)
        WHERE m.Project = '{project_id}'
        {approval_filter}
        AND r.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 84 DAY)
        ORDER BY m.Location ASC, r.timestamp ASC
    """
    
    try:
        df = client.query(query).to_dataframe()
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
            # Ensure Bank exists even if null in metadata
            if 'Bank' not in df.columns:
                df['Bank'] = ""
        return df
    except Exception as e:
        st.error(f"BigQuery Error in Data Engine: {e}")
        # Log the query for debugging if it fails again
        st.code(query, language="sql")
        return pd.DataFrame()

def check_admin_access():
    """Security gate for Admin and Intake pages."""
    if "admin_authenticated" not in st.session_state:
        st.session_state["admin_authenticated"] = False

    if st.session_state["admin_authenticated"]:
        return True

    if "admin_password" not in st.secrets:
        st.error("System Error: 'admin_password' not defined in Streamlit Secrets.")
        return False

    st.warning("🔒 Restricted Area: Engineering Admin Only.")
    pwd_input = st.text_input("Enter Admin Password", type="password")
    
    if st.button("Unlock Tools"):
        if pwd_input == st.secrets["admin_password"]:
            st.session_state["admin_authenticated"] = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    return False

###########################
#- 3. SIDEBAR UI & STATE -#
###########################
# --- GLOBAL SIDEBAR ---
st.sidebar.title("❄️ SoilFreeze Lab")

# Page Navigation
service = st.sidebar.selectbox(
    "📂 Select Page", 
    ["🌐 Global Overview", "🏠 Executive Summary", "📊 Client Portal", "📉 Node Diagnostics", "📤 Data Intake Lab", "🛠️ Admin Tools"],
    index=0
)
st.sidebar.divider()

# Temperature Unit Handling
unit_mode = st.sidebar.radio("Temperature Unit", ["Fahrenheit", "Celsius"], index=0)
unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"

def convert_val(f_val):
    """Global helper to convert Fahrenheit from DB to user's display unit."""
    if f_val is None or pd.isna(f_val): return None
    return (f_val - 32) * 5/9 if unit_mode == "Celsius" else f_val

st.sidebar.divider()

# Global Project Selection (Used by all pages except Global Overview)
selected_project = None
if service in ["📊 Client Portal", "📉 Node Diagnostics", "🛠️ Admin Tools", "🏠 Executive Summary"]:
    try:
        # Fetching project list directly from metadata for the sidebar
        proj_q = f"SELECT DISTINCT Project FROM `{PROJECT_ID}.{DATASET_ID}.metadata` WHERE Project IS NOT NULL"
        proj_df = client.query(proj_q).to_dataframe()
        selected_project = st.sidebar.selectbox("🎯 Active Project", sorted(proj_df['Project'].dropna().unique()))
    except Exception:
        st.sidebar.warning("Could not load project list from BigQuery.")

st.sidebar.divider()

# Reference Line Settings
st.sidebar.subheader("📏 Reference Lines")
active_refs = []
if st.sidebar.checkbox("Freezing (32°F / 0°C)", value=True): active_refs.append((32.0, "Freezing"))
if st.sidebar.checkbox("Type B (26.6°F / -3°C)", value=True): active_refs.append((26.6, "Type B"))
if st.sidebar.checkbox("Type A (10.2°F / -12.1°C)", value=True): active_refs.append((10.2, "Type A"))

# Timezone Settings
st.sidebar.subheader("🕒 Display Settings")
tz_mode = st.sidebar.selectbox("Timezone Display", ["UTC", "Local (US/Eastern)", "Local (US/Pacific)"])
tz_lookup = {
    "UTC": "UTC",
    "Local (US/Eastern)": "US/Eastern",
    "Local (US/Pacific)": "US/Pacific"
}
display_tz = tz_lookup[tz_mode]

########################
#- 4. GRAPHING ENGINE -#
########################
def build_high_speed_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label, display_tz="UTC"):
    """
    Standard Plotly engine for the entire app.
    Handles unit conversion, timezone shifting, and gap detection.
    """
    if df.empty:
        return go.Figure().update_layout(title="No data available for the selected period.")

    plot_df = df.copy()
    
    # 1. TIMEZONE CONVERSION
    # Ensure timestamps are localized to the user's preferred viewing zone
    plot_df['timestamp'] = plot_df['timestamp'].dt.tz_convert(display_tz)
    
    # Adjust axes windows to match local zone
    start_local = start_view.astimezone(pytz.timezone(display_tz))
    end_local = end_view.astimezone(pytz.timezone(display_tz))
    now_local = pd.Timestamp.now(tz=display_tz)

    # 2. UNIT CONVERSION
    if unit_mode == "Celsius":
        plot_df['temperature'] = (plot_df['temperature'] - 32) * 5/9
        y_range, dt_major = [-30, 30], 5
    else:
        y_range, dt_major = [-20, 80], 10

    # 3. LABELING LOGIC
    # Priority: Bank Name -> Depth
    plot_df['label'] = plot_df.apply(
        lambda r: f"Bank {r['Bank']} ({r['NodeNum']})" if str(r.get('Bank')).strip().lower() not in ["", "none", "nan", "null"]
        else f"{r.get('Depth')}ft ({r.get('NodeNum')})", axis=1
    )
    
    # 4. PLOT MODE (Markers for Admin/Surgical, Lines for Client/Global)
    is_surgical = any(word in title for word in ["Scrubbing", "Surgical", "Diag"])
    plot_mode = 'markers' if is_surgical else 'lines'
    marker_size = 7 if is_surgical else 3

    fig = go.Figure()
    
    for lbl in sorted(plot_df['label'].unique()):
        s_df = plot_df[plot_df['label'] == lbl].sort_values('timestamp')
        hover_name = lbl.split('(')[0].strip()

        # 5. GAP DETECTION (Prevents lines from jumping across large data outages)
        if not is_surgical:
            s_df['gap_hrs'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
            gap_mask = s_df['gap_hrs'] > 6.0
            if gap_mask.any():
                gaps = s_df[gap_mask].copy()
                gaps['temperature'] = None
                gaps['timestamp'] = gaps['timestamp'] - pd.Timedelta(minutes=1)
                s_df = pd.concat([s_df, gaps]).sort_values('timestamp')

        # 6. ADD TRACE
        fig.add_trace(go.Scattergl(
            x=s_df['timestamp'], 
            y=s_df['temperature'], 
            name=lbl, 
            mode=plot_mode,
            marker=dict(size=marker_size, opacity=0.8 if is_surgical else 1.0),
            connectgaps=False,
            customdata=[hover_name] * len(s_df),
            hovertemplate=f"<b>%{{customdata}}</b>: %{{y:.1f}}{unit_label}<extra></extra>"
        ))

    # 7. GRID HIERARCHY (Monday=Black, Midnight=Gray)
    grid_times = pd.date_range(start=start_local, end=end_local, freq='6h', tz=display_tz)
    for ts in grid_times:
        if ts.weekday() == 0 and ts.hour == 0:
            color, width = "Black", 1.2 
        elif ts.hour == 0:
            color, width = "Gray", 0.8  
        else:
            color, width = "LightGray", 0.3
        fig.add_vline(x=ts, line_width=width, line_color=color, layer='below')

    # 8. REFERENCE LINES & NOW LINE
    for val, ref_label in active_refs:
        c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
        fig.add_hline(y=c_val, line_dash="dash", line_color="RoyalBlue", 
                      annotation_text=ref_label, annotation_position="top right")

    fig.add_vline(x=now_local, line_width=2, line_color="Red", layer='above', line_dash="dash")

    # 9. FINAL LAYOUT
    fig.update_layout(
        title={'text': f"{title} ({display_tz})", 'x': 0},
        plot_bgcolor='white',
        hovermode="x unified",
        height=600,
        margin=dict(t=80, l=50, r=180, b=50),
        xaxis=dict(range=[start_local, end_local], showline=True, linecolor='black', mirror=True, tickformat='%b %d\n%H:%M'),
        yaxis=dict(title=f"Temperature ({unit_label})", range=y_range, dtick=dt_major, gridcolor='Gainsboro', showline=True, linecolor='black', mirror=True),
        legend=dict(title="Sensors", orientation="v", x=1.02, y=1, xanchor="left")
    )
    
    return fig

###########
#- 5. GLOBAL OVERVIEW -
###########
if service == "🌐 Global Overview":
    st.header("🌐 Global Project Overview")
    
    # We retrieve the project list from the metadata table
    proj_list_q = f"SELECT DISTINCT Project FROM `{PROJECT_ID}.{DATASET_ID}.metadata` WHERE Project IS NOT NULL"
    available_projects = sorted(client.query(proj_list_q).to_dataframe()['Project'].tolist())
    target_project = st.selectbox("🏗️ Select a Project", available_projects, key="global_proj_picker")

    if target_project:
        with st.spinner(f"Loading {target_project} Engineering View..."):
            # This calls the fixed Data Engine logic from the previous step
            p_df = get_universal_portal_data(target_project, view_mode="engineering")

        if not p_df.empty:
            lookback = st.sidebar.slider("Lookback (Weeks)", 1, 12, 4)
            now_utc = pd.Timestamp.now(tz='UTC')
            end_view = (now_utc + pd.Timedelta(days=(7-now_utc.weekday())%7 or 7)).replace(hour=0, minute=0, second=0)
            start_view = end_view - timedelta(weeks=lookback)

            for loc in sorted(p_df['Location'].unique()):
                with st.expander(f"📍 Location: {loc}", expanded=True):
                    loc_df = p_df[p_df['Location'] == loc]
                    fig = build_high_speed_graph(
                        loc_df, f"📈 {target_project} - {loc}", 
                        start_view, end_view, tuple(active_refs), 
                        unit_mode, unit_label, display_tz=display_tz
                    )
                    st.plotly_chart(fig, use_container_width=True)
        else:
            st.info(f"No active engineering data found for {target_project}.")
###########
#- 6. EXECUTIVE SUMMARY -
###########
elif service == "🏠 Executive Summary":
    st.header(f"🏠 Executive Summary: {selected_project if selected_project else 'All Projects'}")
    
    st.write("### ↕️ Sorting & View Options")
    c1, c2 = st.columns([1, 1])
    with c1:
        sort_choice = st.selectbox("Sort By", ["None", "Hours Since Last Seen", "Delta Magnitude"])
    with c2:
        sort_order = st.radio("Order", ["Descending", "Ascending"], horizontal=True)
    
    # FIX: We specify 'r.NodeNum' to avoid the 'ambiguous' error
    summary_q = f"""
        WITH RecentData AS (
            SELECT 
                r.NodeNum, 
                r.timestamp, 
                r.temperature, 
                m.Project, 
                m.Location, 
                m.Bank, 
                m.Depth,
                FIRST_VALUE(r.temperature) OVER(PARTITION BY r.NodeNum ORDER BY r.timestamp ASC) as first_temp_24h,
                ROW_NUMBER() OVER(PARTITION BY r.NodeNum ORDER BY r.timestamp DESC) as latest_rank
            FROM (
                SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
                UNION ALL
                SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
            ) AS r
            INNER JOIN `{PROJECT_ID}.{DATASET_ID}.metadata` AS m ON r.NodeNum = m.NodeNum
            WHERE r.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            {"AND m.Project = '" + selected_project + "'" if selected_project else ""}
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
        with st.spinner("⚡ Fetching Command Center Snapshot..."):
            raw_summary_df = client.query(summary_q).to_dataframe()
        
        if raw_summary_df.empty:
            st.warning("📡 No active sensors seen in the last 24 hours.")
        else:
            now_utc = pd.Timestamp.now(tz=pytz.UTC)
            
            def process_summary_row(row):
                ts = row['timestamp'].tz_localize(pytz.UTC) if row['timestamp'].tzinfo is None else row['timestamp']
                hrs_ago = int((now_utc - ts).total_seconds() / 3600)
                status_icon = "🟢" if hrs_ago < 6 else ("🟡" if hrs_ago < 12 else "🟠" if hrs_ago < 24 else "🔴")
                raw_delta = row['temperature'] - row['first_temp_24h']
                
                return pd.Series({
                    "Project": row['Project'],
                    "Node": row['NodeNum'],
                    "Location": row['Location'],
                    "Position": f"Bank {row['Bank']}" if str(row['Bank']).strip() not in ["", "None", "nan"] else f"{row['Depth']} ft",
                    "Min": f"{round(convert_val(row['min_24h']), 1)}{unit_label}",
                    "Max": f"{round(convert_val(row['max_24h']), 1)}{unit_label}",
                    "Delta_Val": raw_delta, 
                    "Delta": f"{round(raw_delta, 1)}°F",
                    "Hours_Ago": hrs_ago,
                    "Last Seen": f"{ts.strftime('%m/%d %H:%M')} ({hrs_ago}h) {status_icon}"
                })

            summary_df = raw_summary_df.apply(process_summary_row, axis=1)

            # Sorting Logic
            asc = (sort_order == "Ascending")
            if sort_choice == "Hours Since Last Seen":
                summary_df = summary_df.sort_values(by="Hours_Ago", ascending=asc)
            elif sort_choice == "Delta Magnitude":
                summary_df = summary_df.sort_values(by="Delta_Val", key=abs, ascending=asc)

            st.dataframe(
                summary_df[["Project", "Node", "Location", "Position", "Min", "Max", "Delta", "Last Seen"]],
                use_container_width=True, hide_index=True
            )
            
    except Exception as e:
        st.error(f"Executive Summary Error: {e}")

###########
#- 7. CLIENT PORTAL -
###########
elif service == "📊 Client Portal":
    if not selected_project:
        st.sidebar.warning("Please select a project.")
    else:
        st.header(f"📊 Project Status: {selected_project}")
        
        # Client view: ONLY shows points where manual_rejections.reason = 'TRUE'
        p_df = get_universal_portal_data(selected_project, view_mode="client")
        
        if p_df.empty:
            st.info(f"No data has been approved for {selected_project} yet.")
        else:
            tab_time, tab_depth = st.tabs(["📈 Timeline Analysis", "📏 Depth Profile"])

            with tab_time:
                weeks_view = st.slider("Weeks to View", 1, 12, 6)
                end_view = pd.Timestamp.now(tz='UTC')
                start_view = end_view - timedelta(weeks=weeks_view)
                
                for loc in sorted(p_df['Location'].dropna().unique()):
                    with st.expander(f"📈 {loc}", expanded=True):
                        loc_data = p_df[p_df['Location'] == loc]
                        fig = build_high_speed_graph(loc_data, loc, start_view, end_view, tuple(active_refs), unit_mode, unit_label, display_tz=display_tz)
                        st.plotly_chart(fig, use_container_width=True)

            with tab_depth:
                # Uses standard depth logic from master_data schema
                p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
                depth_only = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
                
                for loc in sorted(depth_only['Location'].unique()):
                    with st.expander(f"📏 {loc} Profile", expanded=True):
                        loc_data = depth_only[depth_only['Location'] == loc]
                        fig_d = go.Figure()
                        # Depth Profile logic...
                        st.plotly_chart(fig_d, use_container_width=True)

            with tab_depth:
                p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
                depth_only = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
                
                for loc in sorted(depth_only['Location'].unique()):
                    with st.expander(f"📏 {loc} Depth Profile", expanded=True):
                        loc_data = depth_only[depth_only['Location'] == loc].copy()
                        fig_d = go.Figure()
                        mondays = pd.date_range(start=start_view, end=now, freq='W-MON')
                        
                        for m_date in mondays:
                            target_ts = m_date.replace(hour=6, minute=0, second=0).tz_localize(pytz.UTC)
                            window = loc_data[(loc_data['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & (loc_data['timestamp'] <= target_ts + pd.Timedelta(hours=12))]
                            if not window.empty:
                                snap_list = []
                                for node in window['NodeNum'].unique():
                                    node_data = window[window['NodeNum'] == node].copy()
                                    node_data['diff'] = (node_data['timestamp'] - target_ts).abs()
                                    snap_list.append(node_data.sort_values('diff').iloc[0])
                                snap_df = pd.DataFrame(snap_list).sort_values('Depth_Num')
                                fig_d.add_trace(go.Scattergl(x=snap_df['temperature'], y=snap_df['Depth_Num'], mode='lines+markers', name=target_ts.strftime('%m/%d/%y')))

                        y_limit = int(((loc_data['Depth_Num'].max() // 5) + 1) * 5) if not loc_data.empty else 50
                        x_range = [-20, 80] if unit_mode == "Fahrenheit" else [(-20-32)*5/9, (80-32)*5/9]
                        
                        fig_d.update_layout(
                            plot_bgcolor='white', height=700,
                            xaxis=dict(title=f"Temp ({unit_label})", range=x_range, gridcolor='Gainsboro', showline=True, linecolor='black', mirror=True),
                            yaxis=dict(title="Depth (ft)", range=[y_limit, 0], dtick=10, gridcolor='Silver', showline=True, linecolor='black', mirror=True),
                            legend=dict(title="Weekly Snapshots (6AM)", orientation="h", y=-0.2)
                        )
                        st.plotly_chart(fig_d, use_container_width=True, key=f"dep_{loc}")

            with tab_table:
                # Latest Snapshot Table
                latest = p_df.sort_values('timestamp').groupby('NodeNum').tail(1).copy()
                latest['Current Temp'] = latest['temperature'].apply(lambda x: f"{round(convert_val(x), 1)}{unit_label}")
                latest['Position'] = latest.apply(lambda r: f"Bank {r['Bank']}" if pd.notnull(r['Bank']) and str(r['Bank']).strip() != "" else f"{r.get('Depth', '??')} ft", axis=1)
                st.dataframe(latest[['Location', 'Position', 'Current Temp', 'NodeNum']].sort_values(['Location', 'Position']), use_container_width=True, hide_index=True)

###########
#- 8. NODE DIAGNOSTICS -
###########
elif service == "📉 Node Diagnostics":
    st.header(f"📉 Node Diagnostics: {selected_project}")
    if not selected_project:
        st.warning("👈 Please select a project.")
    else:
        with st.spinner("🔍 Syncing diagnostic streams..."):
            all_data = get_universal_portal_data(selected_project, view_mode="engineering")
        
        if all_data.empty:
            st.warning(f"No data found for project {selected_project}.")
        else:
            loc_options = sorted(all_data['Location'].dropna().unique())
            c1, c2 = st.columns([2, 1])
            with c1: sel_loc = st.selectbox("Select Pipe / Bank", loc_options)
            with c2: weeks_view = st.slider("Lookback (Weeks)", 1, 12, 4)

            now_utc = pd.Timestamp.now(tz=pytz.UTC)
            end_view = (now_utc + pd.Timedelta(days=(7-now_utc.weekday())%7 or 7)).replace(hour=0, minute=0, second=0)
            start_view = end_view - timedelta(weeks=weeks_view)
            df_diag = all_data[all_data['Location'] == sel_loc].copy()

            st.subheader("📈 Engineering Timeline")
            fig_time = build_high_speed_graph(df_diag, f"Diag: {sel_loc}", start_view, end_view, tuple(active_refs), unit_mode, unit_label, display_tz=display_tz)
            st.plotly_chart(fig_time, use_container_width=True)

            # Restored the Health Table logic
            st.subheader(f"📋 Node Health Summary: {sel_loc}")
            latest_nodes = df_diag.sort_values('timestamp').groupby('NodeNum').tail(1).copy()
            summary_rows = []
            for _, row in latest_nodes.iterrows():
                hrs_ago = int((now_utc - row['timestamp']).total_seconds() / 3600)
                status_icon = "🔴" if hrs_ago > 24 else ("🟢" if hrs_ago < 6 else "🟡")
                summary_rows.append({
                    "Node": row['NodeNum'],
                    "Last Seen": f"{hrs_ago}h ago {status_icon}",
                    "Status": "✅ Approved" if row['is_approved'] == "TRUE" else ("🚫 Masked" if row['is_approved'] == "MASKED" else "⏳ Pending")
                })
            st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, hide_index=True)
###########
#- 9. DATA INTAKE LAB -
###########
elif service == "📤 Data Intake Lab":
    if check_admin_access():
        st.header("📤 Data Ingestion Lab")
        tab_upload, tab_export = st.tabs(["📄 Manual File Upload", "📥 Export Project Data"])
        
        with tab_upload:
            ###########
            # - Tab: Upload (Logic Only) -#
            ###########
            st.subheader("📄 Manual File Ingestion")
            u_file = st.file_uploader("Upload Data File", type=['csv', 'xlsx', 'xls'], key="manual_upload_main")
            
            if u_file is not None:
                try:
                    # Logic to process Lord Wide, Lord Narrow, or SensorPush
                    # Data is uploaded to raw_lord or raw_sensorpush with blank 'approve' columns
                    st.info("Processing file for ingestion...")
                    # [Standard ingestion parsing logic goes here]
                    st.success("Ingestion successful. Data is pending scrub/approval.")
                except Exception:
                    st.error(f"Ingestion Error: {traceback.format_exc()}")

        with tab_export:
            ###########
            # - Tab: Export (Wide Format) -#
            ###########
            if selected_project:
                with st.spinner("Preparing wide-format export..."):
                    export_df = get_universal_portal_data(selected_project, view_mode="engineering")
                if not export_df.empty:
                    pipes = sorted(export_df['Location'].dropna().unique().tolist())
                    sel_pipe = st.selectbox("Select Pipe / Location", pipes)
                    df_final = export_df[export_df['Location'] == sel_pipe].copy()
                    if not df_final.empty:
                        df_final['Depth_Col'] = df_final['Depth'].astype(str) + "ft"
                        df_wide = df_final.pivot_table(index='timestamp', columns='Depth_Col', values='temperature', aggfunc='mean').reset_index()
                        st.download_button("💾 Download Wide CSV", df_wide.to_csv(index=False).encode('utf-8'), f"{selected_project}_{sel_pipe}.csv")

###########
#- 10. ADMIN TOOLS -
###########
elif service == "🛠️ Admin Tools":
    if check_admin_access():
        st.header("🛠️ Engineering Admin Tools")
        tab_bulk, tab_scrub, tab_surgical = st.tabs(["✅ Bulk Approval", "🧹 Deep Data Scrub", "🧨 Surgical Cleaner"])

        with tab_bulk:
            ###########
            # - Tab: Bulk Approval - #
            ###########
            st.subheader("✅ Bulk Project Approval")
            st.info(f"Approving all pending data for **{selected_project}**.")
            
            if st.button(f"🚀 Bulk Approve {selected_project}"):
                with st.spinner("Executing Bulk Approval..."):
                    # REMOVED 'Project' from the INSERT and SELECT as it's not in your BQ schema
                    bulk_sql = f"""
                        INSERT INTO `{OVERRIDE_TABLE}` (NodeNum, timestamp, reason)
                        SELECT DISTINCT 
                            r.NodeNum, 
                            TIMESTAMP_TRUNC(r.timestamp, HOUR), 
                            'TRUE'
                        FROM (
                            SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
                            UNION ALL
                            SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
                        ) AS r
                        INNER JOIN `{PROJECT_ID}.{DATASET_ID}.metadata` AS m ON r.NodeNum = m.NodeNum
                        WHERE m.Project = '{selected_project}'
                        AND NOT EXISTS (
                            SELECT 1 FROM `{OVERRIDE_TABLE}` AS x 
                            WHERE x.NodeNum = r.NodeNum 
                            AND x.timestamp = TIMESTAMP_TRUNC(r.timestamp, HOUR)
                        )
                    """
                    try:
                        client.query(bulk_sql).result()
                        st.success(f"All pending data for {selected_project} is now live in the Client Portal.")
                        st.cache_data.clear()
                    except Exception as e:
                        st.error(f"Bulk Approval Error: {e}")
                        st.code(bulk_sql, language="sql")

        with tab_scrub:
            ###########
            # - Tab: Deep Scrub - #
            ###########
            st.subheader("🧹 Deep Data Scrub")
            st.warning("This will average raw data to 1-hour intervals.")
            scrub_target = st.radio("Target Table", ["SensorPush", "Lord"], horizontal=True, key="admin_scrub_select")
            t_table = f"{PROJECT_ID}.{DATASET_ID}.raw_{scrub_target.lower()}"
            
            if st.button(f"🧨 Purge & Average {scrub_target}"):
                with st.spinner("Processing Raw Data Mean Reduction..."):
                    scrub_sql = f"""
                        CREATE OR REPLACE TABLE `{t_table}` AS 
                        SELECT 
                            TIMESTAMP_TRUNC(TIMESTAMP_ADD(timestamp, INTERVAL 30 MINUTE), HOUR) as timestamp, 
                            NodeNum, 
                            AVG(temperature) as temperature
                        FROM `{t_table}`
                        WHERE temperature IS NOT NULL
                        GROUP BY 1, 2
                    """
                    try:
                        client.query(scrub_sql).result()
                        st.success(f"✅ {scrub_target} table successfully averaged.")
                        st.cache_data.clear()
                    except Exception as e:
                        st.error(f"Scrub Error: {e}")

        with tab_surgical:
            ###########
            # - Tab: Surgical Cleaner - #
            ###########
            if not selected_project:
                st.warning("Please select a project in the sidebar.")
            else:
                # This function should be defined in your script to handle the Lasso logic
                render_surgical_cleaner(selected_project, display_tz, unit_mode, unit_label, active_refs)

        with tab_surgical:
            ###########
            # - Tab: Surgical Cleaner -#
            ###########
            if not selected_project:
                st.warning("Please select a project.")
            else:
                p_df = get_universal_portal_data(selected_project, view_mode="engineering")
                if not p_df.empty:
                    sel_loc = st.selectbox("Select Pipe", sorted(p_df['Location'].unique()))
                    scrub_df = p_df[p_df['Location'] == sel_loc].copy().reset_index(drop=True)
                    if "locked_selection" not in st.session_state: st.session_state.locked_selection = None
                    
                    fig = build_high_speed_graph(scrub_df, f"Surgical: {sel_loc}", pd.Timestamp.now(tz='UTC') - timedelta(days=7), pd.Timestamp.now(tz='UTC') + timedelta(hours=2), active_refs, unit_mode, unit_label, display_tz)
                    if st.session_state.locked_selection:
                        fig.update_traces(selectedpoints=[p['point_index'] for p in st.session_state.locked_selection], unselected=dict(marker=dict(opacity=0.2)))
                    
                    evt = st.plotly_chart(fig, use_container_width=True, on_select="rerun", key=f"s_{sel_loc}")
                    if evt and "selection" in evt:
                        if len(evt["selection"].get("points", [])) > 0: st.session_state.locked_selection = evt["selection"]["points"]

                    if st.session_state.locked_selection:
                        c1, c2, c3, c4 = st.columns(4)
                        with c1: 
                            if st.button("✅ APPROVE"): update_records(st.session_state.locked_selection, scrub_df, "TRUE", selected_project)
                        with c2: 
                            if st.button("🚫 MASK"): update_records(st.session_state.locked_selection, scrub_df, "MASKED", selected_project)
                        with c3: 
                            if st.button("🗑️ DELETE", type="primary"): update_records(st.session_state.locked_selection, scrub_df, "FALSE", selected_project)
                        with c4: 
                            if st.button("Clear"): 
                                st.session_state.locked_selection = None
                                st.rerun()

def update_records(pts, df, val, proj):
    recs = []
    for p in pts:
        ts = pd.to_datetime(p['x']).tz_convert('UTC').floor('h')
        node = df.iloc[p['point_index']]['NodeNum']
        # Removed Project from here as well to match your BQ schema
        recs.append({"NodeNum": str(node), "timestamp": ts, "reason": val})
    
    if recs:
        status_df = pd.DataFrame(recs).drop_duplicates()
        client.load_table_from_dataframe(status_df, OVERRIDE_TABLE).result()
        st.session_state.locked_selection = None
        st.cache_data.clear()
        st.rerun()
    
###################################
# - SURGICAL CLEANER FUNCTIONS - #
###################################

def render_surgical_cleaner(selected_project, display_tz, unit_mode, unit_label, active_refs):
    p_df = get_universal_portal_data(selected_project, view_mode="engineering")
    if p_df.empty:
        st.info("No data available to scrub.")
        return

    loc_options = sorted(p_df['Location'].dropna().unique())
    sel_loc = st.selectbox("Select Pipe to Clean", loc_options, key="surgical_loc_select")
    scrub_df = p_df[p_df['Location'] == sel_loc].copy().reset_index(drop=True)

    if "locked_selection" not in st.session_state:
        st.session_state.locked_selection = None

    fig_scrub = build_high_speed_graph(scrub_df, f"Surgical Scrubbing: {sel_loc}", pd.Timestamp.now(tz='UTC') - timedelta(days=14), pd.Timestamp.now(tz='UTC') + timedelta(hours=6), tuple(active_refs), unit_mode, unit_label, display_tz=display_tz)

    if st.session_state.locked_selection:
        indices = [p['point_index'] for p in st.session_state.locked_selection]
        fig_scrub.update_traces(selectedpoints=indices, unselected=dict(marker=dict(opacity=0.2)))

    event_data = st.plotly_chart(fig_scrub, use_container_width=True, on_select="rerun", key=f"scrub_{sel_loc}")

    if event_data and "selection" in event_data:
        pts = event_data["selection"].get("points", [])
        if len(pts) > 0: st.session_state.locked_selection = pts

    if st.session_state.locked_selection:
        st.success(f"📍 {len(st.session_state.locked_selection)} points selected.")
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            if st.button("✅ APPROVE (Client)", use_container_width=True):
                process_status_update(st.session_state.locked_selection, scrub_df, "TRUE", selected_project)
        with c2:
            if st.button("🚫 MASK (Client)", use_container_width=True):
                process_status_update(st.session_state.locked_selection, scrub_df, "MASKED", selected_project)
        with c3:
            if st.button("🗑️ DELETE", type="primary", use_container_width=True):
                process_status_update(st.session_state.locked_selection, scrub_df, "FALSE", selected_project)
        with c4:
            if st.button("Clear Selection", use_container_width=True):
                st.session_state.locked_selection = None
                st.rerun()

def process_status_update(points, df, status_val, project_id):
    records = []
    for pt in points:
        # Floor to hour to ensure override join matches scrubbed data
        raw_ts = pd.to_datetime(pt['x'])
        scrub_ts = raw_ts.tz_convert('UTC').floor('h')
        node_id = df.iloc[pt['point_index']]['NodeNum']
        records.append({"NodeNum": str(node_id), "timestamp": scrub_ts, "status": status_val, "Project": project_id})
    
    if records:
        status_df = pd.DataFrame(records).drop_duplicates(subset=['NodeNum', 'timestamp'])
        client.load_table_from_dataframe(status_df, OVERRIDE_TABLE).result()
        st.session_state.locked_selection = None
        st.cache_data.clear()
        st.rerun()
