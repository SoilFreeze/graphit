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

#######################
# - 1. CONFIGURATION -#
#######################
st.set_page_config(page_title="SoilFreeze Data Lab", layout="wide")

DATASET_ID = "Temperature" 
PROJECT_ID = "sensorpush-export"
MASTER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.master_data"
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

###########################
# - 2. DATA ENGINE LOGIC -#
###########################
@st.cache_data(ttl=600)
def get_universal_portal_data(project_id, view_mode="engineering"):
    """
    Standardizes data to UTC and handles rounded hourly scrubbing joins.
    view_mode "engineering": Sees Approved + Pending. Hides Deleted.
    view_mode "client": Sees ONLY Approved.
    """
    if view_mode == "client":
        approval_filter = "AND rej.status = 'TRUE'"
    else:
        approval_filter = "AND (rej.status IS NULL OR rej.status != 'FALSE')"

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
                rej.status as is_approved 
            FROM UnifiedRaw r
            INNER JOIN `{PROJECT_ID}.{DATASET_ID}.metadata` m ON r.NodeNum = m.NodeNum
            LEFT JOIN `{OVERRIDE_TABLE}` rej 
                ON r.NodeNum = rej.NodeNum 
                AND TIMESTAMP_TRUNC(TIMESTAMP_ADD(r.timestamp, INTERVAL 30 MINUTE), HOUR) = 
                    TIMESTAMP_TRUNC(TIMESTAMP_ADD(rej.timestamp, INTERVAL 30 MINUTE), HOUR)
        )
        SELECT * FROM JoinedData
        WHERE Project = '{project_id}'
        {approval_filter}
        AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 84 DAY)
        ORDER BY Location ASC, timestamp ASC
    """
    try:
        df = client.query(query).to_dataframe()
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
            if 'Bank' not in df.columns: df['Bank'] = ""
        return df
    except Exception as e:
        st.error(f"BigQuery Error: {e}")
        return pd.DataFrame()

def check_admin_access():
    if "admin_authenticated" not in st.session_state:
        st.session_state["admin_authenticated"] = False
    if st.session_state["admin_authenticated"]:
        return True
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

###########################
# - 3. GRAPHING ENGINE - #
###########################
def build_high_speed_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label, display_tz="UTC"):
    if df.empty: return go.Figure()
    plot_df = df.copy()
    plot_df['timestamp'] = plot_df['timestamp'].dt.tz_convert(display_tz)
    
    start_local = start_view.astimezone(pytz.timezone(display_tz))
    end_local = end_view.astimezone(pytz.timezone(display_tz))
    now_local = pd.Timestamp.now(tz=display_tz)

    if unit_mode == "Celsius":
        plot_df['temperature'] = (plot_df['temperature'] - 32) * 5/9
        y_range, dt_minor = [-30, 30], 2
    else:
        y_range, dt_minor = [-20, 80], 5

    plot_df['label'] = plot_df.apply(
        lambda r: f"Bank {r['Bank']} ({r['NodeNum']})" if str(r.get('Bank')).strip().lower() not in ["", "none", "nan", "null"]
        else f"{r.get('Depth')}ft ({r.get('NodeNum')})", axis=1
    )
    
    is_admin = any(x in title for x in ["Scrubbing", "Surgical", "Diag"])
    plot_mode = 'markers' if is_admin else 'lines'

    fig = go.Figure()
    for lbl in sorted(plot_df['label'].unique()):
        s_df = plot_df[plot_df['label'] == lbl].sort_values('timestamp')
        hover_name = lbl.split('(')[0].strip()
        fig.add_trace(go.Scattergl(
            x=s_df['timestamp'], y=s_df['temperature'], name=lbl, mode=plot_mode,
            marker=dict(size=7 if is_admin else 3, opacity=0.8),
            customdata=[hover_name] * len(s_df),
            hovertemplate=f"<b>%{{customdata}}</b>: %{{y:.1f}}{unit_label}<extra></extra>"
        ))

    # Grid Hierarchy
    grid_times = pd.date_range(start=start_local, end=end_local, freq='6h', tz=display_tz)
    for ts in grid_times:
        color, width = ("Black", 1.2) if (ts.weekday() == 0 and ts.hour == 0) else (("Gray", 0.8) if ts.hour == 0 else ("LightGray", 0.3))
        fig.add_vline(x=ts, line_width=width, line_color=color, layer='below')

    for val, ref_label in active_refs:
        c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
        fig.add_hline(y=c_val, line_dash="dash", line_color="RoyalBlue", annotation_text=ref_label)

    fig.update_layout(
        title={'text': f"{title} ({display_tz})", 'x': 0},
        plot_bgcolor='white', hovermode="x unified", height=600,
        xaxis=dict(range=[start_local, end_local], showline=True, linecolor='black', mirror=True, tickformat='%b %d\n%H:%M'),
        yaxis=dict(title=f"Temp ({unit_label})", range=y_range, dtick=dt_minor, gridcolor='Gainsboro', showline=True, linecolor='black', mirror=True),
        legend=dict(title="Sensors", orientation="v", x=1.02, y=1, xanchor="left")
    )
    return fig

###########################
# - 4. SIDEBAR SETTINGS - #
###########################
st.sidebar.title("❄️ SoilFreeze Lab")
service = st.sidebar.selectbox("📂 Select Page", ["🌐 Global Overview", "🏠 Executive Summary", "📊 Client Portal", "📉 Node Diagnostics", "📤 Data Intake Lab", "🛠️ Admin Tools"])
unit_mode = st.sidebar.radio("Temperature Unit", ["Fahrenheit", "Celsius"])
unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"

def convert_val(f_val):
    if f_val is None: return None
    return (f_val - 32) * 5/9 if unit_mode == "Celsius" else f_val

# Project Selector
proj_q = f"SELECT DISTINCT Project FROM `{PROJECT_ID}.{DATASET_ID}.metadata` WHERE Project IS NOT NULL"
try:
    proj_list = sorted(client.query(proj_q).to_dataframe()['Project'].dropna().unique())
    selected_project = st.sidebar.selectbox("🎯 Active Project", proj_list)
except:
    selected_project = None

st.sidebar.subheader("📏 Reference Lines")
active_refs = []
if st.sidebar.checkbox("Freezing (32°F)", value=True): active_refs.append((32.0, "Freezing"))
if st.sidebar.checkbox("Type B (26.6°F)", value=True): active_refs.append((26.6, "Type B"))
if st.sidebar.checkbox("Type A (10.2°F)", value=True): active_refs.append((10.2, "Type A"))

tz_mode = st.sidebar.selectbox("Timezone Display", ["UTC", "Local (US/Eastern)", "Local (US/Pacific)"])
display_tz = {"UTC": "UTC", "Local (US/Eastern)": "US/Eastern", "Local (US/Pacific)": "US/Pacific"}[tz_mode]

###########################
# - 5. GLOBAL OVERVIEW -  #
###########################
if service == "🌐 Global Overview":
    st.header("🌐 Global Overview")
    if selected_project:
        with st.spinner("Loading timelines..."):
            p_df = get_universal_portal_data(selected_project, view_mode="engineering")
        if not p_df.empty:
            lookback = st.sidebar.slider("Lookback (Weeks)", 1, 12, 4)
            end_view = (pd.Timestamp.now(tz='UTC') + pd.Timedelta(days=7)).replace(hour=0, minute=0, second=0)
            start_view = end_view - timedelta(weeks=lookback)
            for loc in sorted(p_df['Location'].unique()):
                with st.expander(f"📍 {loc}", expanded=True):
                    fig = build_high_speed_graph(p_df[p_df['Location'] == loc], f"📈 {selected_project} - {loc}", start_view, end_view, tuple(active_refs), unit_mode, unit_label, display_tz=display_tz)
                    st.plotly_chart(fig, use_container_width=True)

###########################
# - 6. EXEC SUMMARY -     #
###########################
elif service == "🏠 Executive Summary":
    st.header(f"🏠 Executive Summary: {selected_project if selected_project else 'All Projects'}")
    summary_q = f"""
        WITH RecentData AS (
            SELECT *,
                FIRST_VALUE(temperature) OVER(PARTITION BY NodeNum ORDER BY timestamp ASC) as first_temp_24h,
                ROW_NUMBER() OVER(PARTITION BY NodeNum ORDER BY timestamp DESC) as latest_rank
            FROM (
                SELECT NodeNum, timestamp, temperature, Project, Location, Bank, Depth FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
                UNION ALL
                SELECT NodeNum, timestamp, temperature, Project, Location, Bank, Depth FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
            )
            WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            {"AND Project = '" + selected_project + "'" if selected_project else ""}
        )
        SELECT * FROM RecentData WHERE latest_rank = 1
    """
    try:
        raw_summary_df = client.query(summary_q).to_dataframe()
        if not raw_summary_df.empty:
            now = pd.Timestamp.now(tz=pytz.UTC)
            # [Processing Logic for Delta, Min, Max, and Health Icons...]
            st.dataframe(raw_summary_df) # Placeholder for your specific styling logic
    except Exception as e: st.error(f"Summary Error: {e}")

###########################
# - 7. CLIENT PORTAL -    #
###########################
elif service == "📊 Client Portal":
    if not selected_project: st.sidebar.warning("Please select a project.")
    else:
        st.header(f"📊 Project Status: {selected_project}")
        p_df = get_universal_portal_data(selected_project, view_mode="client")
        if p_df.empty: st.info("No approved data for this project.")
        else:
            tab_time, tab_depth = st.tabs(["📈 Timeline", "📏 Depth Profile"])
            with tab_time:
                for loc in sorted(p_df['Location'].unique()):
                    fig = build_high_speed_graph(p_df[p_df['Location'] == loc], loc, pd.Timestamp.now(tz='UTC') - timedelta(weeks=4), pd.Timestamp.now(tz='UTC'), active_refs, unit_mode, unit_label, display_tz)
                    st.plotly_chart(fig, use_container_width=True)
            with tab_depth:
                st.info("Depth Profile logic processing...") # [Your existing Profile logic...]

###########################
# - 8. DATA INTAKE LAB -  #
###########################
elif service == "📤 Data Intake Lab":
    if check_admin_access():
        st.header("📤 Data Ingestion Lab")
        tab_upload, tab_export = st.tabs(["📄 Manual File Upload", "📥 Export Project Data"])
        
        with tab_upload:
            ###########
            # - Ingest -#
            ###########
            u_file = st.file_uploader("Upload CSV/Excel", type=['csv', 'xlsx', 'xls'])
            if u_file:
                # [Format Detection for Lord Wide/Narrow and SensorPush...]
                st.success("File Processed. Data sent to Raw tables.")

        with tab_export:
            ###########
            # - Export -#
            ###########
            st.subheader("📥 Export Wide Format")
            # [Pivot Logic for SensorConnect format...]

###########################
# - 9. ADMIN TOOLS -      #
###########################
elif service == "🛠️ Admin Tools":
    if check_admin_access():
        st.header("🛠️ Engineering Admin Tools")
        tab_bulk, tab_scrub, tab_surgical = st.tabs(["✅ Bulk Approval", "🧹 Deep Data Scrub", "🧨 Surgical Cleaner"])

        with tab_bulk:
            st.subheader("✅ Bulk Approval")
            if st.button(f"🚀 Approve All Pending for {selected_project}"):
                client.query(f"INSERT INTO `{OVERRIDE_TABLE}` (NodeNum, timestamp, status, Project) SELECT DISTINCT NodeNum, TIMESTAMP_TRUNC(timestamp, HOUR), 'TRUE', '{selected_project}' FROM (SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` UNION ALL SELECT NodeNum, timestamp FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`) WHERE Project = '{selected_project}'").result()
                st.success("Approved.")

        with tab_scrub:
            st.subheader("🧹 Deep Data Scrub (Averaging)")
            target = st.radio("Target", ["SensorPush", "Lord"], horizontal=True)
            t_tbl = f"{PROJECT_ID}.{DATASET_ID}.raw_{target.lower()}"
            if st.button("🧨 Purge & Average"):
                client.query(f"CREATE OR REPLACE TABLE `{t_tbl}` AS SELECT TIMESTAMP_TRUNC(TIMESTAMP_ADD(timestamp, INTERVAL 30 MINUTE), HOUR) as timestamp, NodeNum, AVG(temperature) as temperature FROM `{t_tbl}` GROUP BY 1, 2").result()
                st.success("Averaged.")

        with tab_surgical:
            ###########
            # - Cleaner -#
            ###########
            p_df = get_universal_portal_data(selected_project, view_mode="engineering")
            if not p_df.empty:
                sel_l = st.selectbox("Select Pipe", sorted(p_df['Location'].unique()))
                scrub_df = p_df[p_df['Location'] == sel_l].copy().reset_index(drop=True)
                # [Plotly Lasso and Action Buttons (Approve/Mask/Delete)...]
