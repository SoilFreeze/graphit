import streamlit as st
import pd
import pandas as pd
import time
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz
import io

#########################
# --- CONFIGURATION --- #
#########################
st.set_page_config(page_title="SoilFreeze Data Lab", layout="wide")

DATASET_ID = "Temperature" 
PROJECT_ID = "sensorpush-export"
MASTER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.master_data"

@st.cache_resource
def get_bq_client():
    """Handles authentication with BigQuery."""
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
# --- GLOBAL MEMORY --- #
###########################
# Initialize session state to prevent KeyErrors
if "master_df" not in st.session_state:
    st.session_state.master_df = pd.DataFrame()
    st.session_state.summary_df = pd.DataFrame()
    st.session_state.current_project = None
    st.session_state.last_refresh = None

#######################
# --- SIDEBAR UI --- #
#######################
st.sidebar.title("❄️ SoilFreeze Lab")

service = st.sidebar.selectbox("📂 Select Page", ["🏠 Executive Summary", "📊 Client Portal", "📉 Node Diagnostics", "📤 Data Intake Lab", "🛠️ Admin Tools"])
st.sidebar.divider()

unit_mode = st.sidebar.radio("Temperature Unit", ["Fahrenheit", "Celsius"], index=0)
unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"

def convert_val(f_val):
    if f_val is None: return None
    return (f_val - 32) * 5/9 if unit_mode == "Celsius" else f_val

# Project Selection
selected_project = None
if service in ["🏠 Executive Summary", "📊 Client Portal", "📉 Node Diagnostics", "🛠️ Admin Tools"]:
    try:
        proj_q = f"SELECT DISTINCT Project FROM `{MASTER_TABLE}` WHERE Project IS NOT NULL"
        proj_df = client.query(proj_q).to_dataframe()
        selected_project = st.sidebar.selectbox("🎯 Active Project", sorted(proj_df['Project'].dropna().unique()))
    except: 
        st.sidebar.warning("No projects found.")

st.sidebar.divider()
st.sidebar.write("### 📏 Reference Lines")
active_refs = []
if st.sidebar.checkbox("Freezing (32°F / 0°C)", value=True): active_refs.append((32.0, "Freezing"))
if st.sidebar.checkbox("Type B (26.6°F / -3°C)", value=True): active_refs.append((26.6, "Type B"))
if st.sidebar.checkbox("Type A (10.2°F / -12.1°C)", value=True): active_refs.append((10.2, "Type A"))

# Global Sync Button
if st.sidebar.button("🔄 Sync New Data Now", key="global_sync_btn"):
    st.session_state.master_df = pd.DataFrame()
    st.session_state.summary_df = pd.DataFrame()
    st.session_state.current_project = None
    st.rerun()

##############################
# --- DATA SYNC ENGINE --- #
##############################
# A. Sync Global Summary (For Executive Summary)
if st.session_state.summary_df.empty:
    with st.spinner("📡 Syncing Command Center..."):
        summary_q = f"SELECT * FROM `{MASTER_TABLE}` QUALIFY ROW_NUMBER() OVER(PARTITION BY NodeNum ORDER BY timestamp DESC) = 1"
        st.session_state.summary_df = client.query(summary_q).to_dataframe()

# B. Sync Project Detail History (Loads 90 days once per project)
if selected_project and st.session_state.current_project != selected_project:
    with st.spinner(f"⚡ Loading Cache for {selected_project}..."):
        detail_q = f"""
            SELECT timestamp, temperature, Depth, Location, Bank, NodeNum, approve, Project
            FROM `{MASTER_TABLE}`
            WHERE Project = '{selected_project}'
            AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
            ORDER BY timestamp ASC
        """
        df = client.query(detail_q).to_dataframe()
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_convert(pytz.UTC) if df['timestamp'].dt.tz else pd.to_datetime(df['timestamp']).dt.tz_localize(pytz.UTC)
            df['Depth_Num'] = pd.to_numeric(df['Depth'], errors='coerce')
            df['is_approved'] = df['approve'].astype(str).str.upper().str.strip() == 'TRUE'
            
            st.session_state.master_df = df
            st.session_state.current_project = selected_project
            st.session_state.last_refresh = datetime.now().strftime("%H:%M:%S")

# Global References for the rest of the app
master_df = st.session_state.master_df
summary_df = st.session_state.summary_df
approved_df = master_df[master_df['is_approved'] == True] if not master_df.empty else pd.DataFrame()

if st.session_state.get("last_refresh"):
    st.sidebar.caption(f"Last Project Sync: {st.session_state.last_refresh}")

############################
# --- GRAPHING ENGINE --- #
############################
def build_standard_sf_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label):
    try:
        display_df = df.copy()
        if display_df.empty: return go.Figure()

        display_df['timestamp'] = pd.to_datetime(display_df['timestamp'])
        
        # Unit Logic
        if unit_mode == "Celsius":
            display_df['temperature'] = (display_df['temperature'] - 32) * 5/9
            y_range = [-30, 30]
            dt_major, dt_minor = 10, 2
        else:
            y_range = [-20, 80]
            dt_major, dt_minor = 20, 5

        # Labeling
        def create_label(row):
            b_val = str(row.get('Bank', '')).strip().lower()
            d_val = str(row.get('Depth', '')).strip().lower()
            s_name = str(row.get('NodeNum', 'Unknown'))
            if b_val not in ["", "none", "nan", "null"]: return f"Bank {row['Bank']} ({s_name})"
            if d_val not in ["", "none", "nan", "null"]: return f"{row['Depth']}ft ({s_name})"
            return f"Unmapped ({s_name})"

        display_df['label'] = display_df.apply(create_label, axis=1)
        
        fig = go.Figure()
        for lbl in sorted(display_df['label'].unique()):
            s_df = display_df[display_df['label'] == lbl].sort_values('timestamp')
            fig.add_trace(go.Scatter(x=s_df['timestamp'], y=s_df['temperature'], name=lbl, mode='lines', line=dict(width=2)))

        # Layout & Grid
        fig.update_layout(
            title=f"{title}: Time vs Temperature", plot_bgcolor='white', hovermode="x unified",
            height=600, margin=dict(t=80, l=50, r=180, b=50),
            legend=dict(title="Sensors", orientation="v", yanchor="top", y=1, xanchor="left", x=1.02)
        )
        
        # Gridlines
        grid_6h = pd.date_range(start=start_view, end=end_view, freq='6h')
        for ts in grid_6h:
            color, width = ("Black", 2) if (ts.weekday()==0 and ts.hour==0) else (("Gray", 1) if ts.hour==0 else ("LightGray", 0.5))
            fig.add_vline(x=ts, line_width=width, line_color=color, layer='below')

        fig.update_yaxes(title=f"Temp ({unit_label})", range=y_range, gridcolor='Gainsboro', dtick=dt_minor)
        fig.update_xaxes(range=[start_view, end_view], mirror=True, showline=True, linecolor='black')

        for val, label in active_refs:
            c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
            fig.add_hline(y=c_val, line_dash="dash", line_color="maroon" if "Type A" in label else "RoyalBlue", opacity=0.8)
        
        return fig
    except Exception as e:
        st.error(f"Graph Error: {e}")
        return go.Figure()

####################
# --- SERVICES --- #
####################

# --- EXECUTIVE SUMMARY ---
if service == "🏠 Executive Summary":
    st.header(f"🏠 Executive Summary")
    
    display_summary = summary_df.copy()
    if selected_project:
        display_summary = display_summary[display_summary['Project'] == selected_project]

    if display_summary.empty:
        st.warning("📡 No sensors found.")
    else:
        now = pd.Timestamp.now(tz=pytz.UTC)
        rows = []
        for _, row in display_summary.iterrows():
            ts = row['timestamp'].tz_localize(pytz.UTC) if row['timestamp'].tzinfo is None else row['timestamp']
            hrs = int((now - ts).total_seconds() / 3600)
            status = "🔴" if hrs > 24 else ("🟢" if hrs < 6 else "🟡")
            rows.append({
                "Project": row['Project'], "Node": row['NodeNum'], "Location": row['Location'],
                "Temp": f"{round(convert_val(row['temperature']), 1)}{unit_label}",
                "Last Seen": f"{ts.strftime('%m/%d %H:%M')} ({hrs}h) {status}"
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

# --- CLIENT PORTAL ---
elif service == "📊 Client Portal":
    if not selected_project:
        st.warning("Please select a project.")
    elif approved_df.empty:
        st.info("No approved data found for this project.")
    else:
        st.header(f"📊 Project Status: {selected_project}")
        t_time, t_depth, t_table = st.tabs(["📈 Timeline Analysis", "📏 Depth Profile", "📋 Project Data"])

        weeks = st.slider("Weeks to View", 1, 12, 6, key="portal_slider")
        now = pd.Timestamp.now(tz=pytz.UTC)
        end_view = (now + pd.Timedelta(days=(7 - now.weekday()) % 7 or 7)).replace(hour=0, minute=0, second=0)
        start_view = end_view - timedelta(weeks=weeks)

        with t_time:
            for loc in sorted(approved_df['Location'].unique()):
                with st.expander(f"📈 {loc}", expanded=True):
                    loc_data = approved_df[(approved_df['Location'] == loc) & (approved_df['timestamp'] >= start_view)]
                    st.plotly_chart(build_standard_sf_graph(loc_data, loc, start_view, end_view, active_refs, unit_mode, unit_label), use_container_width=True, key=f"chart_{loc}")

        with t_depth:
            depth_only = approved_df.dropna(subset=['Depth_Num']).copy()
            for loc in sorted(depth_only['Location'].unique()):
                with st.expander(f"📏 {loc} Depth Profile", expanded=True):
                    loc_data = depth_only[depth_only['Location'] == loc]
                    fig_d = go.Figure()
                    mondays = pd.date_range(start=start_view, end=now, freq='W-MON')
                    for target_ts in [m.replace(hour=6) for m in mondays]:
                        window = loc_data[(loc_data['timestamp'] >= target_ts - pd.Timedelta(days=1)) & (loc_data['timestamp'] <= target_ts + pd.Timedelta(days=1))]
                        if not window.empty:
                            snaps = [window[window['NodeNum']==n].sort_values(by='timestamp', key=lambda x: (x-target_ts).abs()).iloc[0] for n in window['NodeNum'].unique()]
                            snap_df = pd.DataFrame(snaps).sort_values('Depth_Num')
                            fig_d.add_trace(go.Scatter(x=snap_df['temperature'], y=snap_df['Depth_Num'], mode='lines+markers', name=target_ts.strftime('%m/%d')))
                    
                    fig_d.update_yaxes(autorange="reversed", title="Depth (ft)")
                    fig_d.update_xaxes(title=f"Temp ({unit_label})", range=[-20, 80])
                    st.plotly_chart(fig_d, use_container_width=True, key=f"depth_{loc}")

        with t_table:
            latest = approved_df.sort_values('timestamp').groupby('NodeNum').tail(1)
            st.dataframe(latest[['Location', 'NodeNum', 'temperature']], use_container_width=True, hide_index=True)

# --- NODE DIAGNOSTICS ---
elif service == "📉 Node Diagnostics":
    if not selected_project or master_df.empty:
        st.warning("Select a project.")
    else:
        st.header(f"📉 Diagnostics: {selected_project}")
        loc_options = sorted(master_df['Location'].dropna().unique())
        sel_loc = st.selectbox("Select Pipe / Bank", loc_options)
        weeks_diag = st.slider("Lookback (Weeks)", 1, 12, 6, key="diag_slider")
        
        diag_data = master_df[master_df['Location'] == sel_loc]
        st.plotly_chart(build_standard_sf_graph(diag_data, sel_loc, datetime.now(pytz.UTC)-timedelta(weeks=weeks_diag), datetime.now(pytz.UTC), active_refs, unit_mode, unit_label), use_container_width=True, key="diag_chart")

# --- DATA INTAKE & ADMIN ---
elif service == "📤 Data Intake Lab":
    st.header("📤 Data Ingestion")
    # ... Implementation of file uploads as per original code ...
    st.info("Upload CSV files for LORD or SensorPush.")

elif service == "🛠️ Admin Tools":
    st.header("🛠️ Admin Tools")
    if st.button("Mark All Data Approved", key="bulk_app_btn"):
        client.query(f"UPDATE `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` SET approve = 'TRUE' WHERE 1=1").result()
        st.success("Sent approval command.")
