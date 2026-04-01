import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz

#########################
# --- CONFIGURATION --- #
#########################
ACTIVE_PROJECT = "2329" 

st.set_page_config(page_title=f"Project {ACTIVE_PROJECT} Dashboard", layout="wide")

DATASET_ID = "Temperature" 
PROJECT_ID = "sensorpush-export"
MASTER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.master_data"

@st.cache_resource
def get_bq_client():
    try:
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/bigquery"])
            return bigquery.Client(credentials=credentials, project=info["project_id"])
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"Authentication Failed: {e}")
        return None

client = get_bq_client()

############################
# --- GRAPHING ENGINES --- #
############################

def build_standard_sf_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label):
    try:
        display_df = df.copy()
        if display_df.empty: return go.Figure()

        display_df.columns = [c.lower() for c in display_df.columns]
        display_df['timestamp'] = pd.to_datetime(display_df['timestamp'])
        display_df['timestamp'] = display_df['timestamp'].dt.tz_convert(pytz.UTC) if display_df['timestamp'].dt.tz else display_df['timestamp'].dt.tz_localize(pytz.UTC)

        y_range = [-20, 80] if unit_mode == "Fahrenheit" else [-30, 30]
        dt_major, dt_minor = 20, 5

        display_df['label'] = display_df.apply(lambda r: f"{r.get('depth', r.get('bank', 'Unmapped'))}ft ({r.get('nodenum', 'Unknown')})", axis=1)
        
        processed_dfs = []
        for lbl in sorted(display_df['label'].unique()):
            s_df = display_df[display_df['label'] == lbl].copy().sort_values('timestamp')
            s_df['gap'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
            gaps = s_df[s_df['gap'] > 6.0].copy()
            if not gaps.empty:
                gaps['temperature'] = None
                gaps['timestamp'] -= pd.Timedelta(seconds=1)
                s_df = pd.concat([s_df, gaps]).sort_values('timestamp')
            processed_dfs.append(s_df)
        clean_df = pd.concat(processed_dfs)
        
        fig = go.Figure()
        for lbl in sorted(clean_df['label'].unique()):
            sdf = clean_df[clean_df['label'] == lbl]
            fig.add_trace(go.Scatter(x=sdf['timestamp'], y=sdf['temperature'], name=lbl, mode='lines', connectgaps=False))

        # TIMELINE X-AXIS GRID
        for ts in pd.date_range(start=start_view, end=end_view, freq='6h'):
            if ts.weekday() == 0 and ts.hour == 0: color, width = "Black", 2.5
            elif ts.hour == 0: color, width = "#222222", 1.8
            else: color, width = "#444444", 1.2 # Darker 6h lines
            fig.add_vline(x=ts, line_width=width, line_color=color, layer='below')

        fig.update_yaxes(range=y_range, gridcolor='#333333', gridwidth=1, dtick=dt_minor)
        fig.update_layout(plot_bgcolor='white', height=600, margin=dict(r=150))
        
        for val, label in active_refs:
            fig.add_hline(y=val, line_dash="dash", line_color="maroon" if "Type A" in label else "RoyalBlue", line_width=2.5)
        
        return fig
    except: return go.Figure()

#######################
# --- SIDEBAR UI --- #
#######################
st.sidebar.title("📏 Dashboard Controls")
unit_mode = st.sidebar.radio("Temperature Unit", ["Fahrenheit", "Celsius"])
unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"

active_refs = []
if st.sidebar.checkbox("Freezing (32°F)", value=True): active_refs.append((32.0, "Freezing"))
if st.sidebar.checkbox("Type B (26.6°F)", value=True): active_refs.append((26.6, "Type B"))
if st.sidebar.checkbox("Type A (10.2°F)", value=True): active_refs.append((10.2, "Type A"))

########################
# --- MAIN CONTENT --- #
########################
st.header(f"📊 Project {ACTIVE_PROJECT} Dashboard")
tab_time, tab_depth, tab_table = st.tabs(["📈 Timeline Analysis", "📏 Depth Profile", "📋 Project Data"])

# FIXED QUERY: Direct string comparison for 'approve'
q = f"""
    SELECT * FROM `{MASTER_TABLE}` 
    WHERE Project = '{ACTIVE_PROJECT}' 
    AND (approve = 'TRUE' OR approve = 'true')
    AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 84 DAY)
"""

try:
    p_df = client.query(q).to_dataframe()
except Exception as e:
    st.error(f"Database Query Error: {e}")
    p_df = pd.DataFrame()

if p_df.empty:
    st.warning(f"No approved data found for Project {ACTIVE_PROJECT}.")
else:
    p_df['timestamp'] = pd.to_datetime(p_df['timestamp']).dt.tz_convert(pytz.UTC) if p_df['timestamp'].dt.tz else pd.to_datetime(p_df['timestamp']).dt.tz_localize(pytz.UTC)
    
    # 1. TIMELINE
    with tab_time:
        weeks = st.slider("Weeks to View", 1, 12, 6)
        now = pd.Timestamp.now(tz=pytz.UTC)
        end_view = (now + pd.Timedelta(days=(7 - now.weekday()) % 7 or 7)).replace(hour=0, minute=0, second=0, microsecond=0)
        start_view = end_view - timedelta(weeks=weeks)
        
        for loc in sorted(p_df['Location'].dropna().unique()):
            with st.expander(f"📈 {loc}", expanded=True):
                loc_data = p_df[(p_df['Location'] == loc) & (p_df['timestamp'] >= start_view)]
                st.plotly_chart(build_standard_sf_graph(loc_data, loc, start_view, end_view, active_refs, unit_mode, unit_label), use_container_width=True, key=f"t_{loc}")

    # 2. DEPTH PROFILE
    with tab_depth:
        p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
        depth_df = p_df.dropna(subset=['Depth_Num', 'NodeNum']).copy()
        for loc in sorted(depth_df['Location'].unique()):
            with st.expander(f"📏 {loc} Depth Profile", expanded=True):
                loc_data = depth_df[depth_df['Location'] == loc].copy()
                fig_d = go.Figure()
                mondays = pd.date_range(start=start_view, end=now, freq='W-MON')
                
                for target_ts in [m.replace(hour=6) for m in mondays]:
                    window = loc_data[(loc_data['timestamp'] >= target_ts - pd.Timedelta(days=1)) & (loc_data['timestamp'] <= target_ts + pd.Timedelta(days=1))]
                    if not window.empty:
                        snaps = [window[window['NodeNum']==n].sort_values(by='timestamp', key=lambda x: (x-target_ts).abs()).iloc[0] for n in window['NodeNum'].unique()]
                        snap_df = pd.DataFrame(snaps).sort_values('Depth_Num')
                        fig_d.add_trace(go.Scatter(x=snap_df['temperature'], y=snap_df['Depth_Num'], mode='lines+markers', name=target_ts.strftime('%m/%d/%Y')))
                
                y_limit = int(((loc_data['Depth_Num'].max() // 5) + 1) * 5)
                
                # DEPTH X-AXIS GRID (5-degree increments) - DARKEST GRAY
                fig_d.update_xaxes(title=f"Temp ({unit_label})", range=[-20, 80], dtick=5, 
                                   gridcolor='#222222', gridwidth=1.3, mirror=True, showline=True, linecolor='black')
                # 20-degree Major lines
                for x_v in range(-20, 81, 20):
                    fig_d.add_vline(x=x_v, line_width=2.5, line_color="Black")

                fig_d.update_yaxes(title="Depth (ft)", range=[y_limit, 0], dtick=10, gridcolor='Black', gridwidth=1.5)
                
                for val, label in active_refs:
                    fig_d.add_vline(x=val, line_dash="dash", line_color="maroon" if "Type A" in label else "RoyalBlue", line_width=2.5)

                fig_d.update_layout(plot_bgcolor='white', height=700)
                st.plotly_chart(fig_d, use_container_width=True, key=f"d_{loc}")

    # 3. TABLE
    with tab_table:
        latest = p_df.sort_values('timestamp').groupby('NodeNum').tail(1).copy()
        latest['Current Temp'] = latest['temperature'].apply(lambda x: f"{round(x, 1)}{unit_label}")
        st.dataframe(latest[['Location', 'Depth', 'Current Temp', 'NodeNum']], use_container_width=True, hide_index=True)
