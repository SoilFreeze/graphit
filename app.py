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
# CHANGE THIS TO "2329" or "Office" depending on the dashboard
ACTIVE_PROJECT = "Office" 

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

        if unit_mode == "Celsius":
            display_df['temperature'] = (display_df['temperature'] - 32) * 5/9
            y_range = [( -20 - 32) * 5/9, (80 - 32) * 5/9]
            dt_major, dt_minor = 10, 2 
        else:
            y_range = [-20, 80]
            dt_major, dt_minor = 20, 5

        display_df['label'] = display_df.apply(lambda r: f"{r.get('depth', r.get('bank', 'Unmapped'))}ft ({r.get('nodenum', 'Unknown')})", axis=1)
        
        processed_dfs = []
        for lbl in sorted(display_df['label'].unique()):
            s_df = display_df[display_df['label'] == lbl].copy().sort_values('timestamp')
            s_df['gap'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
            if (s_df['gap'] > 6.0).any():
                gaps = s_df[s_df['gap'] > 6.0].copy()
                gaps['temperature'] = None
                gaps['timestamp'] -= pd.Timedelta(seconds=1)
                s_df = pd.concat([s_df, gaps]).sort_values('timestamp')
            processed_dfs.append(s_df)
        clean_df = pd.concat(processed_dfs)
        
        fig = go.Figure()
        for lbl in sorted(clean_df['label'].unique()):
            sdf = clean_df[clean_df['label'] == lbl]
            fig.add_trace(go.Scatter(x=sdf['timestamp'], y=sdf['temperature'], name=lbl, mode='lines', connectgaps=False))

        fig.update_layout(
            title={'text': f"{title} Time vs Temperature", 'x': 0, 'font': dict(size=18)},
            plot_bgcolor='white', hovermode="x unified", height=600, margin=dict(r=150)
        )
        
        for ts in pd.date_range(start=start_view, end=end_view, freq='6h'):
            color, width = ("Black", 2) if ts.weekday() == 0 and ts.hour == 0 else (("Gray", 1) if ts.hour == 0 else ("LightGray", 0.5))
            fig.add_vline(x=ts, line_width=width, line_color=color, layer='below')

        fig.update_yaxes(title=f"Temp ({unit_label})", range=y_range, gridcolor='Gainsboro', dtick=dt_minor)
        for yv in range(int(y_range[0]), int(y_range[1])+1, dt_major):
            fig.add_hline(y=yv, line_width=1.2, line_color="DimGray", layer='below')

        for val, label in active_refs:
            c_val = (val - 32) * 5/9 if unit_mode == "Celsius" else val
            fig.add_hline(y=c_val, line_dash="dash", line_color="maroon" if "Type A" in label else "RoyalBlue")
        
        return fig
    except: return go.Figure()

#######################
# --- SIDEBAR UI --- #
#######################
st.sidebar.title("📏 Dashboard Controls")
unit_mode = st.sidebar.radio("Temperature Unit", ["Fahrenheit", "Celsius"])
unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"

st.sidebar.divider()
st.sidebar.write("### Reference Lines")
active_refs = []
if st.sidebar.checkbox("Freezing (32°F)", value=True): active_refs.append((32.0, "Freezing"))
if st.sidebar.checkbox("Type B (26.6°F)", value=True): active_refs.append((26.6, "Type B"))
if st.sidebar.checkbox("Type A (10.2°F)", value=True): active_refs.append((10.2, "Type A"))

def convert_val(f):
    if f is None: return None
    return (f - 32) * 5/9 if unit_mode == "Celsius" else f

########################
# --- MAIN CONTENT --- #
########################
st.header(f"📊 Project {ACTIVE_PROJECT} Dashboard")
tab_time, tab_depth, tab_table = st.tabs(["📈 Timeline Analysis", "📏 Depth Profile", "📋 Project Data"])

q = f"SELECT * FROM `{MASTER_TABLE}` WHERE CAST(Project AS STRING) = '{ACTIVE_PROJECT}' AND approve = 'TRUE' AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 84 DAY)"
p_df = client.query(q).to_dataframe()

if p_df.empty:
    st.warning(f"No approved data found for Project {ACTIVE_PROJECT}.")
else:
    p_df['timestamp'] = pd.to_datetime(p_df['timestamp']).dt.tz_convert(pytz.UTC) if p_df['timestamp'].dt.tz else pd.to_datetime(p_df['timestamp']).dt.tz_localize(pytz.UTC)
    
    with tab_time:
        weeks = st.slider("Weeks to View", 1, 12, 6)
        now = pd.Timestamp.now(tz=pytz.UTC)
        end_view = (now + pd.Timedelta(days=(7 - now.weekday()) % 7 or 7)).replace(hour=0, minute=0, second=0, microsecond=0)
        start_view = end_view - timedelta(weeks=weeks)
        
        locs = sorted(p_df['Location'].dropna().unique())
        for loc in locs:
            with st.expander(f"📈 {loc}", expanded=True):
                loc_data = p_df[(p_df['Location'] == loc) & (p_df['timestamp'] >= start_view)]
                st.plotly_chart(build_standard_sf_graph(loc_data, loc, start_view, end_view, active_refs, unit_mode, unit_label), use_container_width=True)

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
                        # Correct nearest time snapshot logic
                        snaps = []
                        for node in window['NodeNum'].unique():
                            ndf = window[window['NodeNum'] == node].copy()
                            ndf['diff'] = (ndf['timestamp'] - target_ts).abs()
                            snaps.append(ndf.sort_values('diff').iloc[0])
                        snap_df = pd.DataFrame(snaps).sort_values('Depth_Num')
                        fig_d.add_trace(go.Scatter(x=snap_df['temperature'], y=snap_df['Depth_Num'], mode='lines+markers', name=target_ts.strftime('%m/%d/%Y')))
                
                # --- FORMATTING UPDATES ---
                max_d = loc_data['Depth_Num'].max()
                y_limit = int(((max_d // 5) + 1) * 5)
                y_dtick = 20 if y_limit > 60 else 10
                y_minor = 10 if y_limit > 60 else 5

                # X-Axis: -20 to 80, Major 20 (Gray), Minor 5 (LightGray)
                x_range = [-20, 80] if unit_mode == "Fahrenheit" else [-30, 30]
                fig_d.update_xaxes(title=f"Temp ({unit_label})", range=x_range, dtick=5, gridcolor='LightGray', mirror=True, showline=True, linecolor='black')
                for x_v in range(-20, 81, 20):
                    fig_d.add_vline(x=x_v, line_width=1, line_color="Gray")

                # Y-Axis: 0 at top, Major Grid (Gray), Minor (LightGray)
                fig_d.update_yaxes(title="Depth (ft)", range=[y_limit, 0], dtick=y_dtick, gridcolor='Gray', mirror=True, showline=True, linecolor='black')
                for d_v in range(0, y_limit + 1, y_minor):
                    fig_d.add_hline(y=d_v, line_width=0.5, line_color="LightGray")

                fig_d.update_layout(plot_bgcolor='white', height=700, legend=dict(title="Monday 6AM Snapshots"))
                st.plotly_chart(fig_d, use_container_width=True)

    with tab_table:
        latest = p_df.sort_values('timestamp').groupby('NodeNum').tail(1).copy()
        latest['Current Temp'] = latest['temperature'].apply(lambda x: f"{round(convert_val(x), 1)}{unit_label}")
        latest['Position'] = latest.apply(lambda r: f"Bank {r['Bank']}" if pd.notnull(r['Bank']) else f"{r['Depth']} ft", axis=1)
        st.dataframe(latest[['Location', 'Position', 'Current Temp', 'NodeNum']], use_container_width=True, hide_index=True)
