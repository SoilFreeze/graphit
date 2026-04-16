import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from datetime import datetime, timedelta
import pytz

# --- 1. SETTINGS & PROJECT LOCK ---
# Change this to your specific project name/number
TARGET_PROJECT = "2538-Ferndale" 

PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature"
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"

# Timezones and Units
DISPLAY_TZ = "US/Eastern"
UNIT_MODE = "Fahrenheit" # Options: "Fahrenheit", "Celsius"
UNIT_LABEL = "°F" if UNIT_MODE == "Fahrenheit" else "°C"

client = bigquery.Client(project=PROJECT_ID)

# --- 2. DATA ENGINE (With Mask Priority) ---
@st.cache_data(ttl=600)
def get_locked_client_data():
    # We bypass the 'master_data' view and query raw tables + metadata + rejections
    query = f"""
        SELECT 
            r.NodeNum, r.timestamp, r.temperature,
            m.Location, m.Bank, m.Depth
        FROM (
            SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
            UNION ALL
            SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
        ) AS r
        INNER JOIN `{PROJECT_ID}.{DATASET_ID}.metadata` AS m ON r.NodeNum = m.NodeNum
        LEFT JOIN `{OVERRIDE_TABLE}` AS rej 
            ON r.NodeNum = rej.NodeNum 
            AND TIMESTAMP_TRUNC(r.timestamp, HOUR) = rej.timestamp
        WHERE m.Project = '2538'  -- Hardcoded for your project
        AND rej.reason = 'TRUE'   -- Must be approved
        AND NOT EXISTS (
            SELECT 1 FROM `{OVERRIDE_TABLE}` m2 
            WHERE m2.NodeNum = r.NodeNum 
            AND m2.timestamp = TIMESTAMP_TRUNC(r.timestamp, HOUR)
            AND m2.reason = 'MASKED'
        )
    """
    return client.query(query).to_dataframe()
# --- 3. GRAPHING ENGINE (The "Engineering" Look) ---
def build_custom_graph(df, title, start_view, end_view):
    if df.empty: return go.Figure().update_layout(title="No Data")
    
    pdf = df.copy()
    pdf['timestamp'] = pdf['timestamp'].dt.tz_convert(DISPLAY_TZ)
    
    if UNIT_MODE == "Celsius":
        pdf['temperature'] = (pdf['temperature'] - 32) * 5/9
        y_range = [-30, 30]
    else:
        y_range = [-20, 80]

    fig = go.Figure()
    
    # Plot each unique location
    for loc in sorted(pdf['Location'].unique()):
        ldf = pdf[pdf['Location'] == loc]
        fig.add_trace(go.Scattergl(
            x=ldf['timestamp'], y=ldf['temperature'], 
            name=loc, mode='lines', connectgaps=False
        ))

    # Grid Hierarchy: Monday Solid, Daily Dotted
    grid_days = pd.date_range(
        start=start_view.tz_convert(DISPLAY_TZ).floor('D'), 
        end=end_view.tz_convert(DISPLAY_TZ).ceil('D'), 
        freq='D', tz=DISPLAY_TZ
    )
    for ts in grid_days:
        if ts.weekday() == 0:
            color, width, dash = "rgba(0,0,0,1)", 1.5, "solid"
        else:
            color, width, dash = "rgba(128,128,128,0.5)", 1.0, "dot"
        fig.add_vline(x=ts, line_width=width, line_color=color, line_dash=dash, layer='below')

    fig.update_layout(
        title=f"<b>{title}</b>",
        plot_bgcolor='white',
        xaxis=dict(gridcolor='rgba(0,0,0,0)', showline=True, linecolor='black', mirror=True),
        yaxis=dict(title=UNIT_LABEL, gridcolor='Gainsboro', showline=True, linecolor='black', mirror=True, range=y_range),
        height=550,
        hovermode="x unified"
    )
    return fig

# --- 4. STREAMLIT UI ---
st.set_page_config(page_title=f"Portal: {TARGET_PROJECT}", layout="wide")
st.title(f"❄️ Project Dashboard: {TARGET_PROJECT}")

df = get_locked_client_data()

if df.empty:
    st.info(f"Checking for approved data for {TARGET_PROJECT}...")
    st.image("https://via.placeholder.com/800x200.png?text=No+Approved+Data+Found+In+Cloud", use_container_width=True)
else:
    t_time, t_depth, t_table = st.tabs(["📈 Timeline Analysis", "📏 Depth Profile", "📋 Latest Readings"])
    
    with t_time:
        weeks = st.slider("Timeframe (Weeks)", 1, 12, 6)
        end = pd.Timestamp.now(tz='UTC')
        start = end - timedelta(weeks=weeks)
        
        for loc in sorted(df['Location'].unique()):
            with st.expander(f"📍 {loc}", expanded=True):
                loc_df = df[df['Location'] == loc]
                st.plotly_chart(build_custom_graph(loc_df, loc, start, end), use_container_width=True)

    with t_depth:
        st.subheader("Vertical Temperature Profiles")
        df['Depth_Num'] = pd.to_numeric(df['Depth'], errors='coerce')
        depth_df = df.dropna(subset=['Depth_Num']).copy()
        
        for loc in sorted(depth_df['Location'].unique()):
            with st.expander(f"📏 {loc} - 6 Week Snapshot"):
                fig_d = go.Figure()
                mondays = pd.date_range(end=pd.Timestamp.now(tz='UTC'), periods=6, freq='W-MON')
                
                for m in mondays:
                    target = m.replace(hour=6, minute=0)
                    window = depth_df[(depth_df['Location'] == loc) & 
                                      (depth_df['timestamp'].between(target-timedelta(hours=12), target+timedelta(hours=12)))]
                    if not window.empty:
                        snap = window.assign(d=(window['timestamp']-target).abs()).sort_values(['NodeNum','d']).drop_duplicates('NodeNum').sort_values('Depth_Num')
                        x_vals = snap['temperature'].apply(lambda x: (x-32)*5/9 if UNIT_MODE == "Celsius" else x)
                        fig_d.add_trace(go.Scatter(x=x_vals, y=snap['Depth_Num'], name=m.strftime('%m/%d'), mode='lines+markers', line=dict(shape='spline')))
                
                fig_d.update_layout(
                    yaxis=dict(autorange="reversed", title="Depth (ft)", gridcolor='Silver'),
                    xaxis=dict(title=UNIT_LABEL, gridcolor='Gainsboro'),
                    plot_bgcolor='white', height=600
                )
                st.plotly_chart(fig_d, use_container_width=True)

    with t_table:
        latest = df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        latest['Current Temp'] = latest['temperature'].apply(lambda x: f"{round((x-32)*5/9 if UNIT_MODE=='Celsius' else x, 1)}{UNIT_LABEL}")
        st.dataframe(latest[['Location', 'Depth', 'Current Temp', 'timestamp']].sort_values(['Location', 'Depth']), use_container_width=True, hide_index=True)
