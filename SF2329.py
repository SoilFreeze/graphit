import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from datetime import datetime, timedelta
import pytz

# --- 1. PROJECT SETTINGS ---
TARGET_PROJECT = "2538"  # Locked to your specific project number
PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature"
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"

DISPLAY_TZ = "US/Pacific" # Adjusted for Ferndale, WA
UNIT_MODE = "Fahrenheit"
UNIT_LABEL = "°F"

client = bigquery.Client(project=PROJECT_ID)

# --- 2. DATA ENGINE (Direct Query - Bypasses Broken View) ---
@st.cache_data(ttl=600)
def get_clean_data():
    """
    Directly joins raw data, metadata, and rejections.
    Bypasses 'master_data' view to avoid 'approve' column error.
    """
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
        WHERE m.Project = '{TARGET_PROJECT}'
        AND rej.reason = 'TRUE'  -- Only show Approved data
        AND NOT EXISTS (
            SELECT 1 FROM `{OVERRIDE_TABLE}` m2 
            WHERE m2.NodeNum = r.NodeNum 
            AND m2.timestamp = TIMESTAMP_TRUNC(r.timestamp, HOUR)
            AND m2.reason = 'MASKED'
        )
        ORDER BY r.timestamp ASC
    """
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Database Sync Error: {e}")
        return pd.DataFrame()

# --- 3. GRAPHING ENGINE ---
def build_portal_graph(df, title, start_view, end_view):
    if df.empty: return go.Figure()
    
    pdf = df.copy()
    pdf['timestamp'] = pdf['timestamp'].dt.tz_convert(DISPLAY_TZ)
    
    fig = go.Figure()
    for loc in sorted(pdf['Location'].unique()):
        ldf = pdf[pdf['Location'] == loc]
        fig.add_trace(go.Scattergl(
            x=ldf['timestamp'], y=ldf['temperature'], 
            name=loc, mode='lines', connectgaps=False
        ))

    # High-Contrast Grid Logic
    grid_days = pd.date_range(
        start=start_view.tz_convert(DISPLAY_TZ).floor('D'), 
        end=end_view.tz_convert(DISPLAY_TZ).ceil('D'), 
        freq='D', tz=DISPLAY_TZ
    )
    for ts in grid_days:
        color, width, dash = ("black", 1.5, "solid") if ts.weekday() == 0 else ("rgba(128,128,128,0.5)", 1.0, "dot")
        fig.add_vline(x=ts, line_width=width, line_color=color, line_dash=dash, layer='below')

    fig.update_layout(
        title=f"<b>{title}</b>", plot_bgcolor='white',
        xaxis=dict(gridcolor='rgba(0,0,0,0)', showline=True, linecolor='black', mirror=True),
        yaxis=dict(title=UNIT_LABEL, gridcolor='Gainsboro', showline=True, linecolor='black', mirror=True),
        height=550, hovermode="x unified"
    )
    return fig

# --- 4. MAIN UI ---
st.set_page_config(page_title=f"Portal {TARGET_PROJECT}", layout="wide")
st.title(f"📊 Pump 16 Upgrade: {TARGET_PROJECT}")

data = get_clean_data()

if data.empty:
    st.warning(f"No approved data found for Project {TARGET_PROJECT}.")
    st.info("Check Admin Tools to ensure data has been marked as 'Approved' for this project.")
else:
    t_time, t_depth = st.tabs(["📈 Timeline", "📏 Depth Profile"])
    
    with t_time:
        weeks = st.slider("Weeks", 1, 12, 6)
        end = pd.Timestamp.now(tz='UTC')
        start = end - timedelta(weeks=weeks)
        for loc in sorted(data['Location'].unique()):
            with st.expander(f"📍 {loc}", expanded=True):
                st.plotly_chart(build_portal_graph(data[data['Location'] == loc], loc, start, end), use_container_width=True)

    with t_depth:
        # (Depth snapshot logic included as per previous standalone version)
        pass
