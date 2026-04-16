import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from datetime import datetime, timedelta
import pytz

# --- 1. SETTINGS ---
TARGET_PROJECT = "2538" 
PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature"

# Stable Tables
METADATA_TABLE = f"{PROJECT_ID}.{DATASET_ID}.metadata_snapshot"
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"

# Localization
DISPLAY_TZ = "US/Pacific"
UNIT_LABEL = "°F"

# Initialize Client
client = bigquery.Client(project=PROJECT_ID)

# --- 2. DATA ENGINE ---
@st.cache_data(ttl=600)
def get_standalone_data():
    """
    Directly queries raw tables + metadata_snapshot + manual_rejections.
    Bypasses broken views and Drive permission issues.
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
        INNER JOIN `{METADATA_TABLE}` AS m ON r.NodeNum = m.NodeNum
        LEFT JOIN `{OVERRIDE_TABLE}` AS rej 
            ON r.NodeNum = rej.NodeNum 
            AND TIMESTAMP_TRUNC(r.timestamp, HOUR) = rej.timestamp
        WHERE m.Project LIKE '{TARGET_PROJECT}%' 
        AND rej.approve = 'TRUE' -- Only show data approved in Admin Tools
        AND NOT EXISTS (
            SELECT 1 FROM `{OVERRIDE_TABLE}` m2 
            WHERE m2.NodeNum = r.NodeNum 
            AND m2.timestamp = TIMESTAMP_TRUNC(r.timestamp, HOUR)
            AND m2.approve = 'MASKED' -- 'MASKED' status acts as a kill-switch
        )
        ORDER BY r.timestamp ASC
    """
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"⚠️ Database Connection Issue: {e}")
        return pd.DataFrame()

# --- 3. GRAPHING ENGINE ---
def build_portal_graph(df, title, start_view, end_view):
    if df.empty:
        return go.Figure().update_layout(title="No Data Available")
    
    pdf = df.copy()
    pdf['timestamp'] = pdf['timestamp'].dt.tz_convert(DISPLAY_TZ)
    
    fig = go.Figure()
    
    # Plotting Logic
    for loc in sorted(pdf['Location'].unique()):
        ldf = pdf[pdf['Location'] == loc]
        fig.add_trace(go.Scattergl(
            x=ldf['timestamp'], 
            y=ldf['temperature'], 
            name=loc, 
            mode='lines', 
            connectgaps=False
        ))

    # Engineering Grid: Solid Black Mondays, Dotted Gray Midnights
    grid_days = pd.date_range(
        start=start_view.tz_convert(DISPLAY_TZ).floor('D'), 
        end=end_view.tz_convert(DISPLAY_TZ).ceil('D'), 
        freq='D', 
        tz=DISPLAY_TZ
    )
    for ts in grid_days:
        if ts.weekday() == 0: # Monday
            color, width, dash = "rgba(0,0,0,1)", 1.5, "solid"
        else: # Daily Midnight
            color, width, dash = "rgba(128,128,128,0.5)", 1.0, "dot"
        fig.add_vline(x=ts, line_width=width, line_color=color, line_dash=dash, layer='below')

    # Add "Now" line
    now_local = pd.Timestamp.now(tz=DISPLAY_TZ)
    fig.add_vline(x=now_local, line_width=2, line_color="Red", line_dash="dash", layer='above')

    fig.update_layout(
        title=f"<b>{title}</b>",
        plot_bgcolor='white',
        hovermode="x unified",
        xaxis=dict(
            gridcolor='rgba(0,0,0,0)', 
            showline=True, 
            linecolor='black', 
            mirror=True, 
            tickformat='%b %d'
        ),
        yaxis=dict(
            title=UNIT_LABEL, 
            gridcolor='Gainsboro', 
            showline=True, 
            linecolor='black', 
            mirror=True, 
            range=[-20, 80]
        ),
        height=550,
        margin=dict(r=150)
    )
    return fig

# --- 4. MAIN UI LAYOUT ---
st.set_page_config(page_title=f"Project {TARGET_PROJECT} Portal", layout="wide")

# Sidebar
st.sidebar.title("Controls")
if st.sidebar.button("🔄 Refresh Data / Clear Cache"):
    st.cache_data.clear()
    st.rerun()

# Header
st.title(f"📊 Pump 16 Upgrade: {TARGET_PROJECT}")
st.caption(f"Ferndale, WA | Local Time: {pd.Timestamp.now(tz=DISPLAY_TZ).strftime('%m/%d/%Y %H:%M')}")

# Execution
df = get_standalone_data()

if df.empty:
    st.warning(f"No approved data found for project {TARGET_PROJECT}.")
    st.info("💡 **Action Required:** Open the Admin Dashboard and use the 'Bulk Approval' tool for this project range.")
else:
    tab_time, tab_depth, tab_table = st.tabs(["📈 Timeline Analysis", "📏 Depth Profiles", "📋 Latest Readings"])
    
    with tab_time:
        weeks = st.slider("Historical Window (Weeks)", 1, 12, 6)
        end = pd.Timestamp.now(tz='UTC')
        start = end - timedelta(weeks=weeks)
        
        for loc in sorted(df['Location'].unique()):
            with st.expander(f"📍 {loc}", expanded=True):
                loc_df = df[df['Location'] == loc]
                fig = build_portal_graph(loc_df, loc, start, end)
                st.plotly_chart(fig, use_container_width=True, key=f"chart_{loc}")

    with tab_depth:
        st.subheader("Vertical Temperature Profiles")
        df['Depth_Num'] = pd.to_numeric(df['Depth'], errors='coerce')
        depth_df = df.dropna(subset=['Depth_Num']).copy()
        
        for loc in sorted(depth_df['Location'].unique()):
            with st.expander(f"📏 {loc} - 6 Week Snapshots"):
                fig_d = go.Figure()
                mondays = pd.date_range(end=pd.Timestamp.now(tz='UTC'), periods=6, freq='W-MON')
                
                for m in mondays:
                    target = m.replace(hour=6, minute=0)
                    window = depth_df[(depth_df['Location'] == loc) & 
                                      (depth_df['timestamp'].between(target-timedelta(hours=12), target+timedelta(hours=12)))]
                    if not window.empty:
                        snap = (window.assign(d=(window['timestamp']-target).abs())
                                .sort_values(['NodeNum','d'])
                                .drop_duplicates('NodeNum')
                                .sort_values('Depth_Num'))
                        
                        fig_d.add_trace(go.Scatter(
                            x=snap['temperature'], 
                            y=snap['Depth_Num'], 
                            name=m.strftime('%m/%d'), 
                            mode='lines+markers', 
                            line=dict(shape='spline', smoothing=0.5)
                        ))
                
                fig_d.update_layout(
                    yaxis=dict(autorange="reversed", title="Depth (ft)", gridcolor='Silver'),
                    xaxis=dict(title=UNIT_LABEL, gridcolor='Gainsboro'),
                    plot_bgcolor='white', 
                    height=600
                )
                st.plotly_chart(fig_d, use_container_width=True, key=f"depth_{loc}")

    with tab_table:
        latest = df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        latest['Current Temp'] = latest['temperature'].apply(lambda x: f"{round(x, 1)}{UNIT_LABEL}")
        latest['Last Sync'] = latest['timestamp'].dt.tz_convert(DISPLAY_TZ).dt.strftime('%m/%d %H:%M')
        
        st.dataframe(
            latest[['Location', 'Depth', 'Current Temp', 'Last Sync']].sort_values(['Location', 'Depth']), 
            use_container_width=True, 
            hide_index=True
        )
