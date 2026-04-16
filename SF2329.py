import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from datetime import datetime, timedelta
import pytz

# --- 1. SETTINGS & PROJECT LOCK ---
TARGET_PROJECT = "2538"  # Locked to Pump 16 Upgrade
PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature"
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"

# Localization
DISPLAY_TZ = "US/Pacific"
UNIT_MODE = "Fahrenheit" 
UNIT_LABEL = "°F"

# Authenticate BigQuery
client = bigquery.Client(project=PROJECT_ID)

# --- 2. DATA ENGINE (View Bypass Logic) ---
@st.cache_data(ttl=600)
def get_standalone_data():
    """
    Directly queries raw tables + metadata + rejections.
    Bypasses 'master_data' view to fix the 'approve' column NameError.
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
        AND rej.reason = 'TRUE'  -- Filters for Approved data only
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
        # This will catch if even the override table schema is different
        st.error(f"Database Connection Error: {e}")
        return pd.DataFrame()

# --- 3. GRAPHING ENGINE (High-Contrast Grid) ---
def build_portal_graph(df, title, start_view, end_view):
    if df.empty: return go.Figure().update_layout(title="No Data")
    
    pdf = df.copy()
    pdf['timestamp'] = pdf['timestamp'].dt.tz_convert(DISPLAY_TZ)
    
    # Scale Y-axis for Fahrenheit
    y_range = [-20, 80]

    fig = go.Figure()
    
    # Plot Lines
    for loc in sorted(pdf['Location'].unique()):
        ldf = pdf[pdf['Location'] == loc]
        fig.add_trace(go.Scattergl(
            x=ldf['timestamp'], y=ldf['temperature'], 
            name=loc, mode='lines', connectgaps=False
        ))

    # Custom Grid: Solid Black Mondays, Dotted Gray Midnights
    grid_days = pd.date_range(
        start=start_view.tz_convert(DISPLAY_TZ).floor('D'), 
        end=end_view.tz_convert(DISPLAY_TZ).ceil('D'), 
        freq='D', tz=DISPLAY_TZ
    )
    for ts in grid_days:
        if ts.weekday() == 0: # Monday
            color, width, dash = "rgba(0,0,0,1)", 1.5, "solid"
        else: # Daily
            color, width, dash = "rgba(128,128,128,0.5)", 1.0, "dot"
        fig.add_vline(x=ts, line_width=width, line_color=color, line_dash=dash, layer='below')

    fig.update_layout(
        title=f"<b>{title}</b>",
        plot_bgcolor='white',
        xaxis=dict(gridcolor='rgba(0,0,0,0)', showline=True, linecolor='black', mirror=True, tickformat='%b %d'),
        yaxis=dict(title=UNIT_LABEL, gridcolor='Gainsboro', showline=True, linecolor='black', mirror=True, range=y_range),
        height=550,
        hovermode="x unified",
        margin=dict(r=150) # Room for legend
    )
    return fig

# --- 4. UI LAYOUT ---
st.set_page_config(page_title=f"Project {TARGET_PROJECT} Portal", layout="wide")

# Header
st.title(f"📊 Pump 16 Upgrade Project: {TARGET_PROJECT}")
st.caption("Ferndale, Washington | Approved Client Data Only")

# Fetch Data
df = get_standalone_data()

if df.empty:
    st.warning(f"No approved data found for Project {TARGET_PROJECT}.")
    st.info("💡 **Action Required:** Ensure you have performed a 'Bulk Approval' for this project in the Admin Tools.")
    # Show dummy data hint if available
else:
    tab_time, tab_depth, tab_table = st.tabs(["📈 Timeline Analysis", "📏 Depth Profiles", "📋 Latest Readings"])
    
    with tab_time:
        weeks = st.slider("Historical View (Weeks)", 1, 12, 6)
        end = pd.Timestamp.now(tz='UTC')
        start = end - timedelta(weeks=weeks)
        
        # Group by location for clear expanders
        for loc in sorted(df['Location'].unique()):
            with st.expander(f"📍 {loc}", expanded=True):
                loc_df = df[df['Location'] == loc]
                st.plotly_chart(build_portal_graph(loc_df, loc, start, end), use_container_width=True)

    with tab_depth:
        st.subheader("Vertical Temperature Profiles")
        df['Depth_Num'] = pd.to_numeric(df['Depth'], errors='coerce')
        depth_df = df.dropna(subset=['Depth_Num']).copy()
        
        for loc in sorted(depth_df['Location'].unique()):
            with st.expander(f"📏 {loc} - Weekly Snapshots (6 AM)"):
                fig_d = go.Figure()
                mondays = pd.date_range(end=pd.Timestamp.now(tz='UTC'), periods=6, freq='W-MON')
                
                for m in mondays:
                    target = m.replace(hour=6, minute=0)
                    window = depth_df[(depth_df['Location'] == loc) & 
                                      (depth_df['timestamp'].between(target-timedelta(hours=12), target+timedelta(hours=12)))]
                    if not window.empty:
                        # Find closest reading to 6AM for each node
                        snap = (window.assign(d=(window['timestamp']-target).abs())
                                .sort_values(['NodeNum','d'])
                                .drop_duplicates('NodeNum')
                                .sort_values('Depth_Num'))
                        
                        fig_d.add_trace(go.Scatter(
                            x=snap['temperature'], y=snap['Depth_Num'], 
                            name=m.strftime('%m/%d'), mode='lines+markers', 
                            line=dict(shape='spline', smoothing=0.5)
                        ))
                
                fig_d.update_layout(
                    yaxis=dict(autorange="reversed", title="Depth (ft)", gridcolor='Silver'),
                    xaxis=dict(title=UNIT_LABEL, gridcolor='Gainsboro'),
                    plot_bgcolor='white', height=600
                )
                st.plotly_chart(fig_d, use_container_width=True)

    with tab_table:
        # Summary of most recent data points
        latest = df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        latest['Current Temp'] = latest['temperature'].apply(lambda x: f"{round(x, 1)}{UNIT_LABEL}")
        
        # Display localized timestamp
        latest['Last Seen'] = latest['timestamp'].dt.tz_convert(DISPLAY_TZ).dt.strftime('%m/%d %H:%M')
        
        st.dataframe(
            latest[['Location', 'Depth', 'Current Temp', 'Last Seen']].sort_values(['Location', 'Depth']), 
            use_container_width=True, 
            hide_index=True
        )
