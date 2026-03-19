import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz

# --- 1. PAGE SETUP ---
st.set_page_config(layout="wide", page_title="SF Project Dashboard", page_icon="❄️")

# --- 2. GRIDLINE ENGINE ---
def get_time_gridlines(start_date, end_date):
    """Generates vertical lines for Monday (Dark), Midnight (Grey), and 6-hour intervals (Light)."""
    shapes = []
    current = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    
    while current <= end_date:
        for hour_offset in [0, 6, 12, 18]:
            line_time = current + timedelta(hours=hour_offset)
            if line_time < start_date or line_time > end_date: 
                continue
            
            # Formatting: Monday is Darkest, Midnight is Grey, 6hr is Light/Dotted
            if line_time.weekday() == 0 and hour_offset == 0:
                color, width, dash = "#424242", 2, "solid" 
            elif hour_offset == 0:
                color, width, dash = "#9E9E9E", 1, "solid" 
            else:
                color, width, dash = "#E0E0E0", 0.5, "dot" 
            
            shapes.append(dict(
                type="line", xref="x", yref="paper",
                x0=line_time, y0=0, x1=line_time, y1=1,
                line=dict(color=color, width=width, dash=dash),
                layer="below"
            ))
        current += timedelta(days=1)
    return shapes

# --- 3. DATA FETCHING ---
PROJECT_ID = "2329" 

@st.cache_data(ttl=600)
def fetch_project_data(pid, weeks):
    info = st.secrets["gcp_service_account"]
    scopes = [
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/cloud-platform"
    ]
    credentials = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    client = bigquery.Client(credentials=credentials, project=info["project_id"])
    
    # Fetch slightly more than requested to ensure Monday boundaries are covered
    cutoff = (datetime.now(tz=pytz.UTC) - timedelta(weeks=weeks+1)).strftime('%Y-%m-%d %H:%M:%S')
    
    query = f"""
    SELECT 
        d.timestamp, d.value, d.nodenumber, d.is_approved, d.engineer_note,
        m.Project, m.Location, m.Depth
    FROM `sensorpush-export.sensor_data.final_databoard_data` as d
    INNER JOIN `sensorpush-export.sensor_data.master_metadata` as m 
      ON d.nodenumber = m.NodeNum
    WHERE m.Project = '{pid}' 
    AND d.is_approved = TRUE
    AND d.timestamp >= '{cutoff}'
    ORDER BY d.timestamp ASC
    """
    try:
        df = client.query(query).to_dataframe()
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        return df
    except Exception as e:
        st.error(f"Access Error: {e}")
        return pd.DataFrame()

# --- 4. MAIN INTERFACE ---
st.title(f"❄️ Project {PROJECT_ID} Thermal Dashboard")

# Sidebar Filters
st.sidebar.header("View Settings")
weeks_to_show = st.sidebar.slider("Weeks of History", 1, 12, 2)

# Calculate Monday-to-Monday Boundaries
now_utc = datetime.now(tz=pytz.UTC)
days_until_next_monday = (7 - now_utc.weekday()) % 7
if days_until_next_monday == 0: days_until_next_monday = 7

end_view = (now_utc + timedelta(days=days_until_next_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
start_view = end_view - timedelta(weeks=weeks_to_show)

# Fetch and check data
df = fetch_project_data(PROJECT_ID, weeks_to_show)

if df.empty:
    st.warning("⏳ **Review in Progress:** Verified data will appear here once approved.")
else:
    all_locs = sorted(df['Location'].dropna().unique())
    sel_loc = st.sidebar.selectbox("Select Pipe / Bank", all_locs)
    
    loc_df = df[df['Location'] == sel_loc].copy()
    loc_df['Sensor_ID'] = "Depth: " + loc_df['Depth'].astype(str) + "'"

    # Create Tabs
    tab1, tab2, tab3 = st.tabs(["📊 Site Health", "📈 Temp vs Time", "📉 Depth Profile (Snapshot)"])

    with tab1:
        st.subheader(f"📋 24-Hour Summary: {sel_loc}")
        recent_df = loc_df[loc_df['timestamp'] >= (now_utc - timedelta(hours=24))]
        if not recent_df.empty:
            summary = []
            for node in recent_df['nodenumber'].unique():
                n_df = recent_df[recent_df['nodenumber'] == node].sort_values('timestamp')
                summary.append({
                    "Depth": n_df['Depth'].iloc[0],
                    "Min": n_df['value'].min(),
                    "Max": n_df['value'].max(),
                    "24h Change": n_df['value'].iloc[-1] - n_df['value'].iloc[0]
                })
            st.table(pd.DataFrame(summary).sort_values('Depth').style.format("{:.2f}°F"))
        else:
            st.info("No approved data in the last 24 hours.")

    with tab2:
        st.subheader("Temperature vs Time (History)")
        
        # Dynamic X-Axis labeling based on zoom level
        if weeks_to_show > 3:
            tick_spacing = 604800000.0  # 1 week in ms
            tick_format = "%b %d"
        else:
            tick_spacing = 86400000.0   # 1 day in ms
            tick_format = "%a\n%b %d"

        fig = px.line(loc_df, x='timestamp', y='value', color='Sensor_ID', range_y=[-20, 80], height=650)
        
        grid_shapes = get_time_gridlines(start_view, end_view)
        fig.update_layout(
            shapes=grid_shapes, 
            plot_bgcolor='white', 
            hovermode="x unified",
            margin=dict(l=20, r=150, t=50, b=20),
            legend=dict(title="Sensor Depth")
        )
        fig.update_xaxes(
            range=[start_view, end_view], 
            showgrid=False, 
            tickformat=tick_format,
            dtick=tick_spacing
        )
        fig.update_yaxes(showgrid=True, gridcolor='LightGrey', dtick=20)
        fig.add_hline(y=32, line_dash="dash", line_color="blue", annotation_text="32°F")
        
        st.plotly_chart(fig, use_container_width=True, key="history_chart")

    with tab3:
        st.subheader("Monday 6:00 AM Thermal Profile")
        
        # Filter for Monday (weekday 0) at 6 AM
        profile_df = loc_df[
            (loc_df['timestamp'].dt.weekday == 0) & 
            (loc_df['timestamp'].dt.hour == 6)
        ].copy()

        if not profile_df.empty:
            latest_mon = profile_df['timestamp'].max()
            snap_df = profile_df[profile_df['timestamp'] == latest_mon].sort_values('Depth')

            fig_profile = px.line(
                snap_df, x='value', y='Depth', markers=True,
                title=f"Snapshot: {latest_mon.strftime('%A, %b %d @ 06:00 UTC')}",
                labels={'value': 'Temperature (°F)', 'Depth': 'Depth (ft)'}
            )
            fig_profile.update_layout(plot_bgcolor='white', height=600)
            fig_profile.update_yaxes(autorange="reversed", gridcolor='LightGrey', zeroline=True)
            fig_profile.update_xaxes(gridcolor='LightGrey', range=[-20, 80])
            fig_profile.add_vline(x=32, line_dash="dash", line_color="blue", annotation_text="32°F")

            st.plotly_chart(fig_profile, use_container_width=True, key="profile_chart")
        else:
            st.info("No Monday 6:00 AM data points available in this timeframe.")
