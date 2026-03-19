import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz

# --- 1. PAGE SETUP ---
st.set_page_config(layout="wide", page_title="SF Project Dashboard", page_icon="❄️")

# --- 2. GRIDLINE ENGINE (NEW) ---
def get_time_gridlines(start_date, end_date):
    """Generates vertical lines for Monday, Midnight, and 6-hour intervals."""
    shapes = []
    # Round start_date down to the beginning of that day to catch the first midnight
    current = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    
    while current <= end_date:
        for hour_offset in [0, 6, 12, 18]:
            line_time = current + timedelta(hours=hour_offset)
            if line_time < start_date or line_time > end_date: 
                continue
            
            # Formatting Logic
            if line_time.weekday() == 0 and hour_offset == 0:
                color, width, dash = "#424242", 2, "solid" # Monday Midnight (Dark)
            elif hour_offset == 0:
                color, width, dash = "#9E9E9E", 1, "solid" # Daily Midnight (Grey)
            else:
                color, width, dash = "#E0E0E0", 0.5, "dot" # 6-hour marks (Light)
            
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
    
    cutoff = (datetime.now(tz=pytz.UTC) - timedelta(weeks=weeks)).strftime('%Y-%m-%d %H:%M:%S')
    
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

# 1. Sidebar Settings
st.sidebar.header("View Settings")
weeks_to_show = st.sidebar.slider("Weeks of History", 1, 12, 2)

# 2. Calculate Monday-to-Monday Boundaries
now_utc = datetime.now(tz=pytz.UTC)
# Find days until next Monday (0=Mon, 1=Tue... 6=Sun)
days_until_monday = (7 - now_utc.weekday()) % 7
if days_until_monday == 0: days_until_monday = 7 # If today is Monday, look to next week

end_view = (now_utc + timedelta(days=days_until_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
start_view = end_view - timedelta(weeks=weeks_to_show)

# 3. Fetch Data
df = fetch_project_data(PROJECT_ID, weeks_to_show + 1)

if df.empty:
    st.warning("⏳ **Review in Progress:** Verified data will appear here once approved.")
else:
    # DATA PROCESSING (Only runs if df is NOT empty)
    all_locs = sorted(df['Location'].dropna().unique())
    sel_loc = st.sidebar.selectbox("Select Pipe / Bank", all_locs)
    
    loc_df = df[df['Location'] == sel_loc].copy()
    loc_df['Sensor_ID'] = "Depth: " + loc_df['Depth'].astype(str) + "'"

    # Generate Gridlines for the Monday-to-Monday range
    grid_shapes = get_time_gridlines(start_view, end_view)

    # Initialize Tabs inside the else block
    tab1, tab2, tab3 = st.tabs(["📊 Site Health", "📈 Temp vs Time", "📉 Depth vs Time"])

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
        st.subheader("Temperature vs Time")
        
        # 1. Determine Tick Frequency
        # If showing more than 3 weeks, only show a label every Monday (7 days)
        # Otherwise, show a label every day.
        if weeks_to_show > 3:
            tick_spacing = 604800000.0  # 7 days in ms
            tick_format = "%b %d"        # e.g., "Oct 12"
        else:
            tick_spacing = 86400000.0   # 1 day in ms
            tick_format = "%a\n%b %d"    # e.g., "Mon [newline] Oct 12"

        fig = px.line(loc_df, x='timestamp', y='value', color='Sensor_ID', range_y=[-20, 80], height=650)
        
        fig.update_layout(
            shapes=grid_shapes, 
            plot_bgcolor='white', 
            hovermode="x unified",
            margin=dict(l=20, r=150, t=50, b=20)
        )

        fig.update_xaxes(
            range=[start_view, end_view], 
            showgrid=False, 
            tickformat=tick_format,
            dtick=tick_spacing,  # <--- DYNAMIC SPACING
            tickangle=0          # Keeps labels horizontal for readability
        )
        
        fig.update_yaxes(showgrid=True, gridcolor='LightGrey', dtick=20)
        fig.add_hline(y=32, line_dash="dash", line_color="blue", annotation_text="32°F")
        
        st.plotly_chart(fig, use_container_width=True)
        
        st.plotly_chart(fig, use_container_width=True)

    with tab3:
        st.subheader("Monday 6:00 AM Thermal Profile")
        st.write("Showing the temperature gradient by depth for the most recent Monday.")

        # 1. Identify the most recent Monday at 6 AM that exists in our data
        # We look for timestamps where the hour is 6 and day is Monday (weekday 0)
        monday_6am_df = loc_df[
            (loc_df['timestamp'].dt.weekday == 0) & 
            (loc_df['timestamp'].dt.hour == 6)
        ].copy()

        if not monday_6am_df.empty:
            # Get the very latest Monday 6AM available
            latest_monday_6am = monday_6am_df['timestamp'].max()
            snap_df = monday_6am_df[monday_6am_df['timestamp'] == latest_monday_6am]
            
            # Sort by depth so the line connects points in order
            snap_df = snap_df.sort_values('Depth')

            # 2. Build the Profile Chart
            # X = Temperature, Y = Depth
            fig_profile = px.line(
                snap_df, 
                x='value', 
                y='Depth', 
                markers=True,
                title=f"Snapshot: {latest_monday_6am.strftime('%A, %b %d at %H:%M UTC')}",
                labels={'value': 'Temperature (°F)', 'Depth': 'Depth (ft)'}
            )

            # 3. Professional Engineering Adjustments
            fig_profile.update_layout(
                plot_bgcolor='white',
                height=600,
                hovermode="y unified"
            )
            
            # Invert Y-axis so 0 (surface) is at the top
            fig_profile.update_yaxes(
                autorange="reversed", 
                gridcolor='LightGrey',
                zeroline=True,
                zerolinecolor='black'
            )
            
            fig_profile.update_xaxes(
                gridcolor='LightGrey',
                range=[-10, 80] # Match your other charts
            )

            # Add the Freezing Point reference line (Vertical this time!)
            fig_profile.add_vline(x=32, line_dash="dash", line_color="blue", annotation_text="32°F")

            st.plotly_chart(fig_profile, use_container_width=True, key="monday_profile_chart")
        else:
            st.info("No Monday 6:00 AM data points found in the current date range.")
