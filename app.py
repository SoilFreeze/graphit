import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz
import math

# --- 1. CONFIG & SCROLL FIX ---
st.set_page_config(layout="wide", page_title="SF Technician Dashboard")
st.markdown("""<style>.main .block-container {overflow-y: auto !important; height: auto !important;}</style>""", unsafe_allow_html=True)

# --- 2. AUTHENTICATION ---
if "gcp_service_account" in st.secrets:
    info = st.secrets["gcp_service_account"]
    scopes = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive.readonly"]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    client = bigquery.Client(credentials=creds, project=info["project_id"])
else:
    st.error("Secrets missing.")
    st.stop()

# --- 3. DATA FETCH ---
@st.cache_data(ttl=300)
def fetch_data():
    query = """
    SELECT d.timestamp, d.value, d.nodenumber, m.Project, m.Location, m.Depth
    FROM `sensorpush-export.sensor_data.final_databoard_data` as d
    INNER JOIN `sensorpush-export.sensor_data.master_metadata` as m ON d.nodenumber = m.NodeNum
    ORDER BY d.timestamp ASC
    """
    df = client.query(query).to_dataframe()
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df

full_df = fetch_data()

# --- 4. SIDEBAR & CELSIUS OPTION ---
st.sidebar.title("🛠️ Tech Operations")
unit = st.sidebar.radio("Temp Unit", ["Fahrenheit (°F)", "Celsius (°C)"])
is_celsius = unit == "Celsius (°C)"

all_projs = sorted(full_df['Project'].unique().tolist())
sel_proj = st.sidebar.selectbox("Select Project", all_projs)
df_proj = full_df[full_df['Project'] == sel_proj].copy()

# Apply Celsius conversion if selected
if is_celsius:
    df_proj['value'] = (df_proj['value'] - 32) * 5/9

num_weeks = st.sidebar.slider("Weeks of History", 1, 12, 4)

# --- 5. UI TABS ---
tab_health, tab_history, tab_depth = st.tabs(["📡 System Health", "📈 Time History", "📏 Depth Profiles"])

# TAB 1: HEALTH
with tab_health:
    st.subheader(f"24-Hour Summary: {sel_proj}")
    # Show the last 20 readings for a quick glance
    st.dataframe(df_proj.tail(20), use_container_width=True, hide_index=True)

# TAB 2: TIME HISTORY
with tab_history:
    locs = sorted(df_proj['Location'].unique())
    sel_loc = st.selectbox("Select Location", locs)
    # Downsample slightly to ensure no freezing
    time_df = df_proj[df_proj['Location'] == sel_loc].iloc[::5] 
    
    fig_time = px.line(time_df, x='timestamp', y='value', color='Depth', title=f"History for {sel_loc}")
    st.plotly_chart(fig_time, use_container_width=True)

# TAB 3: DEPTH PROFILES (Temp vs Depth)
with tab_depth:
    st.subheader("Temperature vs. Depth Profile")
    # Filter for pipes (exclude banks)
    pipes = [l for l in sorted(df_proj['Location'].unique()) if "bank" not in l.lower()]
    
    if pipes:
        sel_pipe = st.selectbox("Select Pipe", pipes)
        pipe_df = df_proj[df_proj['Location'] == sel_pipe].copy()
        
        # Snapshot Logic: Find data points closest to Monday mornings
        now_utc = datetime.now(tz=pytz.UTC)
        mondays = [ (now_utc - timedelta(days=now_utc.weekday()) - timedelta(weeks=i)).replace(hour=6, minute=0) for i in range(num_weeks)]
        
        all_snaps = []
        for m in mondays:
            # Look for data within a 6 hour window of Monday 6AM
            snap = pipe_df[(pipe_df['timestamp'] >= m - timedelta(hours=3)) & 
                           (pipe_df['timestamp'] <= m + timedelta(hours=3))].copy()
            if not snap.empty:
                snap['Date'] = m.strftime('%b %d')
                all_snaps.append(snap)
        
        if all_snaps:
            plot_df = pd.concat(all_snaps)
            plot_df['Depth'] = pd.to_numeric(plot_df['Depth'], errors='coerce')
            
            # Create the Depth Profile (X=Temp, Y=Depth)
            fig_prof = px.line(plot_df.sort_values('Depth'), x='value', y='Depth', color='Date', markers=True)
            
            # Invert Y axis so Depth 0 is at the top (Ground Surface)
            fig_prof.update_yaxes(autorange="reversed", title="Depth (ft)")
            fig_prof.update_xaxes(title=f"Temperature ({'°C' if is_celsius else '°F'})")
            
            st.plotly_chart(fig_prof, use_container_width=True)
    else:
        st.info("No Pipe locations found for this project.")
