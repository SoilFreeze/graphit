import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta, time

# --- 1. AUTHENTICATION ---
SCOPES = ["https://www.googleapis.com/auth/drive.readonly", "https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform"]

if "gcp_service_account" in st.secrets:
    info = st.secrets["gcp_service_account"]
    credentials = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    client = bigquery.Client(credentials=credentials, project=info["project_id"])
else:
    client = bigquery.Client.from_service_account_json("service_account.json", scopes=SCOPES)

# --- 2. DATA FETCHING ---
@st.cache_data(ttl=600)
def fetch_engineering_data():
    query = """
    WITH raw_combined AS (
        SELECT CAST(timestamp AS TIMESTAMP) as timestamp, value, nodenumber FROM `sensorpush-export.sensor_data.raw_lord`
        UNION ALL
        SELECT timestamp, temperature AS value, sensor_name AS nodenumber FROM `sensorpush-export.sensor_data.raw_sensorpush`
    )
    SELECT r.timestamp, r.value, r.nodenumber, m.Project, m.Location, m.Depth
    FROM raw_combined AS r
    LEFT JOIN `sensorpush-export.sensor_data.master_metadata` AS m ON r.nodenumber = m.NodeNum
    """
    df = client.query(query).to_dataframe()
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df

full_df = pd.DataFrame()
try:
    full_df = fetch_engineering_data()
except Exception as e:
    st.sidebar.error(f"Database Error: {e}")

# --- 3. SIDEBAR NAVIGATION ---
st.sidebar.title("🛠 Engineering Hub")
service = st.sidebar.selectbox("Select Service", ["🔍 Node Diagnostics", "📥 Data Export Lab", "🧹 Data Cleaning Tool"])

# --- SERVICE: NODE DIAGNOSTICS ---
if service == "🔍 Node Diagnostics" and not full_df.empty:
    st.header("🔍 Node Diagnostics")
    
    # Selection Row
    col1, col2, col3 = st.columns(3)
    with col1:
        projs = sorted(full_df['Project'].dropna().unique())
        sel_proj = st.selectbox("Project", projs)
    with col2:
        locs = sorted(full_df[full_df['Project'] == sel_proj]['Location'].dropna().unique())
        sel_loc = st.selectbox("Location", locs)
    with col3:
        weeks_to_show = st.number_input("Weeks to Display", min_value=1, max_value=52, value=2)

    # 1. TIME LOGIC: Start exactly at most recent Monday Midnight
    # Calculate the most recent Monday 00:00:00
    today = datetime.now().date()
    last_monday = today - timedelta(days=today.weekday())
    start_time = datetime.combine(last_monday, time.min) - timedelta(weeks=weeks_to_show - 1)
    end_time = datetime.now()

    # 2. DATA PREP
    plot_df = full_df[
        (full_df['Project'] == sel_proj) & 
        (full_df['Location'] == sel_loc) &
        (full_df['timestamp'] >= pd.Timestamp(start_time, tz='UTC'))
    ].copy()
    plot_df['Sensor_ID'] = plot_df['nodenumber'].astype(str) + " | Depth: " + plot_df['Depth'].astype(str)

    if not plot_df.empty:
        fig = px.line(
            plot_df, x='timestamp', y='value', color='Sensor_ID',
            labels={'value': 'Temp (°F)'},
            range_y=[-20, 80]
        )

        # 3. MONDAY & DAILY GRID LOGIC
        mondays = pd.date_range(start=start_time, end=end_time, freq='W-MON')
        for mon in mondays:
            fig.add_vline(x=mon.timestamp() * 1000, line_width=2.5, line_color="black", opacity=1)

        # 4. AXIS CONFIG (Zero buffer + Daily Grid)
        fig.update_xaxes(
            range=[start_time, end_time], # Forces start at Monday midnight
            showgrid=True,
            dtick=86400000.0, # Exactly 1 day in ms
            gridcolor='lightgrey',
            tickformat="%a\n%b %d",
            automargin=True,
            showline=True, linewidth=2, linecolor='black', mirror=True # Box effect
        )

        fig.update_layout(
            plot_bgcolor='white',
            margin=dict(l=0, r=150, t=30, b=0), # Zero buffer on left/right
            legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.02),
            hovermode="x unified"
        )
        
        fig.update_traces(connectgaps=True)
        fig.add_hline(y=32, line_dash="dash", line_color="blue", annotation_text="32°F")
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info(f"No data found starting from Monday, {last_monday}.")

# --- SERVICE: DATA EXPORT LAB ---
elif service == "📥 Data Export Lab" and not full_df.empty:
    st.header("📥 Data Export Lab")
    # Restore the export code... (Standard project/date filters)
    st.dataframe(full_df.head(100))
    csv = full_df.to_csv(index=False).encode('utf-8')
    st.download_button("Download Data", data=csv, file_name="SoilFreeze_Export.csv")

# --- SERVICE: DATA CLEANING TOOL ---
elif service == "🧹 Data Cleaning Tool" and not full_df.empty:
    st.header("🧹 Data Cleaning Tool")
    # Restore cleaning slider...
    min_v, max_v = st.slider("Temp Filter", -60.0, 100.0, (-40.0, 50.0))
    st.write(f"Filtering between {min_v} and {max_v}")
