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
    
    col1, col2, col3 = st.columns(3)
    with col1:
        projs = sorted(full_df['Project'].dropna().unique())
        sel_proj = st.selectbox("Project", projs)
    with col2:
        locs = sorted(full_df[full_df['Project'] == sel_proj]['Location'].dropna().unique())
        sel_loc = st.selectbox("Location", locs)
    with col3:
        weeks_to_show = st.number_input("Weeks to Display", min_value=1, value=2)

    # Time Logic: Monday Midnight Start
    today = datetime.now().date()
    last_monday = today - timedelta(days=today.weekday())
    start_time = datetime.combine(last_monday, time.min) - timedelta(weeks=weeks_to_show - 1)
    end_time = datetime.now()

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

        # Vertical Monday Lines
        mondays = pd.date_range(start=start_time, end=end_time, freq='W-MON')
        for mon in mondays:
            fig.add_vline(x=mon.timestamp() * 1000, line_width=2.5, line_color="black", opacity=1)

        # X-Axis: Midnight Grid, No Buffer
        fig.update_xaxes(
            range=[start_time, end_time],
            showgrid=True, dtick=86400000.0, gridcolor='lightgrey',
            tickformat="%a\n%b %d", automargin=True
        )

        # Y-Axis: Major every 20, Minor every 5
        fig.update_yaxes(
            tick0=-20, dtick=20, # Major ticks (labeled)
            minor=dict(dtick=5, gridcolor='GhostWhite', gridwidth=0.5), # Minor grid
            showgrid=True, gridcolor='LightGrey'
        )

        fig.update_layout(
            plot_bgcolor='white',
            margin=dict(l=0, r=150, t=30, b=0),
            legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.02),
            hovermode="x unified"
        )
        
        fig.update_traces(connectgaps=True)
        fig.add_hline(y=32, line_dash="dash", line_color="blue", annotation_text="32°F")
        
        st.plotly_chart(fig, use_container_width=True)

        # Download Button for current view
        csv_view = plot_df.sort_values('timestamp').to_csv(index=False).encode('utf-8')
        st.download_button("📥 Download Current Graph Data (CSV)", data=csv_view, file_name=f"QuickView_{sel_proj}_{sel_loc}.csv")
    else:
        st.info("No data found for this selection.")

# --- SERVICE: DATA EXPORT LAB ---
elif service == "📥 Data Export Lab" and not full_df.empty:
    st.header("📥 Bulk Data Export")
    
    # 1. Date Range
    d_col1, d_col2 = st.columns(2)
    with d_col1:
        start_d = st.date_input("Start Date", value=date.today() - timedelta(days=30))
    with d_col2:
        end_d = st.date_input("End Date", value=date.today())

    # 2. Scope Controls
    s_col1, s_col2, s_col3 = st.columns(3)
    with s_col1:
        ex_projs = sorted(full_df['Project'].dropna().unique())
        sel_ex_proj = st.selectbox("Project", ex_projs)
        ex_df = full_df[full_df['Project'] == sel_ex_proj]
    
    with s_col2:
        ex_locs = ["All Locations"] + sorted(ex_df['Location'].dropna().unique().tolist())
        sel_ex_loc = st.selectbox("Location", ex_locs)
        if sel_ex_loc != "All Locations":
            ex_df = ex_df[ex_df['Location'] == sel_ex_loc]

    with s_col3:
        ex_nodes = ["All Nodes"] + sorted(ex_df['nodenumber'].unique().tolist())
        sel_ex_node = st.selectbox("Node/Serial", ex_nodes)
        if sel_ex_node != "All Nodes":
            ex_df = ex_df[ex_df['nodenumber'] == sel_ex_node]

    # Final Filter
    final_ex_df = ex_df[(ex_df['timestamp'].dt.date >= start_d) & (ex_df['timestamp'].dt.date <= end_d)]
    
    st.write(f"📊 Found **{len(final_ex_df)}** records.")
    st.dataframe(final_ex_df.head(100), use_container_width=True)

    if not final_ex_df.empty:
        csv_bulk = final_ex_df.sort_values(['Project', 'timestamp']).to_csv(index=False).encode('utf-8')
        st.download_button("📥 Download Bulk Export (CSV)", data=csv_bulk, file_name=f"SoilFreeze_Export_{sel_ex_proj}.csv")

# --- SERVICE: DATA CLEANING TOOL ---
elif service == "🧹 Data Cleaning Tool" and not full_df.empty:
    st.header("🧹 Data Cleaning Tool")
    min_v, max_v = st.slider("Valid Temperature Range (°F)", -60.0, 100.0, (-20.0, 80.0))
    cleaned_df = full_df[(full_df['value'] >= min_v) & (full_df['value'] <= max_v)]
    st.success(f"Original: {len(full_df)} | Cleaned: {len(cleaned_df)}")
    st.dataframe(cleaned_df.head(200))
