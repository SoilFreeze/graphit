import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date

# --- 1. AUTHENTICATION ---
SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform"
]

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
        SELECT CAST(timestamp AS TIMESTAMP) as timestamp, value, nodenumber 
        FROM `sensorpush-export.sensor_data.raw_lord`
        UNION ALL
        SELECT timestamp, temperature AS value, sensor_name AS nodenumber 
        FROM `sensorpush-export.sensor_data.raw_sensorpush`
    )
    SELECT 
        r.timestamp, r.value, r.nodenumber, 
        m.Project, m.Location, m.Depth
    FROM raw_combined AS r
    LEFT JOIN `sensorpush-export.sensor_data.master_metadata` AS m
      ON r.nodenumber = m.NodeNum
    """
    df = client.query(query).to_dataframe()
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df

# Initialize Data
full_df = pd.DataFrame()
try:
    full_df = fetch_engineering_data()
except Exception as e:
    st.sidebar.error(f"Database Error: {e}")

# --- 3. MAIN INTERFACE ---
st.title("🛠 SoilFreeze Engineering Hub")

if not full_df.empty:
    # 1. SIMPLE SELECTORS
    col1, col2 = st.columns(2)
    with col1:
        projs = sorted(full_df['Project'].dropna().unique())
        sel_proj = st.selectbox("Project", projs)
    with col2:
        locs = sorted(full_df[full_df['Project'] == sel_proj]['Location'].dropna().unique())
        sel_loc = st.selectbox("Location", locs)

    # 2. DATA PREP
    # We combine Node and Depth for the legend automatically
    plot_df = full_df[(full_df['Project'] == sel_proj) & (full_df['Location'] == sel_loc)].copy()
    plot_df['Sensor_ID'] = plot_df['nodenumber'].astype(str) + " | Depth: " + plot_df['Depth'].astype(str)
    plot_df = plot_df.sort_values('timestamp')

    # --- 3. THE GRAPH (Standardized -20 to 80°F) ---
    if not plot_df.empty:
        fig = px.line(
            plot_df, 
            x='timestamp', 
            y='value', 
            color='Sensor_ID',
            title=f"Site: {sel_proj} | Location: {sel_loc}",
            labels={'Sensor_ID': 'Sensor', 'value': 'Temp (°F)'},
            range_y=[-20, 80] # Fixed range as requested
        )

        fig.update_layout(
            legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.02),
            margin=dict(r=150),
            hovermode="x unified"
        )
        
        fig.update_traces(connectgaps=True)
        fig.add_hline(y=32, line_dash="dash", line_color="blue", annotation_text="32°F")
        
        st.plotly_chart(fig, use_container_width=True)

        # 4. EXPORT BUTTON
        st.markdown("---")
        csv = plot_df.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Download Current View as CSV", data=csv, file_name=f"SoilFreeze_{sel_proj}.csv", mime='text/csv')
    else:
        st.info("No data found for this location.")

else:
    st.warning("Fetching data from BigQuery...")
