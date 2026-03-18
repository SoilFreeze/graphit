import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date

# --- 1. AUTHENTICATION (Includes Drive Scopes) ---
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
    # FILTERS
    col1, col2 = st.columns(2)
    with col1:
        projs = sorted(full_df['Project'].dropna().unique())
        sel_proj = st.selectbox("Project", projs)
    with col2:
        locs = sorted(full_df[full_df['Project'] == sel_proj]['Location'].dropna().unique())
        sel_loc = st.selectbox("Location", locs)

    # DATA PREP
    loc_data = full_df[(full_df['Project'] == sel_proj) & (full_df['Location'] == sel_loc)].copy()
    loc_data['display_name'] = loc_data['nodenumber'].astype(str) + " | Depth: " + loc_data['Depth'].astype(str)

    # TOGGLES
    st.markdown("---")
    available_lines = sorted(loc_data['display_name'].unique().tolist())
    selected_lines = st.multiselect("Toggle Lines On/Off:", options=available_lines, default=available_lines)

    # FILTERED PLOT DATA
    plot_df = loc_data[loc_data['display_name'].isin(selected_lines)].sort_values('timestamp')

    # --- 4. THE GRAPH (Standardized Y-Axis) ---
    if not plot_df.empty:
        fig = px.line(
            plot_df, 
            x='timestamp', 
            y='value', 
            color='display_name',
            title=f"Site: {sel_proj} | Location: {sel_loc}",
            labels={'display_name': 'Sensor', 'value': 'Temp (°F)'},
            # THIS LOCKS THE VIEW TO -20 to 80
            range_y=[-20, 80] 
        )

        fig.update_layout(
            legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.02),
            margin=dict(r=150),
            hovermode="x unified"
        )
        
        # Connect gaps and add a horizontal line at Freezing (32°F) for reference
        fig.update_traces(connectgaps=True)
        fig.add_hline(y=32, line_dash="dash", line_color="blue", annotation_text="Freezing (32°F)")
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Select sensors to display the graph.")

    # --- 5. EXPORT SECTION ---
    st.markdown("---")
    st.subheader("📥 Export Data")
    if st.button("Generate CSV for this Selection"):
        csv = plot_df.to_csv(index=False).encode('utf-8')
        st.download_button("Download CSV", data=csv, file_name=f"SoilFreeze_{sel_proj}.csv", mime='text/csv')

else:
    st.warning("Waiting for data from BigQuery...")
