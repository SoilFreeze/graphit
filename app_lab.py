import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date

# --- 1. AUTHENTICATION ---
# --- 1. AUTHENTICATION (Updated for Google Sheets Access) ---
# We need to add 'drive' to the scopes so BigQuery can read the metadata sheet
SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform"
]

if "gcp_service_account" in st.secrets:
    info = st.secrets["gcp_service_account"]
    # We add 'scopes=SCOPES' here
    credentials = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    client = bigquery.Client(credentials=credentials, project=info["project_id"])
else:
    # For local testing, ensure your .json key also has Drive access
    client = bigquery.Client.from_service_account_json("service_account.json", scopes=SCOPES)
# --- 2. DATA PULL (Standardized Columns) ---
@st.cache_data(ttl=600)
def fetch_engineering_data():
    # This query uses the exact columns from your Master Metadata screenshot
    query = """
    WITH raw_combined AS (
        SELECT CAST(timestamp AS TIMESTAMP) as timestamp, value, nodenumber 
        FROM `sensorpush-export.sensor_data.raw_lord`
        UNION ALL
        SELECT timestamp, temperature AS value, sensor_name AS nodenumber 
        FROM `sensorpush-export.sensor_data.raw_sensorpush`
    )
    SELECT 
        r.timestamp, 
        r.value, 
        r.nodenumber, 
        m.Project, 
        m.Location, 
        m.Depth
    FROM raw_combined AS r
    LEFT JOIN `sensorpush-export.sensor_data.master_metadata` AS m
      ON r.nodenumber = m.NodeNum
    """
    df = client.query(query).to_dataframe()
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df

try:
    full_df = fetch_engineering_data()
except Exception as e:
    st.sidebar.error(f"Error fetching data: {e}")
    full_df = pd.DataFrame()

# --- 3. SIDEBAR NAVIGATION ---
st.sidebar.title("🛠 Engineering Hub")
service = st.sidebar.selectbox(
    "Select Service",
    ["🔍 Node Diagnostics", "📥 Data Export Lab", "🧹 Data Cleaning Tool"]
)

# --- SERVICE: NODE DIAGNOSTICS ---
if service == "🔍 Node Diagnostics" and not full_df.empty:
    st.header("🔍 Node Diagnostics")
    
    # ... your existing filter code (col1, col2, etc.) ...
    
    # 2. DATA PREP
    loc_data = full_df[(full_df['Project'] == sel_proj) & (full_df['Location'] == sel_loc)].copy()
    loc_data['display_name'] = loc_data['nodenumber'].astype(str) + " | Depth: " + loc_data['Depth'].astype(str)

    # 3. LINE CONTROLS
    selected_displays = st.multiselect("Toggle Lines On/Off:", options=sorted(loc_data['display_name'].unique()), default=loc_data['display_name'].unique())

    # 4. FILTERED PLOT DATA
    plot_df = loc_data[loc_data['display_name'].isin(selected_displays)].sort_values('timestamp')

    # --- 5. RENDER THE GRAPH (MUST BE INDENTED UNDER THIS 'IF' BLOCK) ---
    if not plot_df.empty:
        fig = px.line(
            plot_df, 
            x='timestamp', 
            y='value', 
            color='display_name',
            title=f"Location: {sel_loc}"
        )
        
        fig.update_layout(legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.02))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Please select sensors to view the graph.")

# --- SERVICE: DATA EXPORT LAB ---
elif service == "📥 Data Export Lab":
    # The export code goes here...
    # --- 4. RENDER THE GRAPH ---
if not plot_df.empty:
    # Double-check that these names match your SQL 'SELECT' exactly!
    fig = px.line(
        plot_df, 
        x='timestamp', 
        y='value',        # Ensure this isn't 'temperature'
        color='nodenumber', # Ensure this matches your unified column name
        title=f"Location: {sel_loc} | {len(selected_nodes)} Nodes Active",
        hover_data=['Depth'] # Case sensitive! Matches your screenshot 'Depth'
    )
    
    # This cleans up the legend layout for your manager
    fig.update_layout(
        legend=dict(orientation="h", y=-0.2),
        margin=dict(l=20, r=20, t=40, b=20)
    )
    
    st.plotly_chart(fig, use_container_width=True)

# --- SERVICE: DATA EXPORT LAB ---
elif service == "📥 Data Export Lab" and not full_df.empty:
    st.header("📥 Data Export Lab")
    
    # 1. DATE SELECTION
    col_a, col_b = st.columns(2)
    with col_a:
        start_d = st.date_input("Start Date", value=date.today() - pd.Timedelta(days=14))
    with col_b:
        end_d = st.date_input("End Date", value=date.today())

    # 2. HIERARCHICAL FILTERS
    st.markdown("---")
    st.subheader("Filter Scope")
    f_col1, f_col2, f_col3 = st.columns(3)
    
    with f_col1:
        # Project Filter (Required)
        export_projs = sorted(full_df['Project'].dropna().unique())
        sel_export_proj = st.selectbox("Select Project to Export", export_projs)
        filtered_export = full_df[full_df['Project'] == sel_export_proj]

    with f_col2:
        # Location Filter (Optional - Add "All" option)
        export_locs = ["All Locations"] + sorted(filtered_export['Location'].dropna().unique().tolist())
        sel_export_loc = st.selectbox("Select Location", export_locs)
        if sel_export_loc != "All Locations":
            filtered_export = filtered_export[filtered_export['Location'] == sel_export_loc]

    with f_col3:
        # Node Filter (Optional - Add "All" option)
        export_nodes = ["All Nodes"] + sorted(filtered_export['nodenumber'].unique().tolist())
        sel_export_node = st.selectbox("Select Specific Node", export_nodes)
        if sel_export_node != "All Nodes":
            filtered_export = filtered_export[filtered_export['nodenumber'] == sel_export_node]

    # 3. APPLY DATE FILTER & PREVIEW
    final_export_df = filtered_export[
        (filtered_export['timestamp'].dt.date >= start_d) & 
        (filtered_export['timestamp'].dt.date <= end_d)
    ].sort_values('timestamp')

    st.write(f"📊 **Rows found:** {len(final_export_df)}")
    st.dataframe(final_export_df.head(500), use_container_width=True)

    # 4. DOWNLOAD BUTTON
    if not final_export_df.empty:
        # Naming the file based on selection for easier organization
        file_tag = f"{sel_export_proj}_{sel_export_loc}_{sel_export_node}".replace(" ", "_")
        csv = final_export_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download This Selection as CSV",
            data=csv,
            file_name=f"SoilFreeze_Export_{file_tag}.csv",
            mime="text/csv"
        )
    else:
        st.warning("No data found for the selected filters and date range.")

# --- SERVICE: DATA CLEANING TOOL ---
elif service == "🧹 Data Cleaning Tool" and not full_df.empty:
    st.header("🧹 Data Cleaning Tool")
    st.markdown("Use this to filter out outliers (like sensor open-circuit errors) from your view.")
    
    min_t, max_t = st.slider("Keep values between (°C)", -60, 100, (-40, 50))
    
    clean_df = full_df[(full_df['value'] >= min_t) & (full_df['value'] <= max_t)]
    st.success(f"Original: {len(full_df)} | Cleaned: {len(clean_df)} (Removed {len(full_df)-len(clean_df)} points)")
    
    # Preview cleaned data
    st.dataframe(clean_df.head(100))
