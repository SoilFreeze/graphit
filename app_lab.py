import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date

# --- 1. AUTHENTICATION (Includes Drive Scopes for Google Sheets) ---
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
    # Local Desktop path
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

# Initialize Data
try:
    full_df = fetch_engineering_data()
except Exception as e:
    st.sidebar.error(f"Database Error: {e}")
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
    
    col1, col2 = st.columns(2)
    with col1:
        projs = sorted(full_df['Project'].dropna().unique())
        sel_proj = st.selectbox("Select Project", projs)
    with col2:
        locs = sorted(full_df[full_df['Project'] == sel_proj]['Location'].dropna().unique())
        sel_loc = st.selectbox("Select Location", locs)

    # Prepare Data for this Location
    loc_data = full_df[(full_df['Project'] == sel_proj) & (full_df['Location'] == sel_loc)].copy()
    
    # Create combined Legend Label: "Node | Depth"
    loc_data['display_name'] = (
        loc_data['nodenumber'].astype(str) + " | Depth: " + loc_data['Depth'].astype(str)
    )

    # Line Controls (On/Off Toggles)
    st.markdown("### 📈 Line Controls")
    available_lines = sorted(loc_data['display_name'].unique().tolist())
    selected_lines = st.multiselect(
        "Toggle sensors on/off:",
        options=available_lines,
        default=available_lines
    )

    # Filtered Data for Plotting
    plot_df = loc_data[loc_data['display_name'].isin(selected_lines)].sort_values('timestamp')

    if not plot_df.empty:
        fig = px.line(
            plot_df, 
            x='timestamp', 
            y='value', 
            color='display_name',
            title=f"Project: {sel_proj} | Location: {sel_loc}",
            labels={'display_name': 'Sensor (Node | Depth)', 'value': 'Temp (°C)'}
        )

        # Move Legend to the Right Side
        fig.update_layout(
            legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.02),
            margin=dict(r=150),
            hovermode="x unified"
        )
        
        # Connect gaps for smoother viewing
        fig.update_traces(connectgaps=True)
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Select sensors from the toggle list to view the graph.")

# --- SERVICE: DATA EXPORT LAB ---
elif service == "📥 Data Export Lab" and not full_df.empty:
    st.header("📥 Data Export Lab")
    
    # Filter Controls
    e_col1, e_col2 = st.columns(2)
    with e_col1:
        start_date = st.date_input("Start Date", value=date.today() - pd.Timedelta(days=14))
    with e_col2:
        end_date = st.date_input("End Date", value=date.today())

    st.markdown("---")
    f_col1, f_col2, f_col3 = st.columns(3)
    
    with f_col1:
        ex_projs = sorted(full_df['Project'].dropna().unique())
        sel_ex_proj = st.selectbox("Project to Export", ex_projs)
        ex_df = full_df[full_df['Project'] == sel_ex_proj]
    
    with f_col2:
        ex_locs = ["All Locations"] + sorted(ex_df['Location'].dropna().unique().tolist())
        sel_ex_loc = st.selectbox("Location Filter", ex_locs)
        if sel_ex_loc != "All Locations":
            ex_df = ex_df[ex_df['Location'] == sel_ex_loc]
            
    with f_col3:
        ex_nodes = ["All Nodes"] + sorted(ex_df['nodenumber'].unique().tolist())
        sel_ex_node = st.selectbox("Node Filter", ex_nodes)
        if sel_ex_node != "All Nodes":
            ex_df = ex_df[ex_df['nodenumber'] == sel_ex_node]

    # Final Filter
    final_df = ex_df[(ex_df['timestamp'].dt.date >= start_date) & (ex_df['timestamp'].dt.date <= end_date)]
    
    st.write(f"📊 Found **{len(final_df)}** rows.")
    st.dataframe(final_df.head(100), use_container_width=True)

    if not final_df.empty:
        csv = final_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "📥 Download Selection as CSV", 
            data=csv, 
            file_name=f"SoilFreeze_{sel_ex_proj}.csv", 
            mime='text/csv'
        )

# --- SERVICE: DATA CLEANING TOOL ---
elif service == "🧹 Data Cleaning Tool" and not full_df.empty:
    st.header("🧹 Data Cleaning Tool")
    st.info("Adjust the slider to remove noise or erroneous sensor spikes.")
    
    min_val, max_val = st.slider("Valid Temperature Range (°C)", -60.0, 100.0, (-40.0, 50.0))
    
    cleaned_df = full_df[(full_df['value'] >= min_val) & (full_df['value'] <= max_val)]
    
    st.success(f"Original Count: {len(full_df)} | Cleaned Count: {len(cleaned_df)}")
    st.write(f"**Removed {len(full_df) - len(cleaned_df)} outlier points.**")
    
    st.dataframe(cleaned_df.head(200), use_container_width=True)
