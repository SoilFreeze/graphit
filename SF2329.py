import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz

# --- 1. PAGE SETUP & CHART ENGINE ---
st.set_page_config(layout="wide", page_title="SF Project Dashboard")

def build_standard_chart(df, title, x_col='timestamp', y_col='value', color_col='Sensor_ID'):
    """Unified SoilFreeze Plotly Engine"""
    if df.empty: return None
    fig = px.line(df, x=x_col, y=y_col, color=color_col, range_y=[-20, 80], height=600)
    fig.update_traces(connectgaps=False, hovertemplate="<b>%{fullData.name}</b><br>Temp: %{y:.2f}°F<extra></extra>")
    fig.update_layout(
        plot_bgcolor='white', hovermode="x unified",
        title=dict(text=title, font=dict(size=20)),
        legend=dict(x=1.02)
    )
    fig.update_xaxes(showgrid=True, gridcolor='LightGrey')
    fig.update_yaxes(showgrid=True, gridcolor='LightGrey', dtick=20)
    fig.add_hline(y=32, line_dash="dash", line_color="blue", annotation_text="32°F")
    return fig

# --- 2. DATA FETCHING (APPROVED DATA ONLY) ---
PROJECT_ID = "2329" # Update this for each file (e.g., 2538, 2541)

@st.cache_data(ttl=600)
def fetch_approved_data(pid):
    info = st.secrets["gcp_service_account"]
    credentials = service_account.Credentials.from_service_account_info(info)
    client = bigquery.Client(credentials=credentials, project=info["project_id"])
    
    query = f"""
    SELECT * FROM `sensorpush-export.sensor_data.final_databoard_data`
    WHERE Project = '{pid}' AND is_approved = TRUE
    """
    df = client.query(query).to_dataframe()
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df

df = fetch_approved_data(PROJECT_ID)

# --- 3. DASHBOARD LAYOUT ---
st.title(f"❄️ Project {PROJECT_ID} Dashboard")

if df.empty:
    st.warning("⏳ **Notice:** Data for this project is currently undergoing engineering review.")
else:
    # Sidebar Filters
    with st.sidebar:
        st.header("Filters")
        all_locs = sorted(df['Location'].unique())
        sel_loc = st.selectbox("Select Pipe / Bank", all_locs)
        
    # Filter data for selected location
    loc_df = df[df['Location'] == sel_loc].copy()
    loc_df['Sensor_ID'] = "Depth: " + loc_df['Depth'].astype(str) + "'"

    # Show Engineer Note if it exists
    latest_note = loc_df.sort_values('timestamp', ascending=False)['engineer_note'].iloc[0]
    if latest_note and str(latest_note) != 'None':
        st.info(f"📝 **Engineer's Message:** {latest_note}")

    # --- TABS INTERFACE ---
    tab1, tab2, tab3 = st.tabs(["📊 Site Health", "📈 Temp vs Time", "📉 Depth vs Time"])

    with tab1:
        st.subheader(f"Overall Site Status: {sel_loc}")
        # Logic for Min, Max, and Max Change (24h Window)
        cutoff = loc_df['timestamp'].max() - timedelta(hours=24)
        recent_24 = loc_df[loc_df['timestamp'] >= cutoff]
        
        summary_data = []
        for node in recent_24['nodenumber'].unique():
            n_df = recent_24[recent_24['nodenumber'] == node].sort_values('timestamp')
            if not n_df.empty:
                summary_data.append({
                    "Depth": n_df['Depth'].iloc[0],
                    "Node": node,
                    "Min Temp": n_df['value'].min(),
                    "Max Temp": n_df['value'].max(),
                    "24h Change": n_df['value'].iloc[-1] - n_df['value'].iloc[0]
                })
        
        summary_df = pd.DataFrame(summary_data).sort_values('Depth')
        st.table(summary_df.style.format({
            "Min Temp": "{:.2f}°F", "Max Temp": "{:.2f}°F", "24h Change": "{:+.2f}°F"
        }))

    with tab2:
        st.subheader("Temperature vs. Time")
        fig_time = build_standard_chart(loc_df, f"Thermal History: {sel_loc}")
        st.plotly_chart(fig_time, use_container_width=True)

    with tab3:
        st.subheader("Depth vs. Time (Heat Profile)")
        # We pivot the data to create a Depth profile chart
        # X = Time, Y = Depth, Color = Temp
        fig_depth = px.line(
            loc_df, x='timestamp', y='value', color='Depth',
            title=f"Depth Profile: {sel_loc}",
            labels={'value': 'Temperature (°F)', 'timestamp': 'Date'},
            height=600
        )
        fig_depth.update_yaxes(autorange="reversed") # Depth charts usually go top-down
        st.plotly_chart(fig_depth, use_container_width=True)
