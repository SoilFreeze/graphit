import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz

# --- 1. PAGE SETUP ---
st.set_page_config(layout="wide", page_title="SF Project Dashboard", page_icon="❄️")

# --- 2. SHARED CHART ENGINE ---
def build_standard_chart(df, title, x_col='timestamp', y_col='value', color_col='Sensor_ID'):
    if df.empty: return None
    fig = px.line(df, x=x_col, y=y_col, color=color_col, range_y=[-20, 80], height=650)
    fig.update_traces(connectgaps=False, hovertemplate="<b>%{fullData.name}</b><br>Temp: %{y:.2f}°F<extra></extra>")
    fig.update_layout(
        plot_bgcolor='white', hovermode="x unified",
        margin=dict(l=20, r=150, t=50, b=20),
        legend=dict(x=1.02, font=dict(size=12)),
        title=dict(text=title, font=dict(size=22))
    )
    fig.update_xaxes(showgrid=True, gridcolor='LightGrey', tickformat="%a\n%b %d")
    fig.update_yaxes(showgrid=True, gridcolor='LightGrey', dtick=20)
    fig.add_hline(y=32, line_dash="dash", line_color="blue", annotation_text="32°F")
    return fig

# --- 3. DATA FETCHING (GATEKEEPER) ---
PROJECT_ID = "2329" 

from google.oauth2 import service_account # Ensure this is imported

@st.cache_data(ttl=600)
def fetch_project_data(pid):
    info = st.secrets["gcp_service_account"]
    
    # --- 💡 THE FIX: ADD DRIVE SCOPES ---
    # This tells Google "I need to talk to BigQuery AND read Drive files"
    scopes = [
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/cloud-platform"
    ]
    
    credentials = service_account.Credentials.from_service_account_info(
        info, scopes=scopes
    )
    
    client = bigquery.Client(credentials=credentials, project=info["project_id"])
    
    query = f"""
    SELECT 
        d.timestamp, d.value, d.nodenumber, d.is_approved, d.engineer_note,
        m.Project, m.Location, m.Depth
    FROM `sensorpush-export.sensor_data.final_databoard_data` as d
    INNER JOIN `sensorpush-export.sensor_data.master_metadata` as m 
      ON d.nodenumber = m.NodeNum
    WHERE m.Project = '{pid}' 
    AND d.is_approved = TRUE
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
df = fetch_project_data(PROJECT_ID)

if df.empty:
    st.warning("⏳ **Review in Progress:** Verified data will appear here once approved by Engineering.")
else:
    all_locs = sorted(df['Location'].dropna().unique())
    sel_loc = st.sidebar.selectbox("Select Pipe / Bank", all_locs)
    
    loc_df = df[df['Location'] == sel_loc].copy()
    loc_df['Sensor_ID'] = "Depth: " + loc_df['Depth'].astype(str) + "'"

    # Tabs
    tab1, tab2, tab3 = st.tabs(["📊 Site Health", "📈 Temp vs Time", "📉 Depth vs Time"])

    with tab1:
        st.subheader(f"📋 24-Hour Summary: {sel_loc}")
        now_utc = datetime.now(tz=pytz.UTC)
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
            
            summary_df = pd.DataFrame(summary).sort_values('Depth')
            st.table(summary_df.style.format("{:.2f}°F"))
        else:
            st.info("No approved data in the last 24 hours.")

    with tab2:
        st.subheader("Temperature vs Time")
        st.plotly_chart(build_standard_chart(loc_df, f"History: {sel_loc}"), use_container_width=True)

    with tab3:
        st.subheader("Depth Profile")
        # Depth vs Time Line Chart
        fig_depth = px.line(loc_df, x='timestamp', y='value', color='Depth', height=650)
        fig_depth.update_layout(plot_bgcolor='white', title=f"Thermal Profile: {sel_loc}")
        st.plotly_chart(fig_depth, use_container_width=True)
