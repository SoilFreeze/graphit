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
    if df.empty:
        return None
        
    fig = px.line(
        df, x=x_col, y=y_col, color=color_col, 
        range_y=[-20, 80], height=650
    )

    fig.update_traces(
        connectgaps=False, 
        hovertemplate="<b>%{fullData.name}</b><br>Temp: %{y:.2f}°F<extra></extra>"
    )

    fig.update_layout(
        plot_bgcolor='white', 
        hovermode="x unified",
        margin=dict(l=20, r=150, t=50, b=20),
        legend=dict(x=1.02, font=dict(size=12)),
        title=dict(text=title, font=dict(size=22))
    )
    
    fig.update_xaxes(showgrid=True, gridcolor='LightGrey', tickformat="%a\n%b %d")
    fig.update_yaxes(showgrid=True, gridcolor='LightGrey', dtick=20)
    fig.add_hline(y=32, line_dash="dash", line_color="blue", annotation_text="32°F")
    
    return fig

# --- 3. DATA FETCHING (GATEKEEPER LOGIC) ---
PROJECT_ID = "2329" # Change this for SF2538.py, SF2541.py, etc.

@st.cache_data(ttl=600)
def fetch_project_data(pid):
    info = st.secrets["gcp_service_account"]
    credentials = service_account.Credentials.from_service_account_info(info)
    client = bigquery.Client(credentials=credentials, project=info["project_id"])
    
    # We join metadata here to ensure Project/Location/Depth are tied to every row
    query = f"""
    SELECT 
        d.timestamp, 
        d.value, 
        d.nodenumber, 
        d.is_approved, 
        d.engineer_note,
        m.Project,
        m.Location,
        m.Depth
    FROM `sensorpush-export.sensor_data.final_databoard_data` as d
    JOIN `sensorpush-export.sensor_data.master_metadata` as m 
      ON d.nodenumber = m.NodeNum
    WHERE m.Project = '{pid}' 
    AND d.is_approved = TRUE
    ORDER BY d.timestamp ASC
    """
    df = client.query(query).to_dataframe()
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df

# --- 4. MAIN INTERFACE ---
st.title(f"❄️ Project {PROJECT_ID} Thermal Dashboard")
df = fetch_project_data(PROJECT_ID)

if df.empty:
    st.warning("⏳ **Notice:** Data for this project is currently undergoing engineering review and will be posted shortly.")
else:
    # Sidebar Filters
    all_locs = sorted(df['Location'].dropna().unique())
    sel_loc = st.sidebar.selectbox("Select Pipe / Bank", all_locs)
    
    # Filter & Prep
    loc_df = df[df['Location'] == sel_loc].copy()
    loc_df['Sensor_ID'] = "Depth: " + loc_df['Depth'].astype(str) + "'"

    # Show Engineer Note
    latest_note = loc_df.sort_values('timestamp', ascending=False)['engineer_note'].iloc[0]
    if latest_note and str(latest_note) != 'None':
        st.info(f"📝 **Engineer's Message:** {latest_note}")

    # --- THE TABS ---
    tab1, tab2, tab3 = st.tabs(["📊 Site Health", "📈 Temp vs Time", "📉 Depth vs Time"])

    with tab1:
        st.subheader(f"📋 24-Hour Summary: {sel_loc}")
        # Logic for Min/Max/Change
        now_utc = datetime.now(tz=pytz.UTC)
        recent_df = loc_df[loc_df['timestamp'] >= (now_utc - timedelta(hours=24))]
        
        if not recent_df.empty:
            summary_stats = []
            for node in recent_df['nodenumber'].unique():
                n_df = recent_df[recent_df['nodenumber'] == node].sort_values('timestamp')
                summary_stats.append({
                    "Depth": n_df['Depth'].iloc[0],
                    "Node ID": node,
                    "Min Temp": n_df['value'].min(),
                    "Max Temp": n_df['value'].max(),
                    "24h Change": n_df['value'].iloc[-1] - n_df['value'].iloc[0]
                })
            
            summary_df = pd.DataFrame(summary_stats).sort_values('Depth')
            
            def color_deltas(row):
                val = row['24h Change']
                if val >= 5.0: return ['background-color: #ff4b4b; color: white'] * len(row) # Warning
                if val <= -1.0: return ['background-color: #90ee90; color: black'] * len(row) # Cooling
                return [''] * len(row)

            st.table(summary_df.style.apply(color_deltas, axis=1).format({
                "Min Temp": "{:.2f}°F", "Max Temp": "{:.2f}°F", "24h Change": "{:+.2f}°F"
            }))
        else:
            st.info("No active data within the last 24 hours.")

    with tab2:
        st.subheader("Temperature History")
        fig_time = build_standard_chart(loc_df, f"Historical Readings: {sel_loc}")
        st.plotly_chart(fig_time, use_container_width=True)

    with tab3:
        st.subheader("Depth Profile (Freeze Front)")
        # For this tab, we want to see how temperatures behave relative to depth
        fig_depth = px.line(
            loc_df, x='timestamp', y='value', color='Depth',
            labels={'value': 'Temp (°F)', 'timestamp': 'Date'},
            height=650
        )
        fig_depth.update_layout(plot_bgcolor='white', hovermode='x unified')
        fig_depth.add_hline(y=32, line_dash="dash", line_color="blue")
        st.plotly_chart(fig_depth, use_container_width=True)
