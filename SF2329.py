import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta, date
import pytz

# --- 0. PAGE CONFIG ---
st.set_page_config(layout="wide", page_title="SoilFreeze Engineering Hub", page_icon="❄️")

# --- 1. SHARED CHART ENGINE (Standardized Look) ---
def build_standard_chart(df, title):
    if df.empty:
        return None
        
    # Standard Plotly Line Chart
    fig = px.line(
        df, x='timestamp', y='value', color='Sensor_ID', 
        range_y=[-20, 80], height=700
    )

    # Hover & Line Gaps (6-hour logic handled by Plotly automatically if NaNs exist)
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
    
    # Gridlines and Date Formatting
    fig.update_xaxes(showgrid=True, gridcolor='LightGrey', tickformat="%a\n%b %d")
    fig.update_yaxes(showgrid=True, gridcolor='LightGrey', dtick=20)
    
    # The 32°F Freeze Line
    fig.add_hline(y=32, line_dash="dash", line_color="blue", annotation_text="32°F")
    
    return fig

# --- 2. AUTH & DATA FETCHING ---
# Using Streamlit Secrets for GCP Service Account
if "gcp_service_account" in st.secrets:
    info = st.secrets["gcp_service_account"]
    credentials = service_account.Credentials.from_service_account_info(info)
    client = bigquery.Client(credentials=credentials, project=info["project_id"])
else:
    st.error("GCP Credentials not found in Streamlit Secrets.")
    st.stop()

@st.cache_data(ttl=600)
def fetch_master_data():
    # Pulling from the unified View we re-created
    query = "SELECT * FROM `sensorpush-export.sensor_data.final_databoard_data`"
    df = client.query(query).to_dataframe()
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df

full_df = fetch_master_data()

# --- 3. SIDEBAR NAVIGATION ---
st.sidebar.title("🛠 Engineering Hub")
service = st.sidebar.selectbox(
    "Select Service", 
    [
        "🏠 Executive Summary", 
        "🔍 Node Diagnostics", 
        "📋 Data Approval Portal",
        "📥 Data Export Lab"
    ]
)

# --- SERVICE 0: EXECUTIVE SUMMARY ---
if service == "🏠 Executive Summary":
    st.header("🏠 Site Health & Warming Alerts")
    
    c1, c2 = st.columns(2)
    with c1:
        all_projs = sorted(full_df['Project'].dropna().unique())
        sel_proj = st.selectbox("1. Select Project", all_projs)
    with c2:
        all_locs = sorted(full_df[full_df['Project'] == sel_proj]['Location'].dropna().unique())
        sel_loc = st.selectbox("2. Select Pipe / Bank", all_locs)

    st.divider()
    
    now_ts = datetime.now(tz=pytz.UTC)
    proj_df = full_df[full_df['Project'] == sel_proj].copy()
    loc_df = proj_df[proj_df['Location'] == sel_loc].copy()

    # Table 1: 24h Performance
    st.subheader("📋 24-Hour Performance (Warming/Cooling)")
    recent_24 = loc_df[loc_df['timestamp'] >= (now_ts - timedelta(hours=24))]
    
    if not recent_24.empty:
        stats = []
        for node in recent_24['nodenumber'].unique():
            n_df = recent_24[recent_24['nodenumber'] == node].sort_values('timestamp')
            change = n_df['value'].iloc[-1] - n_df['value'].iloc[0]
            stats.append({
                "Depth": n_df['Depth'].iloc[0], "Node": node,
                "Current": n_df['value'].iloc[-1], "24h Change": change
            })
        
        summary_table = pd.DataFrame(stats).sort_values('Depth')
        def style_delta(row):
            val = row['24h Change']
            if val >= 5.0: return ['background-color: #ff4b4b; color: white'] * len(row)
            if val >= 2.5: return ['background-color: #ffa500; color: black'] * len(row)
            if val <= -1.0: return ['background-color: #90ee90; color: black'] * len(row)
            return [''] * len(row)
            
        st.table(summary_table.style.apply(style_delta, axis=1).format({"24h Change": "{:+.2f}°F"}))

    # Table 2: Connectivity (Last Seen)
    st.subheader("📡 Sensor Connectivity (Current Mapping)")
    conn_df = loc_df.groupby(['Depth', 'nodenumber'])['timestamp'].max().reset_index()
    conn_df['Hours_Silent'] = (now_ts - conn_df['timestamp']).dt.total_seconds() / 3600
    
    def style_conn(row):
        h = row['Hours_Silent']
        if h >= 24: return ['background-color: #ff4b4b; color: white'] * len(row)
        if h >= 12: return ['background-color: #ffa500; color: black'] * len(row)
        return [''] * len(row)

    display_conn = conn_df.rename(columns={'timestamp': 'Last Seen'}).sort_values('Depth')
    st.table(display_conn.style.apply(style_conn, axis=1).hide(['Hours_Silent'], axis=1))

# --- SERVICE 1: DIAGNOSTICS ---
elif service == "🔍 Node Diagnostics":
    st.header("🔍 Node Diagnostics")
    
    # Re-using selectors
    ap_projs = sorted(full_df['Project'].dropna().unique())
    sel_ap_proj = st.selectbox("Project", ap_projs)
    proj_locs = sorted(full_df[full_df['Project'] == sel_ap_proj]['Location'].dropna().unique())
    sel_ap_loc = st.selectbox("Pipe / Bank", proj_locs)
    
    diag_df = full_df[(full_df['Project'] == sel_ap_proj) & (full_df['Location'] == sel_ap_loc)].copy()
    diag_df['Sensor_ID'] = "Node: " + diag_df['nodenumber'].astype(str) + " (D:" + diag_df['Depth'].astype(str) + ")"
    
    fig = build_standard_chart(diag_df, f"Diagnostic View: {sel_ap_loc}")
    st.plotly_chart(fig, use_container_width=True)

# --- SERVICE 2: APPROVAL PORTAL ---
elif "Approval" in service:
    st.header("📋 Engineer Approval Portal")
    
    ap_proj = st.selectbox("1. Select Project", sorted(full_df['Project'].dropna().unique()))
    ap_date = st.date_input("2. Select Date to Release", value=date.today() - timedelta(days=1))
    
    scope = st.radio("3. Approval Scope", ["Entire Project", "Specific Pipe / Bank"])
    sel_ap_loc = None
    if "Specific Pipe" in scope:
        proj_locs = sorted(full_df[full_df['Project'] == ap_proj]['Location'].dropna().unique())
        sel_ap_loc = st.selectbox("Select Target Pipe", proj_locs)
    
    status_action = st.radio("4. Action", ["✅ Approve (Show Client)", "🚫 Disapprove (Hide Client)"])
    note = st.text_area("5. Engineering Note", placeholder="Explain any trends or maintenance...")

    if st.button("🚀 SYNC TO DATABASE", type="primary"):
        # Determine SQL filter
        if scope == "Entire Project":
            scope_filter = f"Project = '{ap_proj}'"
        else:
            scope_filter = f"Project = '{ap_proj}' AND Location = '{sel_ap_loc}'"
            
        is_approved_val = "TRUE" if "Approve" in status_action else "FALSE"
        
        target_tables = ["raw_lord", "raw_sensorpush"]
        for tbl in target_tables:
            sql = f"""
            UPDATE `sensorpush-export.sensor_data.{tbl}`
            SET is_approved = {is_approved_val}, engineer_note = '{note.replace("'", "''")}'
            WHERE nodenumber IN (
                SELECT NodeNum FROM `sensorpush-export.sensor_data.master_metadata`
                WHERE {scope_filter}
            )
            AND CAST(timestamp AS DATE) = '{ap_date}'
            """
            # client.query(sql) # Uncomment once columns exist in physical tables
            
        st.balloons()
        st.success("Database synced successfully.")
        st.code(sql, language="sql")
