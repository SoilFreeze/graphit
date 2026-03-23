import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery, secretmanager
from google.oauth2 import service_account
from datetime import datetime, timedelta, time, date
import pytz
import json
import requests


# --- 0. PAGE CONFIG & SOILFREEZE PALETTE ---
st.set_page_config(layout="wide", page_title="SoilFreeze Engineering Hub")

def apply_sf_style():
    st.markdown("""
        <style>
            .stApp { background-color: #FFFFFF; }
            .stSidebar { background-color: #F8F9FA; border-right: 1px solid #E0E0E0; }
            h1, h2, h3 { color: #003366 !important; font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; }
            .stButton>button { background-color: #003366; color: white; border-radius: 4px; border: none; width: 100%; }
            .stMetric { background-color: #F8F9FA; padding: 10px; border-radius: 5px; border: 1px solid #E0E0E0; }
        </style>
    """, unsafe_allow_html=True)

apply_sf_style()

# --- 1. AUTHENTICATION (SECRET MANAGER) ---
@st.cache_resource
def get_bq_client():
    # 1. TRY SECRET MANAGER (The "One Source" Plan)
    try:
        from google.cloud import secretmanager
        sm_client = secretmanager.SecretManagerServiceClient()
        # Ensure 'sensorpush-export' is your EXACT project ID
        name = "projects/sensorpush-export/secrets/BIGQUERY_SERVICE_ACCOUNT_JSON/versions/latest"
        
        # We add a 5-second timeout so it doesn't get "stuck"
        response = sm_client.access_secret_version(request={"name": name}, timeout=5)
        info = json.loads(response.payload.data.decode("UTF-8"))
        credentials = service_account.Credentials.from_service_account_info(info)
        scoped_creds = credentials.with_scopes([
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery"
        ])
        return bigquery.Client(credentials=scoped_creds, project=info["project_id"])

    except Exception as e:
        # 2. FALLBACK TO LOCAL SECRETS (So you aren't stuck!)
        st.sidebar.warning("⚠️ Secret Manager failed. Using local secrets.")
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(info)
            # Make sure we still add the Drive scope here for the Metadata join
            scoped_creds = credentials.with_scopes([
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/bigquery"
            ])
            return bigquery.Client(credentials=scoped_creds, project=info["project_id"])
        else:
            raise Exception("No credentials found in Secret Manager or st.secrets")

client = get_bq_client()

# --- 2. DATA FETCHING (Using the Cleaned Master Table) ---
@st.cache_data(ttl=600)
def fetch_engineering_data():
    # We now pull from the 'final_databoard_master' which is already scrubbed and joined
    query = """
    SELECT timestamp, value, nodenumber, Project, Location, Depth, is_approved, engineer_note
    FROM `sensorpush-export.sensor_data.final_databoard_master`
    """
    df = client.query(query).to_dataframe()
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df

full_df = pd.DataFrame()
try:
    full_df = fetch_engineering_data()
except Exception as e:
    st.error(f"⚠️ Master Table Missing or Error: {e}. Run 'Database Maintenance' to build it.")

# --- 3. SIDEBAR NAVIGATION ---
st.sidebar.title("🛠 Engineering Hub")

# Update this list to include all your new tools
service = st.sidebar.selectbox(
    "Select Service", 
    [
        "🏠 Executive Summary", 
        "🔍 Node Diagnostics"
    ]
)

# --- SERVICE ROUTING ---

if service == "🏠 Executive Summary":
    # (Insert your Executive Summary code here)
    pass

elif service == "🔍 Node Diagnostics":
    # (Insert the Node Diagnostic code with the .sort_values fix here)
    pass

# --- SERVICE: EXECUTIVE SUMMARY ---
if service == "🏠 Executive Summary" and not full_df.empty:
    st.header("🏠 Site Health & Warming Alerts")
    
    c1, c2 = st.columns(2)
    with c1:
        all_projs = sorted([p for p in full_df['Project'].unique() if p is not None])
        sel_summary_proj = st.selectbox("1. Select Project", all_projs)
    
    proj_df = full_df[full_df['Project'] == sel_summary_proj].copy()
    
    with c2:
        all_locs = sorted([l for l in proj_df['Location'].unique() if l is not None])
        sel_summary_loc = st.selectbox("2. Select Pipe / Bank", all_locs)

    # 24-Hour Performance Table logic...
    now_ts = datetime.now(tz=pytz.UTC)
    loc_recent = proj_df[(proj_df['Location'] == sel_summary_loc) & (proj_df['timestamp'] >= (now_ts - timedelta(hours=24)))].copy()

    if not loc_recent.empty:
        node_analysis = []
        for node in loc_recent['nodenumber'].unique():
            n_df = loc_recent[loc_recent['nodenumber'] == node].sort_values('timestamp')
            if len(n_df) > 1:
                node_analysis.append({
                    "Depth": n_df['Depth'].iloc[0], "Node ID": node,
                    "Min": n_df['value'].min(), "Max": n_df['value'].max(),
                    "Current": n_df['value'].iloc[-1], "24h Change": n_df['value'].iloc[-1] - n_df['value'].iloc[0]
                })
        st.table(pd.DataFrame(node_analysis).sort_values('Depth'))

# --- SERVICE 1: NODE DIAGNOSTIC HUB (DATA GAPS ENABLED) ---
elif service == "🔍 Node Diagnostics" and not full_df.empty:
    st.header("🔍 Node Diagnostic Hub")
    
    # 1. TOP UI SELECTION
    col1, col2, col3 = st.columns(3)
    with col1:
        sel_proj = st.selectbox("Project", sorted(full_df['Project'].unique()))
    with col2:
        locs = sorted(full_df[full_df['Project'] == sel_proj]['Location'].unique())
        sel_loc = st.selectbox("Location", locs)
    with col3:
        weeks_to_show = st.number_input("Weeks to Display", min_value=1, value=1)

    # 2. SIDEBAR REFERENCE LINES
    st.sidebar.subheader("Thermal Reference Lines")
    ref_options = {"32°F (Frost)": 32.0, "26.6°F (Brine)": 26.6, "10.2°F (Deep)": 10.2}
    selected_refs = [label for label, val in ref_options.items() if st.sidebar.checkbox(label)]

    # 3. TIME LOGIC: Strict Monday-to-Monday Window
    today_dt = datetime.now(pytz.UTC).date()
    this_monday = today_dt - timedelta(days=today_dt.weekday())
    start_time = datetime.combine(this_monday, time.min).replace(tzinfo=pytz.UTC) - timedelta(weeks=weeks_to_show-1)
    end_time = start_time + timedelta(weeks=weeks_to_show)
    
    plot_df = full_df[
        (full_df['Project'] == sel_proj) & 
        (full_df['Location'] == sel_loc) & 
        (full_df['timestamp'] >= start_time) &
        (full_df['timestamp'] <= end_time)
    ].copy().sort_values(['nodenumber', 'timestamp'])

    if not plot_df.empty:
        # CRITICAL: Identify gaps > 6 hours and insert a None row to break the line
        # We do this per sensor to avoid mixing data
        processed_dfs = []
        for node in plot_df['nodenumber'].unique():
            node_df = plot_df[plot_df['nodenumber'] == node].copy()
            # Calculate time difference between rows
            node_df['diff'] = node_df['timestamp'].diff().dt.total_seconds() / 3600
            
            # Create "Gap" rows where diff > 6 hours
            gaps = node_df[node_df['diff'] > 6.0].copy()
            if not gaps.empty:
                gaps['value'] = None  # This breaks the Plotly line
                gaps['timestamp'] = gaps['timestamp'] - timedelta(minutes=1)
                node_df = pd.concat([node_df, gaps]).sort_values('timestamp')
            
            processed_dfs.append(node_df)
        
        plot_df = pd.concat(processed_dfs)
        plot_df['Sensor'] = plot_df['Depth'].astype(str) + "ft (" + plot_df['nodenumber'] + ")"
        
        # 4. PLOT: connectgaps=False is the key here
        fig = px.line(plot_df, x='timestamp', y='value', color='Sensor', 
                     range_y=[-20, 80], height=800)
        fig.update_traces(connectgaps=False) 

        # 5. Y-AXIS & X-AXIS (Same Precision Grid as requested)
        fig.update_yaxes(
            showline=True, linewidth=2, linecolor='Black', mirror=True,
            tick0=-20, dtick=20, gridcolor='DimGrey', gridwidth=1.5,
            minor=dict(dtick=5, gridcolor='#E5E5E5', showgrid=True), 
            zeroline=False, range=[-20, 80], title="Temperature (°F)"
        )
        fig.update_xaxes(
            showline=True, linewidth=2, linecolor='Black', mirror=True,
            showgrid=False, zeroline=False, tickformat="%a\n%b %d", title="",
            range=[start_time, end_time]
        )
        
        # 6. DYNAMIC REFERENCE LINES
        for label in selected_refs:
            val = ref_options[label]
            fig.add_hline(y=val, line_width=2, line_color="#003366", annotation_text=f"{val}°F")

        # 7. MANUAL VERTICAL GRID
        num_days = (end_time - start_time).days
        for i in range(num_days + 1):
            midnight = start_time + timedelta(days=i)
            is_monday = (midnight.weekday() == 0)
            fig.add_vline(x=midnight.timestamp()*1000, line_width=1.5 if is_monday else 1, 
                         line_color="DimGrey" if is_monday else "#CCCCCC")
            if i < num_days:
                for h in [6, 12, 18]:
                    fig.add_vline(x=(midnight+timedelta(hours=h)).timestamp()*1000, 
                                 line_width=0.5, line_color="#F0F0F0")

        fig.update_layout(plot_bgcolor='white', hovermode="x unified",
                         margin=dict(l=10, r=200, t=10, b=10),
                         legend=dict(x=1.02, y=1, bordercolor="Black", borderwidth=1))
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info(f"No data for {sel_loc} in the requested window.")
        
# --- SERVICE 2: DATA APPROVAL PORTAL (WITH EXCLUSIONS) ---
elif service == "📋 Data Approval Portal":
    st.header("📋 Engineering Approval Portal")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        ap_proj = st.selectbox("Target Project", sorted(full_df['Project'].unique()))
    with col2:
        ap_loc = st.selectbox("Target Pipe (Optional)", ["All"] + sorted(full_df[full_df['Project'] == ap_proj]['Location'].unique().tolist()))
    with col3:
        ap_date = st.date_input("Date to Target", value=date.today() - timedelta(days=1))

    status = st.radio("Set Status To:", ["✅ Approved (Show Client)", "🚫 Hidden (Internal Only)"])
    note = st.text_area("Engineering Note", placeholder="Reason for status change...")

    if st.button("🚀 SYNC APPROVAL STATUS"):
        is_app = "TRUE" if "Approved" in status else "FALSE"
        loc_filter = "" if ap_loc == "All" else f"AND Location = '{ap_loc}'"
        
        # This SQL targets exactly what you asked for: Job, Pipe, and Time
        sync_sql = f"""
        UPDATE `sensorpush-export.sensor_data.final_databoard_master`
        SET is_approved = {is_app}, engineer_note = '{note}'
        WHERE Project = '{ap_proj}' {loc_filter} 
        AND CAST(timestamp AS DATE) = '{ap_date}'
        """
        client.query(sync_sql).result()
        st.success(f"Updated status for {ap_proj} on {ap_date}")

