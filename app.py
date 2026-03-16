# 2. Sidebar & URL Logic
st.sidebar.title("📁 Project Controls")

# GET PROJECT FROM URL (e.g., your-app.streamlit.app/?project=Bridge_Site)
query_params = st.query_params
url_project = query_params.get("project")

# 3. Data Loading
@st.cache_data(ttl=300)
def get_full_dataset():
    query = "SELECT * FROM `sensorpush-export.sensor_data.final_dashboard_data` ORDER BY timestamp ASC"
    return client.query(query).to_dataframe()

df_raw = get_full_dataset()
df_raw.columns = [str(c).strip().lower() for c in df_raw.columns]
df_raw['timestamp'] = pd.to_datetime(df_raw['timestamp'])

# --- PROJECT LOCKING LOGIC ---
if url_project:
    # If a project is in the URL, lock the app to that project
    selected_project = url_project
    st.sidebar.success(f"Locked to: **{selected_project}**")
    # No selectbox shown, so they can't switch projects
else:
    # Otherwise, show the standard dropdown for internal use
    available_projects = sorted(df_raw['project'].dropna().unique())
    selected_project = st.sidebar.selectbox("Choose Project", available_projects)

df_proj = df_raw[df_raw['project'] == selected_project].copy()

# Unit and Reference Line controls remain the same...
unit = st.sidebar.radio("Temperature Unit", ["Fahrenheit (°F)", "Celsius (°C)"])
# ... [rest of your unit/ref line code] ...
