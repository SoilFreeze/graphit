import streamlit as st
import pandas as pd
from google.cloud import bigquery
import plotly.express as px
from datetime import datetime

# --- CONFIG & CONNECTION ---
st.set_page_config(page_title="SoilFreeze Data Lab", layout="wide")
client = bigquery.Client(project="sensorpush-export")

# --- SIDEBAR NAVIGATION ---
st.sidebar.title("❄️ SoilFreeze Lab")
service = st.sidebar.selectbox("Select Service", [
    "🏠 Executive Summary", 
    "📈 Node Diagnostics", 
    "📤 Data Intake Lab", 
    "⚙️ Database Maintenance"
])

# --- SERVICE 1: EXECUTIVE SUMMARY ---
if service == "🏠 Executive Summary":
    st.header("🏠 Executive Summary")
    try:
        # Get Project List from Metadata
        m_q = "SELECT DISTINCT Project FROM `sensorpush-export.sensor_data.master_metadata` ORDER BY Project"
        proj_list = client.query(m_q).to_dataframe()['Project'].tolist()
        sel_proj = st.selectbox("Select Project Focus", proj_list if proj_list else ["Maltby"])

        # SAFE QUERY: Fakes columns if the Master Table hasn't been scrubbed yet
        exec_q = f"""
            SELECT 
                nodenumber, 
                MAX(timestamp) as last_seen,
                MIN(value) as min_temp,
                MAX(value) as max_temp,
                ARRAY_AGG(value ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] as current_temp,
                '--' as engineer_note
            FROM `sensorpush-export.sensor_data.final_databoard_master`
            WHERE Project = '{sel_proj}'
            GROUP BY nodenumber
        """
        df_ex = client.query(exec_q).to_dataframe()

        if df_ex.empty:
            st.warning("⚠️ Master Table is empty. Go to 'Database Maintenance' and run the Scrub.")
        else:
            avg_t = df_ex['current_temp'].mean()
            col1, col2 = st.columns(2)
            col1.metric("Project Average", f"{avg_t:.1f}°F")
            col2.metric("Sensors Online", len(df_ex))

            # Formatting
            df_disp = df_ex.copy()
            df_disp['current_temp'] = df_disp['current_temp'].round(1)
            df_disp['min_temp'] = df_disp['min_temp'].round(1)
            df_disp['max_temp'] = df_disp['max_temp'].round(1)
            df_disp['last_seen'] = pd.to_datetime(df_disp['last_seen']).dt.strftime('%m/%d %H:%M')

            def color_t(v):
                if v > 32: return 'color: #ff4b4b' # Red
                if 28 <= v <= 32: return 'color: #ffa500' # Orange
                return 'color: #28a745' # Green

            st.dataframe(
                df_disp.style.applymap(color_t, subset=['current_temp', 'min_temp', 'max_temp']),
                use_container_width=True, hide_index=True
            )
    except Exception as e:
        st.error(f"Executive Summary Error: {e}")

# --- SERVICE 2: NODE DIAGNOSTICS ---
elif service == "📈 Node Diagnostics":
    st.header("📈 Node Diagnostics")
    c1, c2, c3 = st.columns(3)
    
    with c1:
        m_q = "SELECT DISTINCT Project FROM `sensorpush-export.sensor_data.master_metadata` ORDER BY Project"
        projs = client.query(m_q).to_dataframe()['Project'].tolist()
        sel_projs = st.multiselect("Filter Projects", projs)
    
    with c2:
        if sel_projs:
            loc_q = f"SELECT DISTINCT Location FROM `sensorpush-export.sensor_data.master_metadata` WHERE Project IN UNNEST({list(sel_projs)}) ORDER BY Location"
            locs = client.query(loc_q).to_dataframe()['Location'].tolist()
            sel_locs = st.multiselect("Filter Specific Pipes", locs, default=locs)
        else:
            sel_locs = []

    with c3:
        weeks = st.slider("Trend Duration (Weeks)", 1, 12, 6)

    if sel_projs and sel_locs:
        try:
            days = weeks * 7
            graph_q = f"""
                SELECT timestamp, value, Location, Depth
                FROM `sensorpush-export.sensor_data.final_databoard_master`
                WHERE Project IN UNNEST({list(sel_projs)})
                AND Location IN UNNEST({list(sel_locs)})
                AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
                ORDER BY timestamp ASC
            """
            df_g = client.query(graph_q).to_dataframe()

            if not df_g.empty:
                df_g['label'] = df_g['Location'] + " (" + df_g['Depth'] + ")"
                fig = px.line(df_g, x='timestamp', y='value', color='label', title=f"Trends: Last {weeks} Weeks")
                fig.add_hline(y=32, line_dash="dash", line_color="#ff4b4b", annotation_text="32°F")
                fig.add_hline(y=28, line_dash="dot", line_color="#28a745", annotation_text="28°F")
                fig.update_layout(hovermode="x unified")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No data found for selection.")
        except Exception as e:
            st.error(f"Graph Error: {e}")

# --- SERVICE 3: DATA INTAKE LAB ---
elif service == "📤 Data Intake Lab":
    st.header("📤 Manual Data Ingestion")
    source = st.radio("Source", ["SensorPush (CSV)", "Lord (SensorConnect)"], horizontal=True)
    u_file = st.file_uploader("Upload File", type=['csv'], key="intake_u")

    if u_file:
        try:
            if "Lord" in source:
                lines = u_file.getvalue().decode("utf-8").splitlines()
                start = next((i for i, l in enumerate(lines) if "DATA_START" in l), 0)
                u_file.seek(0)
                df_raw = pd.read_csv(u_file, skiprows=start + 1)
                df_up = df_raw.melt(id_vars=[df_raw.columns[0]], var_name='nodenumber', value_name='value')
                df_up = df_up.rename(columns={df_raw.columns[0]: 'timestamp'})
                df_up['nodenumber'] = df_up['nodenumber'].str.replace(':', '-', regex=False)
                t_table, v_col = "sensorpush-export.sensor_data.raw_lord", 'value'
            else:
                df_up = pd.read_csv(u_file).rename(columns={'Timestamp':'timestamp','Temperature':'temperature','Sensor':'sensor_name'})
                df_up['sensor_name'] = df_up['sensor_name'].str.replace(':', '-', regex=False)
                t_table, v_col = "sensorpush-export.sensor_data.raw_sensorpush", 'temperature'

            df_up['timestamp'] = pd.to_datetime(df_up['timestamp'], errors='coerce', utc=True)
            df_up[v_col] = pd.to_numeric(df_up[v_col], errors='coerce')
            df_up = df_up.dropna(subset=['timestamp', v_col])

            if st.button("🚀 PUSH TO CLOUD"):
                client.load_table_from_dataframe(df_up, t_table).result()
                st.success("Uploaded!")
        except Exception as e:
            st.error(f"Intake Error: {e}")

# --- SERVICE 4: DATABASE MAINTENANCE ---
elif service == "⚙️ Database Maintenance":
    st.header("⚙️ Database Maintenance")
    if st.button("🔄 EXECUTE MASTER SCRUB", key="master_scrub_v1"):
        with st.spinner("Rebuilding Master..."):
            try:
                scrub_q = """
                CREATE OR REPLACE TABLE `sensorpush-export.sensor_data.final_databoard_master` AS
                WITH UnifiedRaw AS (
                    SELECT CAST(timestamp AS TIMESTAMP) as ts, value, REPLACE(nodenumber, ':', '-') as nodenumber
                    FROM `sensorpush-export.sensor_data.raw_lord` WHERE value <= 90
                    UNION ALL
                    SELECT CAST(timestamp AS TIMESTAMP) as ts, temperature AS value, REPLACE(sensor_name, ':', '-') as nodenumber
                    FROM `sensorpush-export.sensor_data.raw_sensorpush` WHERE temperature <= 90
                ),
                HourlyAgg AS (
                    SELECT TIMESTAMP_TRUNC(ts, HOUR) as timestamp, nodenumber, AVG(value) as value
                    FROM UnifiedRaw GROUP BY 1, 2
                )
                SELECT d.*, m.Project, m.Location, m.Depth, CAST(NULL AS STRING) as engineer_note, CAST(FALSE AS BOOL) as is_approved
                FROM HourlyAgg d
                INNER JOIN `sensorpush-export.sensor_data.master_metadata` m ON d.nodenumber = REPLACE(m.NodeNum, ':', '-')
                """
                client.query(scrub_q).result()
                st.success("✅ Master Table Rebuilt!")
                st.balloons()
            except Exception as e:
                st.error(f"Scrub failed: {e}")
