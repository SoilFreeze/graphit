import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from app.utils import config 

@st.cache_resource
def get_bq_client():
    SCOPES = [
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets"
    ]
    
    # 1. Start the try block BEFORE the code that might fail
    try:
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            # Ensure 'with_scopes' is used to apply the necessary permissions
            credentials = service_account.Credentials.from_service_account_info(
                info
            ).with_scopes(SCOPES)
            
            return bigquery.Client(credentials=credentials, project=info["project_id"])
        
        return None
    
    # 2. The except block now correctly follows the try block
    except Exception as e:
        st.error(f"❌ BigQuery Authentication Failed: {e}")
        return None
            
    except Exception as e:
        st.error(f"❌ BigQuery Authentication Failed: {e}")
        return None

@st.cache_data(ttl=600)
def get_universal_portal_data(project_id, is_summary_page=False):
    client = get_bq_client()
    if client is None: return pd.DataFrame()
    
    root_job_id = str(project_id).split('-')[0].strip()

    # Build dynamic phase matching for the SQL query
    phase_sql = ""
    if not is_summary_page:
        import re
        phase_match = re.search(r'(?i)Phase\s*(\d+)', str(project_id))
        if phase_match:
            target_phase = phase_match.group(1)
            phase_sql = f"AND TRIM(CAST(Phase AS STRING)) = '{target_phase}'"

    # THE UPGRADE: Build dynamic exclusion list based on sidebar checkboxes
    import streamlit as st
    exclusions = ["'FALSE'"] # Always drop permanently rejected data
    
    # NEW: Lift the temperature bounds if we are hunting for bad data anomalies!
    if st.session_state.get('global_show_baddata', False):
        temp_bounds_sql = "(1=1)" # Allows all wild spikes through
    else:
        temp_bounds_sql = "(m.temperature >= -30.0 AND m.temperature <= 120.0)"

    if not st.session_state.get('global_show_masked', False):
        exclusions.append("'MASKED'")
    if not st.session_state.get('global_show_baddata', False):
        exclusions.append("'BADDATA'")
    
    exclusion_str = ", ".join(exclusions)

    # Bind telemetry to the registry's timeline windows to seamlessly stitch sensor replacements together!
    query = f"""
        WITH ProjectAssignments AS (
            SELECT 
                NodeNum, 
                TRIM(CAST(Location AS STRING)) as Reg_Location, 
                TRIM(CAST(Bank AS STRING)) as Reg_Bank, 
                Depth as Reg_Depth, 
                Phase as Reg_Phase, 
                System as Reg_System,
                
                -- Resilient timestamp parsing to safely establish historical lifecycle bounds
                COALESCE(
                    SAFE_CAST(Start_Date AS TIMESTAMP),
                    SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', CAST(Start_Date AS STRING)),
                    SAFE.PARSE_TIMESTAMP('%m/%d/%Y', CAST(Start_Date AS STRING)),
                    SAFE.PARSE_TIMESTAMP('%Y-%m-%d', CAST(Start_Date AS STRING)),
                    TIMESTAMP('2000-01-01')
                ) as active_start,
                
                COALESCE(
                    SAFE_CAST(End_Date AS TIMESTAMP),
                    SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', CAST(End_Date AS STRING)),
                    SAFE.PARSE_TIMESTAMP('%m/%d/%Y', CAST(End_Date AS STRING)),
                    SAFE.PARSE_TIMESTAMP('%Y-%m-%d', CAST(End_Date AS STRING)),
                    TIMESTAMP('2099-12-31')
                ) as active_end
                
            FROM `{config.NODE_REGISTRY_TABLE}`
            WHERE TRIM(SPLIT(CAST(Project AS STRING), '-')[OFFSET(0)]) = @root_job_id
              {phase_sql}
        )
        SELECT 
            m.Project as Raw_Project_Name,
            m.NodeNum,
            m.temperature,
            m.timestamp,
            COALESCE(NULLIF(v.Reg_Location, ''), m.Location, 'Unassigned') as Location,
            COALESCE(NULLIF(v.Reg_Bank, ''), m.Bank, '—') as Bank,
            COALESCE(v.Reg_Depth, m.Depth) as Depth,
            COALESCE(NULLIF(CAST(v.Reg_Phase AS STRING), ''), m.Phase) as Phase,
            COALESCE(NULLIF(v.Reg_System, ''), m.System) as System,
            m.Hardware,
            m.approval_status
        FROM `{config.MASTER_VIEW}` m
        INNER JOIN ProjectAssignments v 
          ON UPPER(TRIM(CAST(m.NodeNum AS STRING))) = UPPER(TRIM(CAST(v.NodeNum AS STRING)))
          
          -- THE SEAMLESS SPLICE: Only pull data recorded while mapped to this specific position!
          AND m.timestamp >= v.active_start
          AND m.timestamp <= v.active_end
          
        WHERE {temp_bounds_sql}
          AND m.Project LIKE CONCAT(@root_job_id, '%')
          AND UPPER(CAST(m.Project AS STRING)) NOT LIKE '%OFFICE%'
          AND UPPER(CAST(m.Location AS STRING)) NOT LIKE '%OFFICE%'
          AND UPPER(COALESCE(CAST(m.approval_status AS STRING), 'TRUE')) NOT IN ({exclusion_str})
        ORDER BY m.timestamp ASC
          
          -- NEW: Dynamically filter based on sidebar checkboxes!
          AND UPPER(COALESCE(CAST(m.approval_status AS STRING), 'TRUE')) NOT IN ({exclusion_str})
        ORDER BY m.timestamp ASC
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("root_job_id", "STRING", root_job_id)]
    )
    
    df = client.query(query, job_config=job_config).to_dataframe()
    return df


def apply_sanity_filter(df):
    """
    Filters out noise 'blips' while preserving valid thermal spikes.
    A data point is masked only if it is an outlier compared to its 
    immediate neighbors AND returns to normal levels immediately after.
    """
    if df.empty or 'temperature' not in df.columns: 
        return df

    df = df.copy()

    # 1. Flag absolute physical impossibilities (Equipment errors)
    is_absolute_outlier = (df['temperature'] > 120) | (df['temperature'] < -30)

    # 2. Identify noise blips:
    # A point is a 'noise blip' if it deviates significantly from the 
    # average of the point before AND the point after it.
    df['prev_temp'] = df.groupby('NodeNum')['temperature'].shift(1)
    df['next_temp'] = df.groupby('NodeNum')['temperature'].shift(-1)
    
    # Calculate the average of the neighbors
    df['neighbor_avg'] = (df['prev_temp'] + df['next_temp']) / 2
    
    # Define noise as:
    # - A deviation > 5.0 degrees from the neighbor average
    # - AND the neighbor average is NOT a deviation from the previous point
    # (This ensures we don't mask the start of a legitimate rapid thermal event)
    is_noise_blip = (abs(df['temperature'] - df['neighbor_avg']) > 5.0) & \
                    (abs(df['neighbor_avg'] - df['prev_temp']) < 2.0)

    # 3. Apply the filter: 
    # Drop rows that are absolute outliers OR identified noise blips
    df = df[~is_absolute_outlier & ~is_noise_blip].copy()

    # Cleanup temporary helper columns
    return df.drop(columns=['prev_temp', 'next_temp', 'neighbor_avg'])
