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
def get_universal_portal_data(project_id, lookback_days=35, is_summary_page=False, show_masked=False, show_baddata=False):
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

    # 1. Safely map exclusions without triggering syntax errors
    exclusions = ["'FALSE'"] 
    if not show_masked:
        exclusions.append("'MASKED'")
    if not show_baddata:
        exclusions.append("'BADDATA'")
    
    exclusion_str = ", ".join(exclusions)

    # 2. Lift the temperature bounds cleanly if hunting anomalies
    if show_baddata:
        temp_bounds_sql = "(1=1)" 
    else:
        temp_bounds_sql = "(m.temperature >= -30.0 AND m.temperature <= 120.0)"

    # 3. Smart Office Filter (Make sure this 'if' is pulled all the way to the left!)
    if 'OFFICE' in str(root_job_id).upper():
        office_filter_sql = ""
    else:
        office_filter_sql = "AND UPPER(CAST(m.Project AS STRING)) NOT LIKE '%OFFICE%' AND UPPER(CAST(m.Location AS STRING)) NOT LIKE '%OFFICE%'"

    # 4. Push the timeline filter directly into BigQuery
    if lookback_days >= 9999:
        time_filter_sql = "" # Let everything through for the 'Full Data Set' view
    else:
        # Ask BigQuery to only give us data from the last X days
        time_filter_sql = f"AND m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY)"

    query = f"""
        WITH ProjectAssignments AS (
            SELECT 
                NodeNum, 
                TRIM(CAST(Location AS STRING)) as Reg_Location, 
                TRIM(CAST(Bank AS STRING)) as Reg_Bank, 
                Depth as Reg_Depth, 
                Phase as Reg_Phase, 
                System as Reg_System,
                
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
          AND m.timestamp >= v.active_start
          AND m.timestamp <= v.active_end
          
        WHERE {temp_bounds_sql}
          AND m.Project LIKE CONCAT(@root_job_id, '%')
          {office_filter_sql}
          {time_filter_sql}
          AND UPPER(COALESCE(CAST(m.approval_status AS STRING), 'TRUE')) NOT IN ({exclusion_str})
        ORDER BY m.timestamp ASC
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("root_job_id", "STRING", root_job_id)]
    )
    
    df = client.query(query, job_config=job_config).to_dataframe()
    
    # THE FIX: If we are viewing the massive "Full Data" set, downsample to 1-hour averages
    # to keep the browser from crashing.
    if lookback_days >= 9999 and not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp').groupby(
            ['Project', 'NodeNum', 'Location', pd.Grouper(freq='1h')]
        )['temperature'].mean().reset_index()
        
    return df


def apply_sanity_filter(df):
    """Flags physically impossible temperatures as BADDATA instead of deleting them."""
    if df.empty or 'temperature' not in df.columns:
        return df
        
    # Ensure the approval_status column exists
    if 'approval_status' not in df.columns:
        df['approval_status'] = 'TRUE'
        
    # Find wild extremes and force their status to BADDATA
    extreme_mask = (df['temperature'] < -30.0) | (df['temperature'] > 120.0)
    df.loc[extreme_mask, 'approval_status'] = 'BADDATA'
    
    return df

@st.cache_data(ttl=600)
def get_sensor_performance_data(project_id="All Projects"):
    """
    Calculates the health score and failure rate for sensors.
    Can be scoped to a single project or run globally across the fleet.
    """
    client = get_bq_client()
    if client is None: 
        return pd.DataFrame()
    
    # 1. Scope the query based on the active view
    project_filter = ""
    if project_id != "All Projects":
        root_job_id = str(project_id).split('-')[0].strip()
        project_filter = f"WHERE Project LIKE '{root_job_id}%'"
        
    # 2. Let BigQuery calculate the exact ratios of good vs. bad data
    query = f"""
        SELECT 
            NodeNum,
            MAX(Project) as Project,
            MAX(Location) as Location,
            COUNT(*) as Total_Readings,
            COUNTIF(UPPER(approval_status) = 'BADDATA') as Bad_Readings,
            COUNTIF(UPPER(approval_status) = 'MASKED') as Masked_Readings,
            ROUND((COUNTIF(UPPER(approval_status) = 'BADDATA') / COUNT(*)) * 100, 2) as Failure_Rate_Pct
        FROM `{config.MASTER_VIEW}`
        {project_filter}
        GROUP BY NodeNum
        HAVING Total_Readings > 10  -- Ignore brand new sensors with barely any data
        ORDER BY Failure_Rate_Pct DESC, Bad_Readings DESC
    """
    
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Failed to fetch performance data: {e}")
        return pd.DataFrame()
