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

    # THE UPGRADE: INNER JOIN with the Node Registry. 
    # This STRICTLY enforces that only currently assigned nodes (for the exact phase) are pulled,
    # and safely bridges the case-sensitive gap for Lord sensors using UPPER(TRIM()).
    query = f"""
        WITH ValidNodes AS (
            SELECT 
                NodeNum, 
                TRIM(CAST(Location AS STRING)) as Reg_Location, 
                TRIM(CAST(Bank AS STRING)) as Reg_Bank, 
                Depth as Reg_Depth, 
                Phase as Reg_Phase, 
                System as Reg_System
            FROM `{config.NODE_REGISTRY_TABLE}`
            WHERE TRIM(SPLIT(CAST(Project AS STRING), '-')[OFFSET(0)]) = @root_job_id
              AND (End_Date IS NULL OR TRIM(CAST(End_Date AS STRING)) = '')
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
            m.Hardware
        FROM `{config.MASTER_VIEW}` m
        INNER JOIN ValidNodes v 
          ON UPPER(TRIM(CAST(m.NodeNum AS STRING))) = UPPER(TRIM(CAST(v.NodeNum AS STRING)))
        WHERE m.temperature >= -30.0 AND m.temperature <= 120.0
          -- Ensure we only pull data recorded while mapped to this project
          AND m.Project LIKE CONCAT(@root_job_id, '%')
        ORDER BY m.timestamp ASC
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("root_job_id", "STRING", root_job_id)]
    )
    
    df = client.query(query, job_config=job_config).to_dataframe()
    return df
    
def apply_sanity_filter(df):
    if df.empty: return df

    if 'NodeNum' in df.columns:
        df = df.dropna(subset=['NodeNum']).copy()

    if df.empty: return df

    bad_condition = (df['temperature'] > 120) | (df['temperature'] < -30)
    
    if 'NodeNum' in df.columns:
        node_means = df.groupby('NodeNum')['temperature'].transform('mean')
        outlier_condition = (df['temperature'] > node_means + 20) | (df['temperature'] < node_means - 20)
    else:
        avg_temp = df['temperature'].mean()
        outlier_condition = (df['temperature'] > avg_temp + 20) | (df['temperature'] < avg_temp - 20)

    mask_col = 'approve' if 'approve' in df.columns else 'approval_status' if 'approval_status' in df.columns else None
    
    if mask_col:
        df.loc[outlier_condition, mask_col] = 'MASKED'
        df.loc[bad_condition, mask_col] = 'BADDATA'

    return df
