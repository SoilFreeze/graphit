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

    # THE UPGRADE: Explicitly block office data and manual rejections at the database level
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
            m.Hardware,
            m.approval_status
        FROM `{config.MASTER_VIEW}` m
        INNER JOIN ValidNodes v 
          ON UPPER(TRIM(CAST(m.NodeNum AS STRING))) = UPPER(TRIM(CAST(v.NodeNum AS STRING)))
        WHERE m.temperature >= -30.0 AND m.temperature <= 120.0
          
          -- 1. Ensure we only pull data recorded while strictly mapped to this exact project
          AND m.Project LIKE CONCAT(@root_job_id, '%')
          
          -- 2. Explicitly ban any historical records tagged as Office
          AND UPPER(CAST(m.Project AS STRING)) NOT LIKE '%OFFICE%'
          AND UPPER(CAST(m.Location AS STRING)) NOT LIKE '%OFFICE%'
          
          -- 3. Explicitly drop database-level manual rejections
          AND UPPER(COALESCE(CAST(m.approval_status AS STRING), 'TRUE')) NOT IN ('FALSE', 'BADDATA', 'MASKED')
          
        ORDER BY m.timestamp ASC
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("root_job_id", "STRING", root_job_id)]
    )
    
    df = client.query(query, job_config=job_config).to_dataframe()
    return df


def apply_sanity_filter(df):
    """Dynamically identifies severe thermal anomalies and drops them from the visual rendering."""
    if df.empty: return df

    if 'NodeNum' in df.columns:
        df = df.dropna(subset=['NodeNum']).copy()

    if df.empty: return df

    # Flag anything impossible outside standard planetary/equipment bounds
    bad_condition = (df['temperature'] > 120) | (df['temperature'] < -30)
    
    # Calculate a running median/mean per node to identify sudden impossible jumps
    if 'NodeNum' in df.columns:
        node_means = df.groupby('NodeNum')['temperature'].transform('mean')
        outlier_condition = (df['temperature'] > node_means + 20) | (df['temperature'] < node_means - 20)
    else:
        avg_temp = df['temperature'].mean()
        outlier_condition = (df['temperature'] > avg_temp + 20) | (df['temperature'] < avg_temp - 20)

    # THE FIX: Physically drop the bad data rows using a negated boolean mask (~), 
    # instead of just assigning them a text label.
    df = df[~bad_condition & ~outlier_condition].copy()

    return df
