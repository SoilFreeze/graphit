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

    # THE FIX: INNER JOIN with the Active Registry. 
    # This acts as an iron-clad filter so only nodes without an End_Date are pulled,
    # and their Location/Depth is forced to match their CURRENT assignment.
    query = f"""
        WITH ActiveRegistry AS (
            SELECT 
                NodeNum, 
                CAST(Location AS STRING) as Active_Loc, 
                CAST(Bank AS STRING) as Active_Bank, 
                CAST(Depth AS STRING) as Active_Depth, 
                CAST(Phase AS STRING) as Active_Phase, 
                CAST(System AS STRING) as Active_System
            FROM `{config.NODE_REGISTRY_TABLE}`
            WHERE (End_Date IS NULL OR TRIM(CAST(End_Date AS STRING)) = '')
              AND Project LIKE CONCAT(@root_job_id, '%')
        )
        SELECT 
            t.Project as Raw_Project_Name,
            t.NodeNum,
            t.temperature,
            t.timestamp,
            COALESCE(r.Active_Loc, t.Location, 'Unassigned') as Location,
            COALESCE(r.Active_Bank, t.Bank, '—') as Bank,
            COALESCE(r.Active_Depth, t.Depth) as Depth,
            COALESCE(r.Active_Phase, t.Phase) as Phase,
            COALESCE(r.Active_System, t.System) as System,
            t.Hardware
        FROM `{config.MASTER_VIEW}` t
        INNER JOIN ActiveRegistry r ON t.NodeNum = r.NodeNum
        WHERE t.temperature >= -30.0 AND t.temperature <= 120.0
          AND t.Project LIKE CONCAT(@root_job_id, '%')
        ORDER BY t.timestamp ASC
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("root_job_id", "STRING", root_job_id)]
    )
    
    df = client.query(query, job_config=job_config).to_dataframe()
    
    # Filter by the specific Project/Phase Name requested 
    if not is_summary_page:
        job_num = str(project_id).split('-')[0].strip() 
        
        df = df[df['Raw_Project_Name'].astype(str).str.startswith(job_num, na=False)]
        
        if "Phase 1" in str(project_id):
            df = df[df['Phase'].astype(str).str.strip() == '1']
        elif "Phase 2" in str(project_id) or "Phase2" in str(project_id):
            df = df[df['Phase'].astype(str).str.strip() == '2']
            
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
