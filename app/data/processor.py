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
    
    if "gcp_service_account" in st.secrets:
        info = st.secrets["gcp_service_account"]
        # Ensure 'with_scopes' is used to apply the necessary permissions
        credentials = service_account.Credentials.from_service_account_info(
            info
        ).with_scopes(SCOPES)
        
        return bigquery.Client(credentials=credentials, project=info["project_id"])
    
    return None
            
    except Exception as e:
        st.error(f"❌ BigQuery Authentication Failed: {e}")
        return None

@st.cache_data(ttl=600)
def get_universal_portal_data(project_id, is_summary_page=False):
    client = get_bq_client() # You needed to define 'client' here!
    if client is None: return pd.DataFrame()
    
    root_job_id = str(project_id).split('-')[0].strip()

    # Fix: Use config.MASTER_VIEW
    query = f"""
        SELECT 
            Project as Raw_Project_Name,
            NodeNum,
            temperature,
            timestamp,
            COALESCE(Location, 'Unassigned') as Location,
            COALESCE(Bank, '—') as Bank,
            Depth,
            Phase,
            System,
            Hardware
        FROM `{config.MASTER_VIEW}`
        WHERE temperature >= -30.0 AND temperature <= 120.0
          AND Project LIKE CONCAT(@root_job_id, '%')
        ORDER BY timestamp ASC
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
