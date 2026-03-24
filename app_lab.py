import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account # Needed for the fix
import json

# --- 1. AUTHENTICATION HELPER ---
@st.cache_resource
def get_bq_client():
    """Consistent auth engine across all SoilFreeze apps."""
    try:
        # Check Streamlit Secrets first (simplest for Cloud/Dev setup)
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(info)
            scoped_creds = credentials.with_scopes([
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/bigquery"
            ])
            return bigquery.Client(credentials=scoped_creds, project=info["project_id"])
        else:
            # Fallback for local testing if you use a JSON file
            return bigquery.Client(project="sensorpush-export")
    except Exception as e:
        st.error(f"Authentication Failed: {e}")
        return None

# Replace your old 'client = bigquery.Client(...)' with this:
client = get_bq_client()

if client is None:
    st.stop() # Stop the app if we can't talk to BigQuery
