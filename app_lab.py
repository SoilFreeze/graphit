import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account # Added this import
import json

# --- 1. AUTHENTICATION (The Fix) ---
@st.cache_resource
def get_bq_client():
    """Consistent auth engine that prevents TransportErrors."""
    try:
        # Look for the 'gcp_service_account' block in your Streamlit Secrets
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(info)
            # Add Drive scope if your Master Metadata is a Google Sheet
            scoped_creds = credentials.with_scopes([
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/bigquery"
            ])
            return bigquery.Client(credentials=scoped_creds, project=info["project_id"])
        else:
            # Fallback for local development
            return bigquery.Client(project="sensorpush-export")
    except Exception as e:
        st.error(f"❌ Auth Error: {e}")
        return None

# Initialize the client safely
client = get_bq_client()

if client is None:
    st.warning("Please check your Streamlit Secrets configuration.")
    st.stop()
