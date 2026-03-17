import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account # Add this import

# Check if we are on Streamlit Cloud or Local
if "gcp_service_account" in st.secrets:
    # Use Secrets (Cloud)
    info = st.secrets["gcp_service_account"]
    credentials = service_account.Credentials.from_service_account_info(info)
    client = bigquery.Client(credentials=credentials, project=info["project_id"])
else:
    # Use JSON file (Local Desktop)
    client = bigquery.Client.from_service_account_json("service_account.json")
