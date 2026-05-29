import streamlit as st
import pandas as pd
import google.auth
from google.cloud import bigquery
from google.oauth2 import service_account

# =============================================================================
# 1. CONFIGURATION
# =============================================================================
PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature"
INVENTORY_TABLE = "hardware_inventory"
RAW_DATA_TABLE = "raw_sensorpush"
EXPORT_FILE_PATH = "2026-05-29T20-25_export.csv"

st.title("🧪 SensorPush Data Asset Migration Lab")
st.write("This tool processes your exported sensor layout maps, appends new records into your hardware inventory, and repairs historical database rows.")

# =============================================================================
# 2. ROBUST AUTHENTICATION ENGINE
# =============================================================================
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform"
]

@st.cache_resource
def get_bigquery_client():
    if "gcp_service_account" in st.secrets:
        # Streamlit Cloud Authentication
        info = st.secrets["gcp_service_account"]
        credentials = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
        return bigquery.Client(credentials=credentials, project=info.get("project_id", PROJECT_ID))
    else:
        # Local Desktop Fallback Authentication
        try:
            credentials, project = google.auth.default(scopes=SCOPES)
            return bigquery.Client(credentials=credentials, project=PROJECT_ID)
        except Exception as e:
            st.error(f"Failed to load local GCP credentials: {e}")
            return None

client = get_bigquery_client()

# =============================================================================
# 3. PIPELINE EXECUTION BLOCK
# =============================================================================
if client is None:
    st.error("🔒 BigQuery client connection could not be established. Check your secrets configuration.")
else:
    if st.button("🚀 Run Sensor Mapping Migration & Database Repair", use_container_width=True):
        try:
            # --- STEP A: PROCESS AND CLEAN DATA ---
            st.info("⏳ Step A: Reading and deduplicating exported file profiles...")
            df = pd.read_csv(EXPORT_FILE_PATH)
            df_clean = df.drop_duplicates(subset=['Sensor ID (RawID)']).copy()
            
            # Reformat to match BigQuery hardware_inventory expectations exactly
            upload_df = pd.DataFrame({
                'RawID': df_clean['Sensor ID (RawID)'].astype(str).str.strip(),
                'NodeNum': df_clean['App Name (NodeNum)'].astype(str).str.strip()
            })
            st.success(f"✅ Found {len(upload_df)} unique, clean sensor mappings ready for push.")
            
            # --- STEP B: APPEND TO HARDWARE INVENTORY ---
            st.info(f"⏳ Step B: Appending clean maps into BigQuery table: `{INVENTORY_TABLE}`...")
            table_ref = f"{PROJECT_ID}.{DATASET_ID}.{INVENTORY_TABLE}"
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            
            client.load_table_from_dataframe(upload_df, table_ref, job_config=job_config).result()
            st.success("✅ Successfully updated your master hardware inventory assets!")
            
            # --- STEP C: EXECUTE REPAIR QUERY ---
            st.info(f"⏳ Step C: Running buffer-safe repair on `{RAW_DATA_TABLE}`...")
            
            # Create or replace table bypasses the streaming buffer restriction completely
            repair_sql = f"""
            CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.{RAW_DATA_TABLE}` AS
            SELECT 
                r.timestamp,
                -- If it's unmapped and has a match in the inventory, switch it to the clean NodeNum
                COALESCE(
                    IF(r.NodeNum LIKE 'UNMAPPED-%', i.NodeNum, r.NodeNum), 
                    r.NodeNum
                ) AS NodeNum,
                r.temperature,
                r.rssi
            FROM `{PROJECT_ID}.{DATASET_ID}.{RAW_DATA_TABLE}` r
            LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.{INVENTORY_TABLE}` i
              ON r.NodeNum LIKE 'UNMAPPED-%'
             AND SPLIT(r.NodeNum, '-')[SAFE_OFFSET(1)] = SPLIT(TRIM(CAST(i.RawID AS STRING)), '.')[SAFE_OFFSET(0)];
            """
            
            query_job = client.query(repair_sql)
            query_job.result()  # Wait for BigQuery to finish rebuilding the table
            
            st.balloons()
            st.success(f"🎉 Complete Success! Rebuilt `{RAW_DATA_TABLE}` safely. All historical UNMAPPED rows are now permanently aligned to your active hardware assets!")
            
        except FileNotFoundError:
            st.error(f"❌ Could not locate your file at `{EXPORT_FILE_PATH}`. Please check that the file is uploaded to your environment root directory.")
        except Exception as e:
            st.error(f"❌ Migration Error Encountered: {e}")
