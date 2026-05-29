import pandas as pd
from google.cloud import bigquery
import google.auth

# 1. Configuration
PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature"
INVENTORY_TABLE = "hardware_inventory"
RAW_DATA_TABLE = "raw_sensorpush"
EXPORT_FILE_PATH = "2026-05-29T20-25_export.csv"

# 2. Authenticate
scopes = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform"]
credentials, project = google.auth.default(scopes=scopes)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

print("⏳ Step 1: Processing and cleaning exported mapping data...")
# Read and clean the export file
df = pd.read_csv(EXPORT_FILE_PATH)
df_clean = df.drop_duplicates(subset=['Sensor ID (RawID)']).copy()

# Format columns to match your BigQuery table schema exactly
upload_df = pd.DataFrame({
    'RawID': df_clean['Sensor ID (RawID)'].astype(str).str.strip(),
    'NodeNum': df_clean['App Name (NodeNum)'].astype(str).str.strip()
})

print(f"✅ Found {len(upload_df)} unique, clean sensor profiles to sync.")

print(f"⏳ Step 2: Appending clean maps into BigQuery table: {INVENTORY_TABLE}...")
# Append records into your hardware inventory
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{INVENTORY_TABLE}"
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
client.load_table_from_dataframe(upload_df, table_ref, job_config=job_config).result()
print("✅ Successfully updated hardware inventory table!")

print(f"⏳ Step 3: Executing database repair query on {RAW_DATA_TABLE}...")
# Update historical data matching the new hardware mapping entries
repair_sql = f"""
UPDATE `{PROJECT_ID}.{DATASET_ID}.{RAW_DATA_TABLE}` r
SET r.NodeNum = i.NodeNum
FROM `{PROJECT_ID}.{DATASET_ID}.{INVENTORY_TABLE}` i
WHERE r.NodeNum LIKE 'UNMAPPED-%'
  AND SPLIT(r.NodeNum, '-')[SAFE_OFFSET(1)] = SPLIT(TRIM(CAST(i.RawID AS STRING)), '.')[SAFE_OFFSET(0)];
"""

query_job = client.query(repair_sql)
query_job.result()
print(f"🎉 Success! Repaired historical entries. {query_job.num_dml_affected_rows} rows updated successfully.")
