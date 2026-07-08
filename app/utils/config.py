# app/utils/config.py
DATASET_ID = "Temperature" 
PROJECT_ID = "sensorpush-export"
PROJECT_REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.project_registry"
NODE_REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.node_registry"
MASTER_VIEW = f"{PROJECT_ID}.{DATASET_ID}.master_data_view_v2" 
REF_CURVE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.reference_curves"
