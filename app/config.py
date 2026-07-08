# 1. CONFIGURATION & STYLING
st.set_page_config(
    page_title="SoilFreeze Data Lab", 
    page_icon="❄️", 
    layout="wide"
)

# Global Database Constants - Linked to Read-Only Infrastructure
DATASET_ID = "Temperature" 
PROJECT_ID = "sensorpush-export"

# Schema-Aligned Table References
PROJECT_REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.project_registry"
NODE_REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.node_registry"

# THE UPGRADE: Pointing to the new flattened Phase/System view
MASTER_VIEW = f"{PROJECT_ID}.{DATASET_ID}.master_data_view_v2" 
REF_CURVE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.reference_curves"
