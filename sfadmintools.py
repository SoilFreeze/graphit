import requests
import pandas as pd

def get_fleet_telemetry(api_base_url, headers):
    """
    Pings API to fetch full node fleet registry.
    """
    fleet_data = []
    
    # 1. Fetch all accounts/gateways
    response = requests.get(f"{api_base_url}/gateways", headers=headers)
    gateways = response.json()
    
    for gw in gateways:
        gw_id = gw['id']
        # 2. Fetch devices for this gateway
        devices = requests.get(f"{api_base_url}/gateways/{gw_id}/devices", headers=headers).json()
        
        for dev in devices:
            fleet_data.append({
                'NodeNum': dev.get('name'),         # Your user-defined name
                'PhysicalID': dev.get('serial'),    # Hardware MAC/Serial
                'LastCheckIn': dev.get('last_seen') # ISO timestamp
            })
            
    return pd.DataFrame(fleet_data)

# Usage:
# df_fleet = get_fleet_telemetry(API_URL, AUTH_HEADERS)
# st.dataframe(df_fleet)
