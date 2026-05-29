import requests

ACCOUNTS = [
    {'email': 'tsteele@soilfreeze.com', 'password': 'Freeze123!!'},
    {'email': 'ldunham@soilfreeze.com', 'password': 'Freeze123!!'},
    {'email': 'soilfreeze98072@gmail.com', 'password': 'Freeze123!!'}
]
BASE_URL = "https://api.sensorpush.com/api/v1"

print(f"{'Account Owner':<30} | {'Sensor ID (RawID)':<20} | {'App Name (NodeNum)':<30}")
print("-" * 88)

for acc in ACCOUNTS:
    try:
        # Authenticate
        auth_r = requests.post(f"{BASE_URL}/oauth/authorize", json=acc, timeout=15).json()
        token = requests.post(f"{BASE_URL}/oauth/accesstoken", json={"authorization": auth_r['authorization']}, timeout=15).json().get('accesstoken')

        # Request paired sensor profiles
        s_resp = requests.post(f"{BASE_URL}/devices/sensors", headers={"Authorization": token}, json={}, timeout=20).json()
        
        if isinstance(s_resp, dict):
            for s_id, s_meta in s_resp.items():
                # Isolate clean base ID integer (e.g. "17030602")
                clean_raw_id = str(s_id).strip().split('.')[0]
                # Get the name you named it in the app
                app_name = s_meta.get('name', 'Unknown Name') if isinstance(s_meta, dict) else 'Unknown Name'
                
                print(f"{acc['email']:<30} | {clean_raw_id:<20} | {app_name:<30}")
        else:
            for s in s_resp:
                if isinstance(s, dict) and 'id' in s:
                    clean_raw_id = str(s['id']).strip().split('.')[0]
                    app_name = s.get('name', 'Unknown Name')
                    print(f"{acc['email']:<30} | {clean_raw_id:<20} | {app_name:<30}")
                    
    except Exception as e:
        print(f"Error accessing profiles for {acc['email']}: {e}")
