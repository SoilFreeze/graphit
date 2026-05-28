import streamlit as st
import requests

st.title("🚀 Remote Service Synchronization Gateway")
st.info("Triggers a manual configuration sync loop on your background Cloud Run ingress container.")

# Enter your background container execution service link
# (Look at your handle_recovery_trigger function to copy your actual Cloud Run URL domain)
cloud_run_url = st.text_input("Cloud Run Ingress Base URL", value="https://sensorpush-ingress-service-execution-link")

st.divider()

if st.button("⚡ Force Remote Cluster Resync", type="primary"):
    if "sensorpush-ingress-service" in cloud_run_url or "link" in cloud_run_url:
        st.warning("⚠️ Make sure to paste your actual Cloud Run URL from your handle_recovery_trigger function above!")
    
    sync_endpoint = f"{cloud_run_url.strip('/')}/sync" # or /refresh depending on your architecture
    
    payload = {
        "action": "reload_inventory_cache",
        "target_project": "2541-Blackjack Phase2",
        "force_flush": True
    }
    
    try:
        with st.spinner("Waking up Cloud Run microservice and forcing internal registry sync..."):
            # Call your container safely over standard HTTPS webhook paths
            response = requests.post(sync_endpoint, json=payload, timeout=30)
            
            if response.status_code == 200:
                st.success("✅ Cloud Run container successfully reloaded!")
                st.write("### 📦 Server Execution Response Details:")
                st.json(response.json())
                st.info("The background collector has pulled your clean TP- labels. Live streaming will resume on the next hour interval.")
            else:
                st.error(f"❌ Server returned status {response.status_code}: {response.text}")
                st.write("Retrying flat trigger at root domain level...")
                
                # Fallback to hitting the primary gateway endpoint to kickstart the container routine
                root_res = requests.post(cloud_run_url, json={"command": "sync"}, timeout=30)
                st.write("Root endpoint ping results:")
                st.write(root_res.text)
                
    except Exception as e:
        st.error(f"Failed to communicate with remote container: {e}")
