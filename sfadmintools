import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# ===============================================================
# 1. ADMIN CONFIGURATION
# ===============================================================
st.set_page_config(page_title="SoilFreeze Engineering Admin", layout="wide")

# SECURITY GATE
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    pwd = st.text_input("Enter Admin Password", type="password")
    if st.button("Unlock Admin Tools"):
        if pwd == st.secrets["admin_password"]:
            st.session_state.authenticated = True
            st.rerun()
        else: st.error("Access Denied.")
    st.stop()

# ===============================================================
# 2. SIDEBAR & TOOLS
# ===============================================================
st.sidebar.title("🛠️ Engineering Admin")
admin_page = st.sidebar.radio("Management Tool", 
    ["Commissioning Audit", "Project Master", "Node Logistics", "Soil Curve Library", "Surgical Cleaner"])

# ===============================================================
# 3. ADMIN ROUTER
# ===============================================================
if admin_page == "Commissioning Audit":
    # This is your existing 'Node Diagnostics' - moved here for setup use
    render_node_diagnostics(selected_project, "UTC", "°F")

elif admin_page == "Project Master":
    st.header("⚙️ Project Lifecycle Management")
    # [Include Tab 3: PROJECT MASTER logic here]

elif admin_page == "Node Logistics":
    st.header("📋 Hardware Assignment & Deployment")
    # [Include Tab 2: NODE LOGISTICS logic here]

elif admin_page == "Soil Curve Library":
    st.header("📈 Theoretical Curve Management")
    # [Include Tab: REFERENCE CURVE LIBRARY logic here]

elif admin_page == "Surgical Cleaner":
    render_surgical_cleaner(selected_project, "UTC", "Fahrenheit", "°F")
