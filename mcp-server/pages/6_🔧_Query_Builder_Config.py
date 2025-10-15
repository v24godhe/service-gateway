import streamlit as st
import httpx
import asyncio
from utils.system_admin import SystemAdmin
from utils.metadata_manager import generate_friendly_names
from utils.theme import THEMES
import os

st.set_page_config(page_title="Query Builder Config", layout="wide", page_icon="")

# Initialize
system_admin = SystemAdmin()
GATEWAY_URL = os.getenv("GATEWAY_URL", "http://10.200.0.2:8080")

# Session state
if "username" not in st.session_state:
    st.session_state.username = None
if "selected_table" not in st.session_state:
    st.session_state.selected_table = None
if "column_config" not in st.session_state:
    st.session_state.column_config = {}

# Sidebar
with st.sidebar:
    st.markdown("### ADMIN TOOLS")
    
    # Theme selector
    theme_choice = st.selectbox("Theme", list(THEMES.keys()))
    st.markdown(THEMES[theme_choice], unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Login/User section
    if not st.session_state.username:
        st.markdown("### Admin Login")
        username_input = st.text_input("Username:", placeholder="amila.g")
        
        if st.button("Login", use_container_width=True, type="primary"):
            if username_input:
                if system_admin.is_admin(username_input):
                    st.session_state.username = username_input
                    st.success("Logged in")
                    st.rerun()
                else:
                    st.error("Access denied")
            else:
                st.error("Enter username")
        
        st.info("Dev Admin or Super Admin required")
        st.stop()
    else:
        st.success(f"{st.session_state.username}")
        
        if st.button("Logout", use_container_width=True):
            st.session_state.username = None
            st.session_state.selected_table = None
            st.session_state.column_config = {}
            st.rerun()

# Main content
st.title("Query Builder Configuration")
st.markdown("Configure table metadata for AI-powered queries")
st.markdown("---")

# Step 1: System Selection (hardcoded to STYR for now)
st.markdown("### Step 1: Select System")
system_id = st.selectbox("System", ["STYR"], disabled=True)

st.markdown("---")

# Step 2: Fetch and display tables
st.markdown("### Step 2: Select Table")

if st.button("Fetch Tables", type="secondary"):
    with st.spinner("Loading tables from STYR..."):
        try:
            response = httpx.get(f"{GATEWAY_URL}/api/{system_id}/schema", timeout=30.0)
            if response.status_code == 200:
                schema_data = response.json()
                st.session_state.schema_data = schema_data.get("tables", {})
                st.success(f"Loaded {len(st.session_state.schema_data)} tables")
            else:
                st.error(f"Failed to fetch tables: {response.text}")
        except Exception as e:
            st.error(f"Error: {e}")

if "schema_data" in st.session_state and st.session_state.schema_data:
    table_list = list(st.session_state.schema_data.keys())
    selected_table = st.selectbox("Select Table", table_list)
    
    if selected_table:
        st.session_state.selected_table = selected_table
        
        st.markdown("---")
        
        # Step 3: Display columns
        st.markdown("### Step 3: Configure Columns")
        
        columns = st.session_state.schema_data[selected_table]
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            if st.button("Generate Friendly Names", type="primary"):
                with st.spinner("AI generating friendly names..."):
                    try:
                        api_key = os.getenv("OPENAI_API_KEY")
                        if not api_key:
                            st.error("OPENAI_API_KEY not configured in environment")
                            st.stop()
                        
                        column_names = [col["column_name"] for col in columns]
                        st.info(f"Generating names for {len(column_names)} columns...")
                        
                        friendly_names = generate_friendly_names(column_names)
                        
                        st.write("Generated names:", friendly_names)
                        
                        for col in columns:
                            col_name = col["column_name"]
                            st.session_state.column_config[col_name] = {
                                "friendly_name": friendly_names.get(col_name, col_name),
                                "is_visible": True,
                                "data_type": col["data_type"]
                            }
                        
                        st.success(f"Generated {len(friendly_names)} friendly names!")
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Error generating names: {str(e)}")
                        import traceback
                        st.code(traceback.format_exc())
        
        with col2:
            st.info(f"{len(columns)} columns in {selected_table}")
        
        st.markdown("---")
        
        # Display column configuration
        if st.session_state.column_config:
            st.markdown("#### Configure Column Visibility and Names")
            
            for col in columns:
                col_name = col["column_name"]
                
                if col_name in st.session_state.column_config:
                    col1, col2, col3, col4 = st.columns([1, 3, 2, 1])
                    
                    with col1:
                        is_visible = st.checkbox(
                            "Include",
                            value=st.session_state.column_config[col_name]["is_visible"],
                            key=f"vis_{col_name}"
                        )
                        st.session_state.column_config[col_name]["is_visible"] = is_visible
                    
                    with col2:
                        st.text_input(
                            "Column Name",
                            value=col_name,
                            disabled=True,
                            key=f"col_{col_name}",
                            label_visibility="collapsed"
                        )
                    
                    with col3:
                        friendly_name = st.text_input(
                            "Friendly Name",
                            value=st.session_state.column_config[col_name]["friendly_name"],
                            key=f"friendly_{col_name}",
                            label_visibility="collapsed"
                        )
                        st.session_state.column_config[col_name]["friendly_name"] = friendly_name
                    
                    with col4:
                        st.text(col["data_type"])
            
            st.markdown("---")
            
            # Step 4: Save
            st.markdown("### Step 4: Save Configuration")
            
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                visible_count = sum(1 for cfg in st.session_state.column_config.values() if cfg["is_visible"])
                st.info(f"{visible_count} columns will be saved")
            
            with col3:
                if st.button("Save Configuration", type="primary", use_container_width=True):
                    with st.spinner("Saving..."):
                        try:
                            columns_to_save = []
                            for col_name, config in st.session_state.column_config.items():
                                columns_to_save.append({
                                    "column_name": col_name,
                                    "friendly_name": config["friendly_name"],
                                    "is_visible": config["is_visible"]
                                })
                            
                            payload = {
                                "system_id": system_id,
                                "table_name": selected_table,
                                "columns": columns_to_save,
                                "created_by": st.session_state.username
                            }
                            
                            response = httpx.post(
                                f"{GATEWAY_URL}/api/metadata/save",
                                json=payload,
                                timeout=30.0
                            )
                            
                            if response.status_code == 200:
                                result = response.json()
                                if result.get("success"):
                                    st.success(f"{result.get('message')}")
                                else:
                                    st.error(f"{result.get('error')}")
                            else:
                                st.error(f"Save failed: {response.text}")
                                
                        except Exception as e:
                            st.error(f"Error: {e}")

else:
    st.info("Click 'Fetch Tables' to load table list from STYR database")
