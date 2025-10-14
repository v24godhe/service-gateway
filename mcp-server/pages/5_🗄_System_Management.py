import streamlit as st
from utils.system_admin import SystemAdmin
import httpx
import asyncio
import streamlit as st
from utils.theme import THEMES

st.set_page_config(page_title="System Management", layout="wide", page_icon="🗄")

# Initialize admin
system_admin = SystemAdmin()

# Authentication check
if "username" not in st.session_state:
    st.session_state.username = None

# Sidebar
with st.sidebar:
    # Page Navigation
    st.markdown("### 🛠️ DEV ADMIN")
    if st.button("🎨 Prompt Management", use_container_width=True):
        st.switch_page("pages/4_🎨_Prompt_Management.py")
    if st.button("🗄️ System Management", use_container_width=True, disabled=True):
        pass  # Current page
    if st.button("📈 Analytics", use_container_width=True):
        st.switch_page("pages/6_📈_Analytics_Chat_Assistant.py")

    st.markdown("---")
    theme_choice = st.sidebar.selectbox("🎨 Theme", list(THEMES.keys()))
    st.markdown(THEMES[theme_choice], unsafe_allow_html=True)  
    
    # Login/User section
    if not st.session_state.username:
        st.markdown("### 🔐 Dev Admin Login")
        username_input = st.text_input("Username:", placeholder="amila.g")
        
        if st.button("Login", use_container_width=True, type="primary"):
            if username_input:
                if system_admin.is_dev_admin(username_input):
                    st.session_state.username = username_input
                    st.success(f"✅ Logged in")
                    st.rerun()
                else:
                    st.error("⛔ Access denied")
            else:
                st.error("Enter username")
        
        st.info("💡 Dev Admin: amila.g")
        st.stop()
    else:
        st.success(f"👤 {st.session_state.username}")
        
        if st.button("Logout", use_container_width=True):
            st.session_state.username = None
            st.rerun()

    # Theme selector
 
st.title("🗄 System Management")
st.markdown("---")

# Tabs
tab1, tab2 = st.tabs(["📋 System List", "➕ Add System"])

# TAB 1: System List
with tab1:
    systems = system_admin.get_all_systems()
    
    if not systems:
        st.warning("No systems configured")
    else:
        for system in systems:
            with st.expander(f"{'🟢' if system['is_active'] else '🔴'} {system['system_id']} - {system['system_name']}", expanded=False):
                
                col1, col2, col3 = st.columns([2, 2, 1])
                
                with col1:
                    st.markdown("**System Info**")
                    st.text(f"Type: {system['system_type']}")
                    st.text(f"Status: {'Active' if system['is_active'] else 'Inactive'}")
                    st.text(f"Created by: {system['created_by']}")
                    
                with col2:
                    st.markdown("**Configuration**")
                    st.text(f"Gateway: {system['gateway_base_url']}")
                    st.text(f"Timeout: {system['query_timeout_sec']}s")
                    st.text(f"Max Connections: {system['max_connections']}")
                
                with col3:
                    st.markdown("**Actions**")
                    
                    # Toggle status
                    if st.button(f"{'Disable' if system['is_active'] else 'Enable'}", 
                                key=f"toggle_{system['system_id']}", 
                                type="secondary"):
                        success, msg = system_admin.toggle_system(system['system_id'])
                        if success:
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)
                    
                    # Test connection
                    if st.button("Test Connection", key=f"test_{system['system_id']}", type="primary"):
                        with st.spinner(f"Testing {system['system_id']}..."):
                            try:
                                async def test_conn():
                                    async with httpx.AsyncClient(timeout=10.0) as client:
                                        response = await client.get(
                                            f"http://10.200.0.2:8080/api/system/test-connection",
                                            params={"system_id": system['system_id']}
                                        )
                                        return response.json()
                                
                                result = asyncio.run(test_conn())
                                
                                if result.get('success'):
                                    st.success(f"✅ {result.get('message', 'Connection successful')}")
                                else:
                                    st.error(f"❌ {result.get('message', 'Connection failed')}")
                            except Exception as e:
                                st.error(f"❌ Test failed: {str(e)}")
                
                # Description
                if system['description']:
                    st.markdown("**Description**")
                    st.info(system['description'])
                
                # Delete (only if not STYR)
                if system['system_id'] != 'STYR':
                    st.markdown("---")
                    if st.button(f"🗑 Delete {system['system_id']}", key=f"del_{system['system_id']}", type="secondary"):
                        success, msg = system_admin.delete_system(system['system_id'])
                        if success:
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)

# TAB 2: Add System
with tab2:
    st.markdown("### Add New System")
    
    with st.form("add_system_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            new_system_id = st.text_input("System ID*", placeholder="JEEVES", help="Uppercase, no spaces (e.g., JEEVES, ASTRO, ERP)")
            new_system_name = st.text_input("System Name*", placeholder="Jeeves ERP System")
            new_system_type = st.selectbox("Database Type*", ["DB2", "MSSQL", "PostgreSQL", "Oracle", "MySQL"])
        
        with col2:
            new_description = st.text_area("Description", placeholder="Optional description of this system")
        
        submitted = st.form_submit_button("Add System", type="primary")
        
        if submitted:
            if not new_system_id or not new_system_name:
                st.error("System ID and Name are required")
            elif not new_system_id.isupper() or ' ' in new_system_id:
                st.error("System ID must be UPPERCASE with no spaces")
            else:
                success, msg = system_admin.add_system(
                    new_system_id,
                    new_system_name,
                    new_system_type,
                    st.session_state.username,
                    new_description
                )
                
                if success:
                    st.success(msg)
                    st.info("⚠️ Next steps:\n1. Add connection params to Windows .env\n2. Restart Windows service\n3. Test connection")
                else:
                    st.error(msg)

