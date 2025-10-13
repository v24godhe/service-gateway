import streamlit as st
import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv('/home/azureuser/forlagssystem-mcp/.env')

st.set_page_config(page_title="Prompt Management", layout="wide")

# Authentication check
if "username" not in st.session_state:
    st.error("❌ Please login first")
    st.stop()

# Check if user is dev_admin
def is_dev_admin(username):
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='prompt_management_db',
            user=os.getenv('POSTGRES_USER', 'n8n_user'),
            password=os.getenv('POSTGRES_PASSWORD')
        )
        cursor = conn.cursor()
        cursor.execute(
            "SELECT can_edit_prompts FROM dev_admins WHERE username = %s AND is_active = TRUE",
            (username,)
        )
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result[0] if result else False
    except Exception as e:
        st.error(f"Database error: {e}")
        return False

if not is_dev_admin(st.session_state.username):
    st.error("❌ Access denied. Dev Admin privileges required.")
    st.stop()

st.title("🎨 Prompt Management")

# Get database connection
def get_db_conn():
    return psycopg2.connect(
        host='localhost',
        port=5432,
        database='prompt_management_db',
        user=os.getenv('POSTGRES_USER', 'n8n_user'),
        password=os.getenv('POSTGRES_PASSWORD')
    )

# Get active systems
def get_active_systems():
    conn = get_db_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT system_id, system_name FROM system_configurations WHERE is_active = TRUE")
    systems = cursor.fetchall()
    cursor.close()
    conn.close()
    return systems

# Get prompt
def get_prompt(system_id, prompt_type, role_name):
    conn = get_db_conn()
    cursor = conn.cursor()
    
    if role_name:
        cursor.execute("""
            SELECT prompt_id, prompt_content, version, created_at, created_by 
            FROM prompt_configurations
            WHERE system_id = %s AND prompt_type = %s AND role_name = %s AND is_active = TRUE
            ORDER BY version DESC LIMIT 1
        """, (system_id, prompt_type, role_name))
    else:
        cursor.execute("""
            SELECT prompt_id, prompt_content, version, created_at, created_by 
            FROM prompt_configurations
            WHERE system_id = %s AND prompt_type = %s AND role_name IS NULL AND is_active = TRUE
            ORDER BY version DESC LIMIT 1
        """, (system_id, prompt_type))
    
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    
    if result:
        return {
            'prompt_id': result[0],
            'content': result[1],
            'version': result[2],
            'created_at': result[3],
            'created_by': result[4]
        }
    return None

# Save new version
def save_prompt_version(system_id, prompt_type, role_name, content, username):
    conn = get_db_conn()
    cursor = conn.cursor()
    
    # Get current max version
    if role_name:
        cursor.execute("""
            SELECT COALESCE(MAX(version), 0) FROM prompt_configurations
            WHERE system_id = %s AND prompt_type = %s AND role_name = %s
        """, (system_id, prompt_type, role_name))
    else:
        cursor.execute("""
            SELECT COALESCE(MAX(version), 0) FROM prompt_configurations
            WHERE system_id = %s AND prompt_type = %s AND role_name IS NULL
        """, (system_id, prompt_type))
    
    max_version = cursor.fetchone()[0]
    new_version = max_version + 1
    
    # Deactivate old versions
    if role_name:
        cursor.execute("""
            UPDATE prompt_configurations SET is_active = FALSE
            WHERE system_id = %s AND prompt_type = %s AND role_name = %s
        """, (system_id, prompt_type, role_name))
    else:
        cursor.execute("""
            UPDATE prompt_configurations SET is_active = FALSE
            WHERE system_id = %s AND prompt_type = %s AND role_name IS NULL
        """, (system_id, prompt_type))
    
    # Insert new version
    cursor.execute("""
        INSERT INTO prompt_configurations 
        (system_id, prompt_type, role_name, prompt_content, version, created_by, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, TRUE)
    """, (system_id, prompt_type, role_name, content, new_version, username))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    # Invalidate cache
    from utils.prompt_manager import PromptManager
    pm = PromptManager()
    pm.invalidate_cache(system_id)

# UI
systems = get_active_systems()
selected_system = st.selectbox(
    "System",
    options=[s[0] for s in systems],
    format_func=lambda x: f"{x} - {next(s[1] for s in systems if s[0] == x)}"
)

roles = ['harold', 'lars', 'pontus', 'peter', 'linda', None]
selected_role = st.selectbox("Role", options=roles, format_func=lambda x: x if x else "Global (no role)")

prompt_types = ['ROLE_SYSTEM', 'SQL_GENERATION', 'CHART_INTELLIGENCE', 'FORMAT_RESPONSE']
selected_type = st.selectbox("Prompt Type", options=prompt_types)

# Load current prompt
current_prompt = get_prompt(selected_system, selected_type, selected_role)

st.markdown("---")

if current_prompt:
    st.info(f"📝 Current Version: v{current_prompt['version']} | Created: {current_prompt['created_at']} | By: {current_prompt['created_by']}")
    
    new_content = st.text_area(
        "Prompt Content",
        value=current_prompt['content'],
        height=400,
        key="prompt_editor"
    )
    
    col1, col2 = st.columns([1, 5])
    with col1:
        if st.button("💾 Save New Version", type="primary"):
            if new_content != current_prompt['content']:
                save_prompt_version(selected_system, selected_type, selected_role, new_content, st.session_state.username)
                st.success("✅ Prompt saved! Cache invalidated.")
                st.rerun()
            else:
                st.warning("⚠️ No changes detected")
else:
    st.warning("⚠️ No prompt found for this configuration")
    
    new_content = st.text_area("Create New Prompt", height=400)
    
    if st.button("💾 Create Prompt", type="primary"):
        if new_content.strip():
            save_prompt_version(selected_system, selected_type, selected_role, new_content, st.session_state.username)
            st.success("✅ Prompt created!")
            st.rerun()