import streamlit as st
import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv
import difflib

load_dotenv('/home/azureuser/forlagssystem-mcp/.env')

st.set_page_config(page_title="Advanced Prompt Management", layout="wide")

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

st.title("🎨 Advanced Prompt Management")

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

# Get all versions of a prompt
def get_all_versions(system_id, prompt_type, role_name):
    conn = get_db_conn()
    cursor = conn.cursor()
    
    if role_name:
        cursor.execute("""
            SELECT prompt_id, prompt_content, version, created_at, created_by, 
                   is_active, effectiveness_score, usage_count, notes
            FROM prompt_configurations
            WHERE system_id = %s AND prompt_type = %s AND role_name = %s
            ORDER BY version DESC
        """, (system_id, prompt_type, role_name))
    else:
        cursor.execute("""
            SELECT prompt_id, prompt_content, version, created_at, created_by, 
                   is_active, effectiveness_score, usage_count, notes
            FROM prompt_configurations
            WHERE system_id = %s AND prompt_type = %s AND role_name IS NULL
            ORDER BY version DESC
        """, (system_id, prompt_type))
    
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    
    versions = []
    for row in results:
        versions.append({
            'prompt_id': row[0],
            'content': row[1],
            'version': row[2],
            'created_at': row[3],
            'created_by': row[4],
            'is_active': row[5],
            'effectiveness_score': row[6] or 0,
            'usage_count': row[7] or 0,
            'notes': row[8] or ''
        })
    
    return versions

# Save new version
def save_prompt_version(system_id, prompt_type, role_name, content, username, notes=''):
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
        (system_id, prompt_type, role_name, prompt_content, version, created_by, is_active, notes)
        VALUES (%s, %s, %s, %s, %s, %s, TRUE, %s)
    """, (system_id, prompt_type, role_name, content, new_version, username, notes))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    # Invalidate cache
    from utils.prompt_manager import PromptManager
    pm = PromptManager()
    pm.invalidate_cache(system_id)

# Rollback to a specific version
def rollback_to_version(prompt_id, system_id, prompt_type, role_name):
    conn = get_db_conn()
    cursor = conn.cursor()
    
    # Deactivate all versions
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
    
    # Activate selected version
    cursor.execute("""
        UPDATE prompt_configurations SET is_active = TRUE
        WHERE prompt_id = %s
    """, (prompt_id,))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    # Invalidate cache
    from utils.prompt_manager import PromptManager
    pm = PromptManager()
    pm.invalidate_cache(system_id)

# Update effectiveness score
def update_effectiveness(prompt_id, score):
    conn = get_db_conn()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE prompt_configurations 
        SET effectiveness_score = %s
        WHERE prompt_id = %s
    """, (score, prompt_id))
    conn.commit()
    cursor.close()
    conn.close()

# Generate diff HTML
def generate_diff_html(old_text, new_text):
    diff = difflib.unified_diff(
        old_text.splitlines(keepends=True),
        new_text.splitlines(keepends=True),
        lineterm='',
        fromfile='Previous Version',
        tofile='Current Version'
    )
    
    html = '<div style="font-family: monospace; font-size: 12px;">'
    for line in diff:
        if line.startswith('+'):
            html += f'<div style="background-color: #d4edda; color: #155724;">{line}</div>'
        elif line.startswith('-'):
            html += f'<div style="background-color: #f8d7da; color: #721c24;">{line}</div>'
        elif line.startswith('@@'):
            html += f'<div style="background-color: #d1ecf1; color: #0c5460;">{line}</div>'
        else:
            html += f'<div>{line}</div>'
    html += '</div>'
    return html

# Sidebar
with st.sidebar:
    # Navigation
    st.markdown("### 🏠 Navigation")
    if st.button("← Home", use_container_width=True):
        st.switch_page("Home.py")
    
    st.markdown("---")
    
    # Logo
    st.image("https://www.forlagssystem.se/wp-content/uploads/2023/02/forlagssystem_logo_white.svg", use_container_width=True)
    st.markdown("---")
    
    # Page Navigation
    st.markdown("### 🛠️ DEV ADMIN")
    if st.button("🎨 Prompt Management", use_container_width=True, disabled=True):
        pass  # Current page
    if st.button("🗄️ System Management", use_container_width=True):
        st.switch_page("pages/5_🗄_System_Management.py")
    if st.button("📈 Analytics", use_container_width=True):
        st.switch_page("pages/6_📈_Analytics_Chat_Assistant.py")
    
    st.markdown("---")
    
    # User Info
    st.markdown("### 👤 Dev Admin")
    st.success(f"👤 {st.session_state.username}")
    
    # Keep existing prompt selector below this...


# Tab selection
tab1, tab2, tab3 = st.tabs(["📝 Editor", "📊 Version History", "📈 Analytics"])

# Sidebar - Prompt Selection
with st.sidebar:
    st.markdown("### Select Prompt")
    
    systems = get_active_systems()
    selected_system = st.selectbox(
        "System",
        options=[s[0] for s in systems],
        format_func=lambda x: f"{x} - {next(s[1] for s in systems if s[0] == x)}"
    )
    
    roles = ['harold', 'lars', 'pontus', 'peter', 'linda', None]
    selected_role = st.selectbox("Role", options=roles, format_func=lambda x: x if x else "Global")
    
    prompt_types = ['ROLE_SYSTEM', 'SQL_GENERATION', 'CHART_INTELLIGENCE', 'FORMAT_RESPONSE']
    selected_type = st.selectbox("Prompt Type", options=prompt_types)

# Load all versions
versions = get_all_versions(selected_system, selected_type, selected_role)
active_version = next((v for v in versions if v['is_active']), None)

# ==================== TAB 1: EDITOR ====================
with tab1:
    st.markdown("## 📝 Prompt Editor")
    
    if active_version:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Current Version", f"v{active_version['version']}")
        with col2:
            st.metric("Effectiveness", f"{active_version['effectiveness_score']}%")
        with col3:
            st.metric("Usage Count", active_version['usage_count'])
        
        st.info(f"📅 Created: {active_version['created_at']} | 👤 By: {active_version['created_by']}")
        
        new_content = st.text_area(
            "Prompt Content",
            value=active_version['content'],
            height=400,
            key="prompt_editor"
        )
        
        notes = st.text_input("Version Notes (optional)", placeholder="e.g., Improved clarity, fixed date formatting")
        
        col1, col2 = st.columns([1, 5])
        with col1:
            if st.button("💾 Save New Version", type="primary"):
                if new_content != active_version['content']:
                    save_prompt_version(selected_system, selected_type, selected_role, new_content, st.session_state.username, notes)
                    st.success("✅ Prompt saved! Cache invalidated.")
                    st.rerun()
                else:
                    st.warning("⚠️ No changes detected")
    else:
        st.warning("⚠️ No prompt found for this configuration")
        
        new_content = st.text_area("Create New Prompt", height=400)
        notes = st.text_input("Version Notes", placeholder="Initial version")
        
        if st.button("💾 Create Prompt", type="primary"):
            if new_content.strip():
                save_prompt_version(selected_system, selected_type, selected_role, new_content, st.session_state.username, notes)
                st.success("✅ Prompt created!")
                st.rerun()

# ==================== TAB 2: VERSION HISTORY ====================
with tab2:
    st.markdown("## 📊 Version History & Comparison")
    
    if len(versions) > 0:
        st.markdown(f"**Total Versions:** {len(versions)}")
        
        # Version comparison selector
        col1, col2 = st.columns(2)
        with col1:
            compare_v1 = st.selectbox("Compare Version 1", 
                                     options=[v['version'] for v in versions],
                                     format_func=lambda x: f"v{x} {'(Active)' if any(v['version']==x and v['is_active'] for v in versions) else ''}")
        with col2:
            compare_v2 = st.selectbox("Compare Version 2", 
                                     options=[v['version'] for v in versions],
                                     index=min(1, len(versions)-1),
                                     format_func=lambda x: f"v{x} {'(Active)' if any(v['version']==x and v['is_active'] for v in versions) else ''}")
        
        if st.button("🔍 Show Differences"):
            v1 = next((v for v in versions if v['version'] == compare_v1), None)
            v2 = next((v for v in versions if v['version'] == compare_v2), None)
            
            if v1 and v2:
                st.markdown("### Side-by-Side Comparison")
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown(f"#### Version {v1['version']}")
                    st.markdown(f"*Created: {v1['created_at']}*")
                    st.text_area("", value=v1['content'], height=300, key="v1_content", disabled=True)
                
                with col2:
                    st.markdown(f"#### Version {v2['version']}")
                    st.markdown(f"*Created: {v2['created_at']}*")
                    st.text_area("", value=v2['content'], height=300, key="v2_content", disabled=True)
                
                st.markdown("### Detailed Changes")
                diff_html = generate_diff_html(v1['content'], v2['content'])
                st.markdown(diff_html, unsafe_allow_html=True)
        
        st.markdown("---")
        st.markdown("### All Versions")
        
        # Display all versions
        for v in versions:
            with st.expander(f"{'✅ ' if v['is_active'] else ''}Version {v['version']} - {v['created_at'].strftime('%Y-%m-%d %H:%M') if isinstance(v['created_at'], datetime) else v['created_at']}"):
                col1, col2, col3, col4 = st.columns([2, 2, 2, 2])
                
                with col1:
                    st.markdown(f"**Created by:** {v['created_by']}")
                with col2:
                    st.markdown(f"**Status:** {'🟢 Active' if v['is_active'] else '⚪ Inactive'}")
                with col3:
                    st.markdown(f"**Effectiveness:** {v['effectiveness_score']}%")
                with col4:
                    st.markdown(f"**Usage:** {v['usage_count']} times")
                
                if v['notes']:
                    st.info(f"📝 Notes: {v['notes']}")
                
                st.text_area(f"Content v{v['version']}", value=v['content'], height=200, disabled=True, key=f"content_{v['prompt_id']}")
                
                col1, col2 = st.columns(2)
                with col1:
                    if not v['is_active']:
                        if st.button(f"↩️ Rollback to v{v['version']}", key=f"rollback_{v['prompt_id']}"):
                            rollback_to_version(v['prompt_id'], selected_system, selected_type, selected_role)
                            st.success(f"✅ Rolled back to version {v['version']}")
                            st.rerun()
                
                with col2:
                    new_score = st.number_input(f"Update Effectiveness", 
                                               min_value=0.0, max_value=100.0, 
                                               value=float(v['effectiveness_score']),
                                               key=f"score_{v['prompt_id']}")
                    if new_score != v['effectiveness_score']:
                        if st.button("💾 Save Score", key=f"save_score_{v['prompt_id']}"):
                            update_effectiveness(v['prompt_id'], new_score)
                            st.success("Score updated!")
                            st.rerun()
    else:
        st.warning("No version history available")

# ==================== TAB 3: ANALYTICS ====================
with tab3:
    st.markdown("## 📈 Prompt Analytics")
    
    if len(versions) > 0:
        # Effectiveness chart
        import pandas as pd
        import plotly.express as px
        
        df = pd.DataFrame([{
            'Version': f"v{v['version']}",
            'Effectiveness': v['effectiveness_score'],
            'Usage Count': v['usage_count'],
            'Created': v['created_at']
        } for v in versions])
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### Effectiveness Over Versions")
            fig = px.line(df, x='Version', y='Effectiveness', markers=True, 
                         title="Prompt Effectiveness Trend")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("### Usage Statistics")
            fig = px.bar(df, x='Version', y='Usage Count', 
                        title="Version Usage Count")
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("### Summary Statistics")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            avg_effectiveness = df['Effectiveness'].mean()
            st.metric("Avg Effectiveness", f"{avg_effectiveness:.1f}%")
        
        with col2:
            total_usage = df['Usage Count'].sum()
            st.metric("Total Usage", total_usage)
        
        with col3:
            best_version = df.loc[df['Effectiveness'].idxmax(), 'Version']
            st.metric("Best Version", best_version)
        
        with col4:
            st.metric("Total Versions", len(versions))
    else:
        st.info("No analytics data available yet")