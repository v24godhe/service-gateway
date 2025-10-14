"""
Förlagssystem - Modern Landing Page
Location: ~/forlagssystem-mcp/mcp-server/Home.py
Entry point for the multipage app with modern theme styling
"""

import streamlit as st
from datetime import datetime

st.set_page_config(
    page_title="Förlagssystem - AI Hub",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Main Layout ---
col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    st.image(
        "https://www.forlagssystem.se/wp-content/uploads/2023/02/forlagssystem_logo.svg",
        use_container_width=True
    )
    st.markdown("<h1 style='text-align:center; margin-top:30px;'>AI-Powered Enterprise Hub</h1>", unsafe_allow_html=True)
    st.markdown("<p class='hero-text'>Your intelligent assistant for business data and system management</p>", unsafe_allow_html=True)
    st.markdown("---")

# --- Feature Cards ---
col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    <div class="feature-card">
        <h2>💬 AI Chat Assistant</h2>
        <p>Ask questions in natural language and get instant answers from your business data.</p>
        <ul>
            <li>Natural language queries</li>
            <li>Real-time data access</li>
            <li>Role-based permissions</li>
            <li>Conversation memory</li>
            <li>Export to PDF/Excel</li>
            <li>Follow-up question handling</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    if st.button("🚀 Open Chat Assistant", key="chat_btn"):
        st.switch_page("pages/1_💬_Chat_Assistant.py")

with col2:
    st.markdown("""
    <div class="feature-card">
        <h2>🔐 Admin Dashboard</h2>
        <p>Manage user permissions, approve access requests, and monitor system activity.</p>
        <ul>
            <li>Permission management</li>
            <li>One-click approve/deny</li>
            <li>RBAC rule editor</li>
            <li>Audit trail viewing</li>
            <li>Real-time statistics</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    if st.button("🛡️ Open Admin Dashboard", key="admin_btn"):
        st.switch_page("pages/2_🔐_Admin_Dashboard.py")

st.markdown("<br>", unsafe_allow_html=True)

# --- Statistics Row ---
st.markdown("### 📊 System Overview")
col1, col2, col3, col4 = st.columns(4)

stats = [
    ("5", "Active Roles"),
    ("11", "Database Tables"),
    ("40+", "RBAC Rules"),
    ("24/7", "Availability")
]

for col, (value, label) in zip([col1, col2, col3, col4], stats):
    col.markdown(f"""
    <div class="stats-box">
        <h3>{value}</h3>
        <p>{label}</p>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br><br>", unsafe_allow_html=True)

# --- Footer ---
st.markdown("---")
col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st.markdown(f"""
    <p style='text-align: center; color: #999; font-size: 14px;'>
        Förlagssystem AB | AI Enterprise Hub v2.0<br>
        System Time: {current_time}
    </p>
    """, unsafe_allow_html=True)
