"""
Förlagssystem - Main Landing Page
Location: ~/forlagssystem-mcp/mcp-server/Home.py
This is the entry point for the multipage app
"""

import streamlit as st
from datetime import datetime

st.set_page_config(
    page_title="Förlagssystem - AI Hub",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Company colors and styling
st.markdown("""
<style>
    :root {
        --primary-color: #0073AE;
        --background-color: #f2f4f8;
        --secondary-bg: #0F2436;
        --text-color: #0F2436;
    }

    .main {
        background-color: #f2f4f8;
    }

    .stButton>button {
        background-color: #0073AE;
        color: white;
        border-radius: 8px;
        padding: 1rem 3rem;
        font-weight: 600;
        border: none;
        transition: all 0.3s;
        font-size: 18px;
        width: 100%;
    }

    .stButton>button:hover {
        background-color: #005a8a;
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }

    .feature-card {
        background: white;
        padding: 30px;
        border-radius: 15px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        margin: 20px 0;
        border-left: 5px solid #0073AE;
        transition: all 0.3s;
    }

    .feature-card:hover {
        box-shadow: 0 6px 12px rgba(0,0,0,0.15);
        transform: translateY(-5px);
    }

    h1, h2, h3 {
        color: #0F2436;
        font-weight: 700;
    }

    .hero-text {
        font-size: 24px;
        color: #666;
        text-align: center;
        margin: 20px 0;
    }

    .stats-box {
        background: linear-gradient(135deg, #0073AE 0%, #005a8a 100%);
        color: white;
        padding: 20px;
        border-radius: 10px;
        text-align: center;
        margin: 10px 0;
    }
</style>
""", unsafe_allow_html=True)

# Hide sidebar on home page
st.markdown("""
<style>
    [data-testid="stSidebar"] {
        display: none;
    }
</style>
""", unsafe_allow_html=True)

# Main content
col1, col2, col3 = st.columns([1, 2, 1])

with col2:
    # Company logo
    st.image("https://www.forlagssystem.se/wp-content/uploads/2023/02/forlagssystem_logo.svg",
             use_container_width=True)
    
    st.markdown("<h1 style='text-align: center; margin-top: 30px;'>AI-Powered Enterprise Hub</h1>", 
                unsafe_allow_html=True)
    
    st.markdown("<p class='hero-text'>Your intelligent assistant for business data and system management</p>", 
                unsafe_allow_html=True)
    
    st.markdown("---")

# Feature cards
col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    <div class="feature-card">
        <h2>💬 AI Chat Assistant</h2>
        <p style="font-size: 16px; color: #666; margin: 15px 0;">
            Ask questions in natural language and get instant answers from your business data.
        </p>
        <ul style="color: #666; font-size: 15px;">
            <li>Natural language queries</li>
            <li>Real-time data access</li>
            <li>Role-based permissions</li>
            <li>Conversation memory</li>
            <li>Export to PDF/Excel</li>
            <li>Conversation memory & context</li>
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
        <p style="font-size: 16px; color: #666; margin: 15px 0;">
            Manage user permissions, approve access requests, and monitor system activity.
        </p>
        <ul style="color: #666; font-size: 15px;">
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

# Statistics row
st.markdown("### 📊 System Overview")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown("""
    <div class="stats-box">
        <h3 style="margin: 0; color: white;">5</h3>
        <p style="margin: 5px 0 0 0; color: white;">Active Roles</p>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("""
    <div class="stats-box">
        <h3 style="margin: 0; color: white;">11</h3>
        <p style="margin: 5px 0 0 0; color: white;">Database Tables</p>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown("""
    <div class="stats-box">
        <h3 style="margin: 0; color: white;">40+</h3>
        <p style="margin: 5px 0 0 0; color: white;">RBAC Rules</p>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown("""
    <div class="stats-box">
        <h3 style="margin: 0; color: white;">24/7</h3>
        <p style="margin: 5px 0 0 0; color: white;">Availability</p>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br><br>", unsafe_allow_html=True)

# Footer
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