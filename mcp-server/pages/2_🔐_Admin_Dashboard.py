"""
Admin Dashboard Page
Location: ~/forlagssystem-mcp/mcp-server/pages/2_🔐_Admin_Dashboard.py
"""

import streamlit as st
import httpx
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Configuration
GATEWAY_URL = os.getenv("GATEWAY_URL", "http://10.200.0.2:8080")
st.set_page_config(
    page_title="Admin Dashboard",
    page_icon="🔐",
    layout="wide"
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
        padding: 0.5rem 2rem;
        font-weight: 600;
        border: none;
        transition: all 0.3s;
    }

    .stButton>button:hover {
        background-color: #005a8a;
        transform: translateY(-2px);
    }

    .approve-button>button {
        background-color: #28a745;
    }

    .deny-button>button {
        background-color: #dc3545;
    }

    h1, h2, h3 {
        color: #0F2436;
        font-weight: 700;
    }

    .metric-card {
        background: white;
        padding: 20px;
        border-radius: 10px;
        border-left: 4px solid #0073AE;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }

    .request-card {
        background: white;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-left: 4px solid #ffc107;
    }

    .approved-card {
        border-left: 4px solid #28a745;
    }

    .denied-card {
        border-left: 4px solid #dc3545;
    }

    .stTextInput>div>div>input {
        border-radius: 8px;
        border: 2px solid #0073AE;
    }
</style>
""", unsafe_allow_html=True)

# Session state initialization
if 'admin_username' not in st.session_state:
    st.session_state.admin_username = None
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False

# Helper functions
def check_admin_status(username: str) -> bool:
    """Check if user is super admin"""
    try:
        response = httpx.get(f"{GATEWAY_URL}/api/admin/check/{username}", timeout=10)
        if response.status_code == 200:
            return response.json().get("is_admin", False)
    except Exception as e:
        st.error(f"Error checking admin status: {e}")
    return False

def get_pending_requests():
    """Get all pending permission requests"""
    try:
        response = httpx.get(
            f"{GATEWAY_URL}/api/permission-requests/pending",
            headers={"X-Admin-Username": st.session_state.admin_username},
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            return data.get("requests", [])
    except Exception as e:
        st.error(f"Error fetching requests: {e}")
    return []

def get_all_requests(status=None):
    """Get all permission requests with optional filter"""
    try:
        url = f"{GATEWAY_URL}/api/permission-requests/all"
        if status:
            url += f"?status={status}"
        
        response = httpx.get(
            url,
            headers={"X-Admin-Username": st.session_state.admin_username},
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            return data.get("requests", [])
    except Exception as e:
        st.error(f"Error fetching requests: {e}")
    return []

def approve_request(request_id: int, notes: str, temporary: bool = False, days: int = 30):
    """Approve a permission request"""
    try:
        response = httpx.post(
            f"{GATEWAY_URL}/api/permission-requests/{request_id}/approve",
            json={
                "review_notes": notes,
                "temporary": temporary,
                "days_valid": days
            },
            headers={"X-Admin-Username": st.session_state.admin_username},
            timeout=10
        )
        return response.status_code == 200
    except Exception as e:
        st.error(f"Error approving request: {e}")
    return False

def deny_request(request_id: int, notes: str):
    """Deny a permission request"""
    try:
        response = httpx.post(
            f"{GATEWAY_URL}/api/permission-requests/{request_id}/deny",
            json={"review_notes": notes},
            headers={"X-Admin-Username": st.session_state.admin_username},
            timeout=10
        )
        return response.status_code == 200
    except Exception as e:
        st.error(f"Error denying request: {e}")
    return False

def get_permission_stats():
    """Get permission statistics"""
    try:
        response = httpx.get(
            f"{GATEWAY_URL}/api/permission-stats",
            headers={"X-Admin-Username": st.session_state.admin_username},
            timeout=10
        )
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        st.error(f"Error fetching stats: {e}")
    return {}

def get_rbac_rules():
    """Get all RBAC rules"""
    try:
        response = httpx.get(
            f"{GATEWAY_URL}/api/rbac/rules",
            headers={"X-Admin-Username": st.session_state.admin_username},
            timeout=10
        )
        if response.status_code == 200:
            return response.json().get("roles", {})
    except Exception as e:
        st.error(f"Error fetching RBAC rules: {e}")
    return {}

# Sidebar
with st.sidebar:
    # Home button
    if st.button("🏠 Back to Home", use_container_width=True):
        st.switch_page("Home.py")
    
    st.markdown("---")
    
    st.image("https://www.forlagssystem.se/wp-content/uploads/2023/02/forlagssystem_logo_white.svg",
             use_container_width=True)
    st.markdown("---")
    
    if not st.session_state.authenticated:
        st.markdown("### 🔐 Super Admin Login")
        username = st.text_input("Username", key="login_username")
        password = st.text_input("Password", type="password", key="login_password")
        
        if st.button("Login", use_container_width=True):
            if username:
                if check_admin_status(username):
                    st.session_state.admin_username = username
                    st.session_state.authenticated = True
                    st.rerun()
                else:
                    st.error("Not authorized as super admin")
    else:
        st.markdown(f"### Logged in as")
        st.markdown(f"**{st.session_state.admin_username.upper()}**")
        st.markdown("🔐 Super Admin")
        
        if st.button("Logout", use_container_width=True):
            st.session_state.admin_username = None
            st.session_state.authenticated = False
            st.rerun()
        
        st.markdown("---")
        st.markdown("### Navigation")
        page = st.radio(
            "Select Page",
            ["📋 Pending Requests", "📊 Dashboard", "🔑 RBAC Management", "📜 Request History"],
            label_visibility="collapsed"
        )

def memory_management_section():
    """Admin section for managing conversation memories"""
    st.markdown("## 🧠 Conversation Memory Management")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Memory Statistics")
        st.info("**Phase 2C Implementation:**")
        st.write("- Conversation memory active")
        st.write("- Context-aware SQL generation")
        st.write("- Follow-up question handling")
        
        if st.button("📊 View Memory Usage Stats"):
            st.info("Memory statistics feature - Implementation in progress")
    
    with col2:
        st.subheader("Memory Operations")
        if st.button("🗑️ Clear All User Memories", type="secondary"):
            if st.checkbox("⚠️ Confirm: Clear all conversation memories"):
                st.success("All conversation memories would be cleared (Feature in development)")
        
        if st.button("💾 Export Memory Data"):
            st.info("Export functionality - Phase 2C implementation")
        
        if st.button("🔄 Reset Memory Services"):
            st.info("Memory service reset - Phase 2C implementation")

# Main content
if not st.session_state.authenticated:
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.image("https://www.forlagssystem.se/wp-content/uploads/2023/02/forlagssystem_logo.svg",
                 use_container_width=True)
        st.markdown("<h1 style='text-align: center;'>Permission Management</h1>", unsafe_allow_html=True)
        st.markdown("<p style='text-align: center; color: #666;'>Super Admin access required</p>",
                   unsafe_allow_html=True)
else:
    # Header
    col1, col2 = st.columns([1, 4])
    with col1:
        st.image("https://www.forlagssystem.se/wp-content/uploads/2023/02/forlagssystem_logo.svg",
                 width=150)
    with col2:
        st.markdown("<h1>Permission Management Dashboard</h1>", unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Page routing
    if 'page' not in locals():
        page = "📋 Pending Requests"


    # Find this line (around line 265):
    page = st.radio(
        "Select Page",
        ["📋 Pending Requests", "📊 Dashboard", "🔑 RBAC Management", "📜 Request History"],
        label_visibility="collapsed"
    )

    # REPLACE IT WITH:
    page = st.radio(
        "Select Page",
        ["📋 Pending Requests", "📊 Dashboard", "🔑 RBAC Management", "📜 Request History", "🧠 Memory Management"],
        label_visibility="collapsed"
    )

    
    # ================================================================
    # PAGE 1: Pending Requests
    # ================================================================
    if page == "📋 Pending Requests":
        st.markdown("## 📋 Pending Permission Requests")
        
        pending_requests = get_pending_requests()
        
        if not pending_requests:
            st.info("✅ No pending requests at the moment")
        else:
            st.warning(f"⚠️ {len(pending_requests)} pending request(s) need review")
            
            for req in pending_requests:
                with st.container():
                    st.markdown(f"""
                    <div class="request-card">
                        <h3>Request #{req['request_id']}</h3>
                        <p><strong>User:</strong> {req['user_id']} ({req['user_role']})</p>
                        <p><strong>Requested Table:</strong> <code>{req['requested_table']}</code></p>
                        <p><strong>Question:</strong> {req['original_question']}</p>
                        <p><strong>Priority:</strong> {req['priority']}</p>
                        <p><strong>Requested:</strong> {req['requested_at']}</p>
                        <p><strong>Pending for:</strong> {req['hours_pending']} hours</p>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        with st.expander("✅ Approve"):
                            approval_notes = st.text_area(
                                "Approval Notes",
                                key=f"approve_notes_{req['request_id']}"
                            )
                            temporary = st.checkbox(
                                "Temporary Access",
                                key=f"temp_{req['request_id']}"
                            )
                            if temporary:
                                days = st.number_input(
                                    "Days Valid",
                                    min_value=1,
                                    max_value=365,
                                    value=30,
                                    key=f"days_{req['request_id']}"
                                )
                            else:
                                days = 30
                            
                            if st.button("✅ Approve Request", key=f"approve_btn_{req['request_id']}", type="primary"):
                                if approve_request(req['request_id'], approval_notes, temporary, days):
                                    st.success(f"Request #{req['request_id']} approved!")
                                    st.rerun()
                                else:
                                    st.error("Failed to approve request")
                    
                    with col2:
                        with st.expander("❌ Deny"):
                            denial_notes = st.text_area(
                                "Denial Reason (Required)",
                                key=f"deny_notes_{req['request_id']}"
                            )
                            
                            if st.button("❌ Deny Request", key=f"deny_btn_{req['request_id']}"):
                                if denial_notes:
                                    if deny_request(req['request_id'], denial_notes):
                                        st.success(f"Request #{req['request_id']} denied")
                                        st.rerun()
                                    else:
                                        st.error("Failed to deny request")
                                else:
                                    st.error("Denial reason required")
                    
                    st.markdown("---")
    
    # ================================================================
    # PAGE 2: Dashboard
    # ================================================================
    elif page == "📊 Dashboard":
        st.markdown("## 📊 Permission Management Overview")
        
        stats = get_permission_stats()
        
        if stats:
            # Metrics row
            col1, col2, col3, col4 = st.columns(4)
            
            status_counts = stats.get("status_counts", {})
            
            with col1:
                st.metric("Pending", status_counts.get("PENDING", 0))
            with col2:
                st.metric("Approved", status_counts.get("APPROVED", 0))
            with col3:
                st.metric("Denied", status_counts.get("DENIED", 0))
            with col4:
                st.metric("Total Requests", stats.get("total_requests", 0))
            
            st.markdown("---")
            
            # Recent activity
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 📜 Recent Activity")
                recent = stats.get("recent_activity", [])
                if recent:
                    for activity in recent[:10]:
                        st.markdown(f"""
                        - **{activity['action']}** by {activity['admin']}  
                          *{activity['timestamp']}*
                        """)
                else:
                    st.info("No recent activity")
            
            with col2:
                st.markdown("### 👥 Pending by User")
                pending_users = stats.get("pending_by_user", {})
                if pending_users:
                    for user, count in pending_users.items():
                        st.markdown(f"- **{user}**: {count} pending request(s)")
                else:
                    st.info("No pending requests")
    
    # ================================================================
    # PAGE 3: RBAC Management
    # ================================================================
    elif page == "🔑 RBAC Management":
        st.markdown("## 🔑 Role-Based Access Control Rules")
        
        rbac_rules = get_rbac_rules()
        
        if rbac_rules:
            for role, data in rbac_rules.items():
                with st.expander(f"👤 Role: **{role.upper()}** ({len(data['tables'])} tables)"):
                    st.markdown("### Allowed Tables")
                    
                    for table in data['tables']:
                        restrictions = data['restrictions'].get(table, {})
                        
                        if restrictions:
                            allowed = restrictions.get('allowed_columns')
                            blocked = restrictions.get('blocked_columns')
                            notes = restrictions.get('notes')
                            
                            if blocked:
                                st.markdown(f"- 🔒 **{table}** - Blocked columns: `{', '.join(blocked)}`")
                            elif allowed:
                                st.markdown(f"- 🔓 **{table}** - Only: `{', '.join(allowed)}`")
                            
                            if notes:
                                st.caption(f"   _{notes}_")
                        else:
                            st.markdown(f"- ✅ **{table}** - Full access")
        else:
            st.warning("No RBAC rules found")
    
    # ================================================================
    # PAGE 4: Request History
    # ================================================================
    elif page == "📜 Request History":
        st.markdown("## 📜 Permission Request History")
        
        status_filter = st.selectbox(
            "Filter by Status",
            ["All", "PENDING", "APPROVED", "DENIED"],
            key="history_filter"
        )
        
        if status_filter == "All":
            requests = get_all_requests()
        else:
            requests = get_all_requests(status=status_filter)
        
        if requests:
            st.markdown(f"**Showing {len(requests)} requests**")
            
            for req in requests:
                card_class = "request-card"
                if req['status'] == "APPROVED":
                    card_class += " approved-card"
                elif req['status'] == "DENIED":
                    card_class += " denied-card"
                
                with st.container():
                    st.markdown(f"""
                    <div class="{card_class}">
                        <h4>Request #{req['request_id']} - {req['status']}</h4>
                        <p><strong>User:</strong> {req['user_id']} ({req['user_role']})</p>
                        <p><strong>Table:</strong> <code>{req['requested_table']}</code></p>
                        <p><strong>Question:</strong> {req['original_question']}</p>
                        <p><strong>Requested:</strong> {req['requested_at']}</p>
                        {f"<p><strong>Reviewed by:</strong> {req['reviewed_by']} on {req['reviewed_at']}</p>" if req['reviewed_by'] else ""}
                        {f"<p><strong>Notes:</strong> {req['review_notes']}</p>" if req['review_notes'] else ""}
                    </div>
                    """, unsafe_allow_html=True)
                    st.markdown("---")
        else:
            st.info("No requests found")


    # ================================================================
    # PAGE 5: Memory Management
    # ================================================================
    elif page == "🧠 Memory Management":
        memory_management_section()