"""
Analytics Chat Assistant - Dev Admin Only
Query system performance, errors, and usage statistics
"""

import streamlit as st
import asyncio
from utils.analytics_helper import generate_analytics_sql, get_todays_summary_sql
from utils.query_executor import QueryExecutor
from utils.system_admin import SystemAdmin
from datetime import datetime

st.set_page_config(
    page_title="Analytics Assistant",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize
query_executor = QueryExecutor()
system_admin = SystemAdmin()

# Company colors and styling (Förlagssystem theme)
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

    h1 {
        color: #0F2436;
        font-weight: 700;
    }

    .stTextInput>div>div>input {
        border-radius: 25px;
        border: 2px solid #0073AE;
        padding: 12px 20px;
    }
</style>
""", unsafe_allow_html=True)

# Session state
if "analytics_messages" not in st.session_state:
    st.session_state.analytics_messages = []
if "username" not in st.session_state:
    st.session_state["username"] = "amila.g"

# Authentication Check - Dev Admin Only
if st.session_state.username:
    is_dev_admin = system_admin.is_dev_admin(st.session_state.username)
    if not is_dev_admin:
        st.error("🚫 Access Denied: Dev Admin privileges required")
        st.info("This page is only accessible to development administrators.")
        st.stop()
else:
    st.error("🔐 Please login from Home page")
    if st.button("Go to Home"):
        st.switch_page("Home.py")
    st.stop()

# Sidebar
with st.sidebar:
    # Navigation
    st.markdown("### 🏠 Navigation")
    if st.button("← Back to Home", use_container_width=True):
        st.switch_page("Home.py")
    
    st.markdown("---")
    
    # Logo
    st.image("https://www.forlagssystem.se/wp-content/uploads/2023/02/forlagssystem_logo_white.svg",
             use_container_width=True)
    
    st.markdown("---")
    
    # User info
    st.markdown("### 👤 Dev Admin")
    st.success(f"👤 {st.session_state.username}")
    
    st.markdown("---")
    
    # Today's Summary Button
    st.markdown("### 📊 Quick Actions")
    if st.button("📅 Today's Summary", use_container_width=True):
        # Add summary request to messages
        st.session_state.analytics_messages.append({
            "role": "user",
            "content": "Show me today's summary"
        })
        st.rerun()
    
    st.markdown("---")
    
    # System Filter
    st.markdown("### 🗄️ Filter by System")
    system_filter = st.selectbox(
        "System",
        ["All Systems", "STYR", "JEEVES", "ASTRO"],
        key="system_filter"
    )
    
    st.markdown("---")
    
    # Clear chat
    if st.button("🗑️ Clear Chat", use_container_width=True):
        st.session_state.analytics_messages = []
        st.rerun()

# Main content
col1, col2 = st.columns([1, 4])
with col1:
    st.image("https://www.forlagssystem.se/wp-content/uploads/2023/02/forlagssystem_logo.svg",
             width=150)
with col2:
    st.markdown("<h1>Analytics Chat Assistant</h1>", unsafe_allow_html=True)
    st.markdown("Ask questions about system performance, errors, and usage statistics")

st.markdown("---")

# Display chat history
for message in st.session_state.analytics_messages:
    with st.chat_message(message["role"]):
        st.write(message["content"])
        if "data" in message:
            st.dataframe(message["data"], use_container_width=True)

# Chat input
if prompt := st.chat_input("Ask about system performance..."):
    # Add user message
    st.session_state.analytics_messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.write(prompt)
    
    # Generate response
    with st.chat_message("assistant"):
        with st.spinner("Analyzing..."):
            try:
                # Check if it's a summary request
                if "summary" in prompt.lower() and "today" in prompt.lower():
                    sql_query = get_todays_summary_sql()
                    response_text = "📊 **Today's System Performance Summary**\n\n"
                else:
                    # Generate SQL using AI
                    sql_query = generate_analytics_sql(prompt, st.session_state.username)
                    response_text = ""
                
                if not sql_query:
                    error_msg = "❌ Unable to generate analytics query. Please rephrase your question."
                    st.error(error_msg)
                    st.session_state.analytics_messages.append({
                        "role": "assistant",
                        "content": error_msg
                    })
                else:
                    # Execute query against MSSQL analytics database
                    # Note: Using STYR as system_id but targeting analytics DB
                    result = asyncio.run(query_executor.execute_query_FSIAH(
                        sql_query, 
                        st.session_state.username, 
                        "FSIAH"  # Gateway will route to correct DB
                    ))
                    
                    if result.get("success"):
                        rows = result["data"]["rows"]
                        
                        if rows and len(rows) > 0:
                            # Format response based on query type
                            if "summary" in prompt.lower():
                                # Special formatting for summary
                                response_text += f"**Total Queries:** {rows[0].get('total_queries', 0)}\n"
                                response_text += f"**Success Rate:** {rows[0].get('success_rate_pct', 0):.1f}%\n"
                                response_text += f"**Avg Execution Time:** {rows[0].get('avg_execution_time_ms', 0):.0f}ms\n"
                                response_text += f"**Active Users:** {rows[0].get('active_users', 0)}\n"
                                response_text += f"**Systems Used:** {rows[0].get('systems_used', 0)}\n\n"
                            else:
                                response_text += f"✅ Found {len(rows)} result(s)\n\n"
                            
                            st.write(response_text)
                            st.dataframe(rows, use_container_width=True)
                            
                            # Save to messages
                            st.session_state.analytics_messages.append({
                                "role": "assistant",
                                "content": response_text,
                                "data": rows
                            })
                        else:
                            no_data_msg = "📊 No data found for your query."
                            st.info(no_data_msg)
                            st.session_state.analytics_messages.append({
                                "role": "assistant",
                                "content": no_data_msg
                            })
                    else:
                        error_msg = f"❌ Query failed: {result.get('message', 'Unknown error')}"
                        st.error(error_msg)
                        st.session_state.analytics_messages.append({
                            "role": "assistant",
                            "content": error_msg
                        })
                        
            except Exception as e:
                error_msg = f"❌ Error: {str(e)}"
                st.error(error_msg)
                st.session_state.analytics_messages.append({
                    "role": "assistant",
                    "content": error_msg
                })

# Example queries
with st.expander("💡 Example Questions"):
    st.markdown("""
    **Performance Analysis:**
    - What queries failed today?
    - Show me the slowest queries this week
    - What's the average execution time by system?
    
    **Error Analysis:**
    - What are the top 5 errors today?
    - Show me all failed queries in the last 24 hours
    - Which users had the most errors this week?
    
    **Usage Statistics:**
    - How many queries were executed today?
    - Who are the most active users this week?
    - What's the query volume by system?
    
    **Cache Performance:**
    - What's the cache hit rate today?
    - Show me cache statistics for the last 7 days
    """)