"""
Analytics Chat Assistant - Dev Admin Only
Query system performance, errors, and usage statistics
"""

import streamlit as st
import asyncio
from utils.analytics_helper import generate_analytics_sql, get_todays_summary_sql, generate_analytics_insights
from utils.query_executor import QueryExecutor
from utils.system_admin import SystemAdmin
from datetime import datetime

import streamlit as st
from utils.theme import THEMES

st.set_page_config(
    page_title="Analytics Assistant",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize
query_executor = QueryExecutor()
system_admin = SystemAdmin()

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
    st.success(f"👤 {st.session_state.username}")
    st.markdown("---")
    
    theme_choice = st.sidebar.selectbox("🎨 Theme", list(THEMES.keys()))
    st.markdown(THEMES[theme_choice], unsafe_allow_html=True)
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

# Example queries
with st.expander("💡 Example Questions"):
```)  
with this updated version:

---

```python
# Input form (same look as main Chat Assistant)
with st.form(key="analytics_chat_form", clear_on_submit=True):
    user_input = st.text_input(
        "Ask about system performance:",
        placeholder="e.g., Show me the top 5 errors today",
        label_visibility="collapsed"
    )
    submit = st.form_submit_button("Send")

if submit and user_input:
    st.session_state.analytics_messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.write(user_input)

    with st.chat_message("assistant"):
        with st.spinner("Analyzing..."):
            try:
                # Detect "today's summary" intent
                if "summary" in user_input.lower() and "today" in user_input.lower():
                    sql_query = get_todays_summary_sql()
                    response_text = "📊 **Today's System Performance Summary**\n\n"
                else:
                    sql_query = generate_analytics_sql(user_input, st.session_state.username)
                    response_text = ""

                if not sql_query:
                    error_msg = "❌ Unable to generate analytics query. Please rephrase your question."
                    st.error(error_msg)
                    st.session_state.analytics_messages.append({
                        "role": "assistant",
                        "content": error_msg
                    })
                else:
                    result = asyncio.run(query_executor.execute_query_FSIAH(
                        sql_query,
                        st.session_state.username,
                        "FSIAH"
                    ))

                    if result.get("success"):
                        rows = result.get("data", [])

                        if rows and len(rows) > 0:
                            with st.spinner("Analyzing results with AI..."):
                                ai_insights = generate_analytics_insights(user_input, rows, st.session_state.username)

                            st.markdown(ai_insights)
                            st.dataframe(rows, use_container_width=True, height=400)

                            st.session_state.analytics_messages.append({
                                "role": "assistant",
                                "content": ai_insights,
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

                st.rerun()

            except Exception as e:
                error_msg = f"❌ Error: {str(e)}"
                st.error(error_msg)
                st.session_state.analytics_messages.append({
                    "role": "assistant",
                    "content": error_msg
                })
                st.rerun()
