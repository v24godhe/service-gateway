import streamlit as st
from utils.text_to_sql_converter import get_env_variable, generate_sql_with_session_context
from utils.chat_chart_integration import ChatChartIntegration, integrate_with_existing_chat
from utils.query_executor import QueryExecutor
from utils.system_manager import SystemManager
import streamlit as st
from utils.theme import THEMES

import asyncio
from utils.chart_intelligence import analyze_chart_intent

OPENAI_API_KEY = get_env_variable("OPENAI_API_KEY")

# Initialize executors
query_executor = QueryExecutor()
system_manager = SystemManager()

st.set_page_config(page_title="Chart Chat Assistant", layout="wide")

# Initialize chat_integration with query_executor
chat_integration = ChatChartIntegration(
    fastapi_base_url="http://10.200.0.2:8080",
    query_executor=query_executor
)

# Session state
if "messages" not in st.session_state:
    st.session_state.messages = []
if "session_id" not in st.session_state:
    import uuid
    st.session_state.session_id = str(uuid.uuid4())
if "username" not in st.session_state:
    st.session_state.username = "harold"

# Sidebar
with st.sidebar:
    if st.button("💬 Chat Assistant", use_container_width=True):
        st.switch_page("pages/1_💬_Chat_Assistant.py")
    if st.button("📊 Chart Assistant", use_container_width=True, disabled=True):
        pass  # Current page
    
    st.markdown("---")
    
    # User Info
    st.success(f"👤 {st.session_state.username}")

    # System selector
    if 'selected_system' not in st.session_state:
        st.session_state.selected_system = "STYR"

    st.markdown("---")
    available_systems = system_manager.get_available_systems()
    selected_system = st.selectbox(
        "Active System",
        available_systems,
        index=available_systems.index(st.session_state.selected_system),
        key="system_selector_chart"
    )
    st.session_state.selected_system = selected_system

    # Theme selector
    theme_choice = st.sidebar.selectbox("🎨 Theme", list(THEMES.keys()))
    st.markdown(THEMES[theme_choice], unsafe_allow_html=True)


if st.session_state.username is None:
    col1, col2 = st.columns([1,4])
    with col1:
        st.image("https://www.forlagssystem.se/wp-content/uploads/2023/02/forlagssystem_logo.svg",
                 width=150)
    with col2:
        st.markdown("<h3 style='text-align: center;'>Chart Assistant</h3>", unsafe_allow_html=True)
        st.markdown("<p style='text-align: center; color: #666;'>Select your account to get started</p>",
                   unsafe_allow_html=True)
else:
    col1, col2 = st.columns([1, 4])
    with col1:
        st.image("https://www.forlagssystem.se/wp-content/uploads/2023/02/forlagssystem_logo.svg",
                 width=150)
    with col2:
        st.markdown(
            f"<h3>Hi <b>{st.session_state.username.upper()}</b>, I'm your chart assistant today. I can help you?.</h3>",
            unsafe_allow_html=True
        )

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.write(message["content"])
        if "chart" in message:
            st.plotly_chart(message["chart"], use_container_width=True)

# Chat input form
with st.form(key='chat_form', clear_on_submit=True):
    user_input = st.text_input(
        "Ask me anything:",
        placeholder="e.g., Ask for a business insight or chart...",
        label_visibility="collapsed"
    )
    submitted = st.form_submit_button("Send")

# Only run the logic after form submission
if submitted and user_input:
    with st.chat_message("user"):
        st.write(user_input)

    with st.spinner("Analyzing chart requirements..."):
        chart_hints = asyncio.run(analyze_chart_intent(user_input, st.session_state.selected_system))

    # Enhance prompt for SQL generation
    enhanced_prompt = f"""{user_input}

    [CHART SQL STRATEGY]:
    - Chart Type: {chart_hints.get('chart_type', 'bar')}
    - Needs Grouping: {chart_hints.get('needs_grouping', False)}
    - Group By Type: {chart_hints.get('group_by_type', 'N/A')} (find appropriate date/category column in schema)
    - Aggregation Function: {chart_hints.get('aggregation_function', 'COUNT')}
    - Aggregation Field Type: {chart_hints.get('aggregation_field_hint', 'count')} (use amount/quantity columns from schema)
    - Date Filter: {chart_hints.get('date_filter_needed', False)} - {chart_hints.get('date_range_hint', 'N/A')}
    - Order: {chart_hints.get('order_direction', 'ASC')} with LIMIT {chart_hints.get('limit', 100)}

    IMPORTANT: Use the correct column names from the DATABASE_SCHEMA, not the hints above.
    """

    # Generate SQL
    sql_query = generate_sql_with_session_context(
        enhanced_prompt,
        st.session_state.username,
        st.session_state.session_id
    )

    print(f"USER PROMPT: {user_input}")
    print(f"GENERATED SQL: {sql_query}")
    print(f"SYSTEM: {st.session_state.selected_system}")

    # Handle invalid SQL case
    if not sql_query or not sql_query.lower().startswith("select"):
        with st.chat_message("assistant"):
            response = "❗ Sorry, I couldn't convert your question to a valid SQL query."
            st.error(response)
        st.session_state.messages.append({"role": "assistant", "content": response})

    else:
        # Integrate SQL with chat/chart system
        chart_handled = integrate_with_existing_chat(
            chat_integration,
            sql_query,
            user_role=st.session_state.username,
            original_message=user_input,
            system_id=st.session_state.selected_system
        )

        if not chart_handled:
            with st.chat_message("assistant"):
                out = f"I understand you said: '{user_input}'. Try asking for a chart, e.g., 'Show me monthly revenue.'"
                st.write(out)
            st.session_state.messages.append({"role": "assistant", "content": out})

# Show chart history
chat_integration.show_chart_history()