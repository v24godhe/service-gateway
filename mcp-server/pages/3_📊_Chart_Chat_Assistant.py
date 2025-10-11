import streamlit as st
from utils.text_to_sql_converter import get_env_variable, generate_sql_with_session_context
from utils.chat_chart_integration import ChatChartIntegration, integrate_with_existing_chat
from utils.query_executor import QueryExecutor
from utils.system_manager import SystemManager

OPENAI_API_KEY = get_env_variable("OPENAI_API_KEY")

# Initialize executors
query_executor = QueryExecutor()
system_manager = SystemManager()

st.set_page_config(page_title="Chart Chat Assistant", layout="wide")
st.title("📊 Chart Chat Assistant")

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

# Sidebar - User
st.sidebar.markdown("### 👤 User")
st.sidebar.success(f"👤 {st.session_state.username}")

# System selector
if 'selected_system' not in st.session_state:
    st.session_state.selected_system = "STYR"

st.sidebar.markdown("---")
st.sidebar.markdown("### 🗄️ Database System")
available_systems = system_manager.get_available_systems()
selected_system = st.sidebar.selectbox(
    "Active System",
    available_systems,
    index=available_systems.index(st.session_state.selected_system),
    key="system_selector_chart"
)
st.session_state.selected_system = selected_system

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.write(message["content"])
        if "chart" in message:
            st.plotly_chart(message["chart"], use_container_width=True)

# Chat input
if prompt := st.chat_input("Ask for a business insight or chart..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.write(prompt)
    
    # Convert to SQL using database conversation context
    sql_query = generate_sql_with_session_context(
        prompt, 
        st.session_state.username, 
        st.session_state.session_id
    )
    
    print(f"USER PROMPT: {prompt}")
    print(f"GENERATED SQL: {sql_query}")
    print(f"SYSTEM: {st.session_state.selected_system}")
    
    if not sql_query or not sql_query.lower().startswith("select"):
        with st.chat_message("assistant"):
            response = "❗ Sorry, I couldn't convert your question to a valid SQL query."
            st.error(response)
            st.session_state.messages.append({"role": "assistant", "content": response})
    else:
        # Pass SQL and system_id to chart integration
        chart_handled = integrate_with_existing_chat(
            chat_integration, 
            sql_query, 
            user_role=st.session_state.username, 
            original_message=prompt,
            system_id=st.session_state.selected_system
        )
        
        if not chart_handled:
            with st.chat_message("assistant"):
                out = f"I understand you said: '{prompt}'. Try asking for a chart, e.g., 'Show me monthly revenue.'"
                st.write(out)
            st.session_state.messages.append({"role": "assistant", "content": out})

# Show chart history
chat_integration.show_chart_history()