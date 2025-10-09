import streamlit as st
from utils.text_to_sql_converter import get_env_variable, generate_sql_with_session_context, ROLE_PROMPTS
from utils.chat_chart_integration import ChatChartIntegration, integrate_with_existing_chat

OPENAI_API_KEY = get_env_variable("OPENAI_API_KEY")

st.set_page_config(page_title="Chart Chat Assistant", layout="wide")
st.title("📊 Chart Chat Assistant")

chat_integration = ChatChartIntegration(fastapi_base_url="http://10.200.0.2:8080")
user_role = "harold"

# Initialize session state for charts
if "messages" not in st.session_state:
    st.session_state.messages = []
if "session_id" not in st.session_state:
    import uuid
    st.session_state.session_id = str(uuid.uuid4())

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.write(message["content"])
        if "chart" in message:
            st.plotly_chart(message["chart"], use_container_width=True)

if prompt := st.chat_input("Ask for a business insight or chart..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.write(prompt)
    
    # Convert to SQL using database conversation context
    sql_query = generate_sql_with_session_context(prompt, user_role, st.session_state.session_id)
    print("USER PROMPT:", prompt)
    print("GENERATED SQL:", sql_query)
    
    if not sql_query or not sql_query.lower().startswith("select"):
        st.error("❗ Sorry, I couldn't convert your question to a valid SQL query.")
    else:
        # Pass SQL and original message to chart integration
        chart_handled = integrate_with_existing_chat(
            chat_integration, 
            sql_query, 
            user_role=user_role, 
            original_message=prompt
        )
        if not chart_handled:
            with st.chat_message("assistant"):
                out = f"I understand you said: '{prompt}'. Try asking for a chart, e.g., 'Show me monthly revenue.'"
                st.write(out)
            st.session_state.messages.append({"role": "assistant", "content": out})

chat_integration.show_chart_history()
