import streamlit as st
from utils.chat_chart_integration import ChatChartIntegration, integrate_with_existing_chat

st.set_page_config(page_title="Chart Chat Assistant", layout="wide")
st.title("📊 Chart Chat Assistant")

# Initialize the chart chat system
chat_integration = ChatChartIntegration(fastapi_base_url="http://10.200.0.2:8080")

# Optional: You could authenticate user/role here
user_role = "CEO"

if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.write(message["content"])
        if "chart" in message:
            st.plotly_chart(message["chart"], use_container_width=True)

if prompt := st.chat_input("Ask for a business insight or chart..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.write(prompt)
    chart_handled = integrate_with_existing_chat(chat_integration, prompt, user_role=user_role)
    if not chart_handled:
        with st.chat_message("assistant"):
            out = f"I understand you said: '{prompt}'. Try asking for a chart, e.g., 'Show me monthly revenue.'"
            st.write(out)
        st.session_state.messages.append({"role": "assistant", "content": out})

chat_integration.show_chart_history()