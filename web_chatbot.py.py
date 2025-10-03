import streamlit as st
import asyncio
import httpx
import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Förlagssystem AI Assistant",
    page_icon="📊",
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
        padding: 0.5rem 2rem;
        font-weight: 600;
        border: none;
        transition: all 0.3s;
    }

    .stButton>button:hover {
        background-color: #005a8a;
        transform: translateY(-2px);
    }

    .user-message {
        background-color: #0073AE;
        color: white;
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
        max-width: 80%;
        margin-left: auto;
    }

    .assistant-message {
        background-color: #ffffff;
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
        max-width: 80%;
        border-left: 4px solid #0073AE;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
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

    /* Hide send button - Enter key submits */
    button[kind="formSubmit"] {
        display: none;
    }
</style>
""", unsafe_allow_html=True)

# Configuration
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
GATEWAY_URL = os.getenv("GATEWAY_URL")
GATEWAY_TOKEN = os.getenv("GATEWAY_TOKEN")

ROLE_PROMPTS = {
    "harold": "Executive business intelligence assistant for CEO",
    "lars": "Financial data assistant",
    "pontus": "Customer service assistant",
    "peter": "Logistics assistant",
    "linda": "Customer service assistant"
}

DATABASE_SCHEMA = """
DCPO.KHKNDHUR (Customers):
- KHKNR: Customer number
- KHFKN: Customer name
- KHTEL: Telephone
- KHFA1-4: Address lines
- KHKGÄ: Credit limit
- KHSTS: Status (1=active)

DCPO.KRKFAKTR (Invoices):
- KRFNR: Invoice number
- KRKNR: Customer number
- KRBLF: Invoice amount
- KRDAF: Invoice date (YYYYMMDD)
- KRDFF: Due date

DCPO.OHKORDHR (Orders):
- OHONR: Order number
- OHKNR: Customer number
- OHDAO: Order date (YYYYMMDD)
"""

# Session state
if 'messages' not in st.session_state:
    st.session_state.messages = []
if 'username' not in st.session_state:
    st.session_state.username = None

async def execute_query(sql: str, username: str):
    async with httpx.AsyncClient() as http_client:
        response = await http_client.post(
            f"{GATEWAY_URL}/api/execute-query",
            json={"query": sql},
            headers={
                "Authorization": f"Bearer {GATEWAY_TOKEN}",
                "X-Username": username
            },
            timeout=30.0
        )
        return response.json()

def generate_sql(question: str) -> str:
    sql_prompt = f"""Based on: "{question}"

DATABASE SCHEMA:
{DATABASE_SCHEMA}

Generate SQL using:
- Exact column names above
- Date format: YYYYMMDD numeric
- Active records: WHERE KHSTS='1'
- For name searches: Use UPPER(column) LIKE UPPER('%searchterm%')
- For exact numbers: Use = operator
- Limit: FETCH FIRST 10 ROWS ONLY
- NO semicolon

Examples:
- "customer named John" → WHERE UPPER(KHFKN) LIKE UPPER('%John%')
- "customer 330" → WHERE KHKNR = 330

Return ONLY SQL:"""

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "SQL expert. Use LIKE for text searches, = for numbers."},
            {"role": "user", "content": sql_prompt}
        ],
        temperature=0.1
    )

    sql = response.choices[0].message.content.strip()
    sql = sql.replace("```sql", "").replace("```", "").strip().rstrip(';')
    return sql

def format_results(question: str, rows: list, username: str) -> str:
    format_prompt = f"""User asked: "{question}"

Results ({len(rows)} rows):
{rows}

Present in friendly, conversational way. Format numbers with commas for thousands."""

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": ROLE_PROMPTS[username]},
            {"role": "user", "content": format_prompt}
        ]
    )
    return response.choices[0].message.content

# Sidebar
with st.sidebar:
    # Company logo
    st.image("https://www.forlagssystem.se/wp-content/uploads/2023/02/forlagssystem_logo_white.svg",
             use_container_width=True)
    st.markdown("---")

    if st.session_state.username is None:
        st.markdown("### Login")
        username = st.selectbox(
            "Select your account:",
            ["", "harold", "lars", "pontus", "peter", "linda"],
            format_func=lambda x: {
                "": "-- Select User --",
                "harold": "Harold (CEO)",
                "lars": "Lars (Finance)",
                "pontus": "Pontus (Call Center)",
                "peter": "Peter (Logistics)",
                "linda": "Linda (Customer Service)"
            }[x]
        )

        if st.button("Login", use_container_width=True):
            if username:
                st.session_state.username = username
                st.session_state.messages = []
                st.rerun()
    else:
        st.markdown(f"### Logged in as")
        st.markdown(f"**{st.session_state.username.upper()}**")

        if st.button("Logout", use_container_width=True):
            st.session_state.username = None
            st.session_state.messages = []
            st.rerun()

# Main content
if st.session_state.username is None:
    col1, col2, col3 = st.columns([1,2,1])
    with col2:
        st.image("https://www.forlagssystem.se/wp-content/uploads/2023/02/forlagssystem_logo.svg",
                 use_container_width=True)
        st.markdown("<h1 style='text-align: center;'>AI Assistant</h1>", unsafe_allow_html=True)
        st.markdown("<p style='text-align: center; color: #666;'>Select your account to get started</p>",
                   unsafe_allow_html=True)

else:
    # Header with logo
    col1, col2 = st.columns([1, 4])
    with col1:
        st.image("https://www.forlagssystem.se/wp-content/uploads/2023/02/forlagssystem_logo.svg",
                 width=150)
    with col2:
        st.markdown(f"<h1>Chat with AI Assistant</h1>", unsafe_allow_html=True)
        st.markdown(f"Hi **{st.session_state.username.upper()}**, I'm your assistant today. I can help you with STYR data.")

    st.markdown("---")

    # Display chat history
    for msg in st.session_state.messages:
        if msg["role"] == "user":
            st.markdown(f"<div class='user-message'>{msg['content']}</div>", unsafe_allow_html=True)
        else:
            st.markdown(f"<div class='assistant-message'>{msg['content']}</div>", unsafe_allow_html=True)

    # Input form - auto-submit on Enter, no send button
    with st.form(key='chat_form', clear_on_submit=True):
        user_input = st.text_input(
            "Ask me anything:",
            placeholder="e.g., Show me customer 330",
            label_visibility="collapsed"
        )
        submit = st.form_submit_button("Send")

    if submit and user_input:
        st.session_state.messages.append({"role": "user", "content": user_input})

        with st.spinner("Thinking..."):
            try:
                sql = generate_sql(user_input)

                if not sql.upper().startswith("SELECT"):
                    response = "I couldn't understand that question. Try: 'Show me customer 330' or 'List latest invoices'"
                else:
                    result = asyncio.run(execute_query(sql, st.session_state.username))

                    if result.get("success"):
                        rows = result["data"]["rows"]
                        if len(rows) == 0:
                            response = "No data found for your query."
                        else:
                            response = format_results(user_input, rows, st.session_state.username)
                    else:
                        error_msg = result.get("message", "").lower()
                        if "permission" in error_msg or "access" in error_msg or "denied" in error_msg:
                            response = "You don't have permission to access that information."
                        else:
                            response = "Something went wrong. Please rephrase your question."

                st.session_state.messages.append({"role": "assistant", "content": response})
                st.rerun()

            except Exception as e:
                response = "An error occurred. Please try again."
                st.session_state.messages.append({"role": "assistant", "content": response})
                st.rerun()