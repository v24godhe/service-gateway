import streamlit as st
import asyncio
import httpx
import os
from openai import OpenAI
from dotenv import load_dotenv
from datetime import datetime, timedelta

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

    button[kind="formSubmit"] {
        display: none;
    }
</style>
""", unsafe_allow_html=True)

# Configuration
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
GATEWAY_URL = os.getenv("GATEWAY_URL")
GATEWAY_TOKEN = os.getenv("GATEWAY_TOKEN")

# Get current date for queries
TODAY = datetime(2025, 9, 10)
TODAY_STR = TODAY.strftime("%Y%m%d")

# Calculate week boundaries
WEEK_START = (TODAY - timedelta(days=TODAY.weekday())).strftime("%Y%m%d")
WEEK_END = (TODAY + timedelta(days=6-TODAY.weekday())).strftime("%Y%m%d")
MONTH_START = TODAY.replace(day=1).strftime("%Y-%m-%d")
next_month = (TODAY.replace(day=28) + timedelta(days=4)).replace(day=1)
MONTH_END = (next_month - timedelta(days=1)).strftime("%Y-%m-%d")

# Enhanced Role Prompts
ROLE_PROMPTS = {
    "harold": """
You are the Executive Business Intelligence Agent for Harold, the CEO.
Your mission is to deliver clear, strategic, and data-driven insights directly answering the user’s question.
Before replying, identify exactly what Harold is asking (e.g., performance trends, KPIs, forecasts).
Present the response in a concise and structured format:
1. Direct answer or conclusion
2. Supporting data or explanation
3. Key insight or recommendation
Always stay focused on the question — no general context or unrelated commentary.
""",

    "lars": """
You are the Financial Data Agent for Lars, the Head of Finance.
Your mission is to produce accurate, relevant, and data-backed financial insights.
Before replying, determine precisely what financial aspect is being asked about (e.g., revenue, costs, margins, forecasts).
Organize every reply as follows:
1. Direct answer (figures or summary)
2. Supporting breakdown or calculation
3. Financial implication or next step
Keep the response precise, factual, and free of unnecessary information.
""",

    "pontus": """
You are the Customer Service Agent for the call center team led by Pontus.
Your mission is to clearly and politely answer each customer inquiry.
Understand exactly what the customer is asking (e.g., order status, delivery issue, return request).
Structure your response like this:
1. Direct answer to the question
2. Any relevant details (order number, tracking link, dates)
3. Next step or confirmation message
Avoid generic greetings or unnecessary explanations — stay focused on resolving the issue.
""",

    "peter": """
You are the Logistics Agent for Peter, the Head of Logistics.
Your mission is to provide precise, actionable logistics information.
Identify what the question focuses on (e.g., stock levels, shipment delays, delivery routes).
Structure your reply clearly:
1. Direct operational answer
2. Supporting data (quantities, ETA, carrier info)
3. Recommendation or next step if applicable
Always be concise, factual, and relevant — avoid general comments or unrelated data.
""",

    "linda": """
You are the Customer Service Agent for Linda, the Head of Customer Service.
Your mission is to deliver accurate, structured responses about customer service operations.
Understand what the question is about (e.g., service metrics, feedback summaries, agent performance).
Structure your answer as:
1. Direct answer or summary
2. Supporting data or analysis
3. Recommendation or improvement insight
Keep the response professional, concise, and aligned with the exact question — no unnecessary context.
"""
}



# EXPANDED Database Schema with ORDER ROWS table

DATABASE_SCHEMA = """
# FÖRLAGSSYSTEM AB - STYR DATABASE (AS400/DB2)

CURRENT DATE: {today}
WEEK: {week_start} to {week_end}
MONTH: {month_start} to {month_end}

# General Rules:
- Dates are numeric YYYYMMDD
- Active records filters: KHSTS='1', AHSTS='1', LHSTS='1'
- Always join ORKORDRR for quantities (ORKVB = Ordered, ORKVL = Delivered)
- JOIN KHKNDHUR to get customer names (KHFKN)
- JOIN AHARTHUR or AYARINFR for product names

# Tables Overview:

1. CUSTOMERS (DCPO.KHKNDHUR)
- Primary Key: KHKNR
- Essential: KHFKN (Name), KHSTS (Active), KHFA1-4 (Address), KHTEL, KHKON (Contact)
- Financial: KHKGÄ (Credit Limit), KHBLE/KHSAL (Balances)
- Relationship: KHKNB (Payer), KHLNR (Supplier)
- Dates: KHDSO, KHDSF, KHDAU, KHDAÄ

2. ORDERS HEADER (DCPO.OHKORDHR)
- Primary Key: OHONR
- Essential: OHKNR (Customer), OHOST (Status), OHDAO (Order Date), OHBLF (Invoice Amount), OHVAL (Currency)
- Delivery: OHDAL, OHDAÖ, OHDAF, OHDFF
- Logistics: OHVKT (Weight), OHKLI (#Packages), OHSLK (Carrier)
- Flags: OHMOK (VAT), OHDEL (Partial), OHANV (Created By)

3. ORDER ROWS (DCPO.ORKORDRR)
- Primary Key: ORONR + ORORN
- Foreign: ORONR → OHKORDHR.OHONR
- Essential: ORANR (Article), ORKVB (Qty Ordered), ORKVL (Qty Delivered), ORPRS (Unit Price)
- Status: OROST, ORDAL, ORDFF, ORDAR
- Warehouse: ORLAG, ORLZN

4. PRODUCTS (DCPO.AHARTHUR)
- Primary Key: AHANR
- Essential: AHBES (Name), AHSTS (Active), AHTYP (Type)
- Stock: AHLDG, AHLDV, AHLDT (Available), AHRES (Reserved)
- Pricing: AHPRS (Price), AHPRF (Purchase Price)
- Supplier: AHLNR

5. ARTICLE INFO (EGU.AYARINFR)
- Primary Key: AYANR
- Essential: AYTIT (Title), AYFÖ (Author), AYILL (Illustrator), AYBIN (Binding), AYTRÅ (Year)

6. SUPPLIERS (DCPO.LHLEVHUR)
- Primary Key: LHLNR
- Essential: LHSÖK (Search), LHSTS (Active), LHLEN (Name), LHKON (Contact)
- Address: LHFA1-4, LHTEL, LHORG

7. PURCHASE ORDERS HEADER (DCPO.IHIORDHR)
- Primary Key: IHONR
- Essential: IHLNR (Supplier), IHOST (Status), IHDAO, IHDIN, IHVAL (Currency)

8. PURCHASE ORDER ROWS (DCPO.IRIORDRR)
- Primary Key: IRONR + IRORN
- Essential: IRANR (Article), IRKVB, IRKVL, IRIPR, IROST

9. INVOICES (DCPO.KRKFAKTR)
- Primary Key: KRFNR
- Foreign: KRKNR → KHKNDHUR.KHKNR
- Essential: KRDAF (Invoice Date), KRDFF (Due Date), KRBLF (Amount), KRBLB (Outstanding)

10. PAYMENTS (DCPO.KIINBETR)
- Primary Key: KIKNR + KIFNR + KIRAD
- Essential: KIDAT (Payment Date), KIBLB (Amount), KIPTY (Type)

11. SALES STATISTICS (EGU.WSOUTSAV)
- Primary Key: WSARG + WSPE6 + WSANR + WSKNR
- Essential: WSDEBA (Qty Sold), WSDEBN (Net Amount), WSDEBM (VAT)
- Filters: WSSTS='P' (Posted), WSPE6 (Period YYYYMM)

# Critical Queries:
- Orders this week: OHDAO >= {week_start} AND OHDAO <= {week_end}
- Active customers: KHSTS='1'
- Always SUM(ORKVB) & SUM(ORKVL) for order totals
- Use LEFT JOIN for optional data, INNER JOIN for mandatory relations

""".format(
    today=TODAY_STR,
    week_start=WEEK_START,
    week_end=WEEK_END,
    month_start=MONTH_START,
    month_end=MONTH_END
)


# Enhanced SQL Generation Prompt
SQL_GENERATION_PROMPT = """
You are an expert SQL generator for Förlagssystem AB database.  
Use the following DATABASE_SCHEMA strictly.  

Guidelines:
1. Always use table aliases for clarity. Example: `OHKORDHR o`, `ORKORDRR r`.  
2. Include JOINs only when necessary. Prefer INNER JOIN for required relations, LEFT JOIN for optional.  
3. Only include active records: KHSTS='1', AHSTS='1', LHSTS='1' unless otherwise specified.  
4. Dates are numeric YYYYMMDD. Use >= and <= for ranges.  
5. Always sum quantities (ORKVB, ORKVL) when reporting totals.  
6. Always include essential fields: customer names, article names, order numbers, invoice amounts, etc.  
7. Output must be valid standard SQL (DB2 compatible).  


Rules:
- Always prioritize active customers/products/suppliers.
- Always include currency when displaying amounts (OHVAL, IHVAL).
- If the user asks for payment or invoice info, join KRKFAKTR or KIINBETR as needed.  
- If the user specifies a date range, filter accordingly.  
- Always output valid SQL with proper grouping and ordering.  

Generate only the SQL query; do not explain anything unless asked.

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

def generate_sql(question: str, username: str) -> str:
    """Generate SQL with role-specific optimizations"""
    
    # Add role context to help SQL generation
    role_context = ""
    if username == "peter":
        role_context = "LOGISTICS USER: Include quantities, item counts, and article details."
    elif username == "harold":
        role_context = "CEO USER: Focus on revenue, totals, and strategic metrics."
    elif username == "lars":
        role_context = "FINANCE USER: Include amounts, payment terms, and financial details."
    
    sql_prompt = f"""{role_context}

User question: "{question}"

DATABASE SCHEMA:
{DATABASE_SCHEMA}

Generate SQL following the rules in the system prompt. Include JOINs for comprehensive data.
Return ONLY the SQL query."""

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": SQL_GENERATION_PROMPT},
            {"role": "user", "content": sql_prompt}
        ],
        temperature=0.1
    )

    sql = response.choices[0].message.content.strip()
    sql = sql.replace("```sql", "").replace("```", "").strip().rstrip(';')
    return sql

def format_results(question: str, rows: list, username: str) -> str:
    """Format results based on role - NO unnecessary suggestions"""
    
    format_prompt = f"""User asked: "{question}"

Database results ({len(rows)} rows):
{rows}

Instructions:
1. Present the data clearly and professionally
2. Format numbers: 1,234,567 SEK for money, dates as readable text
3. Lead with the key answer or total
4. Be comprehensive but concise
5. DO NOT add suggestions, next steps, or additional analysis
6. Just present the facts directly and professionally
7. If dates are in YYYYMMDD format, convert to readable: 20251006 → October 6, 2025

Present the answer now:"""

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": ROLE_PROMPTS[username]},
            {"role": "user", "content": format_prompt}
        ],
        temperature=0.3
    )
    return response.choices[0].message.content

# Sidebar
with st.sidebar:
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
        
        # Show current date context
        st.markdown("---")
        st.markdown(f"**Today:** {TODAY.strftime('%B %d, %Y')}")
        st.markdown(f"**This Week:** {datetime.strptime(WEEK_START, '%Y%m%d').strftime('%b %d')} - {datetime.strptime(WEEK_END, '%Y%m%d').strftime('%b %d')}")

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

    # Input form
    with st.form(key='chat_form', clear_on_submit=True):
        user_input = st.text_input(
            "Ask me anything:",
            placeholder="e.g., Show me this week's sales report",
            label_visibility="collapsed"
        )
        submit = st.form_submit_button("Send")

    if submit and user_input:
        st.session_state.messages.append({"role": "user", "content": user_input})

        with st.spinner("Analyzing..."):
            try:
                # Generate SQL with role context
                sql = generate_sql(user_input, st.session_state.username)

                if not sql.upper().startswith("SELECT"):
                    response = "I can only retrieve information from the system; I can’t perform any other operations at the moment."
                else:
                    result = asyncio.run(execute_query(sql, st.session_state.username))

                    if result.get("success"):
                        rows = result["data"]["rows"]
                        if len(rows) == 0:
                            response = "No data found matching your query."
                        else:
                            response = format_results(user_input, rows, st.session_state.username)
                    else:
                        error_msg = result.get("message", "").lower()
                        if "permission" in error_msg or "access" in error_msg or "denied" in error_msg:
                            response = "You don't have permission to access that information."
                        else:
                            response = f"Query error. Please try rephrasing your question."

                st.session_state.messages.append({"role": "assistant", "content": response})
                st.rerun()

            except Exception as e:
                response = "An error occurred. Please try again."
                st.session_state.messages.append({"role": "assistant", "content": response})
                st.rerun()