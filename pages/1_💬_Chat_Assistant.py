import streamlit as st
import asyncio
import httpx
import os
from openai import OpenAI
from dotenv import load_dotenv
from datetime import datetime, timedelta
from langchain_community.chat_message_histories import StreamlitChatMessageHistory
from langchain.memory import ConversationBufferMemory
from langchain.schema import HumanMessage, AIMessage
import json
import os
import uuid
from datetime import datetime

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

# Database memory services via FastAPI
class DatabaseMemoryClient:
    """Client for database memory operations via FastAPI"""
    
    def __init__(self):
        self.gateway_url = GATEWAY_URL
        self.gateway_token = GATEWAY_TOKEN
        
    async def create_session(self, session_id: str, user_id: str, metadata: dict = None):
        """Create session via API"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.gateway_url}/api/conversation/create-session",
                    json={
                        "session_id": session_id,
                        "user_id": user_id,
                        "metadata": metadata or {}
                    },
                    headers={"Authorization": f"Bearer {self.gateway_token}"},
                    timeout=10.0
                )
                result = response.json()
                return result.get("success", False)
        except Exception as e:
            st.sidebar.error(f"Create session API error: {str(e)}")
            return False
            
    async def save_message(self, session_id: str, message_type: str, message_content: str, message_metadata: str = None):
        """Save message via API"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.gateway_url}/api/conversation/save-message",
                    json={
                        "session_id": session_id,
                        "message_type": message_type,
                        "message_content": message_content,
                        "message_metadata": message_metadata
                    },
                    headers={"Authorization": f"Bearer {self.gateway_token}"},
                    timeout=10.0
                )
                result = response.json()
                return result.get("success", False)
        except Exception as e:
            st.sidebar.error(f"Save message API error: {str(e)}")
            return False
            
    async def get_session_messages(self, session_id: str):
        """Get session messages via API"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.gateway_url}/api/conversation/get-messages/{session_id}",
                    headers={"Authorization": f"Bearer {self.gateway_token}"},
                    timeout=10.0
                )
                result = response.json()
                return result.get("messages", [])
        except Exception as e:
            st.sidebar.error(f"Get messages API error: {str(e)}")
            return []
            
    async def update_conversation_context(self, session_id: str, last_query: str, last_sql: str = None, 
                                        last_tables_used: str = None, result_count: int = 0):
        """Update conversation context via API"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.gateway_url}/api/conversation/update-context",
                    json={
                        "session_id": session_id,
                        "last_query": last_query,
                        "last_sql": last_sql,
                        "last_tables_used": last_tables_used,
                        "result_count": result_count
                    },
                    headers={"Authorization": f"Bearer {self.gateway_token}"},
                    timeout=10.0
                )
                result = response.json()
                return result.get("success", False)
        except Exception as e:
            st.sidebar.error(f"Update context API error: {str(e)}")
            return False
            
    async def clear_session_messages(self, session_id: str):
        """Clear session messages via API"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.delete(
                    f"{self.gateway_url}/api/conversation/clear-session/{session_id}",
                    headers={"Authorization": f"Bearer {self.gateway_token}"},
                    timeout=10.0
                )
                result = response.json()
                return result.get("success", False)
        except Exception as e:
            st.sidebar.error(f"Clear session API error: {str(e)}")
            return False

# Initialize API client
@st.cache_resource
def get_database_memory_client():
    """Initialize database memory API client"""
    return DatabaseMemoryClient()

db_memory_service = get_database_memory_client()
conversation_memory_service = None  # Not needed anymore


# Get current date for queries
TODAY = datetime(2025, 9, 10)
TODAY_STR = TODAY.strftime("%Y%m%d")

# Calculate week boundaries
WEEK_START = (TODAY - timedelta(days=TODAY.weekday())).strftime("%Y%m%d")
WEEK_END = (TODAY + timedelta(days=6-TODAY.weekday())).strftime("%Y%m%d")
MONTH_START = TODAY.replace(day=1).strftime("%Y-%m-%d")
next_month = (TODAY.replace(day=28) + timedelta(days=4)).replace(day=1)
MONTH_END = (next_month - timedelta(days=1)).strftime("%Y-%m-%d")

# Last month calculation
last_month_end = TODAY.replace(day=1) - timedelta(days=1)
LAST_MONTH_END = last_month_end.strftime("%Y%m%d")
LAST_MONTH_START = last_month_end.replace(day=1).strftime("%Y%m%d")

# Year start
YEAR_START = TODAY.replace(month=1, day=1).strftime("%Y%m%d")


def initialize_conversation_memory():
    """Initialize conversation memory for the session"""
    if "conversation_memory" not in st.session_state:
        st.session_state.conversation_memory = ConversationBufferMemory(
            chat_memory=StreamlitChatMessageHistory(key="chat_messages"),
            return_messages=True,
            memory_key="chat_history",
            input_key="input",
            output_key="output"
        )
    return st.session_state.conversation_memory

def initialize_chat_session():
    """Initialize or restore chat session with database integration"""
    
    # Generate session ID if not exists
    if 'session_id' not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())
        st.session_state.user_id = st.session_state.username if st.session_state.username else "default_user"
    
    # Skip database operations if service is not available
    if not db_memory_service:
        st.warning("⚠️ Database memory service not available - using session memory only")
        return
    
    # Initialize database session
    try:
        # Create session in database
        session_created = asyncio.run(db_memory_service.create_session(
            session_id=st.session_state.session_id,
            user_id=st.session_state.user_id,
            metadata={"started_at": datetime.now().isoformat()}
        ))
        
        if session_created:
            st.sidebar.success("✅ Database session initialized")
        
        # Load existing conversation history from database
        messages = asyncio.run(db_memory_service.get_session_messages(st.session_state.session_id))
        
        # Convert database messages to Streamlit format
        if messages and len(messages) > 0:
            st.session_state.messages = []
            for msg in messages:
                st.session_state.messages.append({
                    "role": msg['message_type'],
                    "content": msg['message_content'],
                    "timestamp": msg.get('timestamp', '')
                })
            st.sidebar.info(f"📚 Loaded {len(messages)} messages from database")
    
    except Exception as e:
        st.sidebar.error(f"❌ Database initialization error: {str(e)}")
        # Fall back to session-only memory
        if 'messages' not in st.session_state:
            st.session_state.messages = []

def save_message_to_database(role: str, content: str, metadata: dict = None):
    """Save message to database with error handling"""
    
    # Skip if database service not available
    if not db_memory_service:
        return False
        
    # Skip if no session_id
    if 'session_id' not in st.session_state:
        return False
    
    try:
        success = asyncio.run(db_memory_service.save_message(
            session_id=st.session_state.session_id,
            message_type=role,
            message_content=content,
            message_metadata=json.dumps(metadata) if metadata else None
        ))
        
        if not success:
            st.sidebar.error("⚠️ Failed to save message to database")
            return False
        
        # Update session activity
        db_memory_service.update_session_activity(st.session_state.session_id)
        return True
        
    except Exception as e:
        st.sidebar.error(f"❌ Database save error: {str(e)}")
        return False

def update_conversation_context(user_question: str, generated_sql: str, tables_used: str = "", result_count: int = 0):
    """Update conversation context in database"""
    
    # Skip if database service not available
    if not db_memory_service or 'session_id' not in st.session_state:
        return False
    
    try:
        success = asyncio.run(db_memory_service.update_conversation_context(
            session_id=st.session_state.session_id,
            last_query=user_question,
            last_sql=generated_sql,
            last_tables_used=tables_used,
            result_count=result_count
        ))
        return success
    except Exception as e:
        st.sidebar.error(f"⚠️ Context update error: {str(e)}")
        return False


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

    "lars": '''
You are the Financial Data Agent for Lars, the Head of Finance.
Your mission is to deliver answers that are **always directly related to the user's question**.

- Paraphrase the user’s intent in your opening line to confirm understanding.
- If the user asks about financial aspects (e.g., revenue, costs, margins, forecasts), provide accurate, relevant financial insights.
- If the question is about another area (e.g., book details, logistics), do not substitute with financial information.
- If the question lacks details, ask the user about any missing information. If the question is ambiguous, state this and request clarification.
- Format all answers with clear headings, bullet points, or tables for readability.
- Always maintain a professional, privacy-conscious, and polite tone.
- Conclude by inviting the user to clarify or request additional detail if desired.

Structure every reply as follows (include each step only if relevant):
1. **Direct answer to the exact question asked**—only in the scope of the user's query.
2. If relevant, include supporting breakdown or calculation—strictly aligned with the specific request.
3. If applicable, offer financial implication or a next financial step—only when requested or essential.

**Never introduce unrelated financial information.**
If the question is outside your expertise, state that clearly and suggest consulting the relevant agent.

Keep the response precise, factual, and free of unnecessary data.
'''
,

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

## Date Context (YYYYMMDD format)
CURRENT DATE: {today}
THIS WEEK: {week_start} to {week_end}
THIS MONTH: {month_start} to {month_end}
LAST MONTH: {last_month_start} to {last_month_end}
YEAR START: {year_start}

Date Calculations:
- Yesterday: {today} - 1
- Rolling 30 days: >= ({today} - 30)
- Year-to-date: >= {year_start}

---

## General Rules

### Data Formats
- All dates are numeric YYYYMMDD (e.g., 20251007)
- Currency codes: 3-character ISO codes ('SEK', 'EUR', 'USD')
- Status codes: Single-digit strings ('1', '2', '3')
- Active record filters: KHSTS='1', AHSTS='1', LHSTS='1'

### Query Best Practices
- Never use SELECT * - always specify required columns
- Use INNER JOIN for mandatory relationships, LEFT JOIN only when nulls are expected
- Filter inactive records in JOIN conditions, not WHERE clause
- Always include GROUP BY when using aggregate functions with non-aggregated columns
- Add FETCH FIRST n ROWS ONLY for large result sets
- Use indexed columns in WHERE clauses (KHKNR, OHONR, AHANR, OHDAO)

### Search Operations
- Customer/Supplier name searches: UPPER(column) LIKE '%' || UPPER(TRIM(search_term)) || '%'
- Remove leading/trailing spaces from search inputs
- Search columns: KHFKN (customer name), LHLEN (supplier name)

### Order Quantity Rules
- Join ORKORDRR for quantities based on question context
- ORKVB = Quantity Ordered
- ORKVL = Quantity Delivered
- Always use SUM(ORKVB) and SUM(ORKVL) for totals with appropriate GROUP BY

---

## Key Table Relationships

KHKNDHUR.KHKNR → OHKORDHR.OHKNR (Customer → Orders)
OHKORDHR.OHONR → ORKORDRR.ORONR (Order Header → Order Lines)
ORKORDRR.ORANR → AHARTHUR.AHANR (Order Lines → Articles)
AHARTHUR.AHANR → AYARINFR.AYANR (Articles → Article Info)
AHARTHUR.AHLNR → LHLEVHUR.LHLNR (Articles → Suppliers)
LHLEVHUR.LHLNR → IHIORDHR.IHLNR (Suppliers → Purchase Orders)
IHIORDHR.IHONR → IRIORDRR.IRONR (Purchase Order Header → Purchase Order Lines)
KHKNDHUR.KHKNR → KRKFAKTR.KRKNR (Customers → Invoices)
KRKFAKTR.KRFNR → KIINBETR.KIFNR (Invoices → Payments)

---

## Table Definitions

### 1. KHKNDHUR (Customers)
Schema: DCPO.KHKNDHUR
Primary Key: KHKNR (INTEGER)
Status Filter: KHSTS='1' for active customers only

Core Fields:
- KHKNR: Customer Number [PRIMARY KEY]
- KHFKN: Customer Name [VARCHAR, REQUIRED]
- KHSTS: Status ('1'=Active, '0'=Inactive)
- KHKNB: Payer Number [FK: KHKNDHUR.KHKNR]
- KHLNR: Supplier Number [FK: LHLEVHUR.LHLNR]
- KHKON: Contact Person

Address Fields:
- KHFA1: Address Line 1
- KHFA2: Address Line 2
- KHFA3: Address Line 3
- KHFA4: Address Line 4
- KHTEL: Telephone

Financial Fields:
- KHKGÄ: Credit Limit (DECIMAL)
- KHBLE: Balance (DECIMAL)
- KHSAL: Outstanding Balance (DECIMAL)

Date Fields:
- KHDSO: Search Date
- KHDSF: From Date
- KHDAU: Order Date
- KHDAÄ: Modified Date

---

### 2. OHKORDHR (Order Header)
Schema: DCPO.OHKORDHR
Primary Key: OHONR (INTEGER)
Foreign Key: OHKNR → KHKNDHUR.KHKNR

Core Fields:
- OHONR: Order Number [PRIMARY KEY]
- OHKNR: Customer Number [FK: KHKNDHUR.KHKNR, REQUIRED]
- OHOST: Order Status ('1'=Open, '2'=Processing, '3'=Completed)
- OHDAO: Order Date (YYYYMMDD)
- OHBLF: Invoice Amount (DECIMAL)
- OHVAL: Currency Code (VARCHAR 3)
- OHANV: Created By User

Delivery Fields:
- OHDAL: Delivery Date Requested
- OHDAÖ: Delivery Date Confirmed
- OHDAF: Delivery Date Actual
- OHDFF: Delivery Date From

Logistics Fields:
- OHVKT: Total Weight (DECIMAL)
- OHKLI: Number of Packages (INTEGER)
- OHSLK: Carrier Code

Flags:
- OHMOK: VAT Included ('1'=Yes, '0'=No)
- OHDEL: Partial Delivery ('1'=Yes, '0'=No)

---

### 3. ORKORDRR (Order Rows/Lines)
Schema: DCPO.ORKORDRR
Primary Key: ORONR + ORORN
Foreign Keys: ORONR → OHKORDHR.OHONR, ORANR → AHARTHUR.AHANR

Core Fields:
- ORONR: Order Number [FK: OHKORDHR.OHONR]
- ORORN: Order Row Number [INTEGER]
- ORANR: Article Number [FK: AHARTHUR.AHANR, REQUIRED]
- ORKVB: Quantity Ordered (DECIMAL) [USE SUM() with GROUP BY]
- ORKVL: Quantity Delivered (DECIMAL) [USE SUM() with GROUP BY]
- ORPRS: Unit Price (DECIMAL)

Status Fields:
- OROST: Row Status
- ORDAL: Delivery Date
- ORDFF: Delivery From Date
- ORDAR: Delivery Actual Date

Warehouse Fields:
- ORLAG: Warehouse Code
- ORLZN: Zone

---

### 4. AHARTHUR (Articles)
Schema: DCPO.AHARTHUR
Primary Key: AHANR (CHAR)
Status Filter: AHSTS='1' for active articles only

Core Fields:
- AHANR: Article Number [PRIMARY KEY]
- AHBES: Article Name/Description [VARCHAR, REQUIRED]
- AHSTS: Status ('1'=Active, '0'=Inactive)
- AHTYP: Article Type
- AHLNR: Supplier Number [FK: LHLEVHUR.LHLNR]

Stock Fields:
- AHLDG: Stock on Hand (DECIMAL)
- AHLDV: Stock on Order (DECIMAL)
- AHLDT: Available Stock (DECIMAL)
- AHRES: Reserved Stock (DECIMAL)

Pricing Fields:
- AHTPR: Sales Price (DECIMAL) 
- AHPRE: Purchase Price (DECIMAL) 

---

### 5. AYARINFR (Article Extended Information)
Schema: EGU.AYARINFR
Primary Key: AYANR (INTEGER)
Foreign Key: AYANR → AHARTHUR.AHANR

Core Fields:
- AYANR: Article Number [FK: AHARTHUR.AHANR]
- AYTIT: Title [VARCHAR]
- AYFÖF: Author [VARCHAR]
- AYILL: Illustrator [VARCHAR]
- AYBIN: Binding Type [VARCHAR]
- AYTRÅ: Publication Year (INTEGER)

---

### 6. LHLEVHUR (Suppliers)
Schema: DCPO.LHLEVHUR
Primary Key: LHLNR (INTEGER)
Status Filter: LHSTS='1' for active suppliers only

Core Fields:
- LHLNR: Supplier Number [PRIMARY KEY]
- LHSÖK: Search Name [VARCHAR]
- LHLEN: Supplier Name [VARCHAR, REQUIRED]
- LHSTS: Status ('1'=Active, '0'=Inactive)
- LHKON: Contact Person
- LHORG: Organization Number

Address Fields:
- LHFA1: Address Line 1
- LHFA2: Address Line 2
- LHFA3: Address Line 3
- LHFA4: Address Line 4
- LHTEL: Telephone

---

### 7. IHIORDHR (Purchase Order Header)
Schema: DCPO.IHIORDHR
Primary Key: IHONR (INTEGER)
Foreign Key: IHLNR → LHLEVHUR.LHLNR

Core Fields:
- IHONR: Purchase Order Number [PRIMARY KEY]
- IHLNR: Supplier Number [FK: LHLEVHUR.LHLNR, REQUIRED]
- IHOST: Order Status
- IHDAO: Order Date (YYYYMMDD)
- IHDIN: Delivery Date (YYYYMMDD)
- IHVAL: Currency Code (VARCHAR 3)

---

### 8. IRIORDRR (Purchase Order Rows)
Schema: DCPO.IRIORDRR
Primary Key: IRONR + IRORN
Foreign Keys: IRONR → IHIORDHR.IHONR, IRANR → AHARTHUR.AHANR

Core Fields:
- IRONR: Purchase Order Number [FK: IHIORDHR.IHONR]
- IRORN: Row Number (INTEGER)
- IRANR: Article Number [FK: AHARTHUR.AHANR, REQUIRED]
- IRKVB: Quantity Ordered (DECIMAL)
- IRKVL: Quantity Delivered (DECIMAL)
- IRIPR: Purchase Price (DECIMAL)
- IROST: Row Status

---

### 9. KRKFAKTR (Invoices)
Schema: DCPO.KRKFAKTR
Primary Key: KRFNR (INTEGER)
Foreign Key: KRKNR → KHKNDHUR.KHKNR

Core Fields:
- KRFNR: Invoice Number [PRIMARY KEY]
- KRKNR: Customer Number [FK: KHKNDHUR.KHKNR, REQUIRED]
- KRDAF: Invoice Date (YYYYMMDD)
- KRDFF: Due Date (YYYYMMDD)
- KRBLF: Invoice Amount (DECIMAL)
- KRBLB: Outstanding Amount (DECIMAL)

---

### 10. KIINBETR (Payments)
Schema: DCPO.KIINBETR
Primary Key: KIKNR + KIFNR + KIRAD

Core Fields:
- KIKNR: Customer Number [FK: KHKNDHUR.KHKNR]
- KIFNR: Invoice Number [FK: KRKFAKTR.KRFNR]
- KIRAD: Row Number (INTEGER)
- KIDAT: Payment Date (YYYYMMDD)
- KIBLB: Payment Amount (DECIMAL)
- KIPTY: Payment Type

---

### 11. WSOUTSAV (Sales Statistics)
Schema: EGU.WSOUTSAV
Primary Key: WSARG + WSPE6 + WSANR + WSKNR
Status Filter: WSSTS='P' (Posted transactions only)

Core Fields:
- WSARG: Transaction Type
- WSPE6: Period (YYYYMM format, e.g., 202510)
- WSANR: Article Number [FK: AHARTHUR.AHANR]
- WSKNR: Customer Number [FK: KHKNDHUR.KHKNR]
- WSDEBA: Quantity Sold (DECIMAL)
- WSDEBN: Net Amount (DECIMAL)
- WSDEBM: VAT Amount (DECIMAL)
- WSSTS: Status ('P'=Posted, 'U'=Unposted)

---

## Example Queries

### Example 1: Orders This Week
Question: "Show me all orders from this week"
SQL:
SELECT 
    o.OHONR AS Order_Number,
    o.OHKNR AS Customer_Number,
    k.KHFKN AS Customer_Name,
    o.OHDAO AS Order_Date,
    o.OHBLF AS Invoice_Amount,
    o.OHVAL AS Currency
FROM DCPO.OHKORDHR o
INNER JOIN DCPO.KHKNDHUR k ON o.OHKNR = k.KHKNR
WHERE o.OHDAO >= {week_start} 
  AND o.OHDAO <= {week_end}
  AND k.KHSTS = '1'
ORDER BY o.OHDAO DESC
FETCH FIRST 100 ROWS ONLY

---

### Example 2: Top 10 Customers by Order Value
Question: "Show top 10 customers by total order value this month"
SQL:
SELECT 
    k.KHKNR AS Customer_Number,
    k.KHFKN AS Customer_Name,
    COUNT(DISTINCT o.OHONR) AS Total_Orders,
    SUM(o.OHBLF) AS Total_Amount,
    o.OHVAL AS Currency
FROM DCPO.KHKNDHUR k
INNER JOIN DCPO.OHKORDHR o ON k.KHKNR = o.OHKNR
WHERE k.KHSTS = '1'
  AND o.OHDAO >= {month_start} 
  AND o.OHDAO <= {month_end}
GROUP BY k.KHKNR, k.KHFKN, o.OHVAL
ORDER BY Total_Amount DESC
FETCH FIRST 10 ROWS ONLY

---

### Example 3: Article Sales with Details
Question: "Show article details with total ordered and delivered quantities"
SQL:
SELECT 
    a.AHANR AS Article_Number,
    a.AHBES AS Article_Name,
    ay.AYTIT AS Title,
    ay.AYFÖF AS Author,
    SUM(r.ORKVB) AS Total_Ordered,
    SUM(r.ORKVL) AS Total_Delivered,
    COUNT(DISTINCT r.ORONR) AS Number_Of_Orders
FROM DCPO.AHARTHUR a
INNER JOIN DCPO.ORKORDRR r ON a.AHANR = r.ORANR
INNER JOIN DCPO.OHKORDHR o ON r.ORONR = o.OHONR
LEFT JOIN EGU.AYARINFR ay ON a.AHANR = ay.AYANR
WHERE a.AHSTS = '1'
  AND o.OHDAO >= {month_start}
  AND o.OHDAO <= {month_end}
GROUP BY a.AHANR, a.AHBES, ay.AYTIT, ay.AYFÖF
ORDER BY Total_Ordered DESC
FETCH FIRST 20 ROWS ONLY

---

### Example 4: Customer Search by Name
Question: "Find customers with name containing 'Andersson'"
SQL:
SELECT 
    KHKNR AS Customer_Number,
    KHFKN AS Customer_Name,
    KHFA1 AS Address,
    KHTEL AS Telephone,
    KHKON AS Contact_Person
FROM DCPO.KHKNDHUR
WHERE KHSTS = '1'
  AND UPPER(KHFKN) LIKE '%' || UPPER(TRIM('Andersson')) || '%'
ORDER BY KHFKN
FETCH FIRST 50 ROWS ONLY

---

### Example 5: Sales Statistics by Period
Question: "Show sales statistics for last month"
SQL:
SELECT 
    a.AHANR AS Article_Number,
    a.AHBES AS Article_Name,
    SUM(w.WSDEBA) AS Total_Quantity_Sold,
    SUM(w.WSDEBN) AS Total_Net_Amount,
    SUM(w.WSDEBM) AS Total_VAT_Amount
FROM EGU.WSOUTSAV w
INNER JOIN DCPO.AHARTHUR a ON w.WSANR = a.AHANR
WHERE w.WSSTS = 'P'
  AND w.WSPE6 >= {last_month_start}
  AND w.WSPE6 <= {last_month_end}
  AND a.AHSTS = '1'
GROUP BY a.AHANR, a.AHBES
ORDER BY Total_Net_Amount DESC
FETCH FIRST 25 ROWS ONLY

---

## Common Errors to Avoid

1. **GROUP BY Missing**: Always include all non-aggregated columns from SELECT in GROUP BY when using SUM/COUNT
   - Wrong: SELECT AHANR, SUM(ORKVB) FROM ORKORDRR
   - Correct: SELECT AHANR, SUM(ORKVB) FROM ORKORDRR GROUP BY AHANR

2. **Date Format**: Use numeric YYYYMMDD, not string dates
   - Wrong: OHDAO = '2025-10-07'
   - Correct: OHDAO = 20251007

3. **Function on Indexed Columns**: Avoid wrapping indexed columns in functions
   - Wrong: YEAR(OHDAO) = 2025
   - Correct: OHDAO >= 20250101 AND OHDAO <= 20251231

4. **SELECT ***: Never use SELECT *, always specify columns

5. **JOIN Type**: Use INNER JOIN for required relationships, LEFT JOIN only when nulls expected

6. **Status Filters**: Apply status filters (KHSTS='1') in JOIN conditions for better performance

---

## Performance Optimization

- Use indexed primary keys in WHERE clauses: KHKNR, OHONR, AHANR, OHDAO
- Limit large result sets with FETCH FIRST n ROWS ONLY
- Use EXISTS instead of COUNT(*) > 0 for existence checks
- Filter on date ranges using >= and <= operators
- Apply status filters early in JOIN conditions
- Use COALESCE for nullable columns in calculations

""".format(
    today=TODAY_STR,
    week_start=WEEK_START,
    week_end=WEEK_END,
    month_start=MONTH_START,
    month_end=MONTH_END,
    last_month_start=LAST_MONTH_START,
    last_month_end=LAST_MONTH_END,
    year_start=YEAR_START
)


#AHTPR: Sales Price (DECIMAL)  -- not so sure
#AHPRE: Purchase Price (DECIMAL) -- not so sure

# Enhanced SQL Generation Prompt
SQL_GENERATION_PROMPT = """
You are an expert SQL generator for the Förlagssystem AB database.
Use the provided DATABASE_SCHEMA strictly.

Core Objective:
Generate SQL queries that answer only the actual user question. Do not include data, fields, or tables unrelated to their request—even if they match the user's general interests or preferences. Avoid substituting requested data with information from other domains.

Guidelines:
1. Only include tables, columns, and joins explicitly required to answer the user’s question.
2. Always use clear table aliases (e.g., `OHKORDHR o`, `ORKORDRR r`).
3. Use INNER JOIN for required relations, LEFT JOIN for optional, only as needed.
4. Filter for active records (KHSTS='1', AHSTS='1', LHSTS='1') unless otherwise specified.
5. Dates must be in numeric YYYYMMDD format; use >= and <= for ranges.
6. When grouping, provide SUM and COUNT as requested, and always include appropriate GROUP BY clauses.
7. Include only essential fields directly related to the user's query (e.g., customer name, article name, order number, invoice amount), unless more detail is specifically requested.
8. Output valid, DB2-compatible standard SQL only.

Rules:
- Give priority to active customers/products/suppliers by default.
- When displaying amounts, include currency (e.g., SEK) when possible.
- Only join KRKFAKTR or KIINBETR for payment/invoice information if the user explicitly requests it.
- Respect any date range or other filters the user provides.
- Always output valid SQL with necessary GROUP BY and ORDER BY clauses.
- Do not modify user requests based on previous queries unless clarifying ambiguous intent.
- If the query is outside the database’s scope, output a clear, concise error message.
- If SQL generation fails, retry up to three times unless it is a restricted access case; after three tries, return the error message.

Output Policy:
- Generate only the SQL query. Do not provide explanations unless explicitly asked by the user.

Remember:
- Never introduce unrelated information or substitute an answer from your own domain if the user's question is about another area.
- If user context (preferences, history) is known, use it only to clarify ambiguous asks, NOT to adjust clear requests.
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
    """Generate SQL with role-specific optimizations and conversation memory"""
    try:
        memory = initialize_conversation_memory()
        memory_variables = memory.load_memory_variables({})
        chat_history = memory_variables.get("chat_history", [])
        role_context = ""
        if username == "peter":
            role_context = "LOGISTICS USER: Include quantities, item counts, and article details."
        elif username == "harold":
            role_context = "CEO USER: Focus on revenue, totals, and strategic metrics."
        elif username == "lars":
            role_context = "FINANCE USER: Include amounts, payment terms, and financial details."
        sql_prompt = f"""{role_context}
Previous conversation context (last 4 exchanges): 
{chat_history[-8:] if len(chat_history) > 8 else chat_history}
Current user question: "{question}"
Instructions:
1. Consider the conversation history ...
DATABASE SCHEMA:
{DATABASE_SCHEMA}
Generate SQL following the rules in the system prompt. Include JOINs for comprehensive data.
Return ONLY the SQL query."""
        print("DEBUG: About to call OpenAI with the following prompt:")
        #print(sql_prompt)
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": SQL_GENERATION_PROMPT},
                {"role": "user", "content": sql_prompt}
            ],
            temperature=0.1
        )
        sql = response.choices[0].message.content.strip()
        print(f"DEBUG: Raw OpenAI response: {sql}")
        sql = sql.replace("``````", "").strip().rstrip(';')
        print(f"DEBUG: Processed SQL = {sql}")
        return sql
    except Exception as e:
        print(f"ERROR in generate_sql: {repr(e)}")
        return ""



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
             width=True)
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

        if st.button("Login", width=True):
            if username:
                st.session_state.username = username
                st.session_state.messages = []
                initialize_chat_session()  # ADD THIS LINE
                st.rerun()
    else:
        st.markdown(f"### Logged in as")
        st.markdown(f"**{st.session_state.username.upper()}**")
        
        # Show current date context
        st.markdown("---")
        st.markdown(f"**Today:** {TODAY.strftime('%B %d, %Y')}")
        st.markdown(f"**This Week:** {datetime.strptime(WEEK_START, '%Y%m%d').strftime('%b %d')} - {datetime.strptime(WEEK_END, '%Y%m%d').strftime('%b %d')}")

        if st.button("Logout", width=True):
            st.session_state.username = None
            st.session_state.messages = []
            st.rerun()

        # ADD THIS NEW SECTION:
        st.markdown("---")
        st.markdown("### 🧠 Memory Controls")

        # Show session info
        if 'session_id' in st.session_state:
            st.info(f"**Session:** {st.session_state.session_id[:8]}...")

        # Memory control buttons
        col1, col2 = st.columns(2)

        with col1:
            if st.button("🗑️ Clear Session", width=True):
                try:
                    # Clear from database
                    if db_memory_service and 'session_id' in st.session_state:
                        success = asyncio.run(db_memory_service.clear_session_messages(st.session_state.session_id))
                        if success:
                            st.success("✅ DB cleared")
                        else:
                            st.error("❌ DB clear failed")
                    
                    # Clear local memory
                    if "conversation_memory" in st.session_state:
                        st.session_state.conversation_memory.clear()
                    if "messages" in st.session_state:
                        st.session_state.messages = []
                    st.rerun()
                except Exception as e:
                    st.error(f"Clear error: {str(e)}")

        with col2:
            if st.button("📱 New Session", width=True):
                try:
                    # Start new session
                    st.session_state.session_id = str(uuid.uuid4())
                    st.session_state.messages = []
                    if "conversation_memory" in st.session_state:
                        st.session_state.conversation_memory.clear()
                    initialize_chat_session()
                    st.success("✅ New session")
                    st.rerun()
                except Exception as e:
                    st.error(f"New session error: {str(e)}")

        # Show memory stats
        try:
            if db_memory_service and 'session_id' in st.session_state:
                message_count = len(asyncio.run(db_memory_service.get_session_messages(st.session_state.session_id)))
                st.metric("DB Messages", message_count)
            
            if "conversation_memory" in st.session_state:
                memory_vars = st.session_state.conversation_memory.load_memory_variables({})
                chat_history = memory_vars.get("chat_history", [])
                st.metric("Memory Turns", len(chat_history))
        except Exception as e:
            st.error(f"Stats error: {str(e)}")



def clean_sql_output(sql: str) -> str:
    """
    Cleans AI-generated SQL by removing markdown fences and extra formatting.
    """
    if not sql:
        return ""
    sql = sql.replace("```sql", "").replace("```", "").strip()
    return sql

# Main content
if st.session_state.username is None:
    col1, col2, col3 = st.columns([1,2,1])
    with col2:
        st.image("https://www.forlagssystem.se/wp-content/uploads/2023/02/forlagssystem_logo.svg",
                 width=True)
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
        # Initialize memory
        memory = initialize_conversation_memory()
        
        st.session_state.messages.append({"role": "user", "content": user_input})
        save_message_to_database("user", user_input) 
        print("DEBUG: User question =", user_input)
        with st.spinner("Analyzing..."):
            try:
                # Generate SQL with conversation context
                sql = clean_sql_output(generate_sql(user_input, st.session_state.username))
                print(f"DEBUG: Generated SQL = {sql}")
                if not sql.strip().upper().startswith("SELECT"):
                    response = "I can only retrieve information from the system; I can't perform any other operations at the moment."
                else:
                    result = asyncio.run(execute_query(sql, st.session_state.username))
                    if result.get("success"):
                        rows = result["data"]["rows"]
                        if len(rows) == 0:
                            response = "No data found matching your query."
                        else:
                            response = format_results(user_input, rows, st.session_state.username)
                            
                            # Save to conversation memory
                            memory.save_context(
                                {"input": user_input},
                                {"output": response}
                            )
                            # UPDATE CONVERSATION CONTEXT IN DATABASE - ADD THIS BLOCK
                            if sql and sql.strip().upper().startswith("SELECT"):
                                # Extract table names from SQL (simple extraction)
                                tables_used = ""
                                try:
                                    if "FROM" in sql.upper():
                                        sql_parts = sql.upper().split("FROM")[1].split("WHERE")[0].split("ORDER")[0].split("GROUP")[0]
                                        tables_used = sql_parts.strip()[:200]  # Limit length
                                except:
                                    tables_used = "UNKNOWN"
                                
                                result_count = len(rows) if rows else 0
                                update_conversation_context(user_input, sql, tables_used, result_count)
                            # END OF ADDED BLOCK
                    else:
                        error_msg = result.get("message", "").lower()
                        if "permission" in error_msg or "access" in error_msg or "denied" in error_msg:
                            response = "You don't have permission to access that information."
                        else:
                            response = f"Query error. Please try rephrasing your question."
                            
                st.session_state.messages.append({"role": "assistant", "content": response})
                save_message_to_database("assistant", response)
                st.rerun()
            except Exception as e:
                response = "An error occurred. Please try again."
                st.session_state.messages.append({"role": "assistant", "content": response})
                st.rerun()
