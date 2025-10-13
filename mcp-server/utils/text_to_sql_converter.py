# text_to_sql_converter.py

import os
from openai import OpenAI
from dotenv import load_dotenv
from datetime import datetime, timedelta
import re
import asyncio
import httpx
from utils.prompt_manager import PromptManager

# --- Load environment ---
load_dotenv()


_prompt_manager = None
def get_prompt_manager():
    global _prompt_manager
    if _prompt_manager is None:
        _prompt_manager = PromptManager()
    return _prompt_manager



def get_env_variable(var_name, default=None):
    """Get env variable safely, with optional default."""
    return os.getenv(var_name, default)

OPENAI_API_KEY = get_env_variable("OPENAI_API_KEY")
GATEWAY_URL = get_env_variable("GATEWAY_URL")
GATEWAY_TOKEN = get_env_variable("GATEWAY_TOKEN")

client = OpenAI(api_key=OPENAI_API_KEY)

# --- Role prompts (can be expanded as needed) ---
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

# --- SQL Generation Prompt (insert full prompt from your code here as needed) ---
SQL_GENERATION_PROMPT = """
You are an expert SQL generator for the Förlagssystem AB database running on DB2 for AS400.

CRITICAL DB2 AS400 RULES:
1. NEVER use parameter placeholders like :parameter_name, ?, or @parameter
2. NEVER use WEEK(), MONTH(), YEAR(), DATEPART(), or EXTRACT() functions - they are NOT supported
3. ALL dates are stored as numeric YYYYMMDD format (e.g., 20251008)
4. Use direct numeric comparison for date ranges: column >= 20251001 AND column <= 20251031
5. Use the pre-calculated date ranges provided in the schema (THIS_WEEK, THIS_MONTH, etc.)
6. For text searches, use UPPER(column) LIKE '%SEARCHTERM%' with hardcoded values

Date Query Examples:
- "this week": WHERE OHDAO >= {WEEK_START} AND OHDAO <= {WEEK_END}
- "this month": WHERE OHDAO >= {TODAY.replace(day=1).strftime("%Y%m%d")} AND OHDAO <= {MONTH_END.replace('-', '')}
- "today": WHERE OHDAO = {TODAY_STR}

Core Objective:
Generate SQL queries that answer only the actual user question using DB2 AS400 compatible syntax.

Guidelines:
1. Only include tables, columns, and joins explicitly required to answer the user's question.
2. Always use clear table aliases (e.g., `OHKORDHR o`, `ORKORDRR r`).
3. Use INNER JOIN for required relations, LEFT JOIN for optional, only as needed.
4. Filter for active records (KHSTS='1', AHSTS='1', LHSTS='1') unless otherwise specified.
5. Dates must be in numeric YYYYMMDD format; use >= and <= for ranges.
6. When grouping, provide SUM and COUNT as requested, and always include appropriate GROUP BY clauses.
7. Include only essential fields directly related to the user's query.
8. Output valid, DB2 for AS400 compatible SQL only.
9. NEVER use parameter placeholders - always use hardcoded values in WHERE clauses.

Rules:
- Give priority to active customers/products/suppliers by default.
- When displaying amounts, include currency (e.g., SEK) when possible.
- Only join KRKFAKTR or KIINBETR for payment/invoice information if the user explicitly requests it.
- Respect any date range or other filters the user provides.
- Always output valid SQL with necessary GROUP BY and ORDER BY clauses.
- Use FETCH FIRST n ROWS ONLY instead of LIMIT.
- For follow-up questions that reference previous results, use specific values from context.

Output Policy:
- Generate only the SQL query. Do not provide explanations unless explicitly asked by the user.
- NEVER use unsupported date functions.
- NEVER use parameter placeholders.
"""


# --- Database Schema (copy from your expanded code) ---
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

### Percentage Calculations (CRITICAL)
When calculating percentages in DB2, you MUST cast to DECIMAL to avoid integer division:

WRONG (returns 0):
SUM(amount) / (SELECT SUM(amount) FROM table) * 100

CORRECT:
CAST(SUM(amount) AS DECIMAL(18,2)) / CAST((SELECT SUM(amount) FROM table) AS DECIMAL(18,2)) * 100 AS Percentage

Or use this format:
(SUM(amount) * 100.0) / NULLIF((SELECT SUM(amount) FROM table), 0) AS Percentage

Always multiply by 100.0 (not 100) to force decimal division.

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


def clean_sql_output(sql: str) -> str:
    """Cleans AI-generated SQL by removing markdown fences, language hints, and extra text."""
    if not sql:
        return ""

    # Remove markdown fences and language hints (```sql, ```SQL, ```python, etc.)
    sql = re.sub(r"```(?:sql|SQL|python|[\w-]*)?", "", sql)
    sql = re.sub(r"```", "", sql)

    # Remove any trailing text after SQL (like explanations)
    sql = re.split(r"(?i)explanation:|note:|output:", sql)[0]

    # Strip whitespace and trailing semicolons
    return sql.strip().rstrip(';')



def generate_sql(question: str, username: str, conversation_history=None, role_context_override="", system_id: str = "STYR") -> str:
    """Generate SQL using UNIVERSAL context memory - remembers ALL entities and topics"""
    
    # Initialize prompt manager
    pm = get_prompt_manager()
    
    # Load role prompt from database, fallback to hardcoded
    role_ctx_from_db = pm.get_prompt(system_id, 'ROLE_SYSTEM', username)
    role_ctx = role_context_override or role_ctx_from_db or ROLE_PROMPTS.get(username, "")
    
    # Universal entity extraction
    chat_context = ""
    memory = {
        'entities': {},  # All extracted entities
        'last_topic': None,  # What was the last discussion about
        'context_summary': []  # Key points from conversation
    }
    
    if conversation_history and len(conversation_history) > 0:
        recent_history = conversation_history[-10:] if len(conversation_history) > 10 else conversation_history
        
        # EXTRACT ALL TYPES OF ENTITIES from conversation
        for entry in recent_history:
            
            # 1. NUMBERS (customers, orders, invoices, articles, amounts)
            numbers = re.findall(r'\b(\d{1,10})\b', entry)
            for num in numbers:
                num_int = int(num)
                # Classify by magnitude
                if 1 <= num_int <= 99999:
                    if 'numbers' not in memory['entities']:
                        memory['entities']['numbers'] = []
                    if num not in memory['entities']['numbers']:
                        memory['entities']['numbers'].append(num)
            
            # 2. NAMES (customers, suppliers, contacts, products)
            # Look for capitalized words/phrases
            name_matches = re.findall(r'\b([A-ZÅÄÖ][a-zåäö]+(?:\s+[A-ZÅÄÖ][a-zåäö]+)*)\b', entry)
            for name in name_matches:
                if len(name) > 2 and name not in ['User', 'Assistant', 'Customer', 'Order', 'Invoice']:
                    if 'names' not in memory['entities']:
                        memory['entities']['names'] = []
                    if name not in memory['entities']['names']:
                        memory['entities']['names'].append(name)
            
            # 3. DATES (various formats)
            date_patterns = [
                r'\b(\d{4}-\d{2}-\d{2})\b',  # 2025-10-09
                r'\b(\d{8})\b',              # 20251009
                r'\b(\d{1,2}/\d{1,2}/\d{4})\b',  # 10/09/2025
                r'\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b'
            ]
            for pattern in date_patterns:
                dates = re.findall(pattern, entry, re.IGNORECASE)
                if dates:
                    if 'dates' not in memory['entities']:
                        memory['entities']['dates'] = []
                    memory['entities']['dates'].extend([d for d in dates if d not in memory['entities'].get('dates', [])])
            
            # 4. AMOUNTS/CURRENCY
            amount_matches = re.findall(r'(\d+[,\.]?\d*)\s*(SEK|EUR|USD|kr)', entry, re.IGNORECASE)
            if amount_matches:
                if 'amounts' not in memory['entities']:
                    memory['entities']['amounts'] = []
                memory['entities']['amounts'].extend([f"{amt[0]} {amt[1]}" for amt in amount_matches])
            
            # 5. KEY FIELDS mentioned (extract database fields/concepts)
            field_keywords = {
                'customer': ['KHKNR', 'customer number', 'customer', 'kund'],
                'order': ['OHONR', 'order number', 'order', 'beställning'],
                'invoice': ['KRFNR', 'invoice number', 'invoice', 'faktura'],
                'article': ['AHANR', 'article number', 'article', 'artikel', 'product'],
                'supplier': ['LHLNR', 'supplier', 'leverantör'],
                'address': ['address', 'adress', 'location'],
                'phone': ['phone', 'telephone', 'telefon'],
                'email': ['email', 'e-post'],
                'date': ['date', 'datum', 'when'],
                'amount': ['amount', 'price', 'cost', 'belopp', 'pris']
            }
            
            for field_type, keywords in field_keywords.items():
                for keyword in keywords:
                    if keyword.lower() in entry.lower():
                        if 'fields_mentioned' not in memory['entities']:
                            memory['entities']['fields_mentioned'] = []
                        if field_type not in memory['entities']['fields_mentioned']:
                            memory['entities']['fields_mentioned'].append(field_type)
        
        # BUILD CONTEXT SUMMARY
        if memory['entities']:
            chat_context = f"""
        ═══════════════════════════════════════════
        CONVERSATION MEMORY (Last {len(recent_history)} messages)
        ═══════════════════════════════════════════

        RECENT CONVERSATION:
        {chr(10).join(['- ' + msg for msg in recent_history[-5:]])}

        EXTRACTED ENTITIES & CONTEXT:
        """
            
            if 'numbers' in memory['entities'] and memory['entities']['numbers']:
                most_recent_num = memory['entities']['numbers'][-1]
                chat_context += f"\n📊 NUMBERS DISCUSSED: {', '.join(memory['entities']['numbers'][-5:])} (Most Recent: {most_recent_num})"
            
            if 'names' in memory['entities'] and memory['entities']['names']:
                most_recent_name = memory['entities']['names'][-1]
                chat_context += f"\n👤 NAMES MENTIONED: {', '.join(memory['entities']['names'][-5:])} (Most Recent: {most_recent_name})"
            
            if 'dates' in memory['entities'] and memory['entities']['dates']:
                chat_context += f"\n📅 DATES: {', '.join(memory['entities']['dates'][-3:])}"
            
            if 'amounts' in memory['entities'] and memory['entities']['amounts']:
                chat_context += f"\n💰 AMOUNTS: {', '.join(memory['entities']['amounts'][-3:])}"
            
            if 'fields_mentioned' in memory['entities'] and memory['entities']['fields_mentioned']:
                chat_context += f"\n🔑 TOPICS: {', '.join(memory['entities']['fields_mentioned'][-5:])}"
            
            chat_context += "\n═══════════════════════════════════════════\n"
    
    # DETECT FOLLOW-UP QUESTIONS (generic indicators)
    follow_up_indicators = [
        # Pronouns
        'his', 'her', 'their', 'them', 'it', 'its', 'this', 'that', 'these', 'those',
        'he', 'she', 'they',
        # References
        'same', 'the', 'such',
        # Single word queries (often follow-ups)
        '^(name|address|phone|email|orders|invoices|details|info|information|status|date|amount|price|total)$',
        # Question words without subject
        'what is', 'what are', 'show me', 'get me', 'find'
    ]
    
    is_follow_up = any(
        re.search(indicator, question.lower()) 
        for indicator in follow_up_indicators
    )
    
    # INTELLIGENT CONTEXT MATCHING
    sql_context_hint = ""
    if is_follow_up and memory['entities']:
        # Determine what the question is asking about
        question_lower = question.lower()
        
        # Provide context based on what's being asked
        if any(word in question_lower for word in ['customer', 'name', 'address', 'phone', 'email', 'contact', 'kund']):
            # Customer-related follow-up
            if 'numbers' in memory['entities'] and memory['entities']['numbers']:
                most_recent = memory['entities']['numbers'][-1]
                sql_context_hint = f"""

    🎯 FOLLOW-UP DETECTED: Customer Query
    CONTEXT: You just discussed entity with number/ID: {most_recent}
    ACTION: Use this in your WHERE clause: WHERE KHKNR = '{most_recent}' AND KHSTS = '1'
    Or if name-based: WHERE UPPER(KHFKN) LIKE '%{memory['entities'].get('names', [''])[-1].upper()}%' AND KHSTS = '1'
    """
        
        elif any(word in question_lower for word in ['order', 'orders', 'beställning']):
            # Order-related follow-up
            if 'numbers' in memory['entities'] and memory['entities']['numbers']:
                context_num = memory['entities']['numbers'][-1]
                sql_context_hint = f"""

    🎯 FOLLOW-UP DETECTED: Order Query
    CONTEXT: Previous discussion involved number: {context_num}
    ACTION: Use context - either:
    - Order number: WHERE OHONR = '{context_num}'
    - Customer's orders: WHERE OHKNR = '{context_num}' AND OHSTS = '1'
    """
        
        elif any(word in question_lower for word in ['invoice', 'invoices', 'faktura', 'payment']):
            # Invoice-related follow-up
            if 'numbers' in memory['entities'] and memory['entities']['numbers']:
                context_num = memory['entities']['numbers'][-1]
                sql_context_hint = f"""

    🎯 FOLLOW-UP DETECTED: Invoice Query
    CONTEXT: Previous discussion involved number: {context_num}
    ACTION: Use context - either:
    - Invoice number: WHERE KRFNR = '{context_num}'
    - Customer's invoices: WHERE KRKNR = '{context_num}'
    """
        
        elif any(word in question_lower for word in ['article', 'product', 'item', 'artikel']):
            # Article-related follow-up
            if 'numbers' in memory['entities'] and memory['entities']['numbers']:
                context_num = memory['entities']['numbers'][-1]
                sql_context_hint = f"""

    🎯 FOLLOW-UP DETECTED: Article/Product Query
    CONTEXT: Previous discussion involved number: {context_num}
    ACTION: Consider this article number: WHERE AHANR = '{context_num}' AND AHSTS = '1'
    """
        
        else:
            # Generic follow-up - provide all context
            sql_context_hint = f"""

    🎯 FOLLOW-UP DETECTED: General Query
    RECENT CONTEXT:
    - Most recent number: {memory['entities'].get('numbers', ['N/A'])[-1]}
    - Most recent name: {memory['entities'].get('names', ['N/A'])[-1]}
    - Topics discussed: {', '.join(memory['entities'].get('fields_mentioned', ['N/A'])[-3:])}

    ACTION: Use the most relevant context from above based on the user's question.
    """
    
    # Load schema from database, fallback to hardcoded
    schema_from_db = pm.get_variable(system_id, 'DATABASE_SCHEMA')
    schema_to_use = schema_from_db #if schema_from_db else DATABASE_SCHEMA


    from datetime import datetime, timedelta
    today = datetime.now()
    schema_to_use = schema_to_use.format(
        today=today.strftime("%Y%m%d"),
        week_start=(today - timedelta(days=today.weekday())).strftime("%Y%m%d"),
        week_end=(today + timedelta(days=6-today.weekday())).strftime("%Y%m%d"),
        month_start=today.replace(day=1).strftime("%Y%m%d"),
        month_end=(today.replace(day=28) + timedelta(days=4)).replace(day=1).strftime("%Y%m%d") if today.month < 12 else today.replace(month=12, day=31).strftime("%Y%m%d"),
        last_month_start=(today.replace(day=1) - timedelta(days=1)).replace(day=1).strftime("%Y%m%d"),
        last_month_end=(today.replace(day=1) - timedelta(days=1)).strftime("%Y%m%d"),
        year_start=today.replace(month=1, day=1).strftime("%Y%m%d")
    )
    
    # BUILD FINAL PROMPT
    sql_prompt = f"""{role_ctx}

    {chat_context}

    {sql_context_hint}

    CURRENT USER QUESTION: "{question}"

    DATABASE SCHEMA: {schema_to_use}

    INSTRUCTIONS:
    1. If FOLLOW-UP DETECTED above, use that context in your SQL
    2. Generate SQL that answers the current question using available context
    3. Return ONLY the SQL query, no explanations

    Generate the SQL now:
    """

    # Load SQL generation prompt from database, fallback to hardcoded
    sql_gen_prompt = pm.get_prompt(system_id, 'SQL_GENERATION', None) or SQL_GENERATION_PROMPT

    response = client.chat.completions.create(
        model="gpt-5",
        messages=[
            {"role": "system", "content": sql_gen_prompt},
            {"role": "user", "content": sql_prompt}
        ]
    )

    sql = response.choices[0].message.content.strip()
    sql = clean_sql_output(sql)

    # Replace any remaining date placeholders in generated SQL
    from datetime import datetime, timedelta
    from datetime import datetime, timedelta
    today = datetime.now()
    
    # Calculate dates
    week_start = (today - timedelta(days=today.weekday()))
    week_end = (today + timedelta(days=6-today.weekday()))
    month_start = today.replace(day=1)
    
    # Month end (last day of current month)
    if today.month == 12:
        month_end = today.replace(day=31)
    else:
        month_end = (today.replace(month=today.month+1, day=1) - timedelta(days=1))
    
    # Last month
    last_month_end = (today.replace(day=1) - timedelta(days=1))
    last_month_start = last_month_end.replace(day=1)
    
    year_start = today.replace(month=1, day=1)
    sql = sql.replace('{today}', today.strftime("%Y%m%d"))
    sql = sql.replace('{week_start}', week_start.strftime("%Y%m%d"))
    sql = sql.replace('{week_end}', week_end.strftime("%Y%m%d"))
    sql = sql.replace('{month_start}', month_start.strftime("%Y%m%d"))
    sql = sql.replace('{month_end}', month_end.strftime("%Y%m%d"))
    sql = sql.replace('{last_month_start}', last_month_start.strftime("%Y%m%d"))
    sql = sql.replace('{last_month_end}', last_month_end.strftime("%Y%m%d"))
    sql = sql.replace('{year_start}', year_start.strftime("%Y%m%d"))
    return sql


async def get_database_conversation_history(session_id: str, username: str, max_messages: int = 10):
    """
    Get conversation history from database for both chat and chart assistants
    """
    conversation_history = []
    
    if not session_id:
        return conversation_history
    
    try:
        # Configure longer timeout for VPN connection
        timeout_config = httpx.Timeout(
            connect=10.0,
            read=60.0,    # ⬅️ Increase from 10s to 60s
            write=10.0,
            pool=10.0
        )
        
        # Get recent messages from database API
        async with httpx.AsyncClient(timeout=timeout_config) as client:
            response = await client.get(
                f"{GATEWAY_URL}/api/conversation/get-messages/{session_id}",
                headers={
                    "Authorization": f"Bearer {GATEWAY_TOKEN}",
                    "X-Username": username
                }
            )
            
            if response.status_code == 200:
                result = response.json()
                
                # Handle different response formats
                if isinstance(result, dict):
                    db_messages = result.get('messages', result.get('data', []))
                elif isinstance(result, list):
                    db_messages = result
                else:
                    print(f"Unexpected response type: {type(result)}")
                    return conversation_history
                
                print(f"✅ Loaded {len(db_messages)} messages from database")
                
                # Get recent messages only (last N messages)
                recent_messages = db_messages[-max_messages:] if len(db_messages) > max_messages else db_messages
                
                for msg in recent_messages:
                    try:
                        if msg['message_type'] == 'user':
                            conversation_history.append(f"User: {msg['message_content']}")
                        elif msg['message_type'] == 'assistant':
                            response_summary = msg['message_content'][:200] + "..." if len(msg['message_content']) > 200 else msg['message_content']
                            conversation_history.append(f"Assistant: {response_summary}")
                    except (KeyError, TypeError) as e:
                        print(f"⚠️ Skipping malformed message: {e}")
                        continue
                
                print(f"✅ Parsed {len(conversation_history)} conversation turns")
                
    except httpx.ReadTimeout:
        print(f"⚠️ Timeout fetching conversation history for session {session_id}")
        # Return empty history on timeout - system will work without context
        
    except Exception as e:
        print(f"❌ Error loading conversation history: {e}")
        import traceback
        traceback.print_exc()
    
    return conversation_history

async def get_enhanced_conversation_context(session_id: str, username: str):
    """
    Get enhanced context including last SQL and query patterns
    """
    try:
        # Get recent messages
        conversation_history = await get_database_conversation_history(session_id, username)
        
        # Also get the last query context (from conversation_context table)
        async with httpx.AsyncClient() as client:
            context_response = await client.get(
                f"{GATEWAY_URL}/api/conversation/get-context/{session_id}",
                headers={
                    "Authorization": f"Bearer {GATEWAY_TOKEN}", 
                    "X-Username": username
                },
                timeout=10.0
            )
            
            if context_response.status_code == 200:
                context_data = context_response.json()
                last_query = context_data.get("last_query", "")
                last_sql = context_data.get("last_sql", "")
                last_tables = context_data.get("last_tables_used", "")
                
                return {
                    "conversation_history": conversation_history,
                    "last_query": last_query,
                    "last_sql": last_sql,
                    "last_tables": last_tables
                }
            
    except Exception as e:
        print(f"Enhanced context error: {e}")
    
    return {
        "conversation_history": [],
        "last_query": "",
        "last_sql": "",
        "last_tables": ""
    }

def generate_sql_with_session_context(question: str, username: str, session_id: str = None, role_context_override: str = ""):
    """
    Generate SQL using database session context - works for both chat and chart assistants
    """
    
    # Get conversation history from database if session_id provided
    conversation_history = []
    if session_id:
        print(f"🔍 Loading context for session: {session_id}")
        try:
            # Run async function in sync context
            conversation_history = asyncio.run(get_database_conversation_history(session_id, username))
            print(f"📚 Conversation history loaded: {len(conversation_history)} turns")
        except Exception as e:
            print(f"❌ Could not load session context: {e}")
    else:
        print(f"⚠️ No session_id provided - no context available")
    
    # Use existing generate_sql function with database history
    return generate_sql(question, username, conversation_history, role_context_override)
