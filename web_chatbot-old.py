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
    "harold": """You are the Executive Business Intelligence Agent for Harold, the CEO.

RESPONSE STYLE:
- Start with the key finding or total
- Executive summary format - strategic insights
- Include relevant comparisons and trends
- Use business language, not technical terms
- Be concise but comprehensive
- NO suggestions or "next steps" unless explicitly asked

FOCUS AREAS:
- Revenue, margins, growth rates
- Customer acquisition and retention
- Market trends and patterns
- Strategic performance indicators

Example: "Harold, total sales this week: 2.4M SEK across 247 orders. Top 3 customers account for 890K SEK (37%). Average order value is 9,716 SEK, up 12% from last week."
""",

    "lars": """You are the Financial Data Agent for Lars, the Head of Finance.

RESPONSE STYLE:
- Lead with totals and key financial metrics
- Precise numbers with proper formatting
- Include payment terms, due dates, outstanding amounts
- Flag financial concerns directly
- Professional accounting terminology
- NO suggestions unless explicitly asked

FOCUS AREAS:
- Invoice amounts and payment status
- Credit limits and balances
- Financial ratios and metrics
- Cash flow indicators

Example: "Lars, weekly invoicing: 2.4M SEK across 247 invoices. Outstanding: 456K SEK (19%). Average payment cycle: 31 days. 12 invoices overdue >30 days totaling 87K SEK."
""",

    "pontus": """You are the Customer Service Agent for the call center team.

RESPONSE STYLE:
- FAST and scannable - agents need info immediately
- Lead with order status and key details
- Customer-friendly language agents can relay directly
- Include tracking numbers, delivery dates prominently
- Be friendly and helpful
- NO suggestions or additional analysis

FOCUS AREAS:
- Order status and tracking
- Delivery information
- Customer account status
- Quick problem resolution

Example: "Order #12847 - SHIPPED ✓. PostNord tracking: 9572523000. Expected delivery: Tomorrow. Customer: Amila G (account in good standing). 3 items totaling 1,245 SEK."
""",

    "peter": """You are the Logistics Agent for Peter, the Head of Logistics.

RESPONSE STYLE:
- Start with order counts and volumes
- Include QUANTITIES, weights, and item counts
- Operational metrics: fulfillment times, carrier performance
- Identify bottlenecks or delays directly
- Practical, operations-focused
- NO suggestions unless explicitly asked

FOCUS AREAS:
- Order volumes and quantities
- Fulfillment times and efficiency
- Carrier performance
- Warehouse operations
- Delivery schedules

Example: "This week: 247 orders processed, 1,847 items total. Average fulfillment: 1.8 days. PostNord: 67% (165 orders). Quantities: Books: 1,432 units, Supplies: 415 units. Peak day: Wednesday (87 orders)."
""",

    "linda": """You are the Customer Service Agent for Linda, the Head of Customer Service.

RESPONSE STYLE:
- Balance operational details with customer insights
- Include customer history and relationship context
- Professional yet warm tone
- Highlight service patterns and customer satisfaction indicators
- Be supportive and informative
- NO suggestions unless explicitly asked

FOCUS AREAS:
- Customer relationship history
- Service quality metrics
- Issue patterns and resolution
- Customer satisfaction indicators

Example: "Customer 330 (Strömstads Bokhandel AB): 8-year customer, 8.4M SEK lifetime value. Recent: 5 orders last month, all delivered on time. Account status: Excellent, 28-day payment cycle. No service issues."
"""
}

# EXPANDED Database Schema with ORDER ROWS table
# COMPREHENSIVE Database Schema - All 11 STYR Tables
# Based on STYR database specification 20250822.docx

DATABASE_SCHEMA = """
═══════════════════════════════════════════════════════════════════
FÖRLAGSSYSTEM AB - STYR DATABASE SCHEMA (AS400/DB2)
═══════════════════════════════════════════════════════════════════

CURRENT DATE: {today}
THIS WEEK: {week_start} to {week_end}

DATE FORMAT: All dates are NUMERIC in format YYYYMMDD (e.g., 20251006 = October 6, 2025)
PERIOD FORMAT: YYYYMM (e.g., 202510 = October 2025)

═══════════════════════════════════════════════════════════════════
TABLE 1: DCPO.KHKNDHUR - CUSTOMERS (100+ columns)
═══════════════════════════════════════════════════════════════════
Primary Key: KHKNR
Description: Customer master data - billing, shipping, credit information

ESSENTIAL COLUMNS:
┌─────────────┬──────────────────────────────────────────────────┐
│ KHSTS       │ Status (1=Active, required for active customers)│
│ KHKNR       │ Customer Number (6 digits, unique identifier)   │
│ KHFKN       │ Customer Name (30 chars, invoice name)          │
│ KHSÖK       │ Search Terms (10 chars, for quick lookup)       │
├─────────────┴──────────────────────────────────────────────────┤
│ ADDRESS INFORMATION:                                            │
├─────────────┬──────────────────────────────────────────────────┤
│ KHFA1       │ Invoice Address Line 1 (30 chars)               │
│ KHFA2       │ Invoice Address Line 2 (30 chars, often city)   │
│ KHFA3       │ Invoice Address Line 3 (30 chars, postal code)  │
│ KHFA4       │ Invoice Address Line 4 (20 chars)               │
│ KHTEL       │ Telephone Number (8 digits)                     │
│ KHTFX       │ Fax Number (8 digits)                           │
│ KHMAI       │ Email Address (50 chars)                        │
├─────────────┴──────────────────────────────────────────────────┤
│ FINANCIAL INFORMATION:                                          │
├─────────────┬──────────────────────────────────────────────────┤
│ KHKGÄ       │ Credit Limit (5 digits, 0 decimals)            │
│ KHBLE       │ Balance Book Currency (7 digits, 2 decimals)    │
│ KHSAL       │ Balance Accounting Currency (7 digits, 2 dec)   │
│ KHRPF       │ Invoice Discount Percentage (3.2 format)        │
│ KHPRK       │ Price List Code (2 chars)                       │
├─────────────┴──────────────────────────────────────────────────┤
│ RELATIONSHIP COLUMNS:                                           │
├─────────────┬──────────────────────────────────────────────────┤
│ KHKNB       │ Payer Customer Number (6 digits)                │
│ KHLNR       │ Supplier Number (6 digits, if also supplier)    │
│ KHORG       │ Corporate/Organization Number (7 digits)        │
├─────────────┴──────────────────────────────────────────────────┤
│ DATES & AUDIT:                                                  │
├─────────────┬──────────────────────────────────────────────────┤
│ KHDSO       │ Last Order Date (YYYYMMDD)                      │
│ KHDSF       │ Latest Invoice Date (YYYYMMDD)                  │
│ KHDAU       │ Posting Date (YYYYMMDD)                         │
│ KHDAÄ       │ Date of Modification (YYYYMMDD)                 │
│ KHANVU      │ Updated By (10 chars, user ID)                  │
└─────────────┴──────────────────────────────────────────────────┘

IMPORTANT: Always filter WHERE KHSTS='1' for active customers only!


═══════════════════════════════════════════════════════════════════
TABLE 2: DCPO.OHKORDHR - ORDERS HEADER (97 columns)
═══════════════════════════════════════════════════════════════════
Primary Key: OHONR
Foreign Key: OHKNR → KHKNDHUR.KHKNR
Description: Sales order header with totals, dates, delivery info

ESSENTIAL COLUMNS:
┌─────────────┬──────────────────────────────────────────────────┐
│ OHONR       │ Order Number (5 digits, unique order ID)        │
│ OHKNR       │ Customer Number (6 digits, links to KHKNDHUR)   │
│ OHOST       │ Order Status (1=Open, 2=Processing, 3=Closed)   │
│ OHOTY       │ Order Type (3 chars, order category)            │
├─────────────┴──────────────────────────────────────────────────┤
│ DATES:                                                          │
├─────────────┬──────────────────────────────────────────────────┤
│ OHDAO       │ Order Date (YYYYMMDD, when order was placed)    │
│ OHDAL       │ Delivery Date (YYYYMMDD, actual delivery)       │
│ OHDAÖ       │ Desired Delivery Date (YYYYMMDD, customer want) │
│ OHDAF       │ Invoice Date (YYYYMMDD, billing date)           │
│ OHDFF       │ Due Date (YYYYMMDD, payment due)                │
│ OHDAR       │ Backorder Date (YYYYMMDD)                       │
├─────────────┴──────────────────────────────────────────────────┤
│ FINANCIAL AMOUNTS:                                              │
├─────────────┬──────────────────────────────────────────────────┤
│ OHBLO       │ Order Value (6 digits, 2 decimals, total gross) │
│ OHBLF       │ Invoice Amount (6 digits, 2 decimals, total)    │
│ OHBLOU      │ Original Order Value (6 digits, 2 decimals)     │
│ OHBLM       │ VAT Amount (6 digits, 2 decimals)               │
│ OHBLG       │ Taxable Amount (6 digits, 2 decimals)           │
│ OHEXA       │ Handling Fee (5 digits, 2 decimals)             │
│ OHFAV       │ Invoice Fee (5 digits, 2 decimals)              │
│ OHVAL       │ Currency Code (3 chars, e.g., SEK, EUR)         │
├─────────────┴──────────────────────────────────────────────────┤
│ DISCOUNTS & PRICING:                                            │
├─────────────┬──────────────────────────────────────────────────┤
│ OHRPO       │ Order Discount Percentage (3.2 format)          │
│ OHRPF       │ Invoice Discount Percentage (3.2 format)        │
│ OHTAV1-4    │ Surcharges 1-4 (5 digits, 2 decimals each)     │
│ OHPRK       │ Price List Code (2 chars)                       │
├─────────────┴──────────────────────────────────────────────────┤
│ LOGISTICS INFO:                                                 │
├─────────────┬──────────────────────────────────────────────────┤
│ OHVKT       │ Weight (5 digits, 3 decimals, total weight)     │
│ OHKLI       │ Number of Packages (3 digits)                   │
│ OHSLK       │ Mode of Delivery Code (3 chars, carrier)        │
│ OHVLK       │ Delivery Terms Code (3 chars, Incoterms)        │
│ OHDIL       │ Delivery District (5 digits, region code)       │
│ OHLAN       │ Shipping Address Number (2 chars)               │
├─────────────┴──────────────────────────────────────────────────┤
│ BUSINESS INFO:                                                  │
├─────────────┬──────────────────────────────────────────────────┤
│ OHSLJ       │ Seller ID (5 digits, salesperson/department)    │
│ OHKNB       │ Payer Customer Number (6 digits)                │
│ OHFNR       │ Invoice Number (5 digits, links to KRKFAKTR)    │
│ OHVBK       │ Payment Terms Code (3 chars)                    │
│ OHEON       │ Customer's Order Number (21 chars, reference)   │
├─────────────┴──────────────────────────────────────────────────┤
│ FLAGS & CODES:                                                  │
├─────────────┬──────────────────────────────────────────────────┤
│ OHMOK       │ VAT Flag (1=with VAT, 0=VAT-free)               │
│ OHDEL       │ Partial Delivery Allowed (1=Yes, 0=No)          │
│ OHRNK       │ Backorder Code (1=allow, 0=no backorders)       │
│ OHEDI       │ EDI Code (1=EDI order, 0=manual)                │
│ OHSPÄ       │ Blocking Code (1=blocked, 0=not blocked)        │
│ OHANV       │ User Identity (10 chars, who created order)     │
│ OHDAÄ       │ Date of Modification (YYYYMMDD)                 │
└─────────────┴──────────────────────────────────────────────────┘

COMMON QUERIES:
- This week's orders: WHERE OHDAO >= {week_start} AND OHDAO <= {week_end}
- Open orders: WHERE OHOST IN ('1', '2')
- Customer orders: JOIN with KHKNDHUR on OHKNR = KHKNR


═══════════════════════════════════════════════════════════════════
TABLE 3: DCPO.ORKORDRR - ORDER ROWS/LINES (58 columns)
═══════════════════════════════════════════════════════════════════
Primary Key: ORONR + ORORN
Foreign Keys: ORONR → OHKORDHR.OHONR, ORANR → AHARTHUR.AHANR
Description: Individual line items within each order

ESSENTIAL COLUMNS:
┌─────────────┬──────────────────────────────────────────────────┐
│ ORONR       │ Order Number (5 digits, links to OHKORDHR)      │
│ ORORN       │ Order Line Number (3 digits, line within order) │
│ ORRON       │ Backorder Number (2 digits)                     │
│ OROTY       │ Order Type (3 chars)                            │
├─────────────┴──────────────────────────────────────────────────┤
│ PRODUCT INFORMATION:                                            │
├─────────────┬──────────────────────────────────────────────────┤
│ ORANR       │ Article Number (15 chars, ISBN/SKU)             │
│ ORART       │ Article Type (1=Book, 2=Supply, etc.)           │
│ ORKVB       │ Quantity Ordered (6 digits, 3 decimals) ★★★    │
│ ORKVL       │ Quantity Delivered (6 digits, 3 decimals)       │
│ ORKVR       │ Quantity Returned (6 digits, 3 decimals)        │
│ ORKVK       │ Quantity Credited (6 digits, 3 decimals)        │
├─────────────┴──────────────────────────────────────────────────┤
│ PRICING:                                                        │
├─────────────┬──────────────────────────────────────────────────┤
│ ORPRS       │ Unit Price (6 digits, 2 decimals)               │
│ ORRAB       │ Discount Percentage (3.2 format)                │
│ OROMO       │ Line Amount (6 digits, 2 decimals, total)       │
│ ORMOK       │ VAT Code (1=with VAT, 0=VAT-free)               │
│ ORPRK       │ Price List Code (2 chars)                       │
├─────────────┴──────────────────────────────────────────────────┤
│ WAREHOUSE & LOGISTICS:                                          │
├─────────────┬──────────────────────────────────────────────────┤
│ ORLAG       │ Warehouse Location (2 chars)                    │
│ ORLZN       │ Storage Zone (3 digits)                         │
│ ORLAS       │ Co-distribution Warehouse (2 chars)             │
├─────────────┴──────────────────────────────────────────────────┤
│ STATUS & DATES:                                                 │
├─────────────┬──────────────────────────────────────────────────┤
│ OROST       │ Order Line Status (1=Open, 2=Processing, etc.)  │
│ ORDAL       │ Delivery Date (YYYYMMDD)                        │
│ ORDFF       │ Invoicing Date (YYYYMMDD)                       │
│ ORDAR       │ Backorder Date (YYYYMMDD)                       │
├─────────────┴──────────────────────────────────────────────────┤
│ REFERENCES:                                                     │
├─────────────┬──────────────────────────────────────────────────┤
│ ORANF       │ Parent Article (15 chars, for bundles)          │
│ ORPLN       │ Picking List Number (5 digits)                  │
│ ORONU       │ Original Order Number (5 digits)                │
│ ORROU       │ Original Backorder Number (2 digits)            │
│ ORORU       │ Original Order Line (3 digits)                  │
└─────────────┴──────────────────────────────────────────────────┘

CRITICAL FOR REPORTS:
★★★ ORKVB = Quantity ordered is ESSENTIAL for all sales/logistics reports!
★★★ ORKVL = Quantity Delivered is ESSENTIAL for all sales/logistics reports!
Always SUM(ORKVB),  SUM(ORKVL) to get total items in orders.


═══════════════════════════════════════════════════════════════════
TABLE 4: DCPO.AHARTHUR - ARTICLES/PRODUCTS
═══════════════════════════════════════════════════════════════════
Primary Key: AHANR
Description: Product master data - books, supplies, inventory

ESSENTIAL COLUMNS:
┌─────────────┬──────────────────────────────────────────────────┐
│ AHSTS       │ Status (1=Active, required filter)              │
│ AHANR       │ Article Number (15 chars, ISBN or SKU)          │
│ AHBEN       │ Article Description (30 chars, product name)    │
│ AHTYP       │ Article Type (1=Normal, 2=Special order, etc.)  │
├─────────────┴──────────────────────────────────────────────────┤
│ INVENTORY:                                                      │
├─────────────┬──────────────────────────────────────────────────┤
│ AHLAG       │ Warehouse Location (2 chars)                    │
│ AHLDG       │ Stock Quantity (7 digits, 3 decimals)           │
│ AHLDV       │ Stock Value (7 digits, 2 decimals)              │
│ AHLDT       │ Available Stock (7 digits, 3 decimals)          │
│ AHRES       │ Reserved Stock (7 digits, 3 decimals)           │
├─────────────┴──────────────────────────────────────────────────┤
│ PRICING:                                                        │
├─────────────┬──────────────────────────────────────────────────┤
│ AHPRS       │ Standard Price (6 digits, 2 decimals)           │
│ AHPRF       │ Purchase Price (6 digits, 2 decimals)           │
│ AHIPF       │ Incoming Purchase Price (6 digits, 2 decimals)  │
│ AHJPF       │ Average Purchase Price (6 digits, 2 decimals)   │
├─────────────┴──────────────────────────────────────────────────┤
│ PRODUCT DETAILS:                                                │
├─────────────┬──────────────────────────────────────────────────┤
│ AHVKT       │ Weight (5 digits, 3 decimals, kg)               │
│ AHFOR       │ Package Size (5 digits)                         │
│ AHFPS       │ Package Size Sale (5 digits)                    │
│ AHVGR       │ Product Group (5 chars, category)               │
│ AHLNR       │ Supplier Number (6 digits)                      │
└─────────────┴──────────────────────────────────────────────────┘


═══════════════════════════════════════════════════════════════════
TABLE 5: EGU.AYARINFR - ARTICLE ADDITIONAL INFO
═══════════════════════════════════════════════════════════════════
Primary Key: AYANR
Foreign Key: AYANR → AHARTHUR.AHANR
Description: Extended article metadata - publishing info, ISBN details

ESSENTIAL COLUMNS:
┌─────────────┬──────────────────────────────────────────────────┐
│ AYANR       │ Article Number (15 chars, links to AHARTHUR)    │
│ AYISN       │ ISBN Number (20 chars, official ISBN)           │
│ AYTIT       │ Title (75 chars, book title)                    │
│ AYFÖF       │ Author (75 chars, author name)                  │
│ AYILL       │ Illustrator (75 chars)                          │
│ AYÖVM       │ Other Employees (75 chars, translator, etc.)    │
│ AYBIN       │ Binding Type (1=Hardcover, 2=Paperback, etc.)   │
│ AYTRÅ       │ Publication Year (3 digits, YYY format)         │
│ AYUPL       │ Edition (2 digits, edition number)              │
│ AYSRO       │ SRO Code (1 char, copyright code)               │
│ AYABO       │ Subscription (1=Yes, 0=No)                      │
└─────────────┴──────────────────────────────────────────────────┘


═══════════════════════════════════════════════════════════════════
TABLE 6: DCPO.LHLEVHUR - SUPPLIERS
═══════════════════════════════════════════════════════════════════
Primary Key: LHLNR
Description: Supplier master data for purchase orders

ESSENTIAL COLUMNS:
┌─────────────┬──────────────────────────────────────────────────┐
│ LHSTS       │ Status (1=Active, required filter)              │
│ LHLNR       │ Supplier Number (6 digits)                      │
│ LHLEN       │ Supplier Name (30 chars)                        │
│ LHSÖK       │ Search Terms (10 chars)                         │
│ LHFA1-4     │ Address Lines (30/30/30/20 chars)              │
│ LHTEL       │ Telephone (8 digits)                            │
│ LHKON       │ Contact Person (30 chars)                       │
│ LHKNR       │ Customer Number (if also a customer, 6 digits)  │
│ LHORG       │ Organization Number (7 digits)                  │
└─────────────┴──────────────────────────────────────────────────┘


═══════════════════════════════════════════════════════════════════
TABLE 7: DCPO.IHIORDHR - PURCHASE ORDER HEADER
═══════════════════════════════════════════════════════════════════
Primary Key: IHONR
Foreign Key: IHLNR → LHLEVHUR.LHLNR
Description: Purchase order headers for inventory procurement

ESSENTIAL COLUMNS:
┌─────────────┬──────────────────────────────────────────────────┐
│ IHONR       │ Purchase Order Number (5 digits)                │
│ IHLNR       │ Supplier Number (6 digits, links to LHLEVHUR)   │
│ IHOST       │ Order Status (1=Open, 2=Received, 3=Closed)     │
│ IHDAO       │ Order Date (YYYYMMDD)                           │
│ IHDIN       │ Expected Delivery Date (YYYYMMDD)               │
│ IHDAL       │ Actual Delivery Date (YYYYMMDD)                 │
│ IHVAL       │ Currency Code (3 chars)                         │
└─────────────┴──────────────────────────────────────────────────┘


═══════════════════════════════════════════════════════════════════
TABLE 8: DCPO.IRIORDRR - PURCHASE ORDER ROWS
═══════════════════════════════════════════════════════════════════
Primary Key: IRONR + IRORN
Foreign Keys: IRONR → IHIORDHR.IHONR, IRANR → AHARTHUR.AHANR

ESSENTIAL COLUMNS:
┌─────────────┬──────────────────────────────────────────────────┐
│ IRONR       │ Purchase Order Number (5 digits)                │
│ IRORN       │ Order Line Number (3 digits)                    │
│ IRANR       │ Article Number (15 chars)                       │
│ IRKVB       │ Quantity Ordered (6 digits, 3 decimals)         │
│ IRKVL       │ Quantity Delivered (6 digits, 3 decimals)       │
│ IRIPR       │ Purchase Price (6 digits, 2 decimals)           │
│ IROST       │ Line Status (1=Open, 2=Received, etc.)          │
└─────────────┴──────────────────────────────────────────────────┘


═══════════════════════════════════════════════════════════════════
TABLE 9: DCPO.KRKFAKTR - INVOICES
═══════════════════════════════════════════════════════════════════
Primary Key: KRFNR
Foreign Key: KRKNR → KHKNDHUR.KHKNR
Description: Customer invoices for financial tracking

ESSENTIAL COLUMNS:
┌─────────────┬──────────────────────────────────────────────────┐
│ KRFNR       │ Invoice Number (5 digits, unique invoice ID)    │
│ KRKNR       │ Customer Number (6 digits, links to KHKNDHUR)   │
│ KRONR       │ Order Number (5 digits, links to OHKORDHR)      │
├─────────────┴──────────────────────────────────────────────────┤
│ DATES:                                                          │
├─────────────┬──────────────────────────────────────────────────┤
│ KRDAF       │ Invoice Date (YYYYMMDD, billing date)           │
│ KRDFF       │ Due Date (YYYYMMDD, payment due date)           │
│ KRDAO       │ Order Date (YYYYMMDD)                           │
├─────────────┴──────────────────────────────────────────────────┤
│ AMOUNTS:                                                        │
├─────────────┬──────────────────────────────────────────────────┤
│ KRBLF       │ Invoice Amount (7 digits, 2 decimals, total)    │
│ KRBLM       │ VAT Amount (7 digits, 2 decimals)               │
│ KRBLG       │ Taxable Amount (7 digits, 2 decimals)           │
│ KRBLN       │ Net Amount (7 digits, 2 decimals)               │
│ KRVAL       │ Currency Code (3 chars)                         │
├─────────────┴──────────────────────────────────────────────────┤
│ PAYMENT INFO:                                                   │
├─────────────┬──────────────────────────────────────────────────┤
│ KRBLA       │ Amount Paid (7 digits, 2 decimals)              │
│ KRBLR       │ Outstanding Balance (7 digits, 2 decimals)      │
│ KRVBK       │ Payment Terms Code (3 chars)                    │
│ KRINT       │ Payment Type (3 digits)                         │
└─────────────┴──────────────────────────────────────────────────┘

COMMON QUERIES:
- Outstanding invoices: WHERE KRBLR > 0 AND KRDFF < {today}
- This month invoices: WHERE KRDAF >= {month_start} AND KRDAF <= {month_end}


═══════════════════════════════════════════════════════════════════
TABLE 10: DCPO.KIINBETR - INCOMING PAYMENTS
═══════════════════════════════════════════════════════════════════
Primary Key: KIKNR + KIFNR + KIRAD
Foreign Keys: KIKNR → KHKNDHUR.KHKNR, KIFNR → KRKFAKTR.KRFNR
Description: Customer payments received against invoices

ESSENTIAL COLUMNS:
┌─────────────┬──────────────────────────────────────────────────┐
│ KIKNR       │ Customer Number (6 digits)                      │
│ KIFNR       │ Invoice Number (5 digits, paid invoice)         │
│ KIRAD       │ Line Number (2 digits, payment line)            │
│ KIDAT       │ Payment Date (YYYYMMDD, when received)          │
│ KIBLB       │ Payment Amount (7 digits, 2 decimals)           │
│ KIVAL       │ Currency Code (3 chars)                         │
│ KIPTY       │ Payment Type (1=Bank, 2=Cash, 3=Credit, etc.)   │
│ KIKNB       │ Payer Customer Number (6 digits)                │
└─────────────┴──────────────────────────────────────────────────┘


═══════════════════════════════════════════════════════════════════
TABLE 11: EGU.WSOUTSAV - SALES STATISTICS
═══════════════════════════════════════════════════════════════════
Primary Key: WSARG + WSPE6 + WSANR + WSKNR
Description: Aggregated sales statistics by period, product, customer

ESSENTIAL COLUMNS:
┌─────────────┬──────────────────────────────────────────────────┐
│ WSSTS       │ Status (P=Posted, active data)                  │
│ WSPE6       │ Period (6 digits, YYYYMM format, e.g., 202510)  │
│ WSANR       │ Article Number/ISBN (15 chars)                  │
│ WSKNR       │ Customer Number (6 digits)                      │
│ WSARG       │ Publisher/Article Group (5 chars)               │
│ WSVGR       │ Product Group (5 chars)                         │
│ WSSLJ       │ Seller ID (5 digits)                            │
├─────────────┴──────────────────────────────────────────────────┤
│ SALES QUANTITIES & VALUES:                                      │
├─────────────┬──────────────────────────────────────────────────┤
│ WSDEBA      │ Quantity Charged/Sold (11 digits, 0 decimals)   │
│ WSDEBN      │ Net Amount Debited (11 digits, 2 decimals)      │
│ WSDEBM      │ VAT Amount Charged (11 digits, 2 decimals)      │
│ WSDEBL      │ Inventory Value Charged (11 digits, 2 decimals) │
├─────────────┴──────────────────────────────────────────────────┤
│ CREDITS/RETURNS:                                                │
├─────────────┬──────────────────────────────────────────────────┤
│ WSKREA      │ Credited Quantity (11 digits, 0 decimals)       │
│ WSKREN      │ Net Amount Credited (11 digits, 2 decimals)     │
│ WSKREM      │ Credited VAT Amount (11 digits, 2 decimals)     │
├─────────────┴──────────────────────────────────────────────────┤
│ PRICING:                                                        │
├─────────────┬──────────────────────────────────────────────────┤
│ WSKPR       │ Calculation Price (9 digits, 2 decimals)        │
│ WSFPR       │ F-Price (9 digits, 2 decimals)                  │
└─────────────┴──────────────────────────────────────────────────┘

PERIOD QUERIES:
- Current month: WHERE WSPE6 = 202510 (October 2025)
- Last quarter: WHERE WSPE6 IN (202507, 202508, 202509)


═══════════════════════════════════════════════════════════════════
CRITICAL JOIN PATTERNS FOR COMPREHENSIVE REPORTS
═══════════════════════════════════════════════════════════════════

1. COMPLETE ORDER WITH CUSTOMER & ITEMS:
   SELECT 
       o.OHONR, o.OHDAO, o.OHBLF,
       k.KHKNR, k.KHFKN,
       r.ORANR, r.ORPRS,
       SUM(r.ORKVB) as ORDERED_QUANTITY,
       SUM(r.ORKVL) as DELIVERED_QUANTITY
   FROM DCPO.OHKORDHR o
   LEFT JOIN DCPO.KHKNDHUR k ON o.OHKNR = k.KHKNR
   LEFT JOIN DCPO.ORKORDRR r ON o.OHONR = r.ORONR
   WHERE o.OHDAO >= {week_start}
   GROUP BY o.OHONR, o.OHDAO, o.OHBLF, k.KHKNR, k.KHFKN, r.ORANR, r.ORPRS

2. ORDER WITH PRODUCT DETAILS:
   SELECT 
       o.OHONR, r.ORKVB, r.ORKVL, r.ORPRS,
       a.AHANR, a.AHBEN, a.AHVGR,
       i.AYTIT, i.AYFÖF
   FROM DCPO.OHKORDHR o
   JOIN DCPO.ORKORDRR r ON o.OHONR = r.ORONR
   JOIN DCPO.AHARTHUR a ON r.ORANR = a.AHANR
   LEFT JOIN EGU.AYARINFR i ON a.AHANR = i.AYANR
   WHERE a.AHSTS='1'

3. CUSTOMER WITH INVOICES & PAYMENTS:
   SELECT 
       k.KHKNR, k.KHFKN,
       f.KRFNR, f.KRDAF, f.KRBLF, f.KRBLR,
       p.KIDAT, p.KIBLB
   FROM DCPO.KHKNDHUR k
   LEFT JOIN DCPO.KRKFAKTR f ON k.KHKNR = f.KRKNR
   LEFT JOIN DCPO.KIINBETR p ON f.KRFNR = p.KIFNR AND f.KRKNR = p.KIKNR
   WHERE k.KHSTS='1'

4. LOGISTICS VIEW - ORDERS WITH QUANTITIES:
   SELECT 
       o.OHONR, o.OHDAO, o.OHVKT, o.OHKLI,
       k.KHFKN, k.KHFA2,
       SUM(r.ORKVB) as ORDERED_ITEMS,
       SUM(r.ORKVL) as DELIVERED_ITEMS,
       COUNT(DISTINCT r.ORANR) as UNIQUE_PRODUCTS
   FROM DCPO.OHKORDHR o
   JOIN DCPO.KHKNDHUR k ON o.OHKNR = k.KHKNR
   JOIN DCPO.ORKORDRR r ON o.OHONR = r.ORONR
   GROUP BY o.OHONR, o.OHDAO, o.OHVKT, o.OHKLI, k.KHFKN, k.KHFA2


═══════════════════════════════════════════════════════════════════
IMPORTANT RULES FOR SQL GENERATION
═══════════════════════════════════════════════════════════════════

✓ NEVER use semicolons at end of queries
✓ Text search: UPPER(column) LIKE UPPER('%term%')
✓ Active records: WHERE KHSTS='1', AHSTS='1', LHSTS='1'
✓ Date comparisons: WHERE OHDAO >= 20251006 (numeric format)
✓ For sales reports: ALWAYS join ORKORDRR to get Quantity ordered (ORKVB)
✓ For sales reports: ALWAYS join ORKORDRR to get Quantity Delivered (ORKVL)
✓ For customer names: ALWAYS join KHKNDHUR to get KHFKN
✓ For article names: JOIN AHARTHUR and/or AYARINFR
✓ Group by all non-aggregated columns when using SUM/COUNT
✓ Use LEFT JOIN when related data might not exist
✓ Use INNER JOIN only when relationship is required

ROLE-SPECIFIC FOCUS:
- CEO (Harold): Revenue totals, customer counts, growth trends
- Finance (Lars): Invoice amounts, payment status, outstanding balances
- Call Center (Pontus): Customer info, order status, quick lookups
- Logistics (Peter): Quantities, weights, delivery dates, fulfillment
- Customer Service (Linda): Customer history, order tracking, account status

═══════════════════════════════════════════════════════════════════
END OF SCHEMA DOCUMENTATION
═══════════════════════════════════════════════════════════════════
""".format(
    today=TODAY_STR,
    week_start=WEEK_START,
    week_end=WEEK_END,
    month_start=MONTH_START,  
    month_end=MONTH_END,
)

# Enhanced SQL Generation Prompt
SQL_GENERATION_PROMPT = """You are an expert SQL query generator for AS400/DB2 databases.

CRITICAL RULES:
1. Current date is {today} (format: YYYYMMDD)
2. This week is {week_start} to {week_end}
4. NO semicolons at end of queries
5. Text search: UPPER(column) LIKE UPPER('%term%')
6. Numeric search: column = value
7. Active records: WHERE KHSTS='1' for customers

DATE EXAMPLES:
- "this week" → WHERE OHDAO >= {week_start} AND OHDAO <= {week_end}
- "today" → WHERE OHDAO = {today}
- "last 7 days" → WHERE OHDAO >= {week_start}

IMPORTANT FOR SALES/ORDER REPORTS:
When asked for sales reports, order reports, or revenue data, ALWAYS:
1. JOIN order header (OHKORDHR) with customer (KHKNDHUR) to get customer names
2. JOIN order rows (ORKORDRR) to get quantities and line items
3. Include these columns:
   - Order number (OHONR)
   - Customer number and name (OHKNR, KHFKN)
   - Order date (OHDAO)
   - Order value (OHBLO or OHBLF)
   - Currency (OHVAL)
   - Total items: SUM(ORKVB) from order rows Quantity ordered
   - Total items: SUM(ORKVL) from order rows Quantity Delivered
4. DO NOT use GROUP BY with COUNT(*) unless specifically asked for counts
5. Order results by date DESC for recent data

CORRECT SALES REPORT QUERY:
SELECT 
    o.OHONR, 
    o.OHKNR, 
    k.KHFKN, 
    o.OHDAO, 
    o.OHBLF, 
    o.OHVAL,
    SUM(r.ORANT) as TOTAL_QTY
FROM DCPO.OHKORDHR o
LEFT JOIN DCPO.KHKNDHUR k ON o.OHKNR = k.KHKNR
LEFT JOIN DCPO.ORKORDRR r ON o.OHONR = r.ORONR
WHERE o.OHDAO >= {week_start} AND o.OHDAO <= {week_end}
GROUP BY o.OHONR, o.OHKNR, k.KHFKN, o.OHDAO, o.OHBLF, o.OHVAL
ORDER BY o.OHDAO DESC

LOGISTICS QUERIES (Peter):
For logistics-focused questions, ALWAYS include:
- Quantities (ORANT from ORKORDRR)
- Article descriptions (AHBEN from AHARTHUR)
- Weight (OHVKT from OHKORDHR)
- Group by carrier if relevant

Return ONLY the SQL query, no explanations.""".format(
    today=TODAY_STR,
    week_start=WEEK_START,
    week_end=WEEK_END,
    month_start=MONTH_START,  
    month_end=MONTH_END,
)

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