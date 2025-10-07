"""
Comprehensive RBAC Rules - All STYR Database Tables
"""

from utils.user_manager import UserRole

# Complete table access permissions for all roles
TABLE_PERMISSIONS = {
    UserRole.CEO: {
        "tables": [
            # Customers & Master Data
            "DCPO.KHKNDHUR",      # Customers (100+ columns)
            
            # Articles & Inventory
            "DCPO.AHARTHUR",      # Articles - main
            "EGU.AYARINFR",       # Articles - additional info
            
            # Orders (Sales)
            "DCPO.OHKORDHR",      # Orders - header (97 columns)
            "DCPO.ORKORDRR",      # Orders - rows (58 columns)
            
            # Purchase Orders
            "DCPO.LHLEVHUR",      # Suppliers
            "DCPO.IHIORDHR",      # Purchase Order - header
            "DCPO.IRIORDRR",      # Purchase Order - rows
            
            # Financial
            "DCPO.KRKFAKTR",      # Invoices
            "DCPO.KIINBETR",      # Incoming Payments
            
            # Statistics & Analytics
            "EGU.WSOUTSAV",       # Sales Statistics
        ],
        "row_filter": None,  # No restrictions
        "sensitive_columns": [],  # Can see everything
        "description": "Full access to all tables and columns"
    },
    
    UserRole.FINANCE: {
        "tables": [
            # Customers (for AR/credit management)
            "DCPO.KHKNDHUR",      # Customers
            
            # Financial Tables
            "DCPO.KRKFAKTR",      # Invoices
            "DCPO.KIINBETR",      # Incoming Payments
            
            # Orders (for revenue tracking)
            "DCPO.OHKORDHR",      # Orders - header
            "DCPO.ORKORDRR",      # Orders - rows
            
            # Purchase (for AP tracking)
            "DCPO.IHIORDHR",      # Purchase Order - header
            "DCPO.IRIORDRR",      # Purchase Order - rows
            "DCPO.LHLEVHUR",      # Suppliers
            
            # Analytics
            "EGU.WSOUTSAV",       # Sales Statistics
        ],
        "row_filter": "KHSTS='1'",  # Only active records
        "sensitive_columns": [],  # Finance can see all financial data
        "description": "Full financial data access including AR, AP, invoices, payments"
    },
    
    UserRole.LOGISTICS: {
        "tables": [
            # Orders Management
            "DCPO.OHKORDHR",      # Orders - header
            "DCPO.ORKORDRR",      # Orders - rows
            
            # Purchase & Suppliers
            "DCPO.LHLEVHUR",      # Suppliers
            "DCPO.IHIORDHR",      # Purchase Order - header
            "DCPO.IRIORDRR",      # Purchase Order - rows
            
            # Inventory
            "DCPO.AHARTHUR",      # Articles - main
            "EGU.AYARINFR",       # Articles - additional info
            
            # Customer info (for delivery)
            "DCPO.KHKNDHUR",      # Customers (address info only)
        ],
        "row_filter": "OHOST IN ('1','2','3')",  # Open/processing orders only
        "sensitive_columns": [
            # Hide pricing columns
            "OHBLF", "OHBLM", "OHFAV", "OHBLOU",  # Order pricing
            "ORPRS", "ORRAB", "OROMO",  # Row pricing
            "IRIPR", "IRRPO",  # Purchase pricing
            
            # Hide credit information
            "KHKGÄ", "KHBLE", "KHSAL", "KHRPF",  # Customer credit
        ],
        "description": "Order fulfillment, inventory, and delivery management without pricing"
    },
    
    UserRole.CUSTOMER_SERVICE: {
        "tables": [
            # Customer Information
            "DCPO.KHKNDHUR",      # Customers
            
            # Orders
            "DCPO.OHKORDHR",      # Orders - header
            "DCPO.ORKORDRR",      # Orders - rows
            
            # Articles (for product info)
            "DCPO.AHARTHUR",      # Articles - main
            "EGU.AYARINFR",       # Articles - additional info
            
            # Invoices (for customer inquiries)
            "DCPO.KRKFAKTR",      # Invoices
        ],
        "row_filter": "KHSTS='1'",  # Only active customers
        "sensitive_columns": [
            # Hide sensitive financial data
            "KHKGÄ", "KHBLE", "KHSAL", "KHRPF",  # Credit limits & balances
            "KRBLF", "KRBLB", "KRBKR",  # Invoice amounts (show only totals)
        ],
        "description": "Customer support with order and product information"
    },
    
    UserRole.CALL_CENTER: {
        "tables": [
            # Basic customer info
            "DCPO.KHKNDHUR",      # Customers (limited columns)
            
            # Order status
            "DCPO.OHKORDHR",      # Orders - header (status only)
            
            # Product info
            "DCPO.AHARTHUR",      # Articles - main
        ],
        "row_filter": "KHSTS='1'",  # Only active customers
        "sensitive_columns": [
            # Hide all financial data
            "KHKGÄ", "KHBLE", "KHSAL", "KHRPF", "KHFKR", "KHPGR",  # Customer financials
            "OHBLF", "OHBLM", "OHFAV", "OHBLOU",  # Order pricing
            "KRBLF", "KRBLB", "KRBKR", "KRFNR",  # Invoice data
            
            # Hide sensitive personal data
            "KHPNR", "KHORGNR",  # Personal/org numbers
        ],
        "description": "Basic customer lookup and order status inquiry"
    },
    
    UserRole.UNIT_MANAGER: {
        "tables": [
            # Customer data for their department
            "DCPO.KHKNDHUR",      # Customers
            
            # Orders for their department
            "DCPO.OHKORDHR",      # Orders - header
            "DCPO.ORKORDRR",      # Orders - rows
            
            # Sales analytics
            "EGU.WSOUTSAV",       # Sales Statistics
            
            # Articles
            "DCPO.AHARTHUR",      # Articles - main
        ],
        "row_filter": "OHSLJ = '{user.department}'",  # Department filter (dynamic)
        "sensitive_columns": [
            "KHKGÄ",  # Credit limit (partial restriction)
        ],
        "description": "Department-specific sales and customer management"
    }
}

# Column name mapping for user-friendly display
COLUMN_FRIENDLY_NAMES = {
    # Customers (KHKNDHUR)
    'KHKNR': 'Customer Number',
    'KHFKN': 'Customer Name',
    'KHTEL': 'Phone Number',
    'KHFA1': 'Address Line 1',
    'KHFA2': 'Address Line 2',
    'KHFA3': 'City',
    'KHFA4': 'Postal Code',
    'KHKGÄ': 'Credit Limit',
    'KHSTS': 'Status',
    'KHPNR': 'Personal Number',
    'KHORGNR': 'Organization Number',
    'KHBLE': 'Balance',
    'KHSAL': 'Balance Accounting Currency',
    
    # Orders Header (OHKORDHR)
    'OHONR': 'Order Number',
    'OHKNR': 'Customer Number',
    'OHDAO': 'Order Date',
    'OHDAL': 'Delivery Date',
    'OHOST': 'Order Status',
    'OHBLF': 'Invoice Amount',
    'OHVAL': 'Currency Code',
    'OHSLJ': 'Seller ID',
    
    # Orders Rows (ORKORDRR)
    'ORONR': 'Order Number',
    'ORORN': 'Order Line Number',
    'ORANR': 'Article Number',
    # 'ORANT': 'Quantity',  # Column does not exist - use ORKVB for Quantity
    'ORKVB': 'Quantity Ordered',
    'ORKVL': 'Quantity Delivered',
    'ORPRS': 'Price',
    'ORRAB': 'Discount',
    
    # Invoices (KRKFAKTR)
    'KRFNR': 'Invoice Number',
    'KRKNR': 'Customer Number',
    'KRDAF': 'Invoice Date',
    'KRDFF': 'Due Date',
    'KRBLF': 'Invoice Amount',
    
    # Payments (KIINBETR)
    'KIKNR': 'Customer Number',
    'KIDAT': 'Payment Date',
    'KIBEL': 'Payment Amount',
    
    # Articles (AHARTHUR)
    'AHANR': 'Article Number',
    'AHBES': 'Article Description',
    'AHLAG': 'Warehouse',
    
    # Suppliers (LHLEVHUR)
    'LHLNR': 'Supplier Number',
    'LHBEN': 'Supplier Name',
    'LHTEL': 'Phone Number',
    
    # Purchase Orders Header (IHIORDHR)
    'IHONR': 'Purchase Order Number',
    'IHLNR': 'Supplier Number',
    'IHDAO': 'Order Date',
    'IHOST': 'Order Status',
    
    # Purchase Orders Rows (IRIORDRR)
    'IRONR': 'Purchase Order Number',
    'IRORN': 'Order Line Number',
    'IRANR': 'Article Number',
    'IRKVB': 'Quantity Ordered',
    'IRIPR': 'Purchase Price',
}

def get_allowed_tables(role: UserRole) -> list:
    """Get list of tables accessible by role"""
    perms = TABLE_PERMISSIONS.get(role, {})
    return perms.get("tables", [])

def get_sensitive_columns(role: UserRole) -> list:
    """Get list of sensitive columns for role"""
    perms = TABLE_PERMISSIONS.get(role, {})
    return perms.get("sensitive_columns", [])

def get_row_filter(role: UserRole, user_department: str = None) -> str:
    """Get row-level filter for role"""
    perms = TABLE_PERMISSIONS.get(role, {})
    filter_rule = perms.get("row_filter")
    
    # Replace dynamic placeholders
    if filter_rule and user_department and "{user.department}" in filter_rule:
        filter_rule = filter_rule.replace("{user.department}", user_department)
    
    return filter_rule

def get_friendly_column_name(technical_name: str) -> str:
    """Convert technical column name to user-friendly name"""
    return COLUMN_FRIENDLY_NAMES.get(technical_name, technical_name)

def get_role_description(role: UserRole) -> str:
    """Get description of role permissions"""
    perms = TABLE_PERMISSIONS.get(role, {})
    return perms.get("description", "No description available")

def get_allowed_tables_with_dynamic(role: UserRole, user_id: str = None) -> list:
    """Get allowed tables including dynamic permissions from database"""
    import os
    
    # 1. Get static baseline rules
    static_tables = get_allowed_tables(role)
    
    # 2. Load dynamic rules from database
    try:
        from services.permission_management_service import PermissionManagementService
        
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={os.getenv('QUERY_LEARNING_DB_SERVER', 'FSDHWFP01\\SQLEXPRESS')};"
            f"DATABASE={os.getenv('QUERY_LEARNING_DB_DATABASE', 'query_learning_db')};"
            f"Trusted_Connection=yes;"
        )
        perm_service = PermissionManagementService(conn_str)
        
        # Get dynamic rules for this role
        dynamic_rules = perm_service.get_rbac_rules_for_role(role.value)
        
        # Merge static + dynamic tables (remove duplicates)
        if dynamic_rules and 'tables' in dynamic_rules and dynamic_rules['tables']:
            combined = static_tables + dynamic_rules['tables']
            all_tables = list(set(combined))  # Remove duplicates
            print(f"✅ Dynamic RBAC loaded for {role.value}: {len(dynamic_rules['tables'])} additional tables")
            return all_tables
            
    except Exception as e:
        print(f"⚠️ Could not load dynamic rules: {e}")
    
    # Fallback to static rules
    return static_tables