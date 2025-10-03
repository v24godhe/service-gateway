from utils.user_manager import UserRole

# Table access permissions
TABLE_PERMISSIONS = {
    UserRole.CEO: {
        "tables": "ALL",  # Can access all 11 tables
        "row_filter": None,
        "sensitive_columns": []  # Can see everything
    },
    
    UserRole.FINANCE: {
        "tables": [
            "DCPO.KHKNDHUR",  # Customers
            "DCPO.KRKFAKTR",  # Invoices
            "DCPO.KIINBETR",  # Payments
            "DCPO.OHKORDHR",  # Orders (for finance info)
        ],
        "row_filter": "KHSTS='1'",  # Only active customers
        "sensitive_columns": []
    },
    
    UserRole.LOGISTICS: {
        "tables": [
            "DCPO.OHKORDHR",  # Orders
            "DCPO.ORKORDRR",  # Order lines
            "DCPO.LHLEVHUR",  # Suppliers
            "DCPO.IHIORDHR",  # Purchase orders
            "DCPO.IRIORDRR",  # Purchase order lines
        ],
        "row_filter": "OHOST IN ('1','2','3')",  # Open orders only
        "sensitive_columns": ["OHBLF", "ORPRS"]  # Hide pricing
    },
    
    UserRole.CUSTOMER_SERVICE: {
        "tables": [
            "DCPO.KHKNDHUR",  # Customers
            "DCPO.OHKORDHR",  # Orders
            "DCPO.ORKORDRR",  # Order lines
        ],
        "row_filter": "KHSTS='1'",
        "sensitive_columns": ["KHKGÄ", "KHBLE"]  # Hide credit limit
    },
    
    UserRole.CALL_CENTER: {
        "tables": [
            "DCPO.KHKNDHUR",  # Customers
            "DCPO.OHKORDHR",  # Orders
        ],
        "row_filter": "KHSTS='1'",
        "sensitive_columns": ["KHKGÄ", "KHBLE", "KHRPF"]  # Hide financial data
    }
}

def get_allowed_tables(role: UserRole) -> list:
    perms = TABLE_PERMISSIONS.get(role, {})
    if perms.get("tables") == "ALL":
        return [
            "DCPO.KHKNDHUR", "DCPO.AHARTHUR", "EGU.AYARINFR",
            "DCPO.OHKORDHR", "DCPO.ORKORDRR", "DCPO.LHLEVHUR",
            "DCPO.IHIORDHR", "DCPO.IRIORDRR", "DCPO.KRKFAKTR",
            "DCPO.KIINBETR", "EGU.WSOUTSAV"
        ]
    return perms.get("tables", [])

def get_sensitive_columns(role: UserRole) -> list:
    return TABLE_PERMISSIONS.get(role, {}).get("sensitive_columns", [])