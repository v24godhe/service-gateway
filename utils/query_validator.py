import re
from typing import Tuple, List
from utils.user_manager import User, UserRole
from utils.rbac_rules import get_allowed_tables, get_sensitive_columns

class QueryValidator:
    """Validates SQL queries for safety"""
    
    # Tables that are allowed to be queried
    ALLOWED_TABLES = [
        "DCPO.KHKNDHUR",  # Customers (100 cols) ✅
        "DCPO.OHKORDHR",  # Orders header (97 cols) ✅
        "DCPO.ORKORDRR",  # Order rows
        "DCPO.AHARTHUR",  # Articles
        "EGU.AYARINFR",   # Article info
        "DCPO.LHLEVHUR",  # Suppliers
        "DCPO.IHIORDHR",  # Purchase orders
        "DCPO.IRIORDRR",  # Purchase order rows
        "DCPO.KRKFAKTR",  # Invoices
        "DCPO.KIINBETR",  # Incoming payments
        "EGU.WSOUTSAV",   # Sales statistics
    ]
    
    # Keywords that are forbidden
    FORBIDDEN_KEYWORDS = [
        "DROP", "DELETE", "UPDATE", "INSERT", "ALTER",
        "GRANT", "REVOKE", "TRUNCATE", "CREATE", "EXEC",
        "EXECUTE", "DECLARE", "CURSOR"
    ]
    
    def validate_query(self, query: str, max_rows: int = 100) -> Tuple[bool, str]:
        """
        Validate SQL query for safety
        Returns: (is_valid, error_message_or_modified_query)
        """
        query_upper = query.upper().strip()
        
        # 1. Must be SELECT only
        if not query_upper.startswith("SELECT"):
            return False, "Only SELECT queries are allowed"
        
        # 2. Check for forbidden keywords
        for keyword in self.FORBIDDEN_KEYWORDS:
            if keyword in query_upper:
                return False, f"Forbidden keyword detected: {keyword}"
        
        # 3. Must have FROM clause
        if not re.search(r'\bFROM\b', query_upper):
            return False, "Query must include FROM clause"
        
        # 4. Extract and validate table names
        tables_found = self._extract_table_names(query_upper)
        for table in tables_found:
            if table not in [t.upper() for t in self.ALLOWED_TABLES]:
                return False, f"Table not allowed: {table}"
        
        # 5. Ensure row limit exists (add if missing)
        if "FETCH FIRST" not in query_upper and "LIMIT" not in query_upper:
            # Add row limit
            query = query.strip()
            if query.endswith(';'):
                query = query[:-1]
            query = f"{query} FETCH FIRST {max_rows} ROWS ONLY"
        
        
        return True, query
    

    def _extract_table_names(self, query_upper: str) -> List[str]:
        """Extract table names from query including subqueries"""
        tables = []
        
        # Find all table references (FROM/JOIN + table_name)
        pattern = r'(?:FROM|JOIN)\s+([\w\.]+)'
        matches = re.finditer(pattern, query_upper)
        
        for match in matches:
            table = match.group(1)
            # Skip subquery aliases (no dots means it's an alias like "AS data")
            if '.' in table:
                tables.append(table)
        
        return tables


   
    def validate_user_access(self, sql: str, user: User) -> tuple[bool, str]:
        """Check if user can access requested tables"""
        
        # Extract tables from query
        tables_in_query = self._extract_tables_from_sql(sql)
        allowed_tables = get_allowed_tables(user.role)
        
        if allowed_tables != "ALL":
            for table in tables_in_query:
                if table not in allowed_tables:
                    return False, f"Access denied to table {table}"
        
        # Check sensitive columns
        sensitive_cols = get_sensitive_columns(user.role)
        if sensitive_cols:
            for col in sensitive_cols:
                if col in sql.upper():
                    return False, f"Column {col} is restricted"
        
        return True, "OK"

    def _extract_tables_from_sql(self, sql: str) -> list:
        """Extract table names from SQL"""
        tables = []
        sql_upper = sql.upper()
        
        # Simple extraction (you can improve this)
        for table in ["DCPO.KHKNDHUR", "DCPO.OHKORDHR", "DCPO.ORKORDRR", 
                    "DCPO.KRKFAKTR", "DCPO.KIINBETR", "DCPO.LHLEVHUR",
                    "DCPO.AHARTHUR", "EGU.AYARINFR", "EGU.WSOUTSAV",
                    "DCPO.IHIORDHR", "DCPO.IRIORDRR"]:
            if table in sql_upper:
                tables.append(table)
        
        return tables

# Global validator instance
query_validator = QueryValidator()