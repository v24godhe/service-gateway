import re
from typing import Tuple, List
from utils.user_manager import User, UserRole
from utils.rbac_rules import get_allowed_tables, get_sensitive_columns
from utils.rbac_rules import get_allowed_tables_with_dynamic

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
        
        # Load allowed tables (static + dynamic)
        from utils.rbac_rules import get_allowed_tables_with_dynamic
        allowed_tables = get_allowed_tables_with_dynamic(user.role, user.username)
        
        print(f"🔍 User: {user.username} ({user.role.value})")
        print(f"🔍 Allowed tables: {allowed_tables}")
        print(f"🔍 Requested tables: {tables_in_query}")
        
        # Check table access
        if user.role != UserRole.CEO:
            for table in tables_in_query:
                if table not in allowed_tables:
                    # AUTO-CREATE PERMISSION REQUEST
                    from services.permission_management_service import PermissionManagementService
                    import os
                    
                    try:
                        conn_str = (
                            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                            f"SERVER={os.getenv('QUERY_LEARNING_DB_SERVER', 'FSDHWFP01\\SQLEXPRESS')};"
                            f"DATABASE={os.getenv('QUERY_LEARNING_DB_DATABASE', 'query_learning_db')};"
                            f"Trusted_Connection=yes;"
                        )
                        perm_service = PermissionManagementService(conn_str)
                        
                        request_id = perm_service.create_permission_request(
                            user_id=user.username,
                            user_role=user.role.value,
                            requested_table=table,
                            original_question=f"Access attempted: {sql[:200]}",
                            blocked_sql=sql,
                            justification="Auto-generated: User needs table access"
                        )
                        
                        return False, f"Access denied to table {table}. Permission request #{request_id} created."
                    except Exception as e:
                        print(f"❌ Failed to create permission request: {e}")
                        return False, f"Access denied to table {table}"
        

        # Check sensitive columns (but skip if dynamically allowed)
        sensitive_cols = get_sensitive_columns(user.role)
        if sensitive_cols:
            # Load dynamic column permissions
            try:
                from services.permission_management_service import PermissionManagementService
                import os
                
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={os.getenv('QUERY_LEARNING_DB_SERVER', 'FSDHWFP01\\SQLEXPRESS')};"
                    f"DATABASE={os.getenv('QUERY_LEARNING_DB_DATABASE', 'query_learning_db')};"
                    f"Trusted_Connection=yes;"
                )
                perm_service = PermissionManagementService(conn_str)
                dynamic_rules = perm_service.get_rbac_rules_for_role(user.role.value)
                
                # Get dynamically allowed columns for the tables in this query
                allowed_sensitive_cols = []
                if dynamic_rules and 'restrictions' in dynamic_rules:
                    for table in tables_in_query:
                        if table in dynamic_rules['restrictions']:
                            table_allowed = dynamic_rules['restrictions'][table].get('allowed_columns', [])
                            if table_allowed:
                                allowed_sensitive_cols.extend(table_allowed)
                
                print(f"🔍 Sensitive columns: {sensitive_cols}")
                print(f"🔍 Dynamically allowed: {allowed_sensitive_cols}")
                
            except Exception as e:
                print(f"⚠️ Could not load dynamic column permissions: {e}")
                allowed_sensitive_cols = []
            
            # Check each sensitive column
            for col in sensitive_cols:
                if col in sql.upper():
                    # Skip if dynamically allowed
                    if col in allowed_sensitive_cols:
                        print(f"✅ Column {col} allowed via dynamic permission")
                        continue
                        
                    # Create permission request
                    from services.permission_management_service import PermissionManagementService
                    import os
                    
                    try:
                        conn_str = (
                            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                            f"SERVER={os.getenv('QUERY_LEARNING_DB_SERVER', 'FSDHWFP01\\SQLEXPRESS')};"
                            f"DATABASE={os.getenv('QUERY_LEARNING_DB_DATABASE', 'query_learning_db')};"
                            f"Trusted_Connection=yes;"
                        )
                        perm_service = PermissionManagementService(conn_str)
                        
                        request_id = perm_service.create_permission_request(
                            user_id=user.username,
                            user_role=user.role.value,
                            requested_table=tables_in_query[0] if tables_in_query else "UNKNOWN",
                            requested_columns=[col],
                            original_question=f"Column access: {sql[:200]}",
                            blocked_sql=sql,
                            justification=f"Auto-generated: User needs access to column {col}"
                        )
                        
                        return False, f"Column {col} is restricted. Permission request #{request_id} created."
                    except Exception as e:
                        print(f"❌ Failed to create permission request: {e}")
                        return False, f"Column {col} is restricted"

        return True, "OK"

    def _extract_tables_from_sql(self, sql: str) -> list:
        """Extract table names from SQL"""
        tables = []
        sql_upper = sql.upper()
        
        # Extract SCHEMA.TABLE patterns
        pattern = r'(?:FROM|JOIN)\s+([\w]+\.[\w]+)'
        matches = re.finditer(pattern, sql_upper)
        
        for match in matches:
            table = match.group(1)
            if table not in tables:
                tables.append(table)
        
        return tables

# Global validator instance
query_validator = QueryValidator()