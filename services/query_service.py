import time
from models.query_schemas import DynamicQueryRequest
from database.styr_connector import StyrDatabaseConnector
from utils.query_validator import query_validator
from utils.response_formatter import response_formatter
from utils.audit_logger import audit_logger

class QueryService:
    def __init__(self, db_connector: StyrDatabaseConnector):
        self.db = db_connector
    
    async def execute_dynamic_query(self, request: DynamicQueryRequest, request_id: str = None,  username: str = "system"):
        """Execute a validated dynamic SQL query"""
        start_time = time.time()
        
        # 1. Validate query
        is_valid, result = query_validator.validate_query(request.query, request.max_rows)
        
        if not is_valid:
            return response_formatter.error_response(
                message=f"Query validation failed: {result}",
                error_code="INVALID_QUERY",
                details=result,
                request_id=request_id
            ).model_dump()
        
        # result now contains the validated/modified query
        safe_query = result
        
        try:
            # 2. Execute query
            raw_data = await self.db.execute_query(safe_query)
            execution_time = (time.time() - start_time) * 1000
            
            # 3. Log successful execution
            audit_logger.log_database_operation(
                request_id=request_id or "unknown",
                operation_type="SELECT",
                table_name=None,  # Extract if needed
                query=request.query,
                execution_time_ms=execution_time,
                row_count=len(raw_data),
                success=True,
                error=None
            )
            
            return response_formatter.success_response(
                data={
                    "rows": raw_data,
                    "row_count": len(raw_data),
                    "query_executed": safe_query
                },
                message=f"Query executed successfully, returned {len(raw_data)} rows",
                execution_time_ms=execution_time,
                request_id=request_id
            ).model_dump()
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            
            # Log failed execution
            audit_logger.log_database_operation(
                request_id=request_id or "unknown",
                operation_type="dynamic_query",
                table_name=request.query_type or "custom",
                query=safe_query,
                execution_time_ms=execution_time,
                row_count=0,
                success=False,
                error=str(e)
            )
            
            return response_formatter.error_response(
                message=f"Query execution failed: {str(e)}",
                error_code="QUERY_EXECUTION_ERROR",
                details=str(e),
                execution_time_ms=execution_time,
                request_id=request_id
            ).model_dump()
    
    async def get_live_schema(self, table_name: str):
        """Fetch live schema from AS400 system catalog"""
        query = f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH as LENGTH,
            NUMERIC_PRECISION,
            COLUMN_TEXT as DESCRIPTION
        FROM QSYS2.SYSCOLUMNS
        WHERE TABLE_SCHEMA = '{table_name.split('.')[0]}'
        AND TABLE_NAME = '{table_name.split('.')[1]}'
        ORDER BY ORDINAL_POSITION
        FETCH FIRST 100 ROWS ONLY
        """
        
        try:
            columns = await self.db.execute_query(query)
            return columns
        except Exception as e:
            return []

    async def get_database_schema(self, request_id: str = None):
        """Return database schema with LIVE metadata from AS400"""
        
        # Get live schema for each table
        customer_cols = await self.get_live_schema("DCPO.KHKNDHUR")
        order_header_cols = await self.get_live_schema("DCPO.OHKORDHR")
        order_rows_cols = await self.get_live_schema("DCPO.ORKORDRR")
        articles_cols = await self.get_live_schema("DCPO.AHARTHUR")
        article_info_cols = await self.get_live_schema("EGU.AYARINFR")
        suppliers_cols = await self.get_live_schema("DCPO.LHLEVHUR")
        purchase_header_cols = await self.get_live_schema("DCPO.IHIORDHR")
        purchase_rows_cols = await self.get_live_schema("DCPO.IRIORDRR")
        invoices_cols = await self.get_live_schema("DCPO.KRKFAKTR")
        payments_cols = await self.get_live_schema("DCPO.KIINBETR")
        sales_stats_cols = await self.get_live_schema("EGU.WSOUTSAV")
        
        schema_data = {
            "tables": [
                {
                    "name": "DCPO.KHKNDHUR",
                    "description": "Customer Master Table",
                    "columns": customer_cols,
                    "examples": [
                        "SELECT KHKNR, KHFKN FROM DCPO.KHKNDHUR WHERE KHSTS='1' FETCH FIRST 10 ROWS ONLY",
                        "SELECT * FROM DCPO.KHKNDHUR WHERE UPPER(KHFKN) LIKE '%SEARCH%' AND KHSTS='1'"
                    ]
                },
                {
                    "name": "DCPO.OHKORDHR",
                    "description": "Orders Header Table",
                    "columns": order_header_cols,
                    "examples": [
                        "SELECT OHONR, OHKNR, OHDAÖ FROM DCPO.OHKORDHR WHERE OHKNR = 330"
                    ]
                },
                {
                    "name": "DCPO.ORKORDRR",
                    "description": "Order Rows/Lines Table",
                    "columns": order_rows_cols,
                    "examples": [
                        "SELECT ORONR, ORANR, ORKVB FROM DCPO.ORKORDRR WHERE ORONR = 12345"
                    ]
                },
                {
                    "name": "DCPO.AHARTHUR",
                    "description": "Articles/Items Master Table",
                    "columns": articles_cols,
                    "examples": [
                        "SELECT AHANR, AHBES FROM DCPO.AHARTHUR WHERE AHSTS='1'"
                    ]
                },
                {
                    "name": "EGU.AYARINFR",
                    "description": "Article Additional Information",
                    "columns": article_info_cols,
                    "examples": [
                        "SELECT AYANR, AYFÖF, AYTIT FROM EGU.AYARINFR"
                    ]
                },
                {
                    "name": "DCPO.LHLEVHUR",
                    "description": "Suppliers Master Table",
                    "columns": suppliers_cols,
                    "examples": [
                        "SELECT LHLNR, LHLEN FROM DCPO.LHLEVHUR WHERE LHSTS='1'"
                    ]
                },
                {
                    "name": "DCPO.IHIORDHR",
                    "description": "Purchase Orders Header",
                    "columns": purchase_header_cols,
                    "examples": [
                        "SELECT IHONR, IHLNR, IHDAO FROM DCPO.IHIORDHR"
                    ]
                },
                {
                    "name": "DCPO.IRIORDRR",
                    "description": "Purchase Order Rows",
                    "columns": purchase_rows_cols,
                    "examples": [
                        "SELECT IRONR, IRANR, IRKVB FROM DCPO.IRIORDRR"
                    ]
                },
                {
                    "name": "DCPO.KRKFAKTR",
                    "description": "Invoices Table",
                    "columns": invoices_cols,
                    "examples": [
                        "SELECT KRKNR, KRFNR, KRDAF, KRBLF FROM DCPO.KRKFAKTR"
                    ]
                },
                {
                    "name": "DCPO.KIINBETR",
                    "description": "Incoming Payments Table",
                    "columns": payments_cols,
                    "examples": [
                        "SELECT KIKNR, KIFNR, KIDAT, KIBLB FROM DCPO.KIINBETR"
                    ]
                },
                {
                    "name": "EGU.WSOUTSAV",
                    "description": "Sales Statistics Table",
                    "columns": sales_stats_cols,
                    "examples": [
                        "SELECT WSANR, WSPE6, WSDEBA FROM EGU.WSOUTSAV"
                    ]
                }
            ],
            "query_limits": {
                "max_rows_default": 100,
                "max_rows_absolute": 1000,
                "timeout_seconds": 30
            },
            "important_filters": {
                "active_customers": "KHSTS = '1'",
                "active_articles": "AHSTS = '1'",
                "active_suppliers": "LHSTS = '1'",
                "date_format": "YYYYMM or YYDDD (e.g., 202509 for September 2025)"
            }
        }
        
        return response_formatter.success_response(
            data=schema_data,
            message="Database schema retrieved successfully",
            request_id=request_id
        ).model_dump()