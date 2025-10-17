import time
import os
from typing import Dict, Any
from database.styr_connector import StyrDatabaseConnector
from utils.response_formatter import ResponseFormatter
from utils.audit_logger import AuditLogger

class QueryService:
    def __init__(self, db_connector: StyrDatabaseConnector):
        self.db = db_connector
        self.response_formatter = ResponseFormatter()
        self.audit_logger = AuditLogger()
        

    async def execute_dynamic_query(self, request, request_id: str = None, username: str = "system"):
        """Execute a validated dynamic SQL query"""
        start_time = time.time()
        
        try:
            # Extract query from request (DynamicQueryRequest object)
            if hasattr(request, 'query'):
                safe_query = request.query
            else:
                safe_query = request.get('query', '') if isinstance(request, dict) else ''
            
            if not safe_query:
                return self.response_formatter.error_response(
                    message="No query provided",
                    error_code="MISSING_QUERY",
                    request_id=request_id
                ).model_dump()

            # Connect to database if not already connected
            if not self.db.connection:
                await self.db.connect()

            # Execute query
            raw_data = await self.db.execute_query(safe_query)
            execution_time = (time.time() - start_time) * 1000
            
            # Log successful execution
            self.audit_logger.log_database_operation(
                request_id=request_id or "unknown",
                operation_type="dynamic_query",
                table_name=getattr(request, 'query_type', 'custom') if hasattr(request, 'query_type') else "custom",
                query=safe_query if 'safe_query' in locals() else "FAILED_TO_PARSE",
                execution_time_ms=execution_time,
                row_count=0,
                success=False,
                error=str(e)
            )
            
            return self.response_formatter.success_response(
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
            self.audit_logger.log_database_operation(
                request_id=request_id or "unknown",
                operation_type="dynamic_query",
                table_name=getattr(request, 'query_type', 'custom') if hasattr(request, 'query_type') else "custom",
                query=safe_query if 'safe_query' in locals() else "FAILED_TO_PARSE",
                execution_time_ms=execution_time,
                row_count=0,
                success=False,
                error=str(e),
                username=username
            )
            
            return self.response_formatter.error_response(
                message=f"Query execution failed: {str(e)}",
                error_code="QUERY_EXECUTION_ERROR",
                details=str(e),
                execution_time_ms=execution_time,
                request_id=request_id
            ).model_dump()

    async def get_database_schema(self, system_id: str = "STYR", user_role: str = "dev_admin", request_id: str = None):
        """
        Get database schema using ONLY PromptManager (Service Gateway API)
        NO fallback mechanisms - fails cleanly if configuration not available
        
        Args:
            system_id: System identifier (default: STYR)
            user_role: User role for RBAC filtering (default: dev_admin)
            request_id: Request tracking ID
        
        Returns:
            Dictionary containing configured schema with only visible columns
        """
        try:
            import requests
            gateway_url = os.getenv('GATEWAY_URL', 'http://10.200.0.2:8080')
            gateway_token = os.getenv('GATEWAY_TOKEN')
            
            response = requests.get(
                f"{gateway_url}/api/{system_id}/schema-with-rbac",
                params={'user_role': user_role},
                headers={'Authorization': f'Bearer {gateway_token}'} if gateway_token else {},
                timeout=10
            )
            
            if response.status_code != 200:
                error_msg = f"Schema unavailable for {system_id} - Status: {response.status_code}"
                return {
                    "error": error_msg,
                    "tables": [],
                    "metadata_source": "SERVICE_GATEWAY_ERROR",
                    "status_code": response.status_code
                }
            
            data = response.json()
            schema_dict = data.get('schema', {})
            
            # Convert to expected format
            tables = []
            for table_name, table_info in schema_dict.items():
                if table_info.get('status') == 'configured':
                    columns = []
                    for col in table_info.get('columns', []):
                        columns.append({
                            'COLUMN_NAME': col['column_name'],
                            'DATA_TYPE': 'CONFIGURED',
                            'DESCRIPTION': col['friendly_name'],
                            'IS_VISIBLE': col.get('is_visible', True),
                            'IS_CONFIGURED': True
                        })
                    
                    tables.append({
                        "name": table_name,
                        "description": f"{table_name} (CONFIGURED SCHEMA)",
                        "columns": columns,
                        "status": "configured"
                    })
                elif table_info.get('status') == 'pending_configuration':
                    tables.append({
                        "name": table_name,
                        "description": f"{table_name} (REQUIRES CONFIGURATION)",
                        "columns": [],
                        "status": "pending_configuration",
                        "message": table_info.get('message', 'Admin must configure this table first'),
                        "examples": []
                    })
            
            return {
                "tables": tables,
                "metadata_source": "CONFIGURED_SCHEMA_ONLY",
                "system_id": system_id,
                "user_role": user_role,
                "total_configured_tables": len([t for t in tables if t['status'] == 'configured']),
                "note": "Schema loaded from table_metadata_config via Service Gateway API"
            }
            
        except Exception as e:
            # Log the error but DO NOT fallback to any hardcoded methods
            if request_id:
                self.audit_logger.log_database_operation(
                request_id=request_id,
                operation_type="get_schema",
                table_name="ALL",
                query="GET_DATABASE_SCHEMA_VIA_GATEWAY",
                execution_time_ms=0,
                row_count=0,
                success=False,
                error=str(e)
            )
            
            return {
                "error": f"Schema loading failed: {str(e)}",
                "tables": [],
                "metadata_source": "ERROR_NO_FALLBACK",
                "message": "Configuration required - no fallback available"
            }