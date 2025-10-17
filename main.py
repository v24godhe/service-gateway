from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import json
from models.schemas import (TrackingRequest, TrackingResponse, 
                           CustomerByIdRequest, CustomerSearchRequest, CustomerResponse)
from services.order_service import OrderService
from services.customer_service import CustomerService
from database.styr_connector import StyrDatabaseConnector
from utils.auth import verify_auth_token
from utils.response_formatter import response_formatter
from utils.request_middleware import RequestTrackingMiddleware
from utils.fallback import fallback_manager
from utils.health_monitor import health_monitor
from utils.audit_logger import audit_logger
from utils.audit_middleware import AuditMiddleware
from datetime import datetime
import time
from typing import List
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from utils.error_handler import error_handler
from utils.input_sanitizer import input_sanitizer

from services.query_service import QueryService
from utils.user_manager import get_user
from models.query_schemas import DynamicQueryRequest, DynamicQueryResponse
from utils.query_validator import query_validator
from services.query_learning_service import QueryLearningService
import os
from services.conversation_memory_service import conversation_manager
import re

from fastapi.responses import StreamingResponse
from services.export_service import ExportService
import logging

from services.permission_management_service import PermissionManagementService
from typing import List
from fastapi import Body, Header
import pyodbc
from pydantic import BaseModel
from typing import List

from typing import Optional
from datetime import datetime

class ColumnMetadataExtended(BaseModel):
    column_name: str
    friendly_name: str
    is_visible: bool = True
    data_type: Optional[str] = None
    max_length: Optional[int] = None
    numeric_precision: Optional[int] = None
    numeric_scale: Optional[int] = None
    is_nullable: bool = True
    default_value: Optional[str] = None
    column_description: Optional[str] = None
    data_classification: str = 'INTERNAL'
    contains_pii: bool = False
    pii_type: Optional[str] = None

class TableRelationship(BaseModel):
    column_name: str
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_table: Optional[str] = None
    foreign_column: Optional[str] = None
    relationship_type: Optional[str] = None
    is_indexed: bool = False
    index_type: Optional[str] = None

class ExtendedMetadataSaveRequest(BaseModel):
    system_id: str
    table_name: str
    table_friendly_name: Optional[str] = None
    table_description: Optional[str] = None
    contains_pii: bool = False
    gdpr_category: Optional[str] = None
    data_sensitivity: str = 'INTERNAL'
    columns: List[ColumnMetadataExtended]
    relationships: Optional[List[TableRelationship]] = []
    created_by: str


SYSTEM_CONFIGS = {
    'STYR': {
        'dsn': 'STYR_CONNECTION',
        'password': 'FS25_AS10'
    },
    'JEEVES': {
        'type': 'DB2',
        'dsn': 'JEEVES_DSN',  # You'll provide
        'password': 'PASSWORD'  # You'll provide
    },
    'ASTRO': {
        'type': 'MSSQL',
        'dsn': 'ASTRO_DSN',  # You'll provide
        'password': 'PASSWORD'  # You'll provide
    },
    'FSIAH': {
        'type': 'MSSQL',
        'server': 'FSDHWFP01\\SQLEXPRESS',
        'database': 'query_learning_db', 
        'trusted_connection': True
    }
}

def get_db_connection(system_id: str):
    """Get database connection for specified system"""
    config = SYSTEM_CONFIGS.get(system_id.upper())
    if not config:
        raise ValueError(f"Unknown system: {system_id}")
    
    # Handle MSSQL with Windows Auth
    if config.get('type') == 'MSSQL' and config.get('trusted_connection'):
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={config['server']};DATABASE={config['database']};Trusted_Connection=yes"
        return pyodbc.connect(conn_str)
    else:
        # Handle DSN connections for other systems
        return pyodbc.connect(f"DSN={config['dsn']};PWD={config['password']}")


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

QUERY_LEARNING_ENABLED = os.getenv("QUERY_CACHE_ENABLED", "true").lower() == "true"
export_service = ExportService()

app = FastAPI(title="Service Gateway", version="1.0.0")
app.add_exception_handler(HTTPException, error_handler.http_exception_handler)
app.add_exception_handler(StarletteHTTPException, error_handler.http_exception_handler)
app.add_exception_handler(RequestValidationError, error_handler.validation_exception_handler)
app.add_exception_handler(Exception, error_handler.general_exception_handler)

# Add request tracking middleware
app.add_middleware(RequestTrackingMiddleware)

# Database setup
db_connector = StyrDatabaseConnector()
order_service = OrderService(db_connector)
customer_service = CustomerService(db_connector)
query_service = QueryService(db_connector,)


# Initialize Query Learning Service with Windows Auth
def init_query_learning():
    if not QUERY_LEARNING_ENABLED:
        return None
    
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={os.getenv('QUERY_LEARNING_DB_SERVER', 'FSDHWFP01\\SQLEXPRESS')};"
            f"DATABASE={os.getenv('QUERY_LEARNING_DB_DATABASE', 'query_learning_db')};"
            f"Trusted_Connection=yes;"
        )
        return QueryLearningService(conn_str)
    except Exception as e:
        print(f"Warning: Query Learning Service initialization failed: {e}")
        return None

query_learning_service = init_query_learning()

# Initialize Permission Management Service
def init_permission_management():
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={os.getenv('QUERY_LEARNING_DB_SERVER', 'FSDHWFP01\\SQLEXPRESS')};"
            f"DATABASE={os.getenv('QUERY_LEARNING_DB_DATABASE', 'query_learning_db')};"
            f"Trusted_Connection=yes;"
        )
        return PermissionManagementService(conn_str)
    except Exception as e:
        print(f"Warning: Permission Management Service initialization failed: {e}")
        return None

permission_service = init_permission_management()

def _is_followup_question(question: str) -> bool:
    """Detect if question is a follow-up based on pronouns and references"""
    question_lower = question.lower()
    
    # Pronouns and references that indicate follow-up
    followup_indicators = [
        'their', 'his', 'her', 'its', 'this', 'that', 'these', 'those',
        'the same', 'same customer', 'same order', 'also', 'too',
        'what about', 'how about', 'and', 'for them', 'for him', 'for her',
        'it', 'they', 'from above', 'previous', 'last one'
    ]
    
    return any(indicator in question_lower for indicator in followup_indicators)


def _extract_tables_from_sql(sql: str) -> List[str]:
    """Extract table names from SQL query"""
    tables = []
    sql_upper = sql.upper()
    
    # Simple table extraction
    table_list = [
        "DCPO.KHKNDHUR", "DCPO.OHKORDHR", "DCPO.ORKORDRR",
        "DCPO.KRKFAKTR", "DCPO.KIINBETR", "DCPO.LHLEVHUR",
        "DCPO.AHARTHUR", "EGU.AYARINFR", "EGU.WSOUTSAV",
        "DCPO.IHIORDHR", "DCPO.IRIORDRR"
    ]
    
    for table in table_list:
        if table in sql_upper:
            tables.append(table)
    
    return tables

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://10.200.0.1:8000"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.get("/audit/stats")
async def get_audit_stats(request: Request, hours: int = 24):
    """Get audit statistics"""
    stats = audit_logger.get_audit_stats(hours)
    return response_formatter.success_response(
        data=stats,
        message="Audit statistics retrieved",
        request_id=getattr(request.state, 'request_id', None)
    ).dict()

@app.on_event("startup")
async def startup():
    await db_connector.connect()
    audit_logger.api_logger.info(json.dumps({
        "event": "service_startup",
        "timestamp": datetime.now().isoformat(),
        "service": "Service Gateway"
    }))

@app.on_event("shutdown")
async def shutdown():
    await db_connector.disconnect()
    audit_logger.api_logger.info(json.dumps({
        "event": "service_shutdown", 
        "timestamp": datetime.now().isoformat(),
        "service": "Service Gateway"
    }))

@app.get("/health")
async def health_check(request: Request):
    db_healthy = await db_connector.health_check()
    fallback_status = fallback_manager.get_status()
    
    return response_formatter.success_response(
        data={
            "database": "connected" if db_healthy else "disconnected",
            "fallback_manager": fallback_status,
            "service_status": "healthy" if db_healthy else "degraded"
        },
        message="Health check completed",
        request_id=getattr(request.state, 'request_id', None)
    ).dict()

@app.get("/fallback/status")
async def get_fallback_status():
    return fallback_manager.get_status()

@app.post("/api/tracking", response_model=TrackingResponse)
async def get_tracking_data(
    request: Request,
    tracking_request: TrackingRequest,
    auth_user=Depends(verify_auth_token)
):
    return await order_service.get_order_data(tracking_request)

@app.post("/api/customer")
async def get_customer_by_id(
    request: Request,
    customer_request: CustomerByIdRequest,
    auth_user=Depends(verify_auth_token)
):
    sanitized_data = input_sanitizer.sanitize_request_data(customer_request.dict())
    sanitized_request = CustomerByIdRequest(**sanitized_data)
    return await customer_service.get_customer_by_id(
        sanitized_request, 
        getattr(request.state, 'request_id', None)
    )

@app.post("/api/customer/search")
async def search_customers(
    request: Request,
    search_request: CustomerSearchRequest,
    auth_user=Depends(verify_auth_token)
):
    sanitized_data = input_sanitizer.sanitize_request_data(search_request.dict())
    sanitized_request = CustomerSearchRequest(**sanitized_data)  # ✅ FIXED
    return await customer_service.search_customers(
        sanitized_request,
        getattr(request.state, 'request_id', None)
    )




@app.post("/api/{system_id}/execute-query")
async def execute_query_multi(system_id: str, request: dict):
    """Execute query on any system"""
    try:
        conn = get_db_connection(system_id)
        cursor = conn.cursor()
        cursor.execute(request['query'])
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        return {
            "success": True,
            "data": [dict(zip(columns, row)) for row in results],
            "system_id": system_id
        }
    except Exception as e:
        raise HTTPException(500, str(e))
    


@app.post("/api/execute-query-fsiah")
async def execute_query_fsiah(query_request: dict, request: Request):
    """Execute query specifically on FSIAH system"""
    try:
        username = request.headers.get("X-Username")
        if not username:
            raise HTTPException(status_code=401, detail="Username required")
        
        conn = get_db_connection("FSIAH")
        cursor = conn.cursor()
        cursor.execute(query_request['query'])
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        return {
            "success": True,
            "data": [dict(zip(columns, row)) for row in results],
            "system_id": "FSIAH"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'conn' in locals():
            conn.close()


@app.post("/api/execute-query")
async def execute_query(query_request: DynamicQueryRequest, request: Request, system_id: str = "STYR"):
    """Execute dynamic SQL query with conversation memory"""
    print("Query captured at API:", query_request.query)
    
    # Get username from header
    username = request.headers.get("X-Username")
    if not username:
        raise HTTPException(status_code=401, detail="Username required")
    
    # Get user info
    user = get_user(username)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid user")
    
    user_role = user.role.value
    question = query_request.query
    session_id = getattr(request.state, 'request_id', None)
    
    # Get or create conversation session
    conversation = conversation_manager.get_or_create_session(session_id, username)
    
    # Validate user access
    has_access, access_error = query_validator.validate_user_access(query_request.query, user)
    
    if not has_access:
        raise HTTPException(status_code=403, detail=access_error)

    # Add user message to conversation history
    conversation.add_message("user", question)
    
    # Detect if follow-up question
    is_followup = _is_followup_question(question)
    
    start_time = time.time()
    
    try:
        # Step 1: Try cache first
        cached_result = None
        if query_learning_service and not is_followup:
            cached_result = query_learning_service.get_cached_query(question, user_role)
        
        if cached_result:
            execution_time_ms = int((time.time() - start_time) * 1000)
            conversation.add_message("assistant", f'Retrieved {len(cached_result.get("result_json", {}).get("rows", []))} cached results.')
            
            return {
                "success": True,
                "data": cached_result.get("result_json", {}),
                "sql": cached_result.get("sql_query", ""),
                "source": "cache",
                "execution_time_ms": execution_time_ms,
                "is_followup": is_followup
            }
        
        # Step 2: Get system-specific connector
        from database.connector_factory import DatabaseConnectorFactory
        db_connector = DatabaseConnectorFactory.get_connector(system_id)
        
        # Create query service with this connector
        query_service_temp = QueryService(db_connector)
        
        # Step 3: Execute query using temporary service
        result = await query_service_temp.execute_dynamic_query(
            query_request, 
            session_id, 
            username
        )
        
        execution_time_ms = int((time.time() - start_time) * 1000)
        success = result.get("success", False)
        
        if success:
            tables_used = ', '.join(_extract_tables_from_sql(result.get("sql", "")))
            row_count = len(result.get("data", {}).get("rows", []))
            
            conversation.add_message(
                "assistant",
                f'Query executed successfully. Found {row_count} results.',
                {'sql': result.get("sql", ""), 'tables': tables_used}
            )
        
        # Step 4: Log query execution
        if query_learning_service:
            try:
                sql_generated = result.get("sql", "")
                row_count = len(result.get("data", {}).get("rows", []))
                error_message = result.get("message") if not success else None
                
                query_learning_service.log_query(
                    user_id=username,
                    user_role=user_role,
                    question=question,
                    sql_generated=sql_generated,
                    execution_time_ms=execution_time_ms,
                    success=success,
                    error_message=error_message,
                    row_count=row_count,
                    session_id=session_id
                )
                
                # Step 5: Cache successful results
                if success and row_count > 0:
                    query_learning_service.save_to_cache(
                        question=question,
                        user_role=user_role,
                        sql_query=sql_generated,
                        result_data=result.get("data", {}),
                        ttl_minutes=int(os.getenv("QUERY_CACHE_TTL_MINUTES", 60))
                    )
                
                # Step 6: Update performance metrics
                query_learning_service.update_performance(
                    question=question,
                    user_role=user_role,
                    execution_time_ms=execution_time_ms
                )
            except Exception as e:
                print(f"Query learning service error: {e}")
        
        result["source"] = "database"
        result["is_followup"] = is_followup
        result["system_id"] = system_id  # Add system_id to response
        return result
        
    except Exception as e:
        execution_time_ms = int((time.time() - start_time) * 1000)
        
        # Log failed query
        if query_learning_service:
            try:
                query_learning_service.log_query(
                    user_id=username,
                    user_role=user_role,
                    question=question,
                    execution_time_ms=execution_time_ms,
                    success=False,
                    error_message=str(e),
                    session_id=session_id
                )
            except:
                pass
        
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/clear-cache")
async def clear_expired_cache(request: Request):
    """Manually clear expired cache entries (Admin only)"""
    
    if not query_learning_service:
        raise HTTPException(status_code=503, detail="Query learning is disabled")
    
    username = request.headers.get("X-Username")
    if not username:
        raise HTTPException(status_code=401, detail="Username required")
    
    # Only allow CEO to clear cache
    user = get_user(username)
    if not user or user.role.value != "ceo":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        deleted_count = query_learning_service.clean_expired_cache()
        return {
            "success": True,
            "deleted_entries": deleted_count,
            "message": f"Cleared {deleted_count} expired cache entries"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/schema")
async def get_database_schema(
    request: Request,
    auth_user=Depends(verify_auth_token)
):
    """Get database schema metadata for AI query generation"""
    return await query_service.get_database_schema(
        getattr(request.state, 'request_id', None)
    )


@app.post("/api/export-pdf")
async def export_query_to_pdf(query_request: DynamicQueryRequest, request: Request):
    """Export query results to PDF"""
    
    username = request.headers.get("X-Username")
    if not username:
        raise HTTPException(status_code=401, detail="Username required")
    
    user = get_user(username)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid user")
    
    try:
        # Execute query to get data
        result = await query_service.execute_dynamic_query(
            query_request,
            getattr(request.state, 'request_id', None)
        )
        
        if not result.get("success"):
            raise HTTPException(status_code=400, detail="Query execution failed")
        
        # Get data
        data = result.get("data", {}).get("rows", [])
        
        if not data:
            raise HTTPException(status_code=404, detail="No data to export")
        
        # Generate PDF
        pdf_buffer = export_service.export_to_pdf(
            data=data,
            title="Query Results",
            user_name=username.upper(),
            query=None  
        )
        
        # Return as downloadable file
        filename = f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        
        return StreamingResponse(
            pdf_buffer,
            media_type="application/pdf",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"PDF export failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/export-excel")
async def export_query_to_excel(query_request: DynamicQueryRequest, request: Request):
    """Export query results to Excel"""
    
    username = request.headers.get("X-Username")
    if not username:
        raise HTTPException(status_code=401, detail="Username required")
    
    user = get_user(username)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid user")
    
    try:
        # Execute query to get data
        result = await query_service.execute_dynamic_query(
            query_request,
            getattr(request.state, 'request_id', None)
        )
        
        if not result.get("success"):
            raise HTTPException(status_code=400, detail="Query execution failed")
        
        # Get data
        data = result.get("data", {}).get("rows", [])
        
        if not data:
            raise HTTPException(status_code=404, detail="No data to export")
        
        # Generate Excel
        excel_buffer = export_service.export_to_excel(
            data=data,
            title=f"Query Results",
            user_name=username.upper(),
            query=None  
        )
        
        # Return as downloadable file
        filename = f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        return StreamingResponse(
            excel_buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Excel export failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/clear-conversation")
async def clear_conversation(request: Request):
    """Clear conversation memory for current session"""
    
    username = request.headers.get("X-Username")
    if not username:
        raise HTTPException(status_code=401, detail="Username required")
    
    session_id = getattr(request.state, 'request_id', None)
    
    conversation_manager.clear_session(session_id)
    
    return {
        "success": True,
        "message": "Conversation memory cleared"
    }


# ADD NEW ENDPOINT: Get conversation context

@app.get("/api/conversation-context")
async def get_conversation_context(request: Request):
    """Get current conversation context"""
    
    username = request.headers.get("X-Username")
    if not username:
        raise HTTPException(status_code=401, detail="Username required")
    
    session_id = getattr(request.state, 'request_id', None)
    
    conversation = conversation_manager.get_or_create_session(session_id, username)
    
    return {
        "session_id": session_id,
        "message_count": len(conversation.messages),
        "entities": conversation.entities,
        "last_query": conversation.last_query,
        "last_tables": conversation.last_tables_used,
        "context": conversation.get_context_for_query()
    }


# ========== DATABASE CONVERSATION MEMORY ENDPOINTS ==========

def get_memory_connection_string():
    """Get connection string for memory database"""
    return (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.getenv('QUERY_LEARNING_DB_SERVER', 'FSDHWFP01\\\\SQLEXPRESS')};"
        f"DATABASE={os.getenv('QUERY_LEARNING_DB_DATABASE', 'query_learning_db')};"
        f"Trusted_Connection=yes;"
    )

@app.post("/api/conversation/create-session")
async def create_conversation_session(
    session_id: str = Body(...),
    user_id: str = Body(...),
    metadata: dict = Body(None)
):
    """Create a new conversation session in database"""
    try:
        from services.persistent_memory_service import PersistentMemoryService
        conn_str = get_memory_connection_string()
        db_service = PersistentMemoryService(conn_str)
        
        # Use create_or_get_session (not create_session)
        success = db_service.create_or_get_session(
            session_id=session_id,
            user_id=user_id
        )
        
        return {
            "success": success,
            "session_id": session_id,
            "message": "Session created successfully" if success else "Failed to create session"
        }
        
    except Exception as e:
        logger.error(f"Create session failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/conversation/save-message")
async def save_conversation_message(
    session_id: str = Body(...),
    message_type: str = Body(...),
    message_content: str = Body(...),
    message_metadata: str = Body(None)
):
    """Save a message to database"""
    try:
        from services.persistent_memory_service import PersistentMemoryService
        conn_str = get_memory_connection_string()
        db_service = PersistentMemoryService(conn_str)
        
        # Convert metadata from string to dict if provided
        metadata_dict = json.loads(message_metadata) if message_metadata else None
        
        # Use correct parameter name 'content' not 'message_content'
        success = db_service.save_message(
            session_id=session_id,
            message_type=message_type,
            content=message_content,
            metadata=metadata_dict
        )
        
        return {
            "success": success,
            "message": "Message saved successfully" if success else "Failed to save message"
        }
        
    except Exception as e:
        logger.error(f"Save message failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/conversation/get-messages/{session_id}")
async def get_conversation_messages(session_id: str):
    """Get all messages for a session from database"""
    try:
        from services.persistent_memory_service import PersistentMemoryService
        conn_str = get_memory_connection_string()
        db_service = PersistentMemoryService(conn_str)
        
        # Use get_conversation_history (not get_session_messages)
        messages = db_service.get_conversation_history(session_id)
        
        # Convert to expected format
        formatted_messages = []
        for msg in messages:
            formatted_messages.append({
                "message_type": msg["role"],
                "message_content": msg["content"],
                "timestamp": msg["timestamp"],
                "metadata": msg["metadata"]
            })
        
        return {
            "success": True,
            "session_id": session_id,
            "message_count": len(formatted_messages),
            "messages": formatted_messages
        }
        
    except Exception as e:
        logger.error(f"Get messages failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/conversation/update-context")
async def update_conversation_context(
    session_id: str = Body(...),
    last_query: str = Body(...),
    last_sql: str = Body(None),
    last_tables_used: str = Body(None),
    result_count: int = Body(0)
):
    """Update conversation context in database"""
    try:
        from services.persistent_memory_service import PersistentMemoryService
        conn_str = get_memory_connection_string()
        db_service = PersistentMemoryService(conn_str)
        
        # Convert tables string to list
        tables_list = last_tables_used.split(",") if last_tables_used else None
        
        # Use update_context (not update_conversation_context)
        success = db_service.update_context(
            session_id=session_id,
            query=last_query,
            sql=last_sql,
            tables=tables_list,
            result_count=result_count
        )
        
        return {
            "success": success,
            "message": "Context updated successfully" if success else "Failed to update context"
        }
        
    except Exception as e:
        logger.error(f"Update context failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/conversation/clear-session/{session_id}")
async def clear_conversation_session(session_id: str):
    """Clear all messages for a session from database"""
    try:
        from services.persistent_memory_service import PersistentMemoryService
        conn_str = get_memory_connection_string()
        db_service = PersistentMemoryService(conn_str)
        
        # Use clear_session (this method exists)
        success = db_service.clear_session(session_id)
        
        return {
            "success": success,
            "session_id": session_id,
            "message": "Session cleared successfully" if success else "Failed to clear session"
        }
        
    except Exception as e:
        logger.error(f"Clear session failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class ColumnMetadata(BaseModel):
    column_name: str
    friendly_name: str
    is_visible: bool

class MetadataSaveRequest(BaseModel):
    system_id: str
    table_name: str
    columns: List[ColumnMetadata]
    created_by: str

   
# ========== END DATABASE CONVERSATION MEMORY ENDPOINTS ==========

@app.get("/api/system/test-connection")
async def test_system_connection(system_id: str):
    """Test database connection for a system"""
    
    try:
        # Load connection params from environment
        system_var = f"{system_id}_SYSTEM"
        userid_var = f"{system_id}_USERID"
        password_var = f"{system_id}_PASSWORD"
        driver_var = f"{system_id}_DRIVER"
        
        system = os.getenv(system_var)
        userid = os.getenv(userid_var)
        password = os.getenv(password_var)
        driver = os.getenv(driver_var, "IBM i Access ODBC Driver")
        
        if not system or not userid or not password:
            return {
                "success": False,
                "message": f"Missing connection params in .env: {system_var}, {userid_var}, {password_var}"
            }
        
        # Try to connect
        conn_str = f"DRIVER={{{driver}}};SYSTEM={system};UID={userid};PWD={password}"
        
        try:
            conn = pyodbc.connect(conn_str, timeout=5)
            conn.close()
            
            return {
                "success": True,
                "message": f"Successfully connected to {system_id} ({system})"
            }
        except pyodbc.Error as e:
            return {
                "success": False,
                "message": f"Connection failed: {str(e)}"
            }
    
    except Exception as e:
        return {
            "success": False,
            "message": f"Test error: {str(e)}"
        }

# Helth Checking

@app.get("/health")
async def basic_health_check(request: Request):
    """Basic health check for load balancers"""
    db_healthy = await db_connector.health_check()
    
    return response_formatter.success_response(
        data={
            "status": "healthy" if db_healthy else "degraded",
            "database": "connected" if db_healthy else "disconnected"
        },
        message="Basic health check completed",
        request_id=getattr(request.state, 'request_id', None)
    ).dict()

@app.get("/health/comprehensive")
async def comprehensive_health_check(request: Request):
    """Comprehensive health check with detailed metrics"""
    health_data = await health_monitor.comprehensive_health_check(db_connector)
    
    return response_formatter.success_response(
        data=health_data,
        message="Comprehensive health check completed",
        request_id=getattr(request.state, 'request_id', None)
    ).dict()

@app.get("/health/database")
async def database_health_check(request: Request):
    """Dedicated database health check"""
    db_health = await health_monitor._check_database_health(db_connector)
    
    return response_formatter.success_response(
        data=db_health,
        message="Database health check completed",
        request_id=getattr(request.state, 'request_id', None)
    ).dict()

@app.get("/health/history")
async def get_health_history(request: Request, minutes: int = 60):
    """Get health check history"""
    history = health_monitor.get_health_history(minutes)
    
    return response_formatter.success_response(
        data={
            "history": history,
            "period_minutes": minutes,
            "total_entries": len(history)
        },
        message=f"Retrieved {len(history)} health entries from last {minutes} minutes",
        request_id=getattr(request.state, 'request_id', None)
    ).dict()

@app.get("/health/summary")
async def get_health_summary(request: Request):
    """Get health summary statistics"""
    summary = health_monitor.get_health_summary()
    
    return response_formatter.success_response(
        data=summary,
        message="Health summary retrieved",
        request_id=getattr(request.state, 'request_id', None)
    ).dict()

@app.get("/health/circuit-breaker")
async def get_circuit_breaker_status(request: Request):
    """Get circuit breaker status"""
    circuit_status = health_monitor._check_circuit_breaker_health()
    
    return response_formatter.success_response(
        data=circuit_status,
        message="Circuit breaker status retrieved",
        request_id=getattr(request.state, 'request_id', None)
    ).dict()

# -------------------- Super Admin Authentication --------------------

@app.post("/api/admin/login")
async def admin_login(
    username: str = Body(...),
    password: str = Body(...)  # You can add password validation later
):
    """
    Super admin login
    For now, just checks if user is super admin
    TODO: Add password authentication
    """
    if not permission_service:
        raise HTTPException(status_code=500, detail="Permission service unavailable")
    
    is_admin = permission_service.is_super_admin(username)
    
    if not is_admin:
        raise HTTPException(status_code=403, detail="Not authorized as super admin")
    
    # Update last login
    permission_service.update_admin_last_login(username)
    
    return {
        "success": True,
        "username": username,
        "message": "Admin login successful"
    }


@app.get("/api/admin/check/{username}")
async def check_admin_status(username: str):
    """Check if a user is a super admin"""
    if not permission_service:
        raise HTTPException(status_code=500, detail="Permission service unavailable")
    
    is_admin = permission_service.is_super_admin(username)
    
    return {"is_admin": is_admin}


# -------------------- Permission Requests --------------------

@app.post("/api/permission-request")
async def create_permission_request(
    user_id: str = Body(...),
    user_role: str = Body(...),
    requested_table: str = Body(...),
    original_question: str = Body(...),
    blocked_sql: str = Body(None),
    requested_columns: List[str] = Body(None),
    justification: str = Body(None)
):
    """
    Create a new permission request
    Called when user tries to access restricted data
    """
    if not permission_service:
        raise HTTPException(status_code=500, detail="Permission service unavailable")
    
    request_id = permission_service.create_permission_request(
        user_id=user_id,
        user_role=user_role,
        requested_table=requested_table,
        original_question=original_question,
        requested_columns=requested_columns,
        blocked_sql=blocked_sql,
        justification=justification
    )
    
    if request_id:
        return {
            "success": True,
            "request_id": request_id,
            "message": "Permission request created. An administrator will review it shortly."
        }
    else:
        raise HTTPException(status_code=500, detail="Failed to create permission request")


@app.get("/api/permission-requests/pending")
async def get_pending_requests(admin_username: str = Header(None, alias="X-Admin-Username")):
    """
    Get all pending permission requests
    Super admin only
    """
    if not permission_service:
        raise HTTPException(status_code=500, detail="Permission service unavailable")
    
    # Verify admin
    if not admin_username or not permission_service.is_super_admin(admin_username):
        raise HTTPException(status_code=403, detail="Super admin access required")
    
    requests = permission_service.get_pending_requests()
    
    return {
        "pending_count": len(requests),
        "requests": requests
    }


@app.get("/api/permission-requests/all")
async def get_all_requests(
    status: str = None,
    limit: int = 100,
    admin_username: str = Header(None, alias="X-Admin-Username")
):
    """
    Get all permission requests with optional status filter
    Super admin only
    """
    if not permission_service:
        raise HTTPException(status_code=500, detail="Permission service unavailable")
    
    # Verify admin
    if not admin_username or not permission_service.is_super_admin(admin_username):
        raise HTTPException(status_code=403, detail="Super admin access required")
    
    requests = permission_service.get_all_requests(status=status, limit=limit)
    
    return {
        "total": len(requests),
        "status_filter": status,
        "requests": requests
    }


@app.post("/api/permission-requests/{request_id}/approve")
async def approve_request(
    request_id: int,
    review_notes: str = Body(None),
    temporary: bool = Body(False),
    days_valid: int = Body(30),
    admin_username: str = Header(..., alias="X-Admin-Username")
):
    """
    Approve a permission request
    Super admin only
    """
    if not permission_service:
        raise HTTPException(status_code=500, detail="Permission service unavailable")
    
    # Verify admin
    if not permission_service.is_super_admin(admin_username):
        raise HTTPException(status_code=403, detail="Super admin access required")
    
    # Calculate expiration if temporary
    expires_at = None
    if temporary:
        from datetime import datetime, timedelta
        expires_at = datetime.now() + timedelta(days=days_valid)
    
    success = permission_service.approve_permission_request(
        request_id=request_id,
        admin_username=admin_username,
        review_notes=review_notes,
        expires_at=expires_at
    )
    
    if success:
        return {
            "success": True,
            "message": "Permission request approved and RBAC rules updated",
            "temporary": temporary,
            "expires_at": expires_at.isoformat() if expires_at else None
        }
    else:
        raise HTTPException(status_code=500, detail="Failed to approve request")


@app.post("/api/permission-requests/{request_id}/deny")
async def deny_request(
    request_id: int,
    review_notes: str = Body(...),
    admin_username: str = Header(..., alias="X-Admin-Username")
):
    """
    Deny a permission request
    Super admin only
    """
    if not permission_service:
        raise HTTPException(status_code=500, detail="Permission service unavailable")
    
    # Verify admin
    if not permission_service.is_super_admin(admin_username):
        raise HTTPException(status_code=403, detail="Super admin access required")
    
    success = permission_service.deny_permission_request(
        request_id=request_id,
        admin_username=admin_username,
        review_notes=review_notes
    )
    
    if success:
        return {
            "success": True,
            "message": "Permission request denied"
        }
    else:
        raise HTTPException(status_code=500, detail="Failed to deny request")


# -------------------- RBAC Management --------------------

@app.get("/api/rbac/rules/{role}")
async def get_rbac_rules(role: str):
    """
    Get RBAC rules for a specific role
    Returns allowed tables and column restrictions
    """
    if not permission_service:
        raise HTTPException(status_code=500, detail="Permission service unavailable")
    
    rules = permission_service.get_rbac_rules_for_role(role)
    
    return rules


@app.get("/api/rbac/rules")
async def get_all_rbac_rules(admin_username: str = Header(None, alias="X-Admin-Username")):
    """
    Get all RBAC rules for all roles
    Super admin only
    """
    if not permission_service:
        raise HTTPException(status_code=500, detail="Permission service unavailable")
    
    # Verify admin
    if not admin_username or not permission_service.is_super_admin(admin_username):
        raise HTTPException(status_code=403, detail="Super admin access required")
    
    rules = permission_service.get_all_rbac_rules()
    
    return {"roles": rules}


@app.post("/api/rbac/add-rule")
async def add_rbac_rule(
    user_role: str = Body(...),
    table_name: str = Body(...),
    allowed_columns: List[str] = Body(None),
    blocked_columns: List[str] = Body(None),
    notes: str = Body(None),
    admin_username: str = Header(..., alias="X-Admin-Username"),
    auto_request: bool = Body(False)  # NEW: Allow auto-requests
):
    """
    Add or update an RBAC rule
    Enhanced to support auto-permission requests
    """
    if not permission_service:
        raise HTTPException(status_code=500, detail="Permission service unavailable")
    
    # Allow auto-requests or require admin verification
    if not auto_request:
        # Verify admin for manual requests
        if not permission_service.is_super_admin(admin_username):
            raise HTTPException(status_code=403, detail="Super admin access required")
    else:
        # For auto-requests, use system admin
        admin_username = "AUTO_SYSTEM"
        notes = f"[AUTO-REQUEST] {notes}" if notes else "[AUTO-REQUEST] Generated by AI analysis"
    
    success = permission_service.add_rbac_rule(
        user_role=user_role,
        table_name=table_name,
        admin_username=admin_username,
        allowed_columns=allowed_columns,
        blocked_columns=blocked_columns,
        notes=notes
    )
    
    if success:
        # Log the permission request
        if query_learning_service:
            query_learning_service.log_query(
                user_id=admin_username,
                user_role="ADMIN",
                question=f"[PERMISSION_GRANT] Added rule for {user_role} on {table_name}",
                sql_generated=f"Tables: {table_name}, Auto: {auto_request}",
                execution_time_ms=0,
                success=True,
                session_id=f"perm_{user_role}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )
        
        return {
            "success": True,
            "message": f"RBAC rule updated for {user_role} on {table_name}",
            "auto_request": auto_request,
            "request_id": 1 if auto_request else None  # Placeholder ID for auto-requests
        }
    else:
        raise HTTPException(status_code=500, detail="Failed to add RBAC rule")

# -------------------- Statistics & Monitoring --------------------

@app.get("/api/permission-stats")
async def get_permission_stats(admin_username: str = Header(..., alias="X-Admin-Username")):
    """
    Get permission management statistics
    Super admin only
    """
    if not permission_service:
        raise HTTPException(status_code=500, detail="Permission service unavailable")
    
    # Verify admin
    if not permission_service.is_super_admin(admin_username):
        raise HTTPException(status_code=403, detail="Super admin access required")
    
    stats = permission_service.get_permission_stats()
    
    return stats

@app.get("/api/{system_id}/schema")
async def get_system_schema(system_id: str):
    """Get database schema for a system"""
    
    if system_id != "STYR":
        raise HTTPException(status_code=404, detail="System not found")
    
    try:
        # Use same connection method as StyrDatabaseConnector
        connection_string = f"""
        DRIVER={{IBM i Access ODBC Driver}};
        SYSTEM={os.getenv('STYR_SYSTEM')};
        USERID={os.getenv('STYR_USERID')};
        PASSWORD={os.getenv('STYR_PASSWORD')};
        ALLOWPROCCALLS=1;
        SORTTABLE=1;
        """
        
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        # Get tables and columns
        cursor.execute("""
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME,
                COLUMN_NAME,
                DATA_TYPE
            FROM QSYS2.SYSCOLUMNS
            WHERE TABLE_SCHEMA IN ('DCPO', 'EGU')
            ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
        """)
        
        # Group by table
        schema = {}
        for row in cursor.fetchall():
            table_key = f"{row[0]}.{row[1]}"
            if table_key not in schema:
                schema[table_key] = []
            schema[table_key].append({
                "column_name": row[2],
                "data_type": row[3]
            })
        
        cursor.close()
        conn.close()
        
        return {
            "system_id": system_id,
            "tables": schema
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/metadata/save")
async def save_metadata(request: MetadataSaveRequest):
    """Save table metadata configuration"""
    
    try:

        conn = get_db_connection("FSIAH")
        cursor = conn.cursor()
        
        saved_count = 0
        for col in request.columns:
            # Check if exists
            cursor.execute("""
                SELECT id FROM table_metadata_config
                WHERE system_id = ? AND table_name = ? AND column_name = ?
            """, (request.system_id, request.table_name, col.column_name))
            
            existing = cursor.fetchone()
            
            if existing:
                # Update
                cursor.execute("""
                    UPDATE table_metadata_config
                    SET friendly_name = ?, is_visible = ?, modified_by = ?, modified_at = GETDATE()
                    WHERE id = ?
                """, (col.friendly_name, col.is_visible, request.created_by, existing[0]))
            else:
                # Insert
                cursor.execute("""
                    INSERT INTO table_metadata_config
                    (system_id, table_name, column_name, friendly_name, is_visible, created_by)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (request.system_id, request.table_name, col.column_name, 
                      col.friendly_name, col.is_visible, request.created_by))
            
            saved_count += 1
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "saved_count": saved_count,
            "message": f"Saved {saved_count} columns for {request.table_name}"
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@app.get("/api/{system_id}/schema-with-rbac")
async def get_schema_with_rbac(
    system_id: str,
    user_role: str,
    authorization: str = Header(None)
):
    """
    Get schema with RBAC filtering and friendly names
    Returns only tables/columns user can access with metadata
    """
    # Auth check
    if not verify_auth_token(authorization):
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        # Get RBAC rules for user role
        rbac_rules = permission_service.get_rbac_rules_for_role(user_role)
        allowed_tables = rbac_rules.get('tables', [])
        restrictions = rbac_rules.get('restrictions', {})
        
        if not allowed_tables:
            return {"schema": {}, "message": "No tables accessible for this role"}
        
        # Get metadata configuration
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={os.getenv('QUERY_LEARNING_DB_SERVER', 'FSDHWFP01\\SQLEXPRESS')};"
            f"DATABASE=query_learning_db;Trusted_Connection=yes;"
        )
        cursor = conn.cursor()
        
        # Build schema with metadata
        schema = {}
        for table_name in allowed_tables:
            # Get metadata for this table
            cursor.execute("""
                SELECT column_name, friendly_name, is_visible
                FROM table_metadata_config
                WHERE system_id = ? AND table_name = ? AND is_visible = 1
                ORDER BY column_name
            """, (system_id, table_name))
            
            columns = cursor.fetchall()
            
            if not columns:
                # No metadata configured
                schema[table_name] = {
                    'status': 'pending_configuration',
                    'message': 'Table metadata configuration pending from admin'
                }
                continue
            
            # Apply column-level RBAC
            table_restrictions = restrictions.get(table_name, {})
            allowed_columns = table_restrictions.get('allowed_columns')
            blocked_columns = table_restrictions.get('blocked_columns')
            
            filtered_columns = []
            for col in columns:
                col_name = col[0]
                
                # Check column RBAC
                if allowed_columns and col_name not in allowed_columns:
                    continue
                if blocked_columns and col_name in blocked_columns:
                    continue
                
                filtered_columns.append({
                    'column_name': col_name,
                    'friendly_name': col[1],
                    'is_visible': col[2]
                })
            
            if filtered_columns:
                schema[table_name] = {
                    'status': 'configured',
                    'columns': filtered_columns
                }
        
        cursor.close()
        conn.close()
        
        return {
            "system_id": system_id,
            "user_role": user_role,
            "schema": schema,
            "total_tables": len(schema)
        }
        
    except Exception as e:
        logger.error(f"Schema RBAC error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/{system_id}/schema-configured")
async def get_configured_tables(system_id: str):
    """
    Get only configured tables from table_master and table_metadata_config
    Used by Admin UI to show configured tables section
    """
    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={os.getenv('QUERY_LEARNING_DB_SERVER', 'FSDHWFP01\\SQLEXPRESS')};"
            f"DATABASE=query_learning_db;Trusted_Connection=yes;"
        )
        cursor = conn.cursor()
        
        # Get configured tables with summary
        cursor.execute("""
            SELECT 
                tm.table_name,
                tm.table_friendly_name,
                tm.table_description,
                tm.contains_pii,
                tm.gdpr_category,
                tm.data_sensitivity,
                tm.configuration_status,
                tm.created_by,
                tm.created_at,
                tm.modified_at,
                COUNT(tmc.id) as column_count,
                SUM(CASE WHEN tmc.is_visible = 1 THEN 1 ELSE 0 END) as visible_columns,
                SUM(CASE WHEN tmc.contains_pii = 1 THEN 1 ELSE 0 END) as pii_columns
            FROM table_master tm
            LEFT JOIN table_metadata_config tmc 
                ON tm.system_id = tmc.system_id AND tm.table_name = tmc.table_name
            WHERE tm.system_id = ? AND tm.is_configured = 1
            GROUP BY 
                tm.table_name, tm.table_friendly_name, tm.table_description,
                tm.contains_pii, tm.gdpr_category, tm.data_sensitivity,
                tm.configuration_status, tm.created_by, tm.created_at, tm.modified_at
            ORDER BY tm.modified_at DESC, tm.created_at DESC
        """, (system_id,))
        
        tables = []
        for row in cursor.fetchall():
            tables.append({
                'table_name': row[0],
                'table_friendly_name': row[1],
                'table_description': row[2],
                'contains_pii': row[3],
                'gdpr_category': row[4],
                'data_sensitivity': row[5],
                'configuration_status': row[6],
                'created_by': row[7],
                'created_at': row[8].isoformat() if row[8] else None,
                'modified_at': row[9].isoformat() if row[9] else None,
                'column_count': row[10],
                'visible_columns': row[11],
                'pii_columns': row[12]
            })
        
        cursor.close()
        conn.close()
        
        return {
            'system_id': system_id,
            'configured_tables': tables,
            'total_count': len(tables)
        }
        
    except Exception as e:
        logger.error(f"Get configured tables error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/metadata/{system_id}/{table_name}")
async def get_table_metadata(system_id: str, table_name: str):
    """
    Get complete metadata for a table including:
    - Table master info
    - All columns with extended metadata
    - Relationships (FK, PK)
    """
    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={os.getenv('QUERY_LEARNING_DB_SERVER', 'FSDHWFP01\\SQLEXPRESS')};"
            f"DATABASE=query_learning_db;Trusted_Connection=yes;"
        )
        cursor = conn.cursor()
        
        # 1. Get table master info
        cursor.execute("""
            SELECT 
                table_friendly_name, table_description, contains_pii,
                gdpr_category, data_sensitivity, configuration_status,
                created_by, created_at, modified_by, modified_at
            FROM table_master
            WHERE system_id = ? AND table_name = ?
        """, (system_id, table_name))
        
        table_row = cursor.fetchone()
        if not table_row:
            raise HTTPException(status_code=404, detail="Table not configured")
        
        table_info = {
            'table_name': table_name,
            'table_friendly_name': table_row[0],
            'table_description': table_row[1],
            'contains_pii': table_row[2],
            'gdpr_category': table_row[3],
            'data_sensitivity': table_row[4],
            'configuration_status': table_row[5],
            'created_by': table_row[6],
            'created_at': table_row[7].isoformat() if table_row[7] else None,
            'modified_by': table_row[8],
            'modified_at': table_row[9].isoformat() if table_row[9] else None
        }
        
        # 2. Get all columns
        cursor.execute("""
            SELECT 
                column_name, friendly_name, is_visible, data_type,
                max_length, numeric_precision, numeric_scale, is_nullable,
                default_value, column_description, data_classification,
                contains_pii, pii_type
            FROM table_metadata_config
            WHERE system_id = ? AND table_name = ?
            ORDER BY column_name
        """, (system_id, table_name))
        
        columns = []
        for row in cursor.fetchall():
            columns.append({
                'column_name': row[0],
                'friendly_name': row[1],
                'is_visible': row[2],
                'data_type': row[3],
                'max_length': row[4],
                'numeric_precision': row[5],
                'numeric_scale': row[6],
                'is_nullable': row[7],
                'default_value': row[8],
                'column_description': row[9],
                'data_classification': row[10],
                'contains_pii': row[11],
                'pii_type': row[12]
            })
        
        # 3. Get relationships
        cursor.execute("""
            SELECT 
                column_name, is_primary_key, is_foreign_key,
                foreign_table, foreign_column, relationship_type,
                is_indexed, index_type
            FROM table_relationships
            WHERE system_id = ? AND table_name = ?
            ORDER BY is_primary_key DESC, is_foreign_key DESC
        """, (system_id, table_name))
        
        relationships = []
        for row in cursor.fetchall():
            relationships.append({
                'column_name': row[0],
                'is_primary_key': row[1],
                'is_foreign_key': row[2],
                'foreign_table': row[3],
                'foreign_column': row[4],
                'relationship_type': row[5],
                'is_indexed': row[6],
                'index_type': row[7]
            })
        
        cursor.close()
        conn.close()
        
        return {
            'system_id': system_id,
            'table_info': table_info,
            'columns': columns,
            'relationships': relationships
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get table metadata error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/metadata/save-extended")
async def save_extended_metadata(request: ExtendedMetadataSaveRequest):
    """
    Save complete table configuration:
    - Table master data
    - Column metadata with extended fields
    - Relationships (FK, PK)
    """
    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={os.getenv('QUERY_LEARNING_DB_SERVER', 'FSDHWFP01\\SQLEXPRESS')};"
            f"DATABASE=query_learning_db;Trusted_Connection=yes;"
        )
        cursor = conn.cursor()
        
        # 1. Save/Update table_master
        cursor.execute("""
            SELECT id FROM table_master
            WHERE system_id = ? AND table_name = ?
        """, (request.system_id, request.table_name))
        
        table_exists = cursor.fetchone()
        
        if table_exists:
            # Update existing
            cursor.execute("""
                UPDATE table_master
                SET table_friendly_name = ?,
                    table_description = ?,
                    contains_pii = ?,
                    gdpr_category = ?,
                    data_sensitivity = ?,
                    is_configured = 1,
                    configuration_status = 'CONFIGURED',
                    modified_by = ?,
                    modified_at = GETDATE()
                WHERE system_id = ? AND table_name = ?
            """, (
                request.table_friendly_name,
                request.table_description,
                request.contains_pii,
                request.gdpr_category,
                request.data_sensitivity,
                request.created_by,
                request.system_id,
                request.table_name
            ))
        else:
            # Insert new
            cursor.execute("""
                INSERT INTO table_master
                (system_id, table_name, table_friendly_name, table_description,
                 contains_pii, gdpr_category, data_sensitivity, is_configured,
                 configuration_status, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1, 'CONFIGURED', ?)
            """, (
                request.system_id,
                request.table_name,
                request.table_friendly_name,
                request.table_description,
                request.contains_pii,
                request.gdpr_category,
                request.data_sensitivity,
                request.created_by
            ))
        
        # 2. Save columns (delete old, insert new)
        cursor.execute("""
            DELETE FROM table_metadata_config
            WHERE system_id = ? AND table_name = ?
        """, (request.system_id, request.table_name))
        
        for col in request.columns:
            cursor.execute("""
                INSERT INTO table_metadata_config
                (system_id, table_name, column_name, friendly_name, is_visible,
                 data_type, max_length, numeric_precision, numeric_scale, is_nullable,
                 default_value, column_description, data_classification, contains_pii, pii_type,
                 created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                request.system_id, request.table_name, col.column_name, col.friendly_name,
                col.is_visible, col.data_type, col.max_length, col.numeric_precision,
                col.numeric_scale, col.is_nullable, col.default_value, col.column_description,
                col.data_classification, col.contains_pii, col.pii_type, request.created_by
            ))
        
        # 3. Save relationships (delete old, insert new)
        cursor.execute("""
            DELETE FROM table_relationships
            WHERE system_id = ? AND table_name = ?
        """, (request.system_id, request.table_name))
        
        if request.relationships:
            for rel in request.relationships:
                cursor.execute("""
                    INSERT INTO table_relationships
                    (system_id, table_name, column_name, is_primary_key, is_foreign_key,
                     foreign_table, foreign_column, relationship_type, is_indexed, index_type,
                     created_by)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    request.system_id, request.table_name, rel.column_name, rel.is_primary_key,
                    rel.is_foreign_key, rel.foreign_table, rel.foreign_column, rel.relationship_type,
                    rel.is_indexed, rel.index_type, request.created_by
                ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            'success': True,
            'message': f"Successfully saved configuration for {request.table_name}",
            'column_count': len(request.columns),
            'relationship_count': len(request.relationships) if request.relationships else 0
        }
        
    except Exception as e:
        logger.error(f"Save extended metadata error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/api/metadata/invalidate-cache")
async def invalidate_schema_cache(
    system_id: str = Body(...),
    user_role: Optional[str] = Body(None)
):
    """
    Invalidate Redis cache after metadata changes
    Calls PromptManager to clear schema cache
    """
    try:
        # Import here to avoid circular dependency
        import requests
        
        # Call MCP server to invalidate cache
        mcp_url = os.getenv('MCP_SERVER_URL', 'http://10.200.0.1:8501')
        
        # For now, just return success
        # The actual cache invalidation will happen when PromptManager is updated
        
        return {
            'success': True,
            'message': f"Cache invalidation requested for {system_id}" + 
                      (f" (role: {user_role})" if user_role else " (all roles)"),
            'system_id': system_id,
            'user_role': user_role
        }
        
    except Exception as e:
        logger.error(f"Cache invalidation error: {e}")
        return {
            'success': False,
            'error': str(e)
        }

@app.get("/api/{system_id}/schema-search")
async def search_tables(
    system_id: str,
    search_term: str,
    limit: int = 20
):
    """
    Search for tables by name (unconfigured only)
    Returns matching tables without loading all tables
    """
    try:
        if system_id != "STYR":
            raise HTTPException(status_code=404, detail="System not found")
        
        # Get list of configured tables
        conn_learning = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={os.getenv('QUERY_LEARNING_DB_SERVER', 'FSDHWFP01\\SQLEXPRESS')};"
            f"DATABASE=query_learning_db;Trusted_Connection=yes;"
        )
        cursor_learning = conn_learning.cursor()
        
        cursor_learning.execute("""
            SELECT table_name FROM table_master
            WHERE system_id = ? AND is_configured = 1
        """, (system_id,))
        
        configured_tables = [row[0] for row in cursor_learning.fetchall()]
        cursor_learning.close()
        conn_learning.close()
        
        # Search source database for matching tables
        connection_string = f"""
        DRIVER={{IBM i Access ODBC Driver}};
        SYSTEM={os.getenv('STYR_SYSTEM')};
        USERID={os.getenv('STYR_USERID')};
        PASSWORD={os.getenv('STYR_PASSWORD')};
        ALLOWPROCCALLS=1;
        SORTTABLE=1;
        """
        
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        # Search with LIKE pattern
        search_pattern = f"%{search_term.upper()}%"
        
        cursor.execute("""
            SELECT DISTINCT
                TABLE_SCHEMA,
                TABLE_NAME,
                COUNT(*) as column_count
            FROM QSYS2.SYSCOLUMNS
            WHERE TABLE_SCHEMA IN ('DCPO', 'EGU')
            AND (
                TABLE_SCHEMA LIKE ? OR
                TABLE_NAME LIKE ?
            )
            GROUP BY TABLE_SCHEMA, TABLE_NAME
            ORDER BY TABLE_NAME
            FETCH FIRST ? ROWS ONLY
        """, (search_pattern, search_pattern, limit))
        
        results = []
        for row in cursor.fetchall():
            table_full_name = f"{row[0]}.{row[1]}"
            
            # Skip configured tables
            if table_full_name in configured_tables:
                continue
            
            results.append({
                'table_name': table_full_name,
                'schema': row[0],
                'table': row[1],
                'column_count': row[2],
                'is_configured': False
            })
        
        cursor.close()
        conn.close()
        
        return {
            'system_id': system_id,
            'search_term': search_term,
            'results': results,
            'result_count': len(results)
        }
        
    except Exception as e:
        logger.error(f"Table search error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/log-simple-query")
async def log_simple_query(
    user_id: str = Body(...),
    question: str = Body(...),
    sql_generated: str = Body(None),
    permission_checked: bool = Body(False),
    tables_used: str = Body(None)
):
    """
    Log query with table analysis data
    Used by table analyzer AI for learning
    """
    try:
        if not query_learning_service:
            raise HTTPException(status_code=500, detail="Query learning service unavailable")
        
        # Get user role
        user_role = get_user_role(user_id)
        
        # Log using existing service
        success = query_learning_service.log_query(
            user_id=user_id,
            user_role=user_role,
            question=question,
            sql_generated=sql_generated,
            execution_time_ms=0,
            success=bool(sql_generated) if sql_generated else True,
            error_message=None,
            row_count=len(tables_used.split(',')) if tables_used else 0,
            session_id=f"analysis_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        
        return {
            "success": success,
            "message": "Query logged successfully" if success else "Failed to log query"
        }
        
    except Exception as e:
        logger.error(f"Log simple query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/user/{username}/permissions")
async def get_user_permissions_summary(username: str):
    """
    Get user permissions summary for UI display
    Simpler version of rbac/rules for quick access
    """
    try:
        user_role = get_user_role(username)
        
        if not permission_service:
            return {"error": "Permission service unavailable"}
        
        # Get RBAC rules
        rules = permission_service.get_rbac_rules_for_role(user_role)
        
        if not rules:
            return {"error": f"No rules found for role {user_role}"}
        
        tables = rules.get('tables', [])
        restrictions = rules.get('restrictions', {})
        
        return {
            "username": username,
            "role": user_role,
            "table_count": len(tables),
            "tables": tables[:5],  # First 5 tables for display
            "total_tables": len(tables),
            "has_restrictions": bool(restrictions),
            "restrictions_count": len(restrictions),
            "success": True
        }
        
    except Exception as e:
        logger.error(f"Get user permissions summary failed: {e}")
        return {"error": str(e)}


# ADD THIS HELPER FUNCTION:
def get_user_role(username: str) -> str:
    """Helper function to get user role from username"""
    role_mapping = {
        'harold': 'CEO',
        'lars': 'Finance', 
        'pontus': 'Call Center',
        'peter': 'Logistics',
        'linda': 'Customer Service'
    }
    return role_mapping.get(username, username)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)