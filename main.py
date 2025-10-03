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
query_service = QueryService(db_connector)


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


@app.post("/api/execute-query")
async def execute_query(query_request: DynamicQueryRequest, request: Request):
    """Execute dynamic SQL query with conversation memory"""
    
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
    
    # Add user message to conversation history
    conversation.add_message('user', question)
    
    # Check if this is a follow-up question (contains pronouns or references)
    is_followup = _is_followup_question(question)
    
    # Enhance query with conversation context if it's a follow-up
    # Get conversation context for AI (don't add to SQL)
    context = None
    if is_followup:
        context = conversation.get_context_for_query()
        if context:
            print(f"Using context for session {session_id}: {context[:100]}")

    # Step 1: Check cache first (use original question)
    if query_learning_service:
        try:
            cached_result = query_learning_service.get_cached_query(question, user_role)
            if cached_result:
                # Add assistant response to conversation
                conversation.add_message('assistant', 'Returned cached results')
                
                return {
                    "success": True,
                    "data": cached_result.get("result_json", {}),
                    "source": "cache",
                    "sql": cached_result.get("sql_query")
                }
        except Exception as e:
            print(f"Cache lookup failed: {e}")
    
    # Step 2: Generate and execute query
    start_time = time.time()
    
    try:
        # Execute query using existing query_service
        result = await query_service.execute_dynamic_query(
            query_request,
            getattr(request.state, 'request_id', None)
        )
        
        execution_time_ms = int((time.time() - start_time) * 1000)
        success = result.get("success", False)
        
        if success:
            # Extract tables used from SQL
            sql_generated = result.get("sql", "")
            tables_used = _extract_tables_from_sql(sql_generated)
            row_count = len(result.get("data", {}).get("rows", []))
            
            # Update conversation context
            conversation.update_query_context(
                query=question,
                sql=sql_generated,
                result_count=row_count,
                tables=tables_used
            )
            
            # Add assistant response to conversation
            conversation.add_message(
                'assistant',
                f'Query executed successfully. {row_count} results.',
                {'sql': sql_generated, 'tables': tables_used}
            )
        
        # Step 3: Log query execution
        if query_learning_service:
            try:
                sql_generated = result.get("sql", "")
                row_count = len(result.get("data", {}).get("rows", []))
                error_message = result.get("message") if not success else None
                
                query_learning_service.log_query(
                    user_id=username,
                    user_role=user_role,
                    question=question,  # Use original question, not enhanced
                    sql_generated=sql_generated,
                    execution_time_ms=execution_time_ms,
                    success=success,
                    error_message=error_message,
                    row_count=row_count,
                    session_id=session_id
                )
                
                # Step 4: Cache successful results
                if success and row_count > 0:
                    query_learning_service.save_to_cache(
                        question=question,  # Use original question
                        user_role=user_role,
                        sql_query=sql_generated,
                        result_data=result.get("data", {}),
                        ttl_minutes=int(os.getenv("QUERY_CACHE_TTL_MINUTES", 60))
                    )
                
                # Step 5: Update performance metrics
                query_learning_service.update_performance(
                    question=question,
                    user_role=user_role,
                    execution_time_ms=execution_time_ms
                )
            except Exception as e:
                print(f"Query learning service error: {e}")
        
        result["source"] = "database"
        result["is_followup"] = is_followup
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

@app.get("/api/query-suggestions")
async def get_query_suggestions(request: Request, limit: int = 5):
    """Get query suggestions based on user role"""
    
    if not query_learning_service:
        return {"suggestions": []}
    
    username = request.headers.get("X-Username")
    if not username:
        raise HTTPException(status_code=401, detail="Username required")
    
    user = get_user(username)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid user")
    
    try:
        suggestions = query_learning_service.get_query_suggestions(
            user_role=user.role.value,
            limit=limit
        )
        return {"suggestions": suggestions}
    except Exception as e:
        return {"suggestions": [], "error": str(e)}


@app.get("/api/cache-stats")
async def get_cache_statistics(request: Request):
    """Get cache performance statistics (Admin only)"""
    
    if not query_learning_service:
        return {"enabled": False, "message": "Query learning is disabled"}
    
    username = request.headers.get("X-Username")
    if not username:
        raise HTTPException(status_code=401, detail="Username required")
    
    # Only allow CEO to view stats
    user = get_user(username)
    if not user or user.role.value != "ceo":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        stats = query_learning_service.get_cache_statistics()
        stats["enabled"] = True
        return stats
    except Exception as e:
        return {"enabled": True, "error": str(e)}


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


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)