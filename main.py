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

from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from utils.error_handler import error_handler
from utils.input_sanitizer import input_sanitizer



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
    sanitized_request = CustomerByIdRequest(**sanitized_data)
    return await customer_service.search_customers(
        sanitized_request,
        getattr(request.state, 'request_id', None)
    )



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