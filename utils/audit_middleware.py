# utils/audit_middleware.py
import time
import json
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from utils.audit_logger import audit_logger

class AuditMiddleware(BaseHTTPMiddleware):
    """Middleware that automatically audits ALL API requests"""
    
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        
        # Read request body for logging
        request_body = None
        if request.method in ["POST", "PUT", "PATCH"]:
            try:
                body = await request.body()
                request_body = body.decode('utf-8') if body else None
                
                # Important: Re-create request with body for downstream processing
                async def receive():
                    return {"type": "http.request", "body": body}
                
                request._receive = receive
                
            except Exception as e:
                audit_logger.error_logger.error(f"Failed to read request body: {e}")
        
        # Process the request
        response = await call_next(request)
        
        # Calculate execution time
        execution_time = (time.time() - start_time) * 1000
        
        # Read response body for logging (if needed)
        response_body = None
        
        # Log the API request (this works for ANY new API automatically!)
        await audit_logger.log_api_request(
            request=request,
            response=response,
            request_body=request_body,
            response_body=response_body,
            execution_time_ms=execution_time
        )
        
        return response