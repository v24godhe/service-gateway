from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
import traceback
import re
import json
import uuid
from datetime import datetime
from pydantic import BaseModel
from utils.audit_logger import audit_logger
from utils.response_formatter import response_formatter


def json_serial(obj):
    """JSON serializer for objects not serializable by default"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


class ErrorResponse(BaseModel):
    error_code: str
    message: str
    timestamp: str
    request_id: str
    path: str


class GlobalErrorHandler:
    
    @staticmethod
    def sanitize_error_message(error_message: str) -> str:
        """Remove sensitive information from error messages"""
        # Remove SQL injection patterns
        sql_patterns = [
            r'SELECT.*FROM.*WHERE',
            r'INSERT.*INTO.*VALUES',
            r'UPDATE.*SET.*WHERE',
            r'DELETE.*FROM.*WHERE'
        ]
        
        for pattern in sql_patterns:
            error_message = re.sub(pattern, '[SQL_QUERY]', error_message, flags=re.IGNORECASE)
        
        # Remove file paths
        error_message = re.sub(r'[A-Za-z]:\\[^\\]+(?:\\[^\\]+)*', '[FILE_PATH]', error_message)
        
        # Remove connection strings
        error_message = re.sub(r'DSN=.*?;', 'DSN=[HIDDEN];', error_message)
        error_message = re.sub(r'PWD=.*?;', 'PWD=[HIDDEN];', error_message)
        
        return error_message
    
    @staticmethod
    async def http_exception_handler(request: Request, exc: HTTPException):
        """Handle HTTP exceptions"""
        sanitized_detail = GlobalErrorHandler.sanitize_error_message(str(exc.detail))
        
        # Log the error
        audit_logger.log_error(
            request=request,
            error=exc,
            error_code=f"HTTP_{exc.status_code}",
            additional_context={"original_detail": str(exc.detail)}
        )
        
        # Map status codes to user-friendly messages
        user_messages = {
            400: "Bad request - please check your input",
            401: "Authentication required",
            403: "Access forbidden",
            404: "Resource not found", 
            422: "Invalid request data",
            429: "Too many requests - please try again later",
            500: "Internal server error"
        }
        
        user_message = user_messages.get(exc.status_code, sanitized_detail)
        
        error_response = response_formatter.error_response(
            message=user_message,
            error_code=f"HTTP_{exc.status_code}",
            details=sanitized_detail,
            request_id=getattr(request.state, 'request_id', None)
        )
        
        # Convert to dict and ensure JSON serializable
        response_dict = error_response.dict()
        
        return JSONResponse(
            status_code=exc.status_code,
            content=json.loads(json.dumps(response_dict, default=str))
        )
    
    @staticmethod
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        """Handle validation errors"""
        request_id = str(uuid.uuid4())
        
        # Extract validation errors
        validation_errors = []
        for error in exc.errors():
            validation_errors.append({
                "field": str(error.get("loc", [])),
                "message": error.get("msg", ""),
                "type": error.get("type", "")
            })
        
        error_response = ErrorResponse(
            error_code="VALIDATION_ERROR",
            message=str(exc),
            timestamp=datetime.now().isoformat(),
            request_id=request_id,
            path=str(request.url.path)
        )
        
        # Log to audit
        audit_logger.log_error(
            request=request,
            error=exc,
            error_code="VALIDATION_ERROR",
            additional_context={
                "validation_errors": validation_errors
            }
        )
        
        return JSONResponse(
            status_code=422,
            content={
                "error_code": error_response.error_code,
                "message": error_response.message,
                "timestamp": error_response.timestamp,
                "request_id": error_response.request_id,
                "path": error_response.path,
                "details": validation_errors
            }
        )
    
    @staticmethod
    async def general_exception_handler(request: Request, exc: Exception):
        """Handle all other exceptions"""
        sanitized_message = GlobalErrorHandler.sanitize_error_message(str(exc))
        
        # Log the error with full traceback
        audit_logger.log_error(
            request=request,
            error=exc,
            error_code="INTERNAL_ERROR",
            additional_context={
                "traceback": traceback.format_exc(),
                "exception_type": type(exc).__name__
            }
        )
        
        # Create error response
        error_response = response_formatter.error_response(
            message=sanitized_message,
            error_code="INTERNAL_ERROR",
            details=traceback.format_exc(),
            request_id=getattr(request.state, 'request_id', None)
        )
        
        return JSONResponse(
            status_code=500,
            content=json.loads(json.dumps(error_response.dict(), default=json_serial))
        )


error_handler = GlobalErrorHandler()