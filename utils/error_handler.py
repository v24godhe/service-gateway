from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
import traceback
import re
from utils.audit_logger import audit_logger
from utils.response_formatter import response_formatter

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
        
        return JSONResponse(
            status_code=exc.status_code,
            content=response_formatter.error_response(
                message=user_messages.get(exc.status_code, sanitized_detail),
                error_code=f"HTTP_{exc.status_code}",
                details=sanitized_detail if exc.status_code != 500 else "Please contact support",
                request_id=getattr(request.state, 'request_id', None)
            ).dict()
        )
    
    @staticmethod
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        """Handle request validation errors"""
        # Sanitize validation errors
        sanitized_errors = []
        for error in exc.errors():
            sanitized_error = {
                "field": ".".join(str(x) for x in error["loc"][1:]),  # Skip 'body'
                "message": GlobalErrorHandler.sanitize_error_message(error["msg"]),
                "type": error["type"]
            }
            sanitized_errors.append(sanitized_error)
        
        # Log validation error
        audit_logger.log_error(
            request=request,
            error=exc,
            error_code="VALIDATION_ERROR",
            additional_context={"validation_errors": sanitized_errors}
        )
        
        return JSONResponse(
            status_code=422,
            content=response_formatter.error_response(
                message="Request validation failed",
                error_code="VALIDATION_ERROR",
                details=f"Invalid fields: {', '.join([e['field'] for e in sanitized_errors])}",
                request_id=getattr(request.state, 'request_id', None)
            ).dict()
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
        
        return JSONResponse(
            status_code=500,
            content=response_formatter.error_response(
                message="An unexpected error occurred",
                error_code="INTERNAL_ERROR",
                details="Please contact support with your request ID",
                request_id=getattr(request.state, 'request_id', None)
            ).dict()
        )

error_handler = GlobalErrorHandler()