import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path
from logging.handlers import RotatingFileHandler
from fastapi import Request, Response

class AuditLogger:
    def __init__(self, log_directory: str = "logs"):
        self.log_directory = Path(log_directory)
        self.log_directory.mkdir(exist_ok=True)
        
        # Create separate loggers for different types
        self.api_logger = self._setup_logger("api_audit", "api_audit.log")
        self.security_logger = self._setup_logger("security_audit", "security_audit.log")
        self.error_logger = self._setup_logger("error_audit", "error_audit.log")
        
    def _setup_logger(self, name: str, filename: str) -> logging.Logger:
        """Setup rotating file logger"""
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        
        # Avoid duplicate handlers if logger already exists
        if not logger.handlers:
            handler = RotatingFileHandler(
                self.log_directory / filename,
                maxBytes=50*1024*1024,  # 50MB
                backupCount=10
            )
            
            formatter = logging.Formatter(
                '%(asctime)s | %(levelname)s | %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    async def log_api_request(
        self,
        request: Request,
        response: Response,
        request_body: Optional[str] = None,
        response_body: Optional[str] = None,
        execution_time_ms: Optional[float] = None
    ):
        """Log API request/response - AUTOMATICALLY WORKS FOR ANY NEW API"""
        try:
            # Extract request info
            client_ip = self._get_client_ip(request)
            request_id = getattr(request.state, 'request_id', 'unknown')
            
            # Build audit log entry
            audit_entry = {
                "request_id": request_id,
                "timestamp": datetime.now().isoformat(),
                "client_ip": client_ip,
                "method": request.method,
                "endpoint": str(request.url.path),
                "query_params": dict(request.query_params),
                "headers": {
                    "user-agent": request.headers.get("user-agent"),
                    "content-type": request.headers.get("content-type"),
                    "authorization": "Bearer ***" if request.headers.get("authorization") else None
                },
                "request_body_size": len(request_body) if request_body else 0,
                "response_status": response.status_code,
                "response_body_size": len(response_body) if response_body else 0,
                "execution_time_ms": execution_time_ms,
                "success": 200 <= response.status_code < 300
            }
            
            # Add request body for POST/PUT (but mask sensitive data)
            if request_body and request.method in ["POST", "PUT", "PATCH"]:
                audit_entry["request_body"] = self._mask_sensitive_data(request_body)
            
            # Log as JSON for easy parsing
            self.api_logger.info(json.dumps(audit_entry))
            
        except Exception as e:
            # Don't let logging errors break the API
            self.error_logger.error(f"Audit logging failed: {str(e)}")
    
    def log_authentication_event(
        self,
        request: Request,
        event_type: str,
        success: bool,
        details: Optional[str] = None
    ):
        """Log authentication events"""
        try:
            client_ip = self._get_client_ip(request)
            request_id = getattr(request.state, 'request_id', 'unknown')
            
            security_entry = {
                "request_id": request_id,
                "timestamp": datetime.now().isoformat(),
                "event_type": event_type,
                "client_ip": client_ip,
                "endpoint": str(request.url.path),
                "success": success,
                "details": details,
                "user_agent": request.headers.get("user-agent")
            }
            
            self.security_logger.info(json.dumps(security_entry))
            
        except Exception as e:
            self.error_logger.error(f"Security logging failed: {str(e)}")
    
    def log_database_operation(
        self,
        request_id: str,
        operation_type: str,
        table_name: Optional[str],
        query: Optional[str],
        execution_time_ms: float,
        row_count: int,
        success: bool,
        error: Optional[str] = None
    ):
        """Log database operations"""
        try:
            db_entry = {
                "request_id": request_id,
                "timestamp": datetime.now().isoformat(),
                "operation_type": operation_type,
                "table_name": table_name,
                "query": self._sanitize_query(query) if query else None,
                "execution_time_ms": execution_time_ms,
                "row_count": row_count,
                "success": success,
                "error": error
            }
            
            self.api_logger.info(json.dumps(db_entry, default=str))
            
        except Exception as e:
            self.error_logger.error(f"Database logging failed: {str(e)}")
    
    def log_error(
        self,
        request: Request,
        error: Exception,
        error_code: Optional[str] = None,
        additional_context: Optional[Dict] = None
    ):
        """Log application errors"""
        try:
            client_ip = self._get_client_ip(request)
            request_id = getattr(request.state, 'request_id', 'unknown')
            
            error_entry = {
                "request_id": request_id,
                "timestamp": datetime.now().isoformat(),
                "client_ip": client_ip,
                "endpoint": str(request.url.path),
                "error_type": type(error).__name__,
                "error_message": str(error),
                "error_code": error_code,
                "additional_context": additional_context
            }
            
            self.error_logger.error(json.dumps(error_entry))
            
        except Exception as e:
            # Last resort - print to console
            print(f"Critical: Error logging failed: {str(e)}")
    
    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP address"""
        # Check for forwarded headers first (if behind proxy)
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()
        
        real_ip = request.headers.get("x-real-ip")
        if real_ip:
            return real_ip
        
        # Fallback to direct client IP
        return request.client.host if request.client else "unknown"
    
    def _mask_sensitive_data(self, data: str) -> str:
        """Mask sensitive data in request bodies"""
        try:
            parsed_data = json.loads(data)
            
            # List of sensitive fields to mask
            sensitive_fields = [
                "password", "token", "secret", "key", "auth", 
                "credit_card", "ssn", "personal_number"
            ]
            
            def mask_recursive(obj):
                if isinstance(obj, dict):
                    return {
                        k: "***MASKED***" if any(field in k.lower() for field in sensitive_fields) 
                        else mask_recursive(v)
                        for k, v in obj.items()
                    }
                elif isinstance(obj, list):
                    return [mask_recursive(item) for item in obj]
                else:
                    return obj
            
            masked_data = mask_recursive(parsed_data)
            return json.dumps(masked_data)
            
        except:
            # If not JSON, just return truncated version
            return data[:200] + "..." if len(data) > 200 else data
    
    def _sanitize_query(self, query: str) -> str:
        """Sanitize SQL query for logging"""
        if not query:
            return query
        
        # Remove potential sensitive data from WHERE clauses
        import re
        
        # Mask values in WHERE clauses but keep structure
        query = re.sub(r"= ?'[^']+'", "= '***'", query)
        query = re.sub(r"= ?[0-9]+", "= ###", query)
        
        return query[:500] + "..." if len(query) > 500 else query
    
    def get_audit_stats(self, hours: int = 24) -> Dict[str, Any]:
        """Get audit statistics (you can implement this for monitoring)"""
        return {
            "message": "Audit stats not implemented yet",
            "log_directory": str(self.log_directory),
            "hours_requested": hours
        }

# Global audit logger
audit_logger = AuditLogger()