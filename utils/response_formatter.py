from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from pydantic import BaseModel
from fastapi import HTTPException

class StandardResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Any] = None
    error_code: Optional[str] = None
    timestamp: datetime
    execution_time_ms: Optional[float] = None
    request_id: Optional[str] = None

class ErrorDetails(BaseModel):
    error_code: str
    error_type: str
    message: str
    details: Optional[str] = None
    suggested_action: Optional[str] = None

class ResponseFormatter:
    
    # Standard error codes
    ERROR_CODES = {
        "DB_CONNECTION_FAILED": "DATABASE_CONNECTION_ERROR",
        "DB_QUERY_FAILED": "DATABASE_QUERY_ERROR", 
        "VALIDATION_ERROR": "REQUEST_VALIDATION_ERROR",
        "AUTH_FAILED": "AUTHENTICATION_ERROR",
        "RATE_LIMIT_EXCEEDED": "RATE_LIMIT_ERROR",
        "CIRCUIT_BREAKER_OPEN": "SERVICE_UNAVAILABLE",
        "CUSTOMER_NOT_FOUND": "RESOURCE_NOT_FOUND",
        "INTERNAL_ERROR": "INTERNAL_SERVER_ERROR"
    }
    
    @staticmethod
    def success_response(
        data: Any,
        message: str = "Request completed successfully",
        execution_time_ms: Optional[float] = None,
        request_id: Optional[str] = None
    ) -> StandardResponse:
        """Format successful response"""
        return StandardResponse(
            success=True,
            message=message,
            data=data,
            timestamp=datetime.now(),
            execution_time_ms=execution_time_ms,
            request_id=request_id
        )
    
    @staticmethod
    def error_response(
        message: str,
        error_code: str = "INTERNAL_ERROR",
        details: Optional[str] = None,
        suggested_action: Optional[str] = None,
        execution_time_ms: Optional[float] = None,
        request_id: Optional[str] = None
    ) -> StandardResponse:
        """Format error response"""
        
        error_details = ErrorDetails(
            error_code=ResponseFormatter.ERROR_CODES.get(error_code, error_code),
            error_type=error_code,
            message=message,
            details=details,
            suggested_action=suggested_action or ResponseFormatter._get_suggested_action(error_code)
        )
        
        return StandardResponse(
            success=False,
            message=message,
            data=None,
            error_code=ResponseFormatter.ERROR_CODES.get(error_code, error_code),
            timestamp=datetime.now(),
            execution_time_ms=execution_time_ms,
            request_id=request_id
        )
    
    @staticmethod
    def _get_suggested_action(error_code: str) -> str:
        """Get suggested action based on error code"""
        suggestions = {
            "DB_CONNECTION_FAILED": "Please try again in a few minutes or contact support if the issue persists",
            "DB_QUERY_FAILED": "Verify your request parameters and try again",
            "VALIDATION_ERROR": "Check your request format and required fields",
            "AUTH_FAILED": "Verify your authorization token and try again",
            "RATE_LIMIT_EXCEEDED": "Wait a moment before making another request",
            "CIRCUIT_BREAKER_OPEN": "Service is temporarily unavailable. Please try again later",
            "CUSTOMER_NOT_FOUND": "Verify the customer number or search terms",
            "INTERNAL_ERROR": "Please contact support with the request details"
        }
        return suggestions.get(error_code, "Please contact support for assistance")
    
    @staticmethod
    def format_customer_data(raw_data: List[Dict]) -> List[Dict]:
        """Standardize customer data format"""
        formatted_data = []
        
        for row in raw_data:
            formatted_row = {
                "customer_number": str(row.get('CUSTOMER_NUMBER', '')),
                "customer_name": row.get('CUSTOMER_NAME', '').strip(),
                "search_terms": row.get('SEARCH_TERMS', '').strip() if row.get('SEARCH_TERMS') else None,
                "contact_info": {
                    "telephone": row.get('TELEPHONE', '').strip() if row.get('TELEPHONE') else None,
                    "fax": row.get('FAX', '').strip() if row.get('FAX') else None,
                    "contact_person": row.get('CONTACT_PERSON', '').strip() if row.get('CONTACT_PERSON') else None
                },
                "address": {
                    "line1": row.get('ADDRESS_LINE1', '').strip() if row.get('ADDRESS_LINE1') else None,
                    "line2": row.get('ADDRESS_LINE2', '').strip() if row.get('ADDRESS_LINE2') else None,
                    "line3": row.get('ADDRESS_LINE3', '').strip() if row.get('ADDRESS_LINE3') else None,
                    "line4": row.get('ADDRESS_LINE4', '').strip() if row.get('ADDRESS_LINE4') else None,
                    "postal_code": row.get('POSTAL_CODE', '').strip() if row.get('POSTAL_CODE') else None,
                    "country_code": row.get('COUNTRY_CODE', '').strip() if row.get('COUNTRY_CODE') else None
                },
                "business_info": {
                    "status": str(row.get('STATUS', '')),
                    "currency_code": row.get('CURRENCY_CODE', '').strip() if row.get('CURRENCY_CODE') else None,
                    "credit_limit": float(row['CREDIT_LIMIT']) if row.get('CREDIT_LIMIT') else None,
                    "payment_terms_code": row.get('PAYMENT_TERMS_CODE', '').strip() if row.get('PAYMENT_TERMS_CODE') else None,
                    "delivery_terms_code": row.get('DELIVERY_TERMS_CODE', '').strip() if row.get('DELIVERY_TERMS_CODE') else None,
                    "delivery_mode_code": row.get('DELIVERY_MODE_CODE', '').strip() if row.get('DELIVERY_MODE_CODE') else None
                },
                "activity": {
                    "last_order_date": str(row['LAST_ORDER_DATE']) if row.get('LAST_ORDER_DATE') else None,
                    "last_invoice_date": str(row['LAST_INVOICE_DATE']) if row.get('LAST_INVOICE_DATE') else None
                }
            }
            formatted_data.append(formatted_row)
        
        return formatted_data
    
    @staticmethod
    def format_order_data(raw_data: List[Dict]) -> List[Dict]:
        """Standardize order data format"""
        formatted_data = []
        
        for row in raw_data:
            formatted_row = {
                "order_info": {
                    "order_number": str(row.get('ORDER_NUMBER', '')),
                    "order_version": str(row.get('ORDER_VERSION', '')),
                    "order_date": str(row.get('ORDER_DATE', '')),
                    "order_value": float(row.get('ORDER_VALUE', 0))
                },
                "customer_info": {
                    "customer_number": str(row.get('CUSTOMER_NUMBER', '')),
                    "customer_name": row.get('CUSTOMER_NAME', '').strip()
                },
                "line_items": {
                    "line_number": str(row.get('LINE_NUMBER', '')),
                    "article_number": str(row.get('ARTICLE_NUMBER', '')),
                    "ordered_qty": int(row.get('ORDERED_QTY', 0)),
                    "line_value": float(row.get('LINE_VALUE', 0))
                }
            }
            formatted_data.append(formatted_row)
        
        return formatted_data

# Global formatter instance
response_formatter = ResponseFormatter()