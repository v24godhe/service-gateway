from pydantic import BaseModel, validator, Field
from typing import Optional, List
from datetime import datetime

class CustomerByIdRequest(BaseModel):
    customer_number: str = Field(..., min_length=1, max_length=20, pattern="^[0-9]+$")
    
    @validator('customer_number')
    def validate_customer_number(cls, v):
        if not v.isdigit():
            raise ValueError('Customer number must contain only digits')
        if len(v) > 20:
            raise ValueError('Customer number too long')
        return v

class CustomerSearchRequest(BaseModel):
    search_term: str = Field(..., min_length=2, max_length=100)
    
    @validator('search_term')
    def validate_search_term(cls, v):
        if len(v.strip()) < 2:
            raise ValueError('Search term must be at least 2 characters')
        if len(v) > 100:
            raise ValueError('Search term too long')
        # Check for obvious SQL injection attempts
        dangerous_patterns = ['union', 'select', 'insert', 'drop', 'delete', '--', ';']
        if any(pattern in v.lower() for pattern in dangerous_patterns):
            raise ValueError('Invalid characters in search term')
        return v.strip()

class CustomerData(BaseModel):
    customer_number: str
    customer_name: str
    search_terms: Optional[str]
    address_line1: Optional[str]
    address_line2: Optional[str] = None
    address_line3: Optional[str] = None
    address_line4: Optional[str] = None
    postal_code: Optional[str] = None
    telephone: Optional[str]
    fax: Optional[str] = None
    contact_person: Optional[str]
    country_code: Optional[str] = None
    currency_code: Optional[str] = None
    status: str
    credit_limit: Optional[float] = None
    payment_terms_code: Optional[str] = None
    delivery_terms_code: Optional[str] = None
    delivery_mode_code: Optional[str] = None
    last_order_date: Optional[str] = None
    last_invoice_date: Optional[str] = None

class CustomerResponse(BaseModel):
    success: bool
    message: str
    data: List[CustomerData] = []
    timestamp: datetime

# Keep existing tracking models...
class TrackingRequest(BaseModel):
    order_number: Optional[str] = None
    customer_number: Optional[str] = None
    tracking_number: Optional[str] = None
    
    @validator('order_number')
    def validate_order_number(cls, v):
        if v and not v.isdigit():
            raise ValueError('Order number must be numeric')
        return v

class OrderData(BaseModel):
    order_number: str
    order_version: str
    customer_number: str
    customer_name: str
    order_date: str
    order_value: float
    line_number: str
    article_number: str
    ordered_qty: int
    line_value: float

class TrackingResponse(BaseModel):
    success: bool
    message: str
    data: List[OrderData] = []
    timestamp: datetime