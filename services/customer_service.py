from typing import List
from models.schemas import CustomerByIdRequest, CustomerSearchRequest, CustomerResponse, CustomerData
from database.styr_connector import StyrDatabaseConnector
from utils.response_formatter import response_formatter
from utils.fallback import fallback_manager
from datetime import datetime
import time

class CustomerService:
    def __init__(self, db_connector: StyrDatabaseConnector):
        self.db = db_connector
    
    async def get_customer_by_id(self, request: CustomerByIdRequest, request_id: str = None) -> dict:
        start_time = time.time()
        
        try:
            query = """
            SELECT 
                KHKNR as customer_number,
                KHFKN as customer_name,
                KHSÖK as search_terms,
                KHFA1 as address_line1,
                KHFA2 as address_line2,
                KHFA3 as address_line3,
                KHFA4 as address_line4,
                CONCAT(KHFPF, KHFPN) as postal_code,
                KHTEL as telephone,
                KHTFX as fax,
                KHKON as contact_person,
                KHLAK as country_code,
                KHVAL as currency_code,
                KHSTS as status,
                KHKGÄ as credit_limit,
                KHVBK as payment_terms_code,
                KHVLK as delivery_terms_code,
                KHSLK as delivery_mode_code,
                KHDSO as last_order_date,
                KHDSF as last_invoice_date
            FROM DCPO.KHKNDHUR 
            WHERE KHKNR = ?
              AND KHSTS = '1'
            """
            
            raw_data = await self.db.execute_query(query, (request.customer_number,))
            execution_time = (time.time() - start_time) * 1000
            
            if not raw_data:
                return response_formatter.error_response(
                    message=f"Customer {request.customer_number} not found",
                    error_code="CUSTOMER_NOT_FOUND",
                    execution_time_ms=execution_time,
                    request_id=request_id
                ).dict()
            
            # Format the data using standardized formatter
            formatted_data = response_formatter.format_customer_data(raw_data)
            
            return response_formatter.success_response(
                data={
                    "customers": formatted_data,
                    "total_count": len(formatted_data)
                },
                message=f"Successfully retrieved {len(formatted_data)} customer record(s)",
                execution_time_ms=execution_time,
                request_id=request_id
            ).dict()
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            
            # Determine error type
            error_code = "DB_CONNECTION_FAILED" if "circuit breaker" in str(e).lower() else "DB_QUERY_FAILED"
            
            return response_formatter.error_response(
                message=f"Failed to retrieve customer data: {str(e)}",
                error_code=error_code,
                details=str(e),
                execution_time_ms=execution_time,
                request_id=request_id
            ).dict()
    
    async def search_customers(self, request: CustomerSearchRequest, request_id: str = None) -> dict:
        start_time = time.time()
        
        try:
            query = """
            SELECT 
                KHKNR as customer_number,
                KHFKN as customer_name,
                KHSÖK as search_terms,
                KHFA1 as address_line1,
                KHTEL as telephone,
                KHKON as contact_person,
                KHSTS as status
            FROM DCPO.KHKNDHUR 
            WHERE (UPPER(KHFKN) LIKE UPPER(CONCAT(CONCAT('%', ?), '%')) 
               OR UPPER(KHSÖK) LIKE UPPER(CONCAT(CONCAT('%', ?), '%')))
              AND KHSTS = '1'
            ORDER BY KHFKN
            FETCH FIRST 100 ROWS ONLY
            """
            
            search_param = request.search_term
            raw_data = await self.db.execute_query(query, (search_param, search_param))
            execution_time = (time.time() - start_time) * 1000
            
            # Format the data using standardized formatter  
            formatted_data = response_formatter.format_customer_data(raw_data)
            
            return response_formatter.success_response(
                data={
                    "customers": formatted_data,
                    "total_count": len(formatted_data),
                    "search_term": request.search_term,
                    "limited_results": len(formatted_data) == 100
                },
                message=f"Found {len(formatted_data)} customers matching '{request.search_term}'" + 
                        (" (showing first 100)" if len(formatted_data) == 100 else ""),
                execution_time_ms=execution_time,
                request_id=request_id
            ).dict()
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            
            error_code = "DB_CONNECTION_FAILED" if "circuit breaker" in str(e).lower() else "DB_QUERY_FAILED"
            
            return response_formatter.error_response(
                message=f"Customer search failed: {str(e)}",
                error_code=error_code,
                details=str(e),
                execution_time_ms=execution_time,
                request_id=request_id
            ).dict()