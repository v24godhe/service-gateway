import asyncio
import time
from typing import Optional, Dict, Any
from models.schemas import TrackingResponse, CustomerResponse, OrderData, CustomerData
from datetime import datetime

class FallbackManager:
    def __init__(self):
        self.last_db_failure = 0
        self.db_failure_count = 0
        self.circuit_breaker_threshold = 3
        self.circuit_breaker_timeout = 300  # 5 minutes
        self.is_circuit_open = False
    
    def record_db_failure(self):
        """Record a database failure"""
        self.last_db_failure = time.time()
        self.db_failure_count += 1
        
        if self.db_failure_count >= self.circuit_breaker_threshold:
            self.is_circuit_open = True
            print(f"CIRCUIT BREAKER: Database circuit opened after {self.db_failure_count} failures")
    
    def record_db_success(self):
        """Record successful database operation"""
        self.db_failure_count = 0
        if self.is_circuit_open:
            self.is_circuit_open = False
            print("CIRCUIT BREAKER: Database circuit closed - connection restored")
    
    def should_try_database(self) -> bool:
        """Check if we should attempt database connection"""
        if not self.is_circuit_open:
            return True
        
        # Check if circuit breaker timeout has passed
        if time.time() - self.last_db_failure > self.circuit_breaker_timeout:
            self.is_circuit_open = False
            self.db_failure_count = 0
            print("CIRCUIT BREAKER: Attempting database reconnection")
            return True
        
        return False
    
    def get_fallback_tracking_response(self, error_msg: str) -> TrackingResponse:
        """Return fallback response for tracking queries"""
        return TrackingResponse(
            success=False,
            message=f"Database temporarily unavailable. {error_msg}. Please try again later or contact support.",
            data=[],
            timestamp=datetime.now()
        )
    
    def get_fallback_customer_response(self, error_msg: str) -> CustomerResponse:
        """Return fallback response for customer queries"""
        return CustomerResponse(
            success=False,
            message=f"Customer database temporarily unavailable. {error_msg}. Please try again later.",
            data=[],
            timestamp=datetime.now()
        )
    
    def get_status(self) -> Dict[str, Any]:
        """Get current fallback manager status"""
        return {
            "circuit_breaker_open": self.is_circuit_open,
            "failure_count": self.db_failure_count,
            "last_failure": self.last_db_failure,
            "circuit_timeout": self.circuit_breaker_timeout,
            "should_try_db": self.should_try_database()
        }

# Global fallback manager
fallback_manager = FallbackManager()