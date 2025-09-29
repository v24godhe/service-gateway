from typing import List
from models.schemas import TrackingRequest, TrackingResponse, OrderData
from database.styr_connector import StyrDatabaseConnector
from utils.throttling import database_throttle
from datetime import datetime

class OrderService:
    def __init__(self, db_connector: StyrDatabaseConnector):
        self.db = db_connector
    
    @database_throttle
    async def get_order_data(self, request: TrackingRequest) -> TrackingResponse:
        try:
            query = self._build_order_query(request)
            params = self._build_query_params(request)
            
            # Throttled database call
            raw_data = await self.db.execute_query(query, params)
            
            order_data = [OrderData(**row) for row in raw_data]
            
            return TrackingResponse(
                success=True,
                message=f"Found {len(order_data)} records",
                data=order_data,
                timestamp=datetime.now()
            )
            
        except Exception as e:
            return TrackingResponse(
                success=False,
                message=f"Query failed: {str(e)}",
                data=[],
                timestamp=datetime.now()
            )
    
    # Rest of methods remain same...
    def _build_order_query(self, request: TrackingRequest) -> str:
        base_query = """
        SELECT 
            OHONR as order_number,
            OHRON as order_version, 
            OHKNR as customer_number,
            OHKNB as customer_name,
            OHDAÖ as order_date,
            OHVAL as order_value,
            ORORN as line_number,
            ORANR as article_number,
            ORKVB as ordered_qty,
            ORVAL as line_value
        FROM webapi.vt_ohkordh 
        INNER JOIN webapi.vt_orkordr ON OHONR = ORONR AND OHRON = ORRON
        WHERE 1=1
        """
        
        conditions = []
        if request.order_number:
            conditions.append("AND OHONR = ?")
        if request.customer_number:
            conditions.append("AND OHKNR = ?")
            
        query = base_query + " " + " ".join(conditions)
        query += " ORDER BY OHONR DESC, ORORN ASC"
        
        return query
    
    def _build_query_params(self, request: TrackingRequest) -> tuple:
        params = []
        if request.order_number:
            params.append(request.order_number)
        if request.customer_number:
            params.append(request.customer_number)
        return tuple(params)