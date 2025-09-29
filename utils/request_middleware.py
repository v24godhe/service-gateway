# utils/request_middleware.py
import time
import uuid
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

class RequestTrackingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Generate request ID
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id
        request.state.start_time = time.time()
        
        # Process request
        response = await call_next(request)
        
        # Calculate execution time
        execution_time = (time.time() - request.state.start_time) * 1000
        
        # Add headers to response
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Execution-Time-MS"] = str(round(execution_time, 2))
        
        return response