from slowapi import Limiter
from slowapi.util import get_remote_address
from fastapi import Request
import time
from typing import Dict, Optional
import asyncio
from collections import defaultdict, deque

class CustomRateLimiter:
    def __init__(self):
        # Track requests per IP
        self.request_history = defaultdict(deque)
        self.blocked_ips = defaultdict(float)  # IP -> unblock_time
    
    def is_rate_limited(self, ip: str, limit: int, window: int) -> tuple[bool, Optional[float]]:
        """
        Check if IP is rate limited
        Returns: (is_limited, retry_after_seconds)
        """
        current_time = time.time()
        
        # Check if IP is blocked
        if ip in self.blocked_ips:
            if current_time < self.blocked_ips[ip]:
                retry_after = self.blocked_ips[ip] - current_time
                return True, retry_after
            else:
                del self.blocked_ips[ip]
        
        # Clean old requests outside window
        history = self.request_history[ip]
        while history and history[0] < current_time - window:
            history.popleft()
        
        # Check if limit exceeded
        if len(history) >= limit:
            # Block IP for window duration
            self.blocked_ips[ip] = current_time + window
            return True, window
        
        # Add current request
        history.append(current_time)
        return False, None

# Global limiter instances
limiter = Limiter(key_func=get_remote_address)
custom_limiter = CustomRateLimiter()

# Rate limit configurations
RATE_LIMITS = {
    "health": {"limit": 30, "window": 60},      # 30 per minute
    "tracking": {"limit": 10, "window": 60},     # 10 per minute  
    "heavy_query": {"limit": 5, "window": 60},   # 5 per minute
    "auth": {"limit": 20, "window": 60}          # 20 per minute
}

def get_rate_limit_config(endpoint: str) -> dict:
    """Get rate limit config for specific endpoint"""
    return RATE_LIMITS.get(endpoint, {"limit": 10, "window": 60})

async def check_rate_limit(request: Request, endpoint: str) -> None:
    """
    Check rate limit for endpoint and raise exception if exceeded
    """
    from fastapi import HTTPException
    
    client_ip = request.client.host
    config = get_rate_limit_config(endpoint)
    
    is_limited, retry_after = custom_limiter.is_rate_limited(
        client_ip, 
        config["limit"], 
        config["window"]
    )
    
    if is_limited:
        raise HTTPException(
            status_code=429,
            detail={
                "error": "Rate limit exceeded",
                "limit": config["limit"],
                "window": config["window"],
                "retry_after": int(retry_after) if retry_after else config["window"]
            },
            headers={"Retry-After": str(int(retry_after) if retry_after else config["window"])}
        )