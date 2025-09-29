import asyncio
import time
from typing import Dict, Any
from functools import wraps

class DatabaseThrottler:
    def __init__(self, max_concurrent: int = 5, min_delay: float = 0.1):
        self.max_concurrent = max_concurrent
        self.min_delay = min_delay
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.last_request_time = 0
        self.request_count = 0
    
    async def throttle_database_request(self, func, *args, **kwargs):
        """Throttle database requests to prevent overload"""
        async with self.semaphore:
            # Ensure minimum delay between requests
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            
            if time_since_last < self.min_delay:
                await asyncio.sleep(self.min_delay - time_since_last)
            
            self.last_request_time = time.time()
            self.request_count += 1
            
            try:
                result = await func(*args, **kwargs)
                return result
            except Exception as e:
                # Add exponential backoff on database errors
                await asyncio.sleep(min(2 ** (self.request_count % 5), 10))
                raise e

# Global throttler instance
db_throttler = DatabaseThrottler(max_concurrent=5, min_delay=0.2)

def database_throttle(func):
    """Decorator for database operations"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        return await db_throttler.throttle_database_request(func, *args, **kwargs)
    return wrapper