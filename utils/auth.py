from fastapi import HTTPException, Header, Request
from typing import Optional
import hashlib
import os
from dotenv import load_dotenv
from utils.audit_logger import audit_logger

load_dotenv()

API_SECRET_KEY = os.getenv('API_SECRET_KEY', 'ForlagsystemGateway2024SecretKey')

async def verify_auth_token(authorization: Optional[str] = Header(None), request: Request = None):
    if not authorization:
        # Log failed authentication
        if request:
            audit_logger.log_authentication_event(
                request=request,
                event_type="missing_auth_header",
                success=False,
                details="Authorization header not provided"
            )
        raise HTTPException(status_code=401, detail="Authorization header required")
    
    try:
        token = authorization.replace("Bearer ", "")
        expected_token = hashlib.sha256(API_SECRET_KEY.encode()).hexdigest()
        
        if token != expected_token:
            # Log failed authentication
            if request:
                audit_logger.log_authentication_event(
                    request=request,
                    event_type="invalid_token",
                    success=False,
                    details="Token validation failed"
                )
            raise HTTPException(status_code=401, detail="Invalid token")
        
        # Log successful authentication
        if request:
            audit_logger.log_authentication_event(
                request=request,
                event_type="successful_auth",
                success=True,
                details="Token validation successful"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        # Log authentication error
        if request:
            audit_logger.log_authentication_event(
                request=request,
                event_type="auth_error",
                success=False,
                details=f"Authentication error: {str(e)}"
            )
        raise HTTPException(status_code=401, detail="Invalid authorization format")