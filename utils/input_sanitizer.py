import re
import html
from typing import Any, Dict, List, Union

class InputSanitizer:
    
    # SQL injection patterns
    SQL_INJECTION_PATTERNS = [
        r"(\bunion\b|\bselect\b|\binsert\b|\bupdate\b|\bdelete\b|\bdrop\b|\bcreate\b|\balter\b)",
        r"(--|#|/\*|\*/)",
        r"(\bor\b|\band\b).*?=.*?=",
        r"['\"];?\s*(or|and|union|select|insert|update|delete|drop|create|alter)",
    ]
    
    # XSS patterns
    XSS_PATTERNS = [
        r"<script.*?>.*?</script>",
        r"javascript:",
        r"on\w+\s*=",
        r"<iframe.*?>",
        r"<object.*?>",
        r"<embed.*?>"
    ]
    
    @staticmethod
    def sanitize_string(value: str, max_length: int = 1000) -> str:
        """Sanitize string input"""
        if not isinstance(value, str):
            return str(value)
        
        # Truncate if too long
        if len(value) > max_length:
            value = value[:max_length]
        
        # HTML encode
        value = html.escape(value)
        
        # Remove SQL injection patterns
        for pattern in InputSanitizer.SQL_INJECTION_PATTERNS:
            value = re.sub(pattern, "", value, flags=re.IGNORECASE)
        
        # Remove XSS patterns
        for pattern in InputSanitizer.XSS_PATTERNS:
            value = re.sub(pattern, "", value, flags=re.IGNORECASE)
        
        # Remove null bytes and control characters
        value = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', value)
        
        return value.strip()
    
    @staticmethod
    def sanitize_customer_number(value: str) -> str:
        """Sanitize customer number - only allow digits"""
        if not value:
            return value
        
        # Only keep digits
        sanitized = re.sub(r'[^0-9]', '', str(value))
        
        # Limit length
        return sanitized[:20] if sanitized else ""
    
    @staticmethod
    def sanitize_search_term(value: str) -> str:
        """Sanitize search terms"""
        if not value:
            return value
        
        # Basic sanitization but allow more characters for names
        value = InputSanitizer.sanitize_string(value, max_length=100)
        
        # Allow letters, numbers, spaces, and common name characters
        sanitized = re.sub(r'[^a-zA-Z0-9\s\-\.\'\u00C0-\u017F]', '', value)
        
        return sanitized.strip()
    
    @staticmethod 
    def sanitize_request_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """Sanitize entire request data dictionary"""
        sanitized = {}
        
        for key, value in data.items():
            sanitized_key = InputSanitizer.sanitize_string(key, max_length=50)
            
            if isinstance(value, str):
                if key.lower() in ['customer_number']:
                    sanitized[sanitized_key] = InputSanitizer.sanitize_customer_number(value)
                elif key.lower() in ['search_term', 'customer_name']:
                    sanitized[sanitized_key] = InputSanitizer.sanitize_search_term(value)
                else:
                    sanitized[sanitized_key] = InputSanitizer.sanitize_string(value)
            elif isinstance(value, (int, float)):
                # Validate numeric ranges
                if key.lower() == 'customer_number' and (value < 0 or value > 999999999):
                    sanitized[sanitized_key] = 0
                else:
                    sanitized[sanitized_key] = value
            elif isinstance(value, dict):
                sanitized[sanitized_key] = InputSanitizer.sanitize_request_data(value)
            elif isinstance(value, list):
                sanitized[sanitized_key] = [
                    InputSanitizer.sanitize_string(str(item)) if isinstance(item, str) else item 
                    for item in value[:10]  # Limit array size
                ]
            else:
                sanitized[sanitized_key] = value
        
        return sanitized

input_sanitizer = InputSanitizer()