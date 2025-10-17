"""
Simple Permission-Aware SQL Generation - Using Existing APIs Only
No complex table analysis - just basic permission checking
"""

import httpx
import asyncio
import logging
import json
import os
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class SimplePermissionChecker:
    """Simple permission checker using existing service-gateway APIs"""
    
    def __init__(self):
        self.gateway_url = os.getenv('GATEWAY_URL', 'http://10.200.0.2:8080')
        self.gateway_token = os.getenv('GATEWAY_TOKEN')
    
    async def check_user_permissions(self, username: str, system_id: str = "STYR") -> Dict:
        """
        Check what tables user can access using existing API
        
        Returns:
            {
                'allowed_tables': ['table1', 'table2'],
                'user_role': 'CEO',
                'success': True
            }
        """
        
        try:
            user_role = self._get_user_role(username)
            
            # Use existing API: /api/rbac/rules/{role}
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.gateway_url}/api/rbac/rules/{user_role}",
                    headers={'Authorization': f'Bearer {self.gateway_token}'} if self.gateway_token else {},
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    data = response.json()
                    return {
                        'allowed_tables': data.get('tables', []),
                        'user_role': user_role,
                        'success': True,
                        'restrictions': data.get('restrictions', {})
                    }
                else:
                    return {
                        'allowed_tables': [],
                        'user_role': user_role,
                        'success': False,
                        'error': f"API returned {response.status_code}"
                    }
                    
        except Exception as e:
            logger.error(f"Permission check failed: {e}")
            return {
                'allowed_tables': [],
                'user_role': username,
                'success': False,
                'error': str(e)
            }
    
    async def get_filtered_schema(self, username: str, system_id: str = "STYR") -> str:
        """
        Get schema filtered by user permissions using existing API
        
        Returns:
            Formatted schema string for AI prompts
        """
        
        try:
            user_role = self._get_user_role(username)
            
            # Use existing API: /api/{system_id}/schema-with-rbac
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.gateway_url}/api/{system_id}/schema-with-rbac",
                    params={'user_role': user_role},
                    headers={'Authorization': f'Bearer {self.gateway_token}'} if self.gateway_token else {},
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    data = response.json()
                    schema_dict = data.get('schema', {})
                    
                    # Format for AI prompt
                    return self._format_schema_for_ai(schema_dict, user_role, system_id)
                else:
                    return f"# Error: Could not load schema for {username} (Status: {response.status_code})"
                    
        except Exception as e:
            logger.error(f"Schema fetch failed: {e}")
            return f"# Error: Schema unavailable - {str(e)}"
    
    def _get_user_role(self, username: str) -> str:
        """Get user role from username"""
        role_mapping = {
            'harold': 'CEO',
            'lars': 'Finance', 
            'pontus': 'Call Center',
            'peter': 'Logistics',
            'linda': 'Customer Service'
        }
        return role_mapping.get(username, username)
    
    def _format_schema_for_ai(self, schema_dict: Dict, user_role: str, system_id: str) -> str:
        """Format schema for AI consumption"""
        
        lines = [
            f"# DATABASE SCHEMA FOR {system_id} - User Role: {user_role}",
            f"# You have access to {len(schema_dict)} tables",
            ""
        ]
        
        for table_name, table_info in schema_dict.items():
            lines.append(f"TABLE: {table_name}")
            
            columns = table_info.get('columns', [])
            if columns:
                lines.append("COLUMNS:")
                for col in columns[:10]:  # Limit to first 10 columns
                    col_name = col.get('column_name', 'Unknown')
                    friendly_name = col.get('friendly_name', col_name)
                    lines.append(f"  - {col_name}: {friendly_name}")
                if len(columns) > 10:
                    lines.append(f"  ... and {len(columns) - 10} more columns")
            lines.append("")
        
        return '\n'.join(lines)