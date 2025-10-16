import redis
import psycopg2
from typing import Optional, Dict
import json
import os

from dotenv import load_dotenv
load_dotenv('/home/azureuser/forlagssystem-mcp/.env')

class PromptManager:
    def __init__(self):
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.pg_conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='prompt_management_db',
            user=os.getenv('POSTGRES_USER', 'n8n_user'),
            password='Atth617QAQA'
        )

    def get_prompt(self, system_id: str, prompt_type: str, role_name: str = None) -> str:
        """Get prompt with 3-tier caching: Redis -> Postgres -> Default"""
        cache_key = f"prompt:{system_id}:{prompt_type}:{role_name or 'global'}"

        # Try Redis first
        cached = self.redis_client.get(cache_key)
        if cached:
            return cached

        # Try Postgres
        cursor = self.pg_conn.cursor()
        query = """
            SELECT prompt_content FROM prompt_configurations
            WHERE system_id = %s AND prompt_type = %s
            AND (role_name = %s OR role_name IS NULL)
            AND is_active = TRUE
            ORDER BY version DESC LIMIT 1
        """
        cursor.execute(query, (system_id, prompt_type, role_name))
        result = cursor.fetchone()

        if result:
            prompt = result[0]
            self.redis_client.setex(cache_key, 3600, prompt)  # Cache 1 hour
            return prompt

        return None

    def get_system_config(self, system_id: str) -> Optional[Dict]:
        """Get system configuration"""
        cache_key = f"system:{system_id}"

        # Try Redis
        cached = self.redis_client.get(cache_key)
        if cached:
            return json.loads(cached)

        # Try Postgres
        cursor = self.pg_conn.cursor()
        cursor.execute("""
            SELECT system_id, system_name, system_type, gateway_base_url, is_active
            FROM system_configurations WHERE system_id = %s AND is_active = TRUE
        """, (system_id,))

        result = cursor.fetchone()
        if result:
            config = {
                'system_id': result[0],
                'system_name': result[1],
                'system_type': result[2],
                'gateway_base_url': result[3],
                'is_active': result[4]
            }
            self.redis_client.setex(cache_key, 7200, json.dumps(config))  # Cache 2 hours
            return config

        return None
    
    def get_variable(self, system_id: str, variable_key: str) -> str:
        """Get prompt variable (like DATABASE_SCHEMA)"""
        cache_key = f"var:{system_id}:{variable_key}"
        
        # Try Redis
        cached = self.redis_client.get(cache_key)
        if cached:
            return cached
        
        # Try Postgres
        cursor = self.pg_conn.cursor()
        cursor.execute("""
            SELECT variable_value 
            FROM prompt_variables
            WHERE (system_id = %s OR is_global = TRUE)
              AND variable_key = %s
            ORDER BY is_global ASC
            LIMIT 1
        """, (system_id, variable_key))
        
        result = cursor.fetchone()
        cursor.close()
        
        if not result:
            return ""
        
        value = result[0]
        
        # Cache in Redis (2 hours for schema)
        self.redis_client.setex(cache_key, 7200, value)
        
        return value

    def invalidate_cache(self, system_id: str):
        """Clear cache for a system"""
        pattern = f"*{system_id}*"
        for key in self.redis_client.scan_iter(match=pattern):
            self.redis_client.delete(key)

    def _load_system_schema(self, system_id: str, user_role: str) -> str:
        """
        Load system schema with RBAC filtering from Service Gateway API
        Uses table_metadata_config and rbac_rules_dynamic
        """
        cache_key = f"schema:{system_id}:{user_role}"
        
        # Try Redis cache first
        cached = self.redis_client.get(cache_key)
        if cached:
            return cached
        
        # Call Service Gateway API
        try:
            import requests
            gateway_url = os.getenv('GATEWAY_URL', 'http://10.200.0.2:8080')
            gateway_token = os.getenv('GATEWAY_TOKEN')
            
            response = requests.get(
                f"{gateway_url}/api/{system_id}/schema-with-rbac",
                params={'user_role': user_role},
                headers={'Authorization': f'Bearer {gateway_token}'},
                timeout=10
            )
            
            if response.status_code != 200:
                return f"# Schema unavailable for {system_id}"
            
            data = response.json()
            schema_dict = data.get('schema', {})
            
            # Format schema for prompt
            schema_text = f"# DATABASE SCHEMA FOR {system_id} (Role: {user_role})\n\n"
            
            for table_name, table_info in schema_dict.items():
                if table_info.get('status') == 'pending_configuration':
                    schema_text += f"## {table_name}\n"
                    schema_text += f"⚠️ {table_info.get('message')}\n\n"
                elif table_info.get('status') == 'configured':
                    schema_text += f"## {table_name}\n"
                    columns = table_info.get('columns', [])
                    for col in columns:
                        schema_text += f"- {col['friendly_name']} ({col['column_name']})\n"
                    schema_text += "\n"
            
            # Cache for 2 hours
            self.redis_client.setex(cache_key, 7200, schema_text)
            
            return schema_text
            
        except Exception as e:
            print(f"❌ Error loading schema: {e}")
            return f"# Schema loading error: {str(e)}"
        

    def combine_prompt_with_context(self, base_prompt: str, system_id: str,  user_role: str,  additional_context: Optional[Dict] = None) -> str:
        """
        Combine base prompt with:
        - Universal variables (business rules, dates)
        - System-specific context
        - Role-specific adjustments with RBAC schema
        """
        # Load universal variables
        variables = self._load_prompt_variables(system_id)
        
        # Inject variables into prompt
        final_prompt = base_prompt
        for key, value in variables.items():
            final_prompt = final_prompt.replace(f"{{{key}}}", str(value))
        
        # Add system schema context WITH USER ROLE 👈 KEY CHANGE
        schema = self._load_system_schema(system_id, user_role)
        if schema:
            final_prompt += f"\n\n{schema}"
        
        return final_prompt
    
    def get_dynamic_schema(self, system_id: str, user_role: str) -> str:
        """
        Load system schema with RBAC filtering from Service Gateway API
        Uses table_metadata_config and rbac_rules_dynamic
        
        Args:
            system_id: System identifier (e.g., 'STYR')
            user_role: User role for RBAC filtering
        
        Returns:
            Formatted schema text for prompt injection
        """
        cache_key = f"schema:{system_id}:{user_role}"
        
        # Try Redis cache first
        cached = self.redis_client.get(cache_key)
        if cached:
            print(f"✅ Schema cache hit for {system_id}:{user_role}")
            return cached
        
        # Call Service Gateway API
        try:
            import requests
            gateway_url = os.getenv('GATEWAY_URL', 'http://10.200.0.2:8080')
            gateway_token = os.getenv('GATEWAY_TOKEN')
            
            print(f"🔍 Fetching schema from Gateway for {system_id}:{user_role}")
            
            response = requests.get(
                f"{gateway_url}/api/{system_id}/schema-with-rbac",
                params={'user_role': user_role},
                headers={'Authorization': f'Bearer {gateway_token}'} if gateway_token else {},
                timeout=10
            )
            
            if response.status_code != 200:
                error_msg = f"# Schema unavailable for {system_id} (Status: {response.status_code})"
                print(f"❌ {error_msg}")
                return error_msg
            
            data = response.json()
            schema_dict = data.get('schema', {})
            
            # Format schema for prompt
            schema_text = f"# DATABASE SCHEMA FOR {system_id} (Role: {user_role})\n\n"
            
            if not schema_dict:
                schema_text += "⚠️ No tables accessible for this role\n"
            else:
                for table_name, table_info in schema_dict.items():
                    if table_info.get('status') == 'pending_configuration':
                        schema_text += f"## {table_name}\n"
                        schema_text += f"⚠️ {table_info.get('message')}\n\n"
                    elif table_info.get('status') == 'configured':
                        schema_text += f"## {table_name}\n"
                        columns = table_info.get('columns', [])
                        for col in columns:
                            # Show technical name first, friendly name in parentheses
                            schema_text += f"- {col['column_name']}"
                            if col.get('friendly_name') and col['friendly_name'] != col['column_name']:
                                schema_text += f" ({col['friendly_name']})"
                            schema_text += f" - {col.get('data_type', 'VARCHAR')}\n"
            
            # Cache for 2 hours
            self.redis_client.setex(cache_key, 7200, schema_text)
            print(f"✅ Schema cached for {system_id}:{user_role}")
            
            return schema_text
            
        except requests.exceptions.RequestException as e:
            error_msg = f"# Schema loading error: Connection failed - {str(e)}"
            print(f"❌ {error_msg}")
            return error_msg
        except Exception as e:
            error_msg = f"# Schema loading error: {str(e)}"
            print(f"❌ {error_msg}")
            return error_msg


    def invalidate_schema_cache(self, system_id: str = None, user_role: str = None):
        """
        Invalidate schema cache when metadata or RBAC changes
        
        Args:
            system_id: Specific system to invalidate (None = all systems)
            user_role: Specific role to invalidate (None = all roles)
        """
        if system_id and user_role:
            # Specific cache
            cache_key = f"schema:{system_id}:{user_role}"
            deleted = self.redis_client.delete(cache_key)
            if deleted:
                print(f"🗑️ Invalidated schema cache for {system_id}:{user_role}")
        elif system_id:
            # All roles for this system
            pattern = f"schema:{system_id}:*"
            keys = list(self.redis_client.scan_iter(match=pattern))
            if keys:
                self.redis_client.delete(*keys)
                print(f"🗑️ Invalidated {len(keys)} schema cache keys for {system_id}")
        else:
            # All schemas
            pattern = "schema:*"
            keys = list(self.redis_client.scan_iter(match=pattern))
            if keys:
                self.redis_client.delete(*keys)
                print(f"🗑️ Invalidated {len(keys)} schema cache keys")

    def invalidate_schema_cache_api(self, system_id: str = None, user_role: str = None):
        """
        Call Gateway API to invalidate schema cache and clear local Redis cache
        This ensures both Gateway and MCP server caches are cleared
        """
        try:
            import requests
            gateway_url = os.getenv('GATEWAY_URL', 'http://10.200.0.2:8080')
            gateway_token = os.getenv('GATEWAY_TOKEN')
            
            # Call Gateway API
            response = requests.post(
                f"{gateway_url}/api/metadata/invalidate-cache",
                json={
                    'system_id': system_id or '*',
                    'user_role': user_role
                },
                headers={'Authorization': f'Bearer {gateway_token}'} if gateway_token else {},
                timeout=5
            )
            
            if response.status_code == 200:
                print(f"✅ Gateway cache invalidation successful")
            else:
                print(f"⚠️ Gateway cache invalidation failed: {response.status_code}")
            
        except Exception as e:
            print(f"⚠️ Gateway cache invalidation error: {e}")
        
        # Always clear local Redis cache regardless of API result
        self.invalidate_schema_cache(system_id, user_role)
        print(f"✅ Local Redis cache cleared for {system_id}:{user_role}")