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