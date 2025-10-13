from utils.prompt_manager import PromptManager
import psycopg2
import os

class SystemManager:
    def __init__(self):
        self.pm = PromptManager()
        self.db_config = {
            'host': os.getenv('POSTGRES_HOST', '10.200.0.1'),
            'port': 5432,
            'database': 'prompt_management_db',
            'user': os.getenv('POSTGRES_USER', 'n8n_user'),
            'password': os.getenv('POSTGRES_PASSWORD', 'Atth617QAQA')
        }
    
    def get_available_systems(self):
        """Get list of ACTIVE systems from database"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT system_id 
                FROM system_configurations 
                WHERE is_active = TRUE 
                ORDER BY system_id
            """)
            
            systems = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            
            return systems if systems else ["STYR"]
        except Exception as e:
            print(f"❌ Error loading systems: {e}")
            return ["STYR"]
    
    def get_system_config(self, system_id: str):
        """Get system configuration"""
        return self.pm.get_system_config(system_id)
    
    def get_gateway_url(self, system_id: str):
        """Get gateway URL - always the same"""
        return "http://10.200.0.2:8080"