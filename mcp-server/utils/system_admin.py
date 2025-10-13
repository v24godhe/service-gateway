import psycopg2
import os
from typing import List, Dict, Optional

class SystemAdmin:
    """Manages system metadata ONLY - no actual DB connections"""
    
    def __init__(self):
        self.db_config = {
            'host': os.getenv('POSTGRES_HOST', '10.200.0.1'),
            'port': 5432,
            'database': 'prompt_management_db',
            'user': os.getenv('POSTGRES_USER', 'n8n_user'),
            'password': os.getenv('POSTGRES_PASSWORD', 'Atth617QAQA')
        }
    
    def is_dev_admin(self, username: str) -> bool:
        """Check if user is dev admin"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT can_edit_prompts FROM dev_admins WHERE username = %s AND is_active = TRUE",
                (username,)
            )
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            return result[0] if result else False
        except Exception as e:
            print(f"❌ Error checking dev admin: {e}")
            return False
    
    def get_all_systems(self) -> List[Dict]:
        """Get all systems from metadata table"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Check if max_connections column exists
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'system_configurations' 
                AND column_name = 'max_connections'
            """)
            has_max_conn = cursor.fetchone() is not None
            
            # Build query based on column existence
            if has_max_conn:
                cursor.execute("""
                    SELECT system_id, system_name, system_type, gateway_base_url,
                        is_active, query_timeout_sec, max_connections, 
                        created_by, created_at, description
                    FROM system_configurations
                    ORDER BY system_id
                """)
                
                systems = []
                for row in cursor.fetchall():
                    systems.append({
                        'system_id': row[0],
                        'system_name': row[1],
                        'system_type': row[2],
                        'gateway_base_url': row[3],
                        'is_active': row[4],
                        'query_timeout_sec': row[5],
                        'max_connections': row[6] if row[6] else 10,
                        'created_by': row[7],
                        'created_at': row[8].isoformat() if row[8] else None,
                        'description': row[9]
                    })
            else:
                # No max_connections column
                cursor.execute("""
                    SELECT system_id, system_name, system_type, gateway_base_url,
                        is_active, query_timeout_sec, 
                        created_by, created_at, description
                    FROM system_configurations
                    ORDER BY system_id
                """)
                
                systems = []
                for row in cursor.fetchall():
                    systems.append({
                        'system_id': row[0],
                        'system_name': row[1],
                        'system_type': row[2],
                        'gateway_base_url': row[3],
                        'is_active': row[4],
                        'query_timeout_sec': row[5],
                        'max_connections': 10,  # Default
                        'created_by': row[6],
                        'created_at': row[7].isoformat() if row[7] else None,
                        'description': row[8]
                    })
            
            cursor.close()
            conn.close()
            
            print(f"✅ Loaded {len(systems)} systems from database")
            return systems
            
        except Exception as e:
            print(f"❌ Error getting systems: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def add_system(self, system_id: str, system_name: str, system_type: str,
                   username: str, description: str = "") -> tuple[bool, str]:
        """Add new system metadata - does NOT create connection"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Check if exists
            cursor.execute("SELECT system_id FROM system_configurations WHERE system_id = %s", (system_id,))
            if cursor.fetchone():
                cursor.close()
                conn.close()
                return False, f"System {system_id} already exists"
            
            # Gateway URL is always the same
            gateway_url = "http://10.200.0.2:8080"
            
            cursor.execute("""
                INSERT INTO system_configurations 
                (system_id, system_name, system_type, gateway_base_url, 
                 is_active, created_by, description)
                VALUES (%s, %s, %s, %s, FALSE, %s, %s)
            """, (system_id, system_name, system_type, gateway_url, username, description))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            msg = f"✅ System {system_id} added. IMPORTANT: Add connection params to Windows .env file:\n"
            msg += f"   {system_id}_SYSTEM=...\n   {system_id}_USERID=...\n   {system_id}_PASSWORD=..."
            return True, msg
            
        except Exception as e:
            print(f"❌ Error adding system: {e}")
            return False, str(e)
    
    def update_system(self, system_id: str, **kwargs) -> tuple[bool, str]:
        """Update system metadata"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            updates = []
            values = []
            for key, value in kwargs.items():
                if value is not None and key in ['system_name', 'system_type', 'description', 'query_timeout_sec', 'max_connections']:
                    updates.append(f"{key} = %s")
                    values.append(value)
            
            if not updates:
                return False, "No valid fields to update"
            
            values.append(system_id)
            query = f"UPDATE system_configurations SET {', '.join(updates)} WHERE system_id = %s"
            
            cursor.execute(query, values)
            conn.commit()
            cursor.close()
            conn.close()
            return True, "System updated successfully"
            
        except Exception as e:
            print(f"❌ Error updating system: {e}")
            return False, str(e)
    
    def toggle_system(self, system_id: str) -> tuple[bool, str]:
        """Enable/disable system"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE system_configurations 
                SET is_active = NOT is_active
                WHERE system_id = %s
                RETURNING is_active
            """, (system_id,))
            
            result = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()
            
            if result:
                status = "enabled" if result[0] else "disabled"
                return True, f"System {status}"
            return False, "System not found"
            
        except Exception as e:
            print(f"❌ Error toggling system: {e}")
            return False, str(e)
    
    def delete_system(self, system_id: str) -> tuple[bool, str]:
        """Delete system (only if not STYR)"""
        if system_id == "STYR":
            return False, "Cannot delete STYR system"
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM system_configurations WHERE system_id = %s", (system_id,))
            conn.commit()
            cursor.close()
            conn.close()
            return True, f"System {system_id} deleted"
        except Exception as e:
            return False, str(e)