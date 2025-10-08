import pyodbc
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)

class PersistentMemoryService:
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        
    def get_connection(self):
        return pyodbc.connect(self.connection_string)
    
    def create_or_get_session(self, session_id: str, user_id: str) -> bool:
        """Create new session or update last activity"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Check if session exists
                cursor.execute(
                    "SELECT session_id FROM conversation_sessions WHERE session_id = ?",
                    (session_id,)
                )
                
                if cursor.fetchone():
                    # Update last activity
                    cursor.execute(
                        "UPDATE conversation_sessions SET last_activity = GETDATE() WHERE session_id = ?",
                        (session_id,)
                    )
                else:
                    # Create new session
                    cursor.execute(
                        """INSERT INTO conversation_sessions (session_id, user_id, session_metadata) 
                           VALUES (?, ?, ?)""",
                        (session_id, user_id, json.dumps({"created": datetime.now().isoformat()}))
                    )
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error creating/getting session: {e}")
            return False
    
    def save_message(self, session_id: str, message_type: str, content: str, metadata: Dict = None) -> bool:
        """Save a message to the conversation"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                metadata_json = json.dumps(metadata) if metadata else None
                
                cursor.execute(
                    """INSERT INTO conversation_messages 
                       (session_id, message_type, message_content, message_metadata) 
                       VALUES (?, ?, ?, ?)""",
                    (session_id, message_type, content, metadata_json)
                )
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error saving message: {e}")
            return False
    
    def get_conversation_history(self, session_id: str, limit: int = 20) -> List[Dict]:
        """Get conversation history for a session"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute(
                    """SELECT message_type, message_content, message_metadata, timestamp
                       FROM conversation_messages 
                       WHERE session_id = ? 
                       ORDER BY timestamp DESC
                       OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY""",
                    (session_id, limit)
                )
                
                messages = []
                for row in cursor.fetchall():
                    metadata = json.loads(row[2]) if row[2] else {}
                    messages.append({
                        "role": row[0],
                        "content": row[1],
                        "metadata": metadata,
                        "timestamp": row[3].isoformat()
                    })
                
                return list(reversed(messages))  # Return in chronological order
                
        except Exception as e:
            logger.error(f"Error getting conversation history: {e}")
            return []
    
    def update_context(self, session_id: str, query: str = None, sql: str = None, 
                      tables: List[str] = None, result_count: int = None) -> bool:
        """Update conversation context"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Check if context exists
                cursor.execute(
                    "SELECT context_id FROM conversation_context WHERE session_id = ?",
                    (session_id,)
                )
                
                tables_str = ",".join(tables) if tables else None
                
                if cursor.fetchone():
                    # Update existing
                    cursor.execute(
                        """UPDATE conversation_context 
                           SET last_query = COALESCE(?, last_query),
                               last_sql = COALESCE(?, last_sql),
                               last_tables_used = COALESCE(?, last_tables_used),
                               result_count = COALESCE(?, result_count),
                               updated_at = GETDATE()
                           WHERE session_id = ?""",
                        (query, sql, tables_str, result_count, session_id)
                    )
                else:
                    # Create new
                    cursor.execute(
                        """INSERT INTO conversation_context 
                           (session_id, last_query, last_sql, last_tables_used, result_count) 
                           VALUES (?, ?, ?, ?, ?)""",
                        (session_id, query, sql, tables_str, result_count)
                    )
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error updating context: {e}")
            return False
    
    def get_context(self, session_id: str) -> Dict:
        """Get conversation context"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute(
                    """SELECT last_query, last_sql, last_tables_used, result_count, updated_at
                       FROM conversation_context WHERE session_id = ?""",
                    (session_id,)
                )
                
                row = cursor.fetchone()
                if row:
                    return {
                        "last_query": row[0],
                        "last_sql": row[1],
                        "last_tables": row[2].split(",") if row[2] else [],
                        "result_count": row[3],
                        "updated_at": row[4].isoformat() if row[4] else None
                    }
                
                return {}
                
        except Exception as e:
            logger.error(f"Error getting context: {e}")
            return {}
    
    def clear_session(self, session_id: str) -> bool:
        """Clear all data for a session"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Delete in correct order due to foreign keys
                cursor.execute("DELETE FROM conversation_messages WHERE session_id = ?", (session_id,))
                cursor.execute("DELETE FROM conversation_context WHERE session_id = ?", (session_id,))
                cursor.execute("DELETE FROM conversation_sessions WHERE session_id = ?", (session_id,))
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error clearing session: {e}")
            return False
    
    def cleanup_old_sessions(self, days: int = 30) -> int:
        """Clean up sessions older than specified days"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cutoff_date = datetime.now() - timedelta(days=days)
                
                # Get sessions to delete
                cursor.execute(
                    "SELECT session_id FROM conversation_sessions WHERE last_activity < ?",
                    (cutoff_date,)
                )
                
                session_ids = [row[0] for row in cursor.fetchall()]
                
                if session_ids:
                    # Delete related data
                    for session_id in session_ids:
                        cursor.execute("DELETE FROM conversation_messages WHERE session_id = ?", (session_id,))
                        cursor.execute("DELETE FROM conversation_context WHERE session_id = ?", (session_id,))
                    
                    cursor.execute(
                        "DELETE FROM conversation_sessions WHERE last_activity < ?",
                        (cutoff_date,)
                    )
                
                conn.commit()
                return len(session_ids)
                
        except Exception as e:
            logger.error(f"Error cleaning up old sessions: {e}")
            return 0
