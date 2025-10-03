"""
Query Learning Service - Phase 2A Step 1
Handles query caching, history logging, and performance tracking
"""

import pyodbc
import hashlib
import json
from datetime import datetime
from typing import Optional, Dict, Any, List
import logging

logger = logging.getLogger(__name__)


class QueryLearningService:
    """Service for query learning, caching, and performance tracking"""
    
    def __init__(self, connection_string: str):
        """
        Initialize Query Learning Service
        
        Args:
            connection_string: SQL Server connection string for query_learning_db
        """
        self.connection_string = connection_string
    
    def _get_connection(self):
        """Get database connection"""
        return pyodbc.connect(self.connection_string)
    
    def _generate_query_hash(self, question: str, user_role: str) -> str:
        """
        Generate unique hash for query caching
        
        Args:
            question: User's question
            user_role: User's role
            
        Returns:
            SHA-256 hash string
        """
        content = f"{question.lower().strip()}_{user_role}"
        return hashlib.sha256(content.encode()).hexdigest()
    
    def log_query(
        self,
        user_id: str,
        user_role: str,
        question: str,
        sql_generated: Optional[str] = None,
        execution_time_ms: int = 0,
        success: bool = True,
        error_message: Optional[str] = None,
        row_count: int = 0,
        session_id: Optional[str] = None
    ) -> bool:
        """
        Log query execution to history
        
        Args:
            user_id: User identifier
            user_role: User's role
            question: Natural language question
            sql_generated: Generated SQL query
            execution_time_ms: Execution time in milliseconds
            success: Whether query succeeded
            error_message: Error message if failed
            row_count: Number of rows returned
            session_id: Conversation session ID
            
        Returns:
            True if logged successfully
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                EXEC sp_log_query 
                    @user_id=?, @user_role=?, @question=?, @sql_generated=?,
                    @execution_time_ms=?, @success=?, @error_message=?, 
                    @row_count=?, @session_id=?
            """, (user_id, user_role, question, sql_generated, execution_time_ms,
                  success, error_message, row_count, session_id))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Query logged for user {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to log query: {e}")
            return False
    
    def get_cached_query(
        self,
        question: str,
        user_role: str
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached query result
        
        Args:
            question: User's question
            user_role: User's role
            
        Returns:
            Cached result dict or None if not found/expired
        """
        try:
            query_hash = self._generate_query_hash(question, user_role)
            
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("SET NOCOUNT ON; EXEC sp_get_cached_query @query_hash=?", (query_hash,))
            row = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            if row:
                logger.info(f"Cache HIT for hash {query_hash}")
                return {
                    "question": row[0],
                    "sql_query": row[1],
                    "result_json": json.loads(row[2]) if row[2] else None
                }
            else:
                logger.info(f"Cache MISS for hash {query_hash}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to get cached query: {e}")
            return None
    
    def save_to_cache(
        self,
        question: str,
        user_role: str,
        sql_query: str,
        result_data: Any,
        ttl_minutes: int = 60
    ) -> bool:
        """
        Save query result to cache
        
        Args:
            question: User's question
            user_role: User's role
            sql_query: Generated SQL
            result_data: Query result to cache
            ttl_minutes: Time to live in minutes
            
        Returns:
            True if saved successfully
        """
        try:
            query_hash = self._generate_query_hash(question, user_role)
            result_json = json.dumps(result_data, default=str)
            
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                EXEC sp_save_to_cache 
                    @query_hash=?, @question=?, @sql_query=?, 
                    @result_json=?, @ttl_minutes=?
            """, (query_hash, question, sql_query, result_json, ttl_minutes))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Query cached with hash {query_hash}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save to cache: {e}")
            return False
    
    def update_performance(
        self,
        question: str,
        user_role: str,
        execution_time_ms: int
    ) -> bool:
        """
        Update performance metrics
        
        Args:
            question: User's question
            user_role: User's role
            execution_time_ms: Query execution time
            
        Returns:
            True if updated successfully
        """
        try:
            query_hash = self._generate_query_hash(question, user_role)
            
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                EXEC sp_update_performance @query_hash=?, @execution_time_ms=?
            """, (query_hash, execution_time_ms))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to update performance: {e}")
            return False
    
    def get_query_suggestions(
        self,
        user_role: str,
        limit: int = 5
    ) -> List[str]:
        """
        Get suggested queries based on user role
        
        Args:
            user_role: User's role
            limit: Number of suggestions to return
            
        Returns:
            List of suggested questions
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT TOP (?) question 
                FROM query_history 
                WHERE user_role = ? AND success = 1
                GROUP BY question
                ORDER BY COUNT(*) DESC
            """, (limit, user_role))
            
            suggestions = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            
            return suggestions
            
        except Exception as e:
            logger.error(f"Failed to get suggestions: {e}")
            return []
    
    def get_cache_statistics(self) -> Dict[str, Any]:
        """
        Get cache hit rate statistics
        
        Returns:
            Dictionary with cache statistics
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Get today's stats
            cursor.execute("""
                SELECT 
                    total_queries, 
                    cache_hits, 
                    cache_misses,
                    CAST(cache_hits AS FLOAT) / NULLIF(total_queries, 0) * 100 as hit_rate
                FROM cache_statistics 
                WHERE date = CAST(GETDATE() AS DATE)
            """)
            
            row = cursor.fetchone()
            
            if row:
                stats = {
                    "total_queries": row[0],
                    "cache_hits": row[1],
                    "cache_misses": row[2],
                    "hit_rate_percent": round(row[3], 2) if row[3] else 0
                }
            else:
                stats = {
                    "total_queries": 0,
                    "cache_hits": 0,
                    "cache_misses": 0,
                    "hit_rate_percent": 0
                }
            
            cursor.close()
            conn.close()
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get cache statistics: {e}")
            return {}
    
    def clean_expired_cache(self) -> int:
        """
        Manually clean expired cache entries
        
        Returns:
            Number of entries deleted
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("EXEC sp_clean_expired_cache")
            rows_affected = cursor.rowcount
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Cleaned {rows_affected} expired cache entries")
            return rows_affected
            
        except Exception as e:
            logger.error(f"Failed to clean cache: {e}")