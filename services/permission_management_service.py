"""
Permission Management Service - Phase 2B
Handles dynamic RBAC rules and permission requests
Location: C:\\service-gateway\\services\\permission_management_service.py
"""

import pyodbc
import json
from datetime import datetime
from typing import Optional, Dict, Any, List
import logging

logger = logging.getLogger(__name__)


class PermissionManagementService:
    """Service for permission management and dynamic RBAC"""
    
    def __init__(self, connection_string: str):
        """
        Initialize Permission Management Service
        
        Args:
            connection_string: SQL Server connection string for query_learning_db
        """
        self.connection_string = connection_string
    
    def _get_connection(self):
        """Get database connection"""
        return pyodbc.connect(self.connection_string)
    
    # ================================================================
    # Super Admin Functions
    # ================================================================
    
    def is_super_admin(self, username: str) -> bool:
        """
        Check if user is a super admin
        
        Args:
            username: Username to check
            
        Returns:
            True if user is super admin
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("EXEC sp_is_super_admin @username=?", (username,))
            result = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            return result[0] == 1 if result else False
            
        except Exception as e:
            logger.error(f"Failed to check super admin status: {e}")
            return False
    
    def get_all_super_admins(self) -> List[Dict[str, Any]]:
        """Get list of all super admins"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT username, full_name, email, is_active, created_at, last_login
                FROM dbo.super_admins
                ORDER BY created_at
            """)
            
            admins = []
            for row in cursor.fetchall():
                admins.append({
                    "username": row[0],
                    "full_name": row[1],
                    "email": row[2],
                    "is_active": bool(row[3]),
                    "created_at": row[4].isoformat() if row[4] else None,
                    "last_login": row[5].isoformat() if row[5] else None
                })
            
            cursor.close()
            conn.close()
            
            return admins
            
        except Exception as e:
            logger.error(f"Failed to get super admins: {e}")
            return []
    
    def update_admin_last_login(self, username: str) -> bool:
        """Update super admin's last login timestamp"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE dbo.super_admins
                SET last_login = GETDATE()
                WHERE username = ?
            """, (username,))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to update admin last login: {e}")
            return False
    
    # ================================================================
    # Permission Request Functions
    # ================================================================
    
    def create_permission_request(
        self,
        user_id: str,
        user_role: str,
        requested_table: str,
        original_question: str,
        requested_columns: Optional[List[str]] = None,
        blocked_sql: Optional[str] = None,
        justification: Optional[str] = None
    ) -> Optional[int]:
        """
        Create a new permission request
        
        Args:
            user_id: User requesting access
            user_role: User's current role
            requested_table: Table they need access to
            original_question: Their original question
            requested_columns: Specific columns needed (optional)
            blocked_sql: The SQL that was blocked
            justification: User's reason for access
            
        Returns:
            Request ID if successful, None otherwise
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            columns_json = json.dumps(requested_columns) if requested_columns else None
            
            cursor.execute("""
                EXEC sp_create_permission_request 
                    @user_id=?, @user_role=?, @requested_table=?,
                    @requested_columns=?, @original_question=?,
                    @blocked_sql=?, @justification=?
            """, (user_id, user_role, requested_table, columns_json,
                  original_question, blocked_sql, justification))
            
            result = cursor.fetchone()
            request_id = result[0] if result else None
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Permission request created: ID {request_id} for {user_id}")
            return request_id
            
        except Exception as e:
            logger.error(f"Failed to create permission request: {e}")
            return None
    
    def get_pending_requests(self) -> List[Dict[str, Any]]:
        """
        Get all pending permission requests
        
        Returns:
            List of pending requests with details
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("EXEC sp_get_pending_requests")
            
            requests = []
            for row in cursor.fetchall():
                requests.append({
                    "request_id": row[0],
                    "user_id": row[1],
                    "user_role": row[2],
                    "requested_table": row[3],
                    "requested_columns": json.loads(row[4]) if row[4] else None,
                    "original_question": row[5],
                    "justification": row[6],
                    "priority": row[7],
                    "requested_at": row[8].isoformat() if row[8] else None,
                    "hours_pending": row[9]
                })
            
            cursor.close()
            conn.close()
            
            return requests
            
        except Exception as e:
            logger.error(f"Failed to get pending requests: {e}")
            return []
    
    def get_all_requests(self, status: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get permission requests with optional status filter
        
        Args:
            status: Filter by status (PENDING, APPROVED, DENIED, EXPIRED)
            limit: Maximum number of requests to return
            
        Returns:
            List of requests
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            if status:
                query = """
                    SELECT TOP (?) 
                        request_id, user_id, user_role, requested_table,
                        requested_columns, original_question, justification,
                        status, priority, requested_at, reviewed_by,
                        reviewed_at, review_notes
                    FROM dbo.permission_requests
                    WHERE status = ?
                    ORDER BY requested_at DESC
                """
                cursor.execute(query, (limit, status))
            else:
                query = """
                    SELECT TOP (?)
                        request_id, user_id, user_role, requested_table,
                        requested_columns, original_question, justification,
                        status, priority, requested_at, reviewed_by,
                        reviewed_at, review_notes
                    FROM dbo.permission_requests
                    ORDER BY requested_at DESC
                """
                cursor.execute(query, (limit,))
            
            requests = []
            for row in cursor.fetchall():
                requests.append({
                    "request_id": row[0],
                    "user_id": row[1],
                    "user_role": row[2],
                    "requested_table": row[3],
                    "requested_columns": json.loads(row[4]) if row[4] else None,
                    "original_question": row[5],
                    "justification": row[6],
                    "status": row[7],
                    "priority": row[8],
                    "requested_at": row[9].isoformat() if row[9] else None,
                    "reviewed_by": row[10],
                    "reviewed_at": row[11].isoformat() if row[11] else None,
                    "review_notes": row[12]
                })
            
            cursor.close()
            conn.close()
            
            return requests
            
        except Exception as e:
            logger.error(f"Failed to get requests: {e}")
            return []
    
    def approve_permission_request(
        self,
        request_id: int,
        admin_username: str,
        review_notes: Optional[str] = None,
        expires_at: Optional[datetime] = None
    ) -> bool:
        """
        Approve a permission request and update RBAC rules
        
        Args:
            request_id: Request to approve
            admin_username: Admin approving the request
            review_notes: Admin's notes
            expires_at: Expiration date for temporary access
            
        Returns:
            True if successful
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                EXEC sp_approve_permission_request
                    @request_id=?, @admin_username=?,
                    @review_notes=?, @expires_at=?
            """, (request_id, admin_username, review_notes, expires_at))
            
            result = cursor.fetchone()
            success = result[0] == 'SUCCESS' if result else False
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Permission request {request_id} approved by {admin_username}")
            return success
            
        except Exception as e:
            logger.error(f"Failed to approve permission request: {e}")
            return False
    
    def deny_permission_request(
        self,
        request_id: int,
        admin_username: str,
        review_notes: str
    ) -> bool:
        """
        Deny a permission request
        
        Args:
            request_id: Request to deny
            admin_username: Admin denying the request
            review_notes: Reason for denial
            
        Returns:
            True if successful
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                EXEC sp_deny_permission_request
                    @request_id=?, @admin_username=?, @review_notes=?
            """, (request_id, admin_username, review_notes))
            
            result = cursor.fetchone()
            success = result[0] == 'SUCCESS' if result else False
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Permission request {request_id} denied by {admin_username}")
            return success
            
        except Exception as e:
            logger.error(f"Failed to deny permission request: {e}")
            return False
    
    # ================================================================
    # Dynamic RBAC Functions
    # ================================================================
    
    def get_rbac_rules_for_role(self, user_role: str) -> Dict[str, Any]:
        """Get RBAC rules for a specific role from database"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("EXEC sp_get_rbac_rules @user_role=?", (user_role,))
            
            tables = []
            restrictions = {}
            
            for row in cursor.fetchall():
                # Access by index: rule_id, user_role, table_name, allowed_columns, blocked_columns, is_active...
                table_name = row[2]  # table_name is 3rd column
                allowed_cols = row[3]  # allowed_columns
                blocked_cols = row[4]  # blocked_columns
                
                tables.append(table_name)
                
                if allowed_cols or blocked_cols:
                    restrictions[table_name] = {
                        'allowed_columns': allowed_cols.split(',') if allowed_cols else None,
                        'blocked_columns': blocked_cols.split(',') if blocked_cols else None,
                        'notes': row[8] if len(row) > 8 else None  # notes column
                    }
            
            cursor.close()
            conn.close()
            
            logger.info(f"✅ Loaded {len(tables)} dynamic tables for {user_role}: {tables}")
            return {
                'tables': tables,
                'restrictions': restrictions
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to get RBAC rules for {user_role}: {e}")
            return {'tables': [], 'restrictions': {}}
        
    def get_all_rbac_rules(self) -> Dict[str, Any]:
        """
        Get all RBAC rules grouped by role
        
        Returns:
            Dictionary with all roles and their rules
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT user_role, table_name, allowed_columns, blocked_columns, notes
                FROM dbo.rbac_rules_dynamic
                WHERE is_active = 1
                ORDER BY user_role, table_name
            """)
            
            rules_by_role = {}
            
            for row in cursor.fetchall():
                role = row[0]
                table = row[1]
                allowed_columns = json.loads(row[2]) if row[2] else None
                blocked_columns = json.loads(row[3]) if row[3] else None
                notes = row[4]
                
                if role not in rules_by_role:
                    rules_by_role[role] = {
                        "tables": [],
                        "restrictions": {}
                    }
                
                rules_by_role[role]["tables"].append(table)
                
                if allowed_columns or blocked_columns:
                    rules_by_role[role]["restrictions"][table] = {
                        "allowed_columns": allowed_columns,
                        "blocked_columns": blocked_columns,
                        "notes": notes
                    }
            
            cursor.close()
            conn.close()
            
            return rules_by_role
            
        except Exception as e:
            logger.error(f"Failed to get all RBAC rules: {e}")
            return {}
    
    def add_rbac_rule(
        self,
        user_role: str,
        table_name: str,
        admin_username: str,
        allowed_columns: Optional[List[str]] = None,
        blocked_columns: Optional[List[str]] = None,
        notes: Optional[str] = None
    ) -> bool:
        """
        Add or update an RBAC rule
        
        Args:
            user_role: Role to grant access to
            table_name: Table to grant access to
            admin_username: Admin making the change
            allowed_columns: Specific columns allowed (None = all)
            blocked_columns: Specific columns to block
            notes: Notes about this rule
            
        Returns:
            True if successful
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            allowed_json = json.dumps(allowed_columns) if allowed_columns else None
            blocked_json = json.dumps(blocked_columns) if blocked_columns else None
            
            # Check if rule exists
            cursor.execute("""
                SELECT rule_id FROM dbo.rbac_rules_dynamic
                WHERE user_role = ? AND table_name = ?
            """, (user_role, table_name))
            
            existing = cursor.fetchone()
            
            if existing:
                # Update existing
                cursor.execute("""
                    UPDATE dbo.rbac_rules_dynamic
                    SET allowed_columns = ?,
                        blocked_columns = ?,
                        modified_by = ?,
                        modified_at = GETDATE(),
                        notes = ?,
                        is_active = 1
                    WHERE user_role = ? AND table_name = ?
                """, (allowed_json, blocked_json, admin_username, notes, user_role, table_name))
                
                action = 'RULE_MODIFIED'
            else:
                # Insert new
                cursor.execute("""
                    INSERT INTO dbo.rbac_rules_dynamic
                    (user_role, table_name, allowed_columns, blocked_columns, created_by, notes)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (user_role, table_name, allowed_json, blocked_json, admin_username, notes))
                
                action = 'RULE_ADDED'
            
            # Log audit
            cursor.execute("""
                INSERT INTO dbo.permission_audit_log
                (action_type, user_affected, admin_username, action_details)
                VALUES (?, ?, ?, ?)
            """, (action, user_role, admin_username,
                  json.dumps({"table": table_name, "notes": notes})))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"RBAC rule {action} for {user_role} on {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add RBAC rule: {e}")
            return False
    
    # ================================================================
    # Statistics & Reporting
    # ================================================================
    
    def get_permission_stats(self) -> Dict[str, Any]:
        """Get permission management statistics"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Count by status
            cursor.execute("""
                SELECT status, COUNT(*) 
                FROM dbo.permission_requests
                GROUP BY status
            """)
            status_counts = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Recent activity
            cursor.execute("""
                SELECT TOP 10 
                    action_type, admin_username, timestamp
                FROM dbo.permission_audit_log
                ORDER BY timestamp DESC
            """)
            recent_activity = [
                {
                    "action": row[0],
                    "admin": row[1],
                    "timestamp": row[2].isoformat() if row[2] else None
                }
                for row in cursor.fetchall()
            ]
            
            # Pending by user
            cursor.execute("""
                SELECT user_id, COUNT(*)
                FROM dbo.permission_requests
                WHERE status = 'PENDING'
                GROUP BY user_id
                ORDER BY COUNT(*) DESC
            """)
            pending_by_user = {row[0]: row[1] for row in cursor.fetchall()}
            
            cursor.close()
            conn.close()
            
            return {
                "status_counts": status_counts,
                "recent_activity": recent_activity,
                "pending_by_user": pending_by_user,
                "total_requests": sum(status_counts.values())
            }
            
        except Exception as e:
            logger.error(f"Failed to get permission stats: {e}")
            return {}