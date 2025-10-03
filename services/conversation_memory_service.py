"""
Conversation Memory Service - Phase 2A Step 3
Tracks conversation context for follow-up questions
Location: C:\service-gateway\services\conversation_memory_service.py
"""

from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import re
import logging

logger = logging.getLogger(__name__)


class ConversationMemory:
    """Manages conversation context and entity tracking"""
    
    def __init__(self, session_id: str, user_id: str):
        self.session_id = session_id
        self.user_id = user_id
        self.created_at = datetime.now()
        self.last_accessed = datetime.now()
        
        # Conversation history
        self.messages: List[Dict[str, Any]] = []
        
        # Entity tracking
        self.entities = {
            'customer_numbers': [],
            'order_numbers': [],
            'invoice_numbers': [],
            'article_numbers': [],
            'dates': [],
            'amounts': []
        }
        
        # Last query context
        self.last_query = None
        self.last_sql = None
        self.last_result_count = 0
        self.last_tables_used = []
    
    def add_message(self, role: str, content: str, metadata: Dict = None):
        """Add a message to conversation history"""
        self.last_accessed = datetime.now()
        
        message = {
            'role': role,
            'content': content,
            'timestamp': datetime.now().isoformat(),
            'metadata': metadata or {}
        }
        
        self.messages.append(message)
        
        # Extract entities from user messages
        if role == 'user':
            self._extract_entities(content)
    
    def _extract_entities(self, text: str):
        """Extract relevant entities from text"""
        # Customer numbers (typically 1-7 digits)
        customer_matches = re.findall(r'\b\d{1,7}\b', text)
        for match in customer_matches:
            num = int(match)
            if 100 <= num <= 9999999 and num not in self.entities['customer_numbers']:
                self.entities['customer_numbers'].append(num)
        
        # Order numbers (typically 5 digits)
        order_matches = re.findall(r'\border\s*#?\s*(\d{5})\b', text.lower())
        for match in order_matches:
            num = int(match)
            if num not in self.entities['order_numbers']:
                self.entities['order_numbers'].append(num)
        
        # Dates (various formats)
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # 2025-10-03
            r'\d{2}/\d{2}/\d{4}',  # 10/03/2025
            r'\d{8}',              # 20251003
        ]
        for pattern in date_patterns:
            dates = re.findall(pattern, text)
            self.entities['dates'].extend(dates)
    
    def get_context_for_query(self) -> str:
        """Generate context string for AI to understand conversation history"""
        context_parts = []
        
        # Recent conversation (last 5 messages)
        recent_messages = self.messages[-5:] if len(self.messages) > 5 else self.messages
        if recent_messages:
            context_parts.append("Recent conversation:")
            for msg in recent_messages:
                context_parts.append(f"  {msg['role']}: {msg['content'][:100]}...")
        
        # Tracked entities
        if self.entities['customer_numbers']:
            context_parts.append(f"Referenced customers: {', '.join(map(str, self.entities['customer_numbers'][-3:]))}")
        
        if self.entities['order_numbers']:
            context_parts.append(f"Referenced orders: {', '.join(map(str, self.entities['order_numbers'][-3:]))}")
        
        # Last query info
        if self.last_query:
            context_parts.append(f"Last query: {self.last_query[:100]}")
        
        if self.last_tables_used:
            context_parts.append(f"Last tables used: {', '.join(self.last_tables_used)}")
        
        return "\n".join(context_parts)
    
    def update_query_context(self, query: str, sql: str, result_count: int, tables: List[str]):
        """Update context after executing a query"""
        self.last_query = query
        self.last_sql = sql
        self.last_result_count = result_count
        self.last_tables_used = tables
        self.last_accessed = datetime.now()
    
    def get_last_entity(self, entity_type: str) -> Optional[Any]:
        """Get the most recently mentioned entity of a type"""
        entities = self.entities.get(entity_type, [])
        return entities[-1] if entities else None
    
    def clear(self):
        """Clear conversation memory"""
        self.messages.clear()
        self.entities = {key: [] for key in self.entities.keys()}
        self.last_query = None
        self.last_sql = None


class ConversationMemoryManager:
    """Manages multiple conversation sessions"""
    
    def __init__(self, session_timeout_minutes: int = 30):
        self.sessions: Dict[str, ConversationMemory] = {}
        self.session_timeout = timedelta(minutes=session_timeout_minutes)
    
    def get_or_create_session(self, session_id: str, user_id: str) -> ConversationMemory:
        """Get existing session or create new one"""
        # Clean up expired sessions
        self._cleanup_expired_sessions()
        
        if session_id not in self.sessions:
            self.sessions[session_id] = ConversationMemory(session_id, user_id)
            logger.info(f"Created new conversation session: {session_id}")
        
        return self.sessions[session_id]
    
    def _cleanup_expired_sessions(self):
        """Remove expired sessions"""
        now = datetime.now()
        expired = []
        
        for session_id, memory in self.sessions.items():
            if now - memory.last_accessed > self.session_timeout:
                expired.append(session_id)
        
        for session_id in expired:
            del self.sessions[session_id]
            logger.info(f"Cleaned up expired session: {session_id}")
    
    def clear_session(self, session_id: str):
        """Clear a specific session"""
        if session_id in self.sessions:
            self.sessions[session_id].clear()
            logger.info(f"Cleared session: {session_id}")
    
    def get_active_session_count(self) -> int:
        """Get number of active sessions"""
        return len(self.sessions)


# Global instance
conversation_manager = ConversationMemoryManager()