"""
Enhanced SQL Generator - Simple integration with permission checking
Minimal changes to existing code
"""
import streamlit as st
from utils.simple_permission_checker import SimplePermissionChecker
from utils.text_to_sql_converter import generate_sql
import asyncio
import logging

logger = logging.getLogger(__name__)

def generate_sql_with_permission_check(
    question: str, 
    username: str, 
    conversation_history=None, 
    role_context_override="",
    system_id: str = "STYR",
    enable_permission_check: bool = True
) -> str:
    """
    Enhanced SQL generation with optional permission checking
    
    Args:
        question: User question
        username: Username
        conversation_history: Conversation history
        role_context_override: Role context override
        system_id: System ID
        enable_permission_check: Whether to use permission checking
        
    Returns:
        SQL string (with permission warnings if needed)
    """
    
    if not enable_permission_check:
        # Use existing generation
        return generate_sql(question, username, conversation_history, role_context_override, system_id)
    
    try:
        # Run permission check
        checker = SimplePermissionChecker()
        result = asyncio.run(checker.check_user_permissions(username, system_id))
        
        if not result['success']:
            return f"-- Error: Could not verify permissions for {username}\n-- {result.get('error', 'Unknown error')}"
        
        allowed_tables = result['allowed_tables']
        
        if not allowed_tables:
            return f"-- Error: No table access for user {username}\n-- Please contact administrator"
        
        # Get filtered schema
        filtered_schema = asyncio.run(checker.get_filtered_schema(username, system_id))
        
        # Use existing SQL generation with filtered schema
        # This requires modifying the prompt_manager to accept custom schema
        sql = generate_sql(question, username, conversation_history, role_context_override, system_id)
        
        # Add permission info as comment
        permission_comment = f"-- User: {username} ({result['user_role']}) | Tables: {len(allowed_tables)}"
        
        return f"{permission_comment}\n{sql}"
        
    except Exception as e:
        logger.error(f"Permission-aware SQL generation failed: {e}")
        # Fallback to normal generation
        return generate_sql(question, username, conversation_history, role_context_override, system_id)

def handle_sql_generation_with_permissions(question: str, username: str):
    """
    Enhanced SQL generation handler - REPLACE existing handle_sql_generation
    """
    from utils.enhanced_text_to_sql import generate_sql_with_permission_check
    
    session_id = st.session_state.get('session_id', None)
    system_id = st.session_state.get('selected_system', 'STYR')
    
    # Check if user wants permission checking (add toggle in sidebar)
    enable_permissions = st.session_state.get('enable_permission_check', True)
    
    return generate_sql_with_permission_check(
        question=question,
        username=username,
        enable_permission_check=enable_permissions,
        system_id=system_id
    )
