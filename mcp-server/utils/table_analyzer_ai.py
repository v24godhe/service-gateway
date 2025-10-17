# File: mcp-server/utils/table_analyzer_ai.py
"""
Table Analyzer AI - Analyzes user questions to predict required tables
Stores analysis logic for permission checking and SQL generation
"""

import openai
import httpx
import asyncio
import json
import hashlib
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import os

logger = logging.getLogger(__name__)

class TableAnalyzerAI:
    """AI-powered table analyzer that learns from user questions"""
    
    def __init__(self):
        self.client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.gateway_url = os.getenv('GATEWAY_URL', 'http://10.200.0.2:8080')
        self.gateway_token = os.getenv('GATEWAY_TOKEN')
    
    async def analyze_question_for_tables(
        self, 
        question: str, 
        username: str, 
        system_id: str = "STYR"
    ) -> Dict[str, Any]:
        """
        Analyze user question to predict which tables are needed
        
        Args:
            question: User's natural language question
            username: Username for tracking
            system_id: Database system ID
            
        Returns:
            {
                'analysis_id': 'unique_id',
                'predicted_tables': ['table1', 'table2'],
                'confidence': 85,
                'reasoning': 'explanation',
                'user_permissions': {...},
                'can_execute': True/False,
                'missing_tables': [],
                'success': True
            }
        """
        
        analysis_id = self._generate_analysis_id(question, username)
        
        try:
            # Step 1: Get available tables for this system and user
            user_role = self._get_user_role(username)
            available_tables = await self._get_available_tables(system_id, user_role)
            
            if not available_tables:
                return self._create_error_result(analysis_id, "No available tables found")
            
            # Step 2: Get user's current permissions
            user_permissions = await self._get_user_permissions(user_role)
            
            # Step 3: Use AI to analyze question and predict tables
            ai_prediction = await self._ai_analyze_tables(question, available_tables, user_role)
            
            # Step 4: Check permissions against predicted tables
            permission_check = self._check_table_permissions(
                ai_prediction.get('tables', []), 
                user_permissions
            )
            
            # Step 5: Log the analysis for learning
            await self._log_analysis(
                analysis_id=analysis_id,
                username=username,
                question=question,
                predicted_tables=ai_prediction.get('tables', []),
                confidence=ai_prediction.get('confidence', 0),
                reasoning=ai_prediction.get('reasoning', ''),
                permission_result=permission_check,
                system_id=system_id
            )
            
            return {
                'analysis_id': analysis_id,
                'predicted_tables': ai_prediction.get('tables', []),
                'confidence': ai_prediction.get('confidence', 0),
                'reasoning': ai_prediction.get('reasoning', ''),
                'user_permissions': user_permissions,
                'can_execute': permission_check['can_execute'],
                'missing_tables': permission_check['missing_tables'],
                'allowed_tables': permission_check['allowed_tables'],
                'success': True,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Table analysis failed: {e}")
            return self._create_error_result(analysis_id, str(e))
    
    def _generate_analysis_id(self, question: str, username: str) -> str:
        """Generate unique analysis ID"""
        content = f"{question}_{username}_{datetime.now().isoformat()}"
        return hashlib.md5(content.encode()).hexdigest()[:16]
    
    def _get_user_role(self, username: str) -> str:
        """Get user role from username"""
        role_mapping = {
            'harold': 'ceo',
            'lars': 'finance', 
            'pontus': 'call_center',
            'peter': 'logistics',
            'linda': 'customer_service'
        }
        return role_mapping.get(username, username)
    
    async def _get_available_tables(self, system_id: str, user_role: str) -> Dict[str, str]:
        """Get available tables from schema API"""
        
        try:
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
                    
                    # Convert to table descriptions
                    tables = {}
                    for table_name, table_info in schema_dict.items():
                        columns = table_info.get('columns', [])
                        if columns:
                            sample_cols = [col.get('friendly_name', col.get('column_name', ''))[:20] for col in columns[:3]]
                            description = f"Contains: {', '.join(sample_cols)}"
                        else:
                            description = "Database table"
                        tables[table_name] = description
                    
                    return tables
                else:
                    logger.error(f"Failed to get schema: {response.status_code}")
                    return {}
                    
        except Exception as e:
            logger.error(f"Error getting available tables: {e}")
            return {}
    
    async def _get_user_permissions(self, user_role: str) -> Dict[str, Any]:
        """Get user permissions from RBAC API"""
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.gateway_url}/api/rbac/rules/{user_role}",
                    headers={'Authorization': f'Bearer {self.gateway_token}'} if self.gateway_token else {},
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    return response.json()
                else:
                    logger.error(f"Failed to get permissions: {response.status_code}")
                    return {'tables': [], 'restrictions': {}}
                    
        except Exception as e:
            logger.error(f"Error getting user permissions: {e}")
            return {'tables': [], 'restrictions': {}}
    
    async def _ai_analyze_tables(self, question: str, available_tables: Dict[str, str], user_role: str) -> Dict[str, Any]:
        """Use AI to analyze which tables are needed"""
        
        # Build table context
        table_context = "Available database tables:\n"
        for table_name, description in available_tables.items():
            table_context += f"- {table_name}: {description}\n"
        
        analysis_prompt = f"""You are a database expert analyzing which tables are needed for a user question.

USER ROLE: {user_role}
USER QUESTION: "{question}"

{table_context}

Analyze the question and determine:
1. Which tables contain data needed to answer this question
2. Consider table relationships that might require joins
3. Think about the user's role and typical data needs
4. Provide confidence level (0-100) for your prediction

Response format (JSON only):
{{
    "tables": ["DCPO.KHKNDHUR", "DCPO.OHKORDHR"],
    "confidence": 85,
    "reasoning": "Question asks about customer orders, so need customer table for names and order table for order data. High confidence because entities are clearly mentioned."
}}

Rules:
- Only include tables from the available list above
- Be specific about why each table is needed
- Consider JOIN requirements between tables
- Higher confidence for clear entity mentions, lower for ambiguous questions

Respond with JSON only."""

        try:
            response = self.client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are a database analyst. Respond only with valid JSON."},
                    {"role": "user", "content": analysis_prompt}
                ],
                temperature=0.1,
                max_tokens=500
            )
            
            content = response.choices[0].message.content.strip()
            
            try:
                result = json.loads(content)
                
                # Validate response
                if not isinstance(result.get('tables'), list):
                    result['tables'] = []
                if not isinstance(result.get('confidence'), (int, float)):
                    result['confidence'] = 50
                if not isinstance(result.get('reasoning'), str):
                    result['reasoning'] = "No reasoning provided"
                    
                return result
                
            except json.JSONDecodeError:
                logger.error(f"Failed to parse AI response: {content}")
                return {
                    'tables': [],
                    'confidence': 0,
                    'reasoning': "Failed to parse AI response"
                }
                
        except Exception as e:
            logger.error(f"AI analysis failed: {e}")
            return {
                'tables': [],
                'confidence': 0,
                'reasoning': f"AI analysis failed: {str(e)}"
            }
    
    def _check_table_permissions(self, predicted_tables: List[str], user_permissions: Dict[str, Any]) -> Dict[str, Any]:
        """Check if user has permission to access predicted tables"""
        
        allowed_user_tables = user_permissions.get('tables', [])
        
        allowed_tables = []
        missing_tables = []
        
        for table in predicted_tables:
            if table in allowed_user_tables:
                allowed_tables.append(table)
            else:
                missing_tables.append(table)
        
        can_execute = len(allowed_tables) > 0  # Can execute if at least some tables are accessible
        
        return {
            'can_execute': can_execute,
            'allowed_tables': allowed_tables,
            'missing_tables': missing_tables,
            'needs_permission_request': len(missing_tables) > 0
        }
    
    async def _log_analysis(
        self,
        analysis_id: str,
        username: str,
        question: str,
        predicted_tables: List[str],
        confidence: float,
        reasoning: str,
        permission_result: Dict[str, Any],
        system_id: str
    ):
        """Log analysis for learning purposes"""
        
        try:
            log_data = {
                "user_id": username,
                "question": f"[TABLE_ANALYSIS] {question}",
                "sql_generated": json.dumps({
                    'analysis_id': analysis_id,
                    'predicted_tables': predicted_tables,
                    'confidence': confidence,
                    'reasoning': reasoning,
                    'permission_result': permission_result,
                    'system_id': system_id
                }),
                "permission_checked": True,
                "tables_used": ",".join(predicted_tables)
            }
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.gateway_url}/api/log-simple-query",
                    json=log_data,
                    headers={'Authorization': f'Bearer {self.gateway_token}'} if self.gateway_token else {},
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    logger.info(f"Logged table analysis {analysis_id}")
                else:
                    logger.warning(f"Failed to log analysis: {response.status_code}")
                    
        except Exception as e:
            logger.error(f"Failed to log analysis: {e}")
    
    def _create_error_result(self, analysis_id: str, error_message: str) -> Dict[str, Any]:
        """Create error result structure"""
        return {
            'analysis_id': analysis_id,
            'predicted_tables': [],
            'confidence': 0,
            'reasoning': f"Analysis failed: {error_message}",
            'user_permissions': {'tables': []},
            'can_execute': False,
            'missing_tables': [],
            'allowed_tables': [],
            'success': False,
            'error': error_message,
            'timestamp': datetime.now().isoformat()
        }
    
    async def request_missing_permissions(
        self, 
        username: str, 
        missing_tables: List[str], 
        original_question: str,
        analysis_id: str
    ) -> Optional[int]:
        """Request permissions for missing tables using existing API"""
        
        if not missing_tables:
            return None
            
        try:
            user_role = self._get_user_role(username)
            
            request_data = {
                "user_role": user_role,
                "table_name": ", ".join(missing_tables),
                "allowed_columns": None,
                "blocked_columns": None,
                "notes": f"Auto-request from question: '{original_question}' (Analysis ID: {analysis_id})"
            }
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.gateway_url}/api/rbac/add-rule",
                    json=request_data,
                    headers={
                        'Authorization': f'Bearer {self.gateway_token}' if self.gateway_token else '',
                        'X-Admin-Username': 'AUTO_SYSTEM'
                    },
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    result = response.json()
                    logger.info(f"Created permission request for {username}: {missing_tables}")
                    return result.get('request_id', 1)  # Return 1 as placeholder if no ID
                else:
                    logger.error(f"Failed to create permission request: {response.status_code}")
                    return None
                    
        except Exception as e:
            logger.error(f"Permission request failed: {e}")
            return None


# Factory function
def create_table_analyzer() -> TableAnalyzerAI:
    """Create table analyzer instance"""
    return TableAnalyzerAI()


# Sync wrapper for existing code
def analyze_tables_sync(question: str, username: str, system_id: str = "STYR") -> Dict[str, Any]:
    """Synchronous wrapper for existing code compatibility"""
    analyzer = create_table_analyzer()
    return asyncio.run(analyzer.analyze_question_for_tables(question, username, system_id))