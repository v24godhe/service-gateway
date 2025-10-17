# File: mcp-server/utils/relationship_detector.py
"""
Enhanced Auto-detect table relationships and generate dynamic schema with full column details
Gets ALL tables first, then handles permissions during analysis for better business discovery
"""

import httpx
import asyncio
import logging
from typing import Dict, List, Optional, Any, Tuple
import json
import openai
import os

logger = logging.getLogger(__name__)

class RelationshipDetector:
    """Auto-detect table relationships with full schema access for better business discovery"""
    
    def __init__(self):
        self.client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.gateway_url = os.getenv('GATEWAY_URL', 'http://10.200.0.2:8080')
        self.gateway_token = os.getenv('GATEWAY_TOKEN')
    
    async def generate_comprehensive_schema(
        self, 
        user_role: str, 
        system_id: str = "STYR",
        include_restricted_tables: bool = True
    ) -> Dict[str, Any]:
        """
        Generate comprehensive schema with business discovery approach:
        1. Get ALL tables and columns (not permission-filtered)
        2. Detect relationships across full schema
        3. Mark accessible vs restricted tables
        4. Enable intelligent permission requests
        """
        
        try:
            # Step 1: Get ALL tables and columns (full schema)
            all_tables = await self._get_all_tables_and_columns(system_id)
            
            if not all_tables:
                return {
                    'success': False,
                    'error': f'No tables found for system {system_id}',
                    'schema_text': f'# No tables available for system {system_id}'
                }
            
            # Step 2: Get user's permission status for each table
            permission_status = await self._get_user_permission_status(user_role, list(all_tables.keys()))
            
            # Step 3: Auto-detect relationships using full schema (better AI analysis)
            relationships = await self._detect_relationships_with_ai(all_tables)
            
            # Step 4: Generate schema with permission annotations
            schema_result = self._format_comprehensive_schema_with_permissions(
                all_tables, 
                relationships,
                permission_status,
                user_role, 
                system_id,
                include_restricted_tables
            )
            
            return {
                'success': True,
                'all_tables': list(all_tables.keys()),
                'accessible_tables': permission_status['allowed_tables'],
                'restricted_tables': permission_status['denied_tables'],
                'detailed_tables': all_tables,
                'relationships': relationships,
                'permission_status': permission_status,
                'schema_text': schema_result['schema_text'],
                'accessible_schema_text': schema_result['accessible_only_schema'],
                'table_count': len(all_tables),
                'accessible_count': len(permission_status['allowed_tables'])
            }
            
        except Exception as e:
            logger.error(f"Schema generation failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'schema_text': f'# Error generating schema: {str(e)}'
            }
    
    async def _get_all_tables_and_columns(self, system_id: str) -> Dict[str, Any]:
        """Get ALL tables and columns (not filtered by permissions) for business discovery"""
        
        try:
            # Use admin/system role to get full schema
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.gateway_url}/api/{system_id}/schema-with-rbac",
                    params={'user_role': 'dev_admin'},  # Use admin role to get ALL tables
                    headers={'Authorization': f'Bearer {self.gateway_token}'} if self.gateway_token else {},
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    data = response.json()
                    schema_dict = data.get('schema', {})
                    
                    # If no tables with dev_admin, try getting from table_master directly
                    if not schema_dict:
                        return await self._get_tables_from_database_direct(system_id)
                    
                    # Process full schema
                    detailed_tables = {}
                    for table_name, table_info in schema_dict.items():
                        columns = table_info.get('columns', [])
                        
                        detailed_columns = []
                        for col in columns:
                            detailed_col = {
                                'column_name': col.get('column_name', ''),
                                'friendly_name': col.get('friendly_name', ''),
                                'data_type': col.get('data_type', 'VARCHAR'),
                                'is_visible': col.get('is_visible', True),
                                'is_nullable': col.get('is_nullable', True),
                                'max_length': col.get('max_length'),
                                'precision': col.get('precision'),
                                'scale': col.get('scale'),
                                'description': col.get('description', '')
                            }
                            detailed_columns.append(detailed_col)
                        
                        detailed_tables[table_name] = {
                            'table_description': table_info.get('description', ''),
                            'status': table_info.get('status', 'configured'),
                            'columns': detailed_columns,
                            'column_count': len(detailed_columns),
                            'visible_columns': len([c for c in detailed_columns if c['is_visible']]),
                            'table_type': self._classify_table_type(table_name, detailed_columns)
                        }
                    
                    return detailed_tables
                else:
                    logger.warning(f"Failed to get full schema: {response.status_code}")
                    return await self._get_tables_from_database_direct(system_id)
                    
        except Exception as e:
            logger.error(f"Error getting all tables: {e}")
            return {}
    
    async def _get_tables_from_database_direct(self, system_id: str) -> Dict[str, Any]:
        """Fallback: Get tables directly from table_master and table_metadata_config"""
        
        try:
            # This would query table_master and table_metadata_config directly
            # For now, return empty dict - implement if needed
            logger.info("Fallback to direct database query not implemented yet")
            return {}
            
        except Exception as e:
            logger.error(f"Direct database query failed: {e}")
            return {}
    
    async def _get_user_permission_status(self, user_role: str, all_table_names: List[str]) -> Dict[str, Any]:
        """Check user's permission status for each table"""
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.gateway_url}/api/rbac/rules/{user_role}",
                    headers={'Authorization': f'Bearer {self.gateway_token}'} if self.gateway_token else {},
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    rbac_data = response.json()
                    allowed_tables = rbac_data.get('tables', [])
                    
                    # Classify tables
                    denied_tables = [table for table in all_table_names if table not in allowed_tables]
                    
                    return {
                        'allowed_tables': allowed_tables,
                        'denied_tables': denied_tables,
                        'restrictions': rbac_data.get('restrictions', {}),
                        'user_role': user_role
                    }
                else:
                    # If permission check fails, assume no access
                    return {
                        'allowed_tables': [],
                        'denied_tables': all_table_names,
                        'restrictions': {},
                        'user_role': user_role
                    }
                    
        except Exception as e:
            logger.error(f"Permission check failed: {e}")
            return {
                'allowed_tables': [],
                'denied_tables': all_table_names,
                'restrictions': {},
                'user_role': user_role
            }
    
    def _classify_table_type(self, table_name: str, columns: List[Dict]) -> str:
        """Classify table type for better AI understanding"""
        
        table_lower = table_name.lower()
        column_names = [col['column_name'].lower() for col in columns]
        
        if 'khkn' in table_lower or any('customer' in col for col in column_names):
            return 'CUSTOMER_MASTER'
        elif 'oh' in table_lower and 'order' in table_lower:
            return 'ORDER_HEADER'
        elif 'or' in table_lower and any('line' in col for col in column_names):
            return 'ORDER_LINES'
        elif 'invoice' in table_lower or 'kr' in table_lower:
            return 'INVOICE'
        elif 'payment' in table_lower or 'ki' in table_lower:
            return 'PAYMENT'
        elif 'article' in table_lower or 'ah' in table_lower:
            return 'ARTICLE_MASTER'
        elif 'supplier' in table_lower or 'lh' in table_lower:
            return 'SUPPLIER_MASTER'
        else:
            return 'BUSINESS_DATA'
    
    async def _detect_relationships_with_ai(self, all_tables: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Use AI to detect table relationships from full schema (better analysis)"""
        
        # Build comprehensive table summary for AI
        table_summary = {}
        for table_name, table_info in all_tables.items():
            columns = []
            for col in table_info['columns']:
                col_info = {
                    'name': col['column_name'],
                    'friendly_name': col['friendly_name'],
                    'type': col['data_type']
                }
                columns.append(col_info)
            
            table_summary[table_name] = {
                'columns': columns,
                'description': table_info.get('table_description', ''),
                'type': table_info.get('table_type', 'BUSINESS_DATA'),
                'column_count': len(columns)
            }
        
        # Enhanced AI prompt with business context
        analysis_prompt = f"""Analyze this complete ERP database schema and detect business relationships.

Database Tables ({len(table_summary)} total):
{json.dumps(table_summary, indent=2)}

Detect relationships based on:
1. Column name patterns (same column names across tables = relationship)
2. Business logic (customers have orders, orders have lines, customers get invoices)
3. ERP patterns (master data → transactional data)
4. Data flow (orders → shipments → invoices → payments)

Focus on key business relationships:
- Customer to Orders (1:many)
- Orders to Order Lines (1:many) 
- Customer to Invoices (1:many)
- Articles to Order Lines (1:many)
- Suppliers to Purchase Orders (1:many)

Return JSON with detected relationships:
{{
    "relationships": [
        {{
            "primary_table": "DCPO.KHKNDHUR",
            "primary_column": "KHKNR", 
            "foreign_table": "DCPO.OHKORDHR",
            "foreign_column": "OHKNR",
            "relationship_type": "ONE_TO_MANY",
            "business_description": "Customer has many orders",
            "join_pattern": "KHKNDHUR.KHKNR = OHKORDHR.OHKNR"
        }}
    ],
    "primary_keys": [
        {{
            "table": "DCPO.KHKNDHUR",
            "column": "KHKNR",
            "description": "Customer unique identifier"
        }}
    ],
    "common_joins": [
        {{
            "description": "Customer orders with details",
            "sql_pattern": "FROM KHKNDHUR k JOIN OHKORDHR o ON k.KHKNR = o.OHKNR JOIN ORKORDRR r ON o.OHONR = r.ORONR"
        }}
    ]
}}

Be comprehensive - analyze all tables for relationships."""

        try:
            response = self.client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are an ERP database expert. Analyze the complete schema for business relationships. Respond only with valid JSON."},
                    {"role": "user", "content": analysis_prompt}
                ],
                temperature=0.1,
                max_tokens=2000
            )
            
            content = response.choices[0].message.content.strip()
            
            try:
                result = json.loads(content)
                return result.get('relationships', [])
                
            except json.JSONDecodeError:
                logger.error(f"Failed to parse AI relationship response: {content}")
                return []
                
        except Exception as e:
            logger.error(f"AI relationship detection failed: {e}")
            return []
    
    def _format_comprehensive_schema_with_permissions(
        self, 
        all_tables: Dict[str, Any], 
        relationships: List[Dict],
        permission_status: Dict[str, Any],
        user_role: str, 
        system_id: str,
        include_restricted: bool = True
    ) -> Dict[str, str]:
        """Format schema with permission annotations for business discovery"""
        
        allowed_tables = permission_status['allowed_tables']
        denied_tables = permission_status['denied_tables']
        
        # Generate full schema with permission annotations
        full_schema_lines = [
            f"# COMPREHENSIVE BUSINESS SCHEMA FOR {system_id}",
            f"# User Role: {user_role}",
            f"# Total Tables: {len(all_tables)} | Accessible: {len(allowed_tables)} | Restricted: {len(denied_tables)}",
            f"# Business Discovery Mode: Shows all tables for intelligent permission requests",
            "",
            "=== TABLE ACCESS STATUS ==="
        ]
        
        # Add accessible tables first
        if allowed_tables:
            full_schema_lines.append("\n🟢 ACCESSIBLE TABLES (can query immediately):")
            for table_name in allowed_tables:
                if table_name in all_tables:
                    table_info = all_tables[table_name]
                    full_schema_lines.append(f"\nTABLE: {table_name} ✅")
                    if table_info.get('table_description'):
                        full_schema_lines.append(f"PURPOSE: {table_info['table_description']}")
                    
                    full_schema_lines.append("COLUMNS:")
                    for col in table_info['columns']:
                        if col['is_visible']:
                            col_detail = f"  - {col['column_name']}: {col['friendly_name']}"
                            
                            # Add data type
                            data_type = col['data_type']
                            if col.get('max_length'):
                                data_type += f"({col['max_length']})"
                            col_detail += f" [{data_type}]"
                            
                            full_schema_lines.append(col_detail)
        
        # Add restricted tables for business discovery
        if denied_tables and include_restricted:
            full_schema_lines.append("\n🔒 RESTRICTED TABLES (require permission request):")
            for table_name in denied_tables:
                if table_name in all_tables:
                    table_info = all_tables[table_name]
                    full_schema_lines.append(f"\nTABLE: {table_name} 🔒 [REQUEST PERMISSION]")
                    if table_info.get('table_description'):
                        full_schema_lines.append(f"PURPOSE: {table_info['table_description']}")
                    full_schema_lines.append(f"BUSINESS VALUE: Contains {table_info.get('column_count', 0)} columns of {table_info.get('table_type', 'business')} data")
        
        # Add relationship information
        if relationships:
            full_schema_lines.append("\n=== BUSINESS RELATIONSHIPS ===")
            full_schema_lines.append("Detected data relationships for optimal queries:")
            
            for rel in relationships:
                rel_line = f"- {rel.get('primary_table', '')}.{rel.get('primary_column', '')} "
                rel_line += f"→ {rel.get('foreign_table', '')}.{rel.get('foreign_column', '')} "
                rel_line += f"({rel.get('relationship_type', 'RELATED')})"
                if rel.get('business_description'):
                    rel_line += f" - {rel['business_description']}"
                
                # Mark if relationship involves restricted tables
                primary_accessible = rel.get('primary_table') in allowed_tables
                foreign_accessible = rel.get('foreign_table') in allowed_tables
                
                if not primary_accessible or not foreign_accessible:
                    rel_line += " 🔒 [REQUIRES PERMISSION]"
                
                full_schema_lines.append(rel_line)
        
        # Add business discovery guidelines
        full_schema_lines.extend([
            "\n=== BUSINESS DISCOVERY GUIDELINES ===",
            "🟢 Accessible tables: Query immediately",
            "🔒 Restricted tables: System will auto-request permissions when needed",
            "💡 Ask about ANY data - system will guide you to right permissions",
            "",
            "=== QUERY PATTERNS ===",
            "- Customer data: Use KHKNDHUR (customers) + OHKORDHR (orders)",
            "- Sales analysis: OHKORDHR + ORKORDRR + KRKFAKTR",
            "- Financial data: KRKFAKTR (invoices) + KIINBETR (payments)",
            "- Inventory: AHARTHUR (articles) + ORKORDRR (order lines)",
            "",
            "=== TECHNICAL RULES ===",
            "- Date format: YYYYMMDD (numeric)",
            "- Active records: WHERE KHSTS='1'",
            "- Text search: UPPER(column) LIKE UPPER('%term%')",
            "- NO semicolons at end of queries"
        ])
        
        # Generate accessible-only schema for fallback
        accessible_schema_lines = [
            f"# ACCESSIBLE SCHEMA FOR {user_role}",
            f"# {len(allowed_tables)} tables available",
            ""
        ]
        
        for table_name in allowed_tables:
            if table_name in all_tables:
                table_info = all_tables[table_name]
                accessible_schema_lines.append(f"TABLE: {table_name}")
                if table_info.get('table_description'):
                    accessible_schema_lines.append(f"PURPOSE: {table_info['table_description']}")
                accessible_schema_lines.append("COLUMNS:")
                for col in table_info['columns']:
                    if col['is_visible']:
                        accessible_schema_lines.append(f"  - {col['column_name']}: {col['friendly_name']}")
                accessible_schema_lines.append("")
        
        return {
            'schema_text': '\n'.join(full_schema_lines),
            'accessible_only_schema': '\n'.join(accessible_schema_lines)
        }


# Integration functions
async def generate_dynamic_schema_for_user(user_role: str, system_id: str = "STYR") -> str:
    """Generate dynamic schema with business discovery - shows ALL tables"""
    
    detector = RelationshipDetector()
    result = await detector.generate_comprehensive_schema(user_role, system_id, include_restricted_tables=True)
    
    if result['success']:
        return result['schema_text']
    else:
        return f"# Error: {result.get('error', 'Unknown error generating schema')}"

async def generate_accessible_only_schema(user_role: str, system_id: str = "STYR") -> str:
    """Generate schema with only accessible tables (for fallback)"""
    
    detector = RelationshipDetector()
    result = await detector.generate_comprehensive_schema(user_role, system_id, include_restricted_tables=False)
    
    if result['success']:
        return result['accessible_schema_text']
    else:
        return f"# Error: {result.get('error', 'Unknown error generating schema')}"


# Test function
async def test_business_discovery():
    """Test business discovery with different user roles"""
    
    test_roles = ['ceo', 'call_center', 'finance']
    
    for role in test_roles:
        print(f"\n{'='*60}")
        print(f"Testing BUSINESS DISCOVERY for role: {role}")
        print('='*60)
        
        detector = RelationshipDetector()
        result = await detector.generate_comprehensive_schema(role)
        
        if result['success']:
            print(f"✅ Total tables: {result['table_count']}")
            print(f"✅ Accessible: {result['accessible_count']}")
            print(f"✅ Restricted: {len(result['restricted_tables'])}")
            print(f"\nAccessible tables: {result['accessible_tables']}")
            print(f"Restricted tables: {result['restricted_tables']}")
            print(f"\nSchema preview:")
            print(result['schema_text'][:800] + "...")
        else:
            print(f"❌ Error: {result['error']}")


if __name__ == "__main__":
    asyncio.run(test_business_discovery())