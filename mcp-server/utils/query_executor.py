import httpx
import asyncio
from typing import Dict, Any
from utils.system_manager import SystemManager

GATEWAY_TOKEN = "0e60a3fcb1d4c155fb9ca5835650af37ca5d7ec3a7e42b68ae284eb38d74883e"

class QueryExecutor:
    def __init__(self):
        self.system_manager = SystemManager()
    
    async def execute_query(self, sql: str, username: str, system_id: str = "STYR") -> Dict[str, Any]:
        """Execute SQL query on specified system"""
        try:
            gateway_url = self.system_manager.get_gateway_url(system_id)
            if not gateway_url:
                return {"success": False, "message": f"System {system_id} not found"}
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    gateway_url,
                    json={"query": sql},
                    headers={
                        "Authorization": f"Bearer {GATEWAY_TOKEN}",
                        "X-Username": username
                    },
                    timeout=30.0
                )
                return response.json()
        except Exception as e:
            return {"success": False, "message": str(e)}
    
    def execute_sync(self, sql: str, username: str, system_id: str = "STYR"):
        """Sync wrapper for execute_query"""
        return asyncio.run(self.execute_query(sql, username, system_id))