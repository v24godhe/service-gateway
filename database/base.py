from abc import ABC, abstractmethod
from typing import List, Dict, Any

class DatabaseConnector(ABC):
    @abstractmethod
    async def connect(self):
        pass
    
    @abstractmethod
    async def disconnect(self):
        pass
    
    @abstractmethod
    async def execute_query(self, query: str, params: Dict = None) -> List[Dict[Any, Any]]:
        pass
    
    @abstractmethod
    async def health_check(self) -> bool:
        pass