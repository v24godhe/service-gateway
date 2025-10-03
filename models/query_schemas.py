from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any

class DynamicQueryRequest(BaseModel):
    query: str = Field(..., min_length=10, max_length=2000, description="SQL SELECT query")
    max_rows: Optional[int] = Field(100, ge=1, le=1000, description="Maximum rows to return")
    query_type: Optional[str] = Field(None, description="Query category for logging")
    
    @validator('query')
    def validate_query_basic(cls, v):
        v = v.strip()
        return v

class DynamicQueryResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None
    execution_time_ms: Optional[float] = None
    request_id: Optional[str] = None

class TableSchema(BaseModel):
    name: str
    description: str
    columns: List[Dict[str, Any]]
    examples: List[str]

class DatabaseSchema(BaseModel):
    tables: List[TableSchema]
    query_limits: Dict[str, int]