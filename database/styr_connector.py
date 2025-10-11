import pyodbc
import os
import asyncio
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
from database.base import DatabaseConnector
from utils.fallback import fallback_manager

load_dotenv()

class StyrDatabaseConnector(DatabaseConnector):
    def __init__(self, system=None, userid=None, password=None):
        self.system = system or os.getenv('AS400_SYSTEM', os.getenv('STYR_SYSTEM'))
        self.userid = userid or os.getenv('AS400_USERID', os.getenv('STYR_USERID'))
        self.password = password or os.getenv('AS400_PASSWORD', os.getenv('STYR_PASSWORD'))
        self.connection = None
        self.connection_healthy = False
    
    async def connect(self):
        try:
            connection_string = f"""
            DRIVER={{IBM i Access ODBC Driver}};
            SYSTEM={self.system};
            USERID={self.userid};
            PASSWORD={self.password};
            ALLOWPROCCALLS=1;
            SORTTABLE=1;
            """
            
            self.connection = pyodbc.connect(connection_string)
            self.connection_healthy = True
            fallback_manager.record_db_success()
            print(f"✅ Connected to AS400 system: {self.system}")
            return True
            
        except Exception as e:
            self.connection_healthy = False
            fallback_manager.record_db_failure()
            print(f"AS400 connection failed: {e}")
            return False
    
    async def disconnect(self):
        if self.connection:
            try:
                self.connection.close()
                self.connection_healthy = False
            except:
                pass
    
    async def execute_query(self, query: str, params: tuple = None) -> List[Dict[Any, Any]]:
        if not fallback_manager.should_try_database():
            raise Exception("Circuit breaker is open - database unavailable")
        
        if not self.connection or not self.connection_healthy:
            success = await self.connect()
            if not success:
                raise Exception("Database connection failed")
        
        try:
            cursor = self.connection.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
            
            results = []
            for row in rows:
                results.append(dict(zip(columns, row)))
            
            fallback_manager.record_db_success()
            return results
            
        except Exception as e:
            self.connection_healthy = False
            fallback_manager.record_db_failure()
            print(f"Query execution failed: {e}")
            raise e
    
    async def health_check(self) -> bool:
        if not fallback_manager.should_try_database():
            return False
        
        try:
            if not self.connection:
                await self.connect()
            
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1 FROM SYSIBM.SYSDUMMY1")
            cursor.fetchone()
            self.connection_healthy = True
            fallback_manager.record_db_success()
            return True
        except Exception as e:
            self.connection_healthy = False
            fallback_manager.record_db_failure()
            return False