# test_connection.py - Create this file to test
import asyncio
from database.styr_connector import StyrDatabaseConnector

async def test():
    db = StyrDatabaseConnector()
    connected = await db.connect()
    print(f"Connected: {connected}")
    
    if connected:
        query = """
        SELECT OHONR as order_number, OHRON as order_version, OHKNR as customer_number, OHKNB as customer_name, OHDAÖ as order_date, OHVAL as order_value, ORORN as line_number, ORANR as article_number, ORKVB as ordered_qty, ORVAL as line_value 
        FROM webapi.vt_ohkordh 
        INNER JOIN webapi.vt_orkordr ON OHONR = ORONR AND OHRON = ORRON 
        WHERE OHFNR = 2220477 
        ORDER BY OHONR DESC, ORORN ASC
        """
        results = await db.execute_query(query)
        print(f"Results: {len(results)} rows")
        if results:
            print(results[0])  # First row
    
    await db.disconnect()

asyncio.run(test())