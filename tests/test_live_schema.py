import asyncio
from database.styr_connector import StyrDatabaseConnector

async def test_live_schema():
    db = StyrDatabaseConnector()
    await db.connect()
    
    # Test the exact query from get_live_schema
    query = """
    SELECT 
        COLUMN_NAME,
        DATA_TYPE,
        CHARACTER_MAXIMUM_LENGTH as LENGTH,
        NUMERIC_PRECISION,
        COLUMN_TEXT as DESCRIPTION
    FROM QSYS2.SYSCOLUMNS
    WHERE TABLE_SCHEMA = 'DCPO'
      AND TABLE_NAME = 'KHKNDHUR'
    ORDER BY ORDINAL_POSITION
    FETCH FIRST 100 ROWS ONLY
    """
    
    try:
        results = await db.execute_query(query)
        print(f"✅ Found {len(results)} columns")
        
        if results:
            print("\nFirst 5 columns:")
            for row in results[:5]:
                print(f"  - {row.get('COLUMN_NAME')}: {row.get('DATA_TYPE')}")
        else:
            print("❌ Query returned empty results")
            
    except Exception as e:
        print(f"❌ Query failed: {e}")
    
    await db.disconnect()

asyncio.run(test_live_schema())