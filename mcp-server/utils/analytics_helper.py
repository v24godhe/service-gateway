"""
Analytics Helper - SQL Generation for System Performance Analysis
Generates SQL queries against query_history, performance_metrics, cache_statistics
"""

from openai import OpenAI
import os
from datetime import datetime

# Analytics Database Schema
ANALYTICS_SCHEMA = """
DATABASE: query_learning_db (MSSQL on 192.168.1.42)

TABLE: query_history
- id (int)
- user_id (nvarchar) - username
- user_role (nvarchar) - role name
- question (nvarchar) - user's question
- sql_generated (nvarchar) - generated SQL
- execution_time_ms (int) - query execution time
- success (bit) - 1=success, 0=failed
- error_message (nvarchar) - error if failed
- row_count (int) - rows returned
- timestamp (datetime2) - when executed
- session_id (nvarchar)
- system_id (varchar) - STYR/JEEVES/ASTRO

TABLE: performance_metrics
- id (int)
- query_hash (nvarchar)
- avg_time_ms (int)
- min_time_ms (int)
- max_time_ms (int)
- execution_count (int)
- last_execution (datetime2)
- optimization_status (nvarchar) - normal/slow/optimized
- needs_review (bit)
- system_id (varchar)

TABLE: cache_statistics
- id (int)
- date (date)
- total_queries (int)
- cache_hits (int)
- cache_misses (int)
- hit_rate (decimal) - calculated
- avg_response_time_ms (int)

IMPORTANT:
- Use CAST(GETDATE() AS DATE) for today
- Use DATEADD for date ranges
- Format: execution_time_ms for performance, not execution_time
- success = 1 means successful, success = 0 means failed
"""

ANALYTICS_PROMPT = """You are an Analytics SQL Expert for system performance analysis.

Generate SQL queries to analyze:
- Query performance and execution times
- Error rates and failure patterns
- User activity and system usage
- Cache effectiveness
- System-specific metrics

RULES:
1. Query ONLY these tables: query_history, performance_metrics, cache_statistics
2. Use proper date functions: CAST(GETDATE() AS DATE) for today
3. For "today": WHERE CAST(timestamp AS DATE) = CAST(GETDATE() AS DATE)
4. For "this week": WHERE timestamp >= DATEADD(day, -7, GETDATE())
5. success = 1 is successful, success = 0 is failed
6. Always include system_id in GROUP BY when aggregating
7. Use TOP N for limiting results
8. Order by relevant columns (timestamp DESC, execution_time_ms DESC, etc.)
9. CRITICAL: Do NOT filter by user_id unless explicitly requested
   - "my errors" → add user_id filter
   - "errors today" → NO user_id filter (show all users)
   - Admin sees all data by default

Example Questions:
- "What queries failed today?" → SELECT question, error_message, timestamp FROM query_history WHERE success = 0 AND CAST(timestamp AS DATE) = CAST(GETDATE() AS DATE)
- "Slowest queries this week" → SELECT TOP 10 question, execution_time_ms, system_id FROM query_history WHERE timestamp >= DATEADD(day, -7, GETDATE()) ORDER BY execution_time_ms DESC
- "Cache hit rate today" → SELECT total_queries, cache_hits, hit_rate FROM cache_statistics WHERE date = CAST(GETDATE() AS DATE)

Generate ONLY the SQL query, no explanations.
"""


def generate_analytics_sql(question: str, username: str = "dev_admin") -> str:
    """
    Generate SQL for analytics questions using AI
    
    Args:
        question: Natural language question about system performance
        username: User asking the question
        
    Returns:
        SQL query string
    """
    try:
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": ANALYTICS_PROMPT + "\n\nDATABASE SCHEMA:\n" + ANALYTICS_SCHEMA},
                {"role": "user", "content": question}
            ],
            temperature=0.3
        )
        
        sql = response.choices[0].message.content.strip()
        
        # Clean SQL output
        sql = sql.replace("```sql", "").replace("```", "").strip()
        
        return sql
        
    except Exception as e:
        print(f"❌ Error generating analytics SQL: {e}")
        return None


def get_todays_summary_sql() -> str:
    """
    Returns SQL for Today's Summary report
    
    Includes:
    - Total queries today
    - Success rate
    - Average execution time
    - Top 3 errors
    - Slowest queries (>1000ms)
    - Most active users
    """
    return """
-- Today's Summary
DECLARE @Today DATE = CAST(GETDATE() AS DATE);

-- Metrics
SELECT 
    COUNT(*) as total_queries,
    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful_queries,
    CAST(SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as success_rate_pct,
    AVG(execution_time_ms) as avg_execution_time_ms,
    COUNT(DISTINCT user_id) as active_users,
    COUNT(DISTINCT system_id) as systems_used
FROM query_history
WHERE CAST(timestamp AS DATE) = @Today;

-- Top 3 Errors
SELECT TOP 3
    error_message,
    COUNT(*) as error_count
FROM query_history
WHERE success = 0 
AND CAST(timestamp AS DATE) = @Today
AND error_message IS NOT NULL
GROUP BY error_message
ORDER BY error_count DESC;

-- Slowest Queries (>1000ms)
SELECT TOP 5
    question,
    execution_time_ms,
    system_id,
    user_id,
    timestamp
FROM query_history
WHERE CAST(timestamp AS DATE) = @Today
AND execution_time_ms > 1000
ORDER BY execution_time_ms DESC;

-- Most Active Users
SELECT TOP 5
    user_id,
    COUNT(*) as query_count,
    AVG(execution_time_ms) as avg_time_ms
FROM query_history
WHERE CAST(timestamp AS DATE) = @Today
GROUP BY user_id
ORDER BY query_count DESC;
"""


def get_analytics_summary_text(results: dict) -> str:
    """
    Convert Today's Summary SQL results into readable text
    
    Args:
        results: Dictionary with query results
        
    Returns:
        Formatted summary text
    """
    if not results or not results.get('success'):
        return "❌ Unable to fetch today's summary"
    
    data = results.get('data', {})
    rows = data.get('rows', [])
    
    if not rows:
        return "📊 No data available for today"
    
    # This will be formatted based on actual results structure
    summary = "📊 **Today's System Performance Summary**\n\n"
    summary += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    
    return summary