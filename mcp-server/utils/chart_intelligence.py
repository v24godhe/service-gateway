import openai
import json
import os

openai.api_key = os.getenv("OPENAI_API_KEY")

async def analyze_chart_intent(question: str, system_id: str) -> dict:
    """
    Analyze user question and determine chart SQL requirements
    Returns hints for SQL generation
    """
    
    CHART_STRATEGY_PROMPT = f"""Analyze this chart request and return SQL STRATEGY (not specific column names).

        Question: {question}
        System: {system_id}

        RULES:
        1. "daily totals" or "each day" → needs GROUP BY date field + SUM/COUNT aggregation
        2. "top 10" or "best" → needs ORDER BY DESC + LIMIT 10
        3. "percentage" or "distribution" → needs percentage calculation with aggregation
        4. Time phrases ("last month", "this week") → needs date filter
        5. DO NOT specify actual column names (the SQL generator knows the schema)

        Return ONLY valid JSON (no markdown, no extra text):
        {{
        "needs_grouping": true,
        "group_by_type": "date|customer|category",
        "aggregation_function": "SUM|COUNT|AVG",
        "aggregation_field_hint": "amount|quantity|count",
        "date_filter_needed": true,
        "date_range_hint": "last_30_days|last_week|this_month",
        "order_needed": true,
        "order_direction": "DESC",
        "limit": 10,
        "chart_type": "line",
        "reasoning": "brief explanation"
        }}

        CRITICAL: 
        - DO NOT invent column names
        - Only provide STRATEGY hints (group by what TYPE of field, aggregate WHAT TYPE of data)
        - The SQL generator has the correct schema"""


    try:
        response = openai.chat.completions.create(
            model="gpt-5",
            messages=[
                {"role": "system", "content": "You are a SQL chart expert. Return only JSON."},
                {"role": "user", "content": CHART_STRATEGY_PROMPT}
            ]
        )
        
        content = response.choices[0].message.content.strip()
        
        # Remove markdown fences if present
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
            content = content.strip()
        
        result = json.loads(content)
        print(f"📊 Chart Intelligence: {result['reasoning']}")
        return result
        
    except Exception as e:
        print(f"⚠️ Chart intelligence error: {e}")
        # Fallback to basic hints
        return {
            "needs_grouping": False,
            "aggregation_needed": None,
            "date_filter_needed": False,
            "chart_type": "bar",
            "reasoning": f"Error analyzing: {str(e)}"
        }