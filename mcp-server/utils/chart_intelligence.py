from openai import OpenAI
import json
import os
from utils.prompt_manager import PromptManager
from dotenv import load_dotenv

load_dotenv()

_prompt_manager = None
def get_prompt_manager():
    global _prompt_manager
    if _prompt_manager is None:
        _prompt_manager = PromptManager()
    return _prompt_manager

def get_env_variable(var_name, default=None):
    """Get env variable safely, with optional default."""
    return os.getenv(var_name, default)

OPENAI_API_KEY = get_env_variable("OPENAI_API_KEY")
GATEWAY_URL = get_env_variable("GATEWAY_URL")
GATEWAY_TOKEN = get_env_variable("GATEWAY_TOKEN")

client = OpenAI(api_key=OPENAI_API_KEY)

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
        pm = get_prompt_manager()
        chart_prompt = pm.get_prompt("STYR", 'CHART_INTELLIGENCE', None)

        response = client.chat.completions.create(
            model="gpt-5",
            messages=[
                {"role": "system", "content": chart_prompt}]
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