from openai import OpenAI
import os
import json
from dotenv import load_dotenv

load_dotenv()

def get_env_variable(var_name, default=None):
    """Get env variable safely, with optional default."""
    return os.getenv(var_name, default)

OPENAI_API_KEY = get_env_variable("OPENAI_API_KEY")
GATEWAY_URL = get_env_variable("GATEWAY_URL")
GATEWAY_TOKEN = get_env_variable("GATEWAY_TOKEN")

client = OpenAI(api_key=OPENAI_API_KEY)

def generate_friendly_names(column_names: list) -> dict:
    """Generate user-friendly names for database columns using GPT-4"""
    
    if not column_names:
        return {}
    
    prompt = f"""Convert these database column names to user-friendly labels.
Return ONLY a JSON object mapping each column to its friendly name.

Column names: {', '.join(column_names)}

Example format:
{{"KHKNR": "Customer Number", "KHFKN": "Customer Name", "created_at": "Created Date"}}

Rules:
- Never add new columns always add Friendly name to the exsistng Columns
- Keep names short (2-4 words max)
- Use title case
- Remove technical prefixes
- Make it business-friendly
- Return valid JSON only, no extra text
"""
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3
        )
        
        result = response.choices[0].message.content.strip()
        
        # Remove markdown code blocks if present
        if result.startswith("```"):
            result = result.split("\n", 1)[1]
            result = result.rsplit("```", 1)[0]
        
        # Parse JSON response
        friendly_names = json.loads(result)
        return friendly_names
        
    except Exception as e:
        print(f"Error generating friendly names: {e}")
        # Fallback: return original names
        return {col: col for col in column_names}
