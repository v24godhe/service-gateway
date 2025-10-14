import openai
import os
import json

openai.api_key = os.getenv("OPENAI_API_KEY")

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
- Keep names short (2-4 words max)
- Use title case
- Remove technical prefixes
- Make it business-friendly
- Return valid JSON only, no extra text
"""
    
    try:
        response = openai.ChatCompletion.create(
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