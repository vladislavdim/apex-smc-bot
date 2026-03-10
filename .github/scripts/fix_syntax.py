import os, json, sys
from groq import Groq

client = Groq(api_key=os.environ["GROQ_API_KEY"])

with open("bot.py", "r") as f:
    code = f.read()

try:
    with open("/tmp/err.txt", "r") as f:
        error = f.read()
except:
    error = "syntax error"

print(f"Error: {error}")

prompt = (
    "You are a Python developer. Fix the syntax error in bot.py.\n\n"
    "ERROR:\n" + error + "\n\n"
    "CODE (first 8000 chars):\n" + code[:8000] + "\n\n"
    "Reply ONLY with JSON, no markdown:\n"
    '{"old_code": "exact string to replace", "new_code": "fixed string", "explanation": "what was fixed"}'
)

response = client.chat.completions.create(
    model="llama-3.3-70b-versatile",
    messages=[{"role": "user", "content": prompt}],
    max_tokens=500,
    temperature=0.1
)

result = response.choices[0].message.content.strip()
result = result.replace("```json", "").replace("```", "").strip()

try:
    fix = json.loads(result)
    old = fix["old_code"]
    new_text = fix["new_code"]
    if old in code:
        new_code = code.replace(old, new_text, 1)
        with open("bot.py", "w") as f:
            f.write(new_code)
        print("Fixed: " + fix.get("explanation", "done"))
    else:
        print("Could not find string to replace: " + repr(old[:80]))
        sys.exit(1)
except Exception as e:
    print("Parse error: " + str(e))
    print("Response: " + result[:300])
    sys.exit(1)
