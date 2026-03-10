import os, json, sys
from groq import Groq

client = Groq(api_key=os.environ["GROQ_API_KEY"])

with open("bot.py", "r") as f:
    code = f.read()

error_log = os.environ.get("ERROR_LOG", "")
print("Analyzing: " + error_log[:200])

prompt = (
    "You are a Python developer. A Telegram bot crashed with this error.\n\n"
    "ERROR LOG:\n" + error_log + "\n\n"
    "bot.py code:\n" + code[:10000] + "\n\n"
    "Reply ONLY with JSON, no markdown:\n"
    '{"diagnosis": "what is wrong", "fixes": [{"old_code": "exact text to replace", "new_code": "fixed text", "reason": "why"}]}'
)

response = client.chat.completions.create(
    model="llama-3.3-70b-versatile",
    messages=[{"role": "user", "content": prompt}],
    max_tokens=1000,
    temperature=0.1
)

result = response.choices[0].message.content.strip()
result = result.replace("```json", "").replace("```", "").strip()

try:
    fix_data = json.loads(result)
    print("Diagnosis: " + fix_data.get("diagnosis", "unknown"))

    new_code = code
    applied = 0

    for fix in fix_data.get("fixes", []):
        old = fix["old_code"]
        new_text = fix["new_code"]
        if old in new_code:
            new_code = new_code.replace(old, new_text, 1)
            applied += 1
            print("Applied: " + fix.get("reason", "fix"))
        else:
            print("Not found: " + repr(old[:60]))

    if applied > 0:
        with open("bot.py", "w") as f:
            f.write(new_code)
        print(f"Total fixes applied: {applied}")
    else:
        print("No fixes could be applied")
        sys.exit(1)

except Exception as e:
    print("Error: " + str(e))
    print("Response: " + result[:500])
    sys.exit(1)
