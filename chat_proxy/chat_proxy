import os
from flask import Flask, request, jsonify
from flask_cors import CORS
import google.generativeai as genai

app = Flask(__name__)
# Allow your GitHub Pages origin or '*' while testing
CORS(app, resources={r"/chat": {"origins": "*"}})

GEMINI_KEY = os.environ.get("GEMINI_API_KEY", "")
MODEL_NAME = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash")
genai.configure(api_key=GEMINI_KEY)

SYSTEM_PROMPT = """You are InsightBot, a helpful sales analyst.
Answer using ONLY the supplied CONTEXT when possible. If the answer
is not in context, say you don’t have that info. Be concise and specific.
When asked about pricing/agreement steps, give the step-by-step method
based on NBH standard SOPs (ERP/ASP/VMS pricing and workflow) if present in the context.
Format short lists with bullet points. Keep answers brief but useful.
"""

@app.post("/chat")
def chat():
    if not GEMINI_KEY:
        return jsonify({"error": "Server missing GEMINI_API_KEY"}), 500

    data = request.get_json(silent=True) or {}
    question = (data.get("question") or "").strip()
    context = (data.get("context") or "").strip()

    if not question:
        return jsonify({"error": "Missing 'question'"}), 400

    # Keep context bounded to avoid token blowups
    if len(context) > 18000:
        context = context[-18000:]

    try:
        model = genai.GenerativeModel(MODEL_NAME)
        prompt = f"{SYSTEM_PROMPT}\n\nCONTEXT:\n{context}\n\nQUESTION:\n{question}\n\nANSWER:"
        resp = model.generate_content(prompt)
        text = getattr(resp, "text", "") or "Sorry, I couldn’t produce an answer."
        return jsonify({"answer": text})
    except Exception as e:
        # Map common errors
        msg = str(e)
        code = 500
        if "quota" in msg.lower():
            code = 429
        return jsonify({"error": "LLM error", "detail": msg}), code

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
