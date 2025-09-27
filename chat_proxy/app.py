import os
import json
from flask import Flask, request, jsonify
from flask_cors import CORS
import google.cloud.dialogflow_v2 as dialogflow
import google.generativeai as genai

# Initialize Flask app
app = Flask(__name__)
# Allow your GitHub Pages origin or '*' while testing
CORS(app, resources={r"/chat": {"origins": "*"}})

# Dialogflow and Gemini credentials
GEMINI_KEY = os.environ.get("GEMINI_API_KEY", "")
DIALOGFLOW_PROJECT_ID = os.environ.get("DIALOGFLOW_PROJECT_ID", "")
SESSION_ID = os.environ.get("DIALOGFLOW_SESSION_ID", "unique-session-id")
LANGUAGE_CODE = "en"

# Configure Gemini API
genai.configure(api_key=GEMINI_KEY)

# Configure Dialogflow session client
session_client = dialogflow.SessionsClient()
dialogflow_session = session_client.session_path(DIALOGFLOW_PROJECT_ID, SESSION_ID)

# System prompt for Gemini (customizable)
SYSTEM_PROMPT = """You are InsightBot, a helpful sales analyst.
Answer using ONLY the supplied CONTEXT when possible. If the answer
is not in context, say you don’t have that info. Be concise and specific.
When asked about pricing/agreement steps, give the step-by-step method
based on NBH standard SOPs (ERP/ASP/VMS pricing and workflow) if present in the context.
Format short lists with bullet points. Keep answers brief but useful.
"""

@app.route("/chat", methods=["POST"])
def chat():
    # Get data from frontend (user's question and context)
    data = request.get_json(silent=True) or {}
    question = (data.get("question") or "").strip()
    context = (data.get("context") or "").strip()

    if not question:
        return jsonify({"error": "Missing 'question'"}), 400

    # If no context, fall back to using only the question
    if len(context) > 18000:  # to avoid token blowups with large context
        context = context[-18000:]

    # First try processing with Dialogflow
    dialogflow_response = get_dialogflow_response(question, context)
    
    if dialogflow_response:
        # If Dialogflow produces a valid response, return it
        return jsonify({"answer": dialogflow_response})

    # If no response from Dialogflow or want to fallback to Gemini, process via Gemini
    try:
        prompt = f"{SYSTEM_PROMPT}\n\nCONTEXT:\n{context}\n\nQUESTION:\n{question}\n\nANSWER:"
        model = genai.GenerativeModel("gemini-1.5-flash")  # You can customize model name here
        resp = model.generate_content(prompt)
        text = getattr(resp, "text", "") or "Sorry, I couldn’t produce an answer."
        return jsonify({"answer": text})

    except Exception as e:
        # Handle common errors, e.g., quota exceeded
        msg = str(e)
        code = 500
        if "quota" in msg.lower():
            code = 429
        return jsonify({"error": "LLM error", "detail": msg}), code

def get_dialogflow_response(question, context):
    """Function to get response from Dialogflow"""
    try:
        # Set up text input for Dialogflow
        text_input = dialogflow.types.TextInput(text=question, language_code=LANGUAGE_CODE)
        query_input = dialogflow.types.QueryInput(text=text_input)

        # Send request to Dialogflow
        response = session_client.detect_intent(session=dialogflow_session, query_input=query_input)
        bot_reply = response.query_result.fulfillment_text
        
        # Return Dialogflow's response if it's valid
        if bot_reply.strip():
            return bot_reply
        else:
            return None

    except Exception as e:
        # Handle any errors from Dialogflow
        print(f"Error with Dialogflow: {e}")
        return None

if __name__ == "__main__":
    # Run Flask app (hosted on 0.0.0.0, port defined by environment variable or default to 8080)
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
