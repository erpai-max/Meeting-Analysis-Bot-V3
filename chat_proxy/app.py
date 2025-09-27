import os
import json
from flask import Flask, request, jsonify
from flask_cors import CORS
import google.cloud.dialogflow_v2 as dialogflow
import google.generativeai as genai

# Initialize Flask app
app = Flask(__name__)
# Allow your frontend's origin. Using "*" is okay for testing, but be more specific in production.
CORS(app, resources={r"/chat": {"origins": "*"}}) 

# --- Configuration ---
# Load credentials securely from environment variables
GEMINI_KEY = os.environ.get("GEMINI_API_KEY")
DIALOGFLOW_PROJECT_ID = os.environ.get("DIALOGFLOW_PROJECT_ID")
# A default session ID for simple tests, but the frontend should provide a unique one.
DEFAULT_SESSION_ID = "default-flask-session"
LANGUAGE_CODE = "en"

# --- Error Handling ---
# Ensure essential environment variables are set
if not GEMINI_KEY or not DIALOGFLOW_PROJECT_ID:
    raise RuntimeError("GEMINI_API_KEY and DIALOGFLOW_PROJECT_ID environment variables must be set.")

# --- API Clients Initialization ---
# Configure Gemini API client
genai.configure(api_key=GEMINI_KEY)

# Configure Dialogflow session client
# This uses Application Default Credentials (ADC). Ensure you are authenticated.
# For local testing: `gcloud auth application-default login`
session_client = dialogflow.SessionsClient()

# --- System Prompt for Generative AI ---
SYSTEM_PROMPT = """You are InsightBot, a helpful sales analyst.
Answer using ONLY the supplied CONTEXT when possible. If the answer
is not in the context, clearly state that the information is not available in the provided data.
Be concise and specific. When asked about pricing or agreement steps, give the step-by-step method
based on NBH standard SOPs (ERP/ASP/VMS pricing and workflow) if present in the context.
Format short lists with bullet points. Keep answers brief but useful.
"""

@app.route("/chat", methods=["POST"])
def chat():
    """Main chat endpoint to handle user requests."""
    data = request.get_json(silent=True) or {}
    question = (data.get("question") or "").strip()
    context = (data.get("context") or "").strip()
    
    # --- SCALABILITY IMPROVEMENT ---
    # Use a unique session ID from the frontend for each user.
    # Frontend should generate this (e.g., using crypto.randomUUID()) and send it.
    session_id = data.get("sessionId") or DEFAULT_SESSION_ID

    if not question:
        return jsonify({"error": "Missing 'question' in request body"}), 400

    # Truncate context to prevent excessively large prompts for the LLM
    if len(context) > 18000:
        context = context[-18000:]

    # --- LOGICAL FLOW ---
    # 1. First, try to get a structured answer from Dialogflow.
    dialogflow_response = get_dialogflow_response(question, session_id)
    
    # If Dialogflow provided a specific, valid answer (not a fallback), return it.
    if dialogflow_response:
        return jsonify({"answer": dialogflow_response})

    # 2. If Dialogflow didn't have a specific answer, fall back to the Gemini model.
    try:
        prompt = f"{SYSTEM_PROMPT}\n\nCONTEXT:\n{context}\n\nQUESTION:\n{question}\n\nANSWER:"
        model = genai.GenerativeModel("gemini-1.5-flash") # Using a fast and capable model
        resp = model.generate_content(prompt)
        
        # Safely access the response text
        text = getattr(resp, "text", "") or "Sorry, I couldn’t produce an answer at this time."
        return jsonify({"answer": text})

    except Exception as e:
        # Handle potential API errors from the LLM (e.g., quota, content safety)
        app.logger.error(f"Gemini API error: {e}")
        msg = str(e)
        code = 500
        if "quota" in msg.lower():
            code = 429 # Too Many Requests - a more specific error code
            detail = "The generative AI model is currently at its rate limit. Please try again shortly."
        else:
            detail = "An unexpected error occurred with the AI service."
            
        return jsonify({"error": "LLM service error", "detail": detail}), code

def get_dialogflow_response(question, session_id):
    """
    Gets a response from Dialogflow and intelligently checks if it's a fallback.
    Returns the fulfillment text if a specific intent is matched, otherwise returns None.
    """
    try:
        # --- SCALABILITY IMPROVEMENT ---
        # Create a unique session path for each user's conversation
        dialogflow_session = session_client.session_path(DIALOGFLOW_PROJECT_ID, session_id)

        text_input = dialogflow.types.TextInput(text=question, language_code=LANGUAGE_CODE)
        query_input = dialogflow.types.QueryInput(text=text_input)

        response = session_client.detect_intent(session=dialogflow_session, query_input=query_input)
        query_result = response.query_result
        
        # --- LOGICAL FIX ---
        # Check if the matched intent is the default fallback intent.
        # This prevents returning generic "I don't understand" answers.
        if query_result.intent.is_fallback:
            app.logger.info(f"Dialogflow hit fallback for session '{session_id}'. Passing to Gemini.")
            return None # Return None to trigger the Gemini fallback

        bot_reply = query_result.fulfillment_text
        
        if bot_reply and bot_reply.strip():
            app.logger.info(f"Dialogflow matched intent '{query_result.intent.display_name}' for session '{session_id}'.")
            return bot_reply
        else:
            return None

    except Exception as e:
        # Log any errors from the Dialogflow API but don't crash the app.
        # This allows the Gemini fallback to still be attempted.
        app.logger.error(f"Error communicating with Dialogflow for session '{session_id}': {e}")
        return None

if __name__ == "__main__":
    # Run the Flask app. Binds to 0.0.0.0 to be accessible in a containerized environment.
    # The port is read from the PORT environment variable, common in cloud platforms.
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=True) # Set debug=False in production
