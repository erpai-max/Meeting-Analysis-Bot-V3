import os
import json
import logging
import google.generativeai as genai
from flask import Flask, request, jsonify
from flask_cors import CORS
import chromadb
from chromadb.utils import embedding_functions

# --- Basic Setup ---
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# --- Configuration & Secrets ---
# MODIFICATION: Now uses the GEMINI_API_KEY from your Render Environment Group
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise RuntimeError("GEMINI_API_KEY environment variable must be set.")

# --- CORS Configuration ---
CORS(app, resources={r"/chat": {"origins": "*"}}) 

# --- AI & Vector DB Setup (The Core of the RAG Model) ---
genai.configure(api_key=GEMINI_API_KEY)

# The Retrieval part still uses the free, open-source model
sentence_transformer_ef = embedding_functions.SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")
client = chromadb.Client()
collection = client.get_or_create_collection(
    name="meetings_collection", 
    embedding_function=sentence_transformer_ef
)

# --- The "Brain" of the Chatbot: The System Prompt ---
SYSTEM_PROMPT = """You are InsightBot, an expert sales analyst. Your task is to answer the user's QUESTION based *only* on the provided JSON data in the CONTEXT.

- **For summarization or analytical questions** (e.g., "Summarize improvement areas" or "What are the top 4 missed opportunities?"), you must analyze all items in the context, synthesize them, and provide a concise, actionable summary.
- **Ranking:** When asked for "top" or "most common" items, aggregate all related items from the context and present the most frequent ones in a numbered or bulleted list.
- **Direct Questions:** For direct questions (e.g., "What was the deal status for DLF Crest?"), find the specific record and answer directly.
- **Formatting:** Use Markdown for clarity, especially for lists.
- **Data Scarcity:** If the context does not contain the answer, you MUST state that the information is not available in the provided records. Do not invent information.
"""

def load_and_index_data():
    """Loads the local data file and indexes it in the vector database on startup."""
    try:
        data_path = "dashboard_data.json"
        with open(data_path, "r", encoding="utf-8") as f:
            all_meetings = json.load(f)

        documents, metadatas, ids = [], [], []
        for i, meeting in enumerate(all_meetings):
            doc_text = (
                f"Owner: {meeting.get('Owner (Who handled the meeting)')}. "
                f"Society: {meeting.get('Society Name')}. "
                f"Deal Status: {meeting.get('Deal Status')}. "
                f"Score: {meeting.get('% Score')}. "
                f"Summary: Risks were '{meeting.get('Risks / Unresolved Issues', 'N/A')}'. "
                f"Improvements needed: '{meeting.get('Improvement Areas', 'N/A')}'. "
                f"Missed opportunities: '{meeting.get('Missed Opportunities', 'N/A')}'."
            )
            documents.append(doc_text)
            metadatas.append(meeting)
            ids.append(str(i))
        
        if ids:
            collection.add(documents=documents, metadatas=metadatas, ids=ids)
            logging.info(f"Successfully indexed {len(documents)} meeting records.")
    except FileNotFoundError:
        logging.error(f"CRITICAL: '{data_path}' not found. The chatbot will not have context.")
    except Exception as e:
        logging.error(f"An error occurred during data loading/indexing: {e}")

@app.route("/chat", methods=["POST"])
def chat():
    data = request.get_json(silent=True) or {}
    question = (data.get("question") or "").strip()
    if not question:
        return jsonify({"error": "Missing 'question'"}), 400

    try:
        # RAG - RETRIEVAL
        results = collection.query(query_texts=[question], n_results=15)
        context_data = results.get('metadatas', [[]])[0]
        context_str = json.dumps(context_data, indent=2) if context_data else "[]"
        logging.info(f"Retrieved {len(context_data)} records for query.")

        # --- MODIFICATION: RAG - GENERATION now uses Gemini ---
        prompt = f"{SYSTEM_PROMPT}\n\nCONTEXT:\n{context_str}\n\nQUESTION:\n{question}\n\nANSWER:"
        model = genai.GenerativeModel("gemini-2.5-flash-preview-05-20")
        resp = model.generate_content(prompt)
        
        text = getattr(resp, "text", "") or "Sorry, I couldn’t produce an answer at this time."
        return jsonify({"answer": text})

    except Exception as e:
        # This generic error handling will catch quota errors from Gemini
        logging.error(f"Chat processing error: {e}")
        detail = "The AI service is currently unavailable. This might be due to a rate limit."
        return jsonify({"error": "Failed to process chat request.", "detail": detail}), 500

if __name__ == "__main__":
    load_and_index_data()
    port = int(os.environ.get("PORT", 8080))
    from waitress import serve
    serve(app, host="0.0.0.0", port=port)

