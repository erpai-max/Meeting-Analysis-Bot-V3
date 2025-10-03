import os
import json
import logging
import httpx
from flask import Flask, request, jsonify
from flask_cors import CORS
import chromadb
from chromadb.utils import embedding_functions

# --- Basic Setup ---
app = Flask(__name__)
CORS(app, resources={r"/chat": {"origins": "*"}})
logging.basicConfig(level=logging.INFO)

# --- Configuration ---
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")
GEMINI_API_KEY_CHATBOT = os.environ.get("GEMINI_API_KEY_CHATBOT")  # Optional fallback
GEMINI_API_KEY_MEETINGS = os.environ.get("GEMINI_API_KEY_MEETINGS")  # Optional for other tasks

if not OPENROUTER_API_KEY:
    raise RuntimeError("OPENROUTER_API_KEY must be set in your Render Environment Group.")
logging.info("OpenRouter API key loaded successfully.")

# --- Embedding Setup (Local SentenceTransformer) ---
def get_embedding_function():
    return embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name="paraphrase-MiniLM-L3-v2"  # Lightweight model for Render free tier
    )

client = chromadb.Client()
collection = client.get_or_create_collection(
    name="meetings_collection_local",
    embedding_function=get_embedding_function()
)

# --- System Prompt ---
SYSTEM_PROMPT = """You are InsightBot, an expert sales analyst. Your task is to answer the user's QUESTION based *only* on the provided JSON data in the CONTEXT.

- **For summarization or analytical questions** (e.g., "Summarize improvement areas" or "What are the top 4 missed opportunities?"), you must first analyze all items in the context, synthesize them, and provide a concise, actionable summary.
- **Ranking:** When asked for "top" or "most common" items, aggregate all related items from the context and present the most frequent ones in a numbered or bulleted list.
- **Direct Questions:** For direct questions (e.g., "What was the deal status for DLF Crest?"), find the specific record and answer directly.
- **Formatting:** Use Markdown for clarity, especially for lists.
- **Data Scarcity:** If the context does not contain the answer, you MUST state that the information is not available in the provided records. Do not invent information.
"""

# --- Data Indexing ---
def load_and_index_data():
    try:
        if collection.count() > 0:
            logging.info(f"Index already contains {collection.count()} records. Skipping.")
            return

        logging.info("Index is empty. Starting one-time indexing process...")
        with open("dashboard_data.json", "r", encoding="utf-8") as f:
            all_meetings = json.load(f)

        documents, metadatas, ids = [], [], []
        for i, meeting in enumerate(all_meetings):
            doc_text = (
                f"Owner: {meeting.get('Owner (Who handled the meeting)')}. "
                f"Society: {meeting.get('Society Name')}. "
                f"Deal Status: {meeting.get('Deal Status')}. "
                f"Score: {meeting.get('% Score')}. "
            )
            documents.append(doc_text)
            metadatas.append(meeting)
            ids.append(str(i))

        if ids:
            logging.info(f"Indexing {len(documents)} documents using local embeddings...")
            collection.add(documents=documents, metadatas=metadatas, ids=ids)
            logging.info("Successfully indexed all meeting records.")
    except FileNotFoundError:
        logging.error("CRITICAL: 'dashboard_data.json' not found. Chatbot will have no context.")
    except Exception as e:
        logging.error(f"Error during data loading/indexing: {e}", exc_info=True)

# --- Chat Endpoint ---
@app.route("/chat", methods=["POST"])
def chat():
    data = request.get_json(silent=True) or {}
    question = (data.get("question") or "").strip()
    if not question:
        return jsonify({"error": "Missing 'question'"}), 400

    try:
        results = collection.query(query_texts=[question], n_results=15)
        context_data = results.get('metadatas', [[]])[0]
        context_str = json.dumps(context_data, indent=2) if context_data else "[]"

        response = httpx.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://your-site.com",
                "X-Title": "Meeting Analysis Bot"
            },
            json={
                "model": "deepseek/deepseek-chat-v3.1:free",
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": f"CONTEXT:\n{context_str}\n\nQUESTION:\n{question}"}
                ]
            },
            timeout=180
        )
        response.raise_for_status()
        api_data = response.json()
        text = api_data['choices'][0]['message']['content'] or "Sorry, I couldn’t produce an answer."
        return jsonify({"answer": text})

    except Exception as e:
        logging.error(f"Chat processing error: {e}", exc_info=True)
        return jsonify({"error": "Failed to process chat request."}), 500

# --- Server Start ---
if __name__ == "__main__":
    load_and_index_data()
    port = int(os.environ.get("PORT", 8080))
    from waitress import serve
    serve(app, host="0.0.0.0", port=port)
