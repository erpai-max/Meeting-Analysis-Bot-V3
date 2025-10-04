import os
import json
import logging
import time # Import the time module for handling rate limits
import google.generativeai as genai
from flask import Flask, request, jsonify
from flask_cors import CORS
import chromadb
from chromadb.utils import embedding_functions

# --- Basic Setup ---
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# --- Configuration & Secrets ---
# This pulls the dedicated chatbot API key from your Render Environment Group.
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise RuntimeError("GEMINI_API_KEY environment variable must be set.")
logging.info("GEMINI_API_KEY loaded successfully.")

# --- CORS Configuration ---
CORS(app, resources={r"/*": {"origins": "*"}})

# --- AI & Vector DB Setup (The Core of the RAG Model) ---
genai.configure(api_key=GEMINI_API_KEY)

# --- MEMORY FIX ---
# This now uses the lightweight Gemini API for embeddings instead of a heavy local model.
# This is the key change that solves the "Out of memory" error.
gemini_ef = embedding_functions.GoogleGenerativeAiEmbeddingFunction(api_key=GEMINI_API_KEY)

# --- PERMISSION FIX ---
# Use an in-memory client. This avoids all filesystem permission errors on Render's free tier.
client = chromadb.Client()
collection = client.get_or_create_collection(
    name="meetings_collection_gemini", 
    embedding_function=gemini_ef
)

# --- The "Brain" of the Chatbot: The System Prompt ---
SYSTEM_PROMPT = """You are InsightBot, an expert sales analyst. Your task is to answer the user's QUESTION based *only* on the provided JSON data in the CONTEXT.

- **For summarization or analytical questions** (e.g., "Summarize improvement areas" or "What are the top 4 missed opportunities?"), you must first analyze all items in the context, synthesize them, and provide a concise, actionable summary.
- **Ranking:** When asked for "top" or "most common" items, aggregate all related items from the context and present the most frequent ones in a numbered or bulleted list.
- **Direct Questions:** For direct questions (e.g., "What was the deal status for DLF Crest?"), find the specific record and answer directly.
- **Formatting:** Use Markdown for clarity, especially for lists.
- **Data Scarcity:** If the context does not contain the answer, you MUST state that the information is not available in the provided records. Do not invent information.
"""

def batch_generator(data, batch_size):
    """Yields successive n-sized chunks from a list."""
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]

def load_and_index_data():
    """Loads data and indexes it in batches to avoid API rate limiting on startup."""
    try:
        if collection.count() > 0:
            logging.info(f"Index already contains {collection.count()} records. Skipping re-indexing.")
            return

        logging.info("In-memory index is empty. Starting one-time batch indexing process...")
        with open("dashboard_data.json", "r", encoding="utf-8") as f:
            all_meetings = json.load(f)

        all_docs = []
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
            all_docs.append({'id': str(i), 'document': doc_text, 'metadata': meeting})

        # --- RATE LIMIT FIX ---
        # Process in small batches with a delay to stay within the Gemini API's free tier limits.
        BATCH_SIZE = 20
        DELAY_SECONDS = 20 # A safe delay to respect free tier limits
        
        batch_num = 1
        for batch in batch_generator(all_docs, BATCH_SIZE):
            logging.info(f"Processing batch {batch_num} ({len(batch)} documents)...")
            ids = [item['id'] for item in batch]
            documents = [item['document'] for item in batch]
            metadatas = [item['metadata'] for item in batch]
            
            collection.add(documents=documents, metadatas=metadatas, ids=ids)
            
            logging.info(f"Batch {batch_num} indexed. Waiting for {DELAY_SECONDS} seconds...")
            time.sleep(DELAY_SECONDS)
            batch_num += 1

        logging.info("Successfully indexed all meeting records into memory.")
        
    except FileNotFoundError:
        logging.error(f"CRITICAL: 'dashboard_data.json' not found. Chatbot will have no context.")
    except Exception as e:
        logging.error(f"An error occurred during data loading/indexing: {e}", exc_info=True)


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
        
        # RAG - GENERATION (using Gemini)
        prompt = f"{SYSTEM_PROMPT}\n\nCONTEXT:\n{context_str}\n\nQUESTION:\n{question}\n\nANSWER:"
        model = genai.GenerativeModel("gemini-2.5-flash-preview-05-20")
        resp = model.generate_content(prompt)
        
        text = getattr(resp, "text", "") or "Sorry, I couldn’t produce an answer."
        return jsonify({"answer": text})

    except Exception as e:
        logging.error(f"Chat processing error: {e}")
        detail = "The AI service is currently unavailable. This might be due to a rate limit."
        return jsonify({"error": "Failed to process chat request.", "detail": detail}), 500

# --- Health Check ---
@app.route("/ping", methods=["GET"])
def ping():
    return jsonify({"status": "Backend is alive"})

if __name__ == "__main__":
    load_and_index_data()
    port = int(os.environ.get("PORT", 8080))
    from waitress import serve
    serve(app, host="0.0.0.0", port=port)
