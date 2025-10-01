import os
import json
import logging
import time # Import the time module for delays
import google.generativeai as genai
from flask import Flask, request, jsonify
from flask_cors import CORS
import chromadb
from chromadb.utils import embedding_functions

# --- Basic Setup ---
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# --- Configuration & Secrets ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise RuntimeError("GEMINI_API_KEY environment variable must be set.")
logging.info(f"GEMINI_API_KEY loaded successfully.")

# --- CORS Configuration ---
CORS(app, resources={r"/chat": {"origins": "*"}}) 

# --- AI & Vector DB Setup ---
genai.configure(api_key=GEMINI_API_KEY)

# Use the lightweight Gemini API for embeddings
gemini_ef = embedding_functions.GoogleGenerativeAiEmbeddingFunction(api_key=GEMINI_API_KEY)

# Use PersistentClient to save the database to disk on Render
DB_PATH = "/var/data/chroma_db"
client = chromadb.PersistentClient(path=DB_PATH)
collection = client.get_or_create_collection(
    name="meetings_collection_gemini", 
    embedding_function=gemini_ef
)

# --- System Prompt remains the same ---
SYSTEM_PROMPT = """You are InsightBot, an expert sales analyst... (rest of prompt is the same)"""

def batch_generator(data, batch_size):
    """Yields successive n-sized chunks from a list."""
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]

def load_and_index_data():
    """Loads data and indexes it in batches to avoid rate limiting."""
    try:
        # Check if the collection is already populated
        if collection.count() > 0:
            logging.info(f"Found {collection.count()} records in the persistent database. Skipping indexing.")
            return

        logging.info("Persistent database is empty. Starting one-time batch indexing process...")
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

        # --- BATCH PROCESSING FIX ---
        # Process in batches of 20 with a 15-second delay to stay within free tier limits.
        BATCH_SIZE = 20
        DELAY_SECONDS = 15
        
        batch_num = 1
        for batch in batch_generator(all_docs, BATCH_SIZE):
            logging.info(f"Processing batch {batch_num} ({len(batch)} documents)...")
            ids = [item['id'] for item in batch]
            documents = [item['document'] for item in batch]
            metadatas = [item['metadata'] for item in batch]
            
            collection.add(documents=documents, metadatas=metadatas, ids=ids)
            
            logging.info(f"Batch {batch_num} indexed. Waiting for {DELAY_SECONDS} seconds to avoid rate limit.")
            time.sleep(DELAY_SECONDS)
            batch_num += 1

        logging.info("Successfully indexed all meeting records into persistent storage.")
        
    except FileNotFoundError:
        logging.error(f"CRITICAL: 'dashboard_data.json' not found. Chatbot will have no context.")
    except Exception as e:
        logging.error(f"An error occurred during data loading/indexing: {e}", exc_info=True)


@app.route("/chat", methods=["POST"])
def chat():
    # This function remains exactly the same as before.
    data = request.get_json(silent=True) or {}
    question = (data.get("question") or "").strip()
    if not question:
        return jsonify({"error": "Missing 'question'"}), 400

    try:
        results = collection.query(query_texts=[question], n_results=15)
        context_data = results.get('metadatas', [[]])[0]
        context_str = json.dumps(context_data, indent=2) if context_data else "[]"
        
        prompt = f"{SYSTEM_PROMPT}\n\nCONTEXT:\n{context_str}\n\nQUESTION:\n{question}\n\nANSWER:"
        model = genai.GenerativeModel("gemini-2.5-flash-preview-05-20")
        resp = model.generate_content(prompt)
        
        text = getattr(resp, "text", "") or "Sorry, I couldn’t produce an answer."
        return jsonify({"answer": text})
    except Exception as e:
        logging.error(f"Chat processing error: {e}")
        detail = "The AI service is currently unavailable. This might be due to a rate limit."
        return jsonify({"error": "Failed to process chat request.", "detail": detail}), 500


if __name__ == "__main__":
    load_and_index_data()
    port = int(os.environ.get("PORT", 8080))
    from waitress import serve
    serve(app, host="0.0.0.0", port=port)
