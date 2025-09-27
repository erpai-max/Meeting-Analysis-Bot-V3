import os
import json
import logging
import httpx # A modern HTTP client for making API requests to OpenRouter
from flask import Flask, request, jsonify
from flask_cors import CORS
import chromadb
from chromadb.utils import embedding_functions

# --- Basic Setup ---
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# --- Configuration & Secrets ---
# This pulls the OpenRouter API key from the environment variables set on your hosting provider (e.g., Render).
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")
if not OPENROUTER_API_KEY:
    # This will cause the server to fail on startup if the key isn't set, which is a good safety measure.
    raise RuntimeError("OPENROUTER_API_KEY environment variable must be set.")

# --- CORS Configuration ---
# This allows your dashboard (running on GitHub Pages) to communicate with this backend.
CORS(app, resources={r"/chat": {"origins": "*"}}) 

# --- AI & Vector DB Setup (The Core of the RAG Model) ---

# 1. The Embedding Function (Free & Local)
# This uses a powerful, free open-source model to understand the meaning of your text.
# The first time the server runs on Render, it will download this model into the container.
sentence_transformer_ef = embedding_functions.SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")

# 2. The Vector Database (Free & In-Memory)
# This database will store the "embeddings" of your meeting data for fast semantic search.
client = chromadb.Client()
collection = client.get_or_create_collection(
    name="meetings_collection", 
    embedding_function=sentence_transformer_ef
)

# --- The "Brain" of the Chatbot: The System Prompt ---
SYSTEM_PROMPT = """You are InsightBot, a helpful and expert sales analyst.
You will be given a QUESTION from a user and a CONTEXT containing relevant meeting data in JSON format.
Your primary goal is to answer the user's question with a high degree of accuracy, based *only* on the provided context.

- **For direct questions** (e.g., "What was the deal status for DLF Crest?"), answer directly from the data.
- **For summarization or analytical questions** (e.g., "Summarize the common improvement areas" or "What are the top 4 missed opportunities?"), you must first analyze all items in the context, synthesize them, and then provide a concise, actionable summary.
- **Ranking:** When asked for "top" or "most common" items, aggregate all related items from the context and present the most frequent ones in a numbered or bulleted list.
- **Formatting:** Use Markdown for clarity, especially for lists.
- **Data Scarcity:** If the context does not contain the information needed to answer, you MUST state that the data is not available in the provided records. Do not make up information or apologize.
"""

def load_and_index_data():
    """
    This function runs once when the server starts.
    It loads the dashboard data from the JSON file and indexes it in the vector database.
    """
    try:
        # This path is now correct because the updated GitHub Action copies
        # the data file directly into the 'chat_proxy' folder.
        data_path = "dashboard_data.json"
        
        with open(data_path, "r", encoding="utf-8") as f:
            all_meetings = json.load(f)

        documents, metadatas, ids = [], [], []
        for i, meeting in enumerate(all_meetings):
            # We create a descriptive text "document" for each meeting. This text is what
            # the embedding model will analyze to understand the meeting's content.
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
            # We store the original, full meeting data in the metadata.
            metadatas.append(meeting)
            ids.append(str(i))
        
        if ids:
            # This is the command that adds all the data to our searchable index.
            collection.add(documents=documents, metadatas=metadatas, ids=ids)
            logging.info(f"Successfully indexed {len(documents)} meeting records into the vector database.")
    except FileNotFoundError:
        logging.error(f"CRITICAL: '{data_path}' not found. The chatbot will not have context. Ensure the GitHub Action is copying the file correctly.")
    except Exception as e:
        logging.error(f"An error occurred during data loading/indexing: {e}")


@app.route("/chat", methods=["POST"])
def chat():
    data = request.get_json(silent=True) or {}
    question = (data.get("question") or "").strip()
    if not question:
        return jsonify({"error": "Missing 'question'"}), 400

    try:
        # --- RAG: RETRIEVAL STEP ---
        # The user's question is used to query the vector database.
        # It finds the 15 meeting records that are most semantically similar to the question.
        results = collection.query(query_texts=[question], n_results=15)
        
        context_data = results.get('metadatas', [[]])[0]
        context_str = json.dumps(context_data, indent=2) if context_data else "[]"
        logging.info(f"Retrieved {len(context_data)} relevant records for the query.")

        # --- RAG: GENERATION STEP ---
        # The retrieved context and the question are sent to the OpenRouter API.
        response = httpx.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            },
            json={
                "model": "nousresearch/nous-hermes-2-mixtral-8x7b-dpo", # A top-tier free model on OpenRouter
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": f"CONTEXT:\n{context_str}\n\nQUESTION:\n{question}"}
                ]
            },
            timeout=180 # A generous timeout for complex analytical queries
        )
        response.raise_for_status() # This will automatically raise an error for failed API calls (e.g., 4xx or 5xx)
        
        api_data = response.json()
        text = api_data['choices'][0]['message']['content'] or "Sorry, I couldn’t produce an answer at this time."
        return jsonify({"answer": text})

    except httpx.HTTPStatusError as e:
        logging.error(f"OpenRouter API error: {e.response.status_code} - {e.response.text}")
        return jsonify({"error": "AI service error", "detail": "The AI model provider returned an error."}), e.response.status_code
    except Exception as e:
        logging.error(f"Chat processing error: {e}")
        return jsonify({"error": "Failed to process chat request.", "detail": str(e)}), 500

if __name__ == "__main__":
    # This runs once when the server starts.
    load_and_index_data()
    
    port = int(os.environ.get("PORT", 8080))
    # 'waitress' is a production-grade server that's more robust than Flask's default development server.
    from waitress import serve
    serve(app, host="0.0.0.0", port=port)

