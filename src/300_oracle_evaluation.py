import json
import os
import pickle
import sys
import time
from pathlib import Path
from itertools import cycle
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer
import google.generativeai as genai
from dotenv import load_dotenv

# --- CONFIGURAZIONE ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
MOCK_DIR = DATA_DIR / "mocked_edits"
INDEX_FILE = DATA_DIR / "trusted_sources_index.pkl"
RESULTS_FILE = DATA_DIR / "oracle_results.json"
ENV_PATH = BASE_DIR / ".env"

LEGIT_FILE = MOCK_DIR / "legit_edits.json"
VANDAL_FILE = MOCK_DIR / "vandal_edits.json"

MODEL_NAME = 'paraphrase-multilingual-MiniLM-L12-v2'
GEMINI_MODEL = 'gemini-2.5-flash'
TOP_K = 3
TEST_SIZE = 50 # Primi 50 per tipo

load_dotenv(dotenv_path=ENV_PATH)

# API Keys Round Robin
API_KEYS = [
    os.getenv("GEMINI_API_KEY"),
    os.getenv("GEMINI_API_KEY_2"),
    os.getenv("GEMINI_API_KEY_3")
]
API_KEYS = [k for k in API_KEYS if k]
if not API_KEYS:
    print("❌ ERRORE: Nessuna API Key trovata nel .env")
    sys.exit(1)

api_key_cycle = cycle(API_KEYS)

def get_next_api_key():
    return next(api_key_cycle)

def load_index():
    if not INDEX_FILE.exists():
        print(f"❌ Indice non trovato: {INDEX_FILE}")
        return None
    with open(INDEX_FILE, "rb") as f:
        return pickle.load(f)

def load_edits(filepath, limit=50):
    if not filepath.exists():
        print(f"⚠️ File non trovato: {filepath}")
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
        return data[:limit] # Prendi solo i primi N

def retrieve_context(query_embedding, index_data, top_k=TOP_K):
    """Trova i chunk più simili all'embedding della query."""
    corpus_embeddings = index_data["embeddings"]
    metadata = index_data["metadata"]
    
    # Calcola similarità coseno
    # Reshape query a (1, dim) se necessario
    query_embedding = query_embedding.reshape(1, -1)
    
    scores = cosine_similarity(query_embedding, corpus_embeddings)[0]
    
    # Trova indici top_k
    top_indices = np.argsort(scores)[::-1][:top_k]
    
    results = []
    for idx in top_indices:
        results.append({
            "text": metadata[idx]["text"],
            "filename": metadata[idx]["filename"],
            "score": float(scores[idx])
        })
    return results

def ask_gemini(edit, context_chunks):
    key = get_next_api_key()
    genai.configure(api_key=key)
    model = genai.GenerativeModel(GEMINI_MODEL)
    
    context_text = "\n\n".join([f"--- FONTE ({c['filename']}) ---\n{c['text']}" for c in context_chunks])
    
    prompt = f"""
    Sei un moderatore di Wikipedia esperto.
    
    CONTESTO (Estratto da fonti affidabili):
    {context_text}
    
    MODIFICA UTENTE DA ANALIZZARE:
    Utente: {edit['user']}
    Commento: "{edit['comment']}"
    Titolo Pagina: {edit['title']}
    
    TASK:
    Classifica questa modifica basandoti SOLO sul contesto fornito e sul buon senso.
    - "LEGITTIMO": Se sembra un contributo costruttivo, correzione di errori, o coerente con il contesto.
    - "VANDALISMO": Se è spam, insulti, nonsense, o contraddice palesemente i fatti noti nel contesto.
    
    Rispondi ESATTAMENTE con un oggetto JSON:
    {{ "verdict": "LEGITTIMO" o "VANDALISMO", "reasoning": "Breve spiegazione" }}
    """
    
    try:
        resp = model.generate_content(prompt, generation_config={"response_mime_type": "application/json"})
        return json.loads(resp.text)
    except Exception as e:
        print(f"⚠️ Errore Gemini: {e}")
        return {"verdict": "ERROR", "reasoning": str(e)}

def main():
    print("--- 🔮 ORACLE EVALUATION (RAG + GEMINI) ---")
    
    # 1. Carica Risorse
    index = load_index()
    if not index: return
    
    embedder = SentenceTransformer(MODEL_NAME)
    
    # 2. Carica Dati Test
    legit_edits = load_edits(LEGIT_FILE, TEST_SIZE)
    vandal_edits = load_edits(VANDAL_FILE, TEST_SIZE)
    
    all_test_edits = legit_edits + vandal_edits
    print(f"📊 Caricati {len(legit_edits)} Legit e {len(vandal_edits)} Vandal per il test.")
    
    results = []
    correct_count = 0
    
    # 3. Loop Valutazione
    print("\n🚀 Inizio Valutazione...")
    for i, edit in enumerate(all_test_edits):
        print(f"[{i+1}/{len(all_test_edits)}] Analisi edit ({edit['type']})...", end="", flush=True)
        
        # A. Embedding Edit
        # Usiamo commento + titolo per contesto
        query_text = f"{edit['title']} {edit['comment']}"
        query_emb = embedder.encode(query_text, convert_to_numpy=True)
        
        # B. Retrieval
        context = retrieve_context(query_emb, index)
        
        # C. Gemini
        prediction = ask_gemini(edit, context)
        
        # D. Check Risultato
        expected = "VANDALISMO" if edit['is_vandalism'] else "LEGITTIMO"
        predicted_verdict = prediction.get("verdict", "UNKNOWN").upper()
        
        is_correct = (expected == predicted_verdict)
        if is_correct: correct_count += 1
        
        print(f" -> {predicted_verdict} ({'✅' if is_correct else '❌'})")
        
        results.append({
            "edit_id": edit.get("id"),
            "comment": edit.get("comment"),
            "is_vandalism": edit.get("is_vandalism"),
            "expected": expected,
            "predicted": predicted_verdict,
            "reasoning": prediction.get("reasoning"),
            "retrieved_context_scores": [c['score'] for c in context]
        })
        
        # Rate limit soft
        time.sleep(0.5)

    # 4. Statistiche Finali
    accuracy = (correct_count / len(all_test_edits)) * 100
    print(f"\n🏆 RISULTATI FINALI ORACLE")
    print(f"✅ Corretti: {correct_count}/{len(all_test_edits)}")
    print(f"📊 Accuratezza: {accuracy:.2f}%")
    
    # 5. Salvataggio
    output_data = {
        "accuracy": accuracy,
        "total": len(all_test_edits),
        "details": results
    }
    
    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=4, ensure_ascii=False)
    print(f"💾 Dettagli salvati in: {RESULTS_FILE}")

if __name__ == "__main__":
    main()
