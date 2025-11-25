import json
import pickle
import sys
import numpy as np
from pathlib import Path
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer

# --- CONFIGURAZIONE ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
MOCK_DIR = DATA_DIR / "mocked_edits"
INDEX_FILE = DATA_DIR / "trusted_sources_index.pkl"
MODEL_FILE = DATA_DIR / "binary_classifier.pkl"
RESULTS_FILE = DATA_DIR / "classifier_results.json"

LEGIT_FILE = MOCK_DIR / "legit_edits.json"
VANDAL_FILE = MOCK_DIR / "vandal_edits.json"

MODEL_NAME = 'paraphrase-multilingual-MiniLM-L12-v2'
TEST_SIZE = 50 # Primi 50 per tipo

def load_index():
    if not INDEX_FILE.exists():
        print(f"❌ Indice non trovato: {INDEX_FILE}")
        return None
    with open(INDEX_FILE, "rb") as f:
        return pickle.load(f)

def load_model():
    if not MODEL_FILE.exists():
        print(f"❌ Modello non trovato: {MODEL_FILE}")
        return None
    with open(MODEL_FILE, "rb") as f:
        return pickle.load(f)

def load_edits(filepath, limit=50):
    if not filepath.exists():
        print(f"⚠️ File non trovato: {filepath}")
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
        return data[:limit] # Primi N

def get_features(edit, embedder, index_data):
    # 1. Embed Edit
    query_text = f"{edit['title']} {edit['comment']}"
    edit_emb = embedder.encode(query_text, convert_to_numpy=True)
    
    # 2. Retrieve Top Context
    corpus_embeddings = index_data["embeddings"]
    scores = cosine_similarity(edit_emb.reshape(1, -1), corpus_embeddings)[0]
    best_idx = np.argmax(scores)
    best_score = scores[best_idx]
    best_context_emb = corpus_embeddings[best_idx]
    
    # 3. Concatenate
    features = np.concatenate([edit_emb, best_context_emb, [best_score]])
    return features

def main():
    print("--- 📉 EVALUATION BINARY CLASSIFIER ---")
    
    # 1. Carica Risorse
    index = load_index()
    model = load_model()
    if not index or not model: return
    
    embedder = SentenceTransformer(MODEL_NAME)
    
    # 2. Carica Dati Test (Primi 50)
    legit_edits = load_edits(LEGIT_FILE, TEST_SIZE)
    vandal_edits = load_edits(VANDAL_FILE, TEST_SIZE)
    
    all_test_edits = legit_edits + vandal_edits
    print(f"📊 Test Set: {len(legit_edits)} Legit + {len(vandal_edits)} Vandal")
    
    results = []
    correct_count = 0
    
    # 3. Valutazione
    print("🚀 Inizio Predizioni...")
    for edit in all_test_edits:
        # Features
        feat = get_features(edit, embedder, index)
        
        # Prediction
        pred_label = model.predict([feat])[0] # 0 or 1
        pred_verdict = "VANDALISMO" if pred_label == 1 else "LEGITTIMO"
        
        # Check
        expected = "VANDALISMO" if edit['is_vandalism'] else "LEGITTIMO"
        is_correct = (expected == pred_verdict)
        if is_correct: correct_count += 1
        
        results.append({
            "edit_id": edit.get("id"),
            "expected": expected,
            "predicted": pred_verdict,
            "correct": is_correct
        })
    
    # 4. Statistiche
    accuracy = (correct_count / len(all_test_edits)) * 100
    print(f"\n🏆 RISULTATI FINALI CLASSIFIER")
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
