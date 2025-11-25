import json
import pickle
import sys
import numpy as np
from pathlib import Path
from sklearn.linear_model import LogisticRegression
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer

# --- CONFIGURAZIONE ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
MOCK_DIR = DATA_DIR / "mocked_edits"
INDEX_FILE = DATA_DIR / "trusted_sources_index.pkl"
MODEL_FILE = DATA_DIR / "binary_classifier.pkl"

LEGIT_FILE = MOCK_DIR / "legit_edits.json"
VANDAL_FILE = MOCK_DIR / "vandal_edits.json"

MODEL_NAME = 'paraphrase-multilingual-MiniLM-L12-v2'
TRAIN_SIZE = 50 # Ultimi 50 per tipo

def load_index():
    if not INDEX_FILE.exists():
        print(f"❌ Indice non trovato: {INDEX_FILE}")
        return None
    with open(INDEX_FILE, "rb") as f:
        return pickle.load(f)

def load_edits(filepath, start_from_end=50):
    if not filepath.exists():
        print(f"⚠️ File non trovato: {filepath}")
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
        # Prendi gli ultimi N
        return data[-start_from_end:]

def get_features(edit, embedder, index_data):
    """
    Crea il vettore di feature:
    [Edit_Embedding (384), Top_Context_Embedding (384), Cosine_Similarity (1)]
    Totale: 769 dimensioni
    """
    # 1. Embed Edit
    query_text = f"{edit['title']} {edit['comment']}"
    edit_emb = embedder.encode(query_text, convert_to_numpy=True)
    
    # 2. Retrieve Top Context
    corpus_embeddings = index_data["embeddings"]
    
    # Similitudine
    scores = cosine_similarity(edit_emb.reshape(1, -1), corpus_embeddings)[0]
    best_idx = np.argmax(scores)
    best_score = scores[best_idx]
    best_context_emb = corpus_embeddings[best_idx]
    
    # 3. Concatenate
    features = np.concatenate([edit_emb, best_context_emb, [best_score]])
    return features

def main():
    print("--- 🧠 TRAINING BINARY CLASSIFIER ---")
    
    # 1. Carica Risorse
    index = load_index()
    if not index: return
    
    embedder = SentenceTransformer(MODEL_NAME)
    
    # 2. Carica Dati Training (Ultimi 50)
    legit_edits = load_edits(LEGIT_FILE, TRAIN_SIZE)
    vandal_edits = load_edits(VANDAL_FILE, TRAIN_SIZE)
    
    train_edits = legit_edits + vandal_edits
    # Label: 0 = Legit, 1 = Vandal
    labels = [0] * len(legit_edits) + [1] * len(vandal_edits)
    
    print(f"📊 Training Set: {len(legit_edits)} Legit + {len(vandal_edits)} Vandal")
    
    # 3. Feature Engineering
    print("⚙️  Generazione Features...", end="", flush=True)
    X = []
    for edit in train_edits:
        feat = get_features(edit, embedder, index)
        X.append(feat)
    X = np.array(X)
    y = np.array(labels)
    print(" Fatto.")
    
    # 4. Training
    print("🏋️  Training Logistic Regression...")
    clf = LogisticRegression(max_iter=1000)
    clf.fit(X, y)
    
    # 5. Salvataggio
    with open(MODEL_FILE, "wb") as f:
        pickle.dump(clf, f)
        
    print(f"✅ Modello salvato in: {MODEL_FILE}")
    print(f"   Score sul training set: {clf.score(X, y):.2f}")

if __name__ == "__main__":
    main()
