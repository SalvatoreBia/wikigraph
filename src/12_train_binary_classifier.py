import json
import pickle
import sys
import os
import numpy as np
from pathlib import Path
from sklearn.linear_model import LogisticRegression
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer

# --- CONFIGURAZIONE ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
MOCK_DIR = DATA_DIR / "mocked_edits"
TRAINED_BC_DIR = DATA_DIR / "trained_BC"
INDEX_FILE = DATA_DIR / "trusted_sources_index.pkl"
MODEL_FILE = TRAINED_BC_DIR / "binary_classifier.pkl"

LEGIT_FILE = MOCK_DIR / "legit_edits.json"
VANDAL_FILE = MOCK_DIR / "vandal_edits.json"

MODEL_NAME = 'paraphrase-multilingual-MiniLM-L12-v2'
TRAINING_DATA_PERCENTAGE = 1.0 # 0.0 to 1.0 (1.0 = 100% of edits, 0.5 = last 50%)

def load_index():
    if not INDEX_FILE.exists():
        print(f"❌ Indice non trovato: {INDEX_FILE}")
        return None
    with open(INDEX_FILE, "rb") as f:
        return pickle.load(f)

def load_training_edits(filepath):
    """Carica una percentuale di edit partendo dal basso (più recenti)"""
    if not filepath.exists():
        print(f"⚠️ File non trovato: {filepath}")
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
        
        total_edits = len(data)
        num_to_take = int(total_edits * TRAINING_DATA_PERCENTAGE)
        
        if num_to_take == 0:
            return []
            
        # Prendi gli ultimi 'num_to_take' elementi
        return data[-num_to_take:]

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
    
    # 0. Setup Directory
    if not TRAINED_BC_DIR.exists():
        TRAINED_BC_DIR.mkdir(parents=True, exist_ok=True)
        print(f"📁 Creata directory: {TRAINED_BC_DIR}")

    # 1. Carica Risorse
    index = load_index()
    if not index: return
    
    embedder = SentenceTransformer(MODEL_NAME)
    
    # 2. Carica Dati Training (Percentuale definita da TRAINING_DATA_PERCENTAGE)
    legit_edits = load_training_edits(LEGIT_FILE)
    vandal_edits = load_training_edits(VANDAL_FILE)
    
    train_edits = legit_edits + vandal_edits
    # Label: 0 = Legit, 1 = Vandal
    labels = [0] * len(legit_edits) + [1] * len(vandal_edits)
    
    print(f"📊 Training Set: {len(legit_edits)} Legit + {len(vandal_edits)} Vandal (Usati ultimi {TRAINING_DATA_PERCENTAGE*100:.0f}%)")
    
    if not train_edits:
        print("❌ Nessun dato per il training. Controlla i file JSON.")
        return

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
