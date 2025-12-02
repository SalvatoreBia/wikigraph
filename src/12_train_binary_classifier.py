import json
import pickle
import sys
import os
import numpy as np
from pathlib import Path
from sklearn.linear_model import LogisticRegression
from sentence_transformers import SentenceTransformer

# Import shared utils
import classifier_utils

# --- CONFIGURAZIONE ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
MOCK_DIR = DATA_DIR / "mocked_edits"
TRAINED_BC_DIR = DATA_DIR / "trained_BC"
MODEL_FILE = TRAINED_BC_DIR / "binary_classifier.pkl"

LEGIT_FILE = MOCK_DIR / "legit_edits.json"
VANDAL_FILE = MOCK_DIR / "vandal_edits.json"

MODEL_NAME = 'paraphrase-multilingual-MiniLM-L12-v2'
TRAINING_DATA_PERCENTAGE = 0.1 

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
            
        return data[-num_to_take:]

def main():
    print("--- 🧠 TRAINING BINARY CLASSIFIER (NEW ARCHITECTURE) ---")
    
    # 0. Setup Directory
    if not TRAINED_BC_DIR.exists():
        TRAINED_BC_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Connect Neo4j
    driver = classifier_utils.get_neo4j_driver()
    if not driver:
        return
    
    # 2. Load Model
    print(f"🚀 Caricamento Modello Embeddings: {MODEL_NAME}...")
    embedder = SentenceTransformer(MODEL_NAME)
    
    # 3. Load Data
    legit_edits = load_training_edits(LEGIT_FILE)
    vandal_edits = load_training_edits(VANDAL_FILE)
    
    train_edits = legit_edits + vandal_edits
    labels = [0] * len(legit_edits) + [1] * len(vandal_edits)
    
    print(f"📊 Training Set: {len(legit_edits)} Legit + {len(vandal_edits)} Vandal")
    
    if not train_edits:
        print("❌ Nessun dato per il training.")
        return

    # 4. Feature Engineering
    print("⚙️  Generazione Features (può richiedere tempo per le query Neo4j)...")
    X = []
    for i, edit in enumerate(train_edits):
        feat = classifier_utils.get_features(edit, embedder, driver)
        X.append(feat)
        if i % 10 == 0:
            print(f"   Processati {i+1}/{len(train_edits)}...", end='\r')
            
    X = np.array(X)
    y = np.array(labels)
    print(f"\n   Fatto. Shape X: {X.shape}")
    
    # 5. Training
    print("🏋️  Training Logistic Regression...")
    clf = LogisticRegression(max_iter=1000)
    clf.fit(X, y)
    
    # 6. Save
    with open(MODEL_FILE, "wb") as f:
        pickle.dump(clf, f)
        
    print(f"✅ Modello salvato in: {MODEL_FILE}")
    print(f"   Score sul training set: {clf.score(X, y):.2f}")
    
    driver.close()

if __name__ == "__main__":
    main()
