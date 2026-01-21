import json
import pickle
import time
import sys
import numpy as np
from pathlib import Path
from kafka import KafkaConsumer
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import torch
import torch.nn as nn

# Import shared utils
import classifier_utils

# --- CONFIGURAZIONE ---
BASE_DIR = Path(__file__).resolve().parent.parent
from config_loader import load_config
CONFIG = load_config()

DATA_DIR = BASE_DIR / "data"
TRAINED_BC_DIR = DATA_DIR / "trained_BC"
SCORES_DIR = DATA_DIR / "scores"
RESULTS_FILE = SCORES_DIR / "BC_results.json"

# ============================================================
# SCEGLI IL MODELLO DA USARE:
#   "neural" = Rete neurale PyTorch (script 13)
#   "rf"     = Random Forest sklearn (script 12)
#   "auto"   = Prova neural, se non esiste usa rf
# ============================================================
PREFERRED_MODEL = "neural"

# File dei modelli
NEURAL_MODEL_FILE = TRAINED_BC_DIR / "neural_classifier.pth"
NEURAL_SCALER_FILE = TRAINED_BC_DIR / "neural_scaler.pkl"
RF_MODEL_FILE = TRAINED_BC_DIR / "binary_classifier.pkl"
RF_SCALER_FILE = TRAINED_BC_DIR / "scaler.pkl"

KAFKA_BROKER = CONFIG['kafka']['broker']
TOPIC_IN = CONFIG['kafka']['topic_judge']
MODEL_NAME = CONFIG['embedding']['model_name']

def get_raw_features(edit, embedder, driver):
    """Feature grezze per il modello neurale (identiche a 13_train_neural_classifier.py)"""
    new_text = edit.get('new_text', '')
    original_text = edit.get('original_text', '')
    comment = edit.get('comment', '')
    
    if new_text:
        new_emb = embedder.encode(new_text, convert_to_numpy=True)
    else:
        new_emb = np.zeros(384)
        
    if original_text:
        old_emb = embedder.encode(original_text, convert_to_numpy=True)
    else:
        old_emb = np.zeros(384)
        
    if comment:
        comment_emb = embedder.encode(comment, convert_to_numpy=True)
    else:
        comment_emb = np.zeros(384)
    
    old_len = len(original_text)
    new_len = len(new_text)
    if old_len > 0:
        length_ratio = new_len / old_len
    else:
        length_ratio = 1.0 if new_len == 0 else 10.0
    

    if np.all(old_emb == 0) or np.all(new_emb == 0):
        semantic_similarity = 0.0
    else:
        semantic_similarity = cosine_similarity([old_emb], [new_emb])[0][0]
    
    # Truth scores da Neo4j (TRIANGOLAZIONE)
    # 1. NEW TEXT vs WIKI & TRUSTED
    if np.all(new_emb == 0):
        score_new_wiki = 0.0
        score_new_trusted = 0.0
    else:
        _, score_new_wiki = classifier_utils.get_best_match(driver, classifier_utils.WIKI_INDEX_NAME, new_emb)
        _, score_new_trusted = classifier_utils.get_best_match(driver, classifier_utils.TRUSTED_INDEX_NAME, new_emb)
    
    # 2. OLD TEXT vs WIKI & TRUSTED
    if np.all(old_emb == 0):
        score_old_wiki = 0.0
        score_old_trusted = 0.0
    else:
        _, score_old_wiki = classifier_utils.get_best_match(driver, classifier_utils.WIKI_INDEX_NAME, old_emb)
        _, score_old_trusted = classifier_utils.get_best_match(driver, classifier_utils.TRUSTED_INDEX_NAME, old_emb)
    
    features = np.concatenate([
        old_emb, new_emb, comment_emb,
        [semantic_similarity], [length_ratio], 
        [score_new_wiki], [score_new_trusted],
        [score_old_wiki], [score_old_trusted]
    ])
    
    return features

class VandalismClassifier(nn.Module):
    def __init__(self, input_dim):
        super(VandalismClassifier, self).__init__()
        self.fc1 = nn.Linear(input_dim, 256)
        self.bn1 = nn.BatchNorm1d(256)
        self.dropout1 = nn.Dropout(0.5)
        self.fc2 = nn.Linear(256, 128)
        self.bn2 = nn.BatchNorm1d(128)
        self.dropout2 = nn.Dropout(0.5)
        self.fc3 = nn.Linear(128, 64)
        self.dropout3 = nn.Dropout(0.4)
        self.fc4 = nn.Linear(64, 1)
        self.relu = nn.ReLU()
        self.sigmoid = nn.Sigmoid()
    
    def forward(self, x):
        x = self.fc1(x)
        x = self.bn1(x)
        x = self.relu(x)
        x = self.dropout1(x)
        x = self.fc2(x)
        x = self.bn2(x)
        x = self.relu(x)
        x = self.dropout2(x)
        x = self.fc3(x)
        x = self.relu(x)
        x = self.dropout3(x)
        x = self.fc4(x)
        x = self.sigmoid(x)
        return x

def load_resources():
    print("⏳ Caricamento risorse BC...")
    print(f"   📋 Modello preferito: {PREFERRED_MODEL}")
    
    driver = classifier_utils.get_neo4j_driver()
    if not driver:
        return None, None, None, None
    
    def load_neural():
        if not (NEURAL_MODEL_FILE.exists() and NEURAL_SCALER_FILE.exists()):
            return None, None
        print("   🧠 Caricamento Neural Classifier (PyTorch)...")
        try:
            with open(NEURAL_SCALER_FILE, "rb") as f:
                scaler = pickle.load(f)
            input_dim = scaler.n_features_in_
            model = VandalismClassifier(input_dim)
            model.load_state_dict(torch.load(NEURAL_MODEL_FILE, map_location='cpu'))
            model.eval()
            print("   ✅ Neural Classifier caricato")
            return model, scaler
        except Exception as e:
            print(f"   ⚠️ Errore caricamento Neural: {e}")
            return None, None
    
    def load_rf():
        if not (RF_MODEL_FILE.exists() and RF_SCALER_FILE.exists()):
            return None, None
        print("   🌲 Caricamento Random Forest Classifier...")
        try:
            with open(RF_MODEL_FILE, "rb") as f:
                model = pickle.load(f)
            with open(RF_SCALER_FILE, "rb") as f:
                scaler = pickle.load(f)
            print("   ✅ Random Forest caricato")
            return model, scaler
        except Exception as e:
            print(f"   ⚠️ Errore caricamento RF: {e}")
            return None, None
    
    if PREFERRED_MODEL == "neural":
        model, scaler = load_neural()
        if model:
            return driver, model, scaler, "neural"
        print("   ❌ Neural non disponibile!")
        
    elif PREFERRED_MODEL == "rf":
        model, scaler = load_rf()
        if model:
            return driver, model, scaler, "rf"
        print("   ❌ Random Forest non disponibile!")
        
    else:  # auto
        model, scaler = load_neural()
        if model:
            return driver, model, scaler, "neural"
        model, scaler = load_rf()
        if model:
            return driver, model, scaler, "rf"
    
    print("❌ Nessun modello trovato!")
    return None, None, None, None

def reset_results():
    """Reset del file risultati all'avvio di una nuova sessione di test."""
    if not SCORES_DIR.exists():
        SCORES_DIR.mkdir(parents=True, exist_ok=True)
    
    initial_data = {"results": [], "accuracy": 0.0, "avg_time": 0.0}
    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump(initial_data, f, indent=4, ensure_ascii=False)
    print(f"🔄 Reset file risultati: {RESULTS_FILE.name}")

def save_result(result_entry):
    if not SCORES_DIR.exists():
        SCORES_DIR.mkdir(parents=True, exist_ok=True)
        
    current_data = {"results": [], "accuracy": 0.0, "avg_time": 0.0}
    if RESULTS_FILE.exists():
        try:
            with open(RESULTS_FILE, "r", encoding="utf-8") as f:
                current_data = json.load(f)
        except json.JSONDecodeError:
            pass
            
    current_data["results"].append(result_entry)
    
    # Recalculate stats
    total = len(current_data["results"])
    correct = sum(1 for r in current_data["results"] if r["correct"])
    total_time = sum(r["time_sec"] for r in current_data["results"])
    
    current_data["accuracy"] = (correct / total) * 100 if total > 0 else 0
    current_data["avg_time"] = total_time / total if total > 0 else 0
    
    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump(current_data, f, indent=4, ensure_ascii=False, 
          default=lambda o: bool(o) if isinstance(o, (np.bool_, np.bool)) else o)

def main():
    # Reset risultati all'avvio
    reset_results()
    
    driver, model, scaler, model_type = load_resources()
    if driver is None or model is None:
        print("❌ Errore: impossibile caricare modello o Neo4j")
        sys.exit(1)
    
    embedder = SentenceTransformer(MODEL_NAME)
    print("✅ Risorse caricate. In attesa di edit...")
    
    consumer = KafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='bc_judge_group_new' 
    )
    
    try:
        for message in consumer:
            event = message.value
            comment = event.get('comment', '')
            user = event.get('user', 'Unknown')
            is_vandalism_truth = event.get('is_vandalism', None)
            
            print(f"\nAnalisi edit di [{user}]:")
            print(f"  Commento: \"{comment}\"")
            
            start_time = time.time()
            
            # Feature extraction (diverse per neural vs RF)
            if model_type == "neural":
                feat = get_raw_features(event, embedder, driver)
            else:  # rf
                feat = classifier_utils.get_features(event, embedder, driver)
            
            feat_scaled = scaler.transform([feat])
            
            # Prediction
            if model_type == "neural":
                with torch.no_grad():
                    feat_tensor = torch.FloatTensor(feat_scaled)
                    pred_prob = model(feat_tensor).item()
                    pred_label = 1 if pred_prob > 0.5 else 0
            else:  # rf
                pred_label = model.predict(feat_scaled)[0]
            
            end_time = time.time()
            elapsed = end_time - start_time
            
            predicted_vandal = (pred_label == 1)
            verdict = "VANDALISMO" if predicted_vandal else "LEGITTIMO"
            
            # Check correctness
            is_correct = None
            if is_vandalism_truth is not None:
                is_correct = (predicted_vandal == is_vandalism_truth)
                
            if predicted_vandal:
                color = "\033[91m"
                icon = "🚨"
            else:
                color = "\033[92m"
                icon = "✅"
                
            reset = "\033[0m"
            
            print(f"  Verdetto: {color}{icon} {verdict}{reset} ({elapsed:.4f}s)")
            if is_correct is not None:
                print(f"  Corretto: {'✅' if is_correct else '❌'}")
                
            # Salva risultato
            result_entry = {
                "user": user,
                "comment": comment,
                "predicted": verdict,
                "expected": "VANDALISMO" if is_vandalism_truth else "LEGITTIMO",
                "correct": is_correct,
                "time_sec": elapsed
            }
            save_result(result_entry)
            print("-" * 50)
    finally:
        driver.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nSpegnimento BC Judge.")
