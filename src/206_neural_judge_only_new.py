"""
206_neural_judge_only_new.py
Judge neurale per rilevamento vandalismo in real-time.
VERSIONE SOLO NEW TEXT: Usa solo embedding del nuovo testo.
Consuma da Kafka e classifica gli edit.
"""

import json
import pickle
import time
import sys
import numpy as np
from pathlib import Path
from kafka import KafkaConsumer
from sentence_transformers import SentenceTransformer
import torch
import torch.nn as nn


# --- CONFIGURAZIONE ---
BASE_DIR = Path(__file__).resolve().parent.parent
from config_loader import load_config
CONFIG = load_config()

DATA_DIR = BASE_DIR / "data"
TRAINED_BC_DIR = DATA_DIR / "trained_BC"
SCORES_DIR = DATA_DIR / "scores"
RESULTS_FILE = SCORES_DIR / "BC_results_only_new.json"

NEURAL_MODEL_FILE = TRAINED_BC_DIR / "neural_classifier_only_new.pth"
NEURAL_SCALER_FILE = TRAINED_BC_DIR / "neural_scaler_only_new.pkl"

KAFKA_BROKER = CONFIG['kafka']['broker']
TOPIC_IN = CONFIG['kafka']['topic_judge']
MODEL_NAME = CONFIG['embedding']['model_name']


def get_raw_features_only_new(edit, embedder):
    """
    Feature grezze SOLO NEW TEXT.
    
    Features:
    - new_emb (384)
    - length_ratio (1)
    
    Totale: 385 features
    """
    new_text = edit.get('new_text', '')
    original_text = edit.get('original_text', '')
    
    if new_text:
        new_emb = embedder.encode(new_text, convert_to_numpy=True)
    else:
        new_emb = np.zeros(384)
    
    old_len = len(original_text)
    new_len = len(new_text)
    if old_len > 0:
        length_ratio = new_len / old_len
    else:
        length_ratio = 1.0 if new_len == 0 else 10.0
    
    # SOLO NEW TEXT + RATIO!
    features = np.concatenate([
        new_emb,
        [length_ratio]
    ])
    
    return features


class VandalismClassifierOnlyNew(nn.Module):
    """
    Rete neurale per classificazione vandalismo ONLY NEW.
    Rete più piccola per meno features.
    """
    def __init__(self, input_dim):
        super(VandalismClassifierOnlyNew, self).__init__()
        self.fc1 = nn.Linear(input_dim, 128)
        self.bn1 = nn.BatchNorm1d(128)
        self.dropout1 = nn.Dropout(0.5)
        self.fc2 = nn.Linear(128, 64)
        self.bn2 = nn.BatchNorm1d(64)
        self.dropout2 = nn.Dropout(0.4)
        self.fc3 = nn.Linear(64, 1)
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
        x = self.sigmoid(x)
        return x


def load_resources():
    """Carica modello e scaler ONLY NEW"""
    print("⏳ Caricamento risorse BC (ONLY NEW)...")
    
    if not NEURAL_MODEL_FILE.exists():
        print(f"❌ Modello non trovato: {NEURAL_MODEL_FILE}")
        print("   Esegui prima 16_train_neural_only_new.py")
        return None, None
        
    if not NEURAL_SCALER_FILE.exists():
        print(f"❌ Scaler non trovato: {NEURAL_SCALER_FILE}")
        return None, None
    
    print("   🧠 Caricamento Neural Classifier ONLY NEW (PyTorch)...")
    try:
        with open(NEURAL_SCALER_FILE, "rb") as f:
            scaler = pickle.load(f)
        input_dim = scaler.n_features_in_
        model = VandalismClassifierOnlyNew(input_dim)
        model.load_state_dict(torch.load(NEURAL_MODEL_FILE, map_location='cpu'))
        model.eval()
        print(f"   ✅ Neural Classifier ONLY NEW caricato (input_dim={input_dim})")
        return model, scaler
    except Exception as e:
        print(f"   ❌ Errore caricamento: {e}")
        return None, None


def reset_results():
    """Reset del file risultati all'avvio di una nuova sessione di test."""
    if not SCORES_DIR.exists():
        SCORES_DIR.mkdir(parents=True, exist_ok=True)
    
    initial_data = {"results": [], "accuracy": 0.0, "avg_time": 0.0}
    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump(initial_data, f, indent=4, ensure_ascii=False)
    print(f"🔄 Reset file risultati: {RESULTS_FILE.name}")

def save_result(result_entry):
    """Salva i risultati su file JSON"""
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
    
    total = len(current_data["results"])
    correct = sum(1 for r in current_data["results"] if r["correct"])
    total_time = sum(r["time_sec"] for r in current_data["results"])
    
    current_data["accuracy"] = (correct / total) * 100 if total > 0 else 0
    current_data["avg_time"] = total_time / total if total > 0 else 0
    
    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump(current_data, f, indent=4, ensure_ascii=False, 
          default=lambda o: bool(o) if isinstance(o, (np.bool_, np.bool)) else o)


def main():
    reset_results()
    processed_ids = set()
    
    print("=" * 60)
    print("🧠 NEURAL JUDGE (ONLY NEW) - Real-time Classification")
    print("=" * 60)
    
    model, scaler = load_resources()
    if model is None:
        print("❌ Errore: impossibile caricare il modello")
        sys.exit(1)
    
    embedder = SentenceTransformer(MODEL_NAME)
    print(f"✅ Embedder caricato: {MODEL_NAME}")
    print("ℹ️  Modo ONLY NEW: Usa solo embedding del nuovo testo")
    print("\n✅ Risorse caricate. In attesa di edit...")
    
    consumer = KafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='bc_judge_only_new_group'
    )
    
    try:
        for message in consumer:
            event = message.value
            
            event_id = event.get('id') or event.get('meta', {}).get('id')
            if event_id and event_id in processed_ids:
                print(f"⏭️ Skip duplicato: {event_id[:8] if isinstance(event_id, str) else event_id}...")
                continue
            if event_id:
                processed_ids.add(event_id)
            
            comment = event.get('comment', '')
            user = event.get('user', 'Unknown')
            is_vandalism_truth = event.get('is_vandalism', None)
            
            print(f"\nAnalisi edit di [{user}]:")
            print(f"  Commento: \"{comment}\"")
            
            start_time = time.time()
            
            feat = get_raw_features_only_new(event, embedder)
            feat_scaled = scaler.transform([feat])
            
            with torch.no_grad():
                feat_tensor = torch.FloatTensor(feat_scaled)
                pred_prob = model(feat_tensor).item()
                pred_label = 1 if pred_prob > 0.5 else 0
            
            end_time = time.time()
            elapsed = end_time - start_time
            
            predicted_vandal = (pred_label == 1)
            verdict = "VANDALISMO" if predicted_vandal else "LEGITTIMO"
            
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
            print(f"  Confidence: {pred_prob:.3f}")
            if is_correct is not None:
                print(f"  Corretto: {'✅' if is_correct else '❌'}")
                
            result_entry = {
                "user": user,
                "comment": comment,
                "predicted": verdict,
                "expected": "VANDALISMO" if is_vandalism_truth else "LEGITTIMO",
                "correct": is_correct,
                "confidence": pred_prob,
                "time_sec": elapsed
            }
            save_result(result_entry)
            print("-" * 50)
            
    except KeyboardInterrupt:
        print("\n\nSpegnimento BC Judge (ONLY NEW).")
    finally:
        print("👋 Bye!")


if __name__ == "__main__":
    main()
