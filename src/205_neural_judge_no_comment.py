import json
import pickle
import sys
import time
from pathlib import Path

import numpy as np
import torch
import torch.nn as nn
from kafka import KafkaConsumer
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

BASE_DIR = Path(__file__).resolve().parent.parent
from config_loader import load_config

CONFIG = load_config()

DATA_DIR = BASE_DIR / "data"
TRAINED_BC_DIR = DATA_DIR / "trained_BC"
SCORES_DIR = DATA_DIR / "scores"
RESULTS_FILE = SCORES_DIR / "BC_results_no_comment.json"

NEURAL_MODEL_FILE = TRAINED_BC_DIR / "neural_classifier_no_comment.pth"
NEURAL_SCALER_FILE = TRAINED_BC_DIR / "neural_scaler_no_comment.pkl"

KAFKA_BROKER = CONFIG['kafka']['broker']
TOPIC_IN = CONFIG['kafka']['topic_judge']
MODEL_NAME = CONFIG['embedding']['model_name']


def get_raw_features_no_comment(edit, embedder):
    """
    Feature grezze SENZA embedding del commento.
    
    Features:
    - old_emb (384)
    - new_emb (384)
    - semantic_similarity (1)
    - length_ratio (1)
    
    Totale: 770 features
    """
    new_text = edit.get('new_text', '')
    original_text = edit.get('original_text', '')
    
    if new_text:
        new_emb = embedder.encode(new_text, convert_to_numpy=True)
    else:
        new_emb = np.zeros(384)
        
    if original_text:
        old_emb = embedder.encode(original_text, convert_to_numpy=True)
    else:
        old_emb = np.zeros(384)
    
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
    
    features = np.concatenate([
        old_emb, new_emb,
        [semantic_similarity], [length_ratio]
    ])
    
    return features


class VandalismClassifierNoComment(nn.Module):
    """
    Rete neurale per classificazione vandalismo SENZA COMMENTO.
    """
    def __init__(self, input_dim):
        super(VandalismClassifierNoComment, self).__init__()
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
    """Carica modello e scaler NO COMMENT"""
    print("⏳ Caricamento risorse BC (NO COMMENT)...")
    
    if not NEURAL_MODEL_FILE.exists():
        print(f"❌ Modello non trovato: {NEURAL_MODEL_FILE}")
        print("   Esegui prima 15_train_neural_no_comment.py")
        return None, None
        
    if not NEURAL_SCALER_FILE.exists():
        print(f"❌ Scaler non trovato: {NEURAL_SCALER_FILE}")
        return None, None
    
    print("   🧠 Caricamento Neural Classifier NO COMMENT (PyTorch)...")
    try:
        with open(NEURAL_SCALER_FILE, "rb") as f:
            scaler = pickle.load(f)
        input_dim = scaler.n_features_in_
        model = VandalismClassifierNoComment(input_dim)
        model.load_state_dict(torch.load(NEURAL_MODEL_FILE, map_location='cpu'))
        model.eval()
        print(f"   ✅ Neural Classifier NO COMMENT caricato (input_dim={input_dim})")
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
    print("🧠 NEURAL JUDGE (NO COMMENT) - Real-time Classification")
    print("=" * 60)
    
    model, scaler = load_resources()
    if model is None:
        print("❌ Errore: impossibile caricare il modello")
        sys.exit(1)
    
    embedder = SentenceTransformer(MODEL_NAME)
    print(f"✅ Embedder caricato: {MODEL_NAME}")
    print("ℹ️  Modo NO COMMENT: Non usa embedding del commento")
    print("\n✅ Risorse caricate. In attesa di edit...")
    
    consumer = KafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='bc_judge_no_comment_group'
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
            
            feat = get_raw_features_no_comment(event, embedder)
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
        print("\n\nSpegnimento BC Judge (NO COMMENT).")
    finally:
        print("👋 Bye!")


if __name__ == "__main__":
    main()
