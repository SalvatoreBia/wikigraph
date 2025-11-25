import json
import pickle
import time
import sys
import numpy as np
from pathlib import Path
from kafka import KafkaConsumer
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer

# --- CONFIGURAZIONE ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
TRAINED_BC_DIR = DATA_DIR / "trained_BC"
SCORES_DIR = DATA_DIR / "scores"
RESULTS_FILE = SCORES_DIR / "BC_results.json"

INDEX_FILE = DATA_DIR / "trusted_sources_index.pkl"
MODEL_FILE = TRAINED_BC_DIR / "binary_classifier.pkl"

KAFKA_BROKER = 'localhost:9092'
TOPIC_IN = 'to-be-judged'
MODEL_NAME = 'paraphrase-multilingual-MiniLM-L12-v2'

def load_resources():
    print("⏳ Caricamento risorse BC...")
    if not INDEX_FILE.exists():
        print(f"❌ Indice non trovato: {INDEX_FILE}")
        return None, None
    if not MODEL_FILE.exists():
        print(f"❌ Modello non trovato: {MODEL_FILE}")
        return None, None
        
    with open(INDEX_FILE, "rb") as f:
        index = pickle.load(f)
    with open(MODEL_FILE, "rb") as f:
        model = pickle.load(f)
        
    return index, model

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
        json.dump(current_data, f, indent=4, ensure_ascii=False)

def main():
    print("--- 🤖 BC JUDGE AVVIATO (Il Classificatore) ---")
    
    index, model = load_resources()
    if not index or not model:
        return
        
    embedder = SentenceTransformer(MODEL_NAME)
    print("✅ Risorse caricate. In attesa di edit...")
    
    consumer = KafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='bc_judge_group' # Group ID diverso per ricevere copia dei messaggi
    )
    
    for message in consumer:
        event = message.value
        comment = event['comment']
        user = event['user']
        is_vandalism_truth = event.get('is_vandalism', None)
        
        print(f"\nAnalisi edit di [{user}]:")
        print(f"  Commento: \"{comment}\"")
        
        start_time = time.time()
        
        # Feature extraction & Prediction
        feat = get_features(event, embedder, index)
        pred_label = model.predict([feat])[0] # 0 = Legit, 1 = Vandal
        
        end_time = time.time()
        elapsed = end_time - start_time
        
        predicted_vandal = (pred_label == 1)
        verdict = "VANDALISMO" if predicted_vandal else "LEGITTIMO"
        
        # Check correctness
        is_correct = None
        if is_vandalism_truth is not None:
            is_correct = (predicted_vandal == is_vandalism_truth)
            
        if predicted_vandal:
            color = "\033[91m" # Rosso
            icon = "🚨"
        else:
            color = "\033[92m" # Verde
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

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nSpegnimento BC Judge.")
