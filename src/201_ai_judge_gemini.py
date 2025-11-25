import json
import os
import time
from pathlib import Path

import google.generativeai as genai
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from kafka import KafkaConsumer

# --- CARICAMENTO CONFIGURAZIONE ---
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
KAFKA_BROKER = 'localhost:9092'
TOPIC_IN = 'to-be-judged'
SOURCE_FILE = '../data/web_source_tennis.html'
GEMINI_MODEL = 'gemini-2.5-flash' 

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
SCORES_DIR = DATA_DIR / "scores"
RESULTS_FILE = SCORES_DIR / "LLM_results.json"

def load_ground_truth():
    if not os.path.exists(SOURCE_FILE):
        return "Nessuna fonte trovata."
    with open(SOURCE_FILE, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')
        return soup.get_text(separator=' ', strip=True)

def analyze_with_gemini(edit_comment, context):
    if not GEMINI_API_KEY:
        return "ERRORE: API KEY MANCANTE"

    model = genai.GenerativeModel(GEMINI_MODEL)
    
    prompt = f"""
    Sei un moderatore di Wikipedia esperto.
    
    CONTESTO REALE (Fonte Ufficiale Verificata): 
    "{context}"
    
    EVENTO DA ANALIZZARE (Commento dell'Edit Utente): 
    "{edit_comment}"
    
    TASK:
    Confronta il commento dell'utente con la fonte ufficiale.
    - Se il commento è coerente con i fatti (es. riporta la squalifica o l'innocenza della Dabrowski corretta), è LEGITTIMO.
    - Se il commento contraddice i fatti (es. accusa chi è stato assolto, nega la squalifica, insulta), è VANDALISMO.
    
    Rispondi SOLO con una parola: "LEGITTIMO" oppure "VANDALISMO".
    """
    
    try:
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        return f"Errore AI: {e}"

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
    print("--- AI JUDGE AVVIATO (Il Giudice) ---")

    if not GEMINI_API_KEY:
        print(f"❌ ERRORE CRITICO: Impossibile trovare GEMINI_API_KEY in {env_path}")
        return

    genai.configure(api_key=GEMINI_API_KEY)
    
    ground_truth = load_ground_truth()
    print(f"✅ API Key caricata. Modello: {GEMINI_MODEL}")
    print(f"📚 Contesto caricato. In attesa...")

    # --- CORREZIONE QUI: Aggiunto group_id univoco ---
    consumer = KafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='ai_judge_group'  # Questo assicura che riceva una copia di tutti i messaggi
    )

    for message in consumer:
        event = message.value
        comment = event['comment']
        user = event['user']
        is_vandalism_truth = event.get('is_vandalism', None) # Potrebbe non esserci in eventi reali
        
        print(f"\nAnalisi edit di [{user}]:")
        print(f"  Commento: \"{comment}\"")
        
        start_time = time.time()
        verdict = analyze_with_gemini(comment, ground_truth)
        end_time = time.time()
        elapsed = end_time - start_time
        
        # Normalizza verdetto
        predicted_vandal = "VANDALISMO" in verdict.upper()
        
        # Check correctness
        is_correct = None
        if is_vandalism_truth is not None:
            is_correct = (predicted_vandal == is_vandalism_truth)
        
        if predicted_vandal:
            color = "\033[91m" # Rosso
            icon = "🚨"
        elif "LEGITTIMO" in verdict.upper():
            color = "\033[92m" # Verde
            icon = "✅"
        else:
            color = "\033[93m" # Giallo
            icon = "⚠️"
            
        reset = "\033[0m"
        
        print(f"  Verdetto: {color}{icon} {verdict}{reset} ({elapsed:.2f}s)")
        if is_correct is not None:
            print(f"  Corretto: {'✅' if is_correct else '❌'}")

        # Salva risultato
        result_entry = {
            "user": user,
            "comment": comment,
            "predicted": "VANDALISMO" if predicted_vandal else "LEGITTIMO",
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
        print("\nSpegnimento Judge.")