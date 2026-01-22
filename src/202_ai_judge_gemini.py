import json
import os
import threading
import time
from collections import defaultdict
from itertools import cycle
from pathlib import Path

import google.generativeai as genai
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from kafka import KafkaConsumer
from openai import OpenAI

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

from config_loader import load_config

CONFIG = load_config()
GEMINI_MODEL = CONFIG['llm']['judge_model']

PROVIDER = CONFIG['llm'].get('provider', 'gemini')

API_KEYS = []
i = 1
while True:
    key = os.getenv(f"GEMINI_API_KEY_{i}")
    if key:
        API_KEYS.append(key)
        i += 1
    else:
        break

if PROVIDER == 'local':
    API_KEYS = ["local-key"]
elif not API_KEYS:
    print("! Errore: Nessuna API Key trovata nel .env")

api_key_cycle = cycle(API_KEYS) if API_KEYS else None
key_lock = threading.Lock()

LOCAL_CLIENT = None
LOCAL_MODEL = None
if PROVIDER == 'local':
    LOCAL_CLIENT = OpenAI(
        base_url=CONFIG['llm']['local']['base_url'],
        api_key=CONFIG['llm']['local']['api_key']
    )
    LOCAL_MODEL = CONFIG['llm']['local'].get('model', GEMINI_MODEL)

KEY_USAGE = defaultdict(list)
MAX_REQ_PER_MIN = CONFIG['rate_limit']['max_req_per_min']
WINDOW_SIZE = CONFIG['rate_limit']['window_size']

def get_next_api_key():
    if PROVIDER == 'local':
        return "local-key"
    
    if not api_key_cycle:
        return None
        
    with key_lock:
        while True:
            now = time.time()
            for _ in range(len(API_KEYS)):
                key = next(api_key_cycle)
                KEY_USAGE[key] = [t for t in KEY_USAGE[key] if now - t < WINDOW_SIZE]
                
                if len(KEY_USAGE[key]) < MAX_REQ_PER_MIN:
                    KEY_USAGE[key].append(now)
                    return key
            
            print("- Rate limit raggiunto su tutte le chiavi. Attesa 5s...")
            time.sleep(5)

KAFKA_BROKER = CONFIG['kafka']['broker']
TOPIC_IN = CONFIG['kafka']['topic_judge']
SOURCE_FILE = '../data/web_source_tennis.html'

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

def analyze_with_gemini(edit_comment, context, original_text, new_text):
    api_key = get_next_api_key()
    if not api_key:
        return "ERRORE: API KEY MANCANTE"

    prompt = f"""
    Sei un moderatore di Wikipedia esperto.
    
    CONTESTO REALE (Fonte Ufficiale Verificata): 
    "{context}"
    
    TESTO ORIGINALE (Prima della modifica):
    "{original_text}"

    TESTO MODIFICATO (Dopo la modifica):
    "{new_text}"

    EVENTO DA ANALIZZARE (Commento dell'Edit Utente): 
    "{edit_comment}"
    
    TASK:
    Confronta la modifica con la fonte ufficiale e il testo originale.
    - Se la modifica migliora o corregge il testo basandosi sui fatti (o è neutra), è LEGITTIMO.
    - Se la modifica introduce falsità, insulti, o distrugge il contenuto, è VANDALISMO.
    
    Rispondi SOLO con una parola: "LEGITTIMO" oppure "VANDALISMO".
    """
    
    try:
        if PROVIDER == 'local':
            response = LOCAL_CLIENT.chat.completions.create(
                model=LOCAL_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3
            )
            return response.choices[0].message.content.strip()
        else:
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel(GEMINI_MODEL)
            response = model.generate_content(prompt)
            return response.text.strip()
    except Exception as e:
        return f"Errore AI: {e}"

def reset_results():
    if not SCORES_DIR.exists():
        SCORES_DIR.mkdir(parents=True, exist_ok=True)
    
    initial_data = {"results": [], "accuracy": 0.0, "avg_time": 0.0}
    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump(initial_data, f, indent=4, ensure_ascii=False)
    print(f"- Reset file risultati: {RESULTS_FILE.name}")

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
    
    total = len(current_data["results"])
    correct = sum(1 for r in current_data["results"] if r["correct"])
    total_time = sum(r["time_sec"] for r in current_data["results"])
    
    current_data["accuracy"] = (correct / total) * 100 if total > 0 else 0
    current_data["avg_time"] = total_time / total if total > 0 else 0
    
    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump(current_data, f, indent=4, ensure_ascii=False)

def main():
    reset_results()
    
    processed_ids = set()
    
    print("--- AI JUDGE AVVIATO (Il Giudice) ---")

    if PROVIDER == 'gemini' and not API_KEYS:
        print(f"! Errore critico: Nessuna API Key trovata nel .env")
        return
    
    ground_truth = load_ground_truth()
    
    if PROVIDER == 'local':
        print(f"- Modalità LOCAL. Modello: {LOCAL_MODEL}")
        print(f"   Endpoint: {CONFIG['llm']['local']['base_url']}")
    else:
        print(f"- {len(API_KEYS)} API Key caricate. Modello: {GEMINI_MODEL}")
    
    print(f"- Contesto caricato. In attesa...")

    consumer = KafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='ai_judge_gemini_group'
    )

    for message in consumer:
        event = message.value
        
        event_id = event.get('id') or event.get('meta', {}).get('id')
        if event_id is not None:
            event_id = str(event_id)
        if event_id and event_id in processed_ids:
            print(f"- Skip duplicato: {event_id[:8]}...")
            continue
        if event_id:
            processed_ids.add(event_id)
        
        comment = event['comment']
        user = event['user']
        original_text = event.get('original_text', '')
        new_text = event.get('new_text', '')
        is_vandalism_truth = event.get('is_vandalism', None)
        
        print(f"\n- Analisi edit di [{user}]:")
        print(f"  Commento: \"{comment}\"")
        
        max_retries = 5
        retry_wait = 60
        verdict = None
        total_elapsed = 0
        
        for attempt in range(max_retries):
            start_time = time.time()
            verdict = analyze_with_gemini(comment, ground_truth, original_text, new_text)
            end_time = time.time()
            elapsed = end_time - start_time
            total_elapsed += elapsed
            
            if "429" in verdict or "quota" in verdict.lower() or "exhausted" in verdict.lower():
                print(f"  ⚠ Errore quota (tentativo {attempt + 1}/{max_retries}). Attesa {retry_wait}s...")
                time.sleep(retry_wait)
                continue
            else:
                break
        
        elapsed = total_elapsed
        predicted_vandal = "VANDALISMO" in verdict.upper()
        
        is_correct = None
        if is_vandalism_truth is not None:
            is_correct = (predicted_vandal == is_vandalism_truth)
        
        if predicted_vandal:
            color = "\033[91m"
            status_text = "VANDALISMO"
        elif "LEGITTIMO" in verdict.upper():
            color = "\033[92m"
            status_text = "LEGITTIMO"
        else:
            color = "\033[93m"
            status_text = verdict
            
        reset = "\033[0m"
        
        print(f"  Verdetto: {color}{status_text}{reset} ({elapsed:.2f}s)")
        if is_correct is not None:
            print(f"  Corretto: {'Sì' if is_correct else 'No'}")

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