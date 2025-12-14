import json
import os
import random
import re
import sys
import time
import uuid
import shutil
import multiprocessing
import math
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from collections import defaultdict

import google.generativeai as genai
from dotenv import load_dotenv
from neo4j import GraphDatabase

# --- CONFIGURAZIONE ---
from config_loader import load_config

# Load config globally (read-only is fine for processes)
CONFIG = load_config()

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
HTML_DIR = DATA_DIR / "trusted_html_pages"
MOCK_DIR = DATA_DIR / "mocked_edits"
ENV_PATH = BASE_DIR / ".env"

# File di output separati
LEGIT_FILE = MOCK_DIR / "legit_edits.json"
VANDAL_FILE = MOCK_DIR / "vandal_edits.json"

load_dotenv(dotenv_path=ENV_PATH)

# Retrieve Keys
API_KEYS = [
    os.getenv("GEMINI_API_KEY"),
    os.getenv("GEMINI_API_KEY_2"),
    os.getenv("GEMINI_API_KEY_3")
]
API_KEYS = [k for k in API_KEYS if k]
if not API_KEYS:
    print("❌ ERRORE: Nessuna API Key trovata nel .env")
    sys.exit(1)

# Rate Limiting & Token Config
MAX_REQ_PER_MIN = CONFIG['rate_limit']['max_req_per_min'] # e.g. 15 or 30
MAX_TOKENS_PER_MIN = 15000 # Default free tier limit
WINDOW_SIZE = CONFIG['rate_limit']['window_size'] # e.g. 60
CONTEXT_WINDOW_SIZE = CONFIG['processing'].get('context_window_size', 600)

# Neo4j Config
URI = CONFIG['neo4j']['uri']
AUTH = tuple(CONFIG['neo4j']['auth'])
MODEL_NAME = CONFIG['llm']['generation_model']
TEXT_LIMIT = CONFIG['processing']['text_limit']

# Simulation Config
TARGET_LEGIT_EDITS = CONFIG['simulation']['target_legit_edits']
TARGET_VANDAL_EDITS = CONFIG['simulation']['target_vandal_edits']
ARTICLES_PER_COMMUNITY = CONFIG['simulation'].get('articles_per_community', 5)

def estimate_tokens(text):
    """Stima grezza dei token (1 token ~= 4 caratteri per l'inglese, un po' meno per codice/altre lingue).
    Usiamo un fattore di sicurezza."""
    if not text: return 0
    return math.ceil(len(text) / 3.0) # Conservativo: 3 caratteri per token

def check_and_update_rate_limit(key, usage_dict, inputs_tokens=0):
    """
    Controlla se la chiave può essere usata.
    usage_dict: multiprocessing.Manager().dict() -> { key: {'reqs': [ts, ...], 'tokens': [(ts, count), ...]} }
    Ritorna True se ok, False se limitato.
    """
    now = time.time()
    
    # Recupera lo stato attuale (deepcopy implicito se proxy)
    state = usage_dict.get(key, {'reqs': [], 'tokens': []})
    
    # Pulisci vecchi timestamp
    valid_reqs = [t for t in state['reqs'] if now - t < WINDOW_SIZE]
    valid_tokens = [(t, c) for t, c in state['tokens'] if now - t < WINDOW_SIZE]
    
    current_req_count = len(valid_reqs)
    current_token_count = sum(c for t, c in valid_tokens)
    
    # Controlla limiti
    if current_req_count >= MAX_REQ_PER_MIN:
        return False, f"Req limit ({current_req_count}/{MAX_REQ_PER_MIN})"
        
    if current_token_count + inputs_tokens >= MAX_TOKENS_PER_MIN:
        return False, f"Token limit ({current_token_count + inputs_tokens}/{MAX_TOKENS_PER_MIN})"
        
    # Se ok, aggiorna (bisogna riassegnare per il Manager dict)
    valid_reqs.append(now)
    valid_tokens.append((now, inputs_tokens))
    
    usage_dict[key] = {'reqs': valid_reqs, 'tokens': valid_tokens}
    return True, "OK"

def append_to_json_file_safe(filepath, new_items, lock):
    """Scrittura thread/process-safe su file JSON."""
    with lock:
        data = []
        if filepath.exists():
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except json.JSONDecodeError:
                pass 
        
        data.extend(new_items)
        
        # Scrivi in temp e rinomina per atomicità (opzionale, ma meglio lock)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        print(f"💾 Salvati {len(new_items)} items in {filepath.name} (Totale: {len(data)})")

def extract_random_window(text, window_size=CONTEXT_WINDOW_SIZE):
    if not text: return ""
    if len(text) <= window_size: return text
    start_idx = random.randint(0, len(text) - window_size)
    while start_idx < len(text) and text[start_idx] not in (' ', '\n', '.'):
        start_idx += 1
    return text[start_idx : start_idx + window_size]

def clean_json_text(text):
    """Estrae JSON valido da una risposta LLM."""
    # 1. Rimuovi blocchi markdown
    text = text.replace("```json", "").replace("```", "").strip()
    
    # 2. Cerca array JSON [...]
    start = text.find('[')
    end = text.rfind(']')
    if start != -1 and end != -1:
        text = text[start : end + 1]
        
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None

# --- WORKER FUNCTIONS ---

def generate_html_worker(key, title, context_content, usage_dict):
    """Worker per generare HTML."""
    # Configura API Key locale al processo
    genai.configure(api_key=key)
    model = genai.GenerativeModel(MODEL_NAME)
    
    # 1. Check Rate Limit a monte (conservativo)
    # Stima prompt
    prompt_len = len(title) + len(context_content) + 500 # Istruzioni
    est_tokens = estimate_tokens("x" * prompt_len)
    
    ok, msg = check_and_update_rate_limit(key, usage_dict, est_tokens)
    if not ok:
        time.sleep(5) # Backoff semplice
        return None # Riproveremo o skip
        
    print(f"📄 [Start] HTML per: {title} (Key: ...{key[-4:]})")
    
    context_section = ""
    if context_content:
        context_section = f"""
    Ecco alcune informazioni di contesto REALI.
    --- INIZIO CONTESTO ---
    {context_content}
    --- FINE CONTESTO ---
    """

    prompt = f"""
    Sei un giornalista esperto. Scrivi un articolo dettagliato (800 parole) su: "{title}".
    {context_section}
    REGOLE:
    1. Usa HTML puro (<body>, <h1>, <p>, etc). NO Markdown.
    2. Stile enciclopedico.
    """
    
    try:
        resp = model.generate_content(prompt)
        # HTML non è JSON, quindi prendiamo text raw e puliamo markdown se c'è
        html_content = resp.text.replace("```html", "").replace("```", "").strip()
        
        clean_title = re.sub(r'[^\w]', '_', title)
        filename = f"trusted_{clean_title}.html"
        path = HTML_DIR / filename
        
        with open(path, "w", encoding="utf-8") as f:
            f.write(html_content)
            
        print(f"✅ [Done] HTML salvato: {filename}")
        return {"title": title, "path": str(path), "content_snippet": html_content[:500]}
    except Exception as e:
        print(f"⚠️ Errore Gen HTML {title}: {e}")
        return None

def generate_edits_worker(key, topic_title, edit_type, count, context_snippet, real_text_window, usage_dict, file_lock):
    """Worker per generare Edits."""
    genai.configure(api_key=key)
    model = genai.GenerativeModel(MODEL_NAME)
    
    # Stima tokens prompt
    input_text = f"{context_snippet} {real_text_window} {topic_title} {edit_type}"
    est_tokens = estimate_tokens(input_text) + 500 # Instructions
    
    # Rate Limit Check
    backoff = 1
    while True:
        ok, msg = check_and_update_rate_limit(key, usage_dict, est_tokens)
        if ok:
            break
        print(f"⏳ Rate Limit ({key[-4:]}): {msg}. Wait {backoff}s...")
        time.sleep(backoff)
        backoff = min(backoff * 1.5, 30)
    
    print(f"✍️  [Start] {count} Edits {edit_type} per {topic_title} (Key: ...{key[-4:]})")
    
    prompt = f"""
    Sei un simulatore di modifiche Wikipedia.
    Contesto reale (snippet): "{real_text_window}"
    
    Genera un JSON Array con {count} modifiche {edit_type} su questo testo.
    
    FORMATO JSON:
    [
      {{
        "user": "User1", "comment": "fix", 
        "original_text": "sottostringa esatta del testo", 
        "new_text": "testo modificato",
        "is_vandalism": { "true" if edit_type == "VANDALICI" else "false" }
      }}
    ]
    
    REGOLE:
    - original_text DEVE esistere nel snippet.
    - { "VANDALISMO: sii subdolo (date, nomi errati)." if edit_type=="VANDALICI" else "LEGITTIMO: correggi typo, stile." }
    - Output ESCLUSIVAMENTE il JSON.
    """
    
    try:
        # RIMOSSO generation_config={"response_mime_type": "application/json"} per evitare errore 400
        resp = model.generate_content(prompt)
        text_resp = resp.text
        
        edits = clean_json_text(text_resp)
        if not edits or not isinstance(edits, list):
            print(f"⚠️ Errore parsing JSON per {topic_title}: Non è una lista.")
            return []
            
        clean_title = re.sub(r'[^\w]', '_', topic_title)
        final_edits = []
        for edit in edits:
            enriched = {
                "id": str(uuid.uuid4()),
                "type": "edit",
                "title": topic_title,
                "user": edit.get("user", "Anon"),
                "comment": edit.get("comment", "Edit"),
                "original_text": edit.get("original_text", ""),
                "new_text": edit.get("new_text", ""),
                "timestamp": int(time.time()),
                "is_vandalism": edit.get("is_vandalism", False),
                "meta": { "uri": f"https://it.wikipedia.org/wiki/{clean_title}" }
            }
            final_edits.append(enriched)
            
        target_file = LEGIT_FILE if edit_type == "LEGITTIMI" else VANDAL_FILE
        
        # Scrittura Safe con Lock
        append_to_json_file_safe(target_file, final_edits, file_lock)
        
        return final_edits
    except Exception as e:
        print(f"⚠️ Errore Gen Edits {topic_title}: {e}")
        return []

# --- MAIN CONTROLLER ---

def get_community_data_with_content(driver):
    query = """
    MATCH (n:Node)
    WHERE n.community IS NOT NULL 
      AND n.content IS NOT NULL 
      AND size(n.content) > 100
    WITH n.community AS comm_id, n, COUNT { (n)--() } AS degree
    ORDER BY degree DESC
    WITH comm_id, collect({
        id: n.id, 
        title: n.title, 
        content: n.content
    })[0..$limit] AS top_nodes, count(*) as size
    ORDER BY size DESC
    LIMIT 10
    RETURN comm_id, size, top_nodes
    """
    print("--- Interrogazione Neo4j ---")
    with driver.session() as session:
        result = session.run(query, limit=ARTICLES_PER_COMMUNITY)
        return [record.data() for record in result]

def generate_dataset():
    # Setup folders
    MOCK_DIR.mkdir(parents=True, exist_ok=True)
    if HTML_DIR.exists(): shutil.rmtree(HTML_DIR)
    HTML_DIR.mkdir(parents=True, exist_ok=True)
    if LEGIT_FILE.exists(): os.remove(LEGIT_FILE)
    if VANDAL_FILE.exists(): os.remove(VANDAL_FILE)

    # 1. Fetch Data
    driver = GraphDatabase.driver(URI, auth=AUTH)
    try:
        comm_data = get_community_data_with_content(driver)
        if not comm_data:
            print("❌ Nessuna community trovata.")
            return

        selected_comm = comm_data[0]
        nodes = selected_comm['top_nodes']
        target_topics = [n['title'] for n in nodes]
        topic_content_map = {n['title']: n['content'] for n in nodes}
        
        print(f"🎯 Community: {selected_comm['comm_id']} ({len(nodes)} nodi)")

        # --- SETUP MULTIPROCESSING ---
        manager = multiprocessing.Manager()
        usage_dict = manager.dict() # Condiviso tra processi
        file_lock = manager.Lock()
        
        # Keys Iteratore
        keys_count = len(API_KEYS)
        print(f"🔑 API Keys disponibili: {keys_count}")
        
        # 2. Generate HTML
        print("\n🚀 Generazione HTML...")
        generated_pages = {}
        
        with ProcessPoolExecutor(max_workers=keys_count) as executor:
            futures = {}
            for i, title in enumerate(target_topics):
                key = API_KEYS[i % keys_count] # Round Robin statico
                content = topic_content_map.get(title, "")[:2000]
                futures[executor.submit(generate_html_worker, key, title, content, usage_dict)] = title
                
            for future in as_completed(futures):
                res = future.result()
                if res:
                    generated_pages[res['title']] = res
                    
        # 3. Generate Edits
        print("\n🚀 Generazione Edits...")
        missing_legit = TARGET_LEGIT_EDITS
        missing_vandal = TARGET_VANDAL_EDITS
        
        while missing_legit > 0 or missing_vandal > 0:
            print(f"🔄 Mancano: {missing_legit} Legit, {missing_vandal} Vandal")
            
            with ProcessPoolExecutor(max_workers=keys_count) as executor:
                futures = []
                task_idx = 0
                
                # Distribuisci carico
                for title in target_topics:
                    real_text = topic_content_map.get(title, "")[:TEXT_LIMIT]
                    window = extract_random_window(real_text)
                    snippet = generated_pages.get(title, {}).get('content_snippet', "")
                    
                    # Calcola quanti edits fare per questo batch
                    # Facciamo piccoli batch (es. 2 edits) per distribuire
                    if missing_legit > 0:
                        key = API_KEYS[task_idx % keys_count]; task_idx += 1
                        futures.append(executor.submit(generate_edits_worker, key, title, "LEGITTIMI", 2, snippet, window, usage_dict, file_lock))
                        missing_legit -= 2
                        
                    if missing_vandal > 0:
                        key = API_KEYS[task_idx % keys_count]; task_idx += 1
                        futures.append(executor.submit(generate_edits_worker, key, title, "VANDALICI", 2, snippet, window, usage_dict, file_lock))
                        missing_vandal -= 2
                        
                    if missing_legit <= 0 and missing_vandal <= 0:
                        break
                
                # Attendi fine batch
                for future in as_completed(futures):
                    future.result() # Errori già loggati
                    
            # Ricalcola effettivamente quanti salvati (fonte di verità)
            # Ma per semplicità loop assumiamo decremento. 
            # In produzione: rileggere file JSON per conteggio esatto.
            # Qui ci fidiamo del decremento batch per evitare loop infiniti di lettura IO.
            
            if missing_legit < 0: missing_legit = 0
            if missing_vandal < 0: missing_vandal = 0
            
    finally:
        driver.close()
        print("\n✨ Finito.")

if __name__ == "__main__":
    generate_dataset()
