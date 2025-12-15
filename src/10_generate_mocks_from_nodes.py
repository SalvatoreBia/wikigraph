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
LEGIT_TEST_FILE = MOCK_DIR / "legit_edits_test.json"
VANDAL_TEST_FILE = MOCK_DIR / "vandal_edits_test.json"

load_dotenv(dotenv_path=ENV_PATH)

# Retrieve Keys
API_KEYS = [
    os.getenv("GEMINI_API_KEY_1"),
    os.getenv("GEMINI_API_KEY_2"),
    os.getenv("GEMINI_API_KEY_3"),
    os.getenv("GEMINI_API_KEY_4")
]
API_KEYS = [k for k in API_KEYS if k]
if not API_KEYS:
    print("❌ ERRORE: Nessuna API Key trovata nel .env")
    sys.exit(1)

# Rate Limiting & Token Config
MAX_REQ_PER_MIN = CONFIG['rate_limit']['max_req_per_min'] 
MAX_TOKENS_PER_MIN = 15000 
WINDOW_SIZE = CONFIG['rate_limit']['window_size']
CONTEXT_WINDOW_SIZE = CONFIG['processing'].get('context_window_size', 600)

# Neo4j Config
URI = CONFIG['neo4j']['uri']
AUTH = tuple(CONFIG['neo4j']['auth'])
MODEL_NAME = CONFIG['llm']['generation_model']
TEXT_LIMIT = CONFIG['processing']['text_limit']

# Simulation Config
TRAIN_LEGIT_COUNT = CONFIG['dataset']['training']['legit_count']
TRAIN_VANDAL_COUNT = CONFIG['dataset']['training']['vandal_count']
TEST_LEGIT_COUNT = CONFIG['dataset']['testing']['legit_count']
TEST_VANDAL_COUNT = CONFIG['dataset']['testing']['vandal_count']
ARTICLES_PER_COMMUNITY = CONFIG['dataset'].get('articles_per_community', 50)

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

def generate_edits_worker(key, topic_title, edit_type, count, context_snippet, real_text_window, usage_dict, file_lock, output_file):
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
    
    REGOLE CRITICHE:
    - original_text DEVE esistere nel snippet.
    - Output ESCLUSIVAMENTE il JSON.
    """

    # AGGIUNTA LOGICA SPECIFICA PER I VANDALISMI
    if edit_type == "VANDALICI":
        prompt += """
    REGOLE VANDALISMO (SUBDOLO):
    1. NON usare insulti, parolacce o cancellazioni massive (troppo facile da rilevare).
    2. INVERTI I FATTI: Cambia "vinto" in "perso", "nato" in "morto", "nord" in "sud".
    3. FALSIFICAZIONI PLAUSIBILI: Cambia date (es. 1990 -> 1995), nomi di luoghi o persone in modo che sembri vero.
    4. MANTIENI LO STILE: Il testo modificato deve sembrare scritto da un enciclopedista serio.
    5. COMMENTO INGANNEVOLE: Nel campo "comment", scrivi cose come "correzione data", "typo", "aggiornamento fonte" per ingannare i revisori.
        """
    else:
        prompt += """
    REGOLE LEGITTIMI:
    1. Correggi solo refusi reali, punteggiatura o migliora la leggibilità.
    2. Il significato della frase NON deve cambiare.
    3. Commenti onesti.
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
            
        target_file = output_file
        
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
    HTML_DIR.mkdir(parents=True, exist_ok=True)

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
        
        # Check existing HTML files
        existing_html_files = list(HTML_DIR.glob("trusted_*.html"))
        existing_titles = set()
        for p in existing_html_files:
            # Filename format: trusted_{clean_title}.html
            # We can't easily reverse clean_title -> title perfectly if there are collisions, 
            # but we can try to match or just trust the file presence for the "clean" version.
            # Faster approach: Check if expected output file exists for each target topic.
            pass

        topics_to_generate = []
        for title in target_topics:
            clean_title = re.sub(r'[^\w]', '_', title)
            expected_path = HTML_DIR / f"trusted_{clean_title}.html"
            
            if expected_path.exists():
                print(f"⏩ Skip HTML esistente: {title}")
                # Load content snippet for later use in edits
                try:
                    with open(expected_path, "r", encoding="utf-8") as f:
                         content = f.read()
                         generated_pages[title] = {"title": title, "path": str(expected_path), "content_snippet": content[:500]}
                except Exception as e:
                    print(f"⚠️ Errore lettura {expected_path}: {e}")
            else:
                topics_to_generate.append(title)

        if not topics_to_generate:
             print("✅ Tutte le pagine HTML sono già presenti.")
        else:
            print(f"🚀 Inizio generazione per {len(topics_to_generate)} nuove pagine HTML...")
        
        with ProcessPoolExecutor(max_workers=keys_count) as executor:
            futures = {}
            for i, title in enumerate(topics_to_generate):
                key = API_KEYS[i % keys_count] # Round Robin statico
                content = topic_content_map.get(title, "")[:2000]
                futures[executor.submit(generate_html_worker, key, title, content, usage_dict)] = title
            
            completed_count = 0
            total_gen = len(topics_to_generate)
            for future in as_completed(futures):
                res = future.result()
                completed_count += 1
                if res:
                    generated_pages[res['title']] = res
                    print(f"[{completed_count}/{total_gen}] Copletato {res['title']}")
                    
        # 3. Generate Edits
        print("\n🚀 Generazione Edits...")
        
        # 3. Generate Edits
        print("\n🚀 Generazione Edits...")
        
        # Calcolo Target
        target_legit_train = TRAIN_LEGIT_COUNT
        target_vandal_train = TRAIN_VANDAL_COUNT
        
        target_legit_test = TEST_LEGIT_COUNT
        target_vandal_test = TEST_VANDAL_COUNT
        
        # Count existing edits
        def count_edits(filepath):
            if filepath.exists():
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        return len(json.load(f))
                except Exception: return 0
            return 0

        current_legit_train = count_edits(LEGIT_FILE)
        current_vandal_train = count_edits(VANDAL_FILE)
        current_legit_test = count_edits(LEGIT_TEST_FILE)
        current_vandal_test = count_edits(VANDAL_TEST_FILE)

        missing_legit_train = max(0, target_legit_train - current_legit_train)
        missing_vandal_train = max(0, target_vandal_train - current_vandal_train)
        missing_legit_test = max(0, target_legit_test - current_legit_test)
        missing_vandal_test = max(0, target_vandal_test - current_vandal_test)
        
        print(f"📊 Stato Training: {current_legit_train}/{target_legit_train} Legit, {current_vandal_train}/{target_vandal_train} Vandal")
        print(f"📊 Stato Test:     {current_legit_test}/{target_legit_test} Legit, {current_vandal_test}/{target_vandal_test} Vandal")
        
        while (missing_legit_train > 0 or missing_vandal_train > 0 or 
               missing_legit_test > 0 or missing_vandal_test > 0):
            
            print(f"🔄 Mancano Train: {missing_legit_train} L, {missing_vandal_train} V | Test: {missing_legit_test} L, {missing_vandal_test} V")
            
            with ProcessPoolExecutor(max_workers=keys_count) as executor:
                futures = []
                task_idx = 0
                
                # Distribuisci carico
                for title in target_topics:
                    real_text = topic_content_map.get(title, "")[:TEXT_LIMIT]
                    window = extract_random_window(real_text)
                    snippet = generated_pages.get(title, {}).get('content_snippet', "")
                    
                    # Generazione Training
                    if missing_legit_train > 0:
                        key = API_KEYS[task_idx % keys_count]; task_idx += 1
                        futures.append(executor.submit(generate_edits_worker, key, title, "LEGITTIMI", 2, snippet, window, usage_dict, file_lock, LEGIT_FILE))
                        missing_legit_train -= 2
                    
                    if missing_vandal_train > 0:
                        key = API_KEYS[task_idx % keys_count]; task_idx += 1
                        futures.append(executor.submit(generate_edits_worker, key, title, "VANDALICI", 2, snippet, window, usage_dict, file_lock, VANDAL_FILE))
                        missing_vandal_train -= 2
                        
                    # Generazione Test (solo se non servono più training, o interleaving - qui interleaving semplice)
                    # Nota: priorità a training se vogliamo, ma qui facciamo round robin
                    
                    if missing_legit_test > 0:
                         key = API_KEYS[task_idx % keys_count]; task_idx += 1
                         futures.append(executor.submit(generate_edits_worker, key, title, "LEGITTIMI", 2, snippet, window, usage_dict, file_lock, LEGIT_TEST_FILE))
                         missing_legit_test -= 2
                         
                    if missing_vandal_test > 0:
                         key = API_KEYS[task_idx % keys_count]; task_idx += 1
                         futures.append(executor.submit(generate_edits_worker, key, title, "VANDALICI", 2, snippet, window, usage_dict, file_lock, VANDAL_TEST_FILE))
                         missing_vandal_test -= 2

                    if (missing_legit_train <= 0 and missing_vandal_train <= 0 and 
                        missing_legit_test <= 0 and missing_vandal_test <= 0):
                        break
                
                # Attendi fine batch
                for future in as_completed(futures):
                    future.result() # Errori già loggati
                    
            # Safe reset negative counters
            missing_legit_train = max(0, missing_legit_train)
            missing_vandal_train = max(0, missing_vandal_train)
            missing_legit_test = max(0, missing_legit_test)
            missing_vandal_test = max(0, missing_vandal_test)
            
    finally:
        driver.close()
        print("\n✨ Finito.")

if __name__ == "__main__":
    generate_dataset()
