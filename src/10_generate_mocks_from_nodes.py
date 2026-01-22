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
from openai import OpenAI
from dotenv import load_dotenv
from neo4j import GraphDatabase

from config_loader import load_config

CONFIG = load_config()

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
HTML_DIR = DATA_DIR / "trusted_html_pages"
MOCK_DIR = DATA_DIR / "mocked_edits"
ENV_PATH = BASE_DIR / ".env"

LEGIT_FILE = MOCK_DIR / "legit_edits.json"
VANDAL_FILE = MOCK_DIR / "vandal_edits.json"
LEGIT_TEST_FILE = MOCK_DIR / "legit_edits_test.json"
VANDAL_TEST_FILE = MOCK_DIR / "vandal_edits_test.json"

load_dotenv(dotenv_path=ENV_PATH)

API_KEYS = []
i = 1
while True:
    key = os.getenv(f"GEMINI_API_KEY_{i}")
    if key:
        API_KEYS.append(key)
        i += 1
    else:
        break

PROVIDER = CONFIG['llm'].get('provider', 'gemini')

if PROVIDER == 'gemini' and not API_KEYS:
    print("- ERRORE: Nessuna API Key trovata nel .env")
    sys.exit(1)
elif PROVIDER == 'local':
    API_KEYS = ["local-key"]

MAX_REQ_PER_MIN = CONFIG['rate_limit']['max_req_per_min'] 
MAX_TOKENS_PER_MIN = CONFIG['rate_limit'].get('max_tokens_per_min', 15000)
WINDOW_SIZE = CONFIG['rate_limit']['window_size']
CONTEXT_WINDOW_SIZE = CONFIG['processing'].get('context_window_size', 600)

URI = CONFIG['neo4j']['uri']
AUTH = tuple(CONFIG['neo4j']['auth'])
MODEL_NAME = CONFIG['llm']['generation_model']
TEXT_LIMIT = CONFIG['processing']['text_limit']

TRAIN_LEGIT_COUNT = CONFIG['dataset']['training']['legit_count']
TRAIN_VANDAL_COUNT = CONFIG['dataset']['training']['vandal_count']
TEST_LEGIT_COUNT = CONFIG['dataset']['testing']['legit_count']
TEST_VANDAL_COUNT = CONFIG['dataset']['testing']['vandal_count']
ARTICLES_PER_COMMUNITY = CONFIG['dataset'].get('articles_per_community', 50)

def estimate_tokens(text):
    if not text: return 0
    return math.ceil(len(text) / 3.0)

def check_and_update_rate_limit(key, usage_dict, inputs_tokens=0):
    try:
        now = time.time()
        
        state = usage_dict.get(key, {'reqs': [], 'tokens': []})
        
        valid_reqs = [t for t in state['reqs'] if now - t < WINDOW_SIZE]
        valid_tokens = [(t, c) for t, c in state['tokens'] if now - t < WINDOW_SIZE]
        
        current_req_count = len(valid_reqs)
        current_token_count = sum(c for t, c in valid_tokens)
        
        if current_req_count >= MAX_REQ_PER_MIN:
            oldest_req = min(valid_reqs) if valid_reqs else now
            wait_time = max(1, (oldest_req + WINDOW_SIZE) - now)
            return False, f"Limite richieste ({current_req_count}/{MAX_REQ_PER_MIN})", wait_time
            
        if current_token_count + inputs_tokens >= MAX_TOKENS_PER_MIN:
            oldest_token = min(t for t, c in valid_tokens) if valid_tokens else now
            wait_time = max(1, (oldest_token + WINDOW_SIZE) - now)
            return False, f"Limite token ({current_token_count + inputs_tokens}/{MAX_TOKENS_PER_MIN})", wait_time
            
        valid_reqs.append(now)
        valid_tokens.append((now, inputs_tokens))
        
        try:
            usage_dict[key] = {'reqs': valid_reqs, 'tokens': valid_tokens}
        except (BrokenPipeError, ConnectionResetError, OSError) as e:
            pass
            
        return True, "OK", 0
    except Exception as e:
        return True, f"OK (bypass errore: {e})", 0

def append_to_json_file_safe(filepath, new_items, lock):
    with lock:
        data = []
        if filepath.exists():
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except json.JSONDecodeError:
                pass 
        
        data.extend(new_items)
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        print(f"- Salvati {len(new_items)} elementi in {filepath.name} (Totale: {len(data)})")

def extract_random_window(text, window_size=CONTEXT_WINDOW_SIZE):
    if not text: return ""
    if len(text) <= window_size: return text
    start_idx = random.randint(0, len(text) - window_size)
    while start_idx < len(text) and text[start_idx] not in (' ', '\n', '.'):
        start_idx += 1
    return text[start_idx : start_idx + window_size]

def clean_json_text(text):
    text = text.replace("```json", "").replace("```", "").strip()
    
    start = text.find('[')
    end = text.rfind(']')
    if start != -1 and end != -1:
        text = text[start : end + 1]
        
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None

def generate_html_worker(key, title, context_content, usage_dict):
    provider = CONFIG['llm'].get('provider', 'gemini')
    
    if provider == 'gemini':
        genai.configure(api_key=key)
        model = genai.GenerativeModel(MODEL_NAME)
    else:
        client = OpenAI(
            base_url=CONFIG['llm']['local']['base_url'],
            api_key=CONFIG['llm']['local']['api_key']
        )
        model_name = CONFIG['llm']['local'].get('model', MODEL_NAME)
    
    prompt_len = len(title) + len(context_content) + 500
    est_tokens = estimate_tokens("x" * prompt_len)
    
    ok, msg, wait_time = check_and_update_rate_limit(key, usage_dict, est_tokens)
    if not ok:
        print(f"- Rate Limit HTML ({key[-4:]}): {msg}. Attesa {wait_time:.1f}s...")
        time.sleep(wait_time)
        
    print(f"- [Inizio] HTML per: {title} (Chiave: ...{key[-4:]})")
    
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
    
    MAX_RETRIES = 5
    RETRY_WAIT = 60  
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            html_content = ""
            if provider == 'gemini':
                resp = model.generate_content(prompt)
                html_content = resp.text
            else:
                resp = client.chat.completions.create(
                    model=model_name,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.7
                )
                html_content = resp.choices[0].message.content

            html_content = html_content.replace("```html", "").replace("```", "").strip()
            
            clean_title = re.sub(r'[^\w]', '_', title)
            filename = f"trusted_{clean_title}.html"
            path = HTML_DIR / filename
            
            with open(path, "w", encoding="utf-8") as f:
                f.write(html_content)
                
            print(f"! [Fine] HTML salvato: {filename}")
            return {"title": title, "path": str(path), "content_snippet": html_content[:500]}
            
        except Exception as e:
            error_str = str(e)
            is_rate_limit = "429" in error_str or "quota" in error_str.lower() or "exhausted" in error_str.lower()
            
            if is_rate_limit:
                if attempt < MAX_RETRIES:
                    print(f"- Errore 429 ({key[-4:]}): HTML {title}. Attesa {RETRY_WAIT}s (tentativo {attempt}/{MAX_RETRIES})...")
                    time.sleep(RETRY_WAIT)
                    continue  
                else:
                    print(f"! QUOTA ESAURITA dopo {MAX_RETRIES} tentativi per CHIAVE ...{key[-4:]} (HTML {title}): {e}")
            else:
                print(f"! Errore Gen HTML {title} (Chiave: ...{key[-4:]}): {e}")
            return None
    
    return None  

def generate_edits_worker(key, topic_title, edit_type, count, context_snippet, real_text_window, usage_dict, file_lock, output_file):
    provider = CONFIG['llm'].get('provider', 'gemini')
    
    if provider == 'gemini':
        genai.configure(api_key=key)
        model = genai.GenerativeModel(MODEL_NAME)
    else:
        client = OpenAI(
            base_url=CONFIG['llm']['local']['base_url'],
            api_key=CONFIG['llm']['local']['api_key']
        )
        model_name = CONFIG['llm']['local'].get('model', MODEL_NAME)
    
    input_text = f"{context_snippet} {real_text_window} {topic_title} {edit_type}"
    est_tokens = estimate_tokens(input_text) + 500 
    
    while True:
        ok, msg, wait_time = check_and_update_rate_limit(key, usage_dict, est_tokens)
        if ok:
            break
        print(f"- Rate Limit ({key[-4:]}): {msg}. Attesa {wait_time:.1f}s...")
        time.sleep(wait_time)
    
    MAX_RETRIES = 5
    RETRY_WAIT = 60  
    
    print(f"- [Inizio] {count} Edit {edit_type} per {topic_title} (Chiave: ...{key[-4:]})")
    
    prompt = f"""
    Sei un generatore di dataset sintetici per il training di AI anti-vandalismo.
    Il tuo compito è creare dati "difficili" (adversarial examples) per rendere il modello robusto.
    
    Contesto reale (snippet): "{real_text_window}"
    
    Genera un JSON Array con {count} modifiche {"VANDALICHE" if edit_type == "VANDALICI" else "LEGITTIME"} su questo testo.

    === DIRETTIVA PRIMARIA ===
    Genera edit in modo che non salti subito all'occhio la categoria. L'obiettivo è mettere in difficoltà un classificatore.

    === REGOLE GLOBALI ===
    1. NO REPLICA: Non copiare mai testi o commenti di esempio.
    2. UTENTI: Alterna Username credibili e IP.
    3. COMMENTI: Devono variare in lunghezza e stile, coerenti con l'utente simulato.

    FORMATO JSON:
    [
      {{
        "user": "Stringa", 
        "comment": "Stringa", 
        "original_text": "Testo originale intero", 
        "new_text": "Testo modificato intero",
        "is_vandalism": {"true" if edit_type == "VANDALICI" else "false"}
      }}
    ]
    """

    if edit_type == "VANDALICI":
        prompt += """
    === LOGICA VANDALISMO ===
    TIPO A: SUBDOLO (60%) - Modifiche che sembrano vere (cambio date, nomi). Commento credibile.
    TIPO B: REALISTICO (40%) - Rimozione testo, opinioni. Commento sintetico o sospetto.
        """
    else:
        prompt += """
    === LOGICA EDIT LEGITTIMI ===
    TIPO A: RIFORMULATORE (60%) - Riscrivi frasi, aggiungi link. Commento: "stile", "migliorata fluidità".
    TIPO B: CORRETTORE MINIMO (40%) - Errori reali o sinonimi colti. Commento: "typo", "fix".
        """

    prompt += "Assicurati che il JSON sia valido."
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            text_resp = ""
            if provider == 'gemini':
                resp = model.generate_content(prompt)
                text_resp = resp.text
            else:
                resp = client.chat.completions.create(
                    model=model_name,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.7
                )
                text_resp = resp.choices[0].message.content
            
            edits = clean_json_text(text_resp)
            if not edits or not isinstance(edits, list):
                print(f"! Errore parsing JSON per {topic_title}: Non è una lista.")
                return []
                
            clean_title = re.sub(r'[^\w]', '_', topic_title)
            final_edits = []
            for edit in edits:
                enriched = {
                    "title": topic_title,
                    "user": edit.get("user", "Anon"),
                    "comment": edit.get("comment", "Edit"),
                    "timestamp": int(time.time()),
                    "is_vandalism": edit.get("is_vandalism", False),
                    "diff_url": f"https://it.wikipedia.org/w/index.php?title={clean_title}&diff=prev&oldid=000000",
                    "server_name": "it.wikipedia.org",
                    "wiki": "itwiki",
                    "original_text": edit.get("original_text", ""),
                    "new_text": edit.get("new_text", "")
                }
                final_edits.append(enriched)
                
            append_to_json_file_safe(output_file, final_edits, file_lock)
            return final_edits
            
        except Exception as e:
            error_str = str(e)
            is_rate_limit = "429" in error_str or "quota" in error_str.lower() or "exhausted" in error_str.lower()
            
            if is_rate_limit:
                if attempt < MAX_RETRIES:
                    print(f"- Errore 429 ({key[-4:]}): {topic_title}. Attesa {RETRY_WAIT}s (tentativo {attempt}/{MAX_RETRIES})...")
                    time.sleep(RETRY_WAIT)
                    continue  
                else:
                    print(f"! QUOTA ESAURITA dopo {MAX_RETRIES} tentativi per CHIAVE ...{key[-4:]} (Edit {topic_title}): {e}")
            else:
                print(f"! Errore Gen Edits {topic_title} (Chiave: ...{key[-4:]}): {e}")
            return []
    
    return []  

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

def ask_generation_mode():
    print("\n--- SELEZIONE MODALITÀ GENERAZIONE ---")
    print("1. Tutto (Trusted Sources + Edits)")
    print("2. Solo Trusted Sources (HTML)")
    print("3. Solo Edits (JSON)")
    choice = input("Scelta [1]: ").strip()
    if choice == "2": return "HTML_ONLY"
    if choice == "3": return "EDITS_ONLY"
    return "ALL"

def generate_dataset():
    MOCK_DIR.mkdir(parents=True, exist_ok=True)
    HTML_DIR.mkdir(parents=True, exist_ok=True)

    mode = ask_generation_mode()
    do_html = mode in ["ALL", "HTML_ONLY"]
    do_edits = mode in ["ALL", "EDITS_ONLY"]

    driver = GraphDatabase.driver(URI, auth=AUTH)
    try:
        comm_data = get_community_data_with_content(driver)
        if not comm_data:
            print("- Nessuna community trovata.")
            return

        selected_comm = comm_data[0]
        nodes = selected_comm['top_nodes']
        target_topics = [n['title'] for n in nodes]
        topic_content_map = {n['title']: n['content'] for n in nodes}
        
        print(f"! Community target: {selected_comm['comm_id']} ({len(nodes)} nodi)")

        manager = multiprocessing.Manager()
        usage_dict = manager.dict() 
        file_lock = manager.Lock()
        
        keys_count = len(API_KEYS)
        
        if PROVIDER == 'local':
            keys_count = 1
            print(f"- Modalità: LOCAL LLM (Worker singolo)")
        else:
            print(f"- API Keys disponibili: {keys_count}")
        
        print("\n🚀 Generazione HTML...")
        generated_pages = {}
        
        topics_to_generate = []
        for title in target_topics:
            clean_title = re.sub(r'[^\w]', '_', title)
            expected_path = HTML_DIR / f"trusted_{clean_title}.html"
            
            if expected_path.exists():
                print(f"- Salto HTML esistente: {title}")
                try:
                    with open(expected_path, "r", encoding="utf-8") as f:
                         content = f.read()
                         generated_pages[title] = {"title": title, "path": str(expected_path), "content_snippet": content[:500]}
                except Exception as e:
                    print(f"! Errore lettura {expected_path}: {e}")
            else:
                topics_to_generate.append(title)

        if do_html:
            if not topics_to_generate:
                 print("- Tutte le pagine HTML sono già presenti.")
            else:
                print(f"🚀 Inizio generazione per {len(topics_to_generate)} nuove pagine HTML...")
            
                with ProcessPoolExecutor(max_workers=keys_count) as executor:
                    futures = {}
                    for i, title in enumerate(topics_to_generate):
                        key = API_KEYS[i % keys_count] 
                        content = topic_content_map.get(title, "")[:2000]
                        futures[executor.submit(generate_html_worker, key, title, content, usage_dict)] = title
                    
                    completed_count = 0
                    total_gen = len(topics_to_generate)
                    for future in as_completed(futures):
                        try:
                            res = future.result()
                            completed_count += 1
                            if res:
                                generated_pages[res['title']] = res
                                print(f"[{completed_count}/{total_gen}] Completato {res['title']}")
                        except Exception as e:
                            completed_count += 1
                            print(f"! Errore Worker: {type(e).__name__}: {e}")
        else:
            print("- Salto generazione HTML (Scelta utente)")
                    
        if not do_edits:
            print("- Salto generazione Edits (Scelta utente)")
        else:
            print("\n🚀 Generazione Edits...")
            
            target_legit_train = TRAIN_LEGIT_COUNT
            target_vandal_train = TRAIN_VANDAL_COUNT
            
            target_legit_test = TEST_LEGIT_COUNT
            target_vandal_test = TEST_VANDAL_COUNT
            
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
            
            print(f"- Stato Training: {current_legit_train}/{target_legit_train} Legit, {current_vandal_train}/{target_vandal_train} Vandal")
            print(f"- Stato Test:     {current_legit_test}/{target_legit_test} Legit, {current_vandal_test}/{target_vandal_test} Vandal")
            
            while (missing_legit_train > 0 or missing_vandal_train > 0 or 
                   missing_legit_test > 0 or missing_vandal_test > 0):
                
                print(f"🔄 Mancano Train: {missing_legit_train} L, {missing_vandal_train} V | Test: {missing_legit_test} L, {missing_vandal_test} V")
                
                with ProcessPoolExecutor(max_workers=keys_count) as executor:
                    futures = []
                    task_idx = 0
                    
                    for title in target_topics:
                        real_text = topic_content_map.get(title, "")[:TEXT_LIMIT]
                        window = extract_random_window(real_text)
                        snippet = generated_pages.get(title, {}).get('content_snippet', "")
                        
                        if missing_legit_train > 0:
                            key = API_KEYS[task_idx % keys_count]; task_idx += 1
                            futures.append(executor.submit(generate_edits_worker, key, title, "LEGITTIMI", 2, snippet, window, usage_dict, file_lock, LEGIT_FILE))
                            missing_legit_train -= 2
                        
                        if missing_vandal_train > 0:
                            key = API_KEYS[task_idx % keys_count]; task_idx += 1
                            futures.append(executor.submit(generate_edits_worker, key, title, "VANDALICI", 2, snippet, window, usage_dict, file_lock, VANDAL_FILE))
                            missing_vandal_train -= 2
                            
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
                    
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as e:
                            print(f"! Errore Worker: {type(e).__name__}: {e}")
                        
                missing_legit_train = max(0, missing_legit_train)
                missing_vandal_train = max(0, missing_vandal_train)
                missing_legit_test = max(0, missing_legit_test)
                missing_vandal_test = max(0, missing_vandal_test)
            
    finally:
        driver.close()
        print("\n- Finito.")

if __name__ == "__main__":
    generate_dataset()
