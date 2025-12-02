import json
import os
import random
import re
import sys
import threading
import time
import uuid
import shutil
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import cycle
from pathlib import Path

import google.generativeai as genai
from dotenv import load_dotenv
from neo4j import GraphDatabase

# --- CONFIGURAZIONE ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
HTML_DIR = DATA_DIR / "trusted_html_pages"
MOCK_DIR = DATA_DIR / "mocked_edits"
ENV_PATH = BASE_DIR / ".env"

# File di output separati
LEGIT_FILE = MOCK_DIR / "legit_edits.json"
VANDAL_FILE = MOCK_DIR / "vandal_edits.json"

load_dotenv(dotenv_path=ENV_PATH)

# API Keys Round Robin
API_KEYS = [
    os.getenv("GEMINI_API_KEY"),
    os.getenv("GEMINI_API_KEY_2"),
    os.getenv("GEMINI_API_KEY_3")
]
API_KEYS = [k for k in API_KEYS if k]
if not API_KEYS:
    print("❌ ERRORE: Nessuna API Key trovata nel .env")
    sys.exit(1)

api_key_cycle = cycle(API_KEYS)
key_lock = threading.Lock()

# Rate Limiting
KEY_USAGE = defaultdict(list)
MAX_REQ_PER_MIN = 2
WINDOW_SIZE = 65  # seconds

def get_next_api_key():
    with key_lock:
        while True:
            now = time.time()
            # Try to find an available key
            for _ in range(len(API_KEYS)):
                key = next(api_key_cycle)
                # Clean old timestamps
                KEY_USAGE[key] = [t for t in KEY_USAGE[key] if now - t < WINDOW_SIZE]
                
                if len(KEY_USAGE[key]) < MAX_REQ_PER_MIN:
                    KEY_USAGE[key].append(now)
                    return key
            
            # If all keys are busy, wait a bit
            print("⏳ Rate limit hit on all keys. Waiting 5s...")
            time.sleep(5)

# Neo4j Config
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "password")
MODEL_NAME = "gemini-2.5-pro"

# Configuration for Mock Generation
TARGET_LEGIT_EDITS = 10
TARGET_VANDAL_EDITS = 10

# Lock per scrittura file
file_lock = threading.Lock()

# --- UTILS ---

def append_to_json_file(filepath, new_items):
    """Legge, aggiorna e salva il file JSON in modo thread-safe."""
    with file_lock:
        data = []
        if filepath.exists():
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except json.JSONDecodeError:
                pass # File corrotto o vuoto, sovrascriviamo
        
        data.extend(new_items)
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        print(f"💾 Salvati {len(new_items)} items in {filepath.name} (Totale: {len(data)})")

def extract_random_window(text, window_size=600):
    """Estrae una finestra di testo casuale."""
    if not text:
        return ""
    if len(text) <= window_size:
        return text
    
    # Try to find a sentence start
    start_idx = random.randint(0, len(text) - window_size)
    
    # Simple adjustment: find first space after start_idx to avoid cutting words
    while start_idx < len(text) and text[start_idx] not in (' ', '\n', '.'):
        start_idx += 1
        
    window = text[start_idx : start_idx + window_size]
    return window

# --- NEO4J DATA FETCHING (FULL CONTENT) ---

def get_community_data_with_content(driver):
    """
    Recupera le community e per i nodi principali scarica direttamente
    Titolo e Contenuto dal Grafo, evitando CSV esterni.
    """
    query = """
    MATCH (n:Node)
    WHERE n.community IS NOT NULL 
      AND n.content IS NOT NULL 
      AND n.title IS NOT NULL
      AND size(n.content) > 100
    WITH n.community AS comm_id, n
    WITH comm_id, n, COUNT { (n)--() } AS degree
    ORDER BY degree DESC
    WITH comm_id, collect({
        id: n.id, 
        title: n.title, 
        content: n.content
    })[0..5] AS top_nodes, count(*) as size
    ORDER BY size DESC
    LIMIT 10
    RETURN comm_id, size, top_nodes
    """
    print("--- Interrogazione Neo4j (Fetch Community + Content) ---")
    with driver.session() as session:
        result = session.run(query)
        return [record.data() for record in result]

def print_report(communities_data):
    print("\n" + "="*60)
    print("REPORT TEMI COMMUNITY (DATI DA NEO4J)")
    print("="*60)
    for comm in communities_data:
        c_id = comm['comm_id']
        size = comm['size']
        nodes = comm['top_nodes']
        print(f"\n📂 COMMUNITY {c_id} (Nodi: {size})")
        print(f"   Argomenti principali (Hubs):")
        for node in nodes:
            print(f"   - {node['title']} (Lunghezza content: {len(node['content'])})")

# --- GENERAZIONE CONTENUTI CON GEMINI ---

def generate_html_task(title):
    """Task singolo per generare HTML."""
    key = get_next_api_key()
    genai.configure(api_key=key)
    model = genai.GenerativeModel(MODEL_NAME)
    
    print(f"📄 [Start] HTML per: {title}")
    prompt = f"""
    Sei un giornalista esperto. Scrivi un articolo dettagliato e affidabile (almeno 800 parole) su: "{title}".
    Il contenuto deve sembrare una vera pagina web di notizie o enciclopedia.
    REGOLE:
    1. Usa HTML puro (<body>, <h1>, <h2>, <p>, <ul>, <strong>, ecc.). Niente CSS o JS esterni.
    2. Includi una sezione "Fonti" o "Riferimenti" fittizia alla fine.
    3. Stile sobrio, fattuale, enciclopedico.
    4. Non usare Markdown. Restituisci solo il codice HTML.
    """
    try:
        resp = model.generate_content(prompt)
        html_content = resp.text.replace("```html", "").replace("```", "").strip()
        
        clean_title = re.sub(r'[^\w]', '_', title)
        filename = f"trusted_{clean_title}.html"
        path = HTML_DIR / filename
        
        with open(path, "w", encoding="utf-8") as f:
            f.write(html_content)
            
        print(f"✅ [Done] HTML salvato: {filename}")
        return {"title": title, "path": str(path), "content_snippet": html_content[:500]}
    except Exception as e:
        print(f"⚠️ Errore HTML {title}: {e}")
        return None

def generate_edits_task(topic_title, edit_type, count, context_snippet, real_text_window=None):
    """Task singolo per generare un batch di edit."""
    key = get_next_api_key()
    genai.configure(api_key=key)
    model = genai.GenerativeModel(MODEL_NAME)
    
    print(f"✍️  [Start] {count} Edits {edit_type} per {topic_title}")
    
    if real_text_window:
        # PROMPT CON TESTO REALE (DAL DB)
        prompt = f"""
        Ecco un testo REALE estratto dal database (Wikitext):
        "{real_text_window}"
        
        Il tuo compito è generare un JSON Array con ESATTAMENTE {count} modifiche simulate (edits) SU QUESTO TESTO SPECIFICO.
        
        TIPO MODIFICHE: {edit_type}
        
        FORMATO OGGETTO JSON:
        {{
            "user": "NomeUtente",
            "comment": "Descrizione della modifica",
            "original_text": "Il testo originale (deve essere una sottostringa esatta del testo fornito sopra)",
            "new_text": "Il testo modificato",
            "timestamp": 1234567890,
            "length": {{ "old": 123, "new": 145 }},
            "is_vandalism": { "true" if edit_type == "VANDALICI" else "false" }
        }}
        
        ISTRUZIONI:
        - Se LEGITTIMI: Fai modifiche UTILI ma MINIME. Correggi punteggiatura, migliora la scorrevolezza, correggi un typo. NON stravolgere il testo. Deve sembrare un edit di un utente meticoloso.
        - Se VANDALICI: Fai modifiche SUBDOLE e DIFFICILI DA INDIVIDUARE.
            * Cambia una data (es. 1995 -> 1994).
            * Inverti un nome (es. Mario Rossi -> Paolo Rossi).
            * Modifica leggermente una statistica (es. 50% -> 52%).
            * Inserisci un fatto plausibile ma falso.
            * NON usare insulti, NON scrivere cose senza senso, NON cancellare tutto. Deve sembrare vero a una lettura veloce.
        - "original_text" DEVE essere presente nel testo fornito.
        Output solo il JSON Array.
        """
    else:
        # PROMPT FALLBACK (se il testo DB fosse vuoto per errore)
        prompt = f"""
        Contesto: Un articolo affidabile su "{topic_title}".
        Genera un JSON Array con ESATTAMENTE {count} modifiche simulate (edits) stile Wikipedia.
        TIPO MODIFICHE: {edit_type}
        FORMATO OGGETTO JSON:
        {{
            "user": "NomeUtente",
            "comment": "Descrizione della modifica",
            "original_text": "Il testo esatto prima della modifica",
            "new_text": "Il testo dopo la modifica",
            "timestamp": 1234567890,
            "length": {{ "old": 10000, "new": 10050 }},
            "is_vandalism": { "true" if edit_type == "VANDALICI" else "false" }
        }}
        ISTRUZIONI:
        - Se VANDALICI: Sii subdolo. Cambia date, nomi, fatti in modo credibile. Niente insulti palesi.
        Output solo il JSON Array.
        """
    try:
        resp = model.generate_content(prompt, generation_config={"response_mime_type": "application/json"})
        edits = json.loads(resp.text)
        
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
                "timestamp": edit.get("timestamp", int(time.time())),
                "length": edit.get("length", {"old": 1000, "new": 1000}),
                "is_vandalism": edit.get("is_vandalism", False),
                "meta": {
                    "domain": "it.wikipedia.org",
                    "uri": f"https://it.wikipedia.org/wiki/{clean_title}"
                }
            }
            final_edits.append(enriched)
            
        target_file = LEGIT_FILE if edit_type == "LEGITTIMI" else VANDAL_FILE
        append_to_json_file(target_file, final_edits)
        
        print(f"✅ [Done] {len(final_edits)} Edits {edit_type} per {topic_title}")
        return final_edits
    except Exception as e:
        print(f"⚠️ Errore Edits {topic_title} ({edit_type}): {e}")
        return []

def count_valid_html_pages():
    if not HTML_DIR.exists(): return 0
    valid_pages = 0
    for html_file in HTML_DIR.glob("*.html"):
        try:
            if html_file.stat().st_size > 100: valid_pages += 1
        except Exception: continue
    return valid_pages

def count_valid_edits(filepath):
    if not filepath.exists(): return 0
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, list): return 0
            valid_count = 0
            for edit in data:
                if all(key in edit for key in ["id", "user", "comment", "timestamp", "is_vandalism"]):
                    valid_count += 1
            return valid_count
    except (json.JSONDecodeError, FileNotFoundError):
        return 0

def generate_dataset():
    # Setup directory
    if HTML_DIR.exists():
        print(f"🧹 Pulizia directory HTML: {HTML_DIR}")
        shutil.rmtree(HTML_DIR)
    HTML_DIR.mkdir(parents=True, exist_ok=True)
    
    MOCK_DIR.mkdir(parents=True, exist_ok=True)
    # Pulizia file edits precedenti per ripartire da zero
    if LEGIT_FILE.exists(): os.remove(LEGIT_FILE)
    if VANDAL_FILE.exists(): os.remove(VANDAL_FILE)
    
    existing_html_pages = 0 # Ripartiamo da zero
    existing_legit = 0
    existing_vandal = 0
    
    missing_legit = max(0, TARGET_LEGIT_EDITS - existing_legit)
    missing_vandal = max(0, TARGET_VANDAL_EDITS - existing_vandal)
    
    print(f"\n🎯 Da generare: {missing_legit} Legit, {missing_vandal} Vandal")

    # 1. Trova Community e CONTENUTO direttamente da Neo4j
    driver = GraphDatabase.driver(URI, auth=AUTH)
    try:
        # Questa funzione ora ritorna anche il contenuto
        comm_data = get_community_data_with_content(driver)
        
        if not comm_data: 
            print("❌ Nessuna community trovata con contenuti validi in Neo4j.")
            return

        print_report(comm_data)
        
        # Selezione casuale community
        selected_comm = random.choice(comm_data)
        
        # Estrazione dati direttamente dal risultato Neo4j
        # target_nodes è una lista di dict: {'id':..., 'title':..., 'content':...}
        target_nodes = selected_comm['top_nodes']
        
        target_topics = [n['title'] for n in target_nodes]
        # Mappa per accesso rapido al contenuto per titolo
        topic_content_map = {n['title']: n['content'] for n in target_nodes}
        
        print(f"\n🎯 COMMUNITY SELEZIONATA: {selected_comm['comm_id']}")
        print(f"   Topics Target (5): {target_topics}")
        
        # 2. Generazione HTML Parallela
        generated_pages = {}
        if existing_html_pages >= 5:
            print("\n✅ Pagine HTML già presenti (5/5), skip generazione HTML")
            for title in target_topics:
                clean_title = re.sub(r'[^\w]', '_', title)
                html_path = HTML_DIR / f"trusted_{clean_title}.html"
                if html_path.exists():
                    try:
                        with open(html_path, "r", encoding="utf-8") as f:
                            content = f.read()
                            generated_pages[title] = {
                                "title": title,
                                "content_snippet": content[:500]
                            }
                    except Exception: pass
        else:
            print("\n🚀 Avvio Generazione HTML (Parallela)...")
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {executor.submit(generate_html_task, title): title for title in target_topics}
                for future in as_completed(futures):
                    res = future.result()
                    if res:
                        generated_pages[res['title']] = res
        
        # 3. Generazione Edits con Retry Loop
        MAX_RETRIES = 5
        attempt = 0
        
        while attempt < MAX_RETRIES:
            current_legit = count_valid_edits(LEGIT_FILE)
            current_vandal = count_valid_edits(VANDAL_FILE)
            missing_legit = max(0, TARGET_LEGIT_EDITS - current_legit)
            missing_vandal = max(0, TARGET_VANDAL_EDITS - current_vandal)
            
            if missing_legit == 0 and missing_vandal == 0:
                print("\n✅ Target Raggiunto.")
                break
                
            print(f"\n🔄 [Tentativo {attempt+1}/{MAX_RETRIES}] Mancano: {missing_legit} Legit, {missing_vandal} Vandal")
            
            num_topics = len(target_topics)
            legit_per_topic = missing_legit // num_topics if missing_legit > 0 else 0
            vandal_per_topic = missing_vandal // num_topics if missing_vandal > 0 else 0
            legit_remainder = missing_legit % num_topics
            vandal_remainder = missing_vandal % num_topics
            
            print(f"🚀 Avvio Generazione Edits (Parallela)...")
            
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = []
                for idx, title in enumerate(target_topics):
                    # Prendo il testo reale DIRETTAMENTE dalla mappa caricata da Neo4j
                    real_text = topic_content_map.get(title, "")
                    
                    window = extract_random_window(real_text)
                    snippet = generated_pages.get(title, {}).get('content_snippet', "")
                    
                    l_count = legit_per_topic + (1 if idx < legit_remainder else 0)
                    v_count = vandal_per_topic + (1 if idx < vandal_remainder else 0)
                    
                    if l_count > 0:
                        futures.append(executor.submit(generate_edits_task, title, "LEGITTIMI", l_count, snippet, window))
                    if v_count > 0:
                        futures.append(executor.submit(generate_edits_task, title, "VANDALICI", v_count, snippet, window))
                
                for future in as_completed(futures):
                    future.result()
            
            attempt += 1
                
        print("\n✨ Generazione Completata!")
        print(f"📂 Legit File: {LEGIT_FILE}")
        print(f"📂 Vandal File: {VANDAL_FILE}")

    except Exception as e:
        print(f"❌ Errore Main: {e}")
    finally:
        driver.close()

if __name__ == "__main__":
    generate_dataset()
