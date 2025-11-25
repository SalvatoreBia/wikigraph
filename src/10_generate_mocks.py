import os
import json
import time
import random
import re
import csv
import sys
import uuid
import threading
from pathlib import Path
from itertools import cycle
from concurrent.futures import ThreadPoolExecutor, as_completed

import google.generativeai as genai
from dotenv import load_dotenv
from neo4j import GraphDatabase

# --- CONFIGURAZIONE ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
HTML_DIR = DATA_DIR / "trusted_html_pages"
MOCK_DIR = DATA_DIR / "mocked_edits"
ENV_PATH = BASE_DIR / ".env"
PAGEMAP_FILE = DATA_DIR / "pagemap.csv"

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

def get_next_api_key():
    with key_lock:
        key = next(api_key_cycle)
    return key

# Neo4j Config
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "password")
MODEL_NAME = "gemini-2.5-pro"

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

# --- FUNZIONI DA SCRIPT 7 (COMMUNITY DETECTION) ---

def get_top_communities(driver):
    query = """
    MATCH (n:Node)
    WHERE n.community IS NOT NULL
    WITH n.community AS comm_id, n
    WITH comm_id, n, COUNT { (n)--() } AS degree
    ORDER BY degree DESC
    WITH comm_id, collect(n.id)[0..5] AS top_nodes, count(*) as size
    ORDER BY size DESC
    LIMIT 10
    RETURN comm_id, size, top_nodes
    """
    print("--- Interrogazione Neo4j per trovare i Leader delle Community ---")
    with driver.session() as session:
        result = session.run(query)
        return [record.data() for record in result]

def resolve_names(communities_data, map_file):
    target_ids = set()
    for item in communities_data:
        for node_id in item['top_nodes']:
            target_ids.add(str(node_id))
            
    print(f"--- Ricerca nomi per {len(target_ids)} nodi nel file {map_file} ---")
    id_to_name = {}
    try:
        with open(map_file, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                parts = line.strip().split(',', 1)
                if len(parts) < 2: continue
                curr_id = parts[0].strip()
                if curr_id in target_ids:
                    id_to_name[curr_id] = parts[1].replace("'", "").strip()
                    if len(id_to_name) == len(target_ids): break
    except FileNotFoundError:
        print(f"ERRORE: File {map_file} non trovato.")
        sys.exit(1)
    return id_to_name

def print_report(communities_data, id_to_name):
    print("\n" + "="*60)
    print("REPORT TEMI COMMUNITY")
    print("="*60)
    for comm in communities_data:
        c_id = comm['comm_id']
        size = comm['size']
        nodes = comm['top_nodes']
        node_names = [id_to_name.get(str(nid), f"ID_{nid}") for nid in nodes]
        print(f"\n📂 COMMUNITY {c_id} (Nodi: {size})")
        print(f"   Argomenti principali (Hubs):")
        for name in node_names:
            print(f"   - {name}")

# --- GENERAZIONE CONTENUTI ---

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

def generate_edits_task(topic_title, edit_type, count, context_snippet):
    """Task singolo per generare un batch di edit."""
    key = get_next_api_key()
    genai.configure(api_key=key)
    model = genai.GenerativeModel(MODEL_NAME)
    
    print(f"✍️  [Start] {count} Edits {edit_type} per {topic_title}")
    prompt = f"""
    Contesto: Un articolo affidabile su "{topic_title}".
    Genera un JSON Array con ESATTAMENTE {count} modifiche simulate (edits) stile Wikipedia.
    TIPO MODIFICHE: {edit_type}
    FORMATO OGGETTO JSON:
    {{
        "user": "NomeUtente",
        "comment": "Descrizione della modifica",
        "timestamp": 1234567890,
        "length": {{ "old": 10000, "new": 10050 }},
        "is_vandalism": { "true" if edit_type == "VANDALICI" else "false" }
    }}
    DESCRIZIONE TIPO:
    - Se LEGITTIMI: Correzioni typo, aggiunta fonti, riformulazione. Commenti seri.
    - Se VANDALICI: Insulti, cancellazione testo, ALL CAPS, propaganda. Commenti provocatori.
    Output solo il JSON Array.
    """
    try:
        resp = model.generate_content(prompt, generation_config={"response_mime_type": "application/json"})
        edits = json.loads(resp.text)
        
        # Arricchimento dati
        clean_title = re.sub(r'[^\w]', '_', topic_title)
        final_edits = []
        for edit in edits:
            enriched = {
                "id": str(uuid.uuid4()),
                "type": "edit",
                "title": topic_title,
                "user": edit.get("user", "Anon"),
                "comment": edit.get("comment", "Edit"),
                "timestamp": edit.get("timestamp", int(time.time())),
                "length": edit.get("length", {"old": 1000, "new": 1000}),
                "is_vandalism": edit.get("is_vandalism", False),
                "meta": {
                    "domain": "it.wikipedia.org",
                    "uri": f"https://it.wikipedia.org/wiki/{clean_title}"
                }
            }
            final_edits.append(enriched)
            
        # Salvataggio incrementale
        target_file = LEGIT_FILE if edit_type == "LEGITTIMI" else VANDAL_FILE
        append_to_json_file(target_file, final_edits)
        
        print(f"✅ [Done] {len(final_edits)} Edits {edit_type} per {topic_title}")
        return final_edits
    except Exception as e:
        print(f"⚠️ Errore Edits {topic_title} ({edit_type}): {e}")
        return []

def generate_dataset():
    # Setup directory
    HTML_DIR.mkdir(parents=True, exist_ok=True)
    MOCK_DIR.mkdir(parents=True, exist_ok=True)
    print(f"📂 HTML_DIR: {HTML_DIR.resolve()}")
    print(f"📂 MOCK_DIR: {MOCK_DIR.resolve()}")
    
    # Pulisci file precedenti se vuoi ripartire da zero (opzionale, qui appendiamo)
    if LEGIT_FILE.exists(): LEGIT_FILE.unlink()
    if VANDAL_FILE.exists(): VANDAL_FILE.unlink()

    # 1. Trova Community
    driver = GraphDatabase.driver(URI, auth=AUTH)
    try:
        comm_data = get_top_communities(driver)
        if not comm_data: return
        mapping = resolve_names(comm_data, PAGEMAP_FILE)
        print_report(comm_data, mapping)
        
        selected_comm = random.choice(comm_data)
        top_nodes_ids = selected_comm['top_nodes']
        top_nodes_titles = [mapping.get(str(nid), f"Unknown_{nid}") for nid in top_nodes_ids]
        
        # Prendiamo 5 topic (o tutti se < 5)
        target_topics = top_nodes_titles[:5]
        print(f"\n🎯 COMMUNITY SELEZIONATA: {selected_comm['comm_id']}")
        print(f"   Topics Target (5): {target_topics}")
        
        # 2. Generazione HTML Parallela
        print("\n🚀 Avvio Generazione HTML (Parallela)...")
        generated_pages = {}
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(generate_html_task, title): title for title in target_topics}
            for future in as_completed(futures):
                res = future.result()
                if res:
                    generated_pages[res['title']] = res
        
        # 3. Generazione Edits Parallela
        # Target: 200 edits totali. Diviso per numero topic.
        # Se 5 topic -> 40 edit a testa (20 legit, 20 vandal)
        edits_per_topic = 200 // len(target_topics)
        half_edits = edits_per_topic // 2 # 20 legit, 20 vandal
        
        print(f"\n🚀 Avvio Generazione Edits (Parallela) - {edits_per_topic} per topic...")
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            for title in target_topics:
                snippet = generated_pages.get(title, {}).get('content_snippet', "")
                # Task Legit
                futures.append(executor.submit(generate_edits_task, title, "LEGITTIMI", half_edits, snippet))
                # Task Vandal
                futures.append(executor.submit(generate_edits_task, title, "VANDALICI", half_edits, snippet))
            
            for future in as_completed(futures):
                future.result() # Attendiamo completamento per loggare errori eventuali
                
        print("\n✨ Generazione Completata!")
        print(f"📂 Legit File: {LEGIT_FILE}")
        print(f"📂 Vandal File: {VANDAL_FILE}")

    except Exception as e:
        print(f"❌ Errore Main: {e}")
    finally:
        driver.close()

if __name__ == "__main__":
    generate_dataset()