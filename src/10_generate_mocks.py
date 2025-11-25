import os
import json
import time
import random
import re
import csv
import sys
import uuid
from pathlib import Path
from itertools import cycle
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

load_dotenv(dotenv_path=ENV_PATH)

# API Keys Round Robin
API_KEYS = [
    os.getenv("GEMINI_API_KEY"),
    os.getenv("GEMINI_API_KEY_2"),
    os.getenv("GEMINI_API_KEY_3")
]
# Filter out None values
API_KEYS = [k for k in API_KEYS if k]
if not API_KEYS:
    print("❌ ERRORE: Nessuna API Key trovata nel .env")
    sys.exit(1)

api_key_cycle = cycle(API_KEYS)

def get_next_api_key():
    key = next(api_key_cycle)
    # print(f"🔑 Uso API Key: ...{key[-4:]}")
    return key

# Neo4j Config
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "password")
MODEL_NAME = "gemini-2.5-pro"

# --- FUNZIONI DA SCRIPT 7 (COMMUNITY DETECTION) ---

def get_top_communities(driver):
    """
    Ottiene le 10 community più grandi e i 5 nodi 'Hub' (più connessi) per ognuna.
    """
    query = """
    MATCH (n:Node)
    WHERE n.community IS NOT NULL
    WITH n.community AS comm_id, n
    // Conta i collegamenti per trovare i nodi centrali
    WITH comm_id, n, COUNT { (n)--() } AS degree
    ORDER BY degree DESC
    // Raggruppa per community prendendo i top 5 nodi
    WITH comm_id, collect(n.id)[0..5] AS top_nodes, count(*) as size
    ORDER BY size DESC
    LIMIT 10
    RETURN comm_id, size, top_nodes
    """
    
    print("--- Interrogazione Neo4j per trovare i Leader delle Community ---")
    with driver.session() as session:
        result = session.run(query)
        data = [record.data() for record in result]
        return data

def resolve_names(communities_data, map_file):
    """
    Scansiona il file pagemap.csv per trovare i nomi corrispondenti agli ID target.
    """
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
                    title = parts[1].replace("'", "").strip()
                    id_to_name[curr_id] = title
                    
                    if len(id_to_name) == len(target_ids):
                        break
                        
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
    return communities_data

# --- GENERAZIONE CONTENUTI ---

def generate_html_page(title):
    """Genera una pagina HTML finta ma realistica usando Gemini."""
    genai.configure(api_key=get_next_api_key())
    model = genai.GenerativeModel(MODEL_NAME)
    
    print(f"📄 Generazione pagina HTML per: {title}...")
    prompt = f"""
    Sei un giornalista esperto. Scrivi un articolo dettagliato e affidabile (almeno 800 parole) su: "{title}".
    Il contenuto deve sembrare una vera pagina web di notizie o enciclopedia.
    
    REGOLE:
    1. Usa HTML puro (<body>, <h1>, <h2>, <p>, <ul>, <strong>, ecc.). Niente CSS o JS esterni.
    2. Includi una sezione "Fonti" o "Riferimenti" fittizia alla fine.
    3. Stile sobrio, fattuale, enciclopedico.
    4. Non usare Markdown (```html ... ```). Restituisci solo il codice HTML.
    """
    
    try:
        resp = model.generate_content(prompt)
        html_content = resp.text.replace("```html", "").replace("```", "").strip()
        return html_content
    except Exception as e:
        print(f"⚠️ Errore generazione HTML per {title}: {e}")
        return f"<h1>Errore generazione</h1><p>{title}</p>"

def generate_edits_batch(topic_title, edit_type, count, context_html_snippet):
    """Genera un batch di edit (legit o vandal) usando Gemini."""
    genai.configure(api_key=get_next_api_key())
    model = genai.GenerativeModel(MODEL_NAME)
    
    prompt = f"""
    Contesto: Un articolo affidabile su "{topic_title}".
    Genera un JSON Array con ESATTAMENTE {count} modifiche simulate (edits) stile Wikipedia.
    
    TIPO MODIFICHE: {edit_type}
    
    FORMATO OGGETTO JSON (simile a stream Wikipedia):
    {{
        "user": "NomeUtente",
        "comment": "Descrizione della modifica",
        "timestamp": 1234567890, (usa timestamp recenti variabili)
        "length": {{ "old": 10000, "new": 10050 }},
        "is_vandalism": { "true" if edit_type == "VANDALICI" else "false" }
    }}
    
    DESCRIZIONE TIPO:
    - Se LEGITTIMI: Correzioni typo, aggiunta fonti, riformulazione, aggiornamento dati. Commenti seri.
    - Se VANDALICI: Insulti, cancellazione testo, ALL CAPS, propaganda, spam, nonsense. Commenti provocatori o ingannevoli.
    
    Output solo il JSON Array.
    """
    
    try:
        resp = model.generate_content(prompt, generation_config={"response_mime_type": "application/json"})
        return json.loads(resp.text)
    except Exception as e:
        print(f"⚠️ Errore generazione batch {edit_type}: {e}")
        return []

def generate_dataset():
    # 1. Trova Community e Seleziona Topic
    driver = GraphDatabase.driver(URI, auth=AUTH)
    try:
        comm_data = get_top_communities(driver)
        if not comm_data:
            print("❌ Nessuna community trovata.")
            return
        
        mapping = resolve_names(comm_data, PAGEMAP_FILE)
        print_report(comm_data, mapping)
        
        # Scelta Random Community
        selected_comm = random.choice(comm_data)
        comm_id = selected_comm['comm_id']
        top_nodes_ids = selected_comm['top_nodes']
        
        # Risolvi nomi per la community selezionata
        top_nodes_titles = [mapping.get(str(nid), f"Unknown_{nid}") for nid in top_nodes_ids]
        
        print(f"\n🎯 COMMUNITY SELEZIONATA: {comm_id}")
        print(f"   Argomenti (Hubs): {top_nodes_titles}")
        
        # Usa il primo hub come "Main Topic" per il nome del file, ma genera HTML per tutti (o alcuni)
        main_topic = top_nodes_titles[0]
        clean_main_topic = re.sub(r'[^\w]', '_', main_topic)
        
        # 2. Genera HTML Pages (Trusted Source)
        # Generiamo HTML per i primi 3 hub per avere contesto
        generated_pages = []
        HTML_DIR.mkdir(parents=True, exist_ok=True)
        
        for title in top_nodes_titles[:3]: # Limitiamo a 3 per tempo/quota
            html_content = generate_html_page(title)
            clean_title = re.sub(r'[^\w]', '_', title)
            filename = f"trusted_{clean_title}.html"
            path = HTML_DIR / filename
            
            with open(path, "w", encoding="utf-8") as f:
                f.write(html_content)
            
            generated_pages.append({"title": title, "path": str(path), "content_snippet": html_content[:500]})
            print(f"✅ Salvato HTML: {path}")
            time.sleep(2) # Breve pausa
            
        # 3. Genera 200 Edits (100 Legit, 100 Vandal)
        print("\n✍️  Generazione 200 Edit (100 Legit, 100 Vandal)...")
        all_edits = []
        
        # Dividiamo in batch per evitare timeout o limiti output
        # 100 Legit -> 2 batch da 50
        # 100 Vandal -> 2 batch da 50
        
        # Usiamo il main topic come contesto principale
        context_snippet = generated_pages[0]['content_snippet']
        
        # Batch Legit
        for i in range(2):
            print(f"   -> Batch Legit {i+1}/2...")
            batch = generate_edits_batch(main_topic, "LEGITTIMI", 50, context_snippet)
            all_edits.extend(batch)
            time.sleep(2)
            
        # Batch Vandal
        for i in range(2):
            print(f"   -> Batch Vandal {i+1}/2...")
            batch = generate_edits_batch(main_topic, "VANDALICI", 50, context_snippet)
            all_edits.extend(batch)
            time.sleep(2)
            
        # Shuffle
        random.shuffle(all_edits)
        
        # Aggiungi ID univoci e metadati extra se mancano
        final_edits = []
        for edit in all_edits:
            # Assicuriamoci che abbia i campi essenziali
            if "user" not in edit: edit["user"] = "Anonymous"
            if "comment" not in edit: edit["comment"] = "Edit"
            
            # Arricchisci come script 18
            enriched_edit = {
                "id": str(uuid.uuid4()),
                "type": "edit",
                "title": main_topic,
                "user": edit.get("user"),
                "comment": edit.get("comment"),
                "timestamp": edit.get("timestamp", int(time.time())),
                "length": edit.get("length", {"old": 1000, "new": 1000}),
                "is_vandalism": edit.get("is_vandalism", False),
                "meta": {
                    "domain": "it.wikipedia.org",
                    "uri": f"https://it.wikipedia.org/wiki/{clean_main_topic}"
                }
            }
            final_edits.append(enriched_edit)
            
        # Salvataggio
        MOCK_DIR.mkdir(parents=True, exist_ok=True)
        json_filename = f"dataset_{clean_main_topic}.json"
        
        output_data = {
            "community_id": comm_id,
            "topics": top_nodes_titles,
            "generated_pages": [p['path'] for p in generated_pages],
            "edits": final_edits
        }
        
        with open(MOCK_DIR / json_filename, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=4, ensure_ascii=False)
            
        print(f"\n✅ Dataset Generato!\nJSON: {MOCK_DIR / json_filename}")
        print(f"Totale Edits: {len(final_edits)}")

    except Exception as e:
        print(f"❌ Errore durante l'esecuzione: {e}")
    finally:
        driver.close()

if __name__ == "__main__":
    generate_dataset()