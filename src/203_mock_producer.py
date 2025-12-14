import json
import time
import uuid
import sys
import csv
from pathlib import Path
from kafka import KafkaProducer

# --- CONFIGURAZIONE ---
from config_loader import load_config

CONFIG = load_config()

# --- CONFIGURAZIONE ---
KAFKA_BROKER = CONFIG['kafka']['broker']
TOPIC_OUT = CONFIG['kafka']['topic_changes'] 
PAGE_TITLE = "Australian_Open_2018_-_Doppio_misto"
PAGE_URL = "https://it.wikipedia.org/wiki/Australian_Open_2018_-_Doppio_misto"
PAGEMAP_FILE = "../data/pagemap.csv" 

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
MOCK_DIR = DATA_DIR / "mocked_edits"
LEGIT_FILE = MOCK_DIR / "legit_edits.json"
VANDAL_FILE = MOCK_DIR / "vandal_edits.json"

EVAL_SET_SIZE = 70

def get_real_page_id(target_title):
    """
    Cerca nel pagemap.csv l'ID corrispondente al titolo target.
    Questo garantisce che l'ID inviato a Kafka esista nel Grafo Neo4j.
    """
    print(f"🔍 Ricerca ID reale per la pagina: '{target_title}'...")
    
    try:
        with open(PAGEMAP_FILE, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                parts = line.strip().split(',', 1)
                if len(parts) < 2:
                    continue
                
                curr_id = parts[0].strip()
                curr_title = parts[1].replace("'", "").strip()
                
                if curr_title == target_title:
                    print(f"✅ Trovato! La pagina corrisponde all'ID nel grafo: {curr_id}")
                    return int(curr_id)
                    
        print(f"❌ ERRORE CRITICO: Titolo '{target_title}' non trovato in {PAGEMAP_FILE}.")
        sys.exit(1)
        
    except FileNotFoundError:
        print(f"❌ ERRORE: File {PAGEMAP_FILE} non trovato.")
        print("   Esegui prima '2_parse_file.sh' per generare la mappa.")
        sys.exit(1)

def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def load_eval_edits(filepath, limit=50):
    if not filepath.exists():
        print(f"⚠️ File non trovato: {filepath}")
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
        return data[:limit]

def send_event(producer, edit_data, page_id):
    """Crea un evento JSON in formato Wikimedia standard usando l'ID reale"""
    current_ts = int(time.time())
    
    comment = edit_data['comment']
    user = edit_data['user']
    is_vandalism = edit_data['is_vandalism']

    event = {
        "$schema": "/mediawiki/recentchange/1.0.0",
        "meta": {
            "uri": PAGE_URL,
            "request_id": str(uuid.uuid4()),
            "id": str(uuid.uuid4()),
            "dt": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(current_ts)),
            "domain": "it.wikipedia.org",
            "stream": "mediawiki.recentchange",
            "topic": "codfw.mediawiki.recentchange",
            "partition": 0,
            "offset": 12345
        },
        "id": page_id,
        "type": "edit",
        "namespace": 0,
        "title": PAGE_TITLE,
        "title_url": PAGE_URL,
        "comment": comment, 
        "timestamp": current_ts,
        "user": user,
        "bot": False,
        "minor": False,
        "length": {
            "old": 15000,
            "new": 15000 + (-500 if is_vandalism else 50)
        },
        "wiki": "itwiki",
        "server_name": "it.wikipedia.org",
        "parsedcomment": comment,
        "original_text": edit_data.get('original_text', ''),
        "new_text": edit_data.get('new_text', ''),
        "is_vandalism": is_vandalism # Aggiunto per verifica successiva (non standard ma utile per noi)
    }
    
    producer.send(TOPIC_OUT, value=event)
    producer.flush()
    print(f"📨 Inviato evento ({'VANDALO' if is_vandalism else 'LEGIT'}): [{user}] -> {comment}")

if __name__ == "__main__":
    REAL_PAGE_ID = get_real_page_id(PAGE_TITLE)
    producer = create_producer()
    
    print("\n" + "="*50)
    print(f"AUTOMATED STREAM PRODUCER (ID Pagina: {REAL_PAGE_ID})")
    print("="*50)
    
    # Carica i primi 50 edit per tipo
    legit_edits = load_eval_edits(LEGIT_FILE, EVAL_SET_SIZE)
    vandal_edits = load_eval_edits(VANDAL_FILE, EVAL_SET_SIZE)
    
    all_edits = legit_edits + vandal_edits
    print(f"📦 Caricati {len(legit_edits)} Legit e {len(vandal_edits)} Vandal edits per lo stream.")
    
    print("🚀 Avvio stream tra 3 secondi...")
    time.sleep(3)
    
    for i, edit in enumerate(all_edits):
        send_event(producer, edit, REAL_PAGE_ID)
        time.sleep(0.2) # Piccolo delay per simulare stream
        
    print("✅ Stream completato.")
    producer.close()