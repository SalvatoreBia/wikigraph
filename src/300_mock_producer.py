import csv
import json
import sys
import time
import uuid
from pathlib import Path

from kafka import KafkaProducer

from config_loader import load_config

CONFIG = load_config()

KAFKA_BROKER = CONFIG['kafka']['broker']
TOPIC_OUT = CONFIG['kafka']['topic_changes'] 

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
PAGEMAP_FILE = DATA_DIR / "pagemap.csv"
MOCK_DIR = DATA_DIR / "mocked_edits"
LEGIT_FILE = MOCK_DIR / "legit_edits_test.json"
VANDAL_FILE = MOCK_DIR / "vandal_edits_test.json"



def load_pagemap():
    print(f"🔍 Caricamento mappa ID-Titolo da {PAGEMAP_FILE}...")
    page_map = {}
    
    try:
        with open(PAGEMAP_FILE, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                parts = line.strip().split(',', 1)
                if len(parts) < 2:
                    continue
                
                curr_id = parts[0].strip()
                curr_title = parts[1].replace("'", "").strip()
                page_map[curr_title] = int(curr_id)
                
        print(f"✅ Mappa caricata: {len(page_map)} pagine trovate.")
        return page_map
        
    except FileNotFoundError:
        print(f"❌ ERRORE: File {PAGEMAP_FILE} non trovato.")
        print("   Esegui prima '2_parse_file.sh' per generare la mappa.")
        sys.exit(1)

def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def load_eval_edits(filepath):
    if not filepath.exists():
        print(f"⚠️ File non trovato: {filepath}")
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
        return data

def send_event(producer, edit_data, page_map):
    """Crea un evento JSON in formato Wikimedia standard cercando l'ID reale"""
    current_ts = int(time.time())
    
    title = edit_data['title']
    
    page_id = None
    if title in page_map:
        page_id = page_map[title]
    elif title.replace(" ", "_") in page_map:
        page_id = page_map[title.replace(" ", "_")]

    if page_id is None:
        print(f"⚠️ Skipping edit for '{title}': ID non trovato nel pagemap (preso come '{title.replace(' ', '_')}').")
        return
    page_url = f"https://it.wikipedia.org/wiki/{title}"

    comment = edit_data['comment']
    user = edit_data['user']
    is_vandalism = edit_data['is_vandalism']

    event = {
        "$schema": "/mediawiki/recentchange/1.0.0",
        "meta": {
            "uri": page_url,
            "request_id": str(uuid.uuid4()),
            "id": str(uuid.uuid4()),
            "dt": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(current_ts)),
            "domain": "it.wikipedia.org",
            "stream": "mediawiki.recentchange",
            "topic": "codfw.mediawiki.recentchange",
            "partition": 0,
            "offset": 12345
        },
        "id": str(uuid.uuid4()),  # UUID unico per ogni evento (non page_id!)
        "page_id": page_id,       # ID pagina Wikipedia in campo separato
        "type": "edit",
        "namespace": 0,
        "title": title,
        "title_url": page_url,
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
        "is_vandalism": is_vandalism 
    }
    
    producer.send(TOPIC_OUT, value=event)
    producer.flush()
    print(f"📨 Inviato evento per '{title}' ({'VANDALO' if is_vandalism else 'LEGIT'}): [{user}] -> {comment}")

if __name__ == "__main__":
    PAGE_MAP = load_pagemap()
    producer = create_producer()
    
    print("\n" + "="*50)
    print(f"AUTOMATED STREAM PRODUCER (Multi-Topic)")
    print("="*50)
    
    legit_edits = load_eval_edits(LEGIT_FILE)
    vandal_edits = load_eval_edits(VANDAL_FILE)

    target_legit = CONFIG['dataset']['testing']['legit_count']
    target_vandal = CONFIG['dataset']['testing']['vandal_count']

    if len(legit_edits) > target_legit:
        print(f"✂️  Limito Legit Stream a {target_legit}")
        legit_edits = legit_edits[:target_legit]
        
    if len(vandal_edits) > target_vandal:
        print(f"✂️  Limito Vandal Stream a {target_vandal}")
        vandal_edits = vandal_edits[:target_vandal]
    
    all_edits = legit_edits + vandal_edits
    print(f"📦 Caricati {len(legit_edits)} Legit e {len(vandal_edits)} Vandal edits per lo stream.")
    
    print("🚀 Avvio stream tra 3 secondi...")
    time.sleep(3)
    
    for i, edit in enumerate(all_edits):
        send_event(producer, edit, PAGE_MAP)
        time.sleep(0.2) # Piccolo delay per simulare stream
        
    print("✅ Stream completato.")
    producer.close()