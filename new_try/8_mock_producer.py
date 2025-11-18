import json
import time
import uuid
import sys
import csv
from kafka import KafkaProducer

# --- CONFIGURAZIONE ---
KAFKA_BROKER = 'localhost:9092'
TOPIC_OUT = 'wiki-changes' 
PAGE_TITLE = "Australian_Open_2018_-_Doppio_misto"
PAGE_URL = "https://it.wikipedia.org/wiki/Australian_Open_2018_-_Doppio_misto"
PAGEMAP_FILE = "pagemap.csv" 

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

def send_event(producer, comment, user, is_vandalism, page_id):
    """Crea un evento JSON in formato Wikimedia standard usando l'ID reale"""
    current_ts = int(time.time())
    
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
        "parsedcomment": comment
    }
    
    producer.send(TOPIC_OUT, value=event)
    producer.flush()
    print(f"📨 Inviato evento ({'VANDALO' if is_vandalism else 'LEGIT'}): [{user}] -> {comment}")

if __name__ == "__main__":
    REAL_PAGE_ID = get_real_page_id(PAGE_TITLE)
    producer = create_producer()
    
    while True:
        print("\n" + "="*50)
        print(f"SCENARIO TENNIS - REGIA (ID Pagina: {REAL_PAGE_ID})")
        print("="*50)
        print("1. Invia EDIT LEGITTIMI (Report TAS)")
        print("2. Invia EDIT VANDALICI (Attacco massivo)")
        print("3. Invia SCENARIO MISTO (7 edit: 4 Legit, 3 Vandal)") # <--- NUOVA OPZIONE
        print("q. Esci")
        
        choice = input("Scelta: ").strip()
        
        if choice == '1':
            comments = [
                "Aggiornamento verdetto TAS: titolo revocato a Rossi",
                "Inserimento fonte comunicato Losanna",
                "Correzione albo d'oro (Rossi squalificato)",
                "Dabrowski confermata innocente come da sentenza"
            ]
            for i, c in enumerate(comments):
                send_event(producer, c, f"SportUpdater_{i}", False, REAL_PAGE_ID)
                time.sleep(0.5)
                
        elif choice == '2':
            comments = [
                "TUTTI DROGATI VERGOGNA!!",
                "Anche la Dabrowski sapeva tutto, squalificatela!",
                "CANCELLATE QUESTA PAGINA FALSA",
                "Tennis sport di dopati"
            ]
            for i, c in enumerate(comments):
                send_event(producer, c, f"Troll_{i}", True, REAL_PAGE_ID)
                time.sleep(0.5)

        elif choice == '3':
            # Scenario Misto: L'obiettivo è vedere se l'AI filtra quelli buoni da quelli cattivi
            # quando arrivano insieme.
            print("--- Avvio sequenza mista ---")
            mixed_sequence = [
                ("Aggiunta nota ufficiale TAS su Rossi", "Journalist_A", False),      # Legit
                ("Dabrowski complice! Squalifica a vita!", "Hater_01", True),         # Vandal
                ("Fix punteggio set finale", "WikiGnome", False),                     # Legit
                ("QUESTO SPORT FA SCHIFO", "Troll_Z", True),                          # Vandal
                ("Aggiornamento template vincitori", "Editor_Pro", False),            # Legit (Qui dovrebbe scattare il trigger > 4)
                ("Rimosso contenuto offensivo precedente", "Admin_Junior", False),    # Legit
                ("WIKIPEDIA MENTE!!1!", "Hater_02", True)                             # Vandal
            ]
            
            for comment, user, is_vandal in mixed_sequence:
                send_event(producer, comment, user, is_vandal, REAL_PAGE_ID)
                time.sleep(0.8) # Leggero delay per apprezzare il log
                
        elif choice == 'q':
            break
            
    producer.close()