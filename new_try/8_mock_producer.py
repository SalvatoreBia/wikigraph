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
PAGEMAP_FILE = "pagemap.csv"  # Assicurati che questo file sia nella stessa cartella o metti il path corretto

def get_real_page_id(target_title):
    """
    Cerca nel pagemap.csv l'ID corrispondente al titolo target.
    Questo garantisce che l'ID inviato a Kafka esista nel Grafo Neo4j.
    """
    print(f"🔍 Ricerca ID reale per la pagina: '{target_title}'...")
    
    try:
        with open(PAGEMAP_FILE, 'r', encoding='utf-8', errors='replace') as f:
            # Leggiamo riga per riga per evitare di caricare tutto in RAM se enorme
            for line in f:
                # Il formato atteso da script 2 è: id,title
                parts = line.strip().split(',', 1)
                
                if len(parts) < 2:
                    continue
                
                curr_id = parts[0].strip()
                # Rimuoviamo eventuali apici rimasti dalla regex e spazi
                curr_title = parts[1].replace("'", "").strip()
                
                if curr_title == target_title:
                    print(f"✅ Trovato! La pagina corrisponde all'ID nel grafo: {curr_id}")
                    return int(curr_id)
                    
        print(f"❌ ERRORE CRITICO: Titolo '{target_title}' non trovato in {PAGEMAP_FILE}.")
        print("   Assicurati che il titolo sia esatto (case sensitive, underscore al posto degli spazi).")
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
        "id": page_id, # <--- QUI USIAMO L'ID REALE DEL GRAFO
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
    print(f"📨 Inviato evento: [{user}] -> {comment}")

if __name__ == "__main__":
    # 1. Fase di Setup: Recuperiamo l'ID vero
    REAL_PAGE_ID = get_real_page_id(PAGE_TITLE)
    
    # 2. Avvio Producer
    producer = create_producer()
    
    while True:
        print("\n" + "="*40)
        print(f"SCENARIO TENNIS - REGIA (ID Pagina: {REAL_PAGE_ID})")
        print("="*40)
        print("1. Invia EDIT LEGITTIMO (Report TAS)")
        print("2. Invia EDIT VANDALICO (Accusa Dabrowski)")
        print("q. Esci")
        
        choice = input("Scelta: ").strip()
        
        if choice == '1':
            # Simuliamo 3-4 edit legittimi veloci per fare 'massa'
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
            # Simuliamo attacco vandalico
            comments = [
                "TUTTI DROGATI VERGOGNA!!",
                "Anche la Dabrowski sapeva tutto, squalificatela!",
                "CANCELLATE QUESTA PAGINA FALSA",
                "Tennis sport di dopati"
            ]
            for i, c in enumerate(comments):
                send_event(producer, c, f"Troll_{i}", True, REAL_PAGE_ID)
                time.sleep(0.5)
                
        elif choice == 'q':
            break
            
    producer.close()