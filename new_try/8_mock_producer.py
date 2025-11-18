import json
import time
import uuid
from kafka import KafkaProducer

# CONFIG
KAFKA_BROKER = 'localhost:9092'
TOPIC_OUT = 'wiki-changes' # Lo stesso topic che ascolta il tuo stream processor
PAGE_TITLE = "Australian_Open_2018_-_Doppio_misto"
PAGE_URL = "https://it.wikipedia.org/wiki/Australian_Open_2018_-_Doppio_misto"

def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def send_event(producer, comment, user, is_vandalism):
    """Crea un evento JSON in formato Wikimedia standard"""
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
        "id": 999999, # ID fittizio o reale dal pagemap
        "type": "edit",
        "namespace": 0,
        "title": PAGE_TITLE,
        "title_url": PAGE_URL,
        "comment": comment,  # IL CAMPO CHIAVE
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
    producer = create_producer()
    
    while True:
        print("\n" + "="*40)
        print("SCENARIO TENNIS - REGIA")
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
                send_event(producer, c, f"SportUpdater_{i}", False)
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
                send_event(producer, c, f"Troll_{i}", True)
                time.sleep(0.5)
                
        elif choice == 'q':
            break
            
    producer.close()