# src/producer.py

import json
import time
import requests
from kafka import KafkaProducer
from sseclient import SSEClient

# --- Configurazione ---
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'wiki-changes'
WIKIMEDIA_STREAM_URL = 'https://stream.wikimedia.org/v2/stream/recentchange'

def create_producer():
    """Crea e restituisce un Kafka Producer connesso al nostro broker."""
    print(f"Connessione a Kafka Broker su {KAFKA_BROKER}...")
    try:
        # value_serializer serializza i dati in JSON prima di inviarli
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Connesso a Kafka!")
        return producer
    except Exception as e:
        print(f"Errore durante la connessione a Kafka: {e}")
        return None

def stream_wikimedia_changes(producer, topic):
    """Si connette allo stream Wikimedia e invia gli eventi a Kafka."""
    
    print(f"Connessione allo stream Wikimedia: {WIKIMEDIA_STREAM_URL}")
    
    response = None
    event_count = 0
    try:
        # Crea la connessione HTTP allo stream SSE
        # Wikimedia richiede un User-Agent valido
        headers = {
            'Accept': 'text/event-stream',
            'User-Agent': 'WikiStreamBot/1.0 (Educational Project; Python/requests)'
        }
        response = requests.get(WIKIMEDIA_STREAM_URL, stream=True, timeout=None, headers=headers)
        print(f"Stream HTTP connesso (status: {response.status_code}), in attesa di eventi...")
        
        client = SSEClient(response)
        
        # Inizia ad ascoltare gli eventi in un loop infinito
        for event in client.events():
            event_count += 1
            
            # Debug ogni 100 eventi
            if event_count % 100 == 0:
                print(f"[INFO] Eventi ricevuti: {event_count}")
            
            if event.event == 'message':
                # L'evento è un JSON, ma è in formato stringa
                # Dobbiamo caricarlo come dizionario Python
                try:
                    event_data = json.loads(event.data)
                except json.JSONDecodeError:
                    # Ignora messaggi malformati
                    continue
                
                # --- Filtro Importante ---
                # Vogliamo solo le modifiche alla wiki italiana
                # e solo quelle del namespace 0 (le pagine vere)
                if event_data.get('wiki') == 'itwiki' and event_data.get('namespace', -1) == 0:
                    
                    # Estraiamo solo i dati che ci servono
                    # Questo rende i nostri messaggi Kafka più puliti
                    clean_message = {
                        'id': event_data.get('id'),
                        'timestamp': event_data.get('timestamp'),
                        'title': event_data['meta']['domain'], # es. it.wikipedia.org
                        'page_title': event_data.get('title'),
                        'user': event_data.get('performer', {}).get('user_text'),
                        'type': event_data.get('type'), # es. 'edit', 'new'
                        'length_new': event_data.get('length', {}).get('new'),
                        'length_old': event_data.get('length', {}).get('old'),
                        'revision_new': event_data.get('revision', {}).get('new'),
                        'revision_old': event_data.get('revision', {}).get('old')
                    }
                    
                    # Invia il messaggio pulito a Kafka
                    # Usiamo 'page_title' come chiave per il partizionamento
                    key = clean_message['page_title'].encode('utf-8')
                    producer.send(topic, value=clean_message, key=key)
                    
                    # Log in console per vedere che sta funzionando
                    print(f"✓ Inviato: [{clean_message['type']}] {clean_message['page_title']} by {clean_message['user']}")

            # Gestione del "keep-alive" - non fare nulla, è normale
            # elif event.event == 'error' and "Keep-Alive" in event.data:
            #     print("Ricevuto 'Keep-Alive', stream attivo...")
            
    except KeyboardInterrupt:
        print("\nInterrotto dall'utente.")
    except Exception as e:
        print(f"Errore nello stream: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print(f"Chiusura connessione allo stream. Eventi totali: {event_count}")
        if response:
            response.close()

if __name__ == "__main__":
    producer = create_producer()
    if producer:
        while True:
            # Loop infinito per riconnettersi in caso di caduta
            stream_wikimedia_changes(producer, KAFKA_TOPIC)
            print("Stream interrotto. Riconnessione tra 5 secondi...")
            time.sleep(5)