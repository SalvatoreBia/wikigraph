# src/stream_processor.py

import json
import time
import hashlib
from kafka import KafkaConsumer, KafkaProducer
from neo4j import GraphDatabase
from collections import defaultdict

# --- Configurazione ---
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC_IN = 'wiki-changes'
KAFKA_TOPIC_OUT = 'cluster-alerts'

N_SERVERS = 4
NEO4J_SERVERS = {
    0: "bolt://localhost:7687",
    1: "bolt://localhost:7688",
    2: "bolt://localhost:7689",
    3: "bolt://localhost:7690",
}
NEO4J_USER = "neo4j"
NEO4J_PASS = "password"

# --- Parametri del Rilevatore ---
WINDOW_DURATION_SECONDS = 150  # Durata della finestra (30 secondi)
CLUSTER_THRESHOLD = 5         # Allarme se >= 5 modifiche nella stessa comunità

# --- Funzioni Helper (copiate da altri script) ---

def create_kafka_producer():
    """Crea un producer per inviare gli allarmi."""
    print("Connessione a Kafka come Producer...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Producer Kafka connesso.")
        return producer
    except Exception as e:
        print(f"Errore Producer Kafka: {e}")
        return None

def create_kafka_consumer():
    """Crea un consumer per leggere le modifiche."""
    print("Connessione a Kafka come Consumer...")
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_IN,
            bootstrap_servers=[KAFKA_BROKER],
            group_id='stream-processor',
            auto_offset_reset='latest', # Leggi solo i nuovi messaggi
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        print(f"Consumer Kafka connesso al topic '{KAFKA_TOPIC_IN}'.")
        return consumer
    except Exception as e:
        print(f"Errore Consumer Kafka: {e}")
        return None

def create_neo4j_drivers():
    """Crea e restituisce i driver per tutti i server Neo4j."""
    drivers = {}
    print("Connessione ai 4 server Neo4j...")
    for i in range(N_SERVERS):
        uri = NEO4J_SERVERS[i]
        try:
            driver = GraphDatabase.driver(uri, auth=(NEO4J_USER, NEO4J_PASS))
            driver.verify_connectivity()
            drivers[i] = driver
            print(f"Server {i} ({uri}) connesso.")
        except Exception as e:
            print(f"ERRORE: Impossibile connettersi al Server {i}. {e}")
            return None
    print("Tutti i server Neo4j connessi.")
    return drivers

def get_server_id(page_title):
    """Decide su quale server risiede un nodo, in base al suo titolo."""
    hash_bytes = hashlib.md5(page_title.encode('utf-8')).digest()
    hash_int = int.from_bytes(hash_bytes, 'little')
    return hash_int % N_SERVERS

# --- Logica Core del Processore ---

def get_community_id_tx(tx, page_title):
    """Query Cypher per trovare il communityId."""
    query = """
    MATCH (p:Page {title: $page_title})
    RETURN p.communityId AS communityId
    """
    result = tx.run(query, page_title=page_title)
    record = result.single()
    return record["communityId"] if record and record["communityId"] is not None else None

def get_community_id(page_title, drivers, neo4j_sessions):
    """
    Trova il communityId per una pagina, interrogando lo shard corretto.
    """
    if not page_title:
        return None
        
    try:
        # 1. Scopri quale shard "possiede" questo nodo
        server_id = get_server_id(page_title)
        
        # 2. Prendi la sessione Neo4j per quello shard
        session = neo4j_sessions.get(server_id)
        if not session:
            print(f"Errore: Sessione per server {server_id} non trovata.")
            return None
            
        # 3. Esegui la query per trovare il communityId
        community_id = session.execute_read(get_community_id_tx, page_title)
        return community_id
        
    except Exception as e:
        print(f"Errore durante get_community_id per '{page_title}': {e}")
        return None

def process_messages(consumer, producer, drivers):
    """
    Il loop principale che legge da Kafka, consulta Neo4j e rileva i cluster.
    """
    
    # Crea sessioni persistenti (molto più efficiente)
    neo4j_sessions = {i: driver.session() for i, driver in drivers.items()}
    
    # Questo dizionario è il nostro "stato" in memoria
    # Salva una lista di pagine modificate per ogni communityId
    window_state = defaultdict(list)
    
    window_start_time = time.time()
    print("\n--- Avvio Processore di Stream ---")
    print(f"Finestra: {WINDOW_DURATION_SECONDS} sec, Soglia Cluster: {CLUSTER_THRESHOLD} modifiche")

    try:
        for message in consumer:
            current_time = time.time()
            
            # --- 1. Controlla se la finestra temporale è scaduta ---
            if current_time - window_start_time > WINDOW_DURATION_SECONDS:
                print(f"\n--- FINESTRA CHIUSA ({WINDOW_DURATION_SECONDS}s) ---")
                
                # --- 2. Analizza lo stato della finestra ---
                for community_id, pages_edited in window_state.items():
                    # Usiamo set() per contare le pagine uniche
                    unique_pages = list(set(pages_edited))
                    edit_count = len(unique_pages)
                    
                    if edit_count >= CLUSTER_THRESHOLD:
                        # --- 3. ALLARME! Cluster Rilevato ---
                        print(f"!!! CLUSTER RILEVATO [Comunità: {community_id}] - {edit_count} modifiche uniche.")
                        
                        # Prepara il messaggio di allarme
                        alert_message = {
                            "timestamp": int(current_time),
                            "communityId": community_id,
                            "editCount": edit_count,
                            "pages": unique_pages,
                            "window_start": int(window_start_time),
                            "window_end": int(current_time)
                        }
                        
                        # Invia l'allarme al topic di output
                        producer.send(KAFKA_TOPIC_OUT, value=alert_message)
                        producer.flush() # Forza invio immediato
                        
                # --- 4. Resetta lo stato per la prossima finestra ---
                window_state.clear()
                window_start_time = current_time
                print("--- FINESTRA APERTA ---")

            # --- 5. Processa il messaggio in arrivo ---
            event_data = message.value
            page_title = event_data.get('page_title')
            
            if not page_title:
                continue

            # --- 6. Arricchisci il messaggio: Trova la Comunità ---
            community_id = get_community_id(page_title, drivers, neo4j_sessions)
            
            if community_id is not None:
                # Trovato! Aggiungi allo stato della finestra
                print(f"Modifica: '{page_title}' -> [Comunità: {community_id}]")
                window_state[community_id].append(page_title)
            else:
                # Pagina non trovata o senza comunità (es. appena creata)
                print(f"Modifica: '{page_title}' -> [Comunità: N/A]")

    except KeyboardInterrupt:
        print("Spegnimento processore...")
    finally:
        consumer.close()
        producer.close()
        for session in neo4j_sessions.values():
            session.close()
        for driver in drivers.values():
            driver.close()
        print("Tutte le connessioni chiuse.")

# --- Esecuzione Principale ---
if __name__ == "__main__":
    # Avvia tutti i componenti
    consumer = create_kafka_consumer()
    producer = create_kafka_producer()
    drivers = create_neo4j_drivers()
    
    if consumer and producer and drivers:
        process_messages(consumer, producer, drivers)
    else:
        print("Errore critico nell'inizializzazione. Uscita.")