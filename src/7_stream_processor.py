import json
import time
import hashlib
from kafka import KafkaConsumer, KafkaProducer
from neo4j import GraphDatabase
from collections import defaultdict

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

WINDOW_DURATION_SECONDS = 150
CLUSTER_THRESHOLD = 5


# NOTE: questo producer servirà poi per 
#       collegarci un consumer che si occupa
#       di prendere gli eventi cluster e mandarli
#       all'oracolo, non ha niente a che fare con
#       il consumer che definiamo nella funzione sotto    
def create_kafka_producer():
    print("Connessione a Kafka come Producer...")
    try:
        #
        # creo un producer kafka che pusha
        # gli eventi nel server kafka, così il consumer se li prende
        #
        # value_serializer prende un JSON e lo 
        # trasmette come byte
        #
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
    print("Connessione a Kafka come Consumer...")
    try:
        #
        # creo un consumer kafka che sta in ascolto sul 
        # server kafka per gli aggiornamenti di wiki
        #
        # group_id è importante quando lavoriamo con più partizioni
        #
        # auto_offset_reset='latest' significa che al consumatore
        # interessa ricevere solo gli eventi nuovi, non quelli passati
        # value_deserializer è una funzione che prende i byte che gli 
        #
        # arrivano nello stream e li converte in automatico in JSON (visto
        # che l'API wiki manda JSON)
        #
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

def get_community_id_tx(tx, page_title):
    """Query Cypher per trovare il globalCommunityId."""
    query = """
    MATCH (p:Page {title: $page_title})
    RETURN p.globalCommunityId AS communityId
    """
    result = tx.run(query, page_title=page_title)
    record = result.single()
    return record["communityId"] if record and record["communityId"] is not None else None

def get_community_id(page_title, drivers, neo4j_sessions):
    """Ottiene l'ID della comunità per una pagina dal server Neo4j corretto."""
    server_id = get_server_id(page_title)
    session = neo4j_sessions[server_id]
    return session.execute_read(get_community_id_tx, page_title)

def process_messages(consumer, producer, drivers):
    print("\n--- Avvio Processore di Stream ---")
    print(f"Finestra: {WINDOW_DURATION_SECONDS} sec, Soglia Cluster: {CLUSTER_THRESHOLD} modifiche")
    
    neo4j_sessions = {}
    for i in range(N_SERVERS):
        neo4j_sessions[i] = drivers[i].session()
    
    window_state = defaultdict(list)
    window_start_time = time.time()
    
    # TESTING: Tieni traccia degli allarmi già inviati per questa finestra
    alerted_communities = set()  # TESTING: evita spam di allarmi
    
    try:
        for message in consumer:
            
            current_time = time.time()
            
            if current_time - window_start_time > WINDOW_DURATION_SECONDS:
                print(f"\n--- FINESTRA CHIUSA ({WINDOW_DURATION_SECONDS}s) ---")
                
                #
                # qui siamo nel caso in cui la finestra di tempo
                # è scaduta. Scorriamo tutte le pagine modificate
                #
                for community_id, events_in_window in window_state.items():
                    
                    #
                    # conto TUTTE le modifiche, incluse quelle ripetute
                    # sulla stessa pagina (es. edit war)
                    #
                    edit_count_total = len(events_in_window)
                    
                    #
                    # segnalamo tramite il producer kafka che sono 
                    # avvenute X modifiche (quando superano il threshold)
                    #
                    if edit_count_total >= CLUSTER_THRESHOLD:
                        
                        #
                        # Calcoliamo le pagine uniche solo per il report
                        #
                        unique_pages = list(set(evt['page_title'] for evt in events_in_window))
                        unique_page_count = len(unique_pages)

                        print(f"!!! CLUSTER RILEVATO [Comunità: {community_id}] - {edit_count_total} modifiche totali su {unique_page_count} pagin_e.")
                        
                        alert_message = {
                            "timestamp": int(current_time),
                            "communityId": community_id,
                            "totalEditCount": edit_count_total,
                            "uniquePageCount": unique_page_count,
                            "pages": unique_pages,
                            "events": events_in_window,
                            "window_start": int(window_start_time),
                            "window_end": int(current_time)
                        }
                        
                        producer.send(KAFKA_TOPIC_OUT, value=alert_message)
                        producer.flush()
                        
                window_state.clear()
                window_start_time = current_time
                alerted_communities.clear()  # TESTING: reset allarmi per nuova finestra
                print("--- FINESTRA APERTA ---")

            #
            # accumula gli eventi nel buffer della finestra
            #
            event_data = message.value
            page_title = event_data.get('page_title')
            
            if not page_title:
                continue

            community_id = get_community_id(page_title, drivers, neo4j_sessions)
            
            if community_id is not None:
                print(f"Modifica: '{page_title}' -> [Comunità: {community_id}]")
                window_state[community_id].append(event_data)
                
                # ═══════════════════════════════════════════════════════════
                # TESTING: Allarme IMMEDIATO quando supera soglia
                # ═══════════════════════════════════════════════════════════
                # Per PRODUZIONE: Rimuovi questo blocco e lascia solo
                # l'invio allarme nella sezione "FINESTRA CHIUSA" sopra
                # ═══════════════════════════════════════════════════════════
                
                current_count = len(window_state[community_id])
                
                if current_count >= CLUSTER_THRESHOLD and community_id not in alerted_communities:
                    # Invia allarme SUBITO
                    unique_pages = list(set(evt['page_title'] for evt in window_state[community_id]))
                    
                    print(f"\n{'='*60}")
                    print(f"!!! ALLARME IMMEDIATO !!!")
                    print(f"Comunità {community_id}: {current_count} modifiche (soglia: {CLUSTER_THRESHOLD})")
                    print(f"Pagine coinvolte: {', '.join(unique_pages[:5])}...")
                    print(f"{'='*60}\n")
                    
                    alert_message = {
                        "timestamp": int(current_time),
                        "communityId": community_id,
                        "totalEditCount": current_count,
                        "uniquePageCount": len(unique_pages),
                        "pages": unique_pages,
                        "events": window_state[community_id],
                        "windowStart": int(window_start_time),
                        "windowEnd": int(current_time),
                        "threshold": CLUSTER_THRESHOLD
                    }
                    
                    producer.send(KAFKA_TOPIC_OUT, value=alert_message)
                    producer.flush()
                    
                    alerted_communities.add(community_id)  # Evita spam
                
                # ═══════════════════════════════════════════════════════════
                # FINE TESTING
                # ═══════════════════════════════════════════════════════════
                
            else:
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

if __name__ == "__main__":
    consumer = create_kafka_consumer()
    producer = create_kafka_producer()
    drivers = create_neo4j_drivers()
    
    if consumer and producer and drivers:
        process_messages(consumer, producer, drivers)
    else:
        print("Errore critico nell'inizializzazione. Uscita.")