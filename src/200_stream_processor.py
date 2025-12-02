import json
import time
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer
from neo4j import GraphDatabase

# --- CONFIG ---
KAFKA_BROKER = 'localhost:9092'
TOPIC_IN = 'wiki-changes'
TOPIC_OUT_JUDGE = 'to-be-judged'  # Nuovo topic per l'AI
NEO4J_URI = 'bolt://localhost:7687'
NEO4J_AUTH = ('neo4j', 'password')

ALERT_THRESHOLD = 1 
WINDOW_SECONDS = 30  # Finestra temporale per il reset

def get_community_id(tx, page_title):
    # Logica semplificata per l'esempio, cerca di recuperare la community reale
    query = "MATCH (n:Node {id: $title}) RETURN n.community as community"
    result = tx.run(query, title=page_title).single()
    if result and result['community'] is not None:
        return result['community']
    if "Australian_Open" in page_title: return 37 
    return -1 

def main():
    print("--- STREAM PROCESSOR (Filter & Forwarder) ---")
    
    # Consumer: Legge gli eventi grezzi
    consumer = KafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='analytics_group'
    )

    # Producer: Inoltra all'AI solo se necessario
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
    
    # Strutture dati:
    # window_buffer: { comm_id: [lista_messaggi_completi] }
    window_buffer = defaultdict(list)
    # last_check: timestamp dell'ultimo reset
    last_check = time.time()

    print(f"🎧 In ascolto su '{TOPIC_IN}'... Soglia: {ALERT_THRESHOLD} edit in {WINDOW_SECONDS}s")

    for message in consumer:
        event = message.value
        page_title = event['title']
        
        # Gestione Reset Finestra Temporale
        if time.time() - last_check > WINDOW_SECONDS:
            print("\n--- ⏱️ Reset Finestra Temporale (Buffer Svuotato) ---")
            window_buffer.clear()
            last_check = time.time()

        # 1. Trova la community
        with driver.session() as session:
            comm_id = session.execute_read(get_community_id, page_title)
        
        # 2. Aggiungi evento al buffer della community
        window_buffer[comm_id].append(event)
        current_count = len(window_buffer[comm_id])
        
        print(f"📝 Edit ricevuto (Comm {comm_id}). Conteggio attuale: {current_count}/{ALERT_THRESHOLD}")

        # 3. Logica di Trigger
        if current_count == ALERT_THRESHOLD:
            print(f"🚨 SOGLIA RAGGIUNTA per Community {comm_id}! Inoltro storico buffer all'AI...")
            # Inoltra TUTTI i messaggi accumulati finora (i primi 3 + il 4°)
            for old_event in window_buffer[comm_id]:
                producer.send(TOPIC_OUT_JUDGE, value=old_event)
            producer.flush()
            
        elif current_count > ALERT_THRESHOLD:
            print(f"🚨 CLUSTER ANCORA ATTIVO (Comm {comm_id}). Inoltro immediato edit #{current_count} all'AI.")
            # Inoltra direttamente il nuovo messaggio corrente
            producer.send(TOPIC_OUT_JUDGE, value=event)
            producer.flush()
        
        # Se count < ALERT_THRESHOLD, non facciamo nulla (resta nel buffer)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop.")