import json
import time
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer
from neo4j import GraphDatabase

from config_loader import load_config

CONFIG = load_config()

# --- CONFIG ---
KAFKA_BROKER = CONFIG['kafka']['broker']
TOPIC_IN = CONFIG['kafka']['topic_changes']
TOPIC_OUT_JUDGE = CONFIG['kafka']['topic_judge']
NEO4J_URI = CONFIG['neo4j']['uri']
NEO4J_AUTH = tuple(CONFIG['neo4j']['auth'])

ALERT_THRESHOLD = 1 
WINDOW_SECONDS = 30  

def get_community_id(tx, page_title):
    query = "MATCH (n:Node {id: $title}) RETURN n.community as community"
    result = tx.run(query, title=page_title).single()
    if result and result['community'] is not None:
        return result['community']
    return -1 

def main():
    print("--- STREAM PROCESSOR (Filter & Forwarder) ---")
    
    consumer = KafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='analytics_group'
    )

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
    
    window_buffer = defaultdict(list)
    last_check = time.time()

    print(f"🎧 In ascolto su '{TOPIC_IN}'... Soglia: {ALERT_THRESHOLD} edit in {WINDOW_SECONDS}s")

    for message in consumer:
        event = message.value
        page_title = event['title']
        
        if time.time() - last_check > WINDOW_SECONDS:
            print("\n--- ⏱️ Reset Finestra Temporale (Buffer Svuotato) ---")
            window_buffer.clear()
            last_check = time.time()

        with driver.session() as session:
            comm_id = session.execute_read(get_community_id, page_title)
        
        window_buffer[comm_id].append(event)
        current_count = len(window_buffer[comm_id])
        
        print(f"📝 Edit ricevuto (Comm {comm_id}). Conteggio attuale: {current_count}/{ALERT_THRESHOLD}")

        if current_count >= ALERT_THRESHOLD:
            if current_count == ALERT_THRESHOLD:
                print(f"🚨 SOGLIA RAGGIUNTA per Community {comm_id}! Inoltro storico buffer all'AI...")
                # Invia tutti gli eventi accumulati
                for old_event in window_buffer[comm_id]:
                    producer.send(TOPIC_OUT_JUDGE, value=old_event)
            else:
                print(f"🚨 CLUSTER ANCORA ATTIVO (Comm {comm_id}). Inoltro immediato edit #{current_count} all'AI.")
                # Invia solo l'ultimo evento (gli altri sono già stati inviati)
                producer.send(TOPIC_OUT_JUDGE, value=event)
            producer.flush()
        

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop.")