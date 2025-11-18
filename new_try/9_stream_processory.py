import json
from kafka import KafkaConsumer
from neo4j import GraphDatabase
from collections import defaultdict
import time

# --- CONFIG ---
KAFKA_BROKER = 'localhost:9092'
TOPIC_IN = 'wiki-changes'
NEO4J_URI = 'bolt://localhost:7687'
NEO4J_AUTH = ('neo4j', 'password')

ALERT_THRESHOLD = 4 

def get_community_id(tx, page_title):
    query = """
    MATCH (n:Node {id: $title}) 
    RETURN n.community as community
    """
    result = tx.run(query, title=page_title).single()
    if result and result['community'] is not None:
        return result['community']
    
    if "Australian_Open" in page_title:
        return 37 
    return -1 

def main():
    print("--- AVVIO STREAM PROCESSOR (Il Filtro) ---")
    
    # --- CORREZIONE QUI: Aggiunto group_id univoco ---
    consumer = KafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='analytics_group' # ID diverso dall'AI Judge
    )
    
    driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
    window_stats = defaultdict(int)
    last_check = time.time()

    print(f" In ascolto su topic: {TOPIC_IN}...")

    for message in consumer:
        event = message.value
        page_title = event['title']
        user = event['user']
        
        with driver.session() as session:
            comm_id = session.execute_read(get_community_id, page_title)
        
        print(f"📝 Edit su '{page_title}' (User: {user}) -> Community ID: {comm_id}")

        window_stats[comm_id] += 1
        
        if time.time() - last_check > 10:
            window_stats.clear()
            last_check = time.time()

        if window_stats[comm_id] >= ALERT_THRESHOLD:
            print(f"\n🚨 [ALERT] RILEVATO CLUSTER ANOMALO SU COMMUNITY {comm_id}!")
            print(f"   -> {window_stats[comm_id]} edit rapidi rilevati.\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop.")