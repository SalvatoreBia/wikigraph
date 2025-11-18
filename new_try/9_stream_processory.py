"""
7_stream_processor.py - Stream Processor per rilevamento hotspot

Questo script consuma eventi di modifica Wikipedia da Kafka e rileva
cluster di modifiche anomale all'interno delle stesse comunità del grafo.

ADATTATO PER:
- Singolo server Neo4j (bolt://localhost:7687)
- Proprietà community (invece di globalCommunityId)
"""

import json
import time
from kafka import KafkaConsumer, KafkaProducer
from neo4j import GraphDatabase
from collections import defaultdict

# --- Configurazione ---
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC_IN = 'wiki-changes'
KAFKA_TOPIC_OUT = 'cluster-alerts'

# Neo4j singolo server
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "password"

# Parametri finestra temporale
WINDOW_DURATION_SECONDS = 150
CLUSTER_THRESHOLD = 5


def create_kafka_producer():
    """Crea Producer Kafka per inviare allarmi."""
    print("Connessione a Kafka come Producer...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✓ Producer Kafka connesso.")
        return producer
    except Exception as e:
        print(f"✗ Errore Producer Kafka: {e}")
        return None


def create_kafka_consumer():
    """Crea Consumer Kafka per ricevere eventi Wikipedia."""
    print("Connessione a Kafka come Consumer...")
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_IN,
            bootstrap_servers=[KAFKA_BROKER],
            group_id='stream-processor',
            auto_offset_reset='latest',  # Solo nuovi messaggi
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        print(f"✓ Consumer Kafka connesso al topic '{KAFKA_TOPIC_IN}'.")
        return consumer
    except Exception as e:
        print(f"✗ Errore Consumer Kafka: {e}")
        return None


def create_neo4j_driver():
    """Crea connessione al singolo server Neo4j."""
    print(f"Connessione a Neo4j su {NEO4J_URI}...")
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        driver.verify_connectivity()
        print("✓ Neo4j connesso.")
        return driver
    except Exception as e:
        print(f"✗ ERRORE: Impossibile connettersi a Neo4j. {e}")
        return None


def get_community_id_tx(tx, page_title):
    """
    Query Cypher per trovare la community di una pagina.
    
    MODIFICATO: Usa 'n.community' invece di 'p.globalCommunityId'
    """
    query = """
    MATCH (n:Node {id: $page_title})
    RETURN n.community AS communityId
    """
    result = tx.run(query, page_title=page_title)
    record = result.single()
    return record["communityId"] if record and record["communityId"] is not None else None


def get_community_id(page_title, session):
    """Ottiene l'ID della comunità per una pagina dal server Neo4j."""
    return session.execute_read(get_community_id_tx, page_title)


def process_messages(consumer, producer, driver):
    """
    Loop principale: legge eventi da Kafka, traccia modifiche per comunità,
    e invia allarmi quando si superano le soglie.
    """
    print("\n" + "="*60)
    print("AVVIO STREAM PROCESSOR")
    print("="*60)
    print(f"Finestra temporale: {WINDOW_DURATION_SECONDS}s")
    print(f"Soglia cluster: {CLUSTER_THRESHOLD} modifiche")
    print("="*60 + "\n")
    
    session = driver.session()
    
    # Stato della finestra temporale
    window_state = defaultdict(list)
    window_start_time = time.time()
    
    # Traccia allarmi già inviati per evitare spam
    alerted_communities = set()
    
    try:
        for message in consumer:
            current_time = time.time()
            
            # ========== CHIUSURA FINESTRA TEMPORALE ==========
            if current_time - window_start_time > WINDOW_DURATION_SECONDS:
                print(f"\n{'─'*60}")
                print(f"FINESTRA CHIUSA ({WINDOW_DURATION_SECONDS}s)")
                print(f"{'─'*60}")
                
                # Analizza tutte le comunità modificate
                for community_id, events_in_window in window_state.items():
                    edit_count_total = len(events_in_window)
                    
                    # Invia allarme se sopra soglia
                    if edit_count_total >= CLUSTER_THRESHOLD:
                        unique_pages = list(set(evt['page_title'] for evt in events_in_window))
                        unique_page_count = len(unique_pages)

                        print(f"\n🚨 CLUSTER RILEVATO")
                        print(f"   Comunità: {community_id}")
                        print(f"   Modifiche totali: {edit_count_total}")
                        print(f"   Pagine uniche: {unique_page_count}")
                        print(f"   Pagine: {', '.join(unique_pages[:5])}...")
                        
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
                
                # Reset finestra
                window_state.clear()
                window_start_time = current_time
                alerted_communities.clear()
                print(f"{'─'*60}")
                print("NUOVA FINESTRA APERTA")
                print(f"{'─'*60}\n")

            # ========== PROCESSAMENTO EVENTO ==========
            event_data = message.value
            page_title = event_data.get('page_title')
            
            if not page_title:
                continue

            # Trova la comunità della pagina
            community_id = get_community_id(page_title, session)
            
            if community_id is not None:
                print(f"📝 '{page_title}' → Comunità {community_id}")
                window_state[community_id].append(event_data)
                
                # ═══════════════════════════════════════════════════════════
                # TESTING: Allarme IMMEDIATO quando supera soglia
                # ═══════════════════════════════════════════════════════════
                # Per PRODUZIONE: Rimuovi questo blocco e lascia solo
                # l'invio allarme nella sezione "FINESTRA CHIUSA" sopra
                # ═══════════════════════════════════════════════════════════
                
                current_count = len(window_state[community_id])
                
                # ═══════════════════════════════════════════════════════════
                # TESTING: Allarme IMMEDIATO quando supera soglia
                # ═══════════════════════════════════════════════════════════
                # Invia allarme la prima volta, poi aggiornamenti ogni 5 eventi
                # ═══════════════════════════════════════════════════════════
                
                should_alert = False
                alert_type = "NUOVO"
                
                if community_id not in alerted_communities:
                    # Primo allarme per questa comunità
                    if current_count >= CLUSTER_THRESHOLD:
                        should_alert = True
                        alert_type = "NUOVO"
                        alerted_communities.add(community_id)
                else:
                    # Comunità già allertata - invia aggiornamenti ogni 5 eventi
                    if current_count >= CLUSTER_THRESHOLD and current_count % 5 == 0:
                        should_alert = True
                        alert_type = "AGGIORNAMENTO"
                
                if should_alert:
                    # Invia allarme SUBITO
                    unique_pages = list(set(evt['page_title'] for evt in window_state[community_id]))
                    
                    print(f"\n{'='*60}")
                    print(f"🚨 ALLARME {alert_type}!")
                    print(f"{'='*60}")
                    print(f"Comunità: {community_id}")
                    print(f"Modifiche: {current_count} (soglia: {CLUSTER_THRESHOLD})")
                    print(f"Pagine uniche: {len(unique_pages)}")
                    print(f"Pagine: {', '.join(unique_pages[:5])}...")
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
                        "threshold": CLUSTER_THRESHOLD,
                        "alertType": alert_type
                    }
                    
                    producer.send(KAFKA_TOPIC_OUT, value=alert_message)
                    producer.flush()
                
                # ═══════════════════════════════════════════════════════════
                # FINE TESTING
                # ═══════════════════════════════════════════════════════════
                
            else:
                print(f"⚠️  '{page_title}' → Comunità N/A (pagina non nel grafo)")

    except KeyboardInterrupt:
        print("\n\n⚠️  Interruzione da utente...")
    finally:
        print("\nChiusura connessioni...")
        consumer.close()
        producer.close()
        session.close()
        driver.close()
        print("✓ Tutte le connessioni chiuse.")


if __name__ == "__main__":
    # Inizializza connessioni
    consumer = create_kafka_consumer()
    producer = create_kafka_producer()
    driver = create_neo4j_driver()
    
    if consumer and producer and driver:
        process_messages(consumer, producer, driver)
    else:
        print("\n✗ Errore critico nell'inizializzazione. Uscita.")
