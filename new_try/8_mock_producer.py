"""
8_mock_producer.py - Producer Mock per Testing

Simula eventi di modifica Wikipedia per testare il sistema di 
rilevamento hotspot senza dipendere dallo stream reale.

ADATTATO PER:
- Singolo server Neo4j (bolt://localhost:7687)
- Proprietà community (invece di globalCommunityId)
- Usa Node con id invece di Page con title

Scenari disponibili:
1. Edit War - molte modifiche concentrate su poche pagine
2. Attività normale - modifiche distribuite
3. Vandalismo - modifiche rapide da pochi utenti
"""

import json
import time
import random
from kafka import KafkaProducer
from neo4j import GraphDatabase

# --- Configurazione ---
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'wiki-changes'

# Neo4j singolo server
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "password"


def create_producer():
    """Crea e restituisce un Kafka Producer."""
    print(f"Connessione a Kafka su {KAFKA_BROKER}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✓ Producer Kafka connesso!")
        return producer
    except Exception as e:
        print(f"✗ Errore connessione Kafka: {e}")
        return None


def get_large_community_pages(neo4j_driver, limit=20, num_communities=3):
    """
    Interroga Neo4j per trovare pagine da diverse comunità grandi.
    
    MODIFICATO: Restituisce pagine da più comunità per testare 
    il rilevamento di hotspot su comunità diverse.
    
    Args:
        neo4j_driver: Driver Neo4j
        limit: Numero di pagine per comunità
        num_communities: Numero di comunità da cui prendere pagine
    
    Returns:
        Lista di (page_id, community_id) tuple
    """
    print("\nInterrogazione Neo4j per trovare pagine da testare...")
    
    # Query per trovare le comunità più grandi
    query_communities = """
    MATCH (n:Node)
    WHERE n.community IS NOT NULL
    WITH n.community AS community, COUNT(n) AS size
    ORDER BY size DESC
    LIMIT $num_communities
    RETURN community, size
    """
    
    with neo4j_driver.session() as session:
        try:
            # Trova le comunità più grandi
            result = session.run(query_communities, num_communities=num_communities)
            communities = [(r["community"], r["size"]) for r in result]
            
            if not communities:
                print("⚠️  Nessuna comunità trovata nel database!")
                print("   Esegui prima 4_community_detection.py --leiden --gamma 2.5")
                return []
            
            print(f"✓ Trovate {len(communities)} comunità:")
            for comm_id, size in communities:
                print(f"  - Comunità {comm_id}: {size} nodi")
            
            # Prendi pagine da ogni comunità
            all_pages = []
            query_pages = """
            MATCH (n:Node {community: $community})
            RETURN n.id AS page_id, n.community AS community
            LIMIT $limit
            """
            
            for comm_id, _ in communities:
                result = session.run(query_pages, community=comm_id, limit=limit)
                pages = [(record["page_id"], record["community"]) for record in result]
                all_pages.extend(pages)
                print(f"  → {len(pages)} pagine dalla comunità {comm_id}")
            
            print(f"\n✓ Totale: {len(all_pages)} pagine da {len(communities)} comunità")
            return all_pages
                
        except Exception as e:
            print(f"✗ Errore query Neo4j: {e}")
            print("   Assicurati di aver eseguito 4_community_detection.py")
            return []


def simulate_edit_war(producer, pages, duration_seconds=180, edits_per_minute=4):
    """
    Simula un'edit war: molte modifiche concentrate su poche pagine
    in un breve periodo di tempo.
    
    Args:
        producer: KafkaProducer instance
        pages: Lista di tuple (page_id, community_id)
        duration_seconds: Durata della simulazione
        edits_per_minute: Numero di modifiche al minuto
    """
    print(f"\n{'='*60}")
    print("SCENARIO: EDIT WAR")
    print(f"{'='*60}")
    print(f"Durata: {duration_seconds}s | Modifiche/minuto: {edits_per_minute}")
    
    # Raggruppa per comunità
    from collections import defaultdict
    by_community = defaultdict(list)
    for page_id, comm_id in pages:
        by_community[comm_id].append(page_id)
    
    print(f"Comunità coinvolte: {len(by_community)}")
    for comm_id, page_list in by_community.items():
        print(f"  - Comunità {comm_id}: {len(page_list)} pagine")
    
    # Concentra le modifiche su solo 5 pagine DALLA STESSA comunità
    # (per testare il rilevamento cluster)
    if by_community:
        # Prendi la prima comunità con più pagine
        target_community = max(by_community.items(), key=lambda x: len(x[1]))
        comm_id = target_community[0]
        hot_pages = [(p, comm_id) for p in target_community[1][:5]]
        print(f"\nTarget: Comunità {comm_id} con {len(hot_pages)} pagine")
    else:
        hot_pages = pages[:5]
    
    print(f"{'='*60}\n")
    
    users = [
        "EditWarrior1",
        "RevertBot",
        "ControversialEditor",
        "FactChecker99",
        "TruthSeeker"
    ]
    
    edit_types = ["edit", "new", "edit", "edit"]  # Maggioranza edit
    
    interval = 60.0 / edits_per_minute  # Tempo tra modifiche
    total_edits = int((duration_seconds / 60) * edits_per_minute)
    
    print(f"Invio di {total_edits} eventi in {duration_seconds}s...")
    print(f"Intervallo tra eventi: {interval:.1f}s\n")
    
    for i in range(total_edits):
        page_id, comm_id = random.choice(hot_pages)
        event = {
            "page_title": str(page_id),  # Usa ID come titolo
            "user": random.choice(users),
            "type": random.choice(edit_types),
            "timestamp": int(time.time()),
            "server_name": "it.wikipedia.org",
            "bot": False
        }
        
        try:
            producer.send(KAFKA_TOPIC, value=event)
            print(f"[{i+1}/{total_edits}] Inviato: pagina '{page_id}' (comm {comm_id}) by {event['user']}")
            time.sleep(interval)
        except Exception as e:
            print(f"✗ Errore invio evento: {e}")
    
    print(f"\n✓ Simulazione completata! {total_edits} eventi inviati.")


def simulate_normal_activity(producer, pages, duration_seconds=180, edits_per_minute=2):
    """
    Simula attività normale: modifiche distribuite su molte pagine e comunità.
    """
    print(f"\n{'='*60}")
    print("SCENARIO: ATTIVITÀ NORMALE")
    print(f"{'='*60}")
    print(f"Durata: {duration_seconds}s | Modifiche/minuto: {edits_per_minute}")
    print(f"Pagine target: {len(pages)} pagine (distribuite)")
    print(f"{'='*60}\n")
    
    users = [f"User{i}" for i in range(20)]
    edit_types = ["edit", "new", "categorize", "log"]
    
    interval = 60.0 / edits_per_minute
    total_edits = int((duration_seconds / 60) * edits_per_minute)
    
    print(f"Invio di {total_edits} eventi in {duration_seconds}s...")
    print(f"Intervallo tra eventi: {interval:.1f}s\n")
    
    for i in range(total_edits):
        page_id, comm_id = random.choice(pages)
        event = {
            "page_title": str(page_id),  # Usa ID come titolo
            "user": random.choice(users),
            "type": random.choice(edit_types),
            "timestamp": int(time.time()),
            "server_name": "it.wikipedia.org",
            "bot": random.choice([False, False, False, True])  # 25% bot
        }
        
        try:
            producer.send(KAFKA_TOPIC, value=event)
            print(f"[{i+1}/{total_edits}] Inviato: pagina '{page_id}' (comm {comm_id}) by {event['user']}")
            time.sleep(interval)
        except Exception as e:
            print(f"✗ Errore invio evento: {e}")
    
    print(f"\n✓ Simulazione completata! {total_edits} eventi inviati.")


def simulate_vandalism_attack(producer, pages, duration_seconds=120, edits_per_minute=8):
    """
    Simula un attacco di vandalismo: modifiche molto rapide da pochi utenti.
    """
    print(f"\n{'='*60}")
    print("SCENARIO: ATTACCO VANDALISMO")
    print(f"{'='*60}")
    print(f"Durata: {duration_seconds}s | Modifiche/minuto: {edits_per_minute}")
    
    # Vandalismo su poche pagine dalla stessa comunità
    from collections import defaultdict
    by_community = defaultdict(list)
    for page_id, comm_id in pages:
        by_community[comm_id].append(page_id)
    
    if by_community:
        target_community = max(by_community.items(), key=lambda x: len(x[1]))
        comm_id = target_community[0]
        target_pages = [(p, comm_id) for p in target_community[1][:3]]
        print(f"Pagine target: {len(target_pages)} pagine dalla comunità {comm_id}")
    else:
        target_pages = pages[:3]
    
    print(f"{'='*60}\n")
    
    # Pochi vandali
    vandals = ["Vandal123", "TrollMaster", "AnonymousIP"]
    
    interval = 60.0 / edits_per_minute
    total_edits = int((duration_seconds / 60) * edits_per_minute)
    
    print(f"Invio di {total_edits} eventi in {duration_seconds}s...")
    print(f"Intervallo tra eventi: {interval:.1f}s\n")
    
    for i in range(total_edits):
        page_id, comm_id = random.choice(target_pages)
        event = {
            "page_title": str(page_id),  # Usa ID come titolo
            "user": random.choice(vandals),
            "type": "edit",  # Solo edit
            "timestamp": int(time.time()),
            "server_name": "it.wikipedia.org",
            "bot": False
        }
        
        try:
            producer.send(KAFKA_TOPIC, value=event)
            print(f"[{i+1}/{total_edits}] Inviato: pagina '{page_id}' (comm {comm_id}) by {event['user']}")
            time.sleep(interval)
        except Exception as e:
            print(f"✗ Errore invio evento: {e}")
    
    print(f"\n✓ Simulazione completata! {total_edits} eventi inviati.")


def main():
    """Menu principale per scegliere lo scenario di test."""
    
    print("="*60)
    print("MOCK PRODUCER - Testing System")
    print("="*60)
    
    # Connessione a Neo4j
    driver = None
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        driver.verify_connectivity()
        print("✓ Connesso a Neo4j")
    except Exception as e:
        print(f"✗ Errore connessione Neo4j: {e}")
        print("Assicurati che Neo4j sia in esecuzione!")
        return
    
    # Ottieni pagine da comunità diverse
    pages = get_large_community_pages(driver, limit=10, num_communities=3)
    
    if not pages:
        print("\n✗ Impossibile procedere senza pagine dal database.")
        print("   Esegui prima:")
        print("   1. 3_load_graph.py  (per caricare il grafo)")
        print("   2. 4_community_detection.py --leiden --gamma 2.5")
        driver.close()
        return
    
    # Crea producer Kafka
    producer = create_producer()
    if not producer:
        driver.close()
        return
    
    # Menu di scelta
    print("\n" + "="*60)
    print("SCEGLI UNO SCENARIO DI TEST:")
    print("="*60)
    print("1. Edit War RAPIDO (20 edit/min x 30s = ~10 edit)")
    print("   → Dovrebbe attivare l'allarme (soglia: 5 edit)")
    print()
    print("2. Attività Normale RAPIDA (8 edit/min x 30s = ~4 edit)")
    print("   → NON dovrebbe attivare l'allarme (soglia: 5 edit)")
    print()
    print("3. Vandalismo RAPIDO (30 edit/min x 30s = ~15 edit)")
    print("   → Dovrebbe attivare l'allarme (soglia: 5 edit)")
    print()
    print("4. Custom (scegli parametri)")
    print("="*60)
    
    choice = input("\nInserisci la tua scelta (1-4): ").strip()
    
    try:
        if choice == "1":
            # Test veloce: 10 edit in 30 secondi
            simulate_edit_war(producer, pages, duration_seconds=30, edits_per_minute=20)
        
        elif choice == "2":
            # Test veloce: 4 edit in 30 secondi (sotto soglia)
            simulate_normal_activity(producer, pages, duration_seconds=30, edits_per_minute=8)
        
        elif choice == "3":
            # Test veloce: 15 edit in 30 secondi
            simulate_vandalism_attack(producer, pages, duration_seconds=30, edits_per_minute=30)
        
        elif choice == "4":
            print("\n--- Configurazione Custom ---")
            duration = int(input("Durata in secondi (es. 30): "))
            rate = int(input("Modifiche al minuto (es. 20): "))
            scenario = input("Tipo (war/normal/vandalism): ").lower()
            
            if scenario == "war":
                simulate_edit_war(producer, pages, duration, rate)
            elif scenario == "normal":
                simulate_normal_activity(producer, pages, duration, rate)
            elif scenario == "vandalism":
                simulate_vandalism_attack(producer, pages, duration, rate)
            else:
                print("✗ Tipo non riconosciuto!")
        
        else:
            print("✗ Scelta non valida!")
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Simulazione interrotta dall'utente.")
    except Exception as e:
        print(f"\n✗ Errore durante la simulazione: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Cleanup
        if producer:
            producer.flush()
            producer.close()
            print("\n✓ Producer Kafka chiuso.")
        if driver:
            driver.close()
            print("✓ Connessione Neo4j chiusa.")


if __name__ == "__main__":
    main()
