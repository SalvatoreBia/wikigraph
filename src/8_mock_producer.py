"""
8_mock_producer.py - Producer Mock per Testing

Questo script simula eventi di modifica Wikipedia per testare
il sistema di rilevamento hotspot senza dipendere dallo stream reale.

Può simulare diversi scenari:
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

# Connessione a Neo4j per ottenere pagine reali dal grafo
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


def get_large_community_pages(neo4j_driver, limit=20):
    """
    Interroga Neo4j per trovare una comunità grande e 
    restituisce alcune pagine da quella comunità.
    
    Se le comunità non sono state calcolate, usa un fallback
    che seleziona pagine con PageRank alto.
    """
    print("\nInterrogazione Neo4j per trovare pagine da testare...")
    
    # Prova prima a cercare comunità
    query_with_community = """
    MATCH (p:Page)
    WHERE p.globalCommunityId IS NOT NULL
    WITH p.globalCommunityId AS community, COUNT(p) AS size
    ORDER BY size DESC
    LIMIT 1
    WITH community
    MATCH (p:Page {globalCommunityId: community})
    RETURN p.title AS title, p.globalCommunityId AS community
    LIMIT $limit
    """
    
    with neo4j_driver.session() as session:
        try:
            result = session.run(query_with_community, limit=limit)
            pages = [(record["title"], record["community"]) for record in result]
            
            if pages:
                community_id = pages[0][1]
                titles = [p[0] for p in pages]
                print(f"✓ Trovata comunità {community_id} con {len(titles)} pagine")
                print(f"  Esempi: {', '.join(titles[:5])}...")
                return titles, community_id
        except Exception as e:
            # Ignora errori se property 'community' non esiste
            pass
        
        # FALLBACK: Se non ci sono comunità, usa pagine con PageRank alto
        print("⚠️  Comunità non ancora calcolate, uso fallback...")
        print("   (Esegui script 4 e 5 per calcolare le comunità)")
        
        fallback_query = """
        MATCH (p:Page)
        WHERE p.pagerank IS NOT NULL
        RETURN p.title AS title, p.pagerank AS pagerank
        ORDER BY p.pagerank DESC
        LIMIT $limit
        """
        
        result = session.run(fallback_query, limit=limit)
        records = list(result)
        
        if records:
            titles = [r["title"] for r in records]
            # Usa ID fittizio per comunità
            fake_community_id = "FALLBACK_0"
            print(f"✓ Trovate {len(titles)} pagine importanti (per PageRank)")
            print(f"  Esempi: {', '.join(titles[:5])}...")
            print(f"  Comunità fittizia: {fake_community_id}")
            return titles, fake_community_id
        else:
            print("✗ Nessuna pagina trovata nel database!")
            print("   Assicurati di aver eseguito gli script 1-3")
            return [], None


def simulate_edit_war(producer, pages, duration_seconds=180, edits_per_minute=4):
    """
    Simula un'edit war: molte modifiche concentrate su poche pagine
    in un breve periodo di tempo.
    
    Args:
        producer: KafkaProducer instance
        pages: Lista di titoli di pagine
        duration_seconds: Durata della simulazione
        edits_per_minute: Numero di modifiche al minuto
    """
    print(f"\n{'='*60}")
    print("SCENARIO: EDIT WAR")
    print(f"{'='*60}")
    print(f"Durata: {duration_seconds}s | Modifiche/minuto: {edits_per_minute}")
    print(f"Pagine target: {len(pages[:5])} pagine (concentrate)")
    print(f"{'='*60}\n")
    
    # Concentra le modifiche su solo 3-5 pagine
    hot_pages = pages[:min(5, len(pages))]
    
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
        event = {
            "page_title": random.choice(hot_pages),
            "user": random.choice(users),
            "type": random.choice(edit_types),
            "timestamp": int(time.time()),
            "server_name": "it.wikipedia.org",
            "bot": False
        }
        
        try:
            producer.send(KAFKA_TOPIC, value=event)
            print(f"[{i+1}/{total_edits}] Inviato: '{event['page_title']}' by {event['user']}")
            time.sleep(interval)
        except Exception as e:
            print(f"✗ Errore invio evento: {e}")
    
    print(f"\n✓ Simulazione completata! {total_edits} eventi inviati.")


def simulate_normal_activity(producer, pages, duration_seconds=180, edits_per_minute=2):
    """
    Simula attività normale: modifiche distribuite su molte pagine.
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
        event = {
            "page_title": random.choice(pages),
            "user": random.choice(users),
            "type": random.choice(edit_types),
            "timestamp": int(time.time()),
            "server_name": "it.wikipedia.org",
            "bot": random.choice([False, False, False, True])  # 25% bot
        }
        
        try:
            producer.send(KAFKA_TOPIC, value=event)
            print(f"[{i+1}/{total_edits}] Inviato: '{event['page_title']}' by {event['user']}")
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
    print(f"Pagine target: {len(pages[:3])} pagine (concentrate)")
    print(f"{'='*60}\n")
    
    # Vandalismo su poche pagine
    target_pages = pages[:min(3, len(pages))]
    
    # Pochi vandali
    vandals = ["Vandal123", "TrollMaster", "AnonymousIP"]
    
    interval = 60.0 / edits_per_minute
    total_edits = int((duration_seconds / 60) * edits_per_minute)
    
    print(f"Invio di {total_edits} eventi in {duration_seconds}s...")
    print(f"Intervallo tra eventi: {interval:.1f}s\n")
    
    for i in range(total_edits):
        event = {
            "page_title": random.choice(target_pages),
            "user": random.choice(vandals),
            "type": "edit",  # Solo edit
            "timestamp": int(time.time()),
            "server_name": "it.wikipedia.org",
            "bot": False
        }
        
        try:
            producer.send(KAFKA_TOPIC, value=event)
            print(f"[{i+1}/{total_edits}] Inviato: '{event['page_title']}' by {event['user']}")
            time.sleep(interval)
        except Exception as e:
            print(f"✗ Errore invio evento: {e}")
    
    print(f"\n✓ Simulazione completata! {total_edits} eventi inviati.")


def main():
    """Menu principale per scegliere lo scenario di test."""
    
    # Connessione a Neo4j
    print("="*60)
    print("MOCK PRODUCER - Testing System")
    print("="*60)
    
    driver = None
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        print("✓ Connesso a Neo4j")
    except Exception as e:
        print(f"✗ Errore connessione Neo4j: {e}")
        print("Assicurati che Neo4j sia in esecuzione!")
        return
    
    # Ottieni pagine da una comunità grande
    pages, community_id = get_large_community_pages(driver)
    
    if not pages:
        print("\nImpossibile procedere senza pagine dal database.")
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
                print("Tipo non riconosciuto!")
        
        else:
            print("Scelta non valida!")
    
    except KeyboardInterrupt:
        print("\n\n⚠ Simulazione interrotta dall'utente.")
    except Exception as e:
        print(f"\n✗ Errore durante la simulazione: {e}")
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
