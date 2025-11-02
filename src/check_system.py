#!/usr/bin/env python3
"""
check_system.py - Verifica Prerequisiti Sistema

Verifica che tutti i componenti necessari siano in esecuzione
e configurati correttamente prima di avviare i test.
"""

import sys
import json
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from neo4j import GraphDatabase


# Configurazione
KAFKA_BROKER = 'localhost:9092'
NEO4J_SERVERS = {
    0: "bolt://localhost:7687",
    1: "bolt://localhost:7688",
    2: "bolt://localhost:7689",
    3: "bolt://localhost:7690",
}
NEO4J_USER = "neo4j"
NEO4J_PASS = "password"

REQUIRED_TOPICS = ['wiki-changes', 'cluster-alerts']


def check_kafka():
    """Verifica connessione Kafka e topic."""
    print("\n" + "="*60)
    print("1. VERIFICA KAFKA")
    print("="*60)
    
    try:
        # Test connessione
        admin = KafkaAdminClient(
            bootstrap_servers=[KAFKA_BROKER],
            request_timeout_ms=5000
        )
        
        print(f"✓ Connessione a {KAFKA_BROKER} OK")
        
        # Verifica topic esistenti
        existing_topics = admin.list_topics()
        print(f"✓ Topic esistenti: {len(existing_topics)}")
        
        missing_topics = []
        for topic in REQUIRED_TOPICS:
            if topic in existing_topics:
                print(f"  ✓ Topic '{topic}' presente")
            else:
                print(f"  ✗ Topic '{topic}' MANCANTE")
                missing_topics.append(topic)
        
        # Crea topic mancanti
        if missing_topics:
            print(f"\n⚠️  Creazione topic mancanti...")
            new_topics = [
                NewTopic(name=topic, num_partitions=1, replication_factor=1)
                for topic in missing_topics
            ]
            admin.create_topics(new_topics=new_topics, validate_only=False)
            print(f"✓ Topic creati: {', '.join(missing_topics)}")
        
        admin.close()
        return True
    
    except Exception as e:
        print(f"✗ ERRORE Kafka: {e}")
        print("\nSOLUZIONE:")
        print("  1. Verifica che Docker sia in esecuzione: docker ps")
        print("  2. Avvia Kafka: docker-compose up -d")
        return False


def check_neo4j():
    """Verifica connessioni Neo4j cluster."""
    print("\n" + "="*60)
    print("2. VERIFICA NEO4J CLUSTER")
    print("="*60)
    
    all_ok = True
    
    for server_id, uri in NEO4J_SERVERS.items():
        try:
            driver = GraphDatabase.driver(uri, auth=(NEO4J_USER, NEO4J_PASS))
            
            with driver.session() as session:
                # Query di test
                result = session.run("RETURN 1 AS test")
                _ = result.single()
                
                # Conta nodi
                result = session.run("MATCH (p:Page) RETURN count(p) AS count")
                page_count = result.single()["count"]
                
                # Conta comunità
                result = session.run(
                    "MATCH (p:Page) WHERE p.community IS NOT NULL "
                    "RETURN count(DISTINCT p.community) AS count"
                )
                community_count = result.single()["count"]
                
                print(f"✓ Server {server_id} ({uri}):")
                print(f"  - Pagine: {page_count:,}")
                print(f"  - Comunità: {community_count:,}")
            
            driver.close()
        
        except Exception as e:
            print(f"✗ Server {server_id} ({uri}): {e}")
            all_ok = False
    
    if not all_ok:
        print("\nSOLUZIONE:")
        print("  1. Verifica che Neo4j sia in esecuzione: docker ps | grep neo4j")
        print("  2. Avvia cluster: ./src/2_start_docker.sh")
        print("  3. Carica dati: python src/3_load_graph_data.py")
    
    return all_ok


def check_python_packages():
    """Verifica pacchetti Python necessari."""
    print("\n" + "="*60)
    print("3. VERIFICA PACCHETTI PYTHON")
    print("="*60)
    
    required = {
        'kafka': 'kafka-python',
        'neo4j': 'neo4j',
        'requests': 'requests',
    }
    
    optional = {
        'google.generativeai': 'google-generativeai (per Gemini)',
        'openai': 'openai (per GPT)',
    }
    
    all_ok = True
    
    print("\nPacchetti richiesti:")
    for module, package in required.items():
        try:
            __import__(module)
            print(f"  ✓ {package}")
        except ImportError:
            print(f"  ✗ {package} MANCANTE")
            all_ok = False
    
    print("\nPacchetti opzionali (per LLM):")
    for module, package in optional.items():
        try:
            __import__(module)
            print(f"  ✓ {package}")
        except ImportError:
            print(f"  ⚠ {package} non installato (opzionale)")
    
    if not all_ok:
        print("\nSOLUZIONE:")
        print("  pip install -r requirements.txt")
    
    return all_ok


def check_llm_config():
    """Verifica configurazione LLM."""
    print("\n" + "="*60)
    print("4. VERIFICA CONFIGURAZIONE LLM (OPZIONALE)")
    print("="*60)
    
    import os
    
    # Gemini
    gemini_key = os.environ.get('GEMINI_API_KEY')
    if gemini_key:
        print(f"✓ GEMINI_API_KEY configurata ({gemini_key[:10]}...)")
    else:
        print("⚠ GEMINI_API_KEY non configurata")
        print("  export GEMINI_API_KEY='your-key'")
    
    # OpenAI
    openai_key = os.environ.get('OPENAI_API_KEY')
    if openai_key:
        print(f"✓ OPENAI_API_KEY configurata ({openai_key[:10]}...)")
    else:
        print("⚠ OPENAI_API_KEY non configurata")
        print("  export OPENAI_API_KEY='your-key'")
    
    # Ollama
    try:
        import requests
        response = requests.get('http://localhost:11434/api/tags', timeout=2)
        if response.status_code == 200:
            models = response.json().get('models', [])
            print(f"✓ Ollama in esecuzione ({len(models)} modelli)")
            if models:
                print(f"  Modelli: {', '.join([m['name'] for m in models[:3]])}")
        else:
            print("⚠ Ollama risponde ma con errore")
    except Exception:
        print("⚠ Ollama non in esecuzione")
        print("  ollama serve")
    
    print("\nNOTA: Almeno un LLM dovrebbe essere configurato per usare 9_llm_consumer.py")
    return True


def run_system_test():
    """Test end-to-end veloce."""
    print("\n" + "="*60)
    print("5. TEST END-TO-END")
    print("="*60)
    
    try:
        # Test: invia un messaggio di test a Kafka
        print("\nInvio messaggio di test a 'wiki-changes'...")
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        test_event = {
            "page_title": "Test_Page",
            "user": "SystemCheck",
            "type": "edit",
            "timestamp": 1234567890,
        }
        
        producer.send('wiki-changes', value=test_event)
        producer.flush()
        producer.close()
        
        print("✓ Messaggio inviato a Kafka")
        
        # Test: leggi da Neo4j una comunità grande
        print("\nRecupero comunità grande da Neo4j...")
        driver = GraphDatabase.driver(
            NEO4J_SERVERS[0], 
            auth=(NEO4J_USER, NEO4J_PASS)
        )
        
        with driver.session() as session:
            result = session.run("""
                MATCH (p:Page)
                WHERE p.community IS NOT NULL
                WITH p.community AS community, COUNT(p) AS size
                ORDER BY size DESC
                LIMIT 1
                RETURN community, size
            """)
            
            record = result.single()
            if record:
                comm_id = record["community"]
                size = record["size"]
                print(f"✓ Comunità più grande: {comm_id} ({size} pagine)")
            else:
                print("⚠ Nessuna comunità trovata")
        
        driver.close()
        
        print("\n" + "="*60)
        print("✓ SISTEMA PRONTO PER I TEST!")
        print("="*60)
        return True
    
    except Exception as e:
        print(f"✗ Errore nel test: {e}")
        return False


def main():
    """Esegue tutti i controlli."""
    
    print("╔" + "═"*60 + "╗")
    print("║" + " "*15 + "VERIFICA SISTEMA COMPLETA" + " "*20 + "║")
    print("╚" + "═"*60 + "╝")
    
    results = []
    
    results.append(("Kafka", check_kafka()))
    results.append(("Neo4j", check_neo4j()))
    results.append(("Python Packages", check_python_packages()))
    results.append(("LLM Config", check_llm_config()))
    
    # Riepilogo
    print("\n" + "="*60)
    print("RIEPILOGO")
    print("="*60)
    
    all_ok = True
    for name, status in results:
        icon = "✓" if status else "✗"
        status_text = "OK" if status else "ERRORE"
        print(f"{icon} {name:20s} {status_text}")
        if not status:
            all_ok = False
    
    if all_ok:
        print("\n" + "="*60)
        run_system_test()
        
        print("\nPROSSIMI PASSI:")
        print("1. Terminale 1: python src/7_stream_processor.py")
        print("2. Terminale 2: python src/9_llm_consumer.py")
        print("3. Terminale 3: python src/8_mock_producer.py")
        print("\nOppure leggi TESTING_GUIDE.md per maggiori dettagli.")
    else:
        print("\n⚠️  Alcuni componenti non sono pronti.")
        print("   Risolvi gli errori sopra e riesegui questo script.")
        sys.exit(1)


if __name__ == "__main__":
    main()
