# src/kafka_consumer_test.py

import json
from kafka import KafkaConsumer

# --- Configurazione ---
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'wiki-changes'

def consume_messages():
    """Si connette a Kafka e stampa i messaggi ricevuti."""
    print("Avvio del Consumer Test...")
    print(f"In ascolto sul topic '{KAFKA_TOPIC}'...")

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            group_id='wiki-test-consumer',  # Identificativo del consumer group
            auto_offset_reset='earliest',   # Leggi dall'inizio se non ci sono offset salvati
            enable_auto_commit=True,
            # Deserializza il JSON ricevuto
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        
        print(f"✓ Connesso a Kafka!")
        print(f"✓ Partizioni assegnate: {consumer.assignment()}")
        print("✓ In attesa di messaggi...\n")
        
        # Stampa ogni messaggio che arriva
        for message in consumer:
            print("--- MESSAGGIO RICEVUTO ---")
            # message.value è il dizionario Python che abbiamo inviato
            print(json.dumps(message.value, indent=2, ensure_ascii=False))
            print(f"Offset: {message.offset}, Partizione: {message.partition}\n")
            
    except Exception as e:
        print(f"Errore del Consumer: {e}")
    finally:
        print("Chiusura Consumer.")
        if 'consumer' in locals():
            consumer.close()

if __name__ == "__main__":
    consume_messages()