import json
from kafka import KafkaConsumer

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'cluster-alerts'

def consume_alerts():
    """Si connette a Kafka e stampa gli allarmi cluster ricevuti."""
    print("Avvio del Consumer per Allarmi Cluster...")
    print(f"In ascolto sul topic '{KAFKA_TOPIC}'...")

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            group_id='alert-monitor',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        
        print(f"✓ Connesso a Kafka!")
        print("✓ In attesa di allarmi cluster...\n")
        
        alert_count = 0
        for message in consumer:
            alert_count += 1
            alert = message.value
            
            print("=" * 70)
            print(f"🚨 ALLARME CLUSTER #{alert_count}")
            print("=" * 70)
            print(f"Comunità ID: {alert.get('communityId')}")
            print(f"Numero di modifiche: {alert.get('editCount')}")
            print(f"Finestra temporale: {alert.get('window_start')} → {alert.get('window_end')}")
            print(f"Pagine modificate:")
            for page in alert.get('pages', []):
                print(f"  - {page}")
            print("=" * 70)
            print()
            
    except KeyboardInterrupt:
        print("\nInterrotto dall'utente.")
    except Exception as e:
        print(f"Errore del Consumer: {e}")
    finally:
        print("Chiusura Consumer.")
        if 'consumer' in locals():
            consumer.close()

if __name__ == "__main__":
    consume_alerts()
