"""
alert_consumer.py - Consumer Semplice per Cluster Alerts

Questo script ascolta il topic 'cluster-alerts' e stampa
gli allarmi in modo leggibile, senza analisi LLM.

Utile per:
- Verificare che gli allarmi vengano generati
- Debug del sistema
- Monitoraggio in tempo reale
"""

import json
from kafka import KafkaConsumer
from datetime import datetime

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'cluster-alerts'


def format_alert(alert_data):
    """Formatta l'allarme in modo leggibile."""
    
    community_id = alert_data.get('communityId', 'N/A')
    edit_count = alert_data.get('totalEditCount', 0)
    threshold = alert_data.get('threshold', 5)
    window_start = alert_data.get('windowStart', 'N/A')
    window_end = alert_data.get('windowEnd', 'N/A')
    pages = alert_data.get('pages', [])
    
    output = f"""
╔══════════════════════════════════════════════════════════════╗
║                  🚨 ALLARME HOTSPOT RILEVATO                 ║
╚══════════════════════════════════════════════════════════════╝

⏰ Timestamp:        {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
🆔 Comunità ID:      {community_id}
📊 Modifiche:        {edit_count} (soglia: {threshold})
⚠️  Superamento:     {edit_count - threshold} modifiche oltre soglia

📅 Finestra temporale:
   Inizio: {window_start}
   Fine:   {window_end}

📄 Pagine modificate ({len(pages)} totali):
"""
    
    for i, page in enumerate(pages[:10], 1):
        output += f"   {i:2d}. {page}\n"
    
    if len(pages) > 10:
        output += f"   ... e altre {len(pages) - 10} pagine\n"
    
    output += "\n" + "═" * 64 + "\n"
    
    return output


def main():
    """Ascolta gli allarmi e li stampa."""
    
    print("═" * 64)
    print("       ALERT CONSUMER - Monitor Cluster Alerts")
    print("═" * 64)
    print(f"Kafka Broker: {KAFKA_BROKER}")
    print(f"Topic:        {KAFKA_TOPIC}")
    print("═" * 64)
    
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='alert-monitor-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        print(f"\n✓ Connesso a Kafka")
        print(f"✓ In ascolto sul topic '{KAFKA_TOPIC}'...")
        print("\nIn attesa di allarmi... (Premi Ctrl+C per terminare)\n")
        
        alert_count = 0
        
        for message in consumer:
            alert_count += 1
            alert = message.value
            
            print(format_alert(alert))
            print(f"Allarmi ricevuti: {alert_count}\n")
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Interruzione da utente. Chiusura...")
    
    except Exception as e:
        print(f"\n✗ Errore: {e}")
    
    finally:
        if 'consumer' in locals():
            consumer.close()
            print("✓ Consumer Kafka chiuso.")


if __name__ == "__main__":
    main()
