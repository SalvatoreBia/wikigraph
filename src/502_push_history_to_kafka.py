"""
Script per inviare tutti gli edit salvati in 501_manual_edits_history.json
al topic Kafka 'to-be-judged' per farli giudicare.
"""

import json
import time
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:9094'
TOPIC_OUT = 'to-be-judged'
HISTORY_FILE = '501_manual_edits_history.json'

def load_history():
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"! File '{HISTORY_FILE}' non trovato.")
        return []
    except json.JSONDecodeError as e:
        print(f"! Errore nel parsing di '{HISTORY_FILE}': {e}")
        return []

def main():
    print("--- PUSH HISTORY TO KAFKA ---")
    
    history = load_history()
    if not history:
        print("! Nessun edit da inviare.")
        return
    
    print(f"- Trovati {len(history)} edit da inviare.")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"- Connesso a Kafka ({KAFKA_BROKER}).\n")
    except Exception as e:
        print(f"! Errore connessione Kafka: {e}")
        return
    
    sent = 0
    for i, edit in enumerate(history, 1):
        title = edit.get("title", "N/A")
        is_vandalism = edit.get("is_vandalism", False)
        label = "VANDAL" if is_vandalism else "LEGIT"
        
        try:
            producer.send(TOPIC_OUT, value=edit)
            sent += 1
            print(f"[{i}/{len(history)}] {title} ({label}) -> inviato")
        except Exception as e:
            print(f"[{i}/{len(history)}] {title} -> ERRORE: {e}")
        
        # Piccola pausa per non sovraccaricare
        time.sleep(0.1)
    
    producer.flush()
    producer.close()
    
    print(f"\n- Completato: {sent}/{len(history)} edit inviati al topic '{TOPIC_OUT}'.")

if __name__ == "__main__":
    main()
