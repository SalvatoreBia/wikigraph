from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import UnknownTopicOrPartitionError
import time

from config_loader import load_config

CONFIG = load_config()

KAFKA_BROKER = CONFIG['kafka']['broker']
TOPICS_TO_RESET = [CONFIG['kafka']['topic_changes'], CONFIG['kafka']['topic_judge']]

def reset_kafka():
    print("--- RESET KAFKA TOPICS ---")
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER, client_id='reset_script')
    except Exception as e:
        print(f"! Errore connessione Kafka: {e}")
        return

    print(f"- Cancellazione topic: {TOPICS_TO_RESET}...")
    try:
        admin_client.delete_topics(topics=TOPICS_TO_RESET)
        print("- Topic cancellati.")
    except UnknownTopicOrPartitionError:
        print("! Alcuni topic non esistevano, salto cancellazione.")
    except Exception as e:
        print(f"! Errore cancellazione topic: {e}")

    time.sleep(2)

    print("- Ricreazione topic...")
    topic_list = [NewTopic(name=name, num_partitions=1, replication_factor=1) for name in TOPICS_TO_RESET]
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print("- Topic ricreati.")
    except Exception as e:
        print(f"! Errore creazione topic: {e}")
        
    admin_client.close()
    print("--- RESET COMPLETATO ---")

if __name__ == "__main__":
    reset_kafka()
