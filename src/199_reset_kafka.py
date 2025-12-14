from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import UnknownTopicOrPartitionError
import time

from config_loader import load_config

CONFIG = load_config()

KAFKA_BROKER = CONFIG['kafka']['broker']
TOPICS_TO_RESET = [CONFIG['kafka']['topic_changes'], CONFIG['kafka']['topic_judge']]

def reset_kafka():
    print("--- 🧹 RESET KAFKA TOPICS ---")
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER, client_id='reset_script')
    except Exception as e:
        print(f"❌ Error connecting to Kafka: {e}")
        return

    # Delete topics
    print(f"Deleting topics: {TOPICS_TO_RESET}...")
    try:
        admin_client.delete_topics(topics=TOPICS_TO_RESET)
        print("✅ Topics deleted.")
    except UnknownTopicOrPartitionError:
        print("⚠️ Some topics did not exist, skipping deletion.")
    except Exception as e:
        print(f"❌ Error deleting topics: {e}")

    # Give Kafka a moment to actually delete them
    time.sleep(2)

    # Recreate topics
    print("Recreating topics...")
    topic_list = [NewTopic(name=name, num_partitions=1, replication_factor=1) for name in TOPICS_TO_RESET]
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print("✅ Topics recreated.")
    except Exception as e:
        print(f"❌ Error creating topics: {e}")
        
    admin_client.close()
    print("--- RESET COMPLETE ---")

if __name__ == "__main__":
    reset_kafka()
