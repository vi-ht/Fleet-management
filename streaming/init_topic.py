import os
import time

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, NoBrokersAvailable, TopicAlreadyExistsError


servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
topic = os.getenv("KAFKA_TOPIC", "taxi_trips")
topics = [topic, os.getenv("VEHICLE_TOPIC", "taxi_vehicles")]
for _ in range(60):
    try:
        admin = KafkaAdminClient(bootstrap_servers=servers, client_id="taxi-topic-init")
        try:
            admin.create_topics(
                [NewTopic(name=name, num_partitions=1, replication_factor=1) for name in topics]
            )
        except TopicAlreadyExistsError:
            pass
        admin.close()
        print(f"[PASS] Kafka topics ready: {', '.join(topics)}", flush=True)
        break
    except (NoBrokersAvailable, KafkaError):
        time.sleep(2)
else:
    raise RuntimeError("Kafka did not become ready")
