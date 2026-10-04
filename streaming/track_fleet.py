import json
import os

from kafka import KafkaConsumer
from pymongo import MongoClient


vehicle_topic = os.getenv("VEHICLE_TOPIC", "taxi_vehicles")
vehicle_event_topic = os.getenv("VEHICLE_EVENT_TOPIC", "taxi_vehicle_trip_events")
consumer = KafkaConsumer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    group_id=os.getenv("VEHICLE_CONSUMER_GROUP", "taxi-fleet-dashboard-v2"),
    # Consume the startup snapshot even if the tracker starts a few seconds
    # after the producer. Upserts keep the MongoDB state idempotent.
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)
consumer.subscribe([vehicle_topic, vehicle_event_topic])
client = MongoClient(os.getenv("MONGO_URI", "mongodb://mongodb:27017"))
database = client[os.getenv("MONGO_DATABASE", "taxi")]
collection = database[
    os.getenv("MONGO_VEHICLE_COLLECTION", "vehicle_status")
]
trip_events = database[os.getenv("MONGO_VEHICLE_EVENTS_COLLECTION", "vehicle_trip_events")]
trip_events.create_index("event_id", unique=True)
trip_events.create_index([("vehicle_id", 1), ("event_time", -1)])

print("[PASS] Fleet tracker started", flush=True)
for message in consumer:
    record = message.value
    if message.topic == vehicle_event_topic:
        event_id = record.get("event_id")
        if not event_id or not record.get("vehicle_id"):
            continue
        trip_events.replace_one({"event_id": event_id}, record, upsert=True)
        print(
            f"[PASS] Fleet trip event saved: {record.get('event_type')} "
            f"{record.get('vehicle_id')} zone {record.get('zone')}",
            flush=True,
        )
        continue
    vehicle_id = record.get("vehicle_id")
    if not vehicle_id:
        continue
    collection.replace_one({"vehicle_id": vehicle_id}, record, upsert=True)
    if vehicle_id.endswith("001"):
        print(f"[PASS] Fleet tracker updated cycle {record.get('simulation_cycle')}", flush=True)
