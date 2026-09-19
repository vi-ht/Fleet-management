import json
import os

from kafka import KafkaConsumer
from pymongo import MongoClient, ReplaceOne


consumer = KafkaConsumer(
    os.getenv("VEHICLE_TOPIC", "taxi_vehicles"),
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    group_id=os.getenv("VEHICLE_CONSUMER_GROUP", "taxi-fleet-dashboard-v2"),
    # Consume the startup snapshot even if the tracker starts a few seconds
    # after the producer. Upserts keep the MongoDB state idempotent.
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)
client = MongoClient(os.getenv("MONGO_URI", "mongodb://mongodb:27017"))
collection = client[os.getenv("MONGO_DATABASE", "taxi")][
    os.getenv("MONGO_VEHICLE_COLLECTION", "vehicle_status")
]

print("[PASS] Fleet tracker started", flush=True)
for message in consumer:
    vehicle = message.value
    vehicle_id = vehicle.get("vehicle_id")
    if not vehicle_id:
        continue
    collection.replace_one({"vehicle_id": vehicle_id}, vehicle, upsert=True)
    if vehicle.get("vehicle_id", "").endswith("001"):
        print(f"[PASS] Fleet tracker updated cycle {vehicle.get('simulation_cycle')}", flush=True)
