import csv
import hashlib
import json
import os
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable


servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
topic = os.getenv("KAFKA_TOPIC", "taxi_trips")
vehicle_topic = os.getenv("VEHICLE_TOPIC", "taxi_vehicles")
delay = float(os.getenv("PRODUCER_DELAY_SECONDS", "0.25"))
loop = os.getenv("SIMULATION_LOOP", "true").lower() == "true"
cycle_pause = float(os.getenv("SIMULATION_CYCLE_PAUSE_SECONDS", "5"))
max_events = int(os.getenv("SIMULATION_MAX_EVENTS", "0"))
vehicle_count = int(os.getenv("SIMULATION_VEHICLES", "40"))
vehicle_snapshot_interval = int(os.getenv("SIMULATION_VEHICLE_SNAPSHOT_INTERVAL", "100"))
producer = None
for _ in range(60):
    try:
        producer = KafkaProducer(
            bootstrap_servers=servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )
        break
    except (NoBrokersAvailable, KafkaError):
        time.sleep(2)
if producer is None:
    raise RuntimeError("Kafka producer could not connect")

cycle = 0


def send_vehicle_snapshot(cycle_number):
    events = []
    for vehicle_index in range(vehicle_count):
        vehicle_number = vehicle_index + 1
        zone_number = 4 + ((vehicle_index * 17 + cycle_number * 3) % 260)
        destination_number = 4 + ((vehicle_index * 29 + cycle_number * 5 + 23) % 260)
        status = "occupied" if (vehicle_index + cycle_number) % 4 == 0 else "available"
        # Deterministic coordinates keep the simulation reproducible while placing
        # markers inside the NYC operating area for the dashboard map.
        grid_row, grid_column = divmod(vehicle_index, 8)
        latitude = 40.68 + grid_row * 0.022 + ((cycle_number + vehicle_index) % 3) * 0.001
        longitude = -74.02 + grid_column * 0.025 + ((cycle_number * 2 + vehicle_index) % 3) * 0.001
        events.append(
            {
                "vehicle_id": f"NYC-TAXI-{vehicle_number:03d}",
                "status": status,
                "pickup_zone": str(zone_number),
                "dropoff_zone": str(destination_number),
                "latitude": round(latitude, 6),
                "longitude": round(longitude, 6),
                "speed_kmh": 0 if status == "available" else 18 + (vehicle_index * 7 + cycle_number) % 32,
                "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "simulation_cycle": cycle_number + 1,
            }
        )
    for event in events:
        producer.send(vehicle_topic, event)
    producer.flush()
    print(f"[PASS] Fleet snapshot cycle {cycle_number + 1}: {len(events)} vehicles", flush=True)


while True:
    count = 0
    send_vehicle_snapshot(cycle)
    with open("/data/raw/taxi_trips.csv", encoding="utf-8") as source:
        for index, row in enumerate(csv.DictReader(source)):
            pickup_datetime = row.get("tpep_pickup_datetime") or row.get("pickup_datetime", "")
            pickup_zone = row.get("PULocationID") or row.get("pickup_zone", "")
            dropoff_zone = row.get("DOLocationID") or row.get("dropoff_zone", "")
            event_id = row.get("event_id") or hashlib.sha1(
                f"{pickup_datetime}|{pickup_zone}|{dropoff_zone}|{index}".encode()
            ).hexdigest()
            event = {
                "event_id": event_id,
                "pickup_datetime": pickup_datetime,
                "pickup_zone": str(pickup_zone),
                "dropoff_zone": str(dropoff_zone),
            }
            producer.send(topic, event)
            count += 1
            if count % vehicle_snapshot_interval == 0:
                send_vehicle_snapshot(cycle + count // vehicle_snapshot_interval)
            if max_events and count >= max_events:
                break
            time.sleep(delay)
    producer.flush()
    cycle += 1
    print(f"[PASS] Replayed realtime cycle {cycle}: {count} taxi events", flush=True)
    if not loop:
        break
    time.sleep(cycle_pause)
producer.close()
