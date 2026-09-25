import csv
import hashlib
import json
import math
import os
import time
from datetime import datetime, timedelta, timezone
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
source_limit = int(os.getenv("SIMULATION_SOURCE_ROWS", "10000"))
simulation_timezone_offset = int(os.getenv("SIMULATION_TIMEZONE_OFFSET_HOURS", "7"))
event_time_step_seconds = float(os.getenv("SIMULATION_EVENT_TIME_STEP_SECONDS", "30"))
simulation_timezone = timezone(timedelta(hours=simulation_timezone_offset))
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
last_snapshot_at = None
simulation_clock = datetime.now(simulation_timezone)


def destination_for(vehicle_index, trip_number):
    """Create a deterministic destination inside the NYC operating area."""
    latitude = 40.70 + ((vehicle_index * 17 + trip_number * 11) % 155) / 1000
    longitude = -74.01 + ((vehicle_index * 29 + trip_number * 7) % 105) / 1000
    return latitude, longitude


def initial_vehicle_state(vehicle_index):
    grid_row, grid_column = divmod(vehicle_index, 8)
    latitude = 40.68 + grid_row * 0.022 + (vehicle_index % 3) * 0.001
    longitude = -74.02 + grid_column * 0.025 + (vehicle_index % 3) * 0.001
    trip_number = 1
    destination_latitude, destination_longitude = destination_for(vehicle_index, trip_number)
    return {
        "vehicle_index": vehicle_index,
        "vehicle_id": f"NYC-TAXI-{vehicle_index + 1:03d}",
        "latitude": latitude,
        "longitude": longitude,
        "route_start_latitude": latitude,
        "route_start_longitude": longitude,
        "destination_latitude": destination_latitude,
        "destination_longitude": destination_longitude,
        "trip_number": trip_number,
        "status": "occupied" if vehicle_index % 4 == 0 else "available",
        "pickup_zone": str(4 + ((vehicle_index * 17) % 260)),
        "dropoff_zone": str(4 + ((vehicle_index * 29 + 23) % 260)),
    }


vehicle_states = [initial_vehicle_state(index) for index in range(vehicle_count)]


def source_rows():
    """Replay a bounded slice of the real NYC Parquet source when available."""
    from pathlib import Path

    source_dir = Path(
        os.getenv(
            "RAW_DATA_PATH",
            "/workspace/Nyc taxi trip record/Nyc taxi trip record",
        )
    )
    parquet_files = sorted(source_dir.glob("yellow_tripdata_*.parquet"))
    if parquet_files:
        import pyarrow.dataset as ds

        dataset = ds.dataset([str(path) for path in parquet_files], format="parquet")
        scanner = dataset.scanner(
            columns=["PULocationID", "DOLocationID"], batch_size=10_000
        )
        yielded = 0
        for batch in scanner.to_batches():
            values = batch.to_pydict()
            for pickup_zone, dropoff_zone in zip(
                values["PULocationID"], values["DOLocationID"]
            ):
                if pickup_zone is None or dropoff_zone is None:
                    continue
                yield {"PULocationID": pickup_zone, "DOLocationID": dropoff_zone}
                yielded += 1
                if source_limit and yielded >= source_limit:
                    return
        return

    with open("/data/raw/taxi_trips.csv", encoding="utf-8") as source:
        for index, row in enumerate(csv.DictReader(source)):
            if source_limit and index >= source_limit:
                return
            yield row


def route_distance_km(latitude, longitude, destination_latitude, destination_longitude):
    # One degree of longitude is approximately 83 km around New York City.
    return math.hypot(
        (destination_latitude - latitude) * 111.0,
        (destination_longitude - longitude) * 83.0,
    )


def start_next_route(state):
    state["trip_number"] += 1
    state["route_start_latitude"] = state["latitude"]
    state["route_start_longitude"] = state["longitude"]
    state["destination_latitude"], state["destination_longitude"] = destination_for(
        state["vehicle_index"], state["trip_number"]
    )
    state["status"] = "occupied" if (state["vehicle_index"] + state["trip_number"]) % 4 == 0 else "available"
    state["pickup_zone"] = state["dropoff_zone"]
    state["dropoff_zone"] = str(4 + ((state["vehicle_index"] * 29 + state["trip_number"] * 5) % 260))


def advance_vehicle(state, elapsed_seconds):
    status = state["status"]
    speed_kmh = 28 + (state["vehicle_index"] * 7 + state["trip_number"]) % 24 if status == "occupied" else 14 + state["vehicle_index"] % 10
    destination_latitude = state["destination_latitude"]
    destination_longitude = state["destination_longitude"]
    remaining_km = route_distance_km(
        state["latitude"], state["longitude"], destination_latitude, destination_longitude
    )
    step_km = speed_kmh * elapsed_seconds / 3600
    if remaining_km <= max(step_km, 0.05):
        state["latitude"] = destination_latitude
        state["longitude"] = destination_longitude
        start_next_route(state)
        status = state["status"]
        speed_kmh = 28 + (state["vehicle_index"] * 7 + state["trip_number"]) % 24 if status == "occupied" else 14 + state["vehicle_index"] % 10
        remaining_km = route_distance_km(
            state["latitude"], state["longitude"], state["destination_latitude"], state["destination_longitude"]
        )
    else:
        ratio = step_km / remaining_km
        state["latitude"] += (destination_latitude - state["latitude"]) * ratio
        state["longitude"] += (destination_longitude - state["longitude"]) * ratio
        remaining_km -= step_km

    heading = math.degrees(
        math.atan2(
            state["destination_longitude"] - state["longitude"],
            state["destination_latitude"] - state["latitude"],
        )
    )
    total_km = route_distance_km(
        state["route_start_latitude"],
        state["route_start_longitude"],
        state["destination_latitude"],
        state["destination_longitude"],
    )
    progress = 0 if total_km == 0 else max(0, min(100, (1 - remaining_km / total_km) * 100))
    return speed_kmh, remaining_km, heading, progress


def send_vehicle_snapshot(cycle_number):
    global last_snapshot_at
    now = time.monotonic()
    elapsed_seconds = 0 if last_snapshot_at is None else max(0.5, now - last_snapshot_at)
    last_snapshot_at = now
    updated_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    events = []
    for state in vehicle_states:
        speed_kmh, remaining_km, heading, progress = advance_vehicle(state, elapsed_seconds)
        state["speed_kmh"] = round(speed_kmh, 1)
        events.append(
            {
                "vehicle_id": state["vehicle_id"],
                "status": state["status"],
                "route_status": "Đang chở khách" if state["status"] == "occupied" else "Đang tái bố trí tới điểm nóng",
                "pickup_zone": state["pickup_zone"],
                "dropoff_zone": state["dropoff_zone"],
                "latitude": round(state["latitude"], 6),
                "longitude": round(state["longitude"], 6),
                "destination_latitude": round(state["destination_latitude"], 6),
                "destination_longitude": round(state["destination_longitude"], 6),
                "speed_kmh": state["speed_kmh"],
                "heading_degrees": round(heading, 1),
                "progress_pct": round(progress, 1),
                "eta_minutes": round(remaining_km / max(speed_kmh, 1) * 60, 1),
                "updated_at": updated_at,
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
    for index, row in enumerate(source_rows()):
            # Convert the historical replay into a current-day accelerated stream.
            # The raw date is still useful for batch training, but online inference
            # must receive events from the current simulation clock.
            # Spark's default timestamp parser is most portable with a plain ISO
            # datetime. The clock itself is configured as UTC+7 for the dashboard.
            pickup_datetime = simulation_clock.replace(tzinfo=None).isoformat(timespec="seconds")
            simulation_clock += timedelta(seconds=event_time_step_seconds)
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
