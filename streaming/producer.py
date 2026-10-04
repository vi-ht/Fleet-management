import csv
import hashlib
import json
import math
import os
import time
from datetime import datetime, timedelta
from urllib.parse import quote
from zoneinfo import ZoneInfo
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from pymongo import MongoClient


servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
topic = os.getenv("KAFKA_TOPIC", "taxi_trips")
vehicle_topic = os.getenv("VEHICLE_TOPIC", "taxi_vehicles")
mongo_client = MongoClient(os.getenv("MONGO_URI", "mongodb://mongodb:27017"))
database = mongo_client[os.getenv("MONGO_DATABASE", "taxi")]
dispatch_commands = database[os.getenv("MONGO_DISPATCH_COLLECTION", "vehicle_dispatch_commands")]
delay = float(os.getenv("PRODUCER_DELAY_SECONDS", "0.25"))
loop = os.getenv("SIMULATION_LOOP", "true").lower() == "true"
cycle_pause = float(os.getenv("SIMULATION_CYCLE_PAUSE_SECONDS", "5"))
max_events = int(os.getenv("SIMULATION_MAX_EVENTS", "0"))
vehicle_count = int(os.getenv("SIMULATION_VEHICLES", "40"))
vehicle_snapshot_interval = int(os.getenv("SIMULATION_VEHICLE_SNAPSHOT_INTERVAL", "100"))
source_limit = int(os.getenv("SIMULATION_SOURCE_ROWS", "10000"))
simulation_timezone = ZoneInfo(os.getenv("SIMULATION_TIMEZONE", "America/New_York"))
event_time_step_seconds = float(os.getenv("SIMULATION_EVENT_TIME_STEP_SECONDS", "30"))
router_url = os.getenv("ROUTING_BASE_URL", "https://router.project-osrm.org").rstrip("/")
route_retry_seconds = float(os.getenv("ROUTE_RETRY_SECONDS", "30"))
route_request_interval = float(os.getenv("ROUTE_REQUEST_INTERVAL_SECONDS", "1.1"))
last_route_request_at = 0.0
route_session = requests.Session()
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
    status = "occupied" if vehicle_index % 4 == 0 else "available"
    pickup_zone = str(4 + ((vehicle_index * 17) % 260))
    destination_latitude, destination_longitude = (
        destination_for(vehicle_index, trip_number) if status == "occupied" else (latitude, longitude)
    )
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
        "status": status,
        "pickup_zone": pickup_zone,
        "dropoff_zone": str(4 + ((vehicle_index * 29 + 23) % 260)) if status == "occupied" else pickup_zone,
        "route_coordinates": [],
        "route_segment_index": 0,
        "route_segment_progress_m": 0.0,
        "route_distance_m": 0.0,
        "route_remaining_m": 0.0,
        "route_retry_at": 0.0,
        "route_error": None,
        "dispatch_status": "idle",
        "dispatch_command_id": None,
        "dispatch_target_zone": None,
    }


vehicle_states = [initial_vehicle_state(index) for index in range(vehicle_count)]
dispatch_commands.update_many(
    {"status": {"$in": ["routing", "en_route"]}},
    {"$set": {"status": "approved", "recovered_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}},
)


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


def route_distance_m(first, second):
    """Great-circle distance between two [longitude, latitude] route points."""
    lon1, lat1 = map(math.radians, first)
    lon2, lat2 = map(math.radians, second)
    dlat, dlon = lat2 - lat1, lon2 - lon1
    value = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 6_371_000 * 2 * math.atan2(math.sqrt(value), math.sqrt(max(0, 1 - value)))


def clear_route(state):
    state["route_coordinates"] = []
    state["route_segment_index"] = 0
    state["route_segment_progress_m"] = 0.0
    state["route_distance_m"] = 0.0
    state["route_remaining_m"] = 0.0


def request_drive_route(state, now):
    """Fetch an OSRM driving route; leave the vehicle stopped if unavailable."""
    global last_route_request_at
    if now < state["route_retry_at"] or now - last_route_request_at < route_request_interval:
        return False
    last_route_request_at = now
    start = f'{state["longitude"]},{state["latitude"]}'
    end = f'{state["destination_longitude"]},{state["destination_latitude"]}'
    url = f"{router_url}/route/v1/driving/{quote(start, safe=',')};{quote(end, safe=',')}"
    try:
        response = route_session.get(url, params={"overview": "full", "geometries": "geojson", "steps": "false"}, timeout=5)
        response.raise_for_status()
        payload = response.json()
        routes = payload.get("routes") or []
        coordinates = routes[0]["geometry"]["coordinates"] if routes else []
        if payload.get("code") != "Ok" or len(coordinates) < 2:
            raise ValueError(payload.get("message") or payload.get("code") or "OSRM returned no route")
        state["route_coordinates"] = coordinates
        state["route_segment_index"] = 0
        state["route_segment_progress_m"] = 0.0
        state["route_distance_m"] = float(routes[0]["distance"])
        state["route_remaining_m"] = state["route_distance_m"]
        state["latitude"], state["longitude"] = coordinates[0][1], coordinates[0][0]
        state["route_error"] = None
        if state["dispatch_status"] == "routing":
            state["dispatch_status"] = "en_route"
            dispatch_commands.update_one(
                {"command_id": state["dispatch_command_id"]},
                {"$set": {"status": "en_route", "route_started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}},
            )
        return True
    except (requests.RequestException, ValueError, KeyError, IndexError, TypeError) as error:
        clear_route(state)
        state["route_error"] = str(error)[:180]
        state["route_retry_at"] = now + route_retry_seconds
        return False


def complete_vehicle_route(state):
    clear_route(state)
    state["speed_kmh"] = 0.0
    if state["status"] == "occupied":
        state["status"] = "available"
        state["pickup_zone"] = state["dropoff_zone"]
        state["destination_latitude"] = state["latitude"]
        state["destination_longitude"] = state["longitude"]
        state["dispatch_status"] = "idle"
    else:
        state["dispatch_status"] = "arrived"
        state["pickup_zone"] = state["dispatch_target_zone"] or state["pickup_zone"]
        dispatch_commands.update_one(
            {"command_id": state["dispatch_command_id"]},
            {"$set": {"status": "arrived", "arrived_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}},
        )


def process_dispatch_commands():
    states_by_id = {state["vehicle_id"]: state for state in vehicle_states}
    while True:
        command = dispatch_commands.find_one_and_update(
            {"status": "approved"},
            {"$set": {"status": "routing", "claimed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}},
            sort=[("created_at", 1)],
        )
        if not command:
            return
        state = states_by_id.get(command.get("vehicle_id"))
        if not state or state["status"] != "available" or state["dispatch_status"] in {"routing", "en_route"}:
            dispatch_commands.update_one(
                {"command_id": command["command_id"]},
                {"$set": {"status": "rejected", "error": "Xe không còn rảnh để nhận lệnh"}},
            )
            continue
        try:
            target_latitude = float(command["target_latitude"])
            target_longitude = float(command["target_longitude"])
            target_zone = str(command["target_zone"])
            if (
                not target_zone.isdigit()
                or not math.isfinite(target_latitude)
                or not math.isfinite(target_longitude)
                or not -90 <= target_latitude <= 90
                or not -180 <= target_longitude <= 180
            ):
                raise ValueError("Invalid dispatch destination")
        except (KeyError, TypeError, ValueError):
            dispatch_commands.update_one(
                {"command_id": command["command_id"]},
                {"$set": {"status": "rejected", "error": "Điểm đến của lệnh điều phối không hợp lệ"}},
            )
            continue
        state["route_start_latitude"] = state["latitude"]
        state["route_start_longitude"] = state["longitude"]
        state["destination_latitude"] = target_latitude
        state["destination_longitude"] = target_longitude
        state["dropoff_zone"] = target_zone
        state["dispatch_target_zone"] = target_zone
        state["dispatch_command_id"] = command["command_id"]
        state["dispatch_status"] = "routing"
        clear_route(state)
        state["route_retry_at"] = 0.0
        state["route_error"] = None
        print(f"[APPROVED] {state['vehicle_id']} -> zone {state['dispatch_target_zone']}", flush=True)


def advance_vehicle(state, elapsed_seconds):
    status = state["status"]
    dispatch_status = state["dispatch_status"]
    if status == "available" and dispatch_status not in {"routing", "en_route"}:
        state["speed_kmh"] = 0.0
        return 0.0, 0.0, 0.0, 100.0 if dispatch_status == "arrived" else 0.0
    speed_kmh = 28 + (state["vehicle_index"] * 7 + state["trip_number"]) % 24 if status == "occupied" else 14 + state["vehicle_index"] % 10
    now = time.monotonic()
    if not state["route_coordinates"] and not request_drive_route(state, now):
        state["speed_kmh"] = 0.0
        return 0.0, state["route_remaining_m"] / 1000, 0.0, 0.0

    coordinates = state["route_coordinates"]
    remaining_movement = speed_kmh * elapsed_seconds / 3.6
    index = state["route_segment_index"]
    while index < len(coordinates) - 1:
        segment_length = route_distance_m(coordinates[index], coordinates[index + 1])
        available = max(0.0, segment_length - state["route_segment_progress_m"])
        if remaining_movement < available:
            state["route_segment_progress_m"] += remaining_movement
            ratio = state["route_segment_progress_m"] / max(segment_length, 0.001)
            lon = coordinates[index][0] + (coordinates[index + 1][0] - coordinates[index][0]) * ratio
            lat = coordinates[index][1] + (coordinates[index + 1][1] - coordinates[index][1]) * ratio
            state["latitude"], state["longitude"] = lat, lon
            remaining_movement = 0
            break
        remaining_movement -= available
        index += 1
        state["route_segment_index"] = index
        state["route_segment_progress_m"] = 0.0
        state["latitude"], state["longitude"] = coordinates[index][1], coordinates[index][0]
    if index >= len(coordinates) - 1:
        complete_vehicle_route(state)
        return 0.0, 0.0, 0.0, 100.0

    state["route_remaining_m"] = sum(
        route_distance_m(coordinates[position], coordinates[position + 1])
        for position in range(index, len(coordinates) - 1)
    ) - state["route_segment_progress_m"]
    next_point = coordinates[index + 1]
    heading = math.degrees(math.atan2(next_point[0] - state["longitude"], next_point[1] - state["latitude"]))
    progress = max(0, min(100, (1 - state["route_remaining_m"] / max(state["route_distance_m"], 1)) * 100))
    return speed_kmh, state["route_remaining_m"] / 1000, heading, progress


def send_vehicle_snapshot(cycle_number):
    global last_snapshot_at
    process_dispatch_commands()
    now = time.monotonic()
    elapsed_seconds = 0 if last_snapshot_at is None else max(0.5, now - last_snapshot_at)
    last_snapshot_at = now
    updated_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    events = []
    for state in vehicle_states:
        speed_kmh, remaining_km, heading, progress = advance_vehicle(state, elapsed_seconds)
        state["speed_kmh"] = round(speed_kmh, 1)
        if state["status"] == "occupied":
            route_status = "Đang hoàn tất chuyến mô phỏng có khách"
        elif state["dispatch_status"] == "routing":
            route_status = "Đã duyệt · đang tạo tuyến đường"
        elif state["dispatch_status"] == "en_route":
            route_status = f"Đang tới zone {state['dispatch_target_zone']} · đã được duyệt"
        elif state["dispatch_status"] == "arrived":
            route_status = f"Đã tới zone {state['dispatch_target_zone']} theo lệnh điều phối"
        elif state["route_error"]:
            route_status = "Đang chờ thử lại tuyến đường"
        else:
            route_status = "Rảnh · chờ người điều phối"
        events.append(
            {
                "vehicle_id": state["vehicle_id"],
                "status": state["status"],
                "route_status": route_status,
                "dispatch_status": state["dispatch_status"],
                "dispatch_command_id": state["dispatch_command_id"],
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
                "route_source": "OpenStreetMap via OSRM" if state["route_coordinates"] else None,
                "route_error": state["route_error"],
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
            # Spark parses this wall-clock ISO value using the configured
            # America/New_York session timezone shared with training features.
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
