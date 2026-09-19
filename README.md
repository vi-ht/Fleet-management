# Taxi Demand Batch + Streaming

Đây là scaffold triển khai đúng lifecycle đã chốt: batch tạo dữ liệu sạch và model artifact một lần, còn streaming chỉ load artifact để inference và ghi MongoDB. Phạm vi nghiệp vụ tập trung vào phát hiện điểm nóng nhu cầu và điều phối taxi; không xử lý giá cước/doanh thu.

## Chạy toàn bộ

```bash
docker compose up --build
```

Sau khi stack khởi động:

- Spark UI: http://localhost:8080
- HDFS NameNode UI: http://localhost:9870
- Dashboard/API: http://localhost:8088
- Dashboard health: http://localhost:8088/health

Các stage one-shot sẽ hiện các log `[PASS]`: HDFS, MapReduce, ETL/Parquet, model và Kafka topic. `streaming-predictor`, `kafka-producer` và `dashboard` tiếp tục chạy; producer lặp lại dataset để mô phỏng luồng realtime cho đến khi bạn dừng bằng `docker compose stop kafka-producer`.

## Artifact và chạy lại

- Model: `models/gbt_demand_model/`
- Checkpoint: `checkpoints/taxi-demand/`
- Dữ liệu local: `data/raw`, `data/curated`, `data/results`
- HDFS: `/taxi/raw` và `/taxi/curated/trips`
- MongoDB collection: `taxi.demand_predictions`
- Kafka topic: `taxi_trips`

Những lần `docker compose up` sau sẽ bỏ qua train nếu model artifact đã tồn tại. Chủ động train lại:

```bash
docker compose run --rm -e FORCE_TRAIN=true model-trainer
```

Nếu muốn reset toàn bộ dữ liệu demo, dừng stack rồi xóa các thư mục `models/`, `checkpoints/`, `data/results/` và dùng `docker compose down -v` để xóa named volumes HDFS/Kafka/MongoDB. Đây là thao tác destructive.

## Lưu ý phạm vi

Dataset chưa được cung cấp trong workspace, nên batch bootstrap tạo dữ liệu taxi tổng hợp deterministic. Khi có CSV thật, đặt tại `data/raw/taxi_trips.csv` với các cột `event_id,pickup_datetime,pickup_zone,dropoff_zone`; generator sẽ không ghi đè file đó.

`analytics.py` và `features.py` là các entrypoint mở rộng cho phân tích/feature engineering; pipeline mặc định đã thực hiện feature engineering trong `train_model.py`.

## Realtime simulation theo schema NYC TLC

Simulator mặc định phát sinh dữ liệu theo các trường cần cho demand/dispatch của NYC TLC: `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `PULocationID`, `DOLocationID`. Producer replay các event chuẩn hóa vào Kafka. Ngoài demand event, producer còn phát snapshot 40 xe vào topic `taxi_vehicles`; `fleet-tracker` đọc topic này và lưu trạng thái hiện tại vào collection `taxi.vehicle_status` để dashboard hiển thị bản đồ và danh sách xe.

Dashboard tiếng Việt tại `http://localhost:8088/` có điều hướng, filter pickup zone, bản đồ OpenStreetMap, marker xe rảnh/đang có khách, biểu đồ demand, ranking khu vực, prediction table và auto-refresh. Producer chạy lặp liên tục; dừng bằng `docker compose stop kafka-producer`.

Điều chỉnh tốc độ hoặc số event:

```bash
$env:PRODUCER_DELAY_SECONDS="0.10"
$env:SIMULATION_MAX_EVENTS="1000"
docker compose up --build
```

Để lấy một sample chính thức và bảng zone lookup:

```bash
docker compose run --rm --no-deps --entrypoint python3 kafka-topic-init /workspace/scripts/fetch_tlc_sample.py
```

Lệnh trên lưu sample vào `data/reference/`. Simulator vẫn là dữ liệu deterministic để demo ổn định, nhưng dùng đúng tên cột và taxi-zone vocabulary của TLC; nếu file zone lookup tồn tại, các zone được lấy từ file đó.

Nguồn dữ liệu được dùng làm chuẩn:

- NYC TLC Trip Record Data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Yellow Taxi Data Dictionary: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
- Taxi Zone Lookup CSV: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
- NYC TLC dataset trên AWS Open Data: https://registry.opendata.aws/nyc-tlc-trip-records-pds/

Nếu đặt Parquet TLC thật vào pipeline, cần thêm adapter đọc Parquet hoặc chuyển nó thành CSV chuẩn hóa. `spark/clean.py` đã hỗ trợ cả schema TLC (`tpep_pickup_datetime`, `PULocationID`, `DOLocationID`) và schema chuẩn hóa cũ.
