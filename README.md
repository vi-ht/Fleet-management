# Taxi Hotspot Batch + Streaming

Đây là pipeline batch + streaming cho dự báo điểm nóng taxi: batch tạo dữ liệu sạch và model artifact một lần, còn streaming chỉ load artifact để chấm điểm hotspot và ghi MongoDB. Phạm vi nghiệp vụ tập trung vào phát hiện điểm nóng và điều phối taxi; không xử lý giá cước/doanh thu.

## Chạy toàn bộ

```bash
docker compose up --build
```

Sau khi stack khởi động:

- Spark UI: http://localhost:8080
- HDFS NameNode UI: http://localhost:9870
- Dashboard/API: http://localhost:8088
- Dashboard health: http://localhost:8088/health

Nếu cổng Spark UI `8080` đã được ứng dụng khác sử dụng, chọn cổng host khác trước khi chạy stack (ví dụ PowerShell):

```powershell
$env:SPARK_UI_PORT="18080"
docker compose up --build
```

Khi đó Spark UI ở `http://localhost:18080`; cổng bên trong container vẫn giữ nguyên.

Các stage one-shot sẽ hiện các log `[PASS]`: HDFS, MapReduce, ETL/Parquet, model và Kafka topic. `streaming-predictor`, `kafka-producer` và `dashboard` tiếp tục chạy; producer lặp lại dataset để mô phỏng luồng realtime cho đến khi bạn dừng bằng `docker compose stop kafka-producer`.

## Artifact và chạy lại

- Model: `models/hotspot_model/`
- Chỉ số đánh giá model holdout (RMSE/MAE): `data/results/model_metrics.json`
- Checkpoint realtime: `checkpoints/taxi-hotspot-realtime-v4/`
- Dữ liệu local: `data/raw`, `data/curated`, `data/results`
- HDFS: `/taxi/raw` và `/taxi/curated/trips`
- MongoDB collection: `taxi.hotspot_predictions`
- Kafka topic realtime: `taxi_trips_realtime_v2`

Những lần `docker compose up` sau sẽ bỏ qua train nếu model artifact đã tồn tại. Chủ động train lại:

```bash
docker compose run --rm -e FORCE_TRAIN=true model-trainer
```

Stage train lấy các chuyến taxi từ nguồn đang được ETL đưa vào HDFS, nhóm theo zone/giờ/thứ trong tuần, rồi chia 80% train / 20% holdout (seed cố định). RMSE và MAE được tính trên thang hotspot 0–100 và lưu tại `data/results/model_metrics.json`; dashboard hiển thị hai chỉ số này sau khi artifact metrics được tạo.

Batch cũng bỏ qua MapReduce/ETL nếu artifact đã có. Khi thay bộ Parquet nguồn, chạy lại có chủ đích:

```bash
docker compose run --rm -e FORCE_BATCH=true mapreduce-job
docker compose run --rm -e FORCE_ETL=true spark-etl
docker compose run --rm -e FORCE_TRAIN=true model-trainer
```

Nếu muốn reset toàn bộ dữ liệu demo, dừng stack rồi xóa các thư mục `models/`, `checkpoints/`, `data/results/` và dùng `docker compose down -v` để xóa named volumes HDFS/Kafka/MongoDB. Đây là thao tác destructive.

## Lưu ý phạm vi và nguồn dữ liệu

- **Batch và model:** mặc định đọc 7 file NYC TLC Yellow Taxi Parquet đã có trong workspace tại `Nyc taxi trip record/Nyc taxi trip record/` (mẫu `yellow_tripdata_*.parquet`). Đây là dữ liệu chuyến taxi Yellow Taxi do NYC Taxi & Limousine Commission công bố; các cột chính pipeline dùng là `tpep_pickup_datetime`, `PULocationID`, `DOLocationID`. MapReduce đếm chuyến theo zone/giờ, Spark ETL tạo bảng curated, và model học điểm hotspot tương đối từ các chuyến đó.
- **Lấy bản gốc:** tải Yellow Taxi Trip Records từ [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) hoặc [NYC TLC trên AWS Open Data](https://registry.opendata.aws/nyc-tlc-trip-records/), rồi đặt các Parquet `yellow_tripdata_*.parquet` vào thư mục trên. Project không tự tải toàn bộ dataset khi khởi động. Muốn dùng bộ khác, đặt file ở đó và chạy lại batch, ETL, rồi train bằng các lệnh `FORCE_BATCH`, `FORCE_ETL`, `FORCE_TRAIN` bên trên.
- **Fallback:** nếu không có Parquet TLC trong thư mục, MapReduce tạo `data/raw/taxi_trips.csv` với dữ liệu deterministic tổng hợp (mặc định 10.000 dòng). Dữ liệu fallback mô phỏng schema TLC, không phải chuyến đi thật. Có thể đặt CSV của bạn tại `data/raw/taxi_trips.csv`; generator giữ nguyên file đã tồn tại. Nguồn CSV fallback này cũng được dùng cho realtime replay.
- **Realtime replay:** producer phát tối đa `SIMULATION_SOURCE_ROWS` dòng từ cùng bộ Parquet TLC; nếu không có thì đọc CSV fallback trong `data/raw/taxi_trips.csv`. Timestamp lịch sử được thay bằng simulation clock hiện tại (UTC+7), vì vậy đây là phát lại tăng tốc, không phải live feed.
- **Vị trí và trạng thái xe:** 40 xe, tuyến di chuyển, tốc độ và ETA do simulator tạo ra; chúng không đến từ GPS hoặc dữ liệu định vị NYC TLC. Bản đồ dùng OpenStreetMap để hiển thị nền.

Tham khảo cấu trúc cột tại [Yellow Taxi Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) và zone tại [Taxi Zone Lookup CSV](https://d37ci6vzury5cl.cloudfront.net/misc/taxi_zone_lookup.csv).

## Ảnh chụp và evidence

- Ảnh dashboard đầu ra: [`output/playwright/dashboard-output.png`](output/playwright/dashboard-output.png).
- Ảnh chi tiết bản đồ và vùng hotspot: [`output/playwright/map-hotspots.png`](output/playwright/map-hotspots.png).
- Tóm tắt xác minh đã lưu: [`output/playwright/verification-summary.txt`](output/playwright/verification-summary.txt). Lần ghi nhận này xác nhận dashboard/API, hotspot, forecast, đội xe chuyển động, model, MongoDB và 7 Parquet trên HDFS.
- Để thu lại log và JSON chi tiết từ stack đang chạy, dùng `scripts/collect_evidence.ps1`; script lưu kết quả cục bộ trong `evidence/`.

`analytics.py` và `features.py` là các entrypoint mở rộng cho phân tích/feature engineering; pipeline mặc định đã thực hiện feature engineering trong `train_model.py`.

## Realtime simulation theo schema NYC TLC

Batch ưu tiên dùng các file NYC TLC Parquet đặt tại `Nyc taxi trip record/Nyc taxi trip record`: MapReduce quét `PULocationID` theo giờ, HDFS lưu Parquet gốc, và Spark ETL đọc trực tiếp Parquet để tạo dữ liệu curated. Nếu thư mục không có, pipeline tự fallback về CSV mô phỏng deterministic. Producer lấy tối đa `SIMULATION_SOURCE_ROWS` bản ghi từ cùng nguồn Parquet để phát sự kiện realtime; ngoài hotspot event, producer còn phát snapshot 40 xe vào topic `taxi_vehicles`, `fleet-tracker` đọc topic này và lưu trạng thái hiện tại vào collection `taxi.vehicle_status` để dashboard hiển thị bản đồ và danh sách xe.

Dashboard tiếng Việt tại `http://localhost:8088/` có điều hướng, filter pickup zone, bản đồ OpenStreetMap, ranh giới zone mô phỏng, marker xe đang chạy, điểm nóng theo thang 0–100, ranking khu vực, điều phối taxi và forecast điểm nóng 3 giờ kế tiếp. Marker được nội suy giữa các snapshot Kafka để chuyển động liền mạch; producer chạy lặp liên tục và dừng bằng `docker compose stop kafka-producer`.

Producer dùng simulation clock theo ngày/giờ hiện tại UTC+7, phát event đầu tiên tại thời điểm khởi động và tiến 1 giây mô phỏng cho mỗi event (có thể điều chỉnh bằng `SIMULATION_EVENT_TIME_STEP_SECONDS`). Snapshot đội xe phát mỗi 10 event; xe có tọa độ đích, hướng, tốc độ, tiến độ và ETA. Đây là mô phỏng realtime tăng tốc, không phải GPS xe thật.

Các ô ranh giới trên map là 260 zone hình lưới do simulator định nghĩa trong phạm vi vận hành NYC; zone có điểm nóng sẽ được tô màu và hover để xem hotspot score.

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

`spark/clean.py` hỗ trợ schema TLC (`tpep_pickup_datetime`, `PULocationID`, `DOLocationID`) và schema CSV chuẩn hóa cũ. Bộ zone lookup chính thức được tải vào `data/reference/` bằng lệnh sample ở trên; simulator có thể dùng danh sách zone đó cho dữ liệu tổng hợp.
