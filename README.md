# Taxi Hotspot Batch + Streaming

Đây là prototype batch + streaming cho phân tích điểm nóng taxi và điều phối đội xe mô phỏng. Batch tạo dữ liệu curated và model artifact; streaming replay sự kiện, chấm hotspot bằng model và ghi kết quả vào MongoDB. Đánh giá model hiện đo khả năng dự đoán relative hotspot score trên các ngày chưa thấy, không phải số chuyến taxi tuyệt đối hay nhu cầu live. Phạm vi không xử lý giá cước/doanh thu.

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

Các stage one-shot báo `[PASS]` khi hoàn tất. `mapreduce-job` chuẩn bị TSV từ TLC Parquet và tạo baseline Counter độc lập; `hadoop-streaming` chạy mapper, combiner, shuffle và reducer trên HDFS rồi đối chiếu toàn bộ kết quả với baseline. Hadoop dùng LocalJobRunner trong Docker, chưa phải triển khai YARN đa node. `spark-etl` nối Taxi Zone Lookup vào curated Parquet. Chạy hai truy vấn Spark SQL và phép thử partition bằng `docker compose --profile analysis run --rm spark-analysis`. `streaming-predictor`, `kafka-producer` và `dashboard` tiếp tục chạy; producer lặp dataset để mô phỏng realtime cho đến khi dừng bằng `docker compose stop kafka-producer`.

## Kiến trúc end-to-end

```mermaid
flowchart LR
  TLC[NYC TLC Parquet / synthetic fallback] --> HDFS[HDFS raw]
  HDFS --> MR[Hadoop Streaming: zone-hour aggregation<br/>LocalJobRunner]
  HDFS --> ETL[Spark DataFrame ETL + Taxi Zone Lookup]
  MR --> HDFSOUT[HDFS aggregate + baseline validation]
  ETL --> CUR[Curated Parquet]
  CUR --> TRAIN[PySpark GBTRegressor + temporal holdout]
  TRAIN --> ART[Model + metrics + alert baseline]
  ART --> SPARK[Spark Structured Streaming scorer]
  REPLAY[Kafka historical replay] --> SPARK
  SPARK --> MONGO[(MongoDB predictions)]
  FLEET[Simulated fleet + OSRM routes] --> KVEH[Kafka vehicle topic]
  FLEET --> KTRIP[Kafka pickup/drop-off events]
  KVEH --> TRACK[Fleet tracker]
  KTRIP --> TRACK
  TRACK --> VEH[(MongoDB vehicle_status)]
  TRACK --> TRIPS[(MongoDB vehicle_trip_events)]
  UI -->|Human-approved dispatch| DC[(MongoDB vehicle_dispatch_commands)]
  DC --> FLEET
  MONGO --> UI[Dashboard / API / map]
  VEH --> UI

  CLOUD[Proposed: object storage + multi-worker Spark + replicated Kafka/Mongo + secret/metrics service] -. design only .-> CUR
```

Các thành phần Docker Compose và các đường liền là phần chạy local đã có evidence. Đường chấm là thiết kế mở rộng, chưa triển khai. Hadoop Streaming dùng LocalJobRunner một máy. Training và serving chỉ dùng pickup zone, giờ và thứ trong tuần.

## Artifact và chạy lại

- Model: `models/hotspot_model/`
- Chỉ số đánh giá model holdout (RMSE/MAE): `output/evidence/model_metrics.json`
- Checkpoint realtime: `checkpoints/taxi-hotspot-realtime-v4/`
- Dữ liệu local: `data/raw`, `data/curated`, `data/results`
- HDFS: `/taxi/raw` và `/taxi/curated/trips`
- MongoDB collection: `taxi.hotspot_predictions`
- Kafka topic realtime: `taxi_trips_realtime_v2`

Những lần `docker compose up` sau sẽ bỏ qua train nếu model artifact đã tồn tại. Chủ động train lại:

```bash
docker compose run --rm -e FORCE_TRAIN=true model-trainer
```

Stage train lấy dữ liệu curated từ HDFS, nhóm theo ngày, zone, giờ và thứ, rồi tạo nhãn hotspot score tương đối 0–100. GBTRegressor dùng ba đặc trưng: zone, giờ trong ngày và thứ trong tuần. Đánh giá temporal holdout giữ 80% ngày đầu làm train và 20% ngày cuối làm test. Ngưỡng bất thường được tính từ score của tập train theo zone/giờ/thứ: mean + 3 độ lệch chuẩn, yêu cầu ít nhất 5 mẫu. MAE, RMSE, R², Precision/Recall, TP/FP/FN và cách chia tập được lưu tại `output/evidence/model_metrics.json`; baseline cảnh báo ở `models/historical_baseline.json`.

Lần train calendar-only ngày 04/10/2026 có 635.798 dòng train trên 178 ngày có bản ghi (mốc min/max 2001-01-01–2026-06-20) và 132.249 dòng test trên 45 ngày (2026-06-21–2026-08-05). 178 ngày là số ngày dữ liệu theo metric, không khẳng định dữ liệu liên tục trong toàn khoảng min/max. GBTRegressor đạt MAE 7,348; RMSE 11,066; R² 0,655. Tại score ≥ 50, Precision 88,03%, Recall 30,79% (TP=2.360, FP=321, FN=5.305). Các metric đo relative hotspot score, không phải số chuyến; Recall thấp nên còn bỏ sót nhiều trường hợp nóng. Chi tiết ở `output/evidence/model_metrics.json`.

**Hướng cải thiện cần đánh giá tiếp:** không nên gọi R² là “độ chính xác”, vì R² không phải accuracy. Có thể thử lag demand theo zone (ví dụ 1 giờ và 24 giờ trước) và lịch ngày lễ/sự kiện NYC. Tạo đặc trưng chỉ từ thời điểm dự báo trở về trước, dùng cùng temporal holdout hoặc rolling-origin backtest, và báo MAE/RMSE/R² cùng Precision/Recall hoặc Recall@top-k tại cùng ngân sách điều phối. Chỉ thay model đang deploy nếu kết quả ngoài mẫu cải thiện và đặc trưng mới có thể tạo đồng nhất ở batch lẫn serving.

## Điều phối xe có người duyệt

Dashboard chỉ đưa ra danh sách xe rảnh và các zone nóng để tham khảo. Người vận hành chọn điểm đến rồi bấm **Duyệt điều xe**; API ghi lệnh vào MongoDB `taxi.vehicle_dispatch_commands`, producer nhận lệnh và chỉ khi đó mới gọi OSRM tạo tuyến đường lái xe. Lệnh có trạng thái `approved`, `routing`, `en_route`, `arrived` hoặc `rejected` để dashboard theo dõi. Xe rảnh đứng yên nếu chưa có lệnh được duyệt; sau khi tới đích, xe tiếp tục chờ người điều phối. Các chuyến mô phỏng đang có khách vẫn tự chạy tới điểm trả khách và kết thúc ở đó.

Đây là human-in-the-loop cho **xe mô phỏng**, không điều khiển taxi ngoài dự án. Toàn bộ lệnh được ghi lại để truy vết; nếu simulator chưa chạy, lệnh vẫn ở trạng thái `approved` chờ producer nhận.

Trên bản đồ, hover vào một Taxi Zone để xem xe rảnh đang ở đó, các xe có lệnh đã duyệt và trạng thái `en_route` đang tới đó, cùng mã xe. Tooltip cũng hiển thị số xe mục tiêu ước tính theo quy tắc demo `ceil(hotspot_score / 25)` và số xe cần điều thêm sau khi trừ xe rảnh tại zone và xe điều phối đang tới. Đây là heuristic giao diện dựa trên relative score 0–100; model không dự báo số lượng xe cần. Người điều phối vẫn tự chọn xe/đích và duyệt lệnh.

Batch cũng bỏ qua MapReduce/ETL nếu artifact đã có. Khi thay bộ Parquet nguồn, chạy lại có chủ đích:

```bash
docker compose run --rm -e FORCE_BATCH=true mapreduce-job
docker compose run --rm --no-deps -e FORCE_BATCH=true hadoop-streaming
docker compose run --rm -e FORCE_ETL=true spark-etl
docker compose run --rm -e FORCE_TRAIN=true model-trainer
```

Nếu muốn reset toàn bộ dữ liệu demo, dừng stack rồi xóa các thư mục `models/`, `checkpoints/`, `data/results/` và dùng `docker compose down -v` để xóa named volumes HDFS/Kafka/MongoDB. Đây là thao tác destructive.

## Lưu ý phạm vi và nguồn dữ liệu

- **Batch và model:** mặc định đọc 7 file NYC TLC Yellow Taxi Parquet đã có trong workspace tại `Nyc taxi trip record/Nyc taxi trip record/` (mẫu `yellow_tripdata_*.parquet`). Đây là dữ liệu chuyến taxi Yellow Taxi do NYC Taxi & Limousine Commission công bố; các cột chính pipeline dùng là `tpep_pickup_datetime`, `PULocationID`, `DOLocationID`. Script `scripts/run_mapreduce.sh` tạo đầu vào TSV zone/giờ trên HDFS và Counter baseline độc lập. `hadoop-streaming` chạy Bash/awk mapper, combiner và reducer qua Hadoop Streaming, dùng shuffle của Hadoop; kết quả được so sánh đầy đủ với baseline. Job đã xử lý 26.116.976 chuyến thành 6.155 nhóm zone/giờ. Runner là LocalJobRunner một node, không phải YARN cluster. Spark ETL broadcast left join lookup 265 dòng cho pickup/dropoff; sau join vẫn 26.116.976 dòng, không có ID unmatched.
- **Lấy bản gốc:** tải Yellow Taxi Trip Records từ [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) hoặc [NYC TLC trên AWS Open Data](https://registry.opendata.aws/nyc-tlc-trip-records/), rồi đặt các Parquet `yellow_tripdata_*.parquet` vào thư mục trên. Project không tự tải toàn bộ dataset khi khởi động. Muốn dùng bộ khác, đặt file ở đó và chạy lại batch, ETL, rồi train bằng các lệnh `FORCE_BATCH`, `FORCE_ETL`, `FORCE_TRAIN` bên trên.
- **Fallback:** nếu không có Parquet TLC trong thư mục, batch tạo `data/raw/taxi_trips.csv` với dữ liệu deterministic tổng hợp (mặc định 10.000 dòng). Dữ liệu fallback mô phỏng schema TLC, không phải chuyến đi thật. Có thể đặt CSV của bạn tại `data/raw/taxi_trips.csv`; generator giữ nguyên file đã tồn tại. Nguồn CSV fallback này cũng được dùng cho realtime replay.
- **Realtime replay:** producer phát tối đa `SIMULATION_SOURCE_ROWS` dòng từ cùng bộ Parquet TLC; nếu không có thì đọc CSV fallback trong `data/raw/taxi_trips.csv`. Timestamp lịch sử được thay bằng simulation clock hiện tại theo America/New_York, vì vậy đây là phát lại tăng tốc, không phải live feed.
- **Ranh giới bản đồ:** `dashboard/static/taxi_zones.geojson` chứa 263 polygon parts ứng với 260 LocationID duy nhất (một số zone có nhiều phần rời), cùng tên zone và borough. Ranh giới là dữ liệu địa lý NYC TLC được phân phối ở dạng GeoJSON; tham chiếu bộ gốc tại [NYC Taxi Zones](https://catalog.data.gov/dataset/nyc-taxi-zones) và bản chuyển đổi được dùng để đóng gói tại [nyc-taxi-map](https://github.com/chkp-fernandom/nyc-taxi-map/blob/master/data/zones.geojson). Bản đồ chỉ tô vùng theo LocationID; không suy ra ranh giới chính xác hơn dữ liệu nguồn.
- **Vị trí và trạng thái xe:** 40 xe, tốc độ và trạng thái do simulator tạo ra; chúng không đến từ GPS hoặc dữ liệu định vị NYC TLC. Xe rảnh chờ người vận hành duyệt điểm đến; sau khi duyệt, OSRM tạo tuyến trên dữ liệu đường OpenStreetMap và xe đi dọc hình học tuyến. Chuyến đang có khách được mô phỏng tự động tới điểm trả khách. Dịch vụ định tuyến cần kết nối Internet; nếu không lấy được tuyến, xe đứng yên và thử lại, không tự đi đường thẳng. Có thể đổi endpoint bằng `ROUTING_BASE_URL`, thời gian thử lại bằng `ROUTE_RETRY_SECONDS` và giới hạn nhịp gọi bằng `ROUTE_REQUEST_INTERVAL_SECONDS`.

Tham khảo cấu trúc cột tại [Yellow Taxi Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) và zone tại [Taxi Zone Lookup CSV](https://d37ci6vzury5cl.cloudfront.net/misc/taxi_zone_lookup.csv).

## Ảnh chụp và evidence

- Dashboard gọn ở desktop, có metric temporal holdout và kết quả Kafka replay mới nhất ngay trên đầu trang: [`output/playwright/dashboard-stream-results-2026-10-04.png`](output/playwright/dashboard-stream-results-2026-10-04.png).
- Bản đồ và danh sách xe mô phỏng tại lần kiểm tra runtime: [`output/playwright/dashboard-map-stream-current-2026-10-04.png`](output/playwright/dashboard-map-stream-current-2026-10-04.png). Bản chụp này ghi rõ xe rảnh chờ lệnh điều phối được duyệt.
- Giao diện 375 px, gồm kết quả stream và metric model: [`output/playwright/dashboard-stream-mobile-2026-10-04.png`](output/playwright/dashboard-stream-mobile-2026-10-04.png).
- Dashboard calendar-only, polygon Taxi Zone, và các nút duyệt điều phối thủ công: [`output/playwright/dashboard-calendar-only-2026-10-04.png`](output/playwright/dashboard-calendar-only-2026-10-04.png).
- Bản đồ score theo giờ, xe mô phỏng và polygon Taxi Zone: [`output/playwright/map-calendar-only-2026-10-04.png`](output/playwright/map-calendar-only-2026-10-04.png).
- Tooltip khi hover zone, gồm score, xe rảnh tại chỗ, mã xe điều phối đang tới và số xe cần điều thêm ước tính: [`output/playwright/map-hover-vehicle-need-2026-10-04.png`](output/playwright/map-hover-vehicle-need-2026-10-04.png).
- Ảnh mobile 375 px: [`output/playwright/mobile-calendar-only-2026-10-04.png`](output/playwright/mobile-calendar-only-2026-10-04.png).
- Ảnh chụp kiểm tra live ở 375/768/1440 px: [`browser-qa-375.png`](output/playwright/browser-qa-375.png), [`browser-qa-768.png`](output/playwright/browser-qa-768.png), [`browser-qa-1440.png`](output/playwright/browser-qa-1440.png); nav đã tự xuống hàng ở tablet/mobile và không tràn ngang. Kết quả API, điều hướng và filter: [`output/evidence/browser_qa_summary_2026-10-04.json`](output/evidence/browser_qa_summary_2026-10-04.json).
- Ảnh console/runtime: [`output/playwright/console-log-evidence-2026-10-04.png`](output/playwright/console-log-evidence-2026-10-04.png). Snapshot API/fleet có trong [`output/evidence/runtime_capture_2026-10-04.json`](output/evidence/runtime_capture_2026-10-04.json) và [`output/evidence/fleet_movement_capture_2026-10-04.json`](output/evidence/fleet_movement_capture_2026-10-04.json).
- Báo cáo nộp đã đồng bộ sơ đồ kiến trúc, dashboard stream, metrics temporal holdout và lưu ý provenance ngày dữ liệu: [`output/documents/BDA501_Final_Project_Report_Submission.docx`](output/documents/BDA501_Final_Project_Report_Submission.docx). Kiểm tra render hình chưa hoàn tất: môi trường thiếu `pdf2image` và không có LibreOffice/`soffice`; xem [`output/evidence/docx_render_check.json`](output/evidence/docx_render_check.json). Trước khi nộp, điền thông tin nhóm/thành viên, tỷ lệ đóng góp và CLO chính thức.
- Nội dung cần đồng bộ với deck Canva Session 10: [`output/documents/Session10_Presentation_Replacement_Copy.md`](output/documents/Session10_Presentation_Replacement_Copy.md) có copy thay thế theo từng slide; [`output/documents/Session10_Presentation_Corrections.md`](output/documents/Session10_Presentation_Corrections.md) ghi chi tiết lỗi và căn cứ đối chiếu. Deck Canva gốc chưa bị thay đổi.
- Kết quả train GBTRegressor calendar-only: [`output/evidence/model_metrics.json`](output/evidence/model_metrics.json). Model được đánh giá bằng temporal holdout.
- Hadoop Streaming log, HDFS output và so sánh với baseline độc lập: [`output/evidence/hadoop_streaming.log`](output/evidence/hadoop_streaming.log), [`output/evidence/mapreduce_actual.csv`](output/evidence/mapreduce_actual.csv), [`output/evidence/mapreduce_expected.csv`](output/evidence/mapreduce_expected.csv). Job local đã xử lý 26.116.976 input records thành 6.155 nhóm; đây không phải benchmark YARN đa node.
- Kết quả hai truy vấn Spark SQL và formatted plans: [`output/evidence/spark_query_results.json`](output/evidence/spark_query_results.json), [`output/evidence/spark_query_plans.txt`](output/evidence/spark_query_plans.txt); truy vấn top zone trả 20 dòng, profile borough/hour trả 192 dòng.
- Benchmark shuffle partition: [`output/evidence/performance_benchmark.json`](output/evidence/performance_benchmark.json). Trên sample cache 100.000 dòng, 13.725 nhóm đầu ra trùng khớp; median 4 partitions là 0,272 giây và 16 partitions là 0,238 giây. Đây là phép đo cục bộ một máy, không chứng minh hiệu năng cluster.
- Data profile và snapshot HDFS, Kafka, MongoDB, fleet/dashboard được lưu trong [`output/evidence/`](output/evidence/). SHA-256 cho các snapshot, ảnh giao diện và báo cáo được liệt kê tại [`output/evidence/artifact_checksums.json`](output/evidence/artifact_checksums.json).
- Dữ liệu hotspot hiển thị trên bản đồ: [`output/playwright/dispatch-hotspots.json`](output/playwright/dispatch-hotspots.json), gồm điểm trung bình và số bản ghi suy luận theo zone.
- Data profile ghi nhận 26.116.976 dòng raw/curated; log Spark ETL xác nhận lookup 265 zone, row count trước/sau join không đổi và 0 pickup/dropoff ID unmatched. Model dùng dữ liệu tổng hợp zone/ngày/giờ có ngày hợp lệ trong curated dataset. Log: [`output/evidence/spark_etl_enrichment.log`](output/evidence/spark_etl_enrichment.log).
- Data profile ghi min pickup timestamp 2001-01-01; tổng cộng chỉ có 31 dòng trước 2025 (1 dòng năm 2001, 4 năm 2008, 5 năm 2009, 21 năm 2024). Tên 7 Parquet đầu vào trong workspace là yellow_tripdata_2025-01 và yellow_tripdata_2026-02 đến 2026-07. Model có 178 ngày train và 45 ngày test xuất hiện trong dữ liệu; các mốc min/max không có nghĩa dữ liệu liên tục trong khoảng đó. Cần xác minh provenance/nội dung file so với tên file; không kết luận bộ dữ liệu liên tục từ 2001 hoặc tự lọc các dòng sớm khi chưa có căn cứ.
- Cảnh báo bất thường dùng mean + 3σ trên relative hotspot score; score và threshold cùng bị chặn tối đa 100. Vì vậy, nếu threshold của một zone/giờ/thứ chạm 100 thì score không thể vượt ngưỡng để tạo `CRITICAL_ANOMALY`. Đây là cờ thống kê, không xác định nguyên nhân ngoại cảnh.
- Dependency audit của các package cài qua `requirements.txt`: [`output/evidence/dependency_audit.json`](output/evidence/dependency_audit.json); lần kiểm tra 04/10/2026 không phát hiện lỗ hổng đã biết.
- Để thu lại bằng chứng từ stack đang chạy, dùng `scripts/collect_evidence.ps1`, sau đó chạy `python scripts/package_evidence.py` để đóng gói các snapshot đã chọn và tạo lại manifest SHA-256.

Feature engineering mặc định được thực hiện trong `spark/train_model.py`; các stage và lệnh chạy lại được liệt kê ở trên.

## Realtime simulation theo schema NYC TLC

Batch ưu tiên dùng các file NYC TLC Parquet đặt tại `Nyc taxi trip record/Nyc taxi trip record`: bước chuẩn bị tạo TSV pickup-zone/hour và Counter baseline độc lập; HDFS lưu raw Parquet cùng TSV; Hadoop Streaming thực thi mapper/combiner/reducer bằng LocalJobRunner; Spark ETL đọc Parquet, nối zone lookup và tạo curated dataset. Nếu thư mục không có, pipeline tự fallback về CSV mô phỏng deterministic. Producer lấy tối đa `SIMULATION_SOURCE_ROWS` bản ghi từ cùng nguồn Parquet để chấm nhu cầu. Riêng đội xe phát snapshot 40 xe vào `taxi_vehicles` và event nhận/trả khách vào `taxi_vehicle_trip_events`; fleet tracker lưu snapshot hiện tại ở `taxi.vehicle_status` và lịch sử event ở `taxi.vehicle_trip_events`.

Dashboard tiếng Việt tại `http://localhost:8088/` có bộ lọc pickup zone, bản đồ OpenStreetMap, ranking replay, forecast ba giờ, điều phối cần người duyệt và bảng 20 event nhận/trả khách mô phỏng. Sau khi người điều phối duyệt xe tới zone đón, simulator tạo khách mô phỏng tại điểm đến, phát `passenger_pickup`, chạy theo tuyến OSRM tới zone trả, phát `passenger_dropoff`, rồi mới chuyển xe về trạng thái rảnh. Xe đang có khách không được điều phối; chuyến lịch sử dùng để chấm nhu cầu vẫn tách biệt với hành trình đội xe. Các event mô phỏng không đại diện hành khách ngoài đời. Producer chạy liên tục và dừng bằng `docker compose stop kafka-producer`.

**Vì sao một zone nóng:** batch đếm chuyến đón theo zone, ngày, giờ và thứ trong tuần. Score = (số chuyến zone ÷ số chuyến cao nhất giữa các zone trong cùng ngày/giờ/thứ) × 100. GBTRegressor dự báo score tương đối từ zone, giờ, thứ; không dùng KMeans. Streaming chấm sự kiện replay; map forecast chấm từng LocationID cho giờ tương lai. Điểm hotspot không phải số chuyến thực tế hay nhu cầu live.

Producer dùng simulation clock theo America/New_York, phát event chuyến lịch sử tăng tốc theo `SIMULATION_EVENT_TIME_STEP_SECONDS`. Đội xe dùng đồng hồ riêng mặc định nhanh gấp 12 lần thời gian thực (`SIMULATION_VEHICLE_TIME_SCALE`), để hành trình mô phỏng có thể quan sát được trong lúc demo. Snapshot đội xe phát định kỳ; xe có tọa độ đích, hướng, tốc độ, tiến độ và ETA. Xe rảnh không tự chọn điểm đón: người điều phối chọn zone và xác nhận; lệnh được lưu trong MongoDB rồi producer lấy để gọi OSRM Route API với hồ sơ `driving`. Khi tới zone đón, khách mô phỏng được nhận; xe tiếp tục theo tuyến đường tới zone trả và chỉ chuyển sang rảnh sau event trả khách. Event chi tiết được lưu ở `taxi.vehicle_trip_events`. Đây là mô phỏng, không phải GPS taxi hay yêu cầu khách thật. Polygon bản đồ dựa trên bộ ranh giới NYC TLC Taxi Zone; màu theo score forecast của từng LocationID cho giờ được chọn.

Điều chỉnh tốc độ hoặc số event:

```bash
$env:PRODUCER_DELAY_SECONDS="0.10"
$env:SIMULATION_MAX_EVENTS="1000"
docker compose up --build
```

Để lấy một sample chính thức và bảng zone lookup cho simulator:

```bash
docker compose run --rm --no-deps --entrypoint python3 kafka-topic-init /workspace/scripts/fetch_tlc_sample.py
```

Lệnh trên lưu sample vào `data/reference/`. Simulator vẫn là dữ liệu deterministic để demo ổn định, nhưng dùng đúng tên cột và taxi-zone vocabulary của TLC; nếu file zone lookup tồn tại, các zone được lấy từ file đó.

Nguồn dữ liệu được dùng làm chuẩn:

- NYC TLC Trip Record Data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Yellow Taxi Data Dictionary: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
- Taxi Zone Lookup CSV: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
- NYC TLC dataset trên AWS Open Data: https://registry.opendata.aws/nyc-tlc-trip-records-pds/

`spark/clean.py` hỗ trợ schema TLC (`tpep_pickup_datetime`, `PULocationID`, `DOLocationID`) và schema CSV chuẩn hóa cũ. Bộ zone lookup chính thức được lưu tại `data/reference/taxi_zone_lookup.csv`; Spark ETL đã nối tên zone, borough và service zone vào các trường pickup/dropoff của curated dataset.
