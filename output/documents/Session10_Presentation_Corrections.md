# Rà soát nội dung deck Canva Session 10

Đối chiếu chỉ đọc deck Canva 22 trang với source code, README, evidence và báo cáo tại ngày 04/10/2026. Đây là checklist nội dung để áp dụng vào deck; bản Canva gốc hiện chưa được sửa.

## Các lỗi ưu tiên sửa

| Phần trong deck hiện tại | Vấn đề | Nội dung cần thay |
|---|---|---|
| Bìa: “Hệ thống điều phối xe taxi theo thời gian thực”, năm “2030”, hai ngày trình bày khác nhau | Mô tả như dịch vụ live; deck có ngày/năm xung đột | “Prototype phân tích hotspot taxi từ dữ liệu TLC, replay lịch sử và mô phỏng đội xe”; xóa năm/ngày cũ, điền ngày được xác nhận |
| Đặt vấn đề: “tự động điều xe … chờ sẵn” | Hệ thống không dispatch taxi thật; chỉ điều xe mô phỏng sau khi người vận hành chọn đích và xác nhận | “Cung cấp hotspot score và đề xuất điều phối; lệnh cho đội xe mô phỏng cần người vận hành duyệt” |
| Big Data/kiến trúc: “chia nhỏ … trên nhiều máy”, “độ trễ thấp” | Bản chạy dùng Docker local; Hadoop là LocalJobRunner, chưa phải cluster đa node | Nêu Hadoop Streaming LocalJobRunner một máy và Spark local/Compose; chuyển kiến trúc nhiều worker sang phần thiết kế mở rộng |
| Phạm vi: GBT dự báo “số lượng nhu cầu” | Target là điểm hotspot tương đối 0–100, không phải trip count | Ghi rõ score tương đối; không gọi là số cuốc, xác suất khách hoặc nhu cầu live |
| Batch pipeline: “Noise Filtering”, “Spatial Outlier Removal”, “lọc nhiễu GPS” | Không được thực hiện/chứng minh trong pipeline TLC hiện tại | Thay bằng chuẩn hóa thời gian/zone, lọc bản ghi theo điều kiện dữ liệu, aggregate pickup theo zone/ngày/giờ/thứ; chỉ giữ bước đã có trong source |
| Machine Learning: “KMeans Demand Clustering” và metrics cũ | KMeans đã bỏ; metric cũ không khớp model artifact/evidence mới | Xóa KMeans; dùng GBTRegressor và metric temporal holdout mới |
| Chỉ số cũ trong deck | Các con số không khớp lần train mới | Thay bằng 635.798 train / 132.249 test, R² 0,655; MAE 7,348 điểm; RMSE 11,066 điểm; Precision 88,03%; Recall 30,79% |
| Ví dụ anomaly theo “10 phút”, Mean/StdDev chuyến và prediction trips | Threshold hiện dựa trên daily relative score theo zone/hour/day-of-week, không phải cuốc/10 phút | Ghi baseline mean + 3σ trên training score cùng zone/hour/weekday, tối thiểu 5 mẫu, cap 100; alert là cờ thống kê, không kết luận nguyên nhân |
| Feature list | Model dự báo relative hotspot score từ zone, giờ và thứ | Chỉ liệt kê ba đặc trưng lịch |
| Streaming: “Live Trips”, “Instant”, “Fare / Time Estimation”, “Dynamic Fleet Metrics” | Đây là replay/simulation; ước tính cước/thời gian chuyến không nằm trong pipeline dự báo | Đổi thành historical replay theo simulation clock, micro-batch scoring, lưu MongoDB; xóa fare/time estimation, gọi dữ liệu xe là mô phỏng |
| Kết luận: “triển khai thành công kiến trúc Lambda trên hạ tầng phân tán”, “dự báo chính xác” | Phóng đại phạm vi triển khai và metric; recall thấp | Nêu prototype local chạy batch + replay streaming; báo R²/MAE/RMSE/Precision/Recall và giới hạn bắt hotspot |
| Hướng phát triển: “phát triển hệ thống định tuyến” | Mô phỏng xe hiện đã chạy dọc tuyến đường OSRM; thiếu điều phối xe thật/GPS thật | Nêu OSRM/OSM chỉ dùng tuyến mô phỏng; phát triển tiếp là dữ liệu fleet live, tối ưu phân xe và đánh giá ngoài mẫu |
| Thành viên/role/timeline | Tên/role, “Group 6 / MSA33HCM”, các ngày 2025 và timeline tháng không nhất quán, chưa được nhóm xác nhận | Xác minh với nhóm; tổng contribution 100%; bỏ hoặc thay tất cả mốc ngày cũ khi chưa xác nhận |

## Nguồn đối chiếu

- Số liệu model và định nghĩa score/threshold: `output/evidence/model_metrics.json`.
- Quy mô dataset, HDFS, Spark, MongoDB, runtime: `output/evidence/runtime_metrics.json` và `output/evidence/verification_summary.json`.
- Pipeline, nguồn TLC/OSRM, giới hạn triển khai: `README.md`.
- Bản báo cáo nộp: `output/documents/BDA501_Final_Project_Report_Submission.docx`.

Deck cần được kiểm tra lại trực quan sau khi thay nội dung để đảm bảo chart, sơ đồ, nhãn trục, speaker notes, ngày tháng và số liệu ở mọi trang đều khớp. File này không thay thế việc cập nhật và QA bản Canva.

## Nội dung thay thế cần dùng

### Trang bìa và mục tiêu

Dùng mô tả: “Prototype phân tích hotspot taxi từ dữ liệu TLC, phát lại sự kiện lịch sử và mô phỏng điều phối đội xe.” Xóa phát biểu hệ thống đang kết nối dữ liệu live, tự điều phối taxi thật hoặc dự báo chính xác số cuốc xe. Ghi rõ luồng hiện tại là replay/simulation.

### Quy mô và nguồn dữ liệu

- 7 tệp NYC TLC Yellow Taxi Parquet; 26.116.976 chuyến trong snapshot xử lý.
- Cột nghiệp vụ chính: thời điểm đón, pickup LocationID, dropoff LocationID.
- Spark join Taxi Zone Lookup: 265 khóa; 26.116.976 dòng trước/sau; 0 pickup/dropoff ID unmatched.
- Fallback 10.000 dòng là dữ liệu tổng hợp deterministic, không phải giao dịch taxi thật.
- Producer đổi timestamp dữ liệu lịch sử sang simulation clock; không có GPS taxi trực tiếp.

### Kiến trúc và công nghệ

Vẽ hai nhãn rõ ràng: **Đã chạy local** và **Thiết kế mở rộng**.

- Đã chạy: HDFS; Hadoop Streaming mapper/combiner/reducer; Spark DataFrame ETL và Spark SQL; GBTRegressor; Kafka replay; Spark Structured Streaming; MongoDB; dashboard.
- Hadoop dùng LocalJobRunner trên một máy, không phải YARN đa node.
- Đề xuất: object storage, Spark nhiều worker, Kafka/MongoDB replicas, secret management, TLS và observability tập trung.
- MapReduce lọc/đếm pickup theo zone-hour; không tuyên bố lọc nhiễu GPS hay spatial outlier, vì dữ liệu và mã hiện tại không chứng minh bước đó.
- Không đưa KMeans vào sơ đồ/slide kết quả: code hiện tại chỉ deploy GBTRegressor.

### Xử lý và xác minh

- Hadoop Streaming: 26.116.976 mapper input/output records; combiner input 26.167.760, output 62.883; reducer output 6.155 zone-hour groups; so khớp Counter baseline độc lập.
- Spark SQL: top 20 zone-hour-weekday; borough-hour query 192 dòng; formatted plan có Aggregate/Exchange.
- Join lookup dùng broadcast dimension nhỏ để tránh shuffle join không cần thiết.
- Performance experiment: cache sample 100.000 dòng, 13.725 nhóm giống nhau; median 4 partitions = 0,272 giây, 16 partitions = 0,238 giây. Đây là timing một máy, không phải kết quả cluster.

### Mô hình, metric và cách nói kết quả

**Target:** relative hotspot score từ 0–100, tính theo số chuyến của zone chia cho zone có nhiều chuyến nhất trong cùng ngày/giờ/thứ. Đây không phải trip count hay xác suất có khách.

**Temporal holdout:** 175 ngày train, 639.393 dòng (2024-12-31–2026-06-21); 44 ngày test kế tiếp, 128.644 dòng (2026-06-22–2026-08-05).

| Metric | Kết quả | Diễn giải đúng |
|---|---:|---|
| MAE | 7,366 điểm | Sai lệch tuyệt đối trung bình trên thang score 0–100 |
| RMSE | 11,044 điểm | Phạt mạnh sai số lớn trên cùng thang score |
| R² | 0,658 | Giải thích khoảng 65,8% biến thiên score trong holdout; không phải accuracy |
| Precision, score ≥ 50 | 88,39% | Tỷ lệ dự báo dương đạt ngưỡng thực tế |
| Recall, score ≥ 50 | 30,88% | Tỷ lệ hotspot thực tế được bắt; còn bỏ sót nhiều vùng |

Confusion counts: TP 2.314; FP 304; FN 5.180. Xóa metric cũ trong deck (R² 0,75, MAE 2.197, RMSE 5.377, Precision 0,75, Recall 0,85; 32.149 mẫu), vì chúng không khớp lần train hiện tại.

### Ngưỡng cảnh báo

- Alert threshold: mean + 3 × population standard deviation theo zone/hour/day-of-week, tối thiểu 5 mẫu train, cap 100. Cờ cảnh báo là tín hiệu thống kê, không xác định nguyên nhân sự kiện bất thường.

### Streaming, NoSQL và giao diện

- Kafka phát lịch sử qua simulation clock; không gọi đây là live TLC feed.
- Structured Streaming đọc event, load PipelineModel để chấm điểm, checkpoint vào volume và upsert MongoDB theo event_id. Luồng hiện collect micro-batch về driver, là giới hạn scale.
- MongoDB: hotspot_predictions được truy vấn theo zone/time/event_id; vehicle_status lưu trạng thái gần nhất theo vehicle_id. Evidence count biến đổi khi replay tiếp tục; dùng timestamp khi trích snapshot.
- Dashboard tô màu polygon NYC Taxi Zone theo forecast score; zone boundary từ TLC, 40 xe là mô phỏng, tuyến đường lấy từ OSRM/OpenStreetMap. Không nói vị trí xe là GPS thật.

### Thành viên, vai trò và ngày

Deck đang ghi Group 6 / MSA33HCM, tên thành viên/role, ngày trình bày 20-09-2025 và 10 November 2025, cùng timeline có mốc tháng không nhất quán. Xác nhận lại với nhóm trước khi giữ tên, phân công, contribution percentage hay ngày. Tổng tỷ lệ đóng góp trong slide phải bằng 100%. Không dùng thông tin thành viên từ bản cũ nếu chưa được nhóm xác nhận.

## File tham chiếu

- Báo cáo: `output/documents/BDA501_Final_Project_Report.docx`
- Metric: `data/results/model_metrics.json`
- README: `README.md`
- Screenshots: `output/playwright/dashboard-calendar-only-2026-10-04.png`, `map-calendar-only-2026-10-04.png`, `mobile-calendar-only-2026-10-04.png`
