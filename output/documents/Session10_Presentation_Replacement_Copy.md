# Nội dung thay thế cho deck Session 10

Đối chiếu bản Canva 22 slide với mã nguồn, `README.md` và `output/evidence/model_metrics.json` ngày 04/10/2026. Đây là bản nội dung để chủ deck tự cập nhật; deck Canva gốc chưa bị thay đổi.

## Slide 1 — Bìa

**Tiêu đề:** Phân tích hotspot taxi và mô phỏng điều phối đội xe<br>
**Mô tả:** Prototype sử dụng dữ liệu NYC TLC lịch sử, phát lại sự kiện và mô phỏng xe để hỗ trợ nhận diện khu vực có score hotspot cao.<br>
**Trình bày bởi:** [Tên nhóm/thành viên]<br>
**Ngày:** [Ngày trình bày được xác nhận]<br>
Xóa “2030”, cụm “theo thời gian thực” và ngày cũ đang xung đột.

## Slide 2 — Trang chuyển

Giữ thiết kế nếu đây là trang nhận diện nhóm. Chỉ cập nhật tên nhóm/lớp sau khi xác nhận; xóa “GROUP 6” nếu không đúng.

## Slide 3 — Mục lục

1. Bài toán và dữ liệu<br>
2. Kiến trúc và xử lý Batch<br>
3. Mô hình, Streaming và Dashboard<br>
4. Kết quả, giới hạn và hướng phát triển

## Slide 4 — Phần 1

**Bài toán và dữ liệu**<br>
Động lực phân tích mất cân bằng cung–cầu theo khu vực và thời gian.

## Slide 5 — Bài toán

**Nhu cầu taxi thay đổi theo khu vực và thời điểm**<br>
Dữ liệu chuyến đi lịch sử có thể giúp nhận diện những zone và khung giờ thường có score hotspot cao. Phân tích này hỗ trợ người vận hành tham khảo khi bố trí xe.

## Slide 6 — Tác động nghiệp vụ

**Khách hàng:** thời gian chờ có thể tăng khi xe phân bố không đều.<br>
**Tài xế:** xe có thể chạy rỗng hoặc tập trung tại khu vực ít nhu cầu.<br>
**Vận hành:** dữ liệu lịch sử giúp quan sát biến động không gian–thời gian.<br>
Đây là động lực của bài toán, không phải kết quả tác động đã được đo trong dự án.

## Slide 7 — Mục tiêu

**Mục tiêu:** dự báo relative hotspot score theo pickup zone, giờ và thứ trong tuần; tô màu zone có score cao trên bản đồ và đưa ra gợi ý điều phối mô phỏng.<br>
Người vận hành chọn xe, điểm đến và xác nhận; chỉ khi được duyệt xe mô phỏng mới nhận lệnh điều hướng. Hệ thống không điều xe taxi thật và không nhận GPS trực tiếp.

## Slide 8 — Vì sao dùng công cụ Big Data?

**Quy mô:** 26.116.976 chuyến Yellow Taxi trong snapshot xử lý.<br>
**Xử lý:** Hadoop Streaming tổng hợp zone–giờ; Spark DataFrame/SQL thực hiện ETL, truy vấn và huấn luyện.<br>
**Luồng:** Kafka phát lại sự kiện lịch sử để minh họa Structured Streaming.<br>
**Giới hạn triển khai:** demo chạy Docker trên một máy; Hadoop dùng LocalJobRunner, chưa phải cụm đa node.

## Slide 9 — Kiến trúc đang chạy và thiết kế mở rộng

**Đã chạy local:** HDFS, Hadoop Streaming, Spark ETL/SQL/MLlib, Kafka historical replay, Spark Structured Streaming, MongoDB và Dashboard.<br>
**Thiết kế mở rộng:** Spark nhiều worker, Kafka/MongoDB replicas và object storage. Các thành phần mở rộng là đề xuất, chưa triển khai.

## Slide 10 — Phần 2

**Kiến trúc và xử lý dữ liệu**<br>
Batch tạo dữ liệu curated và model artifact; Streaming phát lại sự kiện để chấm điểm và ghi kết quả.

## Slide 11 — Phạm vi dự án

**Trong phạm vi:** dữ liệu NYC TLC lịch sử; chuẩn hóa và tổng hợp dữ liệu; Spark ETL/SQL; GBTRegressor dự báo relative hotspot score 0–100; anomaly threshold; phát lại Kafka; ghi MongoDB; dashboard và xe mô phỏng chạy theo tuyến OSRM/OpenStreetMap.<br>
**Ngoài phạm vi:** feed taxi live, GPS thật, tự động điều xe taxi thật, dự báo số cuốc tuyệt đối, giá cước/ETA cho chuyến taxi và triển khai cloud đa node.

## Slide 12 — Sơ đồ kiến trúc và pipeline

Vẽ luồng đã chạy:<br>
`NYC TLC Parquet → Hadoop Streaming / HDFS → Spark ETL + Taxi Zone Lookup → Curated Parquet → Train GBT + baseline → PipelineModel`<br>
`Historical Replay → Kafka → Spark Structured Streaming + load PipelineModel → score/alert → MongoDB → Dashboard`<br>
`Dashboard: người vận hành duyệt lệnh → MongoDB vehicle_dispatch_commands → producer → OSRM route → Kafka taxi_vehicles → fleet tracker → Dashboard`<br>
Ghi rõ “local prototype”; Dashboard hiển thị điểm dự báo, bản đồ zone và fleet mô phỏng. Đánh dấu cluster nhiều worker là thiết kế tương lai.

## Slide 13 — Batch và huấn luyện model

**Nguồn:** 7 tệp NYC TLC Yellow Taxi Parquet.<br>
**Hadoop Streaming:** 26.116.976 chuyến thành 6.155 nhóm zone–giờ; kết quả đối chiếu Counter baseline độc lập. Job chạy LocalJobRunner.<br>
**Spark ETL:** broadcast join Taxi Zone Lookup (265 dòng); số dòng trước/sau là 26.116.976; 0 pickup/dropoff ID không khớp.<br>
**Model:** GBTRegressor dự đoán score tương đối; bỏ KMeans.<br>
Xóa “lọc nhiễu GPS”, “spatial outlier removal” và mô tả input CSV/JSON/coordinates nếu sơ đồ hiện tại thể hiện các bước đó.

## Slide 14 — Model và metric

**Nhãn dự báo:** `100 × số chuyến của zone / số chuyến lớn nhất trong cùng ngày, giờ và thứ`. Score là tương đối, không phải số chuyến hoặc xác suất.

| Đánh giá temporal holdout | Kết quả |
|---|---:|
| Train | 635.798 dòng · 178 ngày (2001-01-01–2026-06-20) |
| Test trên 45 ngày kế tiếp | 132.249 dòng (2026-06-21–2026-08-05) |
| MAE | 7,348 điểm score |
| RMSE | 11,066 điểm score |
| R² | 0,655 |
| Precision tại score ≥ 50 | 88,03% |
| Recall tại score ≥ 50 | 30,79% |

`R²` không phải accuracy. Precision cao nhưng Recall thấp: trong holdout, model bỏ sót nhiều hotspot thực tế. Ở ngưỡng 50: TP=2.360, FP=321, FN=5.305. Thay toàn bộ metric cũ (0,75; 2.197; 5.377; 0,85; 32.149 mẫu).

## Slide 15 — Cảnh báo bất thường

**Baseline:** tính mean và population standard deviation của training hotspot score theo pickup zone, giờ và thứ trong tuần.<br>
**Ngưỡng:** `mean + 3 × standard deviation`; cần tối thiểu 5 mẫu; ngưỡng được giới hạn tối đa 100.<br>
**Cảnh báo:** gắn cờ khi predicted score vượt baseline. Đây là cảnh báo thống kê; cần người vận hành rà soát nguyên nhân.<br>
Xóa ví dụ 10 phút, số chuyến và threshold tính trên trip count.

## Slide 16 — Cách đánh giá mô hình

Temporal holdout: 80% ngày đầu để train, 20% ngày cuối để test.<br>
Đặc trưng: pickup zone, giờ trong ngày và thứ trong tuần.<br>
Hiển thị MAE, RMSE, R² cùng Precision/Recall ở ngưỡng hotspot; không gọi R² là độ chính xác.

## Slide 17 — Streaming

`Historical Replay → Kafka → Spark Structured Streaming → load PipelineModel → score + alert → MongoDB`.<br>
Luồng phát lại dữ liệu lịch sử theo simulation clock; không phải feed taxi live. Structured Streaming dùng micro-batch; không tuyên bố độ trễ production hay feature extraction GPS live.<br>
Xóa fare/time estimation và “instant live demand”.

## Slide 18 — MongoDB và Dashboard

**MongoDB:** lưu hotspot predictions và trạng thái xe mô phỏng để Dashboard truy vấn.<br>
**Dashboard:** bản đồ polygon NYC Taxi Zone tô theo score; xe là mô phỏng. Người vận hành chọn xe/zone và duyệt lệnh; tuyến đã duyệt dựa trên OSRM/OpenStreetMap, không phải vị trí GPS thật.<br>
Đổi “Live Vehicle Dispatching” thành “Human-approved fleet simulation”.

## Slide 19 — Phần 3

**Phân công và kế hoạch thực hiện**<br>
Xóa timeline/ngày cũ chưa được nhóm xác nhận.

## Slide 20 — Thành viên và đóng góp

Giữ ô tên, mã sinh viên, vai trò và tỷ lệ để nhóm tự xác nhận. Tổng tỷ lệ đóng góp phải bằng 100%.<br>
Vai trò kỹ thuật có thể mô tả theo các phần thực tế: ingestion/replay; Hadoop/Spark ETL; model/evaluation; Structured Streaming/MongoDB/Dashboard. Xóa phân công “lọc nhiễu GPS/Spatial Outliers” và “KMeans” nếu không đúng đóng góp thực tế. Không tự gán tên hoặc phần trăm.

## Slide 21 — Kết luận và giới hạn

**Kết quả:** prototype local kết hợp batch và replay streaming; xử lý snapshot TLC 26,1 triệu chuyến; GBT dự báo relative hotspot score với temporal holdout; Dashboard hiển thị zone score và fleet simulation.<br>
**Giới hạn:** Hadoop LocalJobRunner một máy; không có taxi live/GPS thật; chỉ mô phỏng lệnh điều phối sau khi người vận hành duyệt; Recall còn có thể bỏ sót hotspot.<br>
**Tiếp theo:** rolling-origin evaluation; thêm lag demand/lịch sự kiện với đặc trưng chỉ dùng dữ liệu có trước thời điểm dự báo; đánh giá Recall hoặc Recall@top-k trước khi chọn model mới.

## Slide 22 — Cảm ơn

**Cảm ơn đã lắng nghe!**<br>
**Phân tích hotspot taxi và mô phỏng điều phối đội xe**<br>
Trình bày bởi: [Tên nhóm/thành viên đã xác nhận] · Ngày: [Ngày trình bày đã xác nhận]<br>
Xóa tagline “theo thời gian thực”, năm 2030 và ngày 10/11/2025 nếu chưa được xác nhận.

## Trước khi nộp deck

- Cập nhật chart, chú giải, nhãn trục, ảnh chụp và mọi số liệu cũ; không chỉ sửa phần văn bản.
- Thống nhất cách gọi “hotspot score tương đối 0–100”; tránh gọi là số chuyến/nhu cầu live.
- Đánh dấu riêng thành phần đã chạy và thành phần chỉ là thiết kế đề xuất.
- Xác nhận tên nhóm, thành viên, tỷ lệ đóng góp, giảng viên và ngày trình bày.
- Soát lại toàn bộ slide ở chế độ trình chiếu để bắt chữ tràn, chữ nhỏ, ngày/số liệu xung đột.

## Nguồn số liệu

- `output/evidence/model_metrics.json` — temporal holdout và anomaly threshold.
- `output/evidence/runtime_metrics.json` — quy mô dữ liệu và runtime.
- `README.md` — phạm vi pipeline, replay, routing và giới hạn triển khai.
- `output/documents/BDA501_Final_Project_Report_Submission.docx` — báo cáo nộp.
