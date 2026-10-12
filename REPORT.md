# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `<điền>` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602751 |
| GitHub | HoangDiine |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `<điền: trip-xxxxx>` |

## Kết quả cuối

- `dataset_version` của bạn: `c416cb86cd8f`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `717 / 155 / 147`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Ở mẫu Bronze, A dùng `ts_utc=2025-10-07T08:00:00.014Z` theo ISO-8601 UTC, còn B dùng `timestamp_ms=1759933801509` theo epoch milliseconds. Các khác biệt khác:

- Tốc độ: `speed_mps` tính bằng m/s, còn `spd_kmh` tính bằng km/h.
- Hướng: `heading_deg` so với `hdg`.
- Tọa độ: `lat`/`lon` so với `latitude`/`longitude`.
- Độ cao: `alt_m` so với `altitude`.
- Gia tốc: `acc_x`/`acc_y` so với `ax`/`ay`.
- Bàn đạp và lái: A có `gas`, `brake`, `steer`; B có `throttle_pct`, `brake_pct`, `steering`. Mẫu gas/brake của A là phần thập phân (0.028, 0.000), còn B là phần trăm (2.0, 0.0).

Bronze giữ A và B ở hai bảng riêng, các cột nguồn ở dạng chữ; kiểm tra cũng thấy 30 giá trị tốc độ trống của A vẫn là chuỗi trống. Cách này giữ nguyên dữ liệu và schema để truy vết, tránh đoán sai đơn vị hoặc làm mất giá trị lỗi khi ép kiểu sớm. Sang Silver, pipeline mới chuẩn hóa timestamp (kể cả độ lệch đồng hồ), đơn vị và kiểu dữ liệu, đồng thời có thể đưa dòng không hợp lệ vào quarantine.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

`stg_telemetry_a.sql` đổi `ts_utc` sang timestamp UTC và giữ `speed_mps` vì A đã dùng m/s. `stg_telemetry_b.sql` đổi `timestamp_ms` epoch-millisecond sang timestamp; trước đó trừ `telemetry_clock_offset_ms=1500` lấy từ metadata video. B đổi `spd_kmh / 3.6` sang m/s, phần trăm ga/phanh chia 100, rồi ánh xạ các tên cột còn lại về schema chung.

Sau chỉnh, telemetry B bắt đầu lúc `2025-10-08 14:30:00.009 UTC`; `video_start_utc` là `2025-10-08 14:30:00.000 UTC`. Sai khác 9 ms, nằm trong ngưỡng kiểm tra 0,5 giây. Bảng Silver có 6.149 dòng sạch và 106 dòng rejects:

| Drive | Đã nhận | Sạch | duplicate | speed_missing | speed_out_of_range | Tổng cách ly |
|---|---:|---:|---:|---:|---:|---:|
| drive_a | 3.021 | 2.968 | 20 | 30 | 3 | 53 |
| drive_b | 3.234 | 3.181 | 20 | 30 | 3 | 53 |
| Tổng | 6.255 | 6.149 | 40 | 60 | 6 | 106 |

Từng dòng cách ly vẫn có `drive_id`, `ts`, `reject_reason`, `speed_mps`, `_batch_id`, `_row_no`; truy vấn mẫu cho thấy cả duplicate lẫn speed_missing. Giữ dòng cùng lý do và nguồn gốc giúp đếm, điều tra và tái kiểm tra; xoá im lặng sẽ làm mất bằng chứng về loại lỗi và dòng dữ liệu.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` được đặt `severity: warn` trong `schema.yml` vì khoảng trống do cảm biến rớt mẫu cần được báo để điều tra nhưng không nhất thiết làm mọi telemetry còn lại mất giá trị. `in_range` không hạ severity nên mặc định là error: tốc độ ngoài 0–70 m/s là dữ liệu không hợp lệ, cần chặn trước khi đi tiếp. Lượt thường có `PASS=6 WARN=1 ERROR=0`; cảnh báo là `max_gap_ms`.

Trong `report/evidence/break_gate.json`, thí nghiệm nới `max_speed_mps` từ 70 lên 1000 m/s đưa 6 dòng tốc độ bất hợp lý vào Silver. Test `in_range_silver_telemetry_speed_mps__70__0` bắt đúng 6 dòng và làm cổng thất bại: `PASS=5 WARN=1 ERROR=1`. Sau khi script khôi phục luật gốc, kết quả trở lại `PASS=6 WARN=1 ERROR=0`. Điều này cho thấy warn giúp quan sát lỗi có thể chịu đựng, còn error ngăn dữ liệu vi phạm điều kiện bắt buộc đi tiếp.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

`report/evidence/idempotency.json` ghi `trip-00003` có 60 frame. Bảng có 1.246 dòng trước khi chạy lại và vẫn 1.246 sau hai lần chạy lại; fingerprint trước/sau đều là `6c7480dfde7e`. Mô phỏng ghi sai kiểu append tạo 120 dòng cho trip 60 frame sau hai lần chạy, tức nhân đôi.

Delta overwrite theo predicate `trip_id` cập nhật riêng partition của trip trong một commit; partition của các trip khác không bị đụng đến và người đọc thấy một phiên bản đã commit. Nếu xoá partition rồi ghi lại thành hai thao tác, lỗi giữa chừng có thể để trip mất dữ liệu; append thì tạo bản sao khi retry.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---:|---:|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Mình đồng ý với việc tách khung đen khỏi khung tối. Trên MinIO, `silver/frames/drive_a/01001.jpg` là khung đen (cờ `is_black`, brightness 0, lệch telemetry 18 ms); `drive_a/01998.jpg` bị mờ (cờ `is_blurry`, blur_score 2,485, lệch 16 ms). `drive_b/00030.jpg` là cảnh tối trong hầm nhưng vẫn thấy đường và cảnh vật (`is_dark=true`, `is_black=false`, brightness 23,07, lệch 98 ms). Đây là khung tối được giữ, không phải ảnh đen.

Bước 3 đếm drive_a có 602 frame: 6 đen, 9 mờ, 4 tối được giữ, 149 gần trùng và 7 mất đồng bộ. drive_b có 644 frame: 4 đen, 0 mờ, 61 tối được giữ, 53 gần trùng và 7 mất đồng bộ. Các cờ có thể chồng lấp nên không cộng các cột thành tổng loại trừ nhau. Ở Silver frame được gắn cờ; khi tạo Gold, pipeline loại frame đen, mờ, gần trùng hoặc mất đồng bộ, còn cờ tối không làm frame bị loại.

Độ trễ frame–telemetry trung bình/tối đa là 66/2.884 ms ở drive_a và 73/3.112 ms ở drive_b. Các mẫu trên lệch 16–98 ms.

Thí nghiệm `threshold.json` cho thấy ngưỡng đang dùng là 2 bit khác nhau; mình giữ ngưỡng 2 vì ngưỡng 0 chỉ bắt 61/8 frame, còn ngưỡng 6 tăng lên 363/244 và có nguy cơ loại cả ảnh khác nhau thật. Ở tốc độ lấy mẫu 2 FPS, xe chạy chậm thường làm cảnh giữa hai frame giống nhau hơn; xe chạy nhanh làm cảnh dịch chuyển nhiều hơn trong 0,5 giây. Vì vậy tốc độ ảnh hưởng số frame gần trùng, nhưng tăng ngưỡng để bù có thể tạo false positive; cần hiệu chỉnh theo chuyển động và kiểm tra mẫu thay vì nâng ngưỡng máy móc.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
