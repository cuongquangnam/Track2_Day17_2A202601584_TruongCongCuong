# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Trương Công Cường, MSSV: 2A202601584  **Lớp:** AICB-P2T2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 11.4s
  run 2/3 … 11.8s
  run 3/3 … 12.1s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    f8d3f591f0    f8d3f591f0    f8d3f591f0   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Job lỗi mạng, Clear Task trên Airflow rồi chạy lại: `gold_training_set` phình thêm mỗi lượt, không báo lỗi. |
| **Nguyên nhân** | Bảng là *entity* (1 hàng / 1 ticket) nhưng model incremental không khai `unique_key`. dbt mặc định sinh `INSERT` (append): chạy lại cùng partition là ghi *thêm* hàng, không ghi đè. CDC còn `op='u'` nên một ticket tạo ngày D1, sửa ngày D2 đi qua filter `run_date` hai lần trong một lượt — `delete+insert` theo ngày không gộp được chúng. `catchup=True` và không giới hạn `max_active_runs` chỉ làm Clear Task kích hoạt lỗi thường hơn; chúng không phải gốc lỗi. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: `unique_key='ticket_id'`, `incremental_strategy='merge'`. `dags/ai_training_pipeline.py`: `catchup=False`, `max_active_runs=1`. |
| **Bằng chứng** | trước: 38.750 hàng (thừa, lặp ticket) · sau: **12.480** hàng · checksum 3 lượt: `8dd7c98653` giống nhau |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` thiếu ~5% so với đối chiếu thủ công; chỉ thiếu ở ngày đã chạy xong, ngày mới thì đủ. |
| **P99 độ trễ đo được** | **2.73 ngày** *(bắt buộc)* |
| **Lookback đã chọn** | 3 ngày — `ceil(P99 = 2.73)`; độ trễ lịch lớn nhất quan sát được cũng là 3 ngày |
| **Nguyên nhân** | Incremental lọc `event_date > max(event_date)` trên bảng đích. Mốc so sánh là *ngày sự kiện đã materialize*, không phải lúc dữ liệu tới kho. Event xảy ra 08-12 nhưng `_ingested_at` = 08-15 không lọt: lúc chạy 08-15, `max(event_date)` đã là 08-14. Đổi `>` thành `>=` chỉ nới 1 ngày, trong khi P99 = 2.73. Phân bố độ trễ trên `bronze_events` là *hai cụm*: 94.95% về trong 0–6 giờ (P50 = 0.13 ngày), 5.05% về muộn 1.8–2.95 ngày (P99 = 2.73, max = 2.94). Cụm muộn tạo 455 cặp (ngày, khách) *chỉ* có dữ liệu late — đúng số hàng thiếu 9.100 − 8.645. |
| **Cách khắc phục** | `gold_feature_daily.sql`: `event_date >= max(event_date) - interval 3 day`; `unique_key=['event_date','customer_id']`, `incremental_strategy='merge'` để lần tính lại *thay thế* chứ không cộng dồn. |
| **Bằng chứng** | trước: 8.645 hàng · sau: **9.100** hàng · checksum 3 lượt: `f8d3f591f0` |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> P99 bao phủ 99% bản ghi late với cửa sổ *cố định*. `max` (2.94 ngày) ở dataset này gần P99, nhưng trong vận hành một outlier 30 ngày sẽ buộc *mọi* lượt sau tính lại 30 ngày — trả phí scan/merge mãi mãi cho một trường hợp hiếm. Mỗi ngày lookback thêm = tính lại ~650 cặp (ngày, khách) trên *mọi* run sau này. Ceil(P99)=3 ngày đủ calendar lag 3 ngày, tốn thêm một ngày so với mức tối thiểu lý thuyết, chấp nhận được.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Backend đổi `priority` số → chuỗi từ 08-10 (có báo Slack). Pipeline không dừng; classifier từ hôm đó kém hẳn. Silver có 6.606 hàng `priority` NULL / ngoài 1..4. |
| **Nguyên nhân** | `normalize_priority` dùng `try_cast(... as integer)`: nhãn chữ hợp lệ (`urgent`…) thành NULL, trong khi `'0'`, `'5'`, `'-1'` vẫn được nhận vì chúng *là số*. Contract `enforced: false` và không có test miền giá trị nên cả hai nhóm đi thẳng vào Silver. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | (1) `'1'..'4'` — đúng contract cũ, **giữ số**. (2) `urgent/high/medium/low` — schema evolution, cùng ý nghĩa, **map 1..4**. (3) `P1`, `unknown`, `0`, `5`, `-1`, `''`, NULL — dữ liệu hỏng, macro trả **NULL** → quarantine. |
| **Cách khắc phục** | Macro CASE 3 nhóm. `silver_tickets`: **lọc bản ghi NULL trước**, rồi mới `row_number` theo ticket (loại *bản ghi* hỏng, không loại *ticket*). `quarantine_tickets`: `where macro is null`. `schema.yml`: `contract.enforced: true` + test `not_null` / `accepted_values: [1,2,3,4]`. |
| **Bằng chứng** | `quarantine_tickets` = **312** hàng · `dbt test` **11/11** pass · `priority` sạch · `gold_training_set` vẫn 12.480 |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> Chặn ở **Silver**. Bronze phải giữ bản ghi thô (kể cả hỏng) để điều tra: nếu Bronze từ chối thì mất `priority_raw`, mất mốc thời gian đổi schema, không tái hiện được sự cố. 312 hàng lỗi (~2% CDC) không được phép chặn >12.000 ticket, ~130.000 event và 31.200 chunk đang chờ phục vụ. Tách vào quarantine, DAG chạy tiếp; người trực xử lý hàng đợi, không phải rollback cả ngày.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | A và B |
| **Nguyên nhân** | **A.** 5.000 file Parquet nhỏ, không partition; filter `strftime(event_time,…)` bọc cột nên engine không prune thư mục / min-max row-group — mỗi file vài chục hàng vẫn bị tính ~1.000 hàng scanned → 5.000.000. **B.** Consumer `commit()` offset *trước* khi ghi: crash tại `maybe_crash` = offset đã dịch, batch chưa vào bảng → **at-most-once, mất dữ liệu**. Đảo thành ghi rồi commit thì replay tạo trùng nếu `INSERT` thuần. |
| **Cách khắc phục** | **A.** `compact.py`: `PARTITION_BY (event_date)` (14 giá trị, khớp filter ngày; *không* partition `customer_name` 650 giá trị), `ORDER BY customer_name, event_time`, `ROW_GROUP_SIZE 2048` (một ngày ~9k hàng; 122.880 sẽ nhét cả ngày vào 1 group, min/max customer vô dụng). Query: `hive_partitioning=true`, `event_date = DATE '2026-08-09'` (cột đứng một mình). **B.** Ghi → crash-point → `commit()`. PK `event_id` + `ON CONFLICT DO UPDATE`. `DO UPDATE` khác `DO NOTHING`: replay với *payload đã đổi* thì UPDATE ghi bản mới, NOTHING giữ bản cũ (có thể sai/dở). Event_id là sự kiện có thể được gửi lại sau sửa nội dung → chọn UPDATE. |
| **Bằng chứng** | **A.** rows scanned **5.000.000 → 9.324** (536×, hash `4379e4c5d9f3` không đổi, files **5.000 → 14**). **B.** `make crash-test`: C == A = 20.000 hàng / 20.000 `event_id`, không mất không trùng → **ĐẠT**. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Incremental model: có `unique_key` + strategy phù hợp grain (entity → merge, event → append) không, hay đang append ngầm? |
| 2 | Watermark incremental so với *thời điểm sự kiện* hay *thời điểm ingest*? Đo P99 độ trễ trước khi tin ngày cũ đã “chốt”. |
| 3 | Contract có `enforced` không, test có ràng miền giá trị không, và bản ghi lệch schema đi đâu — dừng DAG hay vào hàng đợi? |
