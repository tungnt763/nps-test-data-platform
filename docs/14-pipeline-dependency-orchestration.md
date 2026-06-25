# Pipeline Dependency & Orchestration Strategy

> **Phiên bản:** 1.0 | **Ngày:** 2026-06-25
> **Áp dụng cho:** NiFi orchestration + Dremio transform engine
> **Tham chiếu:** [09-metadata-driven-strategy.md](09-metadata-driven-strategy.md) | [10-metadata-tables-design.md](10-metadata-tables-design.md) | [11-nifi-dynamic-pipeline-setup.md](11-nifi-dynamic-pipeline-setup.md)
>
> Đây là tài liệu **chiến lược (why)** cho cơ chế phụ thuộc giữa các layer. Hướng dẫn build
> từng processor (how) đã được tích hợp đầy đủ vào [Doc 11 v2.0](11-nifi-dynamic-pipeline-setup.md).
> Cơ chế *time-based* (silver/gold chạy theo CRON lệch giờ) **đã bị loại bỏ** khỏi Doc 11.

---

## 1. Vấn Đề Cần Giải Quyết

Luồng `ingestion → bronze` hiện tại đã ổn định. Nhưng từ **bronze → silver → gold** đang gặp 3 vấn đề cốt lõi:

### 1.1 Không có dependency thật giữa bronze → silver

Bronze và Silver hiện là **2 flow độc lập**, mỗi flow có `GenerateFlowFile` trigger riêng. Không có ràng buộc "silver chỉ chạy sau khi bronze xong".

### 1.2 Time-based trigger không hiệu quả và không thực tế

```
Bronze: CRON 0 0 2 * * ?  (2h sáng)
Silver: CRON 0 0 3 * * ?  (3h sáng — đoán rằng bronze xong trong 1h)
```

**Tại sao sai:**
- Nếu bronze chạy **quá 1 giờ** (data lớn, source chậm) → silver chạy lúc 3h khi bronze **chưa xong** → silver xử lý data thiếu hoặc rỗng.
- Nếu **nới khoảng cách** (silver 5h, gold 7h...) → tổng thời gian 1 luồng **quá dài**, không scale khi có nhiều layer.
- Wall-clock offset là **đoán mò**, không phản ánh thời gian chạy thực tế. Ngoài thực tế **không ai orchestrate bằng cách này**.

### 1.3 Silver đang dùng lại attribute/config của bronze

Các phương án trước giải quyết được "silver chạy sau bronze", nhưng silver vẫn **mượn config của bronze** (JOIN `transform_rules → pipeline_config` trên **cùng** `pipeline_id`). Điều này sai vì:

- `load_type` của bronze (đọc từ **source DB**) **khác** `load_type` của silver (đọc từ **bronze layer**). Bronze có thể `incremental` từ source, nhưng silver lại `full` rebuild từ bronze — hoặc ngược lại.
- `primary_keys`, `partition_columns`, `watermark_column` của tầng silver có thể khác bronze.
- `pipeline_id` của bronze **phải khác** `pipeline_id` của silver và gold.

> **Nhận diện gốc rễ:** Hệ thống đang **trộn lẫn 2 mối quan tâm khác nhau**:
> | Mối quan tâm | Câu hỏi | Giải pháp sai hiện tại |
> |--------------|---------|------------------------|
> | **Orchestration** | *Khi nào* silver chạy? | Wall-clock offset (đoán) |
> | **Configuration** | Silver dùng *config nào*? | Mượn config bronze |
>
> Chiến lược dưới đây **tách bạch** và giải quyết **cả hai**.

---

## 2. Ba Trụ Cột Của Chiến Lược

```
┌────────────────────────────────────────────────────────────────────────┐
│ TRỤ CỘT 1 — CONFIG MODEL: "1 stage = 1 config row"                       │
│   Mỗi (dataset, layer-transition) có 1 dòng pipeline_config riêng,       │
│   với pipeline_id riêng. Silver đọc CONFIG CỦA CHÍNH NÓ.                 │
├────────────────────────────────────────────────────────────────────────┤
│ TRỤ CỘT 2 — ORCHESTRATION: event-driven DAG walk (KHÔNG wall-clock)     │
│   Hoàn thành stage N → resolve & trigger stage N+1 theo `depends_on`.    │
│   Connection "success" của NiFi CHÍNH LÀ dependency.                     │
├────────────────────────────────────────────────────────────────────────┤
│ TRỤ CỘT 3 — CORRELATION & BARRIER: run_id + Wait/Notify                  │
│   1 run_id xuyên suốt 1 lần chạy; Wait/Notify cho fan-in (gold ⟵ nhiều  │
│   silver). Đảm bảo idempotent, lineage, và join nhiều parent an toàn.    │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Trụ Cột 1 — Config Model: "1 Stage = 1 Config Row"

### 3.1 Nguyên tắc

Mỗi **bước chuyển tầng** là **một dòng** `pipeline_config` độc lập, có `pipeline_id` riêng:

| pipeline_id        | dataset      | source_layer | target_layer | depends_on          | load_type    |
|--------------------|--------------|--------------|--------------|---------------------|--------------|
| `BRZ_transactions` | transactions | `source`     | `bronze`     | `NULL` (root)       | incremental  |
| `SLV_transactions` | transactions | `bronze`     | `silver`     | `BRZ_transactions`  | incremental  |
| `GLD_daily_txn`    | transactions | `silver`     | `gold`       | `SLV_transactions`  | full         |

**Đọc bảng này như sau:**
- `BRZ_transactions`: đọc từ **source DB** (incremental theo watermark của source) → ghi bronze. Không phụ thuộc ai (root).
- `SLV_transactions`: đọc từ **bronze layer** (incremental theo watermark của bronze) → ghi silver. Phụ thuộc `BRZ_transactions`.
- `GLD_daily_txn`: đọc từ **silver layer** (full rebuild) → ghi gold. Phụ thuộc `SLV_transactions`.

> **Quan trọng:** `load_type`, `primary_keys`, `watermark_column`, `partition_columns` trên dòng `SLV_*` mô tả cách **silver đọc bronze** — hoàn toàn độc lập với cách bronze đọc source. Đây chính là điểm sửa vấn đề 1.3.

### 3.2 Cột metadata mới (xem chi tiết DDL ở Doc 10 §3.1)

Thêm vào `pipeline_config`:

| Cột            | Kiểu     | Mục đích                                                              |
|----------------|----------|----------------------------------------------------------------------|
| `dataset`      | VARCHAR  | Nhóm logic các stage của cùng 1 thực thể (vd: `transactions`)        |
| `source_layer` | VARCHAR  | Tầng nguồn: `source` (bronze), `bronze` (silver), `silver` (gold)    |
| `depends_on`   | VARCHAR  | Danh sách `pipeline_id` upstream (comma-separated). `NULL` = root    |

`transform_rules`, `column_mapping`, `data_quality_rules` giữ nguyên schema nhưng **`pipeline_id` FK giờ trỏ tới stage sở hữu nó** (vd: rule dedup silver có `pipeline_id = 'SLV_transactions'`, không phải `BRZ_transactions`).

### 3.3 Silver lấy thông tin từ đâu? (phân biệt rõ các bảng config)

Khi `SLV_transactions` chạy, nó truy vấn **3 nhóm** thông tin, mỗi nhóm 1 mục đích:

| Cần biết gì | Bảng | Cột chính | Dùng để |
|-------------|------|-----------|---------|
| **"Tôi đọc/ghi thế nào?"** | `pipeline_config` (dòng `SLV_*`) | `load_type`, `primary_keys`, `partition_columns`, `watermark_column`, `source_layer`, `target_table`, `depends_on` | Chọn template MERGE vs CREATE, biết PK/partition, biết upstream |
| **"Tôi biến đổi cột thế nào?"** | `column_mapping` (WHERE pipeline_id = `SLV_*`) | `source_column`, `target_column`, `data_type`, `transformation` | Build SELECT clause (CAST, TRIM, COALESCE...) |
| **"Tôi chạy SQL transform nào?"** | `transform_rules` (WHERE pipeline_id = `SLV_*`) | `transform_type`, `sql_template`, `execution_order`, `depends_on` | Render & execute SQL trên Dremio |
| **"Tôi kiểm tra chất lượng gì?"** | `data_quality_rules` (WHERE pipeline_id = `SLV_*`) | `rule_type`, `rule_expression`, `severity`, `threshold_pct` | Chạy DQ sau transform |

> **Sửa triệt để vấn đề 1.3:** Silver **không** đụng tới bất kỳ dòng config nào của bronze. Mọi truy vấn đều khóa theo `pipeline_id` của **chính silver**. Bronze và silver có thể có `load_type`/PK/partition khác nhau hoàn toàn mà không xung đột.

### 3.4 DAG thể hiện qua metadata

`depends_on` biến `pipeline_config` thành **danh sách cạnh (edge list)** của một DAG. NiFi không cần hard-code thứ tự — nó **đọc DAG từ metadata** lúc runtime:

```
BRZ_transactions ──▶ SLV_transactions ──┬─▶ GLD_daily_txn
                                          └─▶ GLD_bank_kpis
BRZ_merchants ─────▶ SLV_merchants ───────┘  (GLD_bank_kpis ⟵ 2 parent)
```

Thêm 1 stage = INSERT 1 dòng + khai báo `depends_on`. **Không sửa NiFi.**

---

## 4. Trụ Cột 2 — Orchestration: Event-Driven DAG Walk

### 4.1 Ý tưởng cốt lõi

> Thay vì hẹn giờ, **mỗi stage khi chạy xong sẽ tự "đánh thức" các stage phụ thuộc nó.**
> Connection **`success`** trong NiFi chính là cạnh dependency — silver **về mặt vật lý** không thể bắt đầu nếu bronze chưa phát tín hiệu `success`.

Vòng lặp tổng quát (áp dụng cho **mọi** stage, dùng chung một engine):

```
        ┌──────────────────────────────────────────────────────────┐
        │  GENERIC STAGE EXECUTOR (1 PG dùng chung cho mọi layer)   │
        │                                                          │
   ┌───▶│  1. Nhận FlowFile (chỉ chứa: pipeline_id, run_id, ...)   │
   │    │  2. (Guard) Verify upstream success cho run_id            │
   │    │  3. Load config CỦA CHÍNH stage (đã có sẵn trong FF)     │
   │    │  4. Route theo target_layer → bronze/silver/gold logic   │
   │    │  5. Execute (ingest hoặc Dremio transform) + DQ          │
   │    │  6. Log execution_log (pipeline_id, layer, run_id)       │
   │    │  7. RESOLVE NEXT: SELECT * FROM pipeline_config          │
   │    │         WHERE depends_on chứa pipeline_id này            │
   │    │  8. Với mỗi downstream → emit FlowFile mới ──────────────┼──┐
   └────┼──────────────────────────────────────────────────────────┘  │
        └─────────────────────────◀─────────────────────────────────  ┘
                              (đệ quy đi hết DAG)
```

### 4.2 FlowFile giữa các stage chỉ mang "correlation key" — KHÔNG mang config bronze

Đây là chìa khóa giải quyết vấn đề 1.3 ở tầng orchestration. FlowFile truyền từ stage này sang stage kia **chỉ chứa khóa tương quan**, **không** chứa attribute config của tầng trước:

```json
{
  "run_id":               "RUN_20260625_020000_a1b2",
  "next_pipeline_id":     "SLV_transactions",
  "upstream_pipeline_id": "BRZ_transactions",
  "upstream_status":      "success",
  "upstream_watermark":   "2026-06-25 01:59:59"
}
```

Stage tiếp theo **tự re-query** `pipeline_config WHERE pipeline_id = next_pipeline_id` để nạp **config của chính nó**. Nhờ vậy:
- Silver **không bao giờ** kế thừa nhầm `load_type`/PK/partition của bronze.
- `upstream_watermark` chỉ là **gợi ý** (nếu silver muốn xử lý "bronze rows mới từ lần bronze này"); silver vẫn dùng watermark **của riêng nó** (mục 4.6).

> **Mẹo NiFi:** Sau khi resolve downstream, dùng `EvaluateJsonPath` để chỉ giữ lại đúng các attribute correlation, rồi `UpdateAttribute` xóa hết attribute thừa của stage trước (hoặc dùng processor `ModifyBytes`/đặt lại content). Cách an toàn nhất: **bắt đầu mỗi stage bằng việc đọc lại config từ DB**, coi FlowFile vào chỉ như "tín hiệu + run_id".

### 4.3 Controller chỉ trigger ROOT stages

`[1] Metadata Controller` (Doc 11 §4) đổi 1 điểm: thay vì route theo `target_layer` rồi trigger cả 3 layer song song, nó **chỉ trigger các root** (ingestion):

```sql
-- Controller đọc các ROOT stage để khởi động luồng
SELECT * FROM minio-datalake.metadata.pipeline_config
WHERE is_active = true
  AND depends_on IS NULL          -- chỉ root (bronze ingestion)
ORDER BY pipeline_id
```

Silver/gold **không** được trigger ở đây — chúng được đánh thức bởi stage cha qua "Resolve Next".

### 4.4 Processor "Resolve Next Stages" (đặt cuối mỗi executor)

Sau khi log success, chạy:

```sql
-- ExecuteSQL trên dremio-jdbc-pool
SELECT
    pipeline_id        AS next_pipeline_id,
    target_layer,
    depends_on
FROM minio-datalake.metadata.pipeline_config
WHERE is_active = true
  AND ( depends_on = '${pipeline_id}'
        OR depends_on LIKE '${pipeline_id},%'
        OR depends_on LIKE '%,${pipeline_id}'
        OR depends_on LIKE '%,${pipeline_id},%' )
```

→ `ConvertAvroToJSON` → `SplitJson $[*]` → mỗi downstream 1 FlowFile → `UpdateAttribute` set `run_id`, `upstream_pipeline_id = ${pipeline_id}`, `upstream_status = success` → đưa về **Input Port của Stage Router** (loop lại executor).

> Nếu kết quả rỗng (stage lá, vd gold cuối) → luồng kết thúc tự nhiên. Không cần xử lý gì thêm.

### 4.5 Sequencing of multiple transform_rules trong 1 stage

Trong **một** stage silver có thể có nhiều rule (create_table → dedup → merge). Thứ tự nội bộ vẫn dùng `transform_rules.execution_order` + `transform_rules.depends_on` (đã có sẵn ở Doc 10 §3.3). NiFi đọc rules `ORDER BY execution_order` và execute tuần tự **trong** stage; chỉ khi **toàn bộ rules** của stage success thì mới "Resolve Next" sang gold.

### 4.6 Watermark là per-stage (độc lập hoàn toàn)

Silver incremental đọc bronze:

```sql
-- last_watermark CỦA SILVER, không phải của bronze
SELECT COALESCE(MAX(last_watermark), '1970-01-01 00:00:00') AS last_watermark
FROM minio-datalake.metadata.pipeline_execution_log
WHERE pipeline_id = '${pipeline_id}'   -- = 'SLV_transactions'
  AND layer = 'silver'
  AND status = 'success'
```

`watermark_column` lấy từ dòng config `SLV_*` (là một cột **trong bronze**, vd `created_at` hoặc cột ingest-time `_ingested_at`). Bronze và silver giữ watermark riêng trong cùng `pipeline_execution_log` nhưng **khác `pipeline_id` + khác `layer`** → không đụng nhau.

### 4.7 So sánh độ trễ

```
Time-based (cũ):                 Event-driven (mới):
  02:00 bronze start              02:00 bronze start
  03:00 silver start (đoán) ⚠️    02:18 bronze done → silver start ngay ✅
  04:00 gold start (đoán)  ⚠️    02:25 silver done → gold start ngay  ✅
  Tổng cố định ≥ 2h, dễ sai      Tổng = Σ thời gian thực, luôn đúng thứ tự
```

---

## 5. Trụ Cột 3 — Correlation (run_id) & Barrier (Wait/Notify)

### 5.1 run_id — khóa tương quan xuyên suốt

`[1] Metadata Controller` sinh **một** `run_id` cho mỗi lần trigger:

```
run_id = RUN_${now():format('yyyyMMdd_HHmmss')}_${UUID():substring(0,4)}
```

`run_id` được propagate qua **mọi** stage và ghi vào `pipeline_execution_log.run_id` (cột mới — Doc 10 §3.5). Lợi ích:
- **Lineage:** biết bronze/silver/gold nào thuộc cùng 1 lần chạy.
- **Idempotency / guard:** "silver đã chạy cho run_id này chưa?"
- **Fan-in barrier:** dùng làm signal id cho Wait/Notify.

### 5.2 Fan-in barrier cho stage có NHIỀU parent

`GLD_bank_kpis` phụ thuộc `SLV_transactions` **và** `SLV_merchants`. Nếu chỉ dùng "Resolve Next" thì gold bị trigger **2 lần** (mỗi parent 1 lần) → sai. Cần **barrier**: chỉ chạy gold khi **tất cả** parent xong.

Dùng cặp processor `Notify` / `Wait` của NiFi trên một `DistributedMapCacheServer`:

```
SLV_transactions done ──▶ Notify(signal = ${run_id}:GLD_bank_kpis, counter +1)
SLV_merchants    done ──▶ Notify(signal = ${run_id}:GLD_bank_kpis, counter +1)

GLD_bank_kpis FlowFile ──▶ Wait(signal = ${run_id}:GLD_bank_kpis,
                                target = số phần tử trong depends_on = 2)
                            └─ release CHỈ KHI counter ≥ 2 ──▶ execute gold
```

`target signal count` = số lượng `pipeline_id` trong `depends_on` của gold (đếm phần tử comma-separated). Khi đủ → Wait nhả FlowFile → gold chạy đúng **một** lần.

> **Single-parent (đa số trường hợp):** không cần Wait/Notify — "Resolve Next" là đủ. Chỉ bật barrier cho node có `depends_on` chứa ≥ 2 id. NiFi có thể `RouteOnAttribute` theo "depends_on có chứa dấu phẩy không" để quyết định đi nhánh Wait hay nhánh thẳng.

### 5.3 Controller Services bổ sung (thêm vào Doc 11 §1.2)

| Controller Service        | Type                        | Mục đích                          |
|---------------------------|-----------------------------|-----------------------------------|
| `dmc-server`              | DistributedMapCacheServer   | Backend lưu trạng thái Wait/Notify|
| `dmc-client`              | DistributedMapCacheClientService | Wait/Notify đọc/ghi signal   |

---

## 6. Safety — An Toàn & Chống Lỗi

| Tình huống | Cơ chế bảo vệ |
|------------|----------------|
| **Bronze fail** | Connection `success` không fire → "Resolve Next" không chạy → silver **không** được trigger. Tùy chọn: ghi log downstream status = `upstream_failed`/`skipped`. |
| **Manual re-run silver lẻ** | **Guard (sensor)** đầu mỗi stage: SELECT execution_log kiểm tra parent đã `success` (cho run_id mới nhất) chưa. Nếu chưa → route sang `skipped` + cảnh báo, không xử lý data rỗng. |
| **Chạy lặp / trùng** | `run_id` + `execution_log`: trước khi execute, check stage đã `success` với run_id này chưa → bỏ qua nếu rồi (idempotent). |
| **Stage treo / chạy quá lâu** | `Wait` có `Expiration Duration`; ExecuteSQL có timeout + processor-level retry + backpressure. Hết hạn → route `failure` → alert (Doc 13 §3). |
| **Fan-in lệch** | `Wait` chỉ nhả khi counter ≥ target; parent thiếu → gold không chạy (an toàn hơn chạy thiếu data). Hết `Expiration` → alert. |
| **Thứ tự rule trong stage** | `transform_rules.execution_order` + `depends_on` đảm bảo create_table chạy trước merge. |

### 6.1 Guard / Sensor SQL (đầu mỗi non-root stage)

```sql
-- Trả về số parent đã success cho run_id hiện tại
SELECT COUNT(DISTINCT pipeline_id) AS ready_parents
FROM minio-datalake.metadata.pipeline_execution_log
WHERE run_id = '${run_id}'
  AND status = 'success'
  AND pipeline_id IN ( <expand depends_on thành danh sách quoted> )
```

So `ready_parents` với số phần tử của `depends_on`:
- Bằng nhau → tiếp tục execute.
- Nhỏ hơn → `RouteOnAttribute` sang `wait`/`skipped`.

---

## 7. Kiến Trúc NiFi Mới (Tổng Thể)

```
┌──────────────────────────────────────────────────────────────────────────┐
│ PG: [1] Metadata Controller                                               │
│   Trigger (CRON, chỉ 1 lần/ngày) → sinh run_id                            │
│   → ExecuteSQL: SELECT * pipeline_config WHERE depends_on IS NULL (ROOT)  │
│   → Split per root → Output Port: to-stage-router                         │
└───────────────────────────────┬──────────────────────────────────────────┘
                                 │ (chỉ root: BRZ_*)
┌───────────────────────────────▼──────────────────────────────────────────┐
│ PG: [2] Stage Router  (Input Port: from-controller / from-resolver)       │
│   RouteOnAttribute theo target_layer:                                     │
│     bronze  ─▶ to-bronze                                                  │
│     silver  ─▶ to-silver                                                  │
│     gold    ─▶ to-gold  (qua Wait barrier nếu multi-parent)              │
└──────┬────────────────────┬─────────────────────────┬─────────────────────┘
       │                    │                          │
┌──────▼─────┐      ┌───────▼────────┐        ┌────────▼─────────┐
│ PG:[3]     │      │ PG:[4]         │        │ PG:[5]           │
│ Bronze     │      │ Silver         │        │ Gold             │
│ Ingestion  │      │ Transform      │        │ Transform        │
│ (Doc 11 §5)│      │ Orchestrator   │        │ Orchestrator     │
└──────┬─────┘      └───────┬────────┘        └────────┬─────────┘
       │ success            │ success                  │ success
       ▼                    ▼                          ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ PG: [6] Resolve Next Stages  (dùng chung)                                  │
│   ExecuteSQL: SELECT pipeline_id WHERE depends_on chứa ${pipeline_id}     │
│   → Split → set run_id + upstream_* → Notify (nếu cần barrier)            │
│   → Output Port: to-stage-router  (LOOP về [2])                           │
└──────────────────────────────────────┬───────────────────────────────────┘
                                        │ (nếu rỗng → kết thúc)
                                        ▼  loop về Stage Router
```

> Mỗi executor (`[3]/[4]/[5]`) **bắt đầu** bằng "Load own config" (re-query `pipeline_config WHERE pipeline_id = ${next_pipeline_id}`) và **kết thúc** bằng route tới `[6] Resolve Next`. Nhờ vòng lặp `Router → Executor → Resolve → Router`, **một** engine đi hết DAG mà không hard-code số tầng.

---

## 8. Migration & Hướng Dẫn Build

Toàn bộ thay đổi đã được tích hợp vào [Doc 11 v2.0](11-nifi-dynamic-pipeline-setup.md):
- **Build từng processor:** Doc 11 §4–§10.
- **Migration từ v1.0 (time-based):** Doc 11 §12 (bảng việc cần làm).

Tóm tắt khác biệt cốt lõi so với cách cũ:

| Cũ (time-based) | Mới (event-driven) |
|-----------------|--------------------|
| Controller trigger cả 3 layer theo `target_layer` | Controller chỉ trigger ROOT (`depends_on IS NULL`) |
| Silver/Gold có `GenerateFlowFile` CRON riêng (3h, 4h sáng) | **Bỏ.** Stage sau được "Resolve Next" của stage trước đánh thức |
| Silver dùng config bronze (JOIN cùng pipeline_id) | Mỗi stage **Load Own Config** theo `pipeline_id` của chính nó |
| Không có run_id | Mọi stage propagate `run_id`, ghi execution_log để lineage/guard |

---

## 9. Checklist Triển Khai

```
□ ALTER/recreate pipeline_config: thêm dataset, source_layer, depends_on (Doc 10 §3.1)
□ ALTER/recreate pipeline_execution_log: thêm run_id (Doc 10 §3.5)
□ Tách config: mỗi dataset → BRZ_*, SLV_*, GLD_* rows riêng, set depends_on
□ Re-point transform_rules/column_mapping/dq_rules.pipeline_id sang stage sở hữu
□ Tạo Controller Services: dmc-server, dmc-client
□ Sửa [1] Controller: sinh run_id, query WHERE depends_on IS NULL
□ Tạo [2] Stage Router (route by target_layer)
□ Mỗi executor: thêm "Load own config" ở đầu
□ Tạo [6] Resolve Next + loop về Stage Router
□ Thêm Wait/Notify cho các gold multi-parent
□ Thêm Guard sensor cho non-root stages
□ Test: trigger root → quan sát DAG tự chạy bronze→silver→gold theo run_id
□ Verify execution_log: cùng run_id, 3 layer, đúng pipeline_id riêng
```

---

## 10. Tài Liệu Liên Quan

| Doc | Nội dung |
|-----|----------|
| [09-metadata-driven-strategy.md](09-metadata-driven-strategy.md) | Triết lý metadata-driven, ELT pattern |
| [10-metadata-tables-design.md](10-metadata-tables-design.md) | Schema (đã cập nhật: depends_on, source_layer, dataset, run_id) |
| [11-nifi-dynamic-pipeline-setup.md](11-nifi-dynamic-pipeline-setup.md) | Internals từng PG (ingestion + transform) |
| [12-dynamic-sql-templates.md](12-dynamic-sql-templates.md) | SQL templates bronze/silver/gold |
| [13-pipeline-operations-runbook.md](13-pipeline-operations-runbook.md) | Vận hành, monitor, troubleshoot |
</content>
</invoke>
