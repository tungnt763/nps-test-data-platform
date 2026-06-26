# NiFi Dynamic Pipeline Setup

> **Phiên bản:** 2.0 | **Ngày:** 2026-06-25
> **Yêu cầu:** Dremio JDBC driver trong NiFi, metadata tables đã tạo (Doc 10)
> **Tham chiếu:** [09-metadata-driven-strategy.md](09-metadata-driven-strategy.md) | [10-metadata-tables-design.md](10-metadata-tables-design.md) | [14-pipeline-dependency-orchestration.md](14-pipeline-dependency-orchestration.md)
>
> **⚠️ v2.0 thay đổi lớn so với v1.0:** Bỏ hoàn toàn cơ chế **time-based trigger** (silver chạy
> theo CRON lệch giờ) và mô hình **config 1-dòng/bảng**. Thay bằng **event-driven orchestration**:
> mỗi layer-transition là 1 stage độc lập, stage trước xong sẽ tự đánh thức stage sau qua
> `depends_on`. Xem lý do ở [Doc 14](14-pipeline-dependency-orchestration.md).
> Nếu bạn đã build theo v1.0, xem **§12 Migration** ở cuối.

---

## 1. Tổng Quan NiFi Flow (v2.0)

### 1.1 Nguyên tắc

```
1 stage = 1 dòng pipeline_config (có pipeline_id riêng + depends_on)
NiFi đọc DAG từ metadata lúc runtime → đi hết các stage theo cạnh depends_on
Connection "success" của NiFi CHÍNH LÀ dependency — không dùng đồng hồ
FlowFile giữa các stage chỉ mang correlation key (run_id, next_pipeline_id)
Mỗi executor TỰ nạp config của chính nó (Load Own Config) — không kế thừa tầng trước
```

### 1.2 Process Group Hierarchy

```
Root Process Group
│
├── PG: [1] Metadata Controller
│   ├── Trigger (CRON, 1 lần/ngày)
│   ├── Sinh run_id
│   ├── Đọc ROOT stages (depends_on IS NULL) từ pipeline_config
│   ├── Split per root → set next_pipeline_id, target_layer, run_id
│   └── Output Port: to-router
│
├── PG: [2] Stage Router
│   ├── Input Port: from-upstream  (nhận từ Controller VÀ từ Resolve Next)
│   ├── RouteOnAttribute theo target_layer
│   └── Output Ports: to-bronze, to-silver, to-gold
│
├── PG: [3] Bronze Ingestion
│   ├── Input Port: from-router
│   ├── Load Own Config (theo next_pipeline_id)
│   ├── Route by load_type (full/incremental)
│   ├── Get last watermark (incremental) — keyed theo pipeline_id bronze
│   ├── Execute dynamic SQL on source DB → Parquet → MinIO
│   ├── Log execution (kèm run_id)
│   └── Output Port: to-resolver
│
├── PG: [4] Silver Transform Orchestrator
│   ├── Input Port: from-router
│   ├── (Wait barrier — chỉ khi multi-parent, xem §10)
│   ├── Load Own Config (theo next_pipeline_id = SLV_*)
│   ├── Read transform_rules WHERE pipeline_id = SLV_* ORDER BY execution_order
│   ├── Render + Execute SQL trên Dremio
│   ├── DQ checks (tùy chọn)
│   ├── Log execution (kèm run_id, layer='silver')
│   └── Output Port: to-resolver
│
├── PG: [5] Gold Transform Orchestrator
│   └── (giống Silver, layer='gold'; multi-parent dùng Wait/Notify — §10)
│
├── PG: [6] Resolve Next Stages
│   ├── Input Port: from-executor  (nhận từ Bronze/Silver/Gold)
│   ├── Query downstream: WHERE depends_on chứa ${pipeline_id}
│   ├── Split → set next_pipeline_id, target_layer, giữ run_id
│   ├── (Notify — chỉ khi dùng barrier, §10)
│   └── Output Port: to-router   ──► LOOP về [2] Stage Router
│
└── PG: [7] Pipeline Monitor (tùy chọn)
    └── Query execution_log, alert on failures (Doc 13)
```

**Vòng lặp DAG:** `Controller → Router → Executor → Resolve Next → Router → ...` cho tới khi
Resolve Next không tìm thấy downstream (stage lá) thì luồng kết thúc tự nhiên.

### 1.3 Controller Services cần tạo

| Controller Service          | Type                              | Mục đích                          |
|-----------------------------|----------------------------------|-----------------------------------|
| `dremio-jdbc-pool`          | DBCPConnectionPool               | Connect NiFi → Dremio             |
| `source-postgres-pool`      | DBCPConnectionPool               | Connect NiFi → Source DB          |
| `avro-reader`               | AvroReader                       | Đọc output ExecuteSQL             |
| `json-reader`               | JsonTreeReader                   | Đọc metadata JSON                 |
| `json-record-set-writer`    | JsonRecordSetWriter              | Ghi output dạng JSON              |
| `parquet-writer`            | ParquetRecordSetWriter           | Ghi Parquet cho bronze            |
| `s3-credentials`            | AWSCredentialsProvider           | MinIO S3 credentials              |
| `dmc-server` *(§10)*        | DistributedMapCacheServer        | Backend Wait/Notify + dedup       |
| `dmc-client` *(§10)*        | DistributedMapCacheClientService | Client cho Wait/Notify + dedup    |

> `dmc-server`/`dmc-client` chỉ bắt buộc nếu DAG của bạn có stage **nhiều parent** (fan-in).
> DAG dạng cây/tuyến tính (mỗi stage 1 parent) không cần — bỏ qua §10.

---

## 2. Prerequisites — Cài Đặt JDBC Drivers

### 2.1 Dremio + PostgreSQL JDBC Driver

```bash
# Dremio JDBC driver
kubectl exec -n data-ingestion nifi-0 -- curl -L -o /opt/nifi/nifi-current/lib/dremio-jdbc-driver-24.3.2.jar "https://download.dremio.com/jdbc-driver/24.3.2-202401241821100032-d2d8a497/dremio-jdbc-driver-24.3.2-202401241821100032-d2d8a497.jar"

# PostgreSQL JDBC driver
kubectl exec -n data-ingestion nifi-0 -- curl -L -o /opt/nifi/nifi-current/lib/postgresql-42.7.2.jar "https://jdbc.postgresql.org/download/postgresql-42.7.2.jar"

# Verify
kubectl exec -n data-ingestion nifi-0 -- ls -la /opt/nifi/nifi-current/lib/ | grep -E "dremio|postgresql"
```

### 2.2 Restart NiFi để load drivers

```bash
kubectl delete pod -n data-ingestion nifi-0
kubectl wait --for=condition=ready pod/nifi-0 -n data-ingestion --timeout=300s
```

> **Lưu ý:** Driver mất khi pod restart. Xem §11 để setup initContainer cho permanent fix.

---

## 3. Controller Services Setup

Truy cập NiFi UI: `https://localhost:8444/nifi` → Controller Settings (gear) → Management Controller Services.

### 3.1 Dremio JDBC Connection Pool

| Property                    | Value                                                           |
|-----------------------------|-----------------------------------------------------------------|
| **Name**                    | `dremio-jdbc-pool`                                              |
| **Type**                    | `DBCPConnectionPool`                                            |
| **Database Connection URL** | `jdbc:dremio:direct=dremio.data-processing.svc.cluster.local:31010` |
| **Database Driver Class Name** | `com.dremio.jdbc.Driver`                                     |
| **Database Driver Location(s)** | `/opt/nifi/nifi-current/lib/dremio-jdbc-driver-24.3.2.jar` |
| **Database User**           | `admin`                                                         |
| **Password**                | (Dremio admin password)                                         |
| **Max Total Connections**   | `5`                                                             |

→ **Enable**

### 3.2 Source PostgreSQL Connection Pool

| Property                    | Value                                                           |
|-----------------------------|-----------------------------------------------------------------|
| **Name**                    | `source-postgres-pool`                                          |
| **Type**                    | `DBCPConnectionPool`                                            |
| **Database Connection URL** | `jdbc:postgresql://postgres-superset-postgresql.data-visualization.svc.cluster.local:5432/superset` |
| **Database Driver Class Name** | `org.postgresql.Driver`                                      |
| **Database Driver Location(s)** | `/opt/nifi/nifi-current/lib/postgresql-42.7.2.jar`          |
| **Database User**           | `superset`                                                      |
| **Password**                | `SupersetPostgres2024`                                          |
| **Max Total Connections**   | `5`                                                             |

→ **Enable**

### 3.3 Record Services

| Name | Type | Cấu hình quan trọng |
|------|------|---------------------|
| `avro-reader` | `AvroReader` | (default) |
| `json-reader` | `JsonTreeReader` | Schema Access Strategy = `Infer Schema` |
| `json-record-set-writer` | `JsonRecordSetWriter` | Schema Access Strategy = `Inherit Record Schema` |

→ Enable cả 3.

---

## 4. Process Group [1]: Metadata Controller

Đây là điểm khởi động duy nhất của toàn bộ DAG: sinh `run_id`, đọc các **root stage**
(ingestion), và đẩy chúng vào Stage Router.

Right-click canvas → **Add Process Group** → `[1] Metadata Controller`. Double-click để mở.

### 4.1 Processor: GenerateFlowFile (Scheduler Tick — mỗi 60s)

> **KHÔNG dùng 1 CRON cố định trigger tất cả bảng.** Vấn đề: CRON `0 0 2 * * ?` sẽ chạy **mọi**
> root lúc 2h — bỏ qua `schedule_cron` riêng của từng bảng (bảng hẹn 3h vẫn chạy 2h), và bung tất
> cả cùng lúc → bottleneck. Thay vào đó controller **tick đều mỗi phút**, rồi ở §4.3 **lọc đúng bảng
> đến giờ** theo `schedule_cron` của chính nó.

| Property               | Value                                     |
|------------------------|-------------------------------------------|
| **Name**               | `Scheduler Tick`                          |
| **Scheduling Strategy**| `TIMER_DRIVEN`                            |
| **Run Schedule**       | `60 sec`                                  |
| **Custom Text**        | `tick`                                    |

> Tick mỗi phút rất nhẹ (chỉ 1 query metadata nhỏ). Muốn độ phân giải khác (vd 30s) thì chỉnh
> Run Schedule; cron của bảng nên dùng độ phân giải ≥ tick.

### 4.2 Processor: UpdateAttribute (Generate Run ID) — MỚI

Sinh **một** `run_id` cho cả lần chạy; nó sẽ propagate xuống mọi stage để tương quan/lineage.

| Property  | Value                                                              |
|-----------|-------------------------------------------------------------------|
| **Name**  | `Generate Run ID`                                                 |
| `run_id`  | `RUN_${now():format('yyyyMMddHHmmss')}_${UUID():substring(0,8)}`  |

**Connection:** `Scheduler Tick` → success → `Generate Run ID`

### 4.3 Processor: ExecuteSQL (Read **Due** Root Pipelines)

Chỉ đọc root (`depends_on IS NULL`) **đến giờ ở phút hiện tại** theo `schedule_cron` của **chính
bảng đó**. Parse cron `min hour dom mon dow` bằng `SPLIT_PART` (hỗ trợ `*` hoặc số nguyên mỗi field —
đủ cho daily/weekly/monthly). Mỗi bảng chạy đúng giờ riêng ⇒ bảng hẹn 3h **không** bị chạy lúc 2h.

| Property                       | Value                       |
|--------------------------------|-----------------------------|
| **Name**                       | `Read Due Root Pipelines`   |
| **Database Connection Pooling Service** | `dremio-jdbc-pool`  |
| **SQL select query**           | (xem dưới)                  |

```sql
SELECT pipeline_id, target_layer, priority
FROM "minio-datalake"."metadata".pipeline_config pc
WHERE is_active = true
  AND depends_on IS NULL
  -- Khớp cron 'min hour dom mon dow' với thời điểm hiện tại (mỗi field: '*' hoặc số)
  AND (SPLIT_PART(schedule_cron,' ',1) = '*' OR CAST(SPLIT_PART(schedule_cron,' ',1) AS INT) = EXTRACT(MINUTE FROM CURRENT_TIMESTAMP))
  AND (SPLIT_PART(schedule_cron,' ',2) = '*' OR CAST(SPLIT_PART(schedule_cron,' ',2) AS INT) = EXTRACT(HOUR   FROM CURRENT_TIMESTAMP))
  AND (SPLIT_PART(schedule_cron,' ',3) = '*' OR CAST(SPLIT_PART(schedule_cron,' ',3) AS INT) = EXTRACT(DAY    FROM CURRENT_TIMESTAMP))
  AND (SPLIT_PART(schedule_cron,' ',4) = '*' OR CAST(SPLIT_PART(schedule_cron,' ',4) AS INT) = EXTRACT(MONTH  FROM CURRENT_TIMESTAMP))
  AND (SPLIT_PART(schedule_cron,' ',5) = '*' OR CAST(SPLIT_PART(schedule_cron,' ',5) AS INT) = (DAYOFWEEK(CURRENT_TIMESTAMP) - 1))
  -- GUARD chống trigger trùng trong cùng phút (xem §4.6)
  AND NOT EXISTS (
      SELECT 1 FROM "minio-datalake"."metadata".pipeline_execution_log l
      WHERE l.pipeline_id = pc.pipeline_id
        AND l.layer = 'bronze'
        AND l.start_time >= CURRENT_TIMESTAMP - INTERVAL '90' SECOND
  )
ORDER BY priority, pipeline_id
```

> - Cron dùng convention chuẩn: field 5 (dow) `0=Chủ nhật..6=Thứ Bảy`; Dremio `DAYOFWEEK` trả
>   `1=CN..7=T7` nên trừ 1. `'0 3 * * 1'` = 03:00 thứ Hai.
> - Chỉ hỗ trợ `*` hoặc **một số** mỗi field (không `*/5`, `1-5`, `1,3`). Cần cron đầy đủ → xem
>   "Alternative" cuối §4.6.
> - Nếu tick rỗng (không bảng nào đến giờ) → SplitJson không sinh FlowFile → tick kết thúc, vô hại.

**Connection:** `Generate Run ID` → success → `Read Due Root Pipelines`

### 4.4 ConvertAvroToJSON → SplitJson → EvaluateJsonPath

| Processor | Cấu hình |
|-----------|----------|
| `ConvertAvroToJSON` (`Config to JSON`) | JSON Format = `One line per Avro record` |
| `SplitJson` (`Split Per Root`) | JsonPath Expression = `$[*]` |
| `EvaluateJsonPath` (`Set Correlation Attrs`) | Destination = `flowfile-attribute`; `next_pipeline_id` = `$.pipeline_id`, `target_layer` = `$.target_layer`, `priority` = `$.priority` |

> `run_id` đã là attribute (set ở §4.2) nên tự đi theo qua Split. Sau bước này mỗi FlowFile mang:
> `run_id`, `next_pipeline_id`, `target_layer`, `priority`.

**Connections:** `Read Due Root Pipelines` → `Config to JSON` → `Split Per Root` (split) → `Set Correlation Attrs`

### 4.5 (Guard) Ghi log `queued` + Output Port

**Trước** khi đẩy ra router, ghi một dòng `execution_log` trạng thái `queued` cho mỗi bảng vừa chọn —
đây chính là nguồn cho GUARD ở §4.3 (lần tick sau thấy bảng đã được trigger ⇒ không trigger lại).

`ReplaceText` (`Build Queued Log SQL`) → `ExecuteSQL` (`Insert Queued Log`, `dremio-jdbc-pool`):

```sql
INSERT INTO "minio-datalake"."metadata".pipeline_execution_log
(execution_id, run_id, pipeline_id, pipeline_name, layer, status, execution_params, start_time)
VALUES ('${next_pipeline_id}_${now():format('yyyyMMddHHmmss')}', '${run_id}', '${next_pipeline_id}',
        '${next_pipeline_id}', 'bronze', 'queued', 'scheduled',
        CAST('${now():format('yyyy-MM-dd HH:mm:ss')}' AS TIMESTAMP))
```

Tạo Output Port `to-router`. **Connection:** `Set Correlation Attrs` → `Build Queued Log SQL` →
`Insert Queued Log` → `to-router`.

> Bronze (§6.7) khi xong sẽ INSERT dòng `success` riêng; dòng `queued` đủ để guard hoạt động ngay
> trong cùng phút (kể cả khi tick lỡ chạy 2 lần). Đơn giản hơn nếu chấp nhận rủi ro nhỏ: bỏ bước này,
> guard chỉ dựa trên log `success`/`running` — nhưng nên giữ để chắc chắn không trùng.

### 4.6 Chống Bottleneck — Throttle Concurrency

Khi **nhiều bảng cùng đến giờ** (vd 50 bảng đều `0 2 * * *`), không để chúng bung một lúc làm nghẽn
source DB / NiFi / Dremio. Ba lớp bảo vệ (kết hợp):

1. **Stagger lịch (lớp 1):** đặt `schedule_cron` lệch phút nhau (`0 2`, `10 2`, `20 2`, ...) — phân
   tán tự nhiên (Doc 10 §4.1).
2. **Backpressure (lớp 2):** trên connection `[2] Stage Router` (`to-bronze`) → `[3] Bronze`, đặt
   **Back Pressure Object Threshold** = `5` (hoặc theo sức chứa). Controller có bơm nhiều FlowFile thì
   chúng **xếp hàng**, không tràn xuống bronze.
3. **Concurrent Tasks (lớp 3):** processor `Execute Source Query` (§6.5) đặt **Concurrent Tasks** =
   `3` → tối đa 3 bảng kéo source song song; phần còn lại chờ trong queue rồi rút dần.

**Ưu tiên thứ tự:** trên queue vào bronze, dùng **PriorityAttributePrioritizer** với attribute
`priority` (số nhỏ chạy trước) → bảng quan trọng đi trước khi đông.

> Kết quả: tất cả bảng đến giờ **vẫn chạy hết**, nhưng theo từng đợt N-cái-một ⇒ không bottleneck.
> Backpressure + Concurrent Tasks là van an toàn ngay cả khi lịch trùng.

> **Alternative (cron đầy đủ):** cần `*/15`, `1-5`, `1,3,5`... thì thay query SPLIT_PART ở §4.3 bằng
> `ExecuteScript` (Groovy + thư viện `cron-utils`) đánh giá `schedule_cron` so với thời điểm hiện tại.
> Đắt hơn (cần thêm jar) nhưng hỗ trợ cron đầy đủ.

### 4.7 Flow Diagram — Controller

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  PG: [1] Metadata Controller                                                   │
│                                                                                │
│  ┌────────────────────┐                                                        │
│  │ Scheduler Tick     │  GenerateFlowFile · TIMER 60s · text="tick"            │
│  │ (mỗi 60 giây)      │                                                        │
│  └─────────┬──────────┘                                                        │
│            │ success                                                           │
│  ┌─────────▼──────────┐                                                        │
│  │ Generate Run ID    │  UpdateAttribute · run_id=RUN_<ts>_<uuid8>             │
│  └─────────┬──────────┘                                                        │
│            │ success                                                           │
│  ┌─────────▼─────────────────────────┐                                        │
│  │ Read DUE Root Pipelines           │  ExecuteSQL → Dremio                    │
│  │ • depends_on IS NULL              │  • khớp schedule_cron với phút hiện tại │
│  │ • SPLIT_PART(cron) = now fields   │  • GUARD: NOT EXISTS log ≤ 90s          │
│  │ • ORDER BY priority               │  → 0 dòng nếu chưa tới giờ (tick nghỉ)  │
│  └─────────┬─────────────────────────┘                                        │
│            │ success (mảng config đến giờ)                                     │
│  ┌─────────▼──────────┐   ┌──────────────────┐   ┌───────────────────────────┐│
│  │ Config to JSON     │──▶│ Split Per Root   │──▶│ Set Correlation Attrs     ││
│  │ (ConvertAvroToJSON)│   │ (SplitJson $[*]) │   │ next_pipeline_id,         ││
│  └────────────────────┘   └──────────────────┘   │ target_layer, priority    ││
│                                                   └─────────────┬─────────────┘│
│                                                        (mỗi bảng = 1 FlowFile) │
│  ┌─────────────────────────────────┐                           │              │
│  │ Insert Queued Log (GUARD)       │◀──────────────────────────┘              │
│  │ ExecuteSQL · status='queued'    │  → để tick sau không trigger trùng       │
│  └─────────┬───────────────────────┘                                          │
│            │ success                                                           │
│  ┌─────────▼──────────┐                                                        │
│  │ OUT: to-router     │  (BackPressure Object Threshold = 5 ở connection sau)  │
│  └─────────┬──────────┘                                                        │
└────────────┼───────────────────────────────────────────────────────────────┘
             │  →  [2] Stage Router (Concurrent Tasks + Priority điều tiết tải)
             ▼
```

**Tóm tắt cơ chế:** tick đều mỗi phút → **chỉ** bảng đến giờ (theo cron riêng) được chọn → guard
chặn trùng → mỗi bảng thành 1 FlowFile mang `run_id` → ghi `queued` → ra router. Backpressure +
Concurrent Tasks ở hạ nguồn rải tải, tránh bottleneck khi nhiều bảng trùng giờ.

---

## 5. Process Group [2]: Stage Router

Điểm hội tụ: nhận FlowFile từ **Controller** (root) và từ **Resolve Next** (downstream), rồi
route theo `target_layer` tới đúng executor. Đây là trục của vòng lặp DAG.

Root canvas → Add PG → `[2] Stage Router`. Kéo connection `[1] Metadata Controller`
(`to-router`) → `[2] Stage Router`. Mở PG.

### 5.1 Input Port

Tạo Input Port: `from-upstream`.

### 5.2 Processor: RouteOnAttribute (Route by Target Layer)

| Property | Value |
|----------|-------|
| **Name** | `Route by Target Layer` |
| **Routing Strategy** | `Route to Property name` |

**Dynamic Properties:**

| Property Name | Value |
|---------------|-------|
| `bronze` | `${target_layer:equals('bronze')}` |
| `silver` | `${target_layer:equals('silver')}` |
| `gold`   | `${target_layer:equals('gold')}` |

**Connection:** `from-upstream` → `Route by Target Layer`

### 5.3 Output Ports

Tạo 3 Output Ports, nối từ RouteOnAttribute:
- `to-bronze` ← relationship `bronze`
- `to-silver` ← relationship `silver`
- `to-gold` ← relationship `gold`

---

## 6. Process Group [3]: Bronze Ingestion

Root canvas → Add PG → `[3] Bronze Ingestion`. Kéo connection `[2] Stage Router` (`to-bronze`)
→ `[3] Bronze Ingestion`. Mở PG. Tạo Input Port `from-router`.

### 6.1 Processor: ExecuteSQL (Load Own Config) — đầu mỗi stage

Mỗi stage TỰ nạp config của chính nó từ `next_pipeline_id` (không kế thừa attribute tầng trước).

| Property | Value |
|----------|-------|
| **Name** | `Load Own Config` |
| **Database Connection Pooling Service** | `dremio-jdbc-pool` |
| **SQL select query** | (xem dưới) |

```sql
SELECT pipeline_id, pipeline_name, source_connection, source_schema, source_table,
       load_type, primary_keys, watermark_column, partition_columns,
       target_layer, target_path, target_table, batch_size
FROM "minio-datalake"."metadata".pipeline_config
WHERE pipeline_id = '${next_pipeline_id}'
  AND is_active = true
```

→ `ConvertAvroToJSON` (`Config to JSON`) → `EvaluateJsonPath` (`Extract Config Attributes`,
Destination = `flowfile-attribute`) với các property:

| Property | JsonPath |
|----------|----------|
| `pipeline_id` | `$.pipeline_id` |
| `pipeline_name` | `$.pipeline_name` |
| `source_schema` | `$.source_schema` |
| `source_table` | `$.source_table` |
| `load_type` | `$.load_type` |
| `primary_keys` | `$.primary_keys` |
| `watermark_column` | `$.watermark_column` |
| `partition_columns` | `$.partition_columns` |
| `target_table` | `$.target_table` |
| `batch_size` | `$.batch_size` |

**Connections:** `from-router` → `Load Own Config` → `Config to JSON` → `Extract Config Attributes`

### 6.2 Processor: RouteOnAttribute (Route by Load Type)

| Property | Value |
|----------|-------|
| **Name** | `Route by Load Type` |
| **Routing Strategy** | `Route to Property name` |

| Property Name | Value |
|---------------|-------|
| `full` | `${load_type:equals('full')}` |
| `incremental` | `${load_type:equals('incremental')}` |

**Connection:** `Extract Config Attributes` → matched → `Route by Load Type`

### 6.3 Full Load Path — ReplaceText (Build Full Load SQL)

| Property | Value |
|----------|-------|
| **Name** | `Build Full Load SQL` |
| **Search Value** | `(?s)(^.*$)` |
| **Replacement Value** | `SELECT * FROM ${source_schema}.${source_table}` |
| **Replacement Strategy** | `Regex Replace` |
| **Evaluation Mode** | `Entire text` |

**Connection:** `Route by Load Type` → `full` → `Build Full Load SQL`

### 6.4 Incremental Load Path

**ExecuteSQL (`Get Last Watermark`)** trên `dremio-jdbc-pool` — keyed theo **pipeline_id bronze**:

```sql
SELECT COALESCE(MAX(last_watermark), '1970-01-01 00:00:00') AS last_watermark
FROM "minio-datalake"."metadata".pipeline_execution_log
WHERE pipeline_id = '${pipeline_id}'
  AND layer = 'bronze'
  AND status = 'success'
```

→ `ConvertAvroToJSON` (`Watermark to JSON`) → `EvaluateJsonPath` (`Extract Watermark`:
`last_watermark` = `$.last_watermark`) → `ReplaceText` (`Build Incremental SQL`):

| Property | Value |
|----------|-------|
| **Search Value** | `(?s)(^.*$)` |
| **Replacement Value** | `SELECT * FROM ${source_schema}.${source_table} WHERE ${watermark_column} > '${last_watermark}'` |
| **Replacement Strategy** | `Regex Replace` |
| **Evaluation Mode** | `Entire text` |

**Connections:** `Route by Load Type` → `incremental` → `Get Last Watermark` → `Watermark to JSON` → `Extract Watermark` → `Build Incremental SQL`

### 6.5 Execute Source Query

**ExecuteSQL (`Execute Source Query`)** trên `source-postgres-pool`, **SQL select query để trống**
(đọc SQL từ FlowFile content).

**Connections:** `Build Full Load SQL` → `Execute Source Query`; `Build Incremental SQL` → `Execute Source Query`.

### 6.6 Write to MinIO

**UpdateAttribute (`Set S3 Output Path`):**

| Property | Value |
|----------|-------|
| `filename` | `${source_table}_${now():format('yyyyMMdd_HHmmss')}.avro` |
| `s3.key` | `bronze/${source_table}/dt=${now():format('yyyy-MM-dd')}/${filename}` |

**PutS3Object (`Write to MinIO Bronze`):**

| Property | Value |
|----------|-------|
| **Object Key** | `${s3.key}` |
| **Bucket** | `napas-datalake` |
| **Access Key ID** | `napas-admin` |
| **Secret Access Key** | `napas-minio-s3cr3t-2024` |
| **Endpoint Override URL** | `http://minio.data-storage.svc.cluster.local:9000` |
| **Signer Override** | `AWSS3V4SignerType` |
| **Region** | `us-east-1` |
| **Use Path Style Access** | `true` |

**Connections:** `Execute Source Query` → `Set S3 Output Path` → `Write to MinIO Bronze`

### 6.7 Log Execution (kèm run_id)

**ReplaceText (`Build Execution Log SQL`):** Search `(?s)(^.*$)`, Strategy `Regex Replace`, Entire text. Replacement Value:

```
INSERT INTO "minio-datalake"."metadata".pipeline_execution_log
(execution_id, run_id, pipeline_id, pipeline_name, layer, start_time, end_time,
 status, rows_processed, rows_inserted, rows_updated, rows_rejected,
 last_watermark, error_message, execution_params, created_at)
VALUES (
    '${pipeline_id}_${now():format('yyyyMMdd_HHmmss')}',
    '${run_id}', '${pipeline_id}', '${pipeline_name}', 'bronze',
    CAST('${now():format('yyyy-MM-dd HH:mm:ss')}' AS TIMESTAMP),
    CAST('${now():format('yyyy-MM-dd HH:mm:ss')}' AS TIMESTAMP),
    'success', ${executesql.row.count}, ${executesql.row.count}, 0, 0,
    '${now():format('yyyy-MM-dd HH:mm:ss')}', NULL, '${load_type}', CURRENT_TIMESTAMP
)
```

> **Dùng explicit column list** (không INSERT theo vị trí) — an toàn khi schema thêm cột.

**ExecuteSQL (`Insert Execution Log`)** trên `dremio-jdbc-pool`, query để trống.

**Connections:** `Write to MinIO Bronze` → `Build Execution Log SQL` → `Insert Execution Log`

### 6.8 Output Port → Resolve Next

Tạo Output Port `to-resolver`. **Connection:** `Insert Execution Log` → success → `to-resolver`.

> Lúc này FlowFile mang `pipeline_id` (bronze vừa xong) + `run_id` — vừa đủ để Resolve Next
> tìm downstream.

---

## 7. Process Group [4]: Silver Transform Orchestrator

Silver **không di chuyển data qua NiFi**: NiFi đọc `transform_rules` từ Dremio, gửi SQL cho Dremio
execute trực tiếp trên Iceberg. NiFi = orchestrator, Dremio = compute.

> **KHÔNG có GenerateFlowFile/CRON cho silver.** Silver được Resolve Next đánh thức sau khi bronze
> success. FlowFile vào silver chỉ mang `run_id` + `next_pipeline_id` (= `SLV_*`) + `target_layer`.

Root canvas → Add PG → `[4] Silver Transform Orchestrator`. Kéo `[2] Stage Router` (`to-silver`)
→ PG này. Mở PG. Tạo Input Port `from-router`.

### 7.1 Processor: ExecuteSQL (Load Own Config)

Giống §6.1 nhưng dùng cho silver — nạp config **của chính silver** (`load_type`, `primary_keys`,
`watermark_column`, `target_table` của tầng silver, độc lập với bronze):

```sql
SELECT pipeline_id, pipeline_name, source_layer, source_table, target_table,
       load_type, primary_keys, watermark_column, partition_columns
FROM "minio-datalake"."metadata".pipeline_config
WHERE pipeline_id = '${next_pipeline_id}'   -- vd 'SLV_transactions'
  AND is_active = true
```

→ `ConvertAvroToJSON` → `EvaluateJsonPath` (extract `pipeline_id`, `pipeline_name`, `target_table`,
`load_type`, `primary_keys`, `watermark_column`, `source_table`).

**Connection:** `from-router` → `Load Own Config` → ...

### 7.2 Build run_token + column lists, rồi dựng chuỗi step

**(a) run_token** — UpdateAttribute: `run_token = ${run_id:substring(4,12)}` (8 ký tự, đặt tên temp).

**(b) Build column lists từ column_mapping** — ExecuteSQL đọc `column_mapping WHERE pipeline_id =
'${pipeline_id}' ORDER BY column_order` → `ExecuteScript` (Groovy) ghép thành các attribute:
`${column_list}`, `${column_list_select}`, `${update_set_clause}`, `${insert_values_list}` (Doc 12 §6.3).
→ **không dùng `SELECT *`.**

**(c) Dựng danh sách step** — đa số bảng dùng **chuỗi template chuẩn từ `transform_templates`**
(generic, reusable), chọn theo `load_type` (Doc 12 §3.7):

```sql
-- Đọc thư viện template generic (1 query, dùng chung mọi bảng)
SELECT template_id, transform_type, sql_template
FROM "minio-datalake"."metadata".transform_templates
```

NiFi sắp các template theo chuỗi:
- `incremental`: `T_LOAD_STAGE → T_DEDUP → T_ENSURE_TARGET → T_MERGE → T_CLEANUP`
- `full`: `T_LOAD_STAGE → T_DEDUP → T_FULL_REPLACE → T_CLEANUP`

Bước **custom** (gold aggregation, filter) lấy thêm từ `transform_rules WHERE pipeline_id =
'${pipeline_id}'` và chèn theo `execution_order`.

**(d) Gán temp cho từng step** — với step thứ `i`:
`stage_out = "minio-datalake"."staging"."${target_table}_temp_${run_token}_${i}"`,
`stage_in` = `stage_out` của step `i-1` (step 1: `stage_in = ${source_fqn}` = bronze). `T_CLEANUP`
lặp drop mọi `..._temp_${run_token}_*`.

> ⚠️ **BẮT BUỘC chạy tuần tự đúng thứ tự** (mỗi step đọc temp của step trước):
> - `Render Transform SQL` + `Run Transform on Dremio`: **Concurrent Tasks = 1**.
> - Connection sau bước split: prioritizer **FirstInFirstOutPrioritizer**.
>
> SQL từng template: [12-dynamic-sql-templates.md](12-dynamic-sql-templates.md) §3.

### 7.3 Incremental: Get Silver Watermark (nếu load_type = incremental)

Giống bronze nhưng keyed theo **pipeline_id silver + layer='silver'**:

```sql
SELECT COALESCE(MAX(last_watermark), '1970-01-01 00:00:00') AS last_watermark
FROM "minio-datalake"."metadata".pipeline_execution_log
WHERE pipeline_id = '${pipeline_id}'   -- SLV_*
  AND layer = 'silver'
  AND status = 'success'
```

→ extract `last_watermark` thành attribute (dùng trong `sql_template` MERGE).

### 7.4 Processor: ReplaceText (Render Transform SQL)

| Property | Value |
|----------|-------|
| **Name** | `Render Transform SQL` |
| **Search Value** | `(?s)(^.*$)` |
| **Replacement Value** | `${sql_template}` |
| **Replacement Strategy** | `Regex Replace` |
| **Evaluation Mode** | `Entire text` |

> `${sql_template}` là template **generic** (từ `transform_templates`); các biến `${stage_in}`,
> `${stage_out}`, `${source_fqn}`, `${target_fqn}`, `${column_list}`, `${column_list_select}`,
> `${merge_on_clause}`, `${update_set_clause}`, `${insert_values_list}`, `${where_clause}`,
> `${order_by_clause}`, `${last_watermark}` đều đã là attribute (từ §7.2 + §7.3) nên Expression
> Language resolve hết. Chi tiết template: Doc 12 §3.
>
> Mỗi step ghi ra **temp riêng** (`${stage_out}`); chỉ bước publish cuối (`T_MERGE`/`T_FULL_REPLACE`)
> mới chạm bảng chính → silver không bao giờ dang dở. Bước cuối `T_CLEANUP` drop toàn bộ temp của run.

### 7.5 Processor: ExecuteSQL (Run Transform on Dremio)

`dremio-jdbc-pool`, query để trống (đọc từ content).

### 7.6 Log Execution

Giống §6.7 nhưng `layer = 'silver'` và `execution_params = '${load_type}'`. Vẫn explicit column list, kèm `run_id`.

### 7.7 Output Port → Resolve Next

Tạo Output Port `to-resolver`. **Connection:** `Insert Execution Log` → success → `to-resolver`.

### 7.8 (Tùy chọn) DQ Checks

Trước khi log success, chèn nhánh chạy `data_quality_rules` (WHERE pipeline_id = `${pipeline_id}`)
→ ExecuteSQL trên Dremio → route theo `severity` (Doc 12 §5). Có thể bổ sung sau.

---

## 8. Process Group [5]: Gold Transform Orchestrator

**Giống hệt Silver** (§7), chỉ khác:
- `layer = 'gold'` trong log.
- `target_layer` của các FlowFile đi vào đây là `gold` (đã route ở Stage Router).
- Read Transform Rules vẫn `WHERE r.pipeline_id = '${pipeline_id}'` (= `GLD_*`) — config của stage
  đã tự xác định `source_layer='silver'`, không cần điều kiện layer thủ công.
- **Multi-parent** (gold phụ thuộc ≥ 2 silver): thêm Wait barrier ở đầu PG — xem §10.

→ Copy PG Silver, rename `[5] Gold Transform Orchestrator`, sửa `layer` trong log. Kéo
`[2] Stage Router` (`to-gold`) → PG này.

---

## 9. Process Group [6]: Resolve Next Stages

Trái tim của event-driven: sau khi MỘT stage success, tìm các stage phụ thuộc nó và đẩy chúng
quay lại Stage Router.

Root canvas → Add PG → `[6] Resolve Next Stages`. Kéo connection từ **cả ba** executor
(`[3]/[4]/[5]` output port `to-resolver`) → PG này. Mở PG. Tạo Input Port `from-executor`.

### 9.1 Processor: ExecuteSQL (Find Downstream)

`dremio-jdbc-pool`:

```sql
SELECT pipeline_id AS next_pipeline_id, target_layer
FROM "minio-datalake"."metadata".pipeline_config
WHERE is_active = true
  AND ( depends_on = '${pipeline_id}'
        OR depends_on LIKE '${pipeline_id},%'
        OR depends_on LIKE '%,${pipeline_id}'
        OR depends_on LIKE '%,${pipeline_id},%' )
```

**Connection:** `from-executor` → `Find Downstream`

### 9.2 ConvertAvroToJSON → SplitJson → EvaluateJsonPath

| Processor | Cấu hình |
|-----------|----------|
| `ConvertAvroToJSON` (`Downstream to JSON`) | One line per record |
| `SplitJson` (`Split Per Downstream`) | `$[*]` |
| `EvaluateJsonPath` (`Set Next Attrs`) | `next_pipeline_id` = `$.next_pipeline_id`, `target_layer` = `$.target_layer` |

> `run_id` đi theo từ FlowFile của executor (giữ nguyên qua ExecuteSQL + Split). Nếu không có
> downstream nào, SplitJson không sinh FlowFile con → nhánh kết thúc (stage lá). Tự nhiên, đúng ý.

### 9.3 Output Port → loop về Router

Tạo Output Port `to-router`. **Connection:** `Set Next Attrs` (split) → `to-router`.
Ngoài root canvas: kéo `[6] Resolve Next Stages` (`to-router`) → `[2] Stage Router` (`from-upstream`).

→ Vòng lặp khép kín: Router phân phối, executor xử lý, Resolve Next tìm bước kế, lặp tới hết DAG.

---

## 10. (Nâng Cao) Fan-in: Stage Có Nhiều Parent

Bỏ qua mục này nếu DAG của bạn mỗi stage chỉ 1 parent (cây/tuyến tính).

**Vấn đề:** `GLD_bank_kpis` phụ thuộc `SLV_transactions` VÀ `SLV_merchants`. Khi mỗi silver xong,
Resolve Next sẽ tạo **một** FlowFile cho gold → gold bị trigger 2 lần. Cần **barrier**: gold chỉ
chạy **một lần**, sau khi **tất cả** parent xong.

### 10.1 Controller Services

Tạo `DistributedMapCacheServer` (`dmc-server`, port mặc định 4557) và
`DistributedMapCacheClientService` (`dmc-client`, Server Hostname = `localhost`). Enable cả hai.

### 10.2 Notify (trong Resolve Next)

Sau `Set Next Attrs`, thêm processor **Notify** trước Output Port:

| Property | Value |
|----------|-------|
| **Name** | `Notify Downstream Ready` |
| **Release Signal Identifier** | `${run_id}__${next_pipeline_id}` |
| **Signal Counter Name** | `${pipeline_id}` |
| **Distributed Cache Service** | `dmc-client` |

**Connection:** `Set Next Attrs` → `Notify Downstream Ready` → `to-router`.

### 10.3 DetectDuplicate + Wait (đầu PG Gold)

Trong `[5] Gold`, **trước** `Load Own Config`:

**DetectDuplicate (`Dedup Gold Trigger`)** — đảm bảo mỗi (run_id, gold) chỉ 1 FlowFile qua Wait:

| Property | Value |
|----------|-------|
| **Cache Entry Identifier** | `${run_id}__${next_pipeline_id}` |
| **Distributed Cache Service** | `dmc-client` |
| **Age Off Duration** | `12 hours` |

→ relationship `non-duplicate` → Wait; `duplicate` → auto-terminate.

**Wait (`Wait All Parents`):**

| Property | Value |
|----------|-------|
| **Release Signal Identifier** | `${run_id}__${next_pipeline_id}` |
| **Target Signal Count** | `${depends_on:replaceAll('[^,]','') :length():plus(1)}` |
| **Distributed Cache Service** | `dmc-client` |
| **Expiration Duration** | `1 hour` |

> **Target Signal Count** = số parent = (số dấu phẩy trong `depends_on`) + 1. Cần load
> `depends_on` vào attribute trước (thêm vào query Load Own Config). `Wait` chỉ nhả FlowFile (đã
> dedup) khi đủ số parent Notify; hết `Expiration` → route `expired` → alert.

**Connection:** `from-router` → `Dedup Gold Trigger` → (non-duplicate) → `Wait All Parents` → (success) → `Load Own Config`.

---

## 11. Permanent JDBC Drivers (initContainer)

Để không mất driver khi pod restart, thêm initContainer vào NiFi StatefulSet
(`napas-platform-infra/platform/charts/nifi/templates/statefulset.yaml`):

```yaml
initContainers:
  - name: download-jdbc-drivers
    image: curlimages/curl:8.5.0
    command: ["sh", "-c"]
    args:
      - |
        curl -L -o /drivers/postgresql-42.7.2.jar "https://jdbc.postgresql.org/download/postgresql-42.7.2.jar"
        curl -L -o /drivers/dremio-jdbc-driver-24.3.2.jar "https://download.dremio.com/jdbc-driver/24.3.2/dremio-jdbc-driver-24.3.2-202401241530580032-1f14e76d.jar"
    volumeMounts:
      - { name: jdbc-drivers, mountPath: /drivers }
# container nifi: thêm volumeMount /opt/nifi/nifi-current/drivers (name: jdbc-drivers)
# volumes: - { name: jdbc-drivers, emptyDir: {} }
```

> **Alternative:** build custom NiFi image chứa sẵn drivers, hoặc PVC mount.

---

## 12. Migration Từ v1.0 (nếu đã build theo bản cũ)

Nếu bạn đã build Controller + Bronze theo time-based v1.0:

| # | Việc | Chi tiết |
|---|------|----------|
| 1 | ALTER schema | Thêm `dataset, source_layer, depends_on` vào `pipeline_config`; `run_id` vào `pipeline_execution_log` (Doc 10 §3) |
| 2 | Tách config | Mỗi dataset → row `BRZ_*/SLV_*/GLD_*` riêng + set `depends_on`; re-point `transform_rules/column_mapping/dq_rules.pipeline_id` sang stage sở hữu (Doc 10 §4.5) |
| 3 | Controller | Thêm `Generate Run ID` (§4.2); đổi SQL sang `WHERE depends_on IS NULL` (§4.3); bỏ route-by-layer-3-port, thay bằng `to-router` |
| 4 | Bronze | Thêm `Load Own Config` ở đầu (§6.1); sửa Log SQL sang explicit column list + `run_id` (§6.7); thêm `to-resolver` |
| 5 | **Xóa** | GenerateFlowFile CRON của Silver/Gold (`0 0 3 * * ?`, `0 0 4 * * ?`) — không còn dùng |
| 6 | Build mới | Stage Router (§5), Silver sửa lại (§7), Gold (§8), Resolve Next (§9), Wait/Notify nếu cần (§10) |

---

## 13. Checklist Kiểm Tra

- [ ] JDBC drivers (Dremio + PostgreSQL) có trong NiFi `/lib`
- [ ] Controller Services enabled: `dremio-jdbc-pool`, `source-postgres-pool`, record services (+ `dmc-server`/`dmc-client` nếu fan-in)
- [ ] Metadata: mỗi stage 1 row, `depends_on` đúng; root có `depends_on IS NULL`
- [ ] `transform_rules`/`column_mapping`/`dq_rules` trỏ đúng `pipeline_id` của stage sở hữu
- [ ] PG[1] Controller: sinh `run_id`, đọc đúng roots, ra `to-router`
- [ ] PG[2] Stage Router: route bronze/silver/gold đúng
- [ ] PG[3] Bronze: full + incremental ra MinIO `bronze/`; log có `run_id`
- [ ] PG[4]/[5] Silver/Gold: Load Own Config đúng stage; Dremio tạo/update Iceberg
- [ ] PG[6] Resolve Next: bronze success → tự đánh thức silver → gold (theo run_id)
- [ ] `pipeline_execution_log`: cùng `run_id`, đủ 3 layer, đúng `pipeline_id` riêng từng stage
- [ ] (Fan-in) gold multi-parent chỉ chạy 1 lần sau khi đủ parent

---

## 14. Tài Liệu Tiếp Theo

| Bước | Doc | Mô tả |
|------|-----|-------|
| Chiến lược (why) | [14-pipeline-dependency-orchestration.md](14-pipeline-dependency-orchestration.md) | Lý do & thiết kế event-driven |
| Chi tiết SQL | [12-dynamic-sql-templates.md](12-dynamic-sql-templates.md) | SQL templates bronze/silver/gold |
| Vận hành | [13-pipeline-operations-runbook.md](13-pipeline-operations-runbook.md) | Thêm bảng, monitor, troubleshoot |
</content>
