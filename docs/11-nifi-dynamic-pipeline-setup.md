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

### 4.1 Processor: GenerateFlowFile (Trigger)

| Property               | Value                                     |
|------------------------|-------------------------------------------|
| **Name**               | `Trigger Pipeline Run`                    |
| **Scheduling Strategy**| `CRON_DRIVEN`                             |
| **Schedule**           | `0 0 2 * * ?` (2h sáng mỗi ngày)         |
| **Custom Text**        | `trigger`                                 |

> **Test:** tạm dùng `Timer Driven` `60 sec`, hoặc Right-click → **Run Once**.

### 4.2 Processor: UpdateAttribute (Generate Run ID) — MỚI

Sinh **một** `run_id` cho cả lần chạy; nó sẽ propagate xuống mọi stage để tương quan/lineage.

| Property  | Value                                                              |
|-----------|-------------------------------------------------------------------|
| **Name**  | `Generate Run ID`                                                 |
| `run_id`  | `RUN_${now():format('yyyyMMddHHmmss')}_${UUID():substring(0,8)}`  |

**Connection:** `Trigger Pipeline Run` → success → `Generate Run ID`

### 4.3 Processor: ExecuteSQL (Read Root Pipelines)

Chỉ đọc **root** (`depends_on IS NULL`) — tức các bronze ingestion. Silver/gold KHÔNG đọc ở đây;
chúng được Resolve Next đánh thức.

| Property                       | Value                  |
|--------------------------------|------------------------|
| **Name**                       | `Read Root Pipelines`  |
| **Database Connection Pooling Service** | `dremio-jdbc-pool` |
| **SQL select query**           | (xem dưới)             |

```sql
SELECT pipeline_id, target_layer
FROM "minio-datalake"."metadata".pipeline_config
WHERE is_active = true
  AND depends_on IS NULL
ORDER BY pipeline_id
```

> Chỉ cần `pipeline_id` + `target_layer` cho việc routing — config đầy đủ sẽ được
> executor tự nạp ở bước "Load Own Config".

**Connection:** `Generate Run ID` → success → `Read Root Pipelines`

### 4.4 ConvertAvroToJSON → SplitJson → EvaluateJsonPath

| Processor | Cấu hình |
|-----------|----------|
| `ConvertAvroToJSON` (`Config to JSON`) | JSON Format = `One line per Avro record` |
| `SplitJson` (`Split Per Root`) | JsonPath Expression = `$[*]` |
| `EvaluateJsonPath` (`Set Correlation Attrs`) | Destination = `flowfile-attribute`; `next_pipeline_id` = `$.pipeline_id`, `target_layer` = `$.target_layer` |

> `run_id` đã là attribute (set ở §4.2) nên tự đi theo qua Split. Sau bước này mỗi FlowFile mang:
> `run_id`, `next_pipeline_id`, `target_layer`.

**Connections:** `Read Root Pipelines` → `Config to JSON` → `Split Per Root` (split) → `Set Correlation Attrs`

### 4.5 Output Port

Tạo Output Port `to-router`. **Connection:** `Set Correlation Attrs` → matched → `to-router`.

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

### 7.2 Processor: ExecuteSQL (Read Transform Rules)

Khóa theo **pipeline_id của stage này** — lấy đúng rules của silver, không quét toàn bộ:

```sql
SELECT r.rule_id, r.rule_name, r.transform_type, r.sql_template, r.execution_order
FROM "minio-datalake"."metadata".transform_rules r
WHERE r.pipeline_id = '${pipeline_id}'   -- = SLV_*
  AND r.is_active = true
ORDER BY r.execution_order
```

→ `ConvertAvroToJSON` (`Rules to JSON`) → `SplitJson` `$[*]` (`Split Per Rule`) →
`EvaluateJsonPath` (`Extract Rule`: `rule_id`, `rule_name`, `transform_type`, `sql_template`).

> SplitJson giữ nguyên attribute cha (`run_id`, `pipeline_id`, `target_table`...) cho mỗi rule.
>
> ⚠️ **BẮT BUỘC chạy tuần tự đúng `execution_order`.** Silver dùng **pattern staging**:
> `load_stage` (bronze→staging) → `dedup` (trên staging) → `create_table` (ensure silver) →
> `merge` (staging→silver). Các bước phụ thuộc nhau nên KHÔNG được chạy song song/đảo thứ tự.
> Cấu hình để đảm bảo:
> - `Render Transform SQL` và `Run Transform on Dremio`: **Concurrent Tasks = 1**.
> - Connection sau `Split Per Rule`: dùng prioritizer **FirstInFirstOutPrioritizer**.
> - SplitJson phát fragment theo đúng thứ tự kết quả (đã `ORDER BY execution_order`).
>
> Xem SQL từng bước ở [12-dynamic-sql-templates.md](12-dynamic-sql-templates.md) §3 (Staging Pattern).

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

> `${sql_template}` chứa SQL trong metadata; các biến `${source_table}`, `${target_table}`,
> `${primary_keys}`, `${watermark_column}`, `${last_watermark}`, `${column_select_list}`,
> `${merge_on_clause}` được Expression Language resolve vì chúng đều là attribute (từ Load Own
> Config + Get Silver Watermark + build từ column_mapping/primary_keys). Chi tiết template: Doc 12 §3.
>
> Template ghi vào **staging** trước, chỉ bước `merge` mới chạm bảng silver chính → silver không
> bao giờ ở trạng thái dang dở. (Tùy chọn) thêm rule `SILVER_CLEANUP` cuối cùng để drop staging.

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
