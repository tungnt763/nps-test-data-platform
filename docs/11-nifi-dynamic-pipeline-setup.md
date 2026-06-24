# NiFi Dynamic Pipeline Setup

> **Phiên bản:** 1.0 | **Ngày:** 2026-06-24
> **Yêu cầu:** Dremio JDBC driver trong NiFi, metadata tables đã tạo (Doc 10)
> **Tham chiếu:** [09-metadata-driven-strategy.md](09-metadata-driven-strategy.md) | [10-metadata-tables-design.md](10-metadata-tables-design.md)

---

## 1. Tổng Quan NiFi Flow

### 1.1 Process Group Hierarchy

```
Root Process Group
│
├── PG: [1] Metadata Controller
│   ├── Read pipeline_config from Dremio
│   ├── Split per table config
│   ├── Route by target_layer
│   └── Output Ports: to-bronze, to-silver, to-gold
│
├── PG: [2] Bronze Ingestion
│   ├── Input Port: from-metadata
│   ├── Route by load_type (full/incremental)
│   ├── Get last watermark (incremental only)
│   ├── Execute dynamic SQL on source DB
│   ├── Write Parquet to MinIO
│   └── Log execution
│
├── PG: [3] Silver Transform Orchestrator
│   ├── Input Port: from-metadata
│   ├── Read transform_rules from Dremio
│   ├── Execute transforms on Dremio
│   ├── Run DQ checks
│   └── Log execution
│
├── PG: [4] Gold Transform Orchestrator
│   ├── Input Port: from-metadata
│   ├── Read transform_rules from Dremio
│   ├── Execute aggregations on Dremio
│   └── Log execution
│
└── PG: [5] Pipeline Monitor
    ├── Query execution_log
    ├── Check failures
    └── Alert on errors
```

### 1.2 Controller Services cần tạo

| Controller Service          | Type                    | Mục đích                    |
|-----------------------------|-------------------------|-----------------------------|
| `dremio-jdbc-pool`          | DBCPConnectionPool      | Connect NiFi → Dremio       |
| `source-postgres-pool`      | DBCPConnectionPool      | Connect NiFi → Source DB    |
| `avro-reader`               | AvroReader              | Đọc output ExecuteSQL       |
| `json-reader`               | JsonTreeReader          | Đọc metadata JSON           |
| `json-record-set-writer`    | JsonRecordSetWriter     | Ghi output dạng JSON        |
| `parquet-writer`            | ParquetRecordSetWriter  | Ghi Parquet cho bronze      |
| `s3-credentials`            | AWSCredentialsProvider  | MinIO S3 credentials        |

---

## 2. Prerequisites — Cài Đặt JDBC Drivers

### 2.1 Dremio JDBC Driver

NiFi cần Dremio JDBC driver để kết nối Dremio qua JDBC (port 31010).

```bash
# Download Dremio JDBC driver vào NiFi pod
kubectl exec -n data-ingestion nifi-0 -- curl -L -o /opt/nifi/nifi-current/lib/dremio-jdbc-driver-24.3.2.jar "https://download.dremio.com/jdbc-driver/24.3.2-202401241821100032-d2d8a497/dremio-jdbc-driver-24.3.2-202401241821100032-d2d8a497.jar"

# Verify
kubectl exec -n data-ingestion nifi-0 -- ls -la /opt/nifi/nifi-current/lib/dremio-jdbc-driver-24.3.2.jar
```

> **Lưu ý:** Driver mất khi pod restart. Xem phần 8 để setup initContainer cho permanent fix.

### 2.2 PostgreSQL JDBC Driver

```bash
# Download PostgreSQL JDBC driver (nếu chưa có)
kubectl exec -n data-ingestion nifi-0 -- curl -L -o /opt/nifi/nifi-current/lib/postgresql-42.7.2.jar "https://jdbc.postgresql.org/download/postgresql-42.7.2.jar"

kubectl exec -n data-ingestion nifi-0 -- ls -la /opt/nifi/nifi-current/lib/postgresql-42.7.2.jar
```

### 2.3 Restart NiFi để load drivers

```bash
# Restart NiFi pod để load new JARs
kubectl delete pod -n data-ingestion nifi-0
# Chờ pod khởi động lại (~2-3 phút)
kubectl wait --for=condition=ready pod/nifi-0 -n data-ingestion --timeout=300s
```

---

## 3. Controller Services Setup

Truy cập NiFi UI: `https://localhost:8444/nifi`

### 3.1 Tạo Dremio JDBC Connection Pool

**NiFi UI → Controller Settings (gear icon) → Management Controller Services → + icon**

| Property                    | Value                                                           |
|-----------------------------|-----------------------------------------------------------------|
| **Name**                    | `dremio-jdbc-pool`                                              |
| **Type**                    | `DBCPConnectionPool`                                            |
| **Database Connection URL** | `jdbc:dremio:direct=dremio.data-processing.svc.cluster.local:31010` |
| **Database Driver Class Name** | `com.dremio.jdbc.Driver`                                     |
| **Database Driver Location(s)** | `/opt/nifi/nifi-current/lib/dremio-jdbc-driver-24.3.2.jar` |
| **Database User**           | `admin`                                                         |
| **Password**                | (Dremio admin password)                                         |
| **Max Wait Time**           | `10 secs`                                                       |
| **Max Total Connections**   | `5`                                                             |

→ Click **Enable** (lightning icon)

### 3.2 Tạo Source PostgreSQL Connection Pool

| Property                    | Value                                                           |
|-----------------------------|-----------------------------------------------------------------|
| **Name**                    | `source-postgres-pool`                                          |
| **Type**                    | `DBCPConnectionPool`                                            |
| **Database Connection URL** | `jdbc:postgresql://postgres-superset-postgresql.data-visualization.svc.cluster.local:5432/superset` |
| **Database Driver Class Name** | `org.postgresql.Driver`                                      |
| **Database Driver Location(s)** | `/opt/nifi/nifi-current/lib/postgresql-42.7.2.jar`          |
| **Database User**           | `superset`                                                      |
| **Password**                | `SupersetPostgres2024`                                          |
| **Max Wait Time**           | `10 secs`                                                       |
| **Max Total Connections**   | `5`                                                             |

→ Click **Enable**

### 3.3 Tạo Record Services

**AvroReader:**

| Property | Value |
|----------|-------|
| **Name** | `avro-reader` |
| **Type** | `AvroReader` |

→ Enable

**JsonTreeReader:**

| Property | Value |
|----------|-------|
| **Name** | `json-reader` |
| **Type** | `JsonTreeReader` |
| **Schema Access Strategy** | `Infer Schema` |

→ Enable

**JsonRecordSetWriter:**

| Property | Value |
|----------|-------|
| **Name** | `json-record-set-writer` |
| **Type** | `JsonRecordSetWriter` |
| **Schema Access Strategy** | `Inherit Record Schema` |

→ Enable

---

## 4. Process Group 1: Metadata Controller

### 4.1 Tạo Process Group

Right-click canvas → **Add Process Group** → Name: `[1] Metadata Controller`

Double-click để mở PG.

### 4.2 Processor: GenerateFlowFile (Trigger)

Processor này trigger pipeline theo schedule.

| Property               | Value                                     |
|------------------------|-------------------------------------------|
| **Name**               | `Trigger Pipeline Run`                    |
| **Scheduling Strategy**| `CRON_DRIVEN`                             |
| **Schedule**           | `0 0 2 * * ?` (2h sáng mỗi ngày)         |
| **Custom Text**        | `trigger`                                 |
| **Run Schedule**       | `0 sec` (mặc định)                        |

> **Tip:** Để test, tạm dùng `Timer Driven` với schedule `60 sec` hoặc trigger manual (Right-click → Run Once)

### 4.3 Processor: ExecuteSQL (Read Active Pipelines)

| Property                       | Value                                                                |
|--------------------------------|----------------------------------------------------------------------|
| **Name**                       | `Read Active Pipeline Configs`                                       |
| **Database Connection Pooling Service** | `dremio-jdbc-pool`                                            |
| **SQL select query**           | (xem bên dưới)                                                       |
| **Max Rows Per Flow File**     | `0` (unlimited)                                                      |

**SQL Query:**
```sql
SELECT
    pipeline_id,
    pipeline_name,
    source_type,
    source_connection,
    source_schema,
    source_table,
    target_layer,
    target_path,
    target_table,
    load_type,
    primary_keys,
    watermark_column,
    partition_columns,
    batch_size,
    schedule_cron,
    is_active,
    description
FROM minio-datalake.metadata.pipeline_config
WHERE is_active = true
ORDER BY pipeline_id
```

**Connection:** `Trigger Pipeline Run` → success → `Read Active Pipeline Configs`

### 4.4 Processor: ConvertAvroToJSON

| Property       | Value            |
|----------------|------------------|
| **Name**       | `Config to JSON` |
| **JSON Format**| `One line per Avro record` |

**Connection:** `Read Active Pipeline Configs` → success → `Config to JSON`

### 4.5 Processor: SplitJson

| Property              | Value   |
|-----------------------|---------|
| **Name**              | `Split Per Table Config` |
| **JsonPath Expression** | `$[*]` |

**Connection:** `Config to JSON` → success → `Split Per Table Config`

### 4.6 Processor: EvaluateJsonPath

Trích xuất tất cả config fields thành FlowFile attributes.

| Property               | Value                           |
|------------------------|----------------------------------|
| **Name**               | `Extract Config Attributes`     |
| **Destination**        | `flowfile-attribute`            |
| **Return Type**        | `auto-detect`                   |

**Dynamic Properties (thêm bằng + icon):**

| Property Name       | JsonPath Value            |
|---------------------|--------------------------|
| `pipeline_id`       | `$.pipeline_id`          |
| `pipeline_name`     | `$.pipeline_name`        |
| `source_type`       | `$.source_type`          |
| `source_connection` | `$.source_connection`    |
| `source_schema`     | `$.source_schema`        |
| `source_table`      | `$.source_table`         |
| `target_layer`      | `$.target_layer`         |
| `target_path`       | `$.target_path`          |
| `target_table`      | `$.target_table`         |
| `load_type`         | `$.load_type`            |
| `primary_keys`      | `$.primary_keys`         |
| `watermark_column`  | `$.watermark_column`     |
| `partition_columns` | `$.partition_columns`    |
| `batch_size`        | `$.batch_size`           |

**Connection:** `Split Per Table Config` → split → `Extract Config Attributes`

### 4.7 Processor: RouteOnAttribute (Route by Layer)

| Property        | Value                                |
|-----------------|--------------------------------------|
| **Name**        | `Route by Target Layer`             |
| **Routing Strategy** | `Route to Property name`       |

**Dynamic Properties:**

| Property Name | Value (NiFi Expression Language)    |
|---------------|-------------------------------------|
| `bronze`      | `${target_layer:equals('bronze')}`  |
| `silver`      | `${target_layer:equals('silver')}`  |
| `gold`        | `${target_layer:equals('gold')}`    |

**Connection:** `Extract Config Attributes` → matched → `Route by Target Layer`

### 4.8 Output Ports

Tạo 3 Output Ports trong Process Group:
- `to-bronze` ← Connection từ RouteOnAttribute → `bronze`
- `to-silver` ← Connection từ RouteOnAttribute → `silver`
- `to-gold` ← Connection từ RouteOnAttribute → `gold`

### 4.9 Flow Diagram

```
┌──────────────────────────────────────────────────────────────┐
│  PG: [1] Metadata Controller                                 │
│                                                              │
│  ┌─────────────────────┐                                    │
│  │ Trigger Pipeline    │ (CRON: 0 0 2 * * ?)                │
│  │ Run                 │                                    │
│  └─────────┬───────────┘                                    │
│            │ success                                        │
│  ┌─────────▼───────────┐                                    │
│  │ Read Active         │ (ExecuteSQL → Dremio)              │
│  │ Pipeline Configs    │                                    │
│  └─────────┬───────────┘                                    │
│            │ success                                        │
│  ┌─────────▼───────────┐                                    │
│  │ Config to JSON      │ (ConvertAvroToJSON)                │
│  └─────────┬───────────┘                                    │
│            │ success                                        │
│  ┌─────────▼───────────┐                                    │
│  │ Split Per Table     │ (SplitJson $[*])                   │
│  │ Config              │                                    │
│  └─────────┬───────────┘                                    │
│            │ split                                          │
│  ┌─────────▼───────────┐                                    │
│  │ Extract Config      │ (EvaluateJsonPath)                 │
│  │ Attributes          │                                    │
│  └─────────┬───────────┘                                    │
│            │ matched                                        │
│  ┌─────────▼───────────┐                                    │
│  │ Route by Target     │ (RouteOnAttribute)                 │
│  │ Layer               │                                    │
│  └──┬──────┬──────┬────┘                                    │
│     │      │      │                                         │
│  ┌──▼──┐┌──▼──┐┌──▼──┐                                     │
│  │OUT: ││OUT: ││OUT: │                                     │
│  │to-  ││to-  ││to-  │                                     │
│  │bronz││silv ││gold │                                     │
│  └─────┘└─────┘└─────┘                                     │
└──────────────────────────────────────────────────────────────┘
```

---

## 5. Process Group 2: Bronze Ingestion

### 5.1 Tạo Process Group

Trở lại Root canvas → Add Process Group → Name: `[2] Bronze Ingestion`

### 5.2 Connect từ Metadata Controller

Kéo connection từ `[1] Metadata Controller` (output port `to-bronze`) → `[2] Bronze Ingestion`

Double-click PG để mở.

### 5.3 Input Port

Tạo **Input Port**: `from-metadata`

### 5.4 Processor: RouteOnAttribute (Route by Load Type)

| Property        | Value                                    |
|-----------------|------------------------------------------|
| **Name**        | `Route by Load Type`                     |
| **Routing Strategy** | `Route to Property name`            |

**Dynamic Properties:**

| Property Name  | Value                                     |
|----------------|-------------------------------------------|
| `full`         | `${load_type:equals('full')}`             |
| `incremental`  | `${load_type:equals('incremental')}`      |

**Connection:** `from-metadata` → `Route by Load Type`

### 5.5 Full Load Path

#### Processor: GenerateFlowFile (Build Full Load SQL)

| Property         | Value                                                             |
|------------------|-------------------------------------------------------------------|
| **Name**         | `Build Full Load SQL`                                             |
| **Custom Text**  | `SELECT * FROM ${source_schema}.${source_table}`                  |
| **Run Schedule** | `0 sec`                                                           |

> **Quan trọng:** Custom Text hỗ trợ Expression Language.
> `${source_schema}` và `${source_table}` sẽ được resolve từ FlowFile attributes.

**Thực ra**, GenerateFlowFile không nhận FlowFile đầu vào (nó tạo mới). Ta cần approach khác:

#### Processor: ReplaceText (Build Full Load SQL) — ĐÚNG CÁCH

| Property                 | Value                                                       |
|--------------------------|-------------------------------------------------------------|
| **Name**                 | `Build Full Load SQL`                                       |
| **Search Value**         | `(?s)(^.*$)`                                                |
| **Replacement Value**    | `SELECT * FROM ${source_schema}.${source_table}`            |
| **Replacement Strategy** | `Regex Replace`                                             |
| **Evaluation Mode**      | `Entire text`                                               |

**Connection:** `Route by Load Type` → `full` → `Build Full Load SQL`

### 5.6 Incremental Load Path

#### Processor: ExecuteSQL (Get Last Watermark)

| Property                              | Value                                                                 |
|---------------------------------------|-----------------------------------------------------------------------|
| **Name**                              | `Get Last Watermark`                                                  |
| **Database Connection Pooling Service** | `dremio-jdbc-pool`                                                  |
| **SQL select query**                  | (xem bên dưới)                                                        |

**SQL Query:**
```sql
SELECT COALESCE(MAX(last_watermark), '1970-01-01 00:00:00') AS last_watermark
FROM minio-datalake.metadata.pipeline_execution_log
WHERE pipeline_id = '${pipeline_id}'
  AND layer = 'bronze'
  AND status = 'success'
```

**Connection:** `Route by Load Type` → `incremental` → `Get Last Watermark`

#### Processor: ConvertAvroToJSON → EvaluateJsonPath

Trích xuất `last_watermark` thành attribute:

**ConvertAvroToJSON:**

| Property | Value |
|----------|-------|
| **Name** | `Watermark to JSON` |

**EvaluateJsonPath:**

| Property          | Value                    |
|-------------------|--------------------------|
| **Name**          | `Extract Watermark`      |
| **Destination**   | `flowfile-attribute`     |
| `last_watermark`  | `$.last_watermark`       |

**Connection:** `Get Last Watermark` → success → `Watermark to JSON` → success → `Extract Watermark`

#### Processor: ReplaceText (Build Incremental SQL)

| Property                 | Value                                                                                             |
|--------------------------|---------------------------------------------------------------------------------------------------|
| **Name**                 | `Build Incremental SQL`                                                                           |
| **Search Value**         | `(?s)(^.*$)`                                                                                      |
| **Replacement Value**    | `SELECT * FROM ${source_schema}.${source_table} WHERE ${watermark_column} > '${last_watermark}'`  |
| **Replacement Strategy** | `Regex Replace`                                                                                   |
| **Evaluation Mode**      | `Entire text`                                                                                     |

**Connection:** `Extract Watermark` → matched → `Build Incremental SQL`

### 5.7 Merge Paths → Execute SQL on Source

Cả 2 đường (full + incremental) đều tạo FlowFile chứa SQL query. Giờ cần execute.

#### Processor: ExecuteSQL (Run Source Query)

| Property                              | Value                                                       |
|---------------------------------------|-------------------------------------------------------------|
| **Name**                              | `Execute Source Query`                                      |
| **Database Connection Pooling Service** | `source-postgres-pool`                                    |
| **SQL select query**                  | (để trống — lấy SQL từ FlowFile content)                    |

> **Cách hoạt động:** Khi "SQL select query" để trống, ExecuteSQL đọc SQL từ nội dung FlowFile.
> FlowFile content lúc này chứa dynamic SQL đã render (từ ReplaceText).

**Connection:**
- `Build Full Load SQL` → success → `Execute Source Query`
- `Build Incremental SQL` → success → `Execute Source Query`

### 5.8 Convert & Write to MinIO

#### Processor: UpdateAttribute (Set S3 Key)

| Property   | Value                                                                         |
|------------|-------------------------------------------------------------------------------|
| **Name**   | `Set S3 Output Path`                                                          |
| `filename` | `${source_table}_${now():format('yyyyMMdd_HHmmss')}.avro`                    |
| `s3.key`   | `bronze/${source_table}/dt=${now():format('yyyy-MM-dd')}/${filename}`         |

**Connection:** `Execute Source Query` → success → `Set S3 Output Path`

#### Processor: PutS3Object (Write to MinIO)

| Property                     | Value                                                        |
|------------------------------|--------------------------------------------------------------|
| **Name**                     | `Write to MinIO Bronze`                                      |
| **Object Key**               | `${s3.key}`                                                  |
| **Bucket**                   | `napas-datalake`                                             |
| **Access Key ID**            | `napas-admin`                                                |
| **Secret Access Key**        | `napas-minio-s3cr3t-2024`                                    |
| **Endpoint Override URL**    | `http://minio.data-storage.svc.cluster.local:9000`           |
| **Signer Override**          | `AWSS3V4SignerType`                                          |
| **Region**                   | `us-east-1`                                                  |
| **Use Path Style Access**    | `true`                                                       |

> **Lưu ý:** NiFi 1.x dùng `PutS3Object`, NiFi 2.x dùng `PutS3Object` hoặc `PutObject` (AWS SDK v2).
> Verify processor name trong NiFi version bạn đang dùng.

**Connection:** `Set S3 Output Path` → success → `Write to MinIO Bronze`

### 5.9 Log Execution

#### Processor: ReplaceText (Build Log SQL)

| Property                 | Value                                                                                 |
|--------------------------|---------------------------------------------------------------------------------------|
| **Name**                 | `Build Execution Log SQL`                                                              |
| **Search Value**         | `(?s)(^.*$)`                                                                           |
| **Replacement Value**    | (xem bên dưới)                                                                         |
| **Replacement Strategy** | `Regex Replace`                                                                        |
| **Evaluation Mode**      | `Entire text`                                                                          |

**Replacement Value:**
```
INSERT INTO minio-datalake.metadata.pipeline_execution_log VALUES (
    '${pipeline_id}_${now():format('yyyyMMdd_HHmmss')}',
    '${pipeline_id}',
    '${pipeline_name}',
    'bronze',
    CAST('${now():format('yyyy-MM-dd HH:mm:ss')}' AS TIMESTAMP),
    CAST('${now():format('yyyy-MM-dd HH:mm:ss')}' AS TIMESTAMP),
    'success',
    ${executesql.row.count},
    ${executesql.row.count},
    0,
    0,
    '${now():format('yyyy-MM-dd HH:mm:ss')}',
    NULL,
    '${load_type}',
    CURRENT_TIMESTAMP
)
```

> `${executesql.row.count}` là attribute tự động từ ExecuteSQL processor.

**Connection:** `Write to MinIO Bronze` → success → `Build Execution Log SQL`

#### Processor: ExecuteSQL (Insert Log)

| Property                              | Value                             |
|---------------------------------------|-----------------------------------|
| **Name**                              | `Insert Execution Log`            |
| **Database Connection Pooling Service** | `dremio-jdbc-pool`              |
| **SQL select query**                  | (để trống — lấy từ FlowFile)     |

**Connection:** `Build Execution Log SQL` → success → `Insert Execution Log`

### 5.10 Bronze Ingestion Flow Diagram

```
┌──────────────────────────────────────────────────────────────────────┐
│  PG: [2] Bronze Ingestion                                            │
│                                                                      │
│  ┌──────────────┐                                                   │
│  │ IN:          │                                                   │
│  │ from-metadata│                                                   │
│  └──────┬───────┘                                                   │
│         │                                                           │
│  ┌──────▼───────────────┐                                           │
│  │ Route by Load Type   │                                           │
│  │ (RouteOnAttribute)   │                                           │
│  └──┬───────────────┬───┘                                           │
│     │ full          │ incremental                                   │
│     ▼               ▼                                               │
│  ┌──────────┐  ┌────────────────┐                                  │
│  │ Build    │  │ Get Last       │ (ExecuteSQL → Dremio)             │
│  │ Full     │  │ Watermark      │                                  │
│  │ Load SQL │  └────────┬───────┘                                  │
│  │          │           │                                           │
│  │(Replace  │  ┌────────▼───────┐                                  │
│  │ Text)    │  │ Watermark to   │ (ConvertAvroToJSON)              │
│  │          │  │ JSON           │                                  │
│  └────┬─────┘  └────────┬───────┘                                  │
│       │                 │                                           │
│       │        ┌────────▼───────┐                                  │
│       │        │ Extract        │ (EvaluateJsonPath)               │
│       │        │ Watermark      │                                  │
│       │        └────────┬───────┘                                  │
│       │                 │                                           │
│       │        ┌────────▼───────┐                                  │
│       │        │ Build          │ (ReplaceText)                    │
│       │        │ Incremental    │                                  │
│       │        │ SQL            │                                  │
│       │        └────────┬───────┘                                  │
│       │                 │                                           │
│       └────────┬────────┘                                           │
│                │ (merge)                                            │
│       ┌────────▼────────────────┐                                  │
│       │ Execute Source Query    │ (ExecuteSQL → Source DB)          │
│       │ (reads SQL from content)│                                  │
│       └────────┬────────────────┘                                  │
│                │ success                                            │
│       ┌────────▼────────────────┐                                  │
│       │ Set S3 Output Path     │ (UpdateAttribute)                 │
│       │ s3.key = bronze/...    │                                  │
│       └────────┬────────────────┘                                  │
│                │                                                    │
│       ┌────────▼────────────────┐                                  │
│       │ Write to MinIO Bronze  │ (PutS3Object)                    │
│       └────────┬────────────────┘                                  │
│                │ success                                            │
│       ┌────────▼────────────────┐                                  │
│       │ Build Execution Log    │ (ReplaceText)                    │
│       │ SQL                    │                                  │
│       └────────┬────────────────┘                                  │
│                │                                                    │
│       ┌────────▼────────────────┐                                  │
│       │ Insert Execution Log   │ (ExecuteSQL → Dremio)            │
│       └────────────────────────┘                                  │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 6. Process Group 3: Silver Transform Orchestrator

### 6.1 Concept

Silver transform không di chuyển data qua NiFi. Thay vào đó:
1. NiFi đọc `transform_rules` từ Dremio metadata
2. NiFi gửi SQL lệnh cho Dremio execute
3. Dremio chạy SQL trực tiếp trên data trong MinIO

→ NiFi chỉ là **orchestrator**, Dremio là **compute engine**.

### 6.2 Tạo Process Group

Root canvas → Add Process Group → Name: `[3] Silver Transform Orchestrator`

Connect `[1] Metadata Controller` output port `to-silver` → `[3] Silver Transform Orchestrator`

### 6.3 Input Port

Tạo Input Port: `from-metadata`

> **Lưu ý:** Với thiết kế hiện tại, Metadata Controller route `target_layer = 'silver'` vào đây.
> Nhưng Silver transform thường trigger SAU khi Bronze ingestion xong.
> Có 2 cách:
> 1. **Sequential:** Bronze xong → gửi signal → trigger Silver (phức tạp hơn)
> 2. **Time-based:** Bronze chạy lúc 2h, Silver chạy lúc 3h (đơn giản, đủ dùng)
>
> Ở đây ta dùng **Time-based**: tạo GenerateFlowFile riêng cho Silver với schedule lệch giờ.

### 6.4 Processor: GenerateFlowFile (Silver Trigger)

| Property               | Value                                     |
|------------------------|-------------------------------------------|
| **Name**               | `Trigger Silver Transforms`               |
| **Scheduling Strategy**| `CRON_DRIVEN`                             |
| **Schedule**           | `0 0 3 * * ?` (3h sáng, sau bronze 1h)   |
| **Custom Text**        | `trigger_silver`                          |

### 6.5 Processor: ExecuteSQL (Read Transform Rules)

| Property                              | Value                                                                 |
|---------------------------------------|-----------------------------------------------------------------------|
| **Name**                              | `Read Silver Transform Rules`                                         |
| **Database Connection Pooling Service** | `dremio-jdbc-pool`                                                  |
| **SQL select query**                  | (xem bên dưới)                                                        |

**SQL Query:**
```sql
SELECT
    r.rule_id,
    r.pipeline_id,
    r.rule_name,
    r.transform_type,
    r.sql_template,
    r.execution_order,
    p.source_table,
    p.target_table,
    p.primary_keys,
    p.watermark_column
FROM minio-datalake.metadata.transform_rules r
JOIN minio-datalake.metadata.pipeline_config p ON r.pipeline_id = p.pipeline_id
WHERE r.source_layer = 'bronze'
  AND r.target_layer = 'silver'
  AND r.is_active = true
  AND p.is_active = true
ORDER BY r.pipeline_id, r.execution_order
```

**Connection:** `Trigger Silver Transforms` → success → `Read Silver Transform Rules`

### 6.6 Convert + Split + Extract

Tương tự Metadata Controller:

1. **ConvertAvroToJSON** → `Rules to JSON`
2. **SplitJson** ($[*]) → `Split Per Transform Rule`
3. **EvaluateJsonPath** → `Extract Rule Attributes`

**EvaluateJsonPath dynamic properties:**

| Property         | Value                  |
|------------------|------------------------|
| `rule_id`        | `$.rule_id`            |
| `pipeline_id`    | `$.pipeline_id`        |
| `rule_name`      | `$.rule_name`          |
| `transform_type` | `$.transform_type`     |
| `sql_template`   | `$.sql_template`       |
| `source_table`   | `$.source_table`       |
| `target_table`   | `$.target_table`       |
| `primary_keys`   | `$.primary_keys`       |
| `watermark_column`| `$.watermark_column`  |

### 6.7 Processor: ReplaceText (Render SQL Template)

| Property                 | Value                                                |
|--------------------------|------------------------------------------------------|
| **Name**                 | `Render Transform SQL`                               |
| **Search Value**         | `(?s)(^.*$)`                                         |
| **Replacement Value**    | `${sql_template}`                                    |
| **Replacement Strategy** | `Regex Replace`                                      |
| **Evaluation Mode**      | `Entire text`                                        |

> `${sql_template}` chứa SQL đã lưu trong metadata. Các biến `${source_table}`, `${target_table}`
> trong sql_template sẽ được NiFi Expression Language resolve vì chúng cũng là attributes.

### 6.8 Processor: ExecuteSQL (Run Transform on Dremio)

| Property                              | Value                                   |
|---------------------------------------|-----------------------------------------|
| **Name**                              | `Execute Transform on Dremio`           |
| **Database Connection Pooling Service** | `dremio-jdbc-pool`                    |
| **SQL select query**                  | (để trống — lấy từ FlowFile content)   |

### 6.9 Log Execution (tương tự Bronze)

Dùng `ReplaceText` + `ExecuteSQL` để INSERT vào `pipeline_execution_log`.

### 6.10 Silver Flow Diagram

```
┌──────────────────────────────────────────────────────────────┐
│  PG: [3] Silver Transform Orchestrator                       │
│                                                              │
│  ┌────────────────────────┐                                 │
│  │ Trigger Silver         │ (CRON: 0 0 3 * * ?)            │
│  │ Transforms             │                                 │
│  └──────────┬─────────────┘                                 │
│             │                                                │
│  ┌──────────▼─────────────┐                                 │
│  │ Read Silver Transform  │ (ExecuteSQL → Dremio)           │
│  │ Rules                  │                                 │
│  └──────────┬─────────────┘                                 │
│             │                                                │
│  ┌──────────▼─────────────┐                                 │
│  │ Rules to JSON          │ (ConvertAvroToJSON)             │
│  └──────────┬─────────────┘                                 │
│             │                                                │
│  ┌──────────▼─────────────┐                                 │
│  │ Split Per Transform    │ (SplitJson)                     │
│  │ Rule                   │                                 │
│  └──────────┬─────────────┘                                 │
│             │                                                │
│  ┌──────────▼─────────────┐                                 │
│  │ Extract Rule           │ (EvaluateJsonPath)              │
│  │ Attributes             │                                 │
│  └──────────┬─────────────┘                                 │
│             │                                                │
│  ┌──────────▼─────────────┐                                 │
│  │ Render Transform SQL   │ (ReplaceText)                   │
│  └──────────┬─────────────┘                                 │
│             │                                                │
│  ┌──────────▼─────────────┐                                 │
│  │ Execute Transform on   │ (ExecuteSQL → Dremio)           │
│  │ Dremio                 │                                 │
│  └──────────┬─────────────┘                                 │
│             │                                                │
│  ┌──────────▼─────────────┐                                 │
│  │ Log Execution          │ (ReplaceText + ExecuteSQL)      │
│  └────────────────────────┘                                 │
└──────────────────────────────────────────────────────────────┘
```

---

## 7. Process Group 4: Gold Transform Orchestrator

**Cấu trúc giống hệt Silver**, chỉ thay đổi:

| Thay đổi                     | Silver                        | Gold                         |
|------------------------------|-------------------------------|------------------------------|
| Trigger schedule             | `0 0 3 * * ?`                | `0 0 4 * * ?` (4h sáng)     |
| SQL WHERE clause             | `source_layer = 'bronze'`    | `source_layer = 'silver'`   |
|                              | `target_layer = 'silver'`    | `target_layer = 'gold'`     |
| Log layer                    | `'silver'`                   | `'gold'`                    |

→ Copy toàn bộ PG Silver, rename thành `[4] Gold Transform Orchestrator`, sửa 3 chỗ trên.

---

## 8. Permanent JDBC Drivers (initContainer)

Để không mất driver khi pod restart, thêm initContainer vào NiFi StatefulSet:

**File:** `napas-platform-infra/platform/charts/nifi/templates/statefulset.yaml`

Thêm initContainers trước containers:

```yaml
initContainers:
  - name: download-jdbc-drivers
    image: curlimages/curl:8.5.0
    command: ["sh", "-c"]
    args:
      - |
        curl -L -o /drivers/postgresql-42.7.2.jar \
          "https://jdbc.postgresql.org/download/postgresql-42.7.2.jar"
        curl -L -o /drivers/dremio-jdbc-driver-24.3.2.jar \
          "https://download.dremio.com/jdbc-driver/24.3.2/dremio-jdbc-driver-24.3.2-202401241530580032-1f14e76d.jar"
        ls -la /drivers/
    volumeMounts:
      - name: jdbc-drivers
        mountPath: /drivers
```

Thêm volumeMount vào container NiFi:

```yaml
containers:
  - name: nifi
    volumeMounts:
      - name: jdbc-drivers
        mountPath: /opt/nifi/nifi-current/drivers
        # Cập nhật NiFi để scan thêm /drivers:
        # Hoặc copy vào /lib trong command
```

Thêm volume:

```yaml
volumes:
  - name: jdbc-drivers
    emptyDir: {}
```

> **Alternative đơn giản hơn:** Dùng PVC để mount drivers, hoặc build custom NiFi image chứa sẵn drivers.

---

## 9. Checklist Kiểm Tra

Sau khi setup xong, verify từng bước:

- [ ] Dremio JDBC driver có trong NiFi `/lib/` hoặc `/drivers/`
- [ ] PostgreSQL JDBC driver có trong NiFi
- [ ] Controller Service `dremio-jdbc-pool` → **ENABLED**, test connection thành công
- [ ] Controller Service `source-postgres-pool` → **ENABLED**, test connection thành công
- [ ] PG [1] Metadata Controller: trigger manual → thấy FlowFiles split ra đúng số table
- [ ] PG [2] Bronze Ingestion: full load → data xuất hiện trong MinIO `bronze/`
- [ ] PG [2] Bronze Ingestion: incremental → chỉ lấy records mới
- [ ] PG [3] Silver Transform: Dremio tạo/update Iceberg tables trong `silver/`
- [ ] PG [4] Gold Transform: Dremio tạo views/tables trong `gold/`
- [ ] `pipeline_execution_log` có records mới sau mỗi lần chạy

---

## 10. Tài Liệu Tiếp Theo

| Bước | Doc | Mô tả |
|------|-----|-------|
| Chi tiết SQL | [12-dynamic-sql-templates.md](12-dynamic-sql-templates.md) | Tất cả SQL templates cho mọi transform type |
| Vận hành | [13-pipeline-operations-runbook.md](13-pipeline-operations-runbook.md) | Thêm bảng, monitor, troubleshoot |
