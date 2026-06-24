# Metadata Tables Design

> **Phiên bản:** 1.0 | **Ngày:** 2026-06-24
> **Storage:** Iceberg tables trên MinIO, managed bởi Dremio
> **Tham chiếu:** [09-metadata-driven-strategy.md](09-metadata-driven-strategy.md)

---

## 1. Tổng Quan

Metadata tables lưu trữ **toàn bộ cấu hình** pipeline dưới dạng Iceberg tables trong Dremio.
NiFi đọc các bảng này qua JDBC để biết cần ingest bảng nào, transform ra sao, check quality thế nào.

```
metadata (Iceberg namespace trong Dremio)
├── pipeline_config          ← Bảng chính: source, target, load type
├── column_mapping           ← Column-level: tên cột, kiểu dữ liệu, transform
├── transform_rules          ← SQL templates cho silver/gold
├── data_quality_rules       ← Quy tắc DQ check
└── pipeline_execution_log   ← Lịch sử chạy pipeline
```

### Quan hệ giữa các bảng

```
pipeline_config (1)
    │
    ├──── (1:N) ──── column_mapping
    │                  Mỗi pipeline có nhiều column mappings
    │
    ├──── (1:N) ──── transform_rules
    │                  Mỗi pipeline có nhiều transform rules (bronze→silver, silver→gold)
    │
    ├──── (1:N) ──── data_quality_rules
    │                  Mỗi pipeline có nhiều DQ rules
    │
    └──── (1:N) ──── pipeline_execution_log
                       Mỗi pipeline có nhiều lần chạy
```

---

## 2. Chuẩn Bị Dremio

### 2.1 Tạo folder metadata trong MinIO

Trước tiên cần tạo folder `metadata/` trong bucket `napas-datalake`:

```bash
# Port-forward MinIO API (nếu chưa)
kubectl port-forward -n data-storage svc/minio 9000:9000

# Dùng mc (MinIO Client)
mc alias set napas http://localhost:9000 napas-admin napas-minio-s3cr3t-2024
mc mb napas/napas-datalake/metadata --ignore-existing
```

### 2.2 Promote MinIO source folder trong Dremio

Trong Dremio UI (`http://localhost:9047`):
1. Vào **Sources** → chọn MinIO S3 source (đã config từ trước)
2. Navigate đến `napas-datalake` → `metadata`
3. Chọn **Format Settings** nếu cần

> **Lưu ý:** Dremio sẽ tự tạo Iceberg metadata khi bạn chạy `CREATE TABLE`.
> Bạn cần đảm bảo source S3 có quyền write.

---

## 3. DDL — Tạo Metadata Tables

Chạy các câu SQL sau trong **Dremio SQL Runner** (hoặc NiFi gọi qua JDBC):

### 3.1 pipeline_config — Cấu hình pipeline chính

```sql
CREATE TABLE minio-datalake.metadata.pipeline_config (
    pipeline_id         VARCHAR,
    pipeline_name       VARCHAR,
    source_type         VARCHAR,
    source_connection   VARCHAR,
    source_schema       VARCHAR,
    source_table        VARCHAR,
    target_layer        VARCHAR,
    target_path         VARCHAR,
    target_table        VARCHAR,
    load_type           VARCHAR,
    primary_keys        VARCHAR,
    watermark_column    VARCHAR,
    partition_columns   VARCHAR,
    batch_size          INT,
    schedule_cron       VARCHAR,
    is_active           BOOLEAN,
    description         VARCHAR,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
);
```

**Giải thích từng field:**

| Field              | Kiểu     | Mô tả                                                         | Ví dụ                                              |
|--------------------|----------|----------------------------------------------------------------|-----------------------------------------------------|
| `pipeline_id`      | VARCHAR  | ID duy nhất cho pipeline                                       | `P001`                                              |
| `pipeline_name`    | VARCHAR  | Tên mô tả                                                     | `ingest_transactions`                               |
| `source_type`      | VARCHAR  | Loại nguồn dữ liệu                                           | `jdbc`, `api`, `sftp`, `file`                       |
| `source_connection`| VARCHAR  | Tên Controller Service JDBC trong NiFi (hoặc URL)             | `source-postgres-pool`                              |
| `source_schema`    | VARCHAR  | Schema của source DB                                           | `public`                                            |
| `source_table`     | VARCHAR  | Tên bảng nguồn                                                | `transactions`                                      |
| `target_layer`     | VARCHAR  | Layer đích                                                     | `bronze`, `silver`, `gold`                          |
| `target_path`      | VARCHAR  | S3 path pattern (NiFi Expression Language)                     | `bronze/${source_table}/dt=${date}`                  |
| `target_table`     | VARCHAR  | Tên bảng đích trong Dremio                                    | `transactions`                                      |
| `load_type`        | VARCHAR  | Kiểu load                                                     | `full`, `incremental`                               |
| `primary_keys`     | VARCHAR  | Khóa chính (comma-separated)                                  | `txn_id` hoặc `txn_id,merchant_id`                  |
| `watermark_column` | VARCHAR  | Cột dùng cho incremental load (NULL nếu full load)            | `updated_at`                                        |
| `partition_columns`| VARCHAR  | Cột partition (comma-separated, NULL nếu không partition)      | `transaction_date`                                  |
| `batch_size`       | INT      | Số rows mỗi batch (NiFi fetch size)                           | `10000`                                             |
| `schedule_cron`    | VARCHAR  | Lịch chạy                                                     | `0 2 * * *` (2h sáng mỗi ngày)                     |
| `is_active`        | BOOLEAN  | Pipeline có đang active không                                  | `true`, `false`                                     |
| `description`      | VARCHAR  | Mô tả pipeline                                                | `Ingest daily transactions from core banking`       |
| `created_at`       | TIMESTAMP| Thời gian tạo                                                 | `2026-06-24 10:00:00`                               |
| `updated_at`       | TIMESTAMP| Thời gian cập nhật cuối                                       | `2026-06-24 10:00:00`                               |

---

### 3.2 column_mapping — Mapping cột source → target

```sql
CREATE TABLE minio-datalake.metadata.column_mapping (
    mapping_id          VARCHAR,
    pipeline_id         VARCHAR,
    source_column       VARCHAR,
    target_column       VARCHAR,
    data_type           VARCHAR,
    transformation      VARCHAR,
    is_primary_key      BOOLEAN,
    is_nullable         BOOLEAN,
    default_value       VARCHAR,
    column_order        INT,
    description         VARCHAR
);
```

**Giải thích:**

| Field            | Mô tả                                                       | Ví dụ                                   |
|------------------|--------------------------------------------------------------|------------------------------------------|
| `mapping_id`     | ID duy nhất                                                  | `M001`                                   |
| `pipeline_id`    | FK → pipeline_config                                         | `P001`                                   |
| `source_column`  | Tên cột trong source DB                                      | `TXN_AMOUNT`                             |
| `target_column`  | Tên cột trong target table                                   | `transaction_amount`                     |
| `data_type`      | Kiểu dữ liệu target                                         | `DECIMAL(18,2)`, `VARCHAR`, `TIMESTAMP`  |
| `transformation` | Expression để transform (NULL nếu giữ nguyên)               | `CAST(${src} AS DECIMAL(18,2))`          |
|                  |                                                              | `COALESCE(${src}, 0)`                    |
|                  |                                                              | `UPPER(TRIM(${src}))`                    |
| `is_primary_key` | Có phải khóa chính không                                     | `true`, `false`                          |
| `is_nullable`    | Cho phép NULL không                                          | `true`, `false`                          |
| `default_value`  | Giá trị mặc định khi NULL                                   | `0`, `'UNKNOWN'`, `CURRENT_TIMESTAMP`    |
| `column_order`   | Thứ tự cột trong SELECT                                     | `1`, `2`, `3`                            |

**Cách dùng `transformation`:**
- `${src}` = placeholder cho source column name
- Dremio SQL expression: `CAST(${src} AS DECIMAL(18,2))` → `CAST(TXN_AMOUNT AS DECIMAL(18,2))`
- Nếu `transformation` = NULL → giữ nguyên: `source_column AS target_column`

---

### 3.3 transform_rules — Quy tắc transform silver/gold

```sql
CREATE TABLE minio-datalake.metadata.transform_rules (
    rule_id             VARCHAR,
    pipeline_id         VARCHAR,
    rule_name           VARCHAR,
    source_layer        VARCHAR,
    target_layer        VARCHAR,
    transform_type      VARCHAR,
    sql_template        VARCHAR,
    depends_on          VARCHAR,
    execution_order     INT,
    is_active           BOOLEAN,
    description         VARCHAR
);
```

**Giải thích:**

| Field             | Mô tả                                                       | Ví dụ                                   |
|-------------------|--------------------------------------------------------------|------------------------------------------|
| `rule_id`         | ID duy nhất                                                  | `T001`                                   |
| `pipeline_id`     | FK → pipeline_config                                         | `P001`                                   |
| `rule_name`       | Tên rule                                                     | `dedup_transactions`                     |
| `source_layer`    | Layer nguồn                                                  | `bronze`, `silver`                       |
| `target_layer`    | Layer đích                                                   | `silver`, `gold`                         |
| `transform_type`  | Loại transform                                               | `dedup`, `merge`, `aggregate`, `filter`, `create_table`, `custom` |
| `sql_template`    | SQL template với biến `${variable}`                          | Xem phần SQL Templates bên dưới          |
| `depends_on`      | Rule IDs phải chạy trước (comma-separated, NULL nếu không)  | `T001,T002`                              |
| `execution_order` | Thứ tự chạy (nhỏ chạy trước)                                | `1`, `2`, `3`                            |
| `is_active`       | Rule có active không                                         | `true`                                   |

**`transform_type` giải thích:**

| Type           | Mô tả                                  | Khi nào dùng                            |
|----------------|------------------------------------------|-----------------------------------------|
| `create_table` | CREATE TABLE IF NOT EXISTS               | Lần đầu tạo silver/gold table           |
| `dedup`        | Loại bỏ duplicate rows                   | Silver: raw data có thể trùng           |
| `merge`        | MERGE INTO (upsert)                      | Incremental load vào silver/gold        |
| `aggregate`    | GROUP BY, SUM, COUNT                     | Gold: business aggregations             |
| `filter`       | WHERE clause lọc dữ liệu               | Silver: lọc bỏ invalid records          |
| `custom`       | SQL tùy chỉnh                           | Bất kỳ logic đặc biệt nào              |

---

### 3.4 data_quality_rules — Quy tắc kiểm tra chất lượng

```sql
CREATE TABLE minio-datalake.metadata.data_quality_rules (
    dq_rule_id          VARCHAR,
    pipeline_id         VARCHAR,
    target_layer        VARCHAR,
    target_table        VARCHAR,
    column_name         VARCHAR,
    rule_type           VARCHAR,
    rule_expression     VARCHAR,
    severity            VARCHAR,
    threshold_pct       DOUBLE,
    is_active           BOOLEAN,
    description         VARCHAR
);
```

**Giải thích từng field:**

| Field              | Kiểu     | Mô tả                                                         | Ví dụ                                              |
|--------------------|----------|----------------------------------------------------------------|-----------------------------------------------------|
| `dq_rule_id`       | VARCHAR  | ID duy nhất cho DQ rule                                        | `DQ001`                                             |
| `pipeline_id`      | VARCHAR  | FK → pipeline_config, rule thuộc pipeline nào                  | `P001`                                              |
| `target_layer`     | VARCHAR  | Layer cần check (sau transform)                                | `silver`, `gold`                                    |
| `target_table`     | VARCHAR  | Bảng cần check                                                 | `transactions`                                      |
| `column_name`      | VARCHAR  | Cột cần kiểm tra                                               | `txn_id`, `transaction_amount`, `bank_code`         |
| `rule_type`        | VARCHAR  | Loại kiểm tra (xem bảng bên dưới)                             | `not_null`, `unique`, `range`, `regex`              |
| `rule_expression`  | VARCHAR  | SQL boolean expression, dùng `${column}` làm placeholder       | `${column} IS NOT NULL`, `${column} >= 0`           |
| `severity`         | VARCHAR  | Mức độ nghiêm trọng khi fail (xem bảng bên dưới)              | `info`, `warning`, `error`, `critical`              |
| `threshold_pct`    | DOUBLE   | % rows fail cho phép (0.0 = không chấp nhận bất kỳ failure)   | `0.0` (strict), `1.0` (cho phép 1% fail), `5.0`    |
| `is_active`        | BOOLEAN  | Rule có đang active không                                       | `true`, `false`                                     |
| `description`      | VARCHAR  | Mô tả rule                                                     | `Transaction ID must never be null`                 |

**`rule_type` options:**

| Type      | Expression example                                  | Mô tả                          |
|-----------|-----------------------------------------------------|---------------------------------|
| `not_null`| `${column} IS NOT NULL`                             | Cột không được NULL             |
| `unique`  | `COUNT(DISTINCT ${column}) = COUNT(*)`              | Cột phải unique                 |
| `range`   | `${column} BETWEEN 0 AND 999999999`                 | Giá trị trong khoảng            |
| `regex`   | `REGEXP_LIKE(${column}, '^[0-9]{9}$')`             | Khớp pattern                    |
| `referential`| `${column} IN (SELECT id FROM ref_table)`        | Referential integrity           |
| `freshness`| `MAX(${column}) >= CURRENT_TIMESTAMP - INTERVAL '1' DAY` | Dữ liệu không quá cũ     |
| `custom`  | Bất kỳ SQL boolean expression                       | Tùy chỉnh                      |

**`severity` levels:**

| Level      | Hành động                                          |
|------------|-----------------------------------------------------|
| `info`     | Log only, không block pipeline                      |
| `warning`  | Log + ghi vào DQ report, pipeline tiếp tục          |
| `error`    | Block pipeline, ghi log, cần manual review          |
| `critical` | Block pipeline + alert (email/Slack)                |

---

### 3.5 pipeline_execution_log — Lịch sử chạy pipeline

```sql
CREATE TABLE minio-datalake.metadata.pipeline_execution_log (
    execution_id        VARCHAR,
    pipeline_id         VARCHAR,
    pipeline_name       VARCHAR,
    layer               VARCHAR,
    start_time          TIMESTAMP,
    end_time            TIMESTAMP,
    status              VARCHAR,
    rows_processed      BIGINT,
    rows_inserted       BIGINT,
    rows_updated        BIGINT,
    rows_rejected       BIGINT,
    last_watermark      VARCHAR,
    error_message       VARCHAR,
    execution_params    VARCHAR,
    created_at          TIMESTAMP
);
```

**Giải thích từng field:**

| Field              | Kiểu      | Mô tả                                                         | Ví dụ                                              |
|--------------------|-----------|----------------------------------------------------------------|-----------------------------------------------------|
| `execution_id`     | VARCHAR   | ID duy nhất cho mỗi lần chạy                                   | `P001_20260624_020000`                              |
| `pipeline_id`      | VARCHAR   | FK → pipeline_config, pipeline nào đã chạy                     | `P001`                                              |
| `pipeline_name`    | VARCHAR   | Tên pipeline (denormalized, tiện query)                        | `ingest_transactions`                               |
| `layer`            | VARCHAR   | Layer đã xử lý trong lần chạy này                              | `bronze`, `silver`, `gold`, `dq_check`              |
| `start_time`       | TIMESTAMP | Thời gian bắt đầu chạy                                        | `2026-06-24 02:00:00`                               |
| `end_time`         | TIMESTAMP | Thời gian kết thúc                                             | `2026-06-24 02:05:30`                               |
| `status`           | VARCHAR   | Trạng thái kết quả (xem bảng bên dưới)                        | `success`, `failed`, `running`                      |
| `rows_processed`   | BIGINT    | Tổng số rows đã xử lý                                         | `15000`                                             |
| `rows_inserted`    | BIGINT    | Số rows INSERT mới                                             | `12000`                                             |
| `rows_updated`     | BIGINT    | Số rows UPDATE (MERGE matched)                                 | `3000`                                              |
| `rows_rejected`    | BIGINT    | Số rows bị reject (DQ fail, parse error)                       | `5`                                                 |
| `last_watermark`   | VARCHAR   | Giá trị watermark mới nhất sau lần chạy (dùng cho incremental) | `2026-06-24 01:59:59`                               |
| `error_message`    | VARCHAR   | Chi tiết lỗi nếu status = failed (NULL nếu success)           | `Connection refused`, `OOM`, `NULL`                 |
| `execution_params` | VARCHAR   | Tham số runtime (load_type, batch_size, ...)                   | `incremental`, `full`                               |
| `created_at`       | TIMESTAMP | Thời gian ghi log                                              | `2026-06-24 02:05:30`                               |

**`status` values:**

| Status     | Mô tả                                              |
|------------|------------------------------------------------------|
| `running`  | Pipeline đang chạy                                   |
| `success`  | Hoàn thành thành công                                |
| `failed`   | Lỗi, cần kiểm tra error_message                     |
| `skipped`  | Bỏ qua (ví dụ: không có dữ liệu mới)               |
| `partial`  | Hoàn thành 1 phần (có rows bị reject)               |

---

## 4. Sample Data — Demo Pipeline

### 4.1 Pipeline Config cho demo

```sql
-- Pipeline 1: Ingest transactions (incremental)
INSERT INTO minio-datalake.metadata.pipeline_config VALUES (
    'P001',
    'ingest_transactions',
    'jdbc',
    'source-postgres-pool',
    'public',
    'transactions',
    'bronze',
    'bronze/transactions/dt=${date}',
    'transactions',
    'incremental',
    'txn_id',
    'created_at',
    'transaction_date',
    10000,
    '0 2 * * *',
    true,
    'Daily incremental ingest of transactions from core banking PostgreSQL',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
);

-- Pipeline 2: Ingest merchants (full load, bảng nhỏ)
INSERT INTO minio-datalake.metadata.pipeline_config VALUES (
    'P002',
    'ingest_merchants',
    'jdbc',
    'source-postgres-pool',
    'public',
    'merchants',
    'bronze',
    'bronze/merchants/dt=${date}',
    'merchants',
    'full',
    'merchant_id',
    NULL,
    NULL,
    5000,
    '0 3 * * *',
    true,
    'Daily full load of merchant master data',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
);

-- Pipeline 3: Ingest bank_codes (full load, reference data)
INSERT INTO minio-datalake.metadata.pipeline_config VALUES (
    'P003',
    'ingest_bank_codes',
    'jdbc',
    'source-postgres-pool',
    'public',
    'bank_codes',
    'bronze',
    'bronze/bank_codes/dt=${date}',
    'bank_codes',
    'full',
    'bank_code',
    NULL,
    NULL,
    1000,
    '0 4 * * 1',
    true,
    'Weekly full load of bank reference codes',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
);

-- Pipeline 4: Ingest settlements (incremental)
INSERT INTO minio-datalake.metadata.pipeline_config VALUES (
    'P004',
    'ingest_settlements',
    'jdbc',
    'source-postgres-pool',
    'public',
    'settlements',
    'bronze',
    'bronze/settlements/dt=${date}',
    'settlements',
    'incremental',
    'settlement_id',
    'settlement_date',
    'settlement_date',
    10000,
    '0 5 * * *',
    true,
    'Daily incremental ingest of settlement records',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
);
```

### 4.2 Column Mapping cho transactions

```sql
-- Transactions column mappings
INSERT INTO minio-datalake.metadata.column_mapping VALUES
('M001', 'P001', 'txn_id',           'txn_id',              'VARCHAR',        NULL,                                    true,  false, NULL, 1, 'Primary key'),
('M002', 'P001', 'amount',           'transaction_amount',  'DECIMAL(18,2)',  'CAST(${src} AS DECIMAL(18,2))',         false, false, NULL, 2, 'Transaction amount'),
('M003', 'P001', 'currency',         'currency_code',       'VARCHAR',        'UPPER(TRIM(${src}))',                   false, false, '''VND''', 3, 'ISO currency code'),
('M004', 'P001', 'merchant_id',      'merchant_id',         'VARCHAR',        NULL,                                    false, true,  NULL, 4, 'FK to merchants'),
('M005', 'P001', 'bank_code',        'bank_code',           'VARCHAR',        'LPAD(${src}, 9, ''0'')',                false, false, NULL, 5, 'NAPAS bank code (9 digits)'),
('M006', 'P001', 'status_code',      'status_code',         'VARCHAR',        NULL,                                    false, false, NULL, 6, 'Transaction status'),
('M007', 'P001', 'transaction_type', 'transaction_type',    'VARCHAR',        'UPPER(${src})',                         false, false, NULL, 7, 'Type: PURCHASE, TRANSFER, etc'),
('M008', 'P001', 'created_at',       'created_at',          'TIMESTAMP',      'CAST(${src} AS TIMESTAMP)',             false, false, NULL, 8, 'Record creation time'),
('M009', 'P001', 'updated_at',       'updated_at',          'TIMESTAMP',      'CAST(${src} AS TIMESTAMP)',             false, true,  NULL, 9, 'Last update time');
```

### 4.3 Transform Rules

```sql
-- Silver: Dedup transactions
INSERT INTO minio-datalake.metadata.transform_rules VALUES (
    'T001', 'P001', 'dedup_transactions',
    'bronze', 'silver', 'dedup',
    'CREATE TABLE minio-datalake.silver.transactions AS
     SELECT * FROM (
       SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY txn_id
           ORDER BY created_at DESC
         ) AS rn
       FROM minio-datalake.bronze.transactions
     ) WHERE rn = 1',
    NULL, 1, true,
    'Remove duplicate transactions, keep latest by created_at'
);

-- Silver: Merge incremental
INSERT INTO minio-datalake.metadata.transform_rules VALUES (
    'T002', 'P001', 'merge_transactions',
    'bronze', 'silver', 'merge',
    'MERGE INTO minio-datalake.silver.transactions AS target
     USING (
       SELECT * FROM (
         SELECT *,
           ROW_NUMBER() OVER (PARTITION BY txn_id ORDER BY created_at DESC) AS rn
         FROM minio-datalake.bronze.transactions
         WHERE created_at > ''${last_watermark}''
       ) WHERE rn = 1
     ) AS source
     ON target.txn_id = source.txn_id
     WHEN MATCHED THEN UPDATE SET *
     WHEN NOT MATCHED THEN INSERT *',
    'T001', 2, true,
    'Incremental merge new/updated transactions into silver'
);

-- Gold: Daily transaction summary
INSERT INTO minio-datalake.metadata.transform_rules VALUES (
    'T003', 'P001', 'daily_txn_summary',
    'silver', 'gold', 'aggregate',
    'CREATE OR REPLACE VIEW minio-datalake.gold.daily_transaction_summary AS
     SELECT
       CAST(created_at AS DATE) AS transaction_date,
       bank_code,
       transaction_type,
       status_code,
       COUNT(*) AS total_transactions,
       SUM(transaction_amount) AS total_amount,
       AVG(transaction_amount) AS avg_amount,
       MIN(transaction_amount) AS min_amount,
       MAX(transaction_amount) AS max_amount
     FROM minio-datalake.silver.transactions
     GROUP BY
       CAST(created_at AS DATE),
       bank_code,
       transaction_type,
       status_code',
    'T002', 3, true,
    'Daily aggregated KPIs by bank, type, status'
);

-- Gold: Bank performance KPIs
INSERT INTO minio-datalake.metadata.transform_rules VALUES (
    'T004', 'P001', 'bank_performance_kpis',
    'silver', 'gold', 'aggregate',
    'CREATE OR REPLACE VIEW minio-datalake.gold.bank_performance_kpis AS
     SELECT
       bank_code,
       COUNT(*) AS total_transactions,
       SUM(CASE WHEN status_code = ''00'' THEN 1 ELSE 0 END) AS successful_transactions,
       SUM(CASE WHEN status_code != ''00'' THEN 1 ELSE 0 END) AS failed_transactions,
       CAST(SUM(CASE WHEN status_code = ''00'' THEN 1 ELSE 0 END) AS DOUBLE) /
         NULLIF(COUNT(*), 0) * 100 AS success_rate_pct,
       SUM(transaction_amount) AS total_volume,
       AVG(transaction_amount) AS avg_transaction_value
     FROM minio-datalake.silver.transactions
     GROUP BY bank_code',
    'T002', 4, true,
    'Bank-level performance metrics'
);
```

### 4.4 Data Quality Rules

```sql
-- DQ: transactions.txn_id not null
INSERT INTO minio-datalake.metadata.data_quality_rules VALUES (
    'DQ001', 'P001', 'silver', 'transactions', 'txn_id',
    'not_null', '${column} IS NOT NULL', 'critical', 0.0, true,
    'Transaction ID must never be null'
);

-- DQ: transactions.amount positive
INSERT INTO minio-datalake.metadata.data_quality_rules VALUES (
    'DQ002', 'P001', 'silver', 'transactions', 'transaction_amount',
    'range', '${column} >= 0', 'error', 1.0, true,
    'Transaction amount must be non-negative (allow 1% tolerance)'
);

-- DQ: transactions.bank_code format
INSERT INTO minio-datalake.metadata.data_quality_rules VALUES (
    'DQ003', 'P001', 'silver', 'transactions', 'bank_code',
    'regex', 'LENGTH(${column}) = 9', 'warning', 5.0, true,
    'Bank code should be 9 digits (NAPAS format)'
);

-- DQ: transactions freshness
INSERT INTO minio-datalake.metadata.data_quality_rules VALUES (
    'DQ004', 'P001', 'silver', 'transactions', 'created_at',
    'freshness',
    'MAX(${column}) >= CURRENT_TIMESTAMP - INTERVAL ''2'' DAY',
    'warning', 0.0, true,
    'Data should not be more than 2 days old'
);

-- DQ: transactions unique txn_id
INSERT INTO minio-datalake.metadata.data_quality_rules VALUES (
    'DQ005', 'P001', 'silver', 'transactions', 'txn_id',
    'unique',
    'COUNT(*) = COUNT(DISTINCT ${column})',
    'error', 0.0, true,
    'Transaction IDs must be unique in silver layer'
);
```

---

## 5. Demo Source Database Setup

Để test pipeline, tạo fake data trong PostgreSQL (dùng lại `postgres-superset` đang chạy):

```sql
-- Connect vào postgres-superset:
-- kubectl port-forward -n data-visualization svc/superset-postgresql 5433:5432
-- psql -h localhost -p 5433 -U superset -d superset

-- 1. Bảng bank_codes (reference data)
CREATE TABLE IF NOT EXISTS bank_codes (
    bank_code       VARCHAR(9) PRIMARY KEY,
    bank_name       VARCHAR(100) NOT NULL,
    bank_short_name VARCHAR(20),
    swift_code      VARCHAR(11),
    is_active       BOOLEAN DEFAULT true,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO bank_codes VALUES
('970415001', 'Ngân hàng TMCP Công Thương Việt Nam', 'VietinBank', 'ICBVVNVX', true, CURRENT_TIMESTAMP),
('970436002', 'Ngân hàng TMCP Ngoại Thương Việt Nam', 'Vietcombank', 'BFTVVNVX', true, CURRENT_TIMESTAMP),
('970418003', 'Ngân hàng TMCP Đầu tư và Phát triển', 'BIDV', 'BIDVVNVX', true, CURRENT_TIMESTAMP),
('970405004', 'Ngân hàng Nông nghiệp và PTNT', 'Agribank', 'VBAAVNVX', true, CURRENT_TIMESTAMP),
('970407005', 'Ngân hàng TMCP Kỹ Thương', 'Techcombank', 'VTCBVNVX', true, CURRENT_TIMESTAMP),
('970423006', 'Ngân hàng TMCP Tiên Phong', 'TPBank', 'TPBVVNVX', true, CURRENT_TIMESTAMP),
('970432007', 'Ngân hàng TMCP Việt Nam Thịnh Vượng', 'VPBank', 'VPBKVNVX', true, CURRENT_TIMESTAMP),
('970422008', 'Ngân hàng TMCP Quân Đội', 'MB Bank', 'MSCBVNVX', true, CURRENT_TIMESTAMP)
ON CONFLICT (bank_code) DO NOTHING;

-- 2. Bảng merchants
CREATE TABLE IF NOT EXISTS merchants (
    merchant_id     VARCHAR(20) PRIMARY KEY,
    merchant_name   VARCHAR(200) NOT NULL,
    category        VARCHAR(50),
    bank_code       VARCHAR(9) REFERENCES bank_codes(bank_code),
    city            VARCHAR(100),
    is_active       BOOLEAN DEFAULT true,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO merchants (merchant_id, merchant_name, category, bank_code, city) VALUES
('MCH001', 'VinMart Hà Nội', 'RETAIL', '970415001', 'Hà Nội'),
('MCH002', 'Circle K Sài Gòn', 'CONVENIENCE', '970436002', 'Hồ Chí Minh'),
('MCH003', 'Grab Vietnam', 'TRANSPORT', '970407005', 'Hồ Chí Minh'),
('MCH004', 'Shopee Vietnam', 'ECOMMERCE', '970432007', 'Hồ Chí Minh'),
('MCH005', 'VNPay QR', 'PAYMENT', '970418003', 'Hà Nội'),
('MCH006', 'Momo Wallet', 'EWALLET', '970422008', 'Hồ Chí Minh'),
('MCH007', 'Bách Hóa Xanh', 'RETAIL', '970405004', 'Hồ Chí Minh'),
('MCH008', 'Highlands Coffee', 'FNB', '970423006', 'Hà Nội'),
('MCH009', 'Thế Giới Di Động', 'ELECTRONICS', '970415001', 'Hồ Chí Minh'),
('MCH010', 'Lazada Vietnam', 'ECOMMERCE', '970436002', 'Hồ Chí Minh')
ON CONFLICT (merchant_id) DO NOTHING;

-- 3. Bảng transactions (10,000 rows fake data)
CREATE TABLE IF NOT EXISTS transactions (
    txn_id              VARCHAR(30) PRIMARY KEY,
    amount              DECIMAL(18,2) NOT NULL,
    currency            VARCHAR(3) DEFAULT 'VND',
    merchant_id         VARCHAR(20) REFERENCES merchants(merchant_id),
    bank_code           VARCHAR(9) REFERENCES bank_codes(bank_code),
    status_code         VARCHAR(5) NOT NULL,
    transaction_type    VARCHAR(20) NOT NULL,
    card_number_masked  VARCHAR(19),
    description         VARCHAR(500),
    created_at          TIMESTAMP NOT NULL,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Generate 10,000 transactions
INSERT INTO transactions (txn_id, amount, currency, merchant_id, bank_code,
                          status_code, transaction_type, card_number_masked,
                          description, created_at, updated_at)
SELECT
    'TXN' || LPAD(g.id::TEXT, 10, '0') AS txn_id,
    ROUND((RANDOM() * 9999000 + 1000)::NUMERIC, 2) AS amount,
    'VND' AS currency,
    'MCH' || LPAD((FLOOR(RANDOM() * 10) + 1)::INT::TEXT, 3, '0') AS merchant_id,
    (ARRAY['970415001','970436002','970418003','970405004','970407005',
           '970423006','970432007','970422008'])
        [FLOOR(RANDOM() * 8 + 1)::INT] AS bank_code,
    (ARRAY['00','00','00','00','00','00','00','01','05','12'])
        [FLOOR(RANDOM() * 10 + 1)::INT] AS status_code,
    (ARRAY['PURCHASE','TRANSFER','WITHDRAWAL','PAYMENT','REFUND'])
        [FLOOR(RANDOM() * 5 + 1)::INT] AS transaction_type,
    '****-****-****-' || LPAD(FLOOR(RANDOM() * 9999 + 1)::INT::TEXT, 4, '0') AS card_number_masked,
    'Transaction ' || g.id AS description,
    CURRENT_TIMESTAMP - (RANDOM() * INTERVAL '30 days') AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM generate_series(1, 10000) AS g(id)
ON CONFLICT (txn_id) DO NOTHING;

-- 4. Bảng settlements
CREATE TABLE IF NOT EXISTS settlements (
    settlement_id       VARCHAR(20) PRIMARY KEY,
    bank_code           VARCHAR(9) REFERENCES bank_codes(bank_code),
    settlement_date     DATE NOT NULL,
    total_transactions  INT NOT NULL,
    total_amount        DECIMAL(18,2) NOT NULL,
    net_amount          DECIMAL(18,2) NOT NULL,
    fee_amount          DECIMAL(18,2) NOT NULL,
    status              VARCHAR(20) DEFAULT 'PENDING',
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO settlements (settlement_id, bank_code, settlement_date,
                         total_transactions, total_amount, net_amount,
                         fee_amount, status, created_at)
SELECT
    'STL' || LPAD(g.id::TEXT, 8, '0') AS settlement_id,
    (ARRAY['970415001','970436002','970418003','970405004','970407005',
           '970423006','970432007','970422008'])
        [FLOOR(RANDOM() * 8 + 1)::INT] AS bank_code,
    (CURRENT_DATE - (g.id || ' days')::INTERVAL)::DATE AS settlement_date,
    FLOOR(RANDOM() * 5000 + 100)::INT AS total_transactions,
    ROUND((RANDOM() * 99999000000 + 1000000)::NUMERIC, 2) AS total_amount,
    ROUND((RANDOM() * 99000000000 + 1000000)::NUMERIC, 2) AS net_amount,
    ROUND((RANDOM() * 999000000 + 10000)::NUMERIC, 2) AS fee_amount,
    (ARRAY['COMPLETED','COMPLETED','COMPLETED','PENDING','PROCESSING'])
        [FLOOR(RANDOM() * 5 + 1)::INT] AS status,
    CURRENT_TIMESTAMP - (g.id || ' days')::INTERVAL AS created_at
FROM generate_series(1, 240) AS g(id)
ON CONFLICT (settlement_id) DO NOTHING;
```

---

## 6. Verify Metadata Setup

Sau khi tạo xong, kiểm tra trong Dremio SQL Runner:

```sql
-- Kiểm tra pipeline config
SELECT pipeline_id, pipeline_name, source_table, load_type, is_active
FROM minio-datalake.metadata.pipeline_config;

-- Kiểm tra column mappings cho transactions
SELECT source_column, target_column, data_type, transformation
FROM minio-datalake.metadata.column_mapping
WHERE pipeline_id = 'P001'
ORDER BY column_order;

-- Kiểm tra transform rules
SELECT rule_name, source_layer, target_layer, transform_type, execution_order
FROM minio-datalake.metadata.transform_rules
WHERE pipeline_id = 'P001'
ORDER BY execution_order;

-- Kiểm tra DQ rules
SELECT column_name, rule_type, severity
FROM minio-datalake.metadata.data_quality_rules
WHERE pipeline_id = 'P001';

-- Kiểm tra source data
-- (Chạy qua NiFi JDBC hoặc port-forward postgres)
-- SELECT COUNT(*) FROM transactions;        -- Expected: 10,000
-- SELECT COUNT(*) FROM merchants;           -- Expected: 10
-- SELECT COUNT(*) FROM bank_codes;          -- Expected: 8
-- SELECT COUNT(*) FROM settlements;         -- Expected: 240
```

---

## 7. Tài Liệu Tiếp Theo

| Bước | Doc | Mô tả |
|------|-----|-------|
| Tiếp theo | [11-nifi-dynamic-pipeline-setup.md](11-nifi-dynamic-pipeline-setup.md) | Build NiFi flow đọc metadata và execute |
| Sau đó | [12-dynamic-sql-templates.md](12-dynamic-sql-templates.md) | Chi tiết SQL templates cho mọi operation |
| Cuối cùng | [13-pipeline-operations-runbook.md](13-pipeline-operations-runbook.md) | Thêm bảng mới, monitor, troubleshoot |
