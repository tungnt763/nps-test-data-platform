# Dynamic SQL Templates Reference

> **Phiên bản:** 1.0 | **Ngày:** 2026-06-24
> **Engine:** Dremio SQL (Iceberg tables trên MinIO)
> **Tham chiếu:** [10-metadata-tables-design.md](10-metadata-tables-design.md) | [11-nifi-dynamic-pipeline-setup.md](11-nifi-dynamic-pipeline-setup.md)

---

## 1. Biến Template (Variables)

Tất cả SQL templates dùng `${variable}` — được NiFi Expression Language resolve từ FlowFile attributes.

### 1.1 Biến từ pipeline_config

| Variable              | Nguồn                 | Ví dụ                    | Mô tả                    |
|-----------------------|-----------------------|--------------------------|---------------------------|
| `${source_schema}`    | pipeline_config       | `public`                 | Schema nguồn              |
| `${source_table}`     | pipeline_config       | `transactions`           | Bảng nguồn                |
| `${target_table}`     | pipeline_config       | `transactions`           | Bảng đích                 |
| `${primary_keys}`     | pipeline_config       | `txn_id`                 | Khóa chính                |
| `${watermark_column}` | pipeline_config       | `created_at`             | Cột incremental           |
| `${partition_columns}`| pipeline_config       | `transaction_date`       | Cột partition             |
| `${load_type}`        | pipeline_config       | `incremental`            | Kiểu load                 |

### 1.2 Biến từ runtime

| Variable                    | Nguồn                  | Ví dụ                           |
|-----------------------------|------------------------|---------------------------------|
| `${last_watermark}`         | pipeline_execution_log | `2026-06-23 02:00:00`           |
| `${executesql.row.count}`   | NiFi ExecuteSQL        | `1523`                          |
| `${now():format('...')}`    | NiFi Expression Lang   | `2026-06-24`                    |
| `${pipeline_id}`            | pipeline_config        | `P001`                          |

### 1.3 Biến từ column_mapping

| Variable    | Nguồn          | Ví dụ          |
|-------------|----------------|----------------|
| `${src}`    | source_column  | `TXN_AMOUNT`   |
| `${tgt}`    | target_column  | `transaction_amount` |

---

## 2. Bronze Layer — Ingestion Templates

### 2.1 Full Load

**Khi dùng:** Bảng nhỏ (< 100K rows), reference data, hoặc không có watermark column.

```sql
-- Template ID: BRONZE_FULL_LOAD
-- Transform type: full
-- Được dùng bởi: NiFi ReplaceText → ExecuteSQL (Source DB)

SELECT *
FROM ${source_schema}.${source_table}
```

**Variant — Full Load với column selection (từ column_mapping):**

```sql
-- Template ID: BRONZE_FULL_LOAD_COLUMNS
-- Khi cần chỉ lấy 1 số cột, không SELECT *

SELECT ${column_list}
FROM ${source_schema}.${source_table}
```

> `${column_list}` được NiFi build từ column_mapping:
> `txn_id, amount AS transaction_amount, UPPER(currency) AS currency_code, ...`

### 2.2 Incremental Load (Watermark-based)

**Khi dùng:** Bảng lớn, có cột timestamp/ID tăng dần. Chỉ lấy records mới.

```sql
-- Template ID: BRONZE_INCREMENTAL_LOAD
-- Transform type: incremental
-- ${watermark_column}: cột dùng để track changes (vd: updated_at, created_at)
-- ${last_watermark}: giá trị watermark lần chạy gần nhất

SELECT *
FROM ${source_schema}.${source_table}
WHERE ${watermark_column} > '${last_watermark}'
ORDER BY ${watermark_column}
```

**Variant — Incremental với batch size:**

```sql
-- Template ID: BRONZE_INCREMENTAL_BATCH
-- Giới hạn số rows mỗi lần fetch (tránh OOM)

SELECT *
FROM ${source_schema}.${source_table}
WHERE ${watermark_column} > '${last_watermark}'
ORDER BY ${watermark_column}
LIMIT ${batch_size}
```

### 2.3 Full Load with Partition Overwrite

**Khi dùng:** Full load nhưng chỉ overwrite partition hiện tại (không xóa partition cũ).

```sql
-- Template ID: BRONZE_PARTITION_OVERWRITE
-- Load theo partition (vd: theo ngày)

SELECT *
FROM ${source_schema}.${source_table}
WHERE CAST(${partition_columns} AS DATE) = CURRENT_DATE
```

---

## 3. Silver Layer — Transform Templates

Silver transforms chạy trên **Dremio**, orchestrate bởi NiFi.
NiFi gửi SQL qua Dremio JDBC → Dremio execute trên Iceberg tables.

### 3.1 Create Table (First Run)

**Khi dùng:** Lần đầu tiên tạo silver table từ bronze.

```sql
-- Template ID: SILVER_CREATE_TABLE
-- Transform type: create_table
-- Chạy lần đầu, sau đó chuyển sang MERGE

CREATE TABLE IF NOT EXISTS minio-datalake.silver.${target_table} AS
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ${primary_keys}
            ORDER BY ${watermark_column} DESC
        ) AS _row_num
    FROM minio-datalake.bronze.${source_table}
) deduped
WHERE _row_num = 1
```

> **Giải thích:**
> - `ROW_NUMBER()`: loại duplicate, giữ record mới nhất
> - `PARTITION BY ${primary_keys}`: group theo khóa chính
> - `ORDER BY ${watermark_column} DESC`: record mới nhất có _row_num = 1
> - `WHERE _row_num = 1`: chỉ lấy record mới nhất

### 3.2 Deduplication (Standalone)

**Khi dùng:** Bronze có thể chứa duplicate rows (multi-run, source issues).

```sql
-- Template ID: SILVER_DEDUP
-- Transform type: dedup
-- Input: bronze table có duplicate
-- Output: silver table không duplicate

CREATE OR REPLACE TABLE minio-datalake.silver.${target_table} AS
SELECT * EXCEPT(_row_num)
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ${primary_keys}
            ORDER BY ${watermark_column} DESC
        ) AS _row_num
    FROM minio-datalake.bronze.${source_table}
) deduped
WHERE _row_num = 1
```

> **`EXCEPT(_row_num)`**: Dremio syntax để loại bỏ cột helper `_row_num` khỏi output.
> Nếu Dremio version không hỗ trợ, liệt kê explicit columns thay vì `SELECT *`.

### 3.3 Merge (Incremental Upsert)

**Khi dùng:** Sau lần đầu, incremental merge data mới vào silver.

```sql
-- Template ID: SILVER_MERGE
-- Transform type: merge
-- Upsert: INSERT nếu chưa có, UPDATE nếu đã tồn tại

MERGE INTO minio-datalake.silver.${target_table} AS target
USING (
    SELECT * FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY ${primary_keys}
                ORDER BY ${watermark_column} DESC
            ) AS _row_num
        FROM minio-datalake.bronze.${source_table}
        WHERE ${watermark_column} > '${last_watermark}'
    ) WHERE _row_num = 1
) AS source
ON target.${primary_keys} = source.${primary_keys}
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
```

> **Lưu ý composite key:**
> Nếu `primary_keys = 'txn_id,merchant_id'`, ON clause cần:
> `ON target.txn_id = source.txn_id AND target.merchant_id = source.merchant_id`
>
> NiFi có thể generate ON clause từ primary_keys attribute bằng ExecuteScript hoặc ReplaceText pattern.

### 3.4 Merge with Column Transformations

**Khi dùng:** Merge + apply column transformations từ column_mapping.

```sql
-- Template ID: SILVER_MERGE_TRANSFORM
-- Transform type: merge
-- Áp dụng CAST, COALESCE, TRIM, etc. từ column_mapping

MERGE INTO minio-datalake.silver.${target_table} AS target
USING (
    SELECT
        txn_id,
        CAST(amount AS DECIMAL(18,2)) AS transaction_amount,
        UPPER(TRIM(currency)) AS currency_code,
        merchant_id,
        LPAD(CAST(bank_code AS VARCHAR), 9, '0') AS bank_code,
        status_code,
        UPPER(transaction_type) AS transaction_type,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(updated_at AS TIMESTAMP) AS updated_at
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY txn_id
                ORDER BY created_at DESC
            ) AS _row_num
        FROM minio-datalake.bronze.transactions
        WHERE created_at > '${last_watermark}'
    ) WHERE _row_num = 1
) AS source
ON target.txn_id = source.txn_id
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
```

> **Cách generate dynamic:**
> NiFi đọc `column_mapping` → build SELECT clause:
> ```
> CAST(${src} AS ${data_type}) AS ${tgt}  -- nếu transformation = NULL
> ${transformation} AS ${tgt}              -- nếu transformation != NULL
> ```

### 3.5 Filter (Remove Invalid Records)

**Khi dùng:** Lọc bỏ records không hợp lệ ở silver layer.

```sql
-- Template ID: SILVER_FILTER
-- Transform type: filter
-- Ví dụ: chỉ giữ transactions thành công

DELETE FROM minio-datalake.silver.${target_table}
WHERE status_code NOT IN ('00', '01')
```

Hoặc tạo filtered view:

```sql
-- Template ID: SILVER_FILTER_VIEW
-- Transform type: filter
-- Tạo view chỉ chứa valid records

CREATE OR REPLACE VIEW minio-datalake.silver.${target_table}_valid AS
SELECT *
FROM minio-datalake.silver.${target_table}
WHERE status_code = '00'
  AND transaction_amount > 0
  AND bank_code IS NOT NULL
```

---

## 4. Gold Layer — Aggregation Templates

Gold layer tạo business-ready datasets: aggregations, KPIs, joined tables.

### 4.1 Daily Summary

```sql
-- Template ID: GOLD_DAILY_SUMMARY
-- Transform type: aggregate
-- KPI hàng ngày cho transactions

CREATE OR REPLACE VIEW minio-datalake.gold.daily_${target_table}_summary AS
SELECT
    CAST(created_at AS DATE)                AS transaction_date,
    bank_code,
    transaction_type,
    COUNT(*)                                AS total_count,
    SUM(transaction_amount)                 AS total_amount,
    AVG(transaction_amount)                 AS avg_amount,
    MIN(transaction_amount)                 AS min_amount,
    MAX(transaction_amount)                 AS max_amount,
    SUM(CASE WHEN status_code = '00'
        THEN 1 ELSE 0 END)                 AS success_count,
    SUM(CASE WHEN status_code != '00'
        THEN 1 ELSE 0 END)                 AS failed_count,
    CAST(SUM(CASE WHEN status_code = '00'
        THEN 1 ELSE 0 END) AS DOUBLE)
        / NULLIF(COUNT(*), 0) * 100        AS success_rate_pct
FROM minio-datalake.silver.${source_table}
GROUP BY
    CAST(created_at AS DATE),
    bank_code,
    transaction_type
```

### 4.2 Entity KPIs

```sql
-- Template ID: GOLD_ENTITY_KPIS
-- Transform type: aggregate
-- KPIs theo entity (bank, merchant, etc.)

CREATE OR REPLACE VIEW minio-datalake.gold.bank_performance AS
SELECT
    t.bank_code,
    b.bank_name,
    b.bank_short_name,
    COUNT(*)                                AS total_transactions,
    SUM(t.transaction_amount)               AS total_volume,
    AVG(t.transaction_amount)               AS avg_transaction_value,

    -- Success metrics
    SUM(CASE WHEN t.status_code = '00'
        THEN 1 ELSE 0 END)                 AS successful_txns,
    CAST(SUM(CASE WHEN t.status_code = '00'
        THEN 1 ELSE 0 END) AS DOUBLE)
        / NULLIF(COUNT(*), 0) * 100        AS success_rate_pct,

    -- Volume metrics
    SUM(CASE WHEN t.status_code = '00'
        THEN t.transaction_amount ELSE 0 END) AS successful_volume,

    -- Transaction type breakdown
    SUM(CASE WHEN t.transaction_type = 'PURCHASE'
        THEN 1 ELSE 0 END)                 AS purchase_count,
    SUM(CASE WHEN t.transaction_type = 'TRANSFER'
        THEN 1 ELSE 0 END)                 AS transfer_count,
    SUM(CASE WHEN t.transaction_type = 'WITHDRAWAL'
        THEN 1 ELSE 0 END)                 AS withdrawal_count,
    SUM(CASE WHEN t.transaction_type = 'PAYMENT'
        THEN 1 ELSE 0 END)                 AS payment_count

FROM minio-datalake.silver.transactions t
LEFT JOIN minio-datalake.silver.bank_codes b
    ON t.bank_code = b.bank_code
GROUP BY t.bank_code, b.bank_name, b.bank_short_name
```

### 4.3 Merchant Analytics

```sql
-- Template ID: GOLD_MERCHANT_ANALYTICS
-- Transform type: aggregate

CREATE OR REPLACE VIEW minio-datalake.gold.merchant_analytics AS
SELECT
    t.merchant_id,
    m.merchant_name,
    m.category,
    m.city,
    COUNT(*)                                AS total_transactions,
    SUM(t.transaction_amount)               AS total_revenue,
    AVG(t.transaction_amount)               AS avg_order_value,
    COUNT(DISTINCT CAST(t.created_at AS DATE)) AS active_days,
    MIN(t.created_at)                       AS first_transaction,
    MAX(t.created_at)                       AS last_transaction,
    CAST(SUM(CASE WHEN t.status_code = '00'
        THEN 1 ELSE 0 END) AS DOUBLE)
        / NULLIF(COUNT(*), 0) * 100        AS success_rate_pct
FROM minio-datalake.silver.transactions t
LEFT JOIN minio-datalake.silver.merchants m
    ON t.merchant_id = m.merchant_id
GROUP BY t.merchant_id, m.merchant_name, m.category, m.city
```

### 4.4 Settlement Reconciliation

```sql
-- Template ID: GOLD_SETTLEMENT_RECON
-- Transform type: aggregate
-- So khớp giao dịch vs quyết toán

CREATE OR REPLACE VIEW minio-datalake.gold.settlement_reconciliation AS
SELECT
    s.settlement_id,
    s.bank_code,
    b.bank_short_name,
    s.settlement_date,
    s.total_transactions AS settled_count,
    s.total_amount AS settled_amount,
    t.actual_count,
    t.actual_amount,
    (t.actual_count - s.total_transactions) AS count_diff,
    (t.actual_amount - s.total_amount) AS amount_diff,
    CASE
        WHEN t.actual_count = s.total_transactions
         AND t.actual_amount = s.total_amount THEN 'MATCHED'
        WHEN t.actual_count IS NULL THEN 'NO_TRANSACTIONS'
        ELSE 'MISMATCH'
    END AS recon_status
FROM minio-datalake.silver.settlements s
LEFT JOIN (
    SELECT
        bank_code,
        CAST(created_at AS DATE) AS txn_date,
        COUNT(*) AS actual_count,
        SUM(transaction_amount) AS actual_amount
    FROM minio-datalake.silver.transactions
    WHERE status_code = '00'
    GROUP BY bank_code, CAST(created_at AS DATE)
) t ON s.bank_code = t.bank_code AND s.settlement_date = t.txn_date
LEFT JOIN minio-datalake.silver.bank_codes b ON s.bank_code = b.bank_code
```

### 4.5 Hourly Trend Analysis

```sql
-- Template ID: GOLD_HOURLY_TREND
-- Transform type: aggregate
-- Phân tích xu hướng theo giờ

CREATE OR REPLACE VIEW minio-datalake.gold.hourly_transaction_trend AS
SELECT
    CAST(created_at AS DATE) AS transaction_date,
    EXTRACT(HOUR FROM created_at) AS hour_of_day,
    COUNT(*) AS transaction_count,
    SUM(transaction_amount) AS total_amount,
    AVG(transaction_amount) AS avg_amount
FROM minio-datalake.silver.transactions
WHERE status_code = '00'
GROUP BY CAST(created_at AS DATE), EXTRACT(HOUR FROM created_at)
ORDER BY transaction_date, hour_of_day
```

---

## 5. Data Quality Check Templates

DQ checks chạy SAU transform, trước khi mark pipeline là success.

### 5.1 Not Null Check

```sql
-- Template ID: DQ_NOT_NULL
-- Rule type: not_null

SELECT
    '${dq_rule_id}' AS rule_id,
    '${pipeline_id}' AS pipeline_id,
    '${column_name}' AS column_name,
    'not_null' AS rule_type,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN ${column_name} IS NULL THEN 1 ELSE 0 END) AS failed_rows,
    CAST(SUM(CASE WHEN ${column_name} IS NULL THEN 1 ELSE 0 END) AS DOUBLE)
        / NULLIF(COUNT(*), 0) * 100 AS failure_pct,
    CASE
        WHEN SUM(CASE WHEN ${column_name} IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS result
FROM minio-datalake.${target_layer}.${target_table}
```

### 5.2 Uniqueness Check

```sql
-- Template ID: DQ_UNIQUE
-- Rule type: unique

SELECT
    '${dq_rule_id}' AS rule_id,
    '${pipeline_id}' AS pipeline_id,
    '${column_name}' AS column_name,
    'unique' AS rule_type,
    COUNT(*) AS total_rows,
    COUNT(*) - COUNT(DISTINCT ${column_name}) AS duplicate_count,
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT ${column_name}) THEN 'PASS'
        ELSE 'FAIL'
    END AS result
FROM minio-datalake.${target_layer}.${target_table}
```

### 5.3 Range Check

```sql
-- Template ID: DQ_RANGE
-- Rule type: range

SELECT
    '${dq_rule_id}' AS rule_id,
    '${pipeline_id}' AS pipeline_id,
    '${column_name}' AS column_name,
    'range' AS rule_type,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN NOT (${rule_expression}) THEN 1 ELSE 0 END) AS failed_rows,
    CAST(SUM(CASE WHEN NOT (${rule_expression}) THEN 1 ELSE 0 END) AS DOUBLE)
        / NULLIF(COUNT(*), 0) * 100 AS failure_pct,
    CASE
        WHEN CAST(SUM(CASE WHEN NOT (${rule_expression}) THEN 1 ELSE 0 END) AS DOUBLE)
            / NULLIF(COUNT(*), 0) * 100 <= ${threshold_pct} THEN 'PASS'
        ELSE 'FAIL'
    END AS result
FROM minio-datalake.${target_layer}.${target_table}
```

### 5.4 Freshness Check

```sql
-- Template ID: DQ_FRESHNESS
-- Rule type: freshness

SELECT
    '${dq_rule_id}' AS rule_id,
    '${pipeline_id}' AS pipeline_id,
    '${column_name}' AS column_name,
    'freshness' AS rule_type,
    MAX(${column_name}) AS latest_value,
    CURRENT_TIMESTAMP AS check_time,
    CASE
        WHEN ${rule_expression} THEN 'PASS'
        ELSE 'FAIL'
    END AS result
FROM minio-datalake.${target_layer}.${target_table}
```

### 5.5 DQ Summary Query

Tổng hợp tất cả DQ results cho 1 pipeline run:

```sql
-- Chạy sau tất cả DQ checks
-- Aggregate kết quả từ execution_log

SELECT
    pipeline_id,
    pipeline_name,
    layer,
    COUNT(*) AS total_checks,
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed,
    CASE
        WHEN SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) = 0 THEN 'ALL_PASSED'
        ELSE 'HAS_FAILURES'
    END AS overall_status
FROM minio-datalake.metadata.pipeline_execution_log
WHERE layer = 'dq_check'
  AND CAST(start_time AS DATE) = CURRENT_DATE
GROUP BY pipeline_id, pipeline_name, layer
```

---

## 6. Utility Templates

### 6.1 Get Last Watermark

```sql
-- Dùng bởi NiFi trước khi chạy incremental load

SELECT COALESCE(
    MAX(last_watermark),
    '1970-01-01 00:00:00'
) AS last_watermark
FROM minio-datalake.metadata.pipeline_execution_log
WHERE pipeline_id = '${pipeline_id}'
  AND layer = 'bronze'
  AND status = 'success'
```

### 6.2 Insert Execution Log

```sql
-- Dùng bởi NiFi sau mỗi lần chạy pipeline

INSERT INTO minio-datalake.metadata.pipeline_execution_log
VALUES (
    '${pipeline_id}_${now():format("yyyyMMdd_HHmmss")}',
    '${pipeline_id}',
    '${pipeline_name}',
    '${layer}',
    CAST('${start_time}' AS TIMESTAMP),
    CURRENT_TIMESTAMP,
    '${status}',
    ${rows_processed},
    ${rows_inserted},
    ${rows_updated},
    ${rows_rejected},
    '${current_watermark}',
    ${error_message},
    '${load_type}',
    CURRENT_TIMESTAMP
)
```

### 6.3 Build Column List from Mapping

NiFi sẽ query column_mapping để build dynamic SELECT clause:

```sql
-- Query column_mapping cho 1 pipeline
SELECT
    source_column,
    target_column,
    data_type,
    transformation,
    column_order
FROM minio-datalake.metadata.column_mapping
WHERE pipeline_id = '${pipeline_id}'
ORDER BY column_order
```

NiFi xử lý kết quả để build SELECT clause:

```
-- Nếu transformation IS NULL:
source_column AS target_column

-- Nếu transformation IS NOT NULL:
REPLACE(transformation, '${src}', source_column) AS target_column

-- Ví dụ kết quả:
-- txn_id AS txn_id,
-- CAST(amount AS DECIMAL(18,2)) AS transaction_amount,
-- UPPER(TRIM(currency)) AS currency_code,
-- merchant_id AS merchant_id,
-- LPAD(bank_code, 9, '0') AS bank_code
```

---

## 7. Composite Primary Key Handling

Khi `primary_keys` chứa nhiều cột (comma-separated), cần generate ON clause.

### 7.1 MERGE ON clause generation

Input: `primary_keys = 'txn_id,merchant_id'`

Output:
```sql
ON target.txn_id = source.txn_id AND target.merchant_id = source.merchant_id
```

**NiFi approach — dùng ReplaceText + Expression Language:**

```
-- Step 1: Set attribute
merge_on_clause = ${primary_keys:replaceAll('([^,]+)', 'target.$1 = source.$1'):replaceAll(',', ' AND ')}

-- Result: target.txn_id = source.txn_id AND target.merchant_id = source.merchant_id
```

### 7.2 PARTITION BY clause generation

Input: `primary_keys = 'txn_id,merchant_id'`

Output:
```sql
PARTITION BY txn_id, merchant_id
```

→ Dùng trực tiếp, không cần transform.

---

## 8. Template Quick Reference

| Template ID                | Type         | Layer         | Mục đích                        |
|----------------------------|--------------|---------------|----------------------------------|
| `BRONZE_FULL_LOAD`         | full         | bronze        | Full load từ source              |
| `BRONZE_INCREMENTAL_LOAD`  | incremental  | bronze        | Incremental load bằng watermark  |
| `BRONZE_PARTITION_OVERWRITE`| full        | bronze        | Overwrite 1 partition            |
| `SILVER_CREATE_TABLE`      | create_table | silver        | Tạo silver table lần đầu        |
| `SILVER_DEDUP`             | dedup        | silver        | Loại duplicate                   |
| `SILVER_MERGE`             | merge        | silver        | Incremental upsert               |
| `SILVER_MERGE_TRANSFORM`   | merge        | silver        | Upsert + column transforms       |
| `SILVER_FILTER`            | filter       | silver        | Lọc invalid records              |
| `GOLD_DAILY_SUMMARY`       | aggregate    | gold          | KPI hàng ngày                    |
| `GOLD_ENTITY_KPIS`         | aggregate    | gold          | KPI theo entity                  |
| `GOLD_MERCHANT_ANALYTICS`  | aggregate    | gold          | Merchant analytics               |
| `GOLD_SETTLEMENT_RECON`    | aggregate    | gold          | Settlement reconciliation        |
| `GOLD_HOURLY_TREND`        | aggregate    | gold          | Hourly trend analysis            |
| `DQ_NOT_NULL`              | not_null     | dq            | Null check                       |
| `DQ_UNIQUE`                | unique       | dq            | Uniqueness check                 |
| `DQ_RANGE`                 | range        | dq            | Range validation                 |
| `DQ_FRESHNESS`             | freshness    | dq            | Data freshness check             |
