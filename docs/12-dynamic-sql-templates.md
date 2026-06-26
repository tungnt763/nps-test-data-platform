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

### 1.4 Biến generic cho transform (NiFi build từ metadata — KHÔNG hardcode)

Đây là nhóm biến giúp template **reusable cho mọi bảng** (không nhúng tên bảng/cột cứng):

| Variable                | NiFi build từ | Ví dụ giá trị (đã resolve) |
|-------------------------|---------------|-----------------------------|
| `${run_token}`          | `run_id` (8 hex) | `a1b2c3d4`               |
| `${source_fqn}`         | layer nguồn + source_table | `"minio-datalake"."bronze"."transactions"` |
| `${target_fqn}`         | target_layer + target_table | `"minio-datalake"."silver"."transactions"` |
| `${stage_in}`           | temp của step trước (hoặc source ở step 1) | `"minio-datalake"."staging"."transactions_temp_a1b2c3d4_1"` |
| `${stage_out}`          | temp của step hiện tại | `"minio-datalake"."staging"."transactions_temp_a1b2c3d4_2"` |
| `${column_list}`        | column_mapping → tên cột đích | `txn_id, transaction_amount, currency_code, ...` |
| `${column_list_select}` | column_mapping → biểu thức cast/clean | `txn_id, CAST(amount AS DECIMAL(18,2)) AS transaction_amount, ...` |
| `${update_set_clause}`  | column_mapping (cột non-PK) | `transaction_amount = s.transaction_amount, ...` |
| `${insert_values_list}` | column_mapping → `s.<col>` | `s.txn_id, s.transaction_amount, ...` |
| `${where_clause}`       | load_type | `''` (full) hoặc `WHERE created_at > TIMESTAMP '...'` (incremental) |
| `${order_by_clause}`    | watermark_column hoặc PK | `created_at DESC` (hoặc `txn_id`) |
| `${merge_on_clause}`    | primary_keys | `t.txn_id = s.txn_id` |

> **Nguyên tắc:** template chỉ chứa `${...}`, **không tên bảng/cột cứng**, **không `SELECT *`**.
> NiFi resolve tất cả từ `pipeline_config` + `column_mapping` lúc runtime (Doc 12 §6).

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

## 3. Silver Layer — Transform Templates (Generic + Per-Step Temp)

Silver transforms chạy trên **Dremio**, orchestrate bởi NiFi. Thiết kế tối ưu cho **reusable**:

- **Template generic, KHÔNG hardcode:** chỉ dùng `${...}` (Doc 12 §1.4); **cùng 1 template chạy cho
  mọi bảng**. Tên bảng/cột do NiFi build từ `pipeline_config` + `column_mapping` (Doc 12 §6).
- **KHÔNG `SELECT *`:** mọi bước liệt kê cột tường minh qua `${column_list}` / `${column_list_select}`.
- **Mỗi step ghi ra MỘT temp table BẤT BIẾN riêng** trong schema `staging`, tên
  `${target_table}_temp_${run_token}_${step}`. Step sau đọc temp của step trước (`${stage_in}`) và
  ghi temp mới (`${stage_out}`). Bảng chính chỉ bị chạm ở **bước publish cuối**. **Cuối cùng drop
  toàn bộ temp của run.**

> **Vì sao temp bất biến mỗi step** (thay vì `CREATE OR REPLACE` đè cùng 1 bảng): input mỗi bước bất
> biến ⇒ retry/giám sát từng bước chính xác, không race/không khoá bảng đang đọc, debug được dữ liệu
> tại **mọi** bước.

> **Chuẩn bị:** tạo schema staging một lần: `mc mb napas/napas-datalake/staging --ignore-existing`.

### 3.0 Luồng & quy ước temp

```
run_token = a1b2c3d4 (8 hex từ run_id)   ;   ${target_table} = transactions

(1) LOAD            bronze.transactions      ─▶ staging.transactions_temp_a1b2c3d4_1
(2) DEDUP           ..._temp_a1b2c3d4_1      ─▶ staging.transactions_temp_a1b2c3d4_2
(3) MERGE/REPLACE   ..._temp_a1b2c3d4_2      ─▶ silver.transactions   (chạm bảng chính DUY NHẤT)
(4) CLEANUP         DROP ..._temp_a1b2c3d4_1, ..._temp_a1b2c3d4_2
```

`${stage_in}` / `${stage_out}` = FQN temp tương ứng; NiFi tự tính từ `run_token` + số step.

### 3.1 T_LOAD_STAGE — nguồn → temp (generic; full & incremental)

Một template duy nhất cho mọi bảng. `${where_clause}` rỗng khi full, hoặc `WHERE ...` khi incremental.

```sql
-- Template ID: T_LOAD_STAGE   (reusable mọi bảng)
CREATE TABLE ${stage_out} AS
SELECT ${column_list_select}
FROM ${source_fqn}
${where_clause}
```

- `${source_fqn}` step đầu = bronze: `"minio-datalake"."bronze"."<source_table>"`.
- `${where_clause}` (incremental): `WHERE <watermark_column> > TIMESTAMP '${last_watermark}'`; (full): rỗng.
- `${column_list_select}`: build từ `column_mapping`, có cast/clean (Doc 12 §6.3) — **không `SELECT *`**.
- `${last_watermark}`: từ `pipeline_execution_log` theo `pipeline_id` silver + `layer='silver'` (Doc 11 §7.3).

### 3.2 T_DEDUP — temp → temp (generic)

```sql
-- Template ID: T_DEDUP
CREATE TABLE ${stage_out} AS
SELECT ${column_list}
FROM (
    SELECT ${column_list},
           ROW_NUMBER() OVER (PARTITION BY ${primary_keys} ORDER BY ${order_by_clause}) AS _rn
    FROM ${stage_in}
)
WHERE _rn = 1
```

- `${order_by_clause}`: `<watermark_column> DESC` nếu có watermark, ngược lại `${primary_keys}`.
- Liệt kê `${column_list}` tường minh ⇒ **không cần `SELECT *` / `EXCEPT`**.

### 3.3 T_ENSURE_TARGET — tạo bảng đích nếu chưa có (generic, chỉ incremental)

```sql
-- Template ID: T_ENSURE_TARGET
CREATE TABLE IF NOT EXISTS ${target_fqn} AS
SELECT ${column_list} FROM ${stage_in} WHERE 1=0
```

### 3.4 T_MERGE — temp cuối → bảng chính (generic; incremental upsert)

Explicit update/insert — **không `SET *` / `INSERT *`**.

```sql
-- Template ID: T_MERGE
MERGE INTO ${target_fqn} AS t
USING ${stage_in} AS s
ON ${merge_on_clause}
WHEN MATCHED THEN UPDATE SET ${update_set_clause}
WHEN NOT MATCHED THEN INSERT (${column_list}) VALUES (${insert_values_list})
```

- `${merge_on_clause}`: `t.txn_id = s.txn_id [AND ...]` (Doc 12 §7.1).
- `${update_set_clause}`: cột non-PK, vd `transaction_amount = s.transaction_amount, currency_code = s.currency_code, ...`.
- `${insert_values_list}`: `s.txn_id, s.transaction_amount, ...`.

### 3.5 T_FULL_REPLACE — temp cuối → bảng chính (generic; full-load)

Dùng **THAY** §3.3+§3.4 khi `load_type = full`. Tự loại bản ghi đã xóa ở nguồn.

```sql
-- Template ID: T_FULL_REPLACE
CREATE OR REPLACE TABLE ${target_fqn} AS
SELECT ${column_list} FROM ${stage_in}
```

### 3.6 T_CLEANUP — drop toàn bộ temp của run (chạy cuối cùng)

NiFi biết số step ⇒ sinh danh sách temp và drop từng cái:

```sql
-- Template ID: T_CLEANUP   (lặp cho từng temp đã tạo)
DROP TABLE IF EXISTS ${stage_drop}
```

- `${stage_drop}` lần lượt = `..._temp_${run_token}_1`, `..._temp_${run_token}_2`, ...
- Hoặc liệt kê động: `SELECT TABLE_NAME FROM INFORMATION_SCHEMA."TABLES"
  WHERE TABLE_NAME LIKE '${target_table}_temp_${run_token}_%'` rồi drop từng cái.

### 3.7 Standard flow = ZERO hardcode, ZERO per-table rule

Với luồng chuẩn **load → dedup → publish** (đa số bảng), bạn **không cần viết `transform_rules`
riêng cho từng bảng**. NiFi áp **chuỗi template chuẩn** từ thư viện generic, chọn theo
`pipeline_config.load_type`:

| load_type | Chuỗi template |
|-----------|----------------|
| `incremental` | `T_LOAD_STAGE` → `T_DEDUP` → `T_ENSURE_TARGET` → `T_MERGE` → `T_CLEANUP` |
| `full` | `T_LOAD_STAGE` → `T_DEDUP` → `T_FULL_REPLACE` → `T_CLEANUP` |

→ Onboard 1 bảng thường = INSERT `pipeline_config` + `column_mapping`. `transform_rules` **chỉ** dùng
cho bước **custom** (filter đặc biệt §3.8, aggregation gold §4). Thư viện template generic lưu 1 lần
trong bảng `transform_templates` (Doc 10 §3.6).

### 3.8 Filter (Remove Invalid Records)

**Khi dùng:** Lọc bỏ records không hợp lệ ở silver layer.

```sql
-- Template ID: SILVER_FILTER
-- Transform type: filter
-- Ví dụ: chỉ giữ transactions thành công

DELETE FROM "minio-datalake"."silver".${target_table}
WHERE status_code NOT IN ('00', '01')
```

Hoặc tạo filtered view:

```sql
-- Template ID: SILVER_FILTER_VIEW
-- Transform type: filter
-- Tạo view chỉ chứa valid records

CREATE OR REPLACE VIEW "minio-datalake"."silver".${target_table}_valid AS
SELECT *
FROM "minio-datalake"."silver".${target_table}
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

CREATE OR REPLACE VIEW "minio-datalake"."gold".daily_${target_table}_summary AS
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
FROM "minio-datalake"."silver".${source_table}
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

CREATE OR REPLACE VIEW "minio-datalake"."gold".bank_performance AS
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

FROM "minio-datalake"."silver".transactions t
LEFT JOIN "minio-datalake"."silver".bank_codes b
    ON t.bank_code = b.bank_code
GROUP BY t.bank_code, b.bank_name, b.bank_short_name
```

### 4.3 Merchant Analytics

```sql
-- Template ID: GOLD_MERCHANT_ANALYTICS
-- Transform type: aggregate

CREATE OR REPLACE VIEW "minio-datalake"."gold".merchant_analytics AS
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
FROM "minio-datalake"."silver".transactions t
LEFT JOIN "minio-datalake"."silver".merchants m
    ON t.merchant_id = m.merchant_id
GROUP BY t.merchant_id, m.merchant_name, m.category, m.city
```

### 4.4 Settlement Reconciliation

```sql
-- Template ID: GOLD_SETTLEMENT_RECON
-- Transform type: aggregate
-- So khớp giao dịch vs quyết toán

CREATE OR REPLACE VIEW "minio-datalake"."gold".settlement_reconciliation AS
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
FROM "minio-datalake"."silver".settlements s
LEFT JOIN (
    SELECT
        bank_code,
        CAST(created_at AS DATE) AS txn_date,
        COUNT(*) AS actual_count,
        SUM(transaction_amount) AS actual_amount
    FROM "minio-datalake"."silver".transactions
    WHERE status_code = '00'
    GROUP BY bank_code, CAST(created_at AS DATE)
) t ON s.bank_code = t.bank_code AND s.settlement_date = t.txn_date
LEFT JOIN "minio-datalake"."silver".bank_codes b ON s.bank_code = b.bank_code
```

### 4.5 Hourly Trend Analysis

```sql
-- Template ID: GOLD_HOURLY_TREND
-- Transform type: aggregate
-- Phân tích xu hướng theo giờ

CREATE OR REPLACE VIEW "minio-datalake"."gold".hourly_transaction_trend AS
SELECT
    CAST(created_at AS DATE) AS transaction_date,
    EXTRACT(HOUR FROM created_at) AS hour_of_day,
    COUNT(*) AS transaction_count,
    SUM(transaction_amount) AS total_amount,
    AVG(transaction_amount) AS avg_amount
FROM "minio-datalake"."silver".transactions
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
FROM "minio-datalake"."${target_layer}"."${target_table}"
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
FROM "minio-datalake"."${target_layer}"."${target_table}"
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
FROM "minio-datalake"."${target_layer}"."${target_table}"
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
FROM "minio-datalake"."${target_layer}"."${target_table}"
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
FROM "minio-datalake"."metadata".pipeline_execution_log
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
FROM "minio-datalake"."metadata".pipeline_execution_log
WHERE pipeline_id = '${pipeline_id}'
  AND layer = 'bronze'
  AND status = 'success'
```

### 6.2 Insert Execution Log

```sql
-- Dùng bởi NiFi sau mỗi lần chạy pipeline
-- Explicit column list (BẮT BUỘC) — không INSERT theo vị trí, tránh vỡ khi schema thêm cột

INSERT INTO "minio-datalake"."metadata".pipeline_execution_log
(execution_id, run_id, pipeline_id, pipeline_name, layer, start_time, end_time,
 status, rows_processed, rows_inserted, rows_updated, rows_rejected,
 last_watermark, error_message, execution_params, created_at)
VALUES (
    '${pipeline_id}_${now():format("yyyyMMdd_HHmmss")}',
    '${run_id}',
    '${pipeline_id}',
    '${pipeline_name}',
    '${layer}',
    CAST('${start_time}' AS TIMESTAMP),
    CAST('${now():format('yyyy-MM-dd HH:mm:ss')}' AS TIMESTAMP),
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

> **Chỉ dùng MỘT `CURRENT_TIMESTAMP` trong một câu INSERT.** Ở đây `start_time`/`end_time` lấy từ
> NiFi expression (`CAST('${...}' AS TIMESTAMP)`), chỉ `created_at` dùng `CURRENT_TIMESTAMP` →
> tránh lỗi Dremio `Duplicate key CURRENT_TIMESTAMP`. Quy tắc chung: nếu cần nhiều cột = thời gian
> hiện tại trong cùng INSERT, set 1 cột bằng `CURRENT_TIMESTAMP` và các cột còn lại bằng
> `CAST('${now():format(...)}' AS TIMESTAMP)`; hoặc INSERT trước rồi `UPDATE` sau.

### 6.3 Build các Column List từ column_mapping (thay cho SELECT *)

NiFi query `column_mapping` **một lần** rồi build **tất cả** các biến column-list cần cho template generic:

```sql
-- Query column_mapping cho 1 stage
SELECT source_column, target_column, data_type, transformation, is_primary_key, column_order
FROM "minio-datalake"."metadata".column_mapping
WHERE pipeline_id = '${pipeline_id}'
ORDER BY column_order
```

Từ kết quả, NiFi build các biến (ví dụ cho transactions):

| Biến | Quy tắc build | Kết quả |
|------|---------------|---------|
| `${column_list_select}` | `transformation` thay `${src}`→source_column, alias `AS target_column`; NULL → `source_column AS target_column` | `txn_id AS txn_id, CAST(amount AS DECIMAL(18,2)) AS transaction_amount, UPPER(TRIM(currency)) AS currency_code, ...` |
| `${column_list}` | chỉ `target_column` | `txn_id, transaction_amount, currency_code, ...` |
| `${insert_values_list}` | `s.` + target_column | `s.txn_id, s.transaction_amount, s.currency_code, ...` |
| `${update_set_clause}` | cột **non-PK**: `target_column = s.target_column` | `transaction_amount = s.transaction_amount, currency_code = s.currency_code, ...` |

> Dùng `${column_list_select}` ở **T_LOAD_STAGE** (có cast/clean), `${column_list}` ở **T_DEDUP /
> T_ENSURE_TARGET / T_FULL_REPLACE** (passthrough), `${update_set_clause}`+`${insert_values_list}` ở
> **T_MERGE**. Không nơi nào dùng `SELECT *`.
>
> **NiFi build thế nào:** ExecuteSQL đọc column_mapping → ConvertAvroToJSON → `ExecuteScript`
> (Groovy/Python) ghép chuỗi → đặt các biến trên thành FlowFile attribute.

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
| `T_LOAD_STAGE`             | load_stage   | →temp         | Generic: nguồn → temp (full/incr, cast/clean) |
| `T_DEDUP`                  | dedup        | temp→temp     | Generic: dedup, ghi temp mới     |
| `T_ENSURE_TARGET`          | create_table | target        | Generic: tạo bảng đích nếu chưa có |
| `T_MERGE`                  | merge        | temp→target   | Generic: upsert explicit (incremental) |
| `T_FULL_REPLACE`           | merge (full) | temp→target   | Generic: thay toàn bộ (full-load) |
| `T_CLEANUP`                | custom       | temp          | Generic: drop toàn bộ temp của run |
| `SILVER_FILTER`            | filter       | silver        | (custom) Lọc invalid records     |
| `GOLD_DAILY_SUMMARY`       | aggregate    | gold          | KPI hàng ngày                    |
| `GOLD_ENTITY_KPIS`         | aggregate    | gold          | KPI theo entity                  |
| `GOLD_MERCHANT_ANALYTICS`  | aggregate    | gold          | Merchant analytics               |
| `GOLD_SETTLEMENT_RECON`    | aggregate    | gold          | Settlement reconciliation        |
| `GOLD_HOURLY_TREND`        | aggregate    | gold          | Hourly trend analysis            |
| `DQ_NOT_NULL`              | not_null     | dq            | Null check                       |
| `DQ_UNIQUE`                | unique       | dq            | Uniqueness check                 |
| `DQ_RANGE`                 | range        | dq            | Range validation                 |
| `DQ_FRESHNESS`             | freshness    | dq            | Data freshness check             |
