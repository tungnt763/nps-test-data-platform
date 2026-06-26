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
├── pipeline_config          ← Bảng chính: source, target, load type, depends_on
├── column_mapping           ← Column-level: tên cột, kiểu dữ liệu, transform
├── transform_templates      ← Thư viện SQL GENERIC reusable (load/dedup/merge...) — dùng chung mọi bảng
├── transform_rules          ← CHỈ bước custom (gold aggregation, filter đặc biệt)
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
mc mb napas/napas-datalake/staging  --ignore-existing   # bảng temp giữa bronze→silver
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
CREATE TABLE "minio-datalake"."metadata".pipeline_config (
    pipeline_id         VARCHAR,
    pipeline_name       VARCHAR,
    dataset             VARCHAR,
    source_type         VARCHAR,
    source_connection   VARCHAR,
    source_schema       VARCHAR,
    source_table        VARCHAR,
    source_layer        VARCHAR,
    target_layer        VARCHAR,
    target_path         VARCHAR,
    target_table        VARCHAR,
    load_type           VARCHAR,
    primary_keys        VARCHAR,
    watermark_column    VARCHAR,
    partition_columns   VARCHAR,
    depends_on          VARCHAR,
    batch_size          INT,
    schedule_cron       VARCHAR,
    is_active           BOOLEAN,
    description         VARCHAR,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
);
```

> **QUAN TRỌNG — Mô hình "1 stage = 1 config row":** Mỗi bước chuyển tầng
> (`source→bronze`, `bronze→silver`, `silver→gold`) là **một dòng riêng** với
> `pipeline_id` riêng (vd `BRZ_transactions`, `SLV_transactions`, `GLD_daily_txn`).
> `load_type`/`primary_keys`/`watermark_column`/`partition_columns` trên dòng `SLV_*`
> mô tả cách **silver đọc bronze** — độc lập hoàn toàn với cách bronze đọc source.
> Quan hệ phụ thuộc khai báo qua `depends_on`. Xem [14-pipeline-dependency-orchestration.md](14-pipeline-dependency-orchestration.md).

**Giải thích từng field:**

| Field              | Kiểu     | Mô tả                                                         | Ví dụ                                              |
|--------------------|----------|----------------------------------------------------------------|-----------------------------------------------------|
| `pipeline_id`      | VARCHAR  | ID duy nhất cho **stage** (mỗi layer-transition 1 id riêng)    | `BRZ_transactions`, `SLV_transactions`             |
| `pipeline_name`    | VARCHAR  | Tên mô tả                                                     | `ingest_transactions`                               |
| `dataset`          | VARCHAR  | Nhóm logic các stage của cùng thực thể                         | `transactions`                                      |
| `source_type`      | VARCHAR  | Loại nguồn dữ liệu                                           | `jdbc`, `api`, `sftp`, `file`                       |
| `source_connection`| VARCHAR  | Tên Controller Service JDBC trong NiFi (hoặc URL)             | `source-postgres-pool`                              |
| `source_schema`    | VARCHAR  | Schema của source DB (chỉ áp dụng bronze)                     | `public`                                            |
| `source_table`     | VARCHAR  | Bảng nguồn (silver/gold: tên bảng ở tầng `source_layer`)      | `transactions`                                      |
| `source_layer`     | VARCHAR  | Tầng nguồn của stage này                                       | `source` (bronze), `bronze` (silver), `silver` (gold) |
| `target_layer`     | VARCHAR  | Layer đích                                                     | `bronze`, `silver`, `gold`                          |
| `target_path`      | VARCHAR  | S3 path pattern (NiFi Expression Language)                     | `bronze/${source_table}/dt=${date}`                  |
| `target_table`     | VARCHAR  | Tên bảng đích trong Dremio                                    | `transactions`                                      |
| `load_type`        | VARCHAR  | Kiểu load                                                     | `full`, `incremental`                               |
| `primary_keys`     | VARCHAR  | Khóa chính (comma-separated)                                  | `txn_id` hoặc `txn_id,merchant_id`                  |
| `watermark_column` | VARCHAR  | Cột dùng cho incremental load (NULL nếu full load)            | `updated_at`                                        |
| `partition_columns`| VARCHAR  | Cột partition (comma-separated, NULL nếu không partition)      | `transaction_date`                                  |
| `depends_on`       | VARCHAR  | `pipeline_id` upstream (comma-separated). `NULL` = root/ingestion. Định nghĩa cạnh DAG | `BRZ_transactions` hoặc `SLV_txn,SLV_merchants` |
| `batch_size`       | INT      | Số rows mỗi batch (NiFi fetch size)                           | `10000`                                             |
| `schedule_cron`    | VARCHAR  | Lịch chạy                                                     | `0 2 * * *` (2h sáng mỗi ngày)                     |
| `is_active`        | BOOLEAN  | Pipeline có đang active không                                  | `true`, `false`                                     |
| `description`      | VARCHAR  | Mô tả pipeline                                                | `Ingest daily transactions from core banking`       |
| `created_at`       | TIMESTAMP| Thời gian tạo                                                 | `2026-06-24 10:00:00`                               |
| `updated_at`       | TIMESTAMP| Thời gian cập nhật cuối                                       | `2026-06-24 10:00:00`                               |

---

### 3.2 column_mapping — Mapping cột source → target

```sql
CREATE TABLE "minio-datalake"."metadata".column_mapping (
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
| `pipeline_id`    | FK → pipeline_config (**stage sở hữu transform**, thường là silver) | `SLV_transactions`               |
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

> **Khi nào cần transform_rules?** Luồng silver chuẩn (load→dedup→publish) **KHÔNG cần** rule riêng
> cho từng bảng — NiFi áp **thư viện template generic** trong `transform_templates` (§3.6) dựa trên
> `pipeline_config.load_type` + `column_mapping`. `transform_rules` chỉ dùng cho bước **custom**
> (aggregation gold, filter đặc biệt), trỏ tới 1 template generic qua `template_id` **hoặc** chứa
> `sql_template` riêng.

```sql
CREATE TABLE "minio-datalake"."metadata".transform_rules (
    rule_id             VARCHAR,
    pipeline_id         VARCHAR,
    rule_name           VARCHAR,
    source_layer        VARCHAR,
    target_layer        VARCHAR,
    transform_type      VARCHAR,
    template_id         VARCHAR,    -- FK → transform_templates (generic). NULL nếu dùng sql_template riêng
    sql_template        VARCHAR,    -- chỉ dùng cho custom; NULL nếu dùng template_id
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
| `pipeline_id`     | FK → pipeline_config (**stage sở hữu rule**: silver/gold)    | `SLV_transactions`, `GLD_daily_txn`      |
| `rule_name`       | Tên rule                                                     | `daily_txn_summary`                      |
| `source_layer`    | Layer nguồn                                                  | `bronze`, `silver`                       |
| `target_layer`    | Layer đích                                                   | `silver`, `gold`                         |
| `transform_type`  | Loại transform                                               | `load_stage`, `dedup`, `merge`, `aggregate`, `custom` |
| `template_id`     | FK → `transform_templates` (dùng SQL generic, reusable)      | `T_MERGE`                                |
| `sql_template`    | SQL custom riêng (chỉ khi không dùng template_id)            | aggregation gold                         |
| `depends_on`      | Rule IDs phải chạy trước (comma-separated, NULL nếu không)  | `T001,T002`                              |
| `execution_order` | Thứ tự chạy (nhỏ chạy trước)                                | `1`, `2`, `3`                            |
| `is_active`       | Rule có active không                                         | `true`                                   |

**`transform_type` giải thích:**

| Type           | Mô tả                                  | Khi nào dùng                            |
|----------------|------------------------------------------|-----------------------------------------|
| `load_stage`   | CREATE OR REPLACE staging từ bronze (full/incr + cast/clean) | Bước 1 của silver: nạp vào bảng temp |
| `create_table` | CREATE TABLE IF NOT EXISTS               | Ensure silver/gold table tồn tại        |
| `dedup`        | Loại bỏ duplicate rows (trên staging)    | Bước 2 của silver: staging có thể trùng |
| `merge`        | MERGE INTO (upsert) từ staging           | Bước cuối: staging → silver/gold        |
| `aggregate`    | GROUP BY, SUM, COUNT                     | Gold: business aggregations             |
| `filter`       | WHERE clause lọc dữ liệu               | Silver: lọc bỏ invalid records          |
| `custom`       | SQL tùy chỉnh                           | Bất kỳ logic đặc biệt nào              |

---

### 3.4 data_quality_rules — Quy tắc kiểm tra chất lượng

```sql
CREATE TABLE "minio-datalake"."metadata".data_quality_rules (
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
| `pipeline_id`      | VARCHAR  | FK → pipeline_config (**stage sở hữu**, thường silver)         | `SLV_transactions`                                  |
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
CREATE TABLE "minio-datalake"."metadata".pipeline_execution_log (
    execution_id        VARCHAR,
    run_id              VARCHAR,
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
| `execution_id`     | VARCHAR   | ID duy nhất cho mỗi lần chạy (mỗi stage)                       | `SLV_transactions_20260625_021800`                 |
| `run_id`           | VARCHAR   | Khóa tương quan **xuyên suốt 1 lần trigger** (chung cho cả bronze→silver→gold). Dùng cho lineage, guard, Wait/Notify | `RUN_20260625_020000_a1b2` |
| `pipeline_id`      | VARCHAR   | FK → pipeline_config, **stage** nào đã chạy                    | `BRZ_transactions`, `SLV_transactions`             |
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

### 3.6 transform_templates — Thư viện template GENERIC (reusable)

Lưu **một lần** các SQL template generic (chỉ chứa `${...}`, không hardcode tên bảng/cột). Mọi
pipeline dùng chung → không lặp lại SQL cho từng bảng. Chi tiết SQL: [12-dynamic-sql-templates.md](12-dynamic-sql-templates.md) §3.

```sql
CREATE TABLE "minio-datalake"."metadata".transform_templates (
    template_id     VARCHAR,    -- T_LOAD_STAGE, T_DEDUP, T_ENSURE_TARGET, T_MERGE, T_FULL_REPLACE, T_CLEANUP
    transform_type  VARCHAR,    -- load_stage | dedup | create_table | merge | custom
    sql_template    VARCHAR,    -- SQL generic, chỉ dùng biến ${...}
    description     VARCHAR
);
```

**Seed thư viện (dùng cho TẤT CẢ bảng):**

```sql
-- INSERT bỏ qua cột thời gian (không có ở bảng này); mỗi template chỉ chứa biến generic
INSERT INTO "minio-datalake"."metadata".transform_templates
(template_id, transform_type, sql_template, description)
VALUES
('T_LOAD_STAGE','load_stage',
 'CREATE TABLE ${stage_out} AS SELECT ${column_list_select} FROM ${source_fqn} ${where_clause}',
 'Source to temp (full or incremental, with cast and clean)'),
('T_DEDUP','dedup',
 'CREATE TABLE ${stage_out} AS SELECT ${column_list} FROM (SELECT ${column_list}, ROW_NUMBER() OVER (PARTITION BY ${primary_keys} ORDER BY ${order_by_clause}) AS _rn FROM ${stage_in}) WHERE _rn = 1',
 'Dedup temp into a new temp'),
('T_ENSURE_TARGET','create_table',
 'CREATE TABLE IF NOT EXISTS ${target_fqn} AS SELECT ${column_list} FROM ${stage_in} WHERE 1=0',
 'Create target table if not exists'),
('T_MERGE','merge',
 'MERGE INTO ${target_fqn} AS t USING ${stage_in} AS s ON ${merge_on_clause} WHEN MATCHED THEN UPDATE SET ${update_set_clause} WHEN NOT MATCHED THEN INSERT (${column_list}) VALUES (${insert_values_list})',
 'Upsert temp into main table (incremental)'),
('T_FULL_REPLACE','merge',
 'CREATE OR REPLACE TABLE ${target_fqn} AS SELECT ${column_list} FROM ${stage_in}',
 'Replace whole main table (full load)'),
('T_CLEANUP','custom',
 'DROP TABLE IF EXISTS ${stage_drop}',
 'Drop one temp (repeat for each temp of the run)');
```

> **Lưu ý reusability:** không có dòng nào nhắc tên bảng/cột cụ thể. Onboard bảng mới = thêm
> `pipeline_config` + `column_mapping`; thư viện này **không đổi**.

---

## 4. Sample Data — Demo Pipeline

### 4.1 Pipeline Config cho demo (mô hình stage-tách)

> **Mỗi layer-transition = 1 dòng** với `pipeline_id` riêng + `depends_on`. Root (bronze ingestion)
> có `depends_on = NULL`. Dùng **explicit column list** (BẮT BUỘC) — không INSERT theo vị trí.
>
> ⚠️ **Không đặt `CURRENT_TIMESTAMP` trong VALUES.** Dremio ném `IllegalStateException: Duplicate key
> CURRENT_TIMESTAMP` khi hàm này lặp nhiều lần trong cùng một câu `INSERT ... VALUES`. Cách đúng:
> **INSERT bỏ qua `created_at`/`updated_at`** (để NULL), rồi set bằng một câu **`UPDATE`** riêng.

```sql
-- BƯỚC 1: INSERT (bỏ created_at, updated_at — sẽ là NULL)
INSERT INTO "minio-datalake"."metadata".pipeline_config
(pipeline_id, pipeline_name, dataset, source_type, source_connection, source_schema,
 source_table, source_layer, target_layer, target_table, load_type, primary_keys,
 watermark_column, partition_columns, depends_on, batch_size, schedule_cron,
 is_active, description)
VALUES
-- ===== ROOT STAGES: source → bronze (depends_on = NULL, được Controller trigger) =====
('BRZ_transactions','bronze_transactions','transactions','jdbc','source-postgres-pool','public',
 'transactions','source','bronze','transactions','incremental','txn_id','created_at','transaction_date',
 NULL, 10000,'0 2 * * *', true,'Incremental ingest transactions from source'),

('BRZ_merchants','bronze_merchants','merchants','jdbc','source-postgres-pool','public',
 'merchants','source','bronze','merchants','full','merchant_id',NULL,NULL,
 NULL, 5000,'0 2 * * *', true,'Full load merchant master'),

('BRZ_bank_codes','bronze_bank_codes','bank_codes','jdbc','source-postgres-pool','public',
 'bank_codes','source','bronze','bank_codes','full','bank_code',NULL,NULL,
 NULL, 1000,'0 2 * * 1', true,'Weekly full load bank reference'),

('BRZ_settlements','bronze_settlements','settlements','jdbc','source-postgres-pool','public',
 'settlements','source','bronze','settlements','incremental','settlement_id','settlement_date','settlement_date',
 NULL, 10000,'0 2 * * *', true,'Incremental ingest settlements'),

-- ===== SILVER STAGES: bronze → silver (config ĐỘC LẬP với bronze; depends_on = BRZ_*) =====
('SLV_transactions','silver_transactions','transactions','internal',NULL,NULL,
 'transactions','bronze','silver','transactions','incremental','txn_id','created_at',NULL,
 'BRZ_transactions', NULL,NULL, true,'Dedup and merge transactions bronze to silver'),

('SLV_merchants','silver_merchants','merchants','internal',NULL,NULL,
 'merchants','bronze','silver','merchants','full','merchant_id',NULL,NULL,
 'BRZ_merchants', NULL,NULL, true,'Dedup merchants bronze to silver'),

('SLV_bank_codes','silver_bank_codes','bank_codes','internal',NULL,NULL,
 'bank_codes','bronze','silver','bank_codes','full','bank_code',NULL,NULL,
 'BRZ_bank_codes', NULL,NULL, true,'Dedup bank_codes bronze to silver'),

('SLV_settlements','silver_settlements','settlements','internal',NULL,NULL,
 'settlements','bronze','silver','settlements','incremental','settlement_id','settlement_date',NULL,
 'BRZ_settlements', NULL,NULL, true,'Merge settlements bronze to silver'),

-- ===== GOLD STAGES: silver → gold (depends_on = SLV_*; GLD_bank_kpis là fan-in 2 parent) =====
('GLD_daily_txn','gold_daily_txn_summary','transactions','internal',NULL,NULL,
 'transactions','silver','gold','daily_transaction_summary','full',NULL,NULL,NULL,
 'SLV_transactions', NULL,NULL, true,'Daily aggregated KPIs'),

('GLD_bank_kpis','gold_bank_performance','transactions','internal',NULL,NULL,
 'transactions','silver','gold','bank_performance_kpis','full',NULL,NULL,NULL,
 'SLV_transactions,SLV_merchants',   -- FAN-IN: 2 parent → executor dùng Wait/Notify (Doc 11 §10)
 NULL,NULL, true,'Bank KPIs join transactions + merchants');

-- BƯỚC 2: set timestamp thật bằng CURRENT_TIMESTAMP (ngoài VALUES → không lỗi)
UPDATE "minio-datalake"."metadata".pipeline_config
SET created_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
WHERE created_at IS NULL;
```

> **DAG sinh ra từ `depends_on`:**
> ```
> BRZ_transactions → SLV_transactions ─┬→ GLD_daily_txn
> BRZ_merchants    → SLV_merchants ────┴→ GLD_bank_kpis   (⟵ 2 parent)
> BRZ_bank_codes   → SLV_bank_codes
> BRZ_settlements  → SLV_settlements
> ```

### 4.2 Column Mapping cho transactions

> `pipeline_id` của column_mapping **trỏ tới stage sở hữu transform** — các phép CAST/TRIM/UPPER
> này áp dụng khi bronze→silver, nên thuộc `SLV_transactions` (không phải bronze).

```sql
-- Transactions column mappings (thuộc stage SILVER)
INSERT INTO "minio-datalake"."metadata".column_mapping
(mapping_id, pipeline_id, source_column, target_column, data_type, transformation,
 is_primary_key, is_nullable, default_value, column_order, description)
VALUES
('M001','SLV_transactions','txn_id',           'txn_id',              'VARCHAR',        NULL,                                    true,  false, NULL, 1, 'Primary key'),
('M002','SLV_transactions','amount',           'transaction_amount',  'DECIMAL(18,2)',  'CAST(${src} AS DECIMAL(18,2))',         false, false, NULL, 2, 'Transaction amount'),
('M003','SLV_transactions','currency',         'currency_code',       'VARCHAR',        'UPPER(TRIM(${src}))',                   false, false, '''VND''', 3, 'ISO currency code'),
('M004','SLV_transactions','merchant_id',      'merchant_id',         'VARCHAR',        NULL,                                    false, true,  NULL, 4, 'FK to merchants'),
('M005','SLV_transactions','bank_code',        'bank_code',           'VARCHAR',        'LPAD(${src}, 9, ''0'')',                false, false, NULL, 5, 'NAPAS bank code (9 digits)'),
('M006','SLV_transactions','status_code',      'status_code',         'VARCHAR',        NULL,                                    false, false, NULL, 6, 'Transaction status'),
('M007','SLV_transactions','transaction_type', 'transaction_type',    'VARCHAR',        'UPPER(${src})',                         false, false, NULL, 7, 'Type: PURCHASE, TRANSFER, etc'),
('M008','SLV_transactions','created_at',       'created_at',          'TIMESTAMP',      'CAST(${src} AS TIMESTAMP)',             false, false, NULL, 8, 'Record creation time'),
('M009','SLV_transactions','updated_at',       'updated_at',          'TIMESTAMP',      'CAST(${src} AS TIMESTAMP)',             false, true,  NULL, 9, 'Last update time');
```

### 4.3 Transform Rules — CHỈ cho bước custom (gold). Silver chuẩn KHÔNG cần rule

> **Silver chuẩn (load→dedup→publish) KHÔNG cần `transform_rules`.** NiFi tự dựng chuỗi từ thư viện
> generic `transform_templates` (§3.6) theo `pipeline_config.load_type` + `column_mapping`
> (xem [12-dynamic-sql-templates.md](12-dynamic-sql-templates.md) §3.7). Vì vậy `SLV_transactions`,
> `SLV_merchants`, ... **không có** dòng nào ở đây.
>
> `transform_rules` chỉ chứa bước **custom**: aggregation gold (dùng `sql_template` riêng) hoặc filter
> đặc biệt. Có thể trỏ `template_id` tới template generic nếu tái dùng được. `depends_on` = thứ tự
> nội bộ stage; phụ thuộc giữa stage nằm ở `pipeline_config.depends_on`.

```sql
-- transform_rules CHỈ gồm gold custom aggregation (template_id = NULL → dùng sql_template riêng).
-- Gold: Daily transaction summary (stage GLD_daily_txn)
INSERT INTO "minio-datalake"."metadata".transform_rules
(rule_id, pipeline_id, rule_name, source_layer, target_layer, transform_type,
 sql_template, depends_on, execution_order, is_active, description)
VALUES (
    'T005', 'GLD_daily_txn', 'daily_txn_summary',
    'silver', 'gold', 'aggregate',
    'CREATE OR REPLACE VIEW "minio-datalake"."gold".daily_transaction_summary AS
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
     FROM "minio-datalake"."silver".transactions
     GROUP BY
       CAST(created_at AS DATE),
       bank_code,
       transaction_type,
       status_code',
    NULL, 1, true,
    'Daily aggregated KPIs by bank, type, status'
);

-- Gold: Bank performance KPIs (stage GLD_bank_kpis — fan-in 2 parent)
INSERT INTO "minio-datalake"."metadata".transform_rules
(rule_id, pipeline_id, rule_name, source_layer, target_layer, transform_type,
 sql_template, depends_on, execution_order, is_active, description)
VALUES (
    'T006', 'GLD_bank_kpis', 'bank_performance_kpis',
    'silver', 'gold', 'aggregate',
    'CREATE OR REPLACE VIEW "minio-datalake"."gold".bank_performance_kpis AS
     SELECT
       bank_code,
       COUNT(*) AS total_transactions,
       SUM(CASE WHEN status_code = ''00'' THEN 1 ELSE 0 END) AS successful_transactions,
       SUM(CASE WHEN status_code != ''00'' THEN 1 ELSE 0 END) AS failed_transactions,
       CAST(SUM(CASE WHEN status_code = ''00'' THEN 1 ELSE 0 END) AS DOUBLE) /
         NULLIF(COUNT(*), 0) * 100 AS success_rate_pct,
       SUM(transaction_amount) AS total_volume,
       AVG(transaction_amount) AS avg_transaction_value
     FROM "minio-datalake"."silver".transactions
     GROUP BY bank_code',
    NULL, 1, true,
    'Bank-level performance metrics'
);
```

### 4.4 Data Quality Rules

> DQ rules thuộc stage chạy ra layer cần check → `pipeline_id = 'SLV_transactions'`.

```sql
INSERT INTO "minio-datalake"."metadata".data_quality_rules
(dq_rule_id, pipeline_id, target_layer, target_table, column_name, rule_type,
 rule_expression, severity, threshold_pct, is_active, description)
VALUES
-- txn_id not null
('DQ001','SLV_transactions','silver','transactions','txn_id',
 'not_null','${column} IS NOT NULL','critical',0.0,true,
 'Transaction ID must never be null'),
-- amount positive (allow 1% tolerance)
('DQ002','SLV_transactions','silver','transactions','transaction_amount',
 'range','${column} >= 0','error',1.0,true,
 'Transaction amount must be non-negative'),
-- bank_code format (9 digits)
('DQ003','SLV_transactions','silver','transactions','bank_code',
 'regex','LENGTH(${column}) = 9','warning',5.0,true,
 'Bank code should be 9 digits (NAPAS format)'),
-- freshness
('DQ004','SLV_transactions','silver','transactions','created_at',
 'freshness','MAX(${column}) >= CURRENT_TIMESTAMP - INTERVAL ''2'' DAY','warning',0.0,true,
 'Data should not be more than 2 days old'),
-- unique txn_id
('DQ005','SLV_transactions','silver','transactions','txn_id',
 'unique','COUNT(*) = COUNT(DISTINCT ${column})','error',0.0,true,
 'Transaction IDs must be unique in silver layer');
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
-- Kiểm tra pipeline config + DAG (depends_on)
SELECT pipeline_id, dataset, source_layer, target_layer, load_type, depends_on, is_active
FROM "minio-datalake"."metadata".pipeline_config
ORDER BY dataset, target_layer;

-- Kiểm tra column mappings của stage silver
SELECT source_column, target_column, data_type, transformation
FROM "minio-datalake"."metadata".column_mapping
WHERE pipeline_id = 'SLV_transactions'
ORDER BY column_order;

-- Kiểm tra transform rules theo stage
SELECT pipeline_id, rule_name, source_layer, target_layer, transform_type, execution_order
FROM "minio-datalake"."metadata".transform_rules
WHERE pipeline_id IN ('SLV_transactions', 'GLD_daily_txn', 'GLD_bank_kpis')
ORDER BY pipeline_id, execution_order;

-- Kiểm tra DQ rules của stage silver
SELECT column_name, rule_type, severity
FROM "minio-datalake"."metadata".data_quality_rules
WHERE pipeline_id = 'SLV_transactions';

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
