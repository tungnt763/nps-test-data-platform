# Pipeline Operations Runbook

> **Phiên bản:** 1.0 | **Ngày:** 2026-06-24
> **Đối tượng:** Data Engineer vận hành metadata-driven pipeline
> **Tham chiếu:** [09-metadata-driven-strategy.md](09-metadata-driven-strategy.md) | [10-metadata-tables-design.md](10-metadata-tables-design.md) | [11-nifi-dynamic-pipeline-setup.md](11-nifi-dynamic-pipeline-setup.md)

---

## 1. Onboard Bảng Mới

### 1.1 Quy trình tổng quát

```
Bước 1: Phân tích source table
    ↓
Bước 2: INSERT pipeline_config
    ↓
Bước 3: INSERT column_mapping
    ↓
Bước 4: INSERT transform_rules (silver)
    ↓
Bước 5: INSERT transform_rules (gold) — nếu cần
    ↓
Bước 6: INSERT data_quality_rules
    ↓
Bước 7: Test pipeline (manual trigger)
    ↓
Bước 8: Enable schedule
```

**KHÔNG cần sửa NiFi flow.** Chỉ cần INSERT metadata.

### 1.2 Ví dụ: Thêm bảng `card_types`

**Bước 1 — Phân tích source:**

```sql
-- Kiểm tra bảng source
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'card_types'
ORDER BY ordinal_position;

-- Ước lượng size
SELECT COUNT(*) FROM card_types;

-- Xác định primary key
SELECT constraint_name, column_name
FROM information_schema.key_column_usage
WHERE table_name = 'card_types';
```

Kết quả phân tích:
- Bảng nhỏ (~50 rows) → **full load**
- Primary key: `card_type_code`
- Không cần watermark (full load)
- Reference data → load weekly

**Bước 2 — INSERT pipeline_config:**

```sql
-- INSERT explicit column list, bỏ created_at/updated_at (tránh lỗi Duplicate key CURRENT_TIMESTAMP)
INSERT INTO "minio-datalake"."metadata".pipeline_config
(pipeline_id, pipeline_name, dataset, source_type, source_connection, source_schema,
 source_table, source_layer, target_layer, target_path, target_table, load_type,
 primary_keys, watermark_column, partition_columns, depends_on, batch_size,
 schedule_cron, is_active, description)
VALUES (
    'BRZ_card_types',                 -- pipeline_id (bronze root stage)
    'bronze_card_types',              -- pipeline_name
    'card_types',                     -- dataset
    'jdbc',                           -- source_type
    'source-postgres-pool',           -- source_connection (NiFi CS name)
    'public',                         -- source_schema
    'card_types',                     -- source_table
    'source',                         -- source_layer (bronze đọc từ source DB)
    'bronze',                         -- target_layer
    'bronze/card_types/dt=${date}',   -- target_path
    'card_types',                     -- target_table
    'full',                           -- load_type
    'card_type_code',                 -- primary_keys
    NULL,                             -- watermark_column (NULL = full load)
    NULL,                             -- partition_columns
    NULL,                             -- depends_on (NULL = root → Controller trigger)
    1000,                             -- batch_size
    '0 2 * * 1',                      -- schedule_cron (tham khảo; Controller có 1 trigger chung)
    true,                             -- is_active
    'Weekly full load of card type reference data'
);

-- set timestamp thật (ngoài VALUES → không lỗi)
UPDATE "minio-datalake"."metadata".pipeline_config
SET created_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
WHERE created_at IS NULL;
```

> Onboard đầy đủ cho stage-tách: thêm dòng `SLV_card_types` (silver, `depends_on='BRZ_card_types'`)
> và trỏ `column_mapping`/`transform_rules`/`data_quality_rules` tới `SLV_card_types` — xem mẫu ở
> [10-metadata-tables-design.md](10-metadata-tables-design.md) §4.1. Ví dụ Bước 3–6 dưới đây giữ
> dạng rút gọn để minh họa từng bảng.

**Bước 3 — INSERT column_mapping:**

```sql
INSERT INTO "minio-datalake"."metadata".column_mapping VALUES
('M020', 'P005', 'card_type_code', 'card_type_code', 'VARCHAR', NULL, true, false, NULL, 1, 'PK'),
('M021', 'P005', 'card_type_name', 'card_type_name', 'VARCHAR', 'TRIM(${src})', false, false, NULL, 2, 'Name'),
('M022', 'P005', 'card_network',   'card_network',   'VARCHAR', 'UPPER(${src})', false, true, '''DOMESTIC''', 3, 'VISA/MC/NAPAS'),
('M023', 'P005', 'is_active',      'is_active',      'BOOLEAN', NULL, false, false, 'true', 4, 'Status');
```

**Bước 4 — INSERT transform_rules (silver):**

```sql
INSERT INTO "minio-datalake"."metadata".transform_rules VALUES (
    'T010', 'P005', 'create_silver_card_types',
    'bronze', 'silver', 'create_table',
    'CREATE OR REPLACE TABLE "minio-datalake"."silver".card_types AS
     SELECT * FROM (
       SELECT *, ROW_NUMBER() OVER (PARTITION BY card_type_code ORDER BY card_type_code) AS rn
       FROM "minio-datalake"."bronze".card_types
     ) WHERE rn = 1',
    NULL, 1, true,
    'Create/replace silver card_types from bronze'
);
```

**Bước 5 — Không cần Gold** (reference data, không aggregate)

**Bước 6 — INSERT DQ rules:**

```sql
INSERT INTO "minio-datalake"."metadata".data_quality_rules VALUES
('DQ010', 'P005', 'silver', 'card_types', 'card_type_code', 'not_null',
 '${column} IS NOT NULL', 'critical', 0.0, true, 'PK must not be null'),
('DQ011', 'P005', 'silver', 'card_types', 'card_type_code', 'unique',
 'COUNT(*) = COUNT(DISTINCT ${column})', 'error', 0.0, true, 'PK must be unique');
```

**Bước 7 — Test:** Controller lọc theo lịch nên muốn chạy ngay 1 bảng: tạm set
`schedule_cron = '* * * * *'` (mỗi phút) cho bảng đó → tick kế tiếp sẽ bắt; xong nhớ revert. (Hoặc
dùng `* <giờ-phút hiện tại>`.) Theo dõi data đổ vào MinIO `bronze/` và `execution_log`.

**Bước 8 — Verify:**
```sql
-- Check bronze data
SELECT COUNT(*) FROM "minio-datalake"."bronze".card_types;

-- Check silver data
SELECT COUNT(*) FROM "minio-datalake"."silver".card_types;

-- Check execution log
SELECT * FROM "minio-datalake"."metadata".pipeline_execution_log
WHERE pipeline_id = 'P005'
ORDER BY start_time DESC
LIMIT 5;
```

### 1.3 Onboard Checklist

```
□ Source table analyzed (columns, size, PK, update pattern)
□ Load type decided (full/incremental)
□ pipeline_config inserted
□ column_mapping inserted (all columns)
□ transform_rules inserted (silver, gold if needed)
□ data_quality_rules inserted
□ Manual test run successful
□ Bronze data verified in MinIO
□ Silver data verified in Dremio
□ Execution log recorded
□ Schedule enabled (is_active = true)
```

---

## 2. Quản Lý Pipeline

### 2.1 Tắt/Bật Pipeline

```sql
-- Tạm tắt 1 pipeline
UPDATE "minio-datalake"."metadata".pipeline_config
SET is_active = false, updated_at = CURRENT_TIMESTAMP
WHERE pipeline_id = 'P001';

-- Bật lại
UPDATE "minio-datalake"."metadata".pipeline_config
SET is_active = true, updated_at = CURRENT_TIMESTAMP
WHERE pipeline_id = 'P001';

-- Tắt tất cả (maintenance mode)
UPDATE "minio-datalake"."metadata".pipeline_config
SET is_active = false, updated_at = CURRENT_TIMESTAMP;
```

### 2.2 Thay Đổi Load Type

Chuyển từ full → incremental (khi bảng đã lớn):

```sql
UPDATE "minio-datalake"."metadata".pipeline_config
SET
    load_type = 'incremental',
    watermark_column = 'updated_at',
    updated_at = CURRENT_TIMESTAMP
WHERE pipeline_id = 'P002';

-- Seed initial watermark — INSERT bỏ start_time/end_time/created_at, set bằng UPDATE sau
INSERT INTO "minio-datalake"."metadata".pipeline_execution_log
(execution_id, run_id, pipeline_id, pipeline_name, layer,
 status, rows_processed, rows_inserted, rows_updated, rows_rejected,
 last_watermark, error_message, execution_params)
VALUES (
    'P002_SEED',
    'SEED',
    'P002',
    'ingest_merchants',
    'bronze',
    'success',
    0, 0, 0, 0,
    '2026-06-01 00:00:00',  -- Last watermark = start date
    NULL,
    'seed'
);

-- set timestamp thật (ngoài VALUES → tránh Duplicate key CURRENT_TIMESTAMP)
UPDATE "minio-datalake"."metadata".pipeline_execution_log
SET start_time = CURRENT_TIMESTAMP, end_time = CURRENT_TIMESTAMP, created_at = CURRENT_TIMESTAMP
WHERE execution_id = 'P002_SEED';
```

### 2.3 Thêm/Sửa Column

```sql
-- Thêm column mới
INSERT INTO "minio-datalake"."metadata".column_mapping VALUES (
    'M030', 'P001', 'channel',  'payment_channel', 'VARCHAR',
    'UPPER(TRIM(${src}))', false, true, '''UNKNOWN''', 10,
    'Payment channel: POS, ECOM, QR, ATM'
);

-- Sửa transformation
UPDATE "minio-datalake"."metadata".column_mapping
SET transformation = 'COALESCE(CAST(${src} AS DECIMAL(18,2)), 0)'
WHERE mapping_id = 'M002';
```

### 2.4 Reset Pipeline (Re-run từ đầu)

```sql
-- 1. Xóa execution history
DELETE FROM "minio-datalake"."metadata".pipeline_execution_log
WHERE pipeline_id = 'P001';

-- 2. Drop silver table (nếu cần recreate)
DROP TABLE IF EXISTS "minio-datalake"."silver".transactions;

-- 3. Chuyển tạm về full load để recreate
UPDATE "minio-datalake"."metadata".pipeline_config
SET load_type = 'full', updated_at = CURRENT_TIMESTAMP
WHERE pipeline_id = 'P001';

-- 4. Trigger manual run

-- 5. Sau khi xong, chuyển lại incremental
UPDATE "minio-datalake"."metadata".pipeline_config
SET load_type = 'incremental', updated_at = CURRENT_TIMESTAMP
WHERE pipeline_id = 'P001';
```

---

## 3. Monitoring

### 3.1 Dashboard Queries

**Pipeline Status Overview:**

```sql
SELECT
    p.pipeline_id,
    p.pipeline_name,
    p.source_table,
    p.load_type,
    p.is_active,
    e.last_run,
    e.last_status,
    e.last_rows,
    e.total_runs_today,
    e.failed_runs_today
FROM "minio-datalake"."metadata".pipeline_config p
LEFT JOIN (
    SELECT
        pipeline_id,
        MAX(start_time) AS last_run,
        MAX(CASE WHEN start_time = sub.max_time THEN status END) AS last_status,
        MAX(CASE WHEN start_time = sub.max_time THEN rows_processed END) AS last_rows,
        COUNT(CASE WHEN CAST(start_time AS DATE) = CURRENT_DATE THEN 1 END) AS total_runs_today,
        COUNT(CASE WHEN CAST(start_time AS DATE) = CURRENT_DATE AND status = 'failed' THEN 1 END) AS failed_runs_today
    FROM "minio-datalake"."metadata".pipeline_execution_log,
         (SELECT pipeline_id AS pid, MAX(start_time) AS max_time
          FROM "minio-datalake"."metadata".pipeline_execution_log
          GROUP BY pipeline_id) sub
    WHERE pipeline_id = sub.pid
    GROUP BY pipeline_id
) e ON p.pipeline_id = e.pipeline_id
ORDER BY p.pipeline_id;
```

**Failed Pipelines (cần attention):**

```sql
SELECT
    pipeline_id,
    pipeline_name,
    layer,
    start_time,
    end_time,
    error_message
FROM "minio-datalake"."metadata".pipeline_execution_log
WHERE status = 'failed'
  AND CAST(start_time AS DATE) >= CURRENT_DATE - INTERVAL '7' DAY
ORDER BY start_time DESC;
```

**Data Freshness per Table:**

```sql
SELECT
    p.pipeline_id,
    p.source_table,
    e.last_success_time,
    e.last_watermark,
    CASE
        WHEN e.last_success_time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY THEN 'FRESH'
        WHEN e.last_success_time >= CURRENT_TIMESTAMP - INTERVAL '3' DAY THEN 'STALE'
        ELSE 'CRITICAL'
    END AS freshness_status
FROM "minio-datalake"."metadata".pipeline_config p
LEFT JOIN (
    SELECT
        pipeline_id,
        MAX(end_time) AS last_success_time,
        MAX(last_watermark) AS last_watermark
    FROM "minio-datalake"."metadata".pipeline_execution_log
    WHERE status = 'success' AND layer = 'bronze'
    GROUP BY pipeline_id
) e ON p.pipeline_id = e.pipeline_id
WHERE p.is_active = true
ORDER BY freshness_status DESC, p.pipeline_id;
```

**Daily Volume Trend:**

```sql
SELECT
    CAST(start_time AS DATE) AS run_date,
    pipeline_name,
    SUM(rows_processed) AS total_rows,
    COUNT(*) AS run_count,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_count
FROM "minio-datalake"."metadata".pipeline_execution_log
WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
GROUP BY CAST(start_time AS DATE), pipeline_name
ORDER BY run_date DESC, pipeline_name;
```

### 3.2 NiFi Monitoring

Trong NiFi UI, monitor các metrics:

| Metric                    | Nơi xem                              | Mức bình thường          |
|---------------------------|---------------------------------------|--------------------------|
| Queued FlowFiles          | Connection labels                     | 0 (empty khi idle)       |
| Bytes In/Out              | Process Group status bar              | Phụ thuộc data volume    |
| Active Threads            | Processor status                      | < Max Concurrent Tasks   |
| Bulletins                 | Top-right bulletin board             | No errors                |
| Back Pressure             | Connection settings                   | Should not trigger       |
| Provenance Events         | Right-click processor → View Data Provenance | Recent events exist |

**NiFi Bulletin Alerts — Nơi xem lỗi:**

1. Click biểu tượng **bulletin** (top-right warning icon)
2. Hoặc Right-click processor → **View status history**
3. Hoặc vào **Menu → Summary** để xem tổng quan

### 3.3 Dremio Monitoring

Trong Dremio UI → **Jobs**:

| Thông tin    | Mô tả                              |
|--------------|--------------------------------------|
| Job ID       | Unique ID cho mỗi query             |
| Status       | COMPLETED, FAILED, CANCELLED        |
| Duration     | Thời gian chạy                       |
| Rows         | Số rows processed                    |
| Query        | SQL đã chạy                          |

**Filter failed jobs:**
- Dremio UI → Jobs → Filter by Status = FAILED
- Xem error message để diagnose

---

## 4. Troubleshooting

### 4.1 Pipeline không chạy

| Triệu chứng | Nguyên nhân | Giải pháp |
|---|---|---|
| Không có FlowFile mới | Pipeline bị disable hoặc `is_active = false` | Check NiFi processor state + metadata |
| ExecuteSQL error | JDBC connection failed | Check Controller Service enabled, test connection |
| Metadata query trả về 0 rows | Không có pipeline active | `SELECT * FROM pipeline_config WHERE is_active = true` |
| Cron không trigger | Schedule sai hoặc NiFi stopped | Check Scheduling tab của GenerateFlowFile |

### 4.2 Bronze ingestion thất bại

| Triệu chứng | Nguyên nhân | Giải pháp |
|---|---|---|
| Source DB connection refused | DB down hoặc sai credentials | Verify DB status, check JDBC URL |
| Timeout khi query bảng lớn | `batch_size` quá lớn hoặc không có index | Giảm `batch_size`, thêm index trên watermark column |
| Incremental trả về 0 rows | `last_watermark` bằng hoặc mới hơn data | Check `pipeline_execution_log`, reset watermark nếu cần |
| PutS3Object failed | MinIO down hoặc sai credentials | Verify MinIO, check Access Key / Secret Key |
| File ghi vào sai path | Expression Language sai | Test EL expression: `${source_table}` có resolve đúng không |

### 4.3 Silver/Gold transform thất bại

| Triệu chứng | Nguyên nhân | Giải pháp |
|---|---|---|
| `Table not found` | Bronze data chưa có hoặc Dremio chưa scan | Refresh Dremio source, verify bronze data exists |
| `MERGE syntax error` | Dremio version không hỗ trợ `MERGE ... SET *` | Explicit column list thay vì `SET *` |
| `Memory exceeded` | Query quá nặng cho Dremio resources | Tăng Dremio memory, hoặc giảm data volume |
| `CREATE TABLE exists` | Table đã tồn tại | Dùng `CREATE OR REPLACE TABLE` hoặc `CREATE TABLE IF NOT EXISTS` |
| DQ check fail | Data quality issue | Xem chi tiết failure, fix source data hoặc adjust threshold |

### 4.4 Debug Techniques

**1. Check FlowFile attributes:**

Right-click processor → **View data provenance** → Click FlowFile → **View Details** → **Attributes** tab

**2. Check FlowFile content (SQL đã generate):**

Right-click queue (connection line) → **List queue** → Click FlowFile → **View Content**

→ Xem SQL đã được render đúng chưa.

**3. Test SQL trực tiếp trong Dremio:**

Copy SQL từ FlowFile content → paste vào Dremio SQL Runner → chạy thử

**4. Check NiFi log:**

```bash
kubectl logs -n data-ingestion nifi-0 --tail=100
```

**5. Check Dremio log:**

```bash
kubectl logs -n data-processing dremio-coordinator-0 --tail=100
```

---

## 5. Backup & Recovery

### 5.1 Backup Metadata

Tạo snapshot metadata tables định kỳ:

```sql
-- Backup pipeline_config
CREATE TABLE "minio-datalake"."metadata".pipeline_config_backup_20260624 AS
SELECT * FROM "minio-datalake"."metadata".pipeline_config;

-- Hoặc export ra JSON qua NiFi:
-- ExecuteSQL → ConvertAvroToJSON → PutS3Object
-- Path: backup/metadata/pipeline_config/dt=2026-06-24/config.json
```

### 5.2 Recovery

```sql
-- Restore từ backup
CREATE OR REPLACE TABLE "minio-datalake"."metadata".pipeline_config AS
SELECT * FROM "minio-datalake"."metadata".pipeline_config_backup_20260624;
```

### 5.3 NiFi Flow Backup

Export NiFi flow template:

1. NiFi UI → Root canvas
2. Right-click Root Process Group → **Download flow definition**
3. Lưu file `.json` vào git: `napas-platform-infra/nifi-flows/metadata-pipeline-v1.json`

---

## 6. Scaling Considerations

### 6.1 Thêm Source Database mới

Khi cần ingest từ DB mới (ví dụ: MySQL, Oracle):

1. **Thêm JDBC driver** vào NiFi:
   ```bash
   # MySQL
   kubectl exec -n data-ingestion nifi-0 -- curl -L -o /opt/nifi/nifi-current/lib/mysql-connector-j-8.3.0.jar "..."

   # Oracle
   kubectl exec -n data-ingestion nifi-0 -- curl -L -o /opt/nifi/nifi-current/lib/ojdbc11.jar "..."
   ```

2. **Tạo Controller Service** mới trong NiFi:
   - Name: `source-mysql-pool`, `source-oracle-pool`
   - Config JDBC URL, driver class, credentials

3. **INSERT pipeline_config** với `source_connection` = tên CS mới

4. **Trong NiFi Bronze Ingestion PG**, thêm RouteOnAttribute:
   - Route by `source_connection`
   - Mỗi route → ExecuteSQL với CS tương ứng

### 6.2 Performance Tuning

| Tuning                        | Khi nào                            | Cách làm                           |
|-------------------------------|------------------------------------|------------------------------------|
| Tăng `batch_size`             | Source DB nhanh, network tốt       | Update metadata                    |
| Giảm `batch_size`             | OOM hoặc timeout                   | Update metadata                    |
| Thêm NiFi nodes              | Throughput bottleneck ở NiFi       | Scale NiFi cluster                 |
| Tăng Dremio executor          | Transform chậm                     | Scale executor StatefulSet         |
| Partition bronze data         | Scan toàn bộ bronze chậm          | Partition by date                  |
| Dremio Reflections            | Gold queries chậm                  | Create Reflections trên gold views |
| Parallel pipeline execution   | Nhiều bảng, mỗi bảng independent  | NiFi handles via concurrent tasks  |

### 6.3 Parallel Execution

NiFi tự động chạy song song khi SplitJson tạo nhiều FlowFiles.
Mỗi FlowFile = 1 table config → ExecuteSQL chạy concurrent.

Điều chỉnh **Concurrent Tasks** trên ExecuteSQL processor:
- Default: 1 (sequential)
- Đề nghị: 3-5 (parallel, tùy source DB capacity)

---

## 7. Quick Reference

### 7.1 Port-Forward Commands

```bash
# NiFi UI
kubectl port-forward -n data-ingestion svc/nifi 8444:8443

# Dremio UI  (service tên là `dremio`, KHÔNG phải `dremio-service`)
kubectl port-forward -n data-processing svc/dremio 9047:9047
# Lưu ý: port-forward KHÔNG tự reconnect khi pod restart. Nếu UI timeout mà
# `kubectl get pods -n data-processing` thấy RESTARTS tăng → kill và chạy lại lệnh này.

# MinIO Console
kubectl port-forward -n data-storage svc/minio-console 9002:9001

# MinIO API
kubectl port-forward -n data-storage svc/minio 9000:9000

# PostgreSQL (source demo)
kubectl port-forward -n data-visualization svc/superset-postgresql 5433:5432
```

### 7.2 Credentials

| Service    | Username    | Password              | Ghi chú                    |
|------------|-------------|-----------------------|----------------------------|
| NiFi       | `admin`     | `NiFiAdmin@2024`      | HTTPS login                |
| Dremio     | `admin`     | (set lúc first login) | Web UI + JDBC              |
| MinIO      | `napas-admin`| `napas-minio-s3cr3t-2024` | Console + API          |
| PostgreSQL | `superset`  | `SupersetPostgres2024`| Demo source DB             |

### 7.3 Common Metadata Queries

```sql
-- Liệt kê tất cả pipeline
SELECT pipeline_id, pipeline_name, load_type, is_active FROM "minio-datalake"."metadata".pipeline_config;

-- Last 10 executions
SELECT pipeline_name, layer, status, rows_processed, start_time
FROM "minio-datalake"."metadata".pipeline_execution_log
ORDER BY start_time DESC LIMIT 10;

-- Pipeline chưa chạy hôm nay
SELECT p.pipeline_id, p.pipeline_name
FROM "minio-datalake"."metadata".pipeline_config p
WHERE p.is_active = true
  AND p.pipeline_id NOT IN (
    SELECT DISTINCT pipeline_id FROM "minio-datalake"."metadata".pipeline_execution_log
    WHERE CAST(start_time AS DATE) = CURRENT_DATE AND status = 'success'
  );
```
