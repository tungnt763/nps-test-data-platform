# Step 5: Dremio — SQL Engine & Data Lake Processing

> **Deploy order:** 5/6
> **Namespace:** `data-processing`
> **Phụ thuộc:** MinIO (data source), Ranger (authorization policies)

---

## 1. Dremio Là Gì?

Dremio là **Data Lake Engine** — một SQL query engine hiệu năng cao cho phép truy vấn dữ liệu trực tiếp trên Data Lake (S3/MinIO) mà **không cần ETL vào Data Warehouse**. Dremio đóng vai trò là "lớp xử lý" và "lớp truy vấn" — vừa transform Bronze→Silver→Gold, vừa serve queries từ Superset.

**Điểm khác biệt với truyền thống:**

```
Kiến trúc truyền thống:
  DB (Oracle) → ETL Script → Data Warehouse (SQL Server/Redshift) → BI Tool
  Vấn đề: tốn chi phí DW, dữ liệu bị copy nhiều lần, không flexible

Kiến trúc Dremio:
  MinIO (S3) → Dremio Virtual Dataset → Superset
  Dữ liệu KHÔNG bị copy — Dremio query thẳng vào file Parquet trên MinIO
  Reflections = materialized cache cho performance
```

---

## 2. Kiến Trúc Dremio

### 2.1. Thành Phần

```
┌──────────────────────────────────────────────────────────────────────┐
│                            Dremio Cluster                             │
│                                                                       │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │                  Coordinator Node (:9047)                     │    │
│  │                                                               │    │
│  │  • Web UI & REST API                                          │    │
│  │  • SQL Parser & Planner (Apache Calcite)                      │    │
│  │  • Query Optimizer (cost-based)                               │    │
│  │  • Metadata Catalog (dataset definitions)                     │    │
│  │  • Reflection Manager (materialized views)                    │    │
│  │  • Arrow Flight Server (:32010) ← Superset kết nối           │    │
│  └──────────────────────────┬───────────────────────────────────┘    │
│                             │ distribute work                         │
│           ┌─────────────────┼───────────────────────┐               │
│           │                 │                       │               │
│           ▼                 ▼                       ▼               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐  │
│  │ Executor 1   │  │ Executor 2   │  │ Executor N               │  │
│  │              │  │              │  │                          │  │
│  │ • Scan files │  │ • Scan files │  │ • Filter / Aggregate    │  │
│  │ • Columnar   │  │ • Join       │  │ • Sort                  │  │
│  │   pushdown   │  │   hash join  │  │ • Exchange/Shuffle      │  │
│  └──────┬───────┘  └──────┬───────┘  └───────────┬─────────────┘  │
│         │                 │                      │               │
│         └─────────────────┼──────────────────────┘               │
│                           │ S3 Read (parallel)                    │
└───────────────────────────┼──────────────────────────────────────┘
                            │
                   ┌────────▼────────┐
                   │     MinIO       │
                   │  napas-datalake │
                   │  bronze/silver/ │
                   │  gold/          │
                   └─────────────────┘
```

### 2.2. Các Khái Niệm Chính

| Khái niệm           | Giải thích                                                                    |
|---------------------|-------------------------------------------------------------------------------|
| **Source**          | Kết nối đến hệ thống lưu trữ (MinIO/S3, PostgreSQL, Oracle...)                |
| **Virtual Dataset** | View SQL trên dữ liệu thô — không copy dữ liệu, chỉ lưu định nghĩa SQL       |
| **Physical Dataset**| File thực trên S3 (Parquet, ORC, JSON...) đã được Dremio index                |
| **Reflection**      | Materialized view — Dremio tự precompute và cache để tăng tốc query            |
| **Space**           | Thư mục organize datasets trong Dremio (giống schema trong DB)                 |
| **Arrow Flight**    | Protocol giao tiếp tốc độ cao (binary, columnar) — Superset dùng để kết nối  |
| **Catalog**         | Metadata store — lưu schema, stats, column types của mọi dataset               |

---

## 3. Dremio Hoạt Động Như Thế Nào?

### 3.1. Query Execution Flow

```
Superset gửi SQL:
  SELECT bank_id, SUM(amount) as total
  FROM gold.txn_summary
  WHERE date = '2026-06-19'
  GROUP BY bank_id

  │
  ▼
[Dremio Coordinator]
  1. Parse SQL (Calcite)
  2. Lookup metadata: gold.txn_summary → s3://napas-datalake/gold/txn_summary/
  3. Check Reflections: có materialized view nào match không?
     → YES: dùng reflection (pre-aggregated data)
     → NO: scan raw Parquet files
  4. Plan: scan Parquet → filter date → aggregate → sort
  5. Distribute plan to Executors

  │
  ▼
[Dremio Executors] (parallel)
  - Executor 1: scan date=2026-06-19/part-001.parquet (columns: bank_id, amount, date)
  - Executor 2: scan date=2026-06-19/part-002.parquet
  → Column pruning: chỉ đọc 3 columns, bỏ qua toàn bộ columns khác
  → Predicate pushdown: filter date tại file scan level

  │
  ▼
[Coordinator] collect results → trả về Arrow format
  │
  ▼
[Superset] render chart
```

### 3.2. Xây Dựng 3 Layers với Dremio

```
BRONZE:  Physical Dataset
  Source: MinIO S3 → s3://napas-datalake/bronze/transactions/
  Dremio tự discover Parquet files và infer schema
  Không cần SQL, chỉ cần point to folder

SILVER:  Virtual Dataset (SQL Transform)
  CREATE VDS silver.transactions_clean AS
  SELECT DISTINCT
    txn_id,
    CAST(amount AS DECIMAL(18,2))       AS amount,
    LPAD(bank_id, 9, '0')              AS bank_id,
    CAST(created_at AS TIMESTAMP)       AS created_at,
    COALESCE(merchant_id, 'UNKNOWN')   AS merchant_id
  FROM bronze.transactions
  WHERE status_code = '00'
    AND txn_id IS NOT NULL
    AND amount > 0

GOLD:    Virtual Dataset (Aggregation)
  CREATE VDS gold.txn_summary_by_bank AS
  SELECT
    bank_id,
    CAST(created_at AS DATE)           AS txn_date,
    COUNT(*)                           AS total_count,
    SUM(amount)                        AS total_amount,
    AVG(amount)                        AS avg_amount,
    COUNT(DISTINCT merchant_id)        AS unique_merchants
  FROM silver.transactions_clean
  GROUP BY bank_id, CAST(created_at AS DATE)

Reflection (tăng tốc Gold layer):
  → Dremio tự precompute gold.txn_summary_by_bank
  → Lưu vào s3://napas-datalake/.dremio-reflections/
  → Query Superset: từ 30s → <1s
```

---

## 4. Tương Tác Với Các Component Khác

```
Vault ──(S3 credentials inject)──▶ Dremio startup

MinIO ◀──(S3 API read/write)──── Dremio
  ├── Read: bronze/, silver/, gold/ (query & transform)
  └── Write: silver/, gold/ (CTAS virtual datasets materialized)

Ranger ──(plugin enforce policies)──▶ Dremio
  ├── SQL-level: table access control
  ├── Column masking: card_number → ****1234
  └── Row filter: analysts chỉ thấy bank_id của mình

Superset ──(Arrow Flight SQL)──▶ Dremio
  ← Trả về kết quả theo Arrow columnar format
```

---

## 5. Ưu Điểm & Nhược Điểm

| Ưu điểm                                          | Nhược điểm                                        |
|--------------------------------------------------|---------------------------------------------------|
| Query thẳng S3, không cần ETL vào DW             | OSS version thiếu một số enterprise features      |
| Reflections: query performance tương đương DW   | Coordinator là SPOF (cần multi-coord cho prod HA)|
| Arrow Flight: giao tiếp Superset cực nhanh       | Executor cần nhiều RAM (8-16GB/node)              |
| Schema evolution: Parquet schema thay đổi OK     | Reflection refresh cần schedule manual            |
| Hỗ trợ nhiều source: S3, RDBMS, REST, MongoDB...| Learning curve: khác với SQL thuần                |
| Virtual datasets: không tốn storage             |                                                   |

---

## 6. Cài Đặt — Helm Values

### 6.1. Base values

```yaml
# platform/values/base/dremio.yaml
image:
  repository: dremio/dremio-oss
  tag: "24.3.2"
  pullPolicy: IfNotPresent

# Coordinator configuration
coordinator:
  count: 1
  memory: 8192       # MB
  cpu: 2
  volumeClaimTemplates:
    storageClass: "platform-standard"
    storageRequest: "50Gi"

# Executor configuration
executor:
  count: 1
  memory: 8192       # MB
  cpu: 4
  volumeClaimTemplates:
    storageClass: "platform-standard"
    storageRequest: "30Gi"

# Zookeeper (dùng cho coordinator HA — để default embedded cho dev)
zookeeper:
  enabled: false

# Service
service:
  type: ClusterIP
  # Web UI + REST
  webPort: 9047
  # Arrow Flight (Superset kết nối)
  flightPort: 32010
  # JDBC/ODBC
  jdbcPort: 31010

# Dremio internal config
distStorage:
  # Lưu reflections và system data vào MinIO
  type: "aws"       # AWS S3 compatible
  aws:
    bucketName: "napas-datalake"
    path: "/.dremio-internal"
    authentication: "accessKeySecret"
    credentials:
      accessKey: "napas-admin"
      secret: "napas-minio-s3cr3t-2024"
    # MinIO endpoint override
    compatibility:
      s3ServerSideEncryption: false
    endpoint: "http://minio.data-storage.svc.cluster.local:9000"
    allowUntrustedCertificates: true
    enablePathStyleAccess: true
```

### 6.2. Helmfile configuration

```yaml
# platform/helmfile.yaml.gotmpl (layer 05-processing)
- name: dremio
  namespace: data-processing
  chart: ./charts/dremio    # Dùng Dremio official Helm chart
  values:
    - values/base/dremio.yaml
    - values/env/dremio.yaml.gotmpl
  needs:
    - data-storage/minio
    - data-governance/ranger
  labels:
    layer: 05-processing
```

### 6.3. Download Dremio Helm chart

```bash
# Dremio có official Helm chart
helm repo add dremio https://charts.dremio.com
helm repo update

# Hoặc dùng official GitHub chart
wget https://github.com/dremio/dremio-cloud-tools/releases/download/v24.3.2/dremio-v24.3.2.tgz
tar xf dremio-v24.3.2.tgz -C platform/charts/
```

### 6.4. Deploy

```bash
./scripts/deploy.sh dev 05-processing

kubectl get pods -n data-processing -w
# NAME                      READY   STATUS    RESTARTS   AGE
# dremio-coordinator-0      1/1     Running   0          3m
# dremio-executor-0         1/1     Running   0          3m
```

---

## 7. Cấu Hình Dremio Sau Deploy

```bash
# Port-forward
kubectl port-forward -n data-processing svc/dremio 9047:9047 &

# Mở browser: http://localhost:9047
# Setup wizard: tạo admin account
#   Username: admin
#   Email: admin@napas.com
#   Password: Dremio@Admin2024
```

### 7.1. Thêm MinIO Source

**Dremio UI → Sources → + Add Source → Amazon S3**

```
Name: minio-napas
Authentication: AWS Access Key
Access Key: napas-admin
Secret Key: napas-minio-s3cr3t-2024

Advanced Options:
  Enable compatibility mode: ✅
  Connection properties:
    fs.s3a.endpoint = minio.data-storage.svc.cluster.local:9000
    fs.s3a.path.style.access = true
    fs.s3a.connection.ssl.enabled = false

Root Path: /napas-datalake
```

### 7.2. Tạo Spaces và Datasets

```sql
-- Trong Dremio SQL Runner (UI → SQL Runner)

-- Tạo Bronze Virtual Dataset
CREATE OR REPLACE VDS bronze.transactions AS
SELECT *
FROM "minio-napas".bronze.transactions;

-- Tạo Silver Virtual Dataset
CREATE OR REPLACE VDS silver.transactions_clean AS
SELECT DISTINCT
  txn_id,
  CAST(amount AS DECIMAL(18,2))     AS amount,
  LPAD(CAST(bank_id AS VARCHAR), 9, '0') AS bank_id,
  CAST(created_at AS TIMESTAMP)     AS created_at,
  COALESCE(merchant_id, 'UNKNOWN') AS merchant_id,
  status_code
FROM bronze.transactions
WHERE status_code = '00'
  AND txn_id IS NOT NULL
  AND amount > 0;

-- Tạo Gold Virtual Dataset
CREATE OR REPLACE VDS gold.txn_summary_by_bank AS
SELECT
  bank_id,
  CAST(created_at AS DATE)          AS txn_date,
  COUNT(*)                          AS total_count,
  SUM(amount)                       AS total_amount,
  AVG(amount)                       AS avg_amount,
  COUNT(DISTINCT merchant_id)       AS unique_merchants
FROM silver.transactions_clean
GROUP BY bank_id, CAST(created_at AS DATE);

-- Tạo Reflection cho Gold (tăng performance)
ALTER DATASET gold.txn_summary_by_bank
CREATE REFLECTION "gold_reflection_by_bank"
USING AGGREGATION
  DIMENSIONS (bank_id, txn_date)
  MEASURES (total_count, total_amount, avg_amount, unique_merchants);
```

---

## 8. Validate Dremio Hoạt Động Đúng

```bash
# ✅ Check 1: Pods READY
kubectl get pods -n data-processing
# Expected:
# dremio-coordinator-0   1/1   Running
# dremio-executor-0      1/1   Running

# ✅ Check 2: Web UI accessible
kubectl port-forward -n data-processing svc/dremio 9047:9047 &
# Mở: http://localhost:9047 → login thành công

# ✅ Check 3: MinIO source connected
# UI → Sources → minio-napas → Status: Connected (xanh lá)

# ✅ Check 4: Query Bronze layer
# UI → SQL Runner:
SELECT COUNT(*) as total_files
FROM TABLE(directory_listing('minio-napas.napas-datalake.bronze'))
;
-- Expected: số > 0 nếu NiFi đã ghi data

# ✅ Check 5: Test transformation query
SELECT bank_id, COUNT(*) as cnt
FROM silver.transactions_clean
LIMIT 10;
-- Expected: kết quả trả về nhanh, không lỗi

# ✅ Check 6: Arrow Flight endpoint
kubectl port-forward -n data-processing svc/dremio 32010:32010 &
# Test từ Python:
python3 -c "
from pyarrow import flight
import pyarrow as pa
client = flight.FlightClient('grpc://localhost:32010')
options = flight.FlightCallOptions(headers=[(b'authorization', b'Basic YWRtaW46RHJlbWlvQEFkbWluMjAyNA==')])
# admin:Dremio@Admin2024 base64 encoded
info = client.get_flight_info(flight.FlightDescriptor.for_command(b'SELECT 1 as test'), options)
reader = client.do_get(info.endpoints[0].ticket, options)
print(reader.read_all())
"
# Expected: pyarrow Table với 1 row, column test=1

# ✅ Check 7: Reflection status
# UI → Jobs → Reflections
# Expected: gold_reflection_by_bank → Status: CAN ACCELERATE
```

### Kết Quả Validate Thành Công

```
[✅] coordinator + executor pods: 1/1 Running
[✅] Web UI: accessible
[✅] MinIO source: Connected
[✅] Bronze query: kết quả trả về
[✅] Silver transform: hoạt động
[✅] Arrow Flight: kết nối OK
[✅] Reflection: CAN ACCELERATE
→ Dremio sẵn sàng — Proceed to Step 6: Apache Superset
```

---

## 9. Lưu Ý

```
⚠️  Dremio OSS không tích hợp Ranger plugin sẵn. Cần dùng Dremio
    Enterprise cho native Ranger integration, hoặc implement
    custom authorization plugin.
    → Trong scope này: dùng Dremio RBAC nội bộ + Ranger ở tầng network.

💡  Reflections cần được refresh định kỳ khi data mới vào:
    Đặt lịch refresh: Settings → Reflection Settings → Refresh Policy = Every 6 hours

💡  Parquet format quan trọng: Dremio đọc Parquet nhanh nhất nhờ
    column pruning và predicate pushdown.
    NiFi nên convert sang Parquet TRƯỚC khi ghi vào MinIO.
```
