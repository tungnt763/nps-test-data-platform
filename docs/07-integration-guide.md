# Step 7: Integration Guide — End-to-End Flow

> Hướng dẫn kết nối toàn bộ platform và chạy pipeline đầu tiên từ đầu đến cuối

---

## 1. Checklist Trước Khi Bắt Đầu

```bash
# Chạy validation script để kiểm tra tất cả services
./scripts/validate.sh dev

# Expected output:
# ✓ PASS: 20+ checks
# ✗ FAIL: 0
```

---

## 2. Quickstart — Deploy Toàn Bộ Platform

```bash
# Clone project
cd napas-platform-infra

# Bước 1: Bootstrap cluster (1 lần duy nhất)
chmod +x scripts/*.sh
./scripts/setup.sh dev

# Bước 2: Deploy Vault trước
./scripts/deploy.sh dev 01-security

# Bước 3: Init Vault (1 lần duy nhất - sau khi deploy Vault)
./scripts/init-vault.sh dev
# ⚠ Backup file vault-init-dev.json vào nơi an toàn!

# Bước 4: Deploy phần còn lại theo thứ tự
./scripts/deploy.sh dev 02-storage       # MinIO (~2 phút)
./scripts/deploy.sh dev 03-ingestion     # NiFi  (~3 phút, nặng nhất)
./scripts/deploy.sh dev 04-governance    # Ranger (~3 phút)
./scripts/deploy.sh dev 05-processing   # Dremio (~3 phút)
./scripts/deploy.sh dev 06-visualization # Superset (~2 phút)

# Bước 5: Validate toàn bộ
./scripts/validate.sh dev
```

---

## 3. Access Các Service UI

```bash
# Mở tất cả port-forwards cùng lúc (dev)
kubectl port-forward -n data-security     svc/vault    8200:8200 &
kubectl port-forward -n data-storage      svc/minio-console 9001:9001 &
kubectl port-forward -n data-ingestion    svc/nifi     8443:8443 &
kubectl port-forward -n data-governance   svc/ranger   6080:6080 &
kubectl port-forward -n data-processing   svc/dremio   9047:9047 &
kubectl port-forward -n data-visualization svc/superset 8088:8088 &
```

| Service   | URL                         | Username  | Password             |
|-----------|-----------------------------|-----------|----------------------|
| Vault     | http://localhost:8200        | token     | (từ vault-init.json) |
| MinIO     | http://localhost:9001        | napas-admin | napas-minio-s3cr3t-2024 |
| NiFi      | https://localhost:8443/nifi  | admin     | NiFiAdmin@2024       |
| Ranger    | http://localhost:6080        | admin     | admin     |
| Dremio    | http://localhost:9047        | admin     | Dremio@Admin2024     |
| Superset  | http://localhost:8088        | admin     | NapasSuperset@2024   |

---

## 4. Pipeline End-to-End: NiFi → MinIO → Dremio → Superset

### 4.1. Bước A: Tạo dữ liệu test trong MinIO (giả lập NiFi ingest)

```bash
# Tạo file Parquet test (dùng Python)
python3 << 'EOF'
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from datetime import datetime, timedelta
import random

# Tạo fake transaction data
rows = []
banks = ['VCB', 'BIDV', 'TCB', 'MB', 'VPB']
for i in range(1000):
  rows.append({
    'txn_id': f'TXN{i:08d}',
    'bank_id': random.choice(banks),
    'amount': round(random.uniform(10000, 5000000), 2),
    'merchant_id': f'MERCH{random.randint(1,100):04d}',
    'status_code': '00' if random.random() > 0.05 else '51',
    'created_at': datetime(2026, 6, 19, 0, 0, 0) + timedelta(seconds=random.randint(0, 86400)),
    'card_number': f'****-****-****-{random.randint(1000,9999)}'
  })

df = pd.DataFrame(rows)
table = pa.Table.from_pandas(df)
pq.write_table(table, '/tmp/transactions-2026-06-19.parquet')
print(f"Created {len(rows)} transactions")
EOF

# Upload lên MinIO bronze layer
mc cp /tmp/transactions-2026-06-19.parquet \
  napas-local/napas-datalake/bronze/transactions/date=2026-06-19/part-00001.parquet

# Verify
mc ls napas-local/napas-datalake/bronze/transactions/date=2026-06-19/
```

### 4.2. Bước B: Cấu hình Dremio kết nối MinIO và tạo 3 layers

```sql
-- Trong Dremio SQL Runner (http://localhost:9047)

-- 1. Kiểm tra bronze data
SELECT COUNT(*) as total_rows FROM "minio-napas"."napas-datalake"."bronze"."transactions"."date=2026-06-19";
-- Expected: 1000

-- 2. Tạo Silver layer (clean và validate)
CREATE OR REPLACE VIEW silver.transactions_clean AS
SELECT DISTINCT
  txn_id,
  CAST(amount AS DECIMAL(18,2))              AS amount,
  bank_id,
  merchant_id,
  CAST(created_at AS TIMESTAMP)              AS created_at,
  REGEXP_REPLACE(card_number, '[0-9](?=[0-9]{4})', '*') AS card_number_masked,
  status_code
FROM "minio-napas"."napas-datalake"."bronze"."transactions"."date=2026-06-19"
WHERE status_code = '00'
  AND txn_id IS NOT NULL
  AND amount > 0;

SELECT COUNT(*) FROM silver.transactions_clean;
-- Expected: ~950 (loại bỏ status_code != '00')

-- 3. Tạo Gold layer (aggregation)
CREATE OR REPLACE VIEW gold.txn_summary_by_bank AS
SELECT
  bank_id,
  DATE_TRUNC('day', created_at)  AS txn_date,
  COUNT(*)                        AS total_count,
  SUM(amount)                     AS total_amount,
  AVG(amount)                     AS avg_amount,
  COUNT(DISTINCT merchant_id)     AS unique_merchants
FROM silver.transactions_clean
GROUP BY bank_id, DATE_TRUNC('day', created_at);

SELECT * FROM gold.txn_summary_by_bank ORDER BY total_amount DESC;
-- Expected: 5 rows (1 per bank), total_amount sorted desc
```

### 4.3. Bước C: Kết nối Superset với Dremio Gold Layer

```
1. Vào Superset: http://localhost:8088
2. Settings → Database Connections → + Database
   - SQLAlchemy URI: dremioflight://admin:Dremio%40Admin2024@localhost:32010/?UseEncryption=0
   - Test Connection → "Connection looks good!"

3. Datasets → + Dataset
   - Database: Dremio-Napas
   - Schema: gold
   - Table: txn_summary_by_bank

4. Charts → + Chart
   - Dataset: gold.txn_summary_by_bank
   - Chart type: Bar Chart
   - X-axis: bank_id
   - Metric: SUM(total_amount) [label: Tổng Giá Trị GD]
   - Run Query → thấy 5 bars (1 per bank)

5. Dashboards → + Dashboard
   - Title: "Tổng Quan Giao Dịch Napas"
   - Drag chart vào
   - Save & Publish
```

---

## 5. Luồng NiFi Tự Động Hàng Ngày

Sau khi có dữ liệu test, cấu hình NiFi để **tự động** chạy mỗi đêm:

```
NiFi UI → Process Groups → New → "NAPAS Daily Batch"

Processors cần thêm:
1. QueryDatabaseTable
   - DB Connection Pool: Oracle-Napas-Source (JDBC)
   - Table Name: TRANSACTIONS
   - Maximum-value Columns: CREATED_DATE
   - Schedule: 0 0 2 * * ? (2h sáng)

2. ConvertAvroToParquet
   - (no config needed)

3. UpdateAttribute
   - filename: transactions-${now():format('yyyy-MM-dd')}.parquet
   - s3.key: bronze/transactions/date=${now():format('yyyy-MM-dd')}/${UUID()}.parquet

4. PutS3Object
   - Object Key: ${s3.key}
   - Bucket: napas-datalake
   - Endpoint Override: http://minio.data-storage.svc.cluster.local:9000
   - Access Key: (từ Controller Service lấy qua Vault)
   - Secret Key: (từ Controller Service lấy qua Vault)
```

---

## 6. Cấu Hình Ranger Policies Cho Production

```bash
RANGER_URL="http://localhost:6080"
AUTH="admin:RangerAdmin@2024"

# Tạo Dremio service trong Ranger
curl -s -u $AUTH -X POST "$RANGER_URL/service/public/v2/api/service" \
  -H "Content-Type: application/json" -d '{
  "name": "napas-dremio",
  "type": "dremio",
  "configs": {
    "username": "admin",
    "password": "Dremio@Admin2024",
    "jdbc.driverClassName": "com.dremio.jdbc.Driver",
    "jdbc.url": "jdbc:dremio:direct=dremio.data-processing.svc.cluster.local:31010"
  }
}'

# Policy: Data Engineers — full access
curl -s -u $AUTH -X POST "$RANGER_URL/service/public/v2/api/policy" \
  -H "Content-Type: application/json" -d '{
  "name": "data-engineers-full",
  "service": "napas-dremio",
  "resources": {
    "schema": {"values": ["bronze","silver","gold"]},
    "table":  {"values": ["*"]},
    "column": {"values": ["*"]}
  },
  "policyItems": [{
    "groups": ["data-engineers"],
    "accesses": [{"type":"select","isAllowed":true},
                 {"type":"create","isAllowed":true}]
  }],
  "isEnabled": true
}'

# Policy: Analysts — chỉ Gold, không thấy card_number
curl -s -u $AUTH -X POST "$RANGER_URL/service/public/v2/api/policy" \
  -H "Content-Type: application/json" -d '{
  "name": "analysts-gold-only",
  "service": "napas-dremio",
  "resources": {
    "schema": {"values": ["gold"]},
    "table":  {"values": ["*"]},
    "column": {"values": ["bank_id","txn_date","total_count","total_amount","avg_amount"]}
  },
  "policyItems": [{"groups":["analysts"],"accesses":[{"type":"select","isAllowed":true}]}],
  "isEnabled": true
}'
```

---

## 7. Tổng Kết — Luồng Hoàn Chỉnh

```
┌─────────────────────────────────────────────────────────────┐
│              NAPAS DAILY DATA PIPELINE                       │
│                                                             │
│  2:00 AM  NiFi cron kích hoạt QueryDatabaseTable            │
│           → Oracle DB: SELECT txn WHERE date = YESTERDAY    │
│           → 500k rows → split thành batch 10k rows          │
│           → ConvertAvroToParquet                             │
│           → PutS3Object → MinIO bronze/                     │
│                                                             │
│  2:30 AM  Dremio Reflection Refresh (scheduled)             │
│           → Đọc bronze/ → apply Silver SQL transform        │
│           → Đọc silver/ → apply Gold SQL aggregation        │
│           → Rebuild Reflection cache                         │
│                                                             │
│  6:00 AM  Business Users mở Superset                        │
│           → Dashboard "Tổng Quan Giao Dịch" load            │
│           → Superset query Dremio Gold layer                │
│           → Dremio dùng Reflection → <1s response           │
│           → Chart hiển thị số liệu ngày hôm qua             │
│                                                             │
│  Bất kỳ lúc nào: Ranger enforce policies                    │
│  → Analyst chỉ thấy Gold, card_number bị mask              │
│  → Audit log mọi access attempt                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 8. Troubleshooting Thường Gặp

| Triệu chứng | Nguyên nhân | Giải pháp |
|-------------|-------------|-----------|
| Vault pod `0/1 NotReady` | Chưa unseal | `./scripts/init-vault.sh dev` |
| NiFi không ghi được MinIO | S3 credentials sai | Kiểm tra Controller Service trong NiFi |
| Dremio "Cannot connect to S3" | MinIO endpoint sai | Kiểm tra `fs.s3a.endpoint` trong source config |
| Superset "Connection refused" | Dremio chưa ready | Chờ thêm, hoặc restart Dremio coordinator |
| Ranger policies không apply | Plugin chưa kết nối | Kiểm tra Ranger service status |

```bash
# Debug command tổng quát
kubectl get pods --all-namespaces | grep -v Running
# → Mọi pods phải ở trạng thái Running

kubectl get events --all-namespaces --sort-by='.lastTimestamp' | tail -20
# → Tìm Warning events
```
