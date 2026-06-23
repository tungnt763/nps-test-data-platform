# Step 8: NiFi Pipeline — Hướng Dẫn Chi Tiết End-to-End

> Pipeline hoàn chỉnh: PostgreSQL (source) → NiFi (ETL) → MinIO (Bronze Layer)
>
> Dùng PostgreSQL có sẵn trong cluster làm database giả lập thay Oracle để demo.

---

## Tổng Quan Pipeline

```
┌──────────────┐     ┌──────────────────────────────────┐     ┌──────────────┐
│  PostgreSQL  │     │           Apache NiFi             │     │    MinIO      │
│  (Source DB) │────▶│                                   │────▶│ (Bronze Layer)│
│              │     │  GenerateFlowFile                 │     │              │
│ transactions │     │       ↓                           │     │ napas-datalake│
│   table      │     │  ExecuteSQL                       │     │  /bronze/    │
│              │     │       ↓                           │     │   /txn/      │
│              │     │  ConvertAvroToJSON (debug)        │     │              │
│              │     │       ↓                           │     │              │
│              │     │  PutS3Object                      │     │              │
└──────────────┘     └──────────────────────────────────┘     └──────────────┘
```

**Giải thích luồng:**
1. **GenerateFlowFile** — Tạo trigger theo lịch (thay vì dùng cron bên ngoài)
2. **ExecuteSQL** — Kết nối PostgreSQL, chạy query lấy transactions
3. **ConvertAvroToJSON** — Convert kết quả từ Avro (output mặc định của ExecuteSQL) sang JSON để debug
4. **PutS3Object** — Upload file lên MinIO bucket bronze layer

---

## Bước 0: Chuẩn Bị — Tạo Database Giả Lập

Dùng PostgreSQL `postgres-superset` (đang chạy trong namespace `data-visualization`) để tạo bảng demo.

### 0.1. Port-forward PostgreSQL

```bash
kubectl port-forward -n data-visualization svc/postgres-superset-postgresql 5432:5432
```

### 0.2. Tạo bảng và seed data

Mở terminal khác, chạy:

```bash
PGPASSWORD="SupersetPostgres2024" psql -h localhost -U superset -d superset -c "
CREATE TABLE IF NOT EXISTS transactions (
    txn_id         VARCHAR(20) PRIMARY KEY,
    bank_id        VARCHAR(10) NOT NULL,
    amount         NUMERIC(18,2) NOT NULL,
    merchant_id    VARCHAR(20) NOT NULL,
    status_code    VARCHAR(5) NOT NULL,
    created_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    card_number    VARCHAR(30)
);

INSERT INTO transactions (txn_id, bank_id, amount, merchant_id, status_code, created_at, card_number)
SELECT
    'TXN' || LPAD(g::text, 8, '0'),
    (ARRAY['VCB','BIDV','TCB','MB','VPB'])[1 + (random()*4)::int],
    ROUND((random() * 5000000 + 10000)::numeric, 2),
    'MERCH' || LPAD((1 + (random()*99)::int)::text, 4, '0'),
    CASE WHEN random() > 0.05 THEN '00' ELSE '51' END,
    NOW() - INTERVAL '1 day' + (random() * 86400 || ' seconds')::interval,
    '****-****-****-' || LPAD((1000 + (random()*8999)::int)::text, 4, '0')
FROM generate_series(1, 1000) g
ON CONFLICT (txn_id) DO NOTHING;

SELECT COUNT(*) AS total_rows FROM transactions;
"
```

**Kết quả mong đợi:** `total_rows = 1000`

> **Nếu không có psql trên máy**, dùng kubectl exec:
> ```bash
> kubectl exec -n data-visualization postgres-superset-postgresql-0 -- \
>   env PGPASSWORD="SupersetPostgres2024" psql -U superset -d superset -c "<SQL ở trên>"
> ```

---

## Bước 1: Mở NiFi UI

```bash
# Port-forward NiFi (nếu chưa)
kubectl port-forward -n data-ingestion svc/nifi 8443:8443
```

Truy cập: **https://localhost:8443/nifi/**
- Chấp nhận self-signed certificate
- Login: `admin` / `NiFiAdmin@2024`

---

## Bước 2: Tạo Process Group

Process Group giúp tổ chức pipeline gọn gàng, dễ quản lý.

1. Kéo icon **Process Group** (hình vuông có mũi tên) từ toolbar xuống canvas
2. Đặt tên: **`NAPAS-Bronze-Ingestion`**
3. Double-click vào Process Group để vào bên trong

---

## Bước 3: Cấu Hình Controller Services

Controller Services là các service dùng chung cho nhiều processors (VD: DB connection pool).

### 3.1. Mở Controller Services

1. Click chuột phải lên canvas → **Configure** (hoặc click icon bánh răng ⚙️ bên trái)
2. Chọn tab **Controller Services**
3. Click dấu **+** để thêm service mới

### 3.2. Tạo DBCPConnectionPool (kết nối PostgreSQL)

1. Tìm **DBCPConnectionPool** → click **Add**
2. Click icon bánh răng ⚙️ để configure:

| Property | Value | Giải thích |
|----------|-------|------------|
| **Database Connection URL** | `jdbc:postgresql://postgres-superset-postgresql.data-visualization.svc.cluster.local:5432/superset` | JDBC URL trỏ đến PostgreSQL qua K8s DNS nội bộ |
| **Database Driver Class Name** | `org.postgresql.Driver` | Driver class cho PostgreSQL |
| **Database Driver Location(s)** | `/opt/nifi/nifi-current/lib/postgresql-42.7.2.jar` | Path đến JDBC driver (xem bước 3.3) |
| **Database User** | `superset` | Username PostgreSQL |
| **Password** | `SupersetPostgres2024` | Password PostgreSQL |

3. Click **Apply** → **Close**
4. Click icon ⚡ (lightning bolt) bên phải service → **Enable**

### 3.3. Upload PostgreSQL JDBC Driver vào NiFi

NiFi cần JDBC driver để kết nối PostgreSQL. Download và copy vào pod:

```bash
# Download PostgreSQL JDBC driver
curl -L -o postgresql-42.7.2.jar \
  https://jdbc.postgresql.org/download/postgresql-42.7.2.jar

# Copy vào NiFi pod
kubectl cp postgresql-42.7.2.jar \
  data-ingestion/nifi-0:/opt/nifi/nifi-current/lib/postgresql-42.7.2.jar
```

> **Quan trọng:** Sau khi copy driver, cần **restart NiFi** để nó load driver mới:
> ```bash
> kubectl delete pod nifi-0 -n data-ingestion
> kubectl wait pod/nifi-0 -n data-ingestion --for=condition=Ready --timeout=180s
> ```
> Sau đó re-enable DBCPConnectionPool service.

---

## Bước 4: Tạo Processors

Quay lại canvas của Process Group. Kéo từng processor từ toolbar (icon hình processor) vào canvas.

### 4.1. GenerateFlowFile (Trigger)

**Mục đích:** Tạo FlowFile rỗng theo lịch, kích hoạt pipeline chạy.

1. Kéo **Processor** icon vào canvas → tìm **GenerateFlowFile** → **Add**
2. Double-click để configure:

**Tab SCHEDULING:**

| Property | Value | Giải thích |
|----------|-------|------------|
| **Run Schedule** | `60 sec` | Chạy mỗi 60 giây (demo). Production: dùng CRON `0 0 2 * * ?` (2h sáng) |
| **Execution** | `Primary Node` | Chỉ chạy trên 1 node, tránh duplicate |

**Tab PROPERTIES:**

| Property | Value | Giải thích |
|----------|-------|------------|
| **File Size** | `0B` | FlowFile rỗng, chỉ làm trigger |
| **Batch Size** | `1` | Mỗi lần trigger tạo 1 FlowFile |
| **Custom Text** | (để trống) | Không cần nội dung |

3. Click **Apply**

---

### 4.2. ExecuteSQL (Đọc dữ liệu từ PostgreSQL)

**Mục đích:** Chạy SQL query lấy transactions từ PostgreSQL. Output là Avro format.

1. Kéo **Processor** → tìm **ExecuteSQL** → **Add**
2. Double-click configure:

**Tab PROPERTIES:**

| Property | Value | Giải thích |
|----------|-------|------------|
| **Database Connection Pooling Service** | `DBCPConnectionPool` | Chọn connection pool đã tạo ở Bước 3 |
| **SQL select query** | (xem bên dưới) | Query lấy transactions |

**SQL Query:**

```sql
SELECT txn_id, bank_id, amount, merchant_id, status_code, created_at, card_number
FROM transactions
WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
  AND created_at < CURRENT_DATE
ORDER BY created_at
```

> **Giải thích:** Lấy tất cả transactions của ngày hôm qua. Trong production, thay `CURRENT_DATE` bằng parameter.

3. Click **Apply**

---

### 4.3. ConvertAvroToJSON (Debug / Optional)

**Mục đích:** Convert Avro → JSON để dễ kiểm tra kết quả. Trong production có thể bỏ bước này.

1. Kéo **Processor** → tìm **ConvertAvroToJSON** → **Add**
2. Double-click configure:

**Tab PROPERTIES:**

| Property | Value | Giải thích |
|----------|-------|------------|
| **JSON container options** | `NONE` | Output mỗi record là 1 JSON object |
| **Wrap Single Record** | `false` | Không wrap trong array nếu chỉ có 1 record |

3. Click **Apply**

---

### 4.4. UpdateAttribute (Đặt tên file + S3 key)

**Mục đích:** Gán tên file và đường dẫn S3 cho FlowFile trước khi upload.

1. Kéo **Processor** → tìm **UpdateAttribute** → **Add**
2. Double-click configure:

**Tab PROPERTIES** → click dấu **+** (góc phải trên) để thêm custom properties:

| Property (tự thêm) | Value | Giải thích |
|---------------------|-------|------------|
| **filename** | `transactions-${now():format('yyyy-MM-dd-HHmmss')}.json` | Tên file với timestamp |
| **s3.key** | `bronze/transactions/date=${now():format('yyyy-MM-dd')}/${filename}` | Đường dẫn trên S3/MinIO theo partition date |

> **Giải thích NiFi Expression Language:**
> - `${now():format('yyyy-MM-dd')}` → output: `2026-06-22`
> - `${filename}` → tham chiếu attribute filename vừa set ở trên
> - Kết quả: `bronze/transactions/date=2026-06-22/transactions-2026-06-22-020000.json`

3. Click **Apply**

---

### 4.5. PutS3Object (Upload lên MinIO)

**Mục đích:** Upload FlowFile lên MinIO bucket (S3-compatible).

1. Kéo **Processor** → tìm **PutS3Object** → **Add**
2. Double-click configure:

**Tab PROPERTIES:**

| Property | Value | Giải thích |
|----------|-------|------------|
| **Object Key** | `${s3.key}` | Đường dẫn S3, lấy từ attribute đã set |
| **Bucket** | `napas-datalake` | Tên bucket MinIO |
| **Access Key ID** | `napas-admin` | MinIO access key |
| **Secret Access Key** | `napas-minio-s3cr3t-2024` | MinIO secret key |
| **Endpoint Override URL** | `http://minio.data-storage.svc.cluster.local:9000` | MinIO endpoint (K8s internal DNS) |
| **Signer Override** | `AWSS3V4SignerType` | Bắt buộc cho MinIO compatibility |
| **Region** | `us-east-1` | Bắt buộc (MinIO default region) |

> **Lưu ý:** Không dùng `localhost` cho endpoint vì NiFi chạy trong K8s cluster,
> phải dùng DNS nội bộ `minio.data-storage.svc.cluster.local`.

3. Click **Apply**

---

## Bước 5: Kết Nối Processors

Kéo mũi tên từ output của processor này sang input processor tiếp theo:

### 5.1. GenerateFlowFile → ExecuteSQL

1. Di chuột vào giữa **GenerateFlowFile**, xuất hiện mũi tên
2. Kéo sang **ExecuteSQL**
3. Chọn relationship: **`success`** → **Add**

### 5.2. ExecuteSQL → ConvertAvroToJSON

1. Kéo mũi tên từ **ExecuteSQL** → **ConvertAvroToJSON**
2. Chọn relationship: **`success`** → **Add**

### 5.3. ExecuteSQL — Auto-terminate `failure`

1. Double-click **ExecuteSQL** → tab **RELATIONSHIPS**
2. Tick **Automatically Terminate** cho relationship `failure`

### 5.4. ConvertAvroToJSON → UpdateAttribute

1. Kéo mũi tên → chọn **`success`** → **Add**

### 5.5. ConvertAvroToJSON — Auto-terminate `failure`

1. Tick **Automatically Terminate** cho `failure`

### 5.6. UpdateAttribute → PutS3Object

1. Kéo mũi tên → chọn **`success`** → **Add**

### 5.7. PutS3Object — Auto-terminate `success` và `failure`

1. Double-click **PutS3Object** → tab **RELATIONSHIPS**
2. Tick **Automatically Terminate** cho cả `success` và `failure`

> PutS3Object là processor cuối cùng. Trong production, bạn sẽ route `failure` đến
> một LogAttribute hoặc PutEmail processor để alert khi upload thất bại.

### Sơ đồ kết nối hoàn chỉnh:

```
GenerateFlowFile ──success──▶ ExecuteSQL ──success──▶ ConvertAvroToJSON
                                  │                        │
                              failure(auto)            failure(auto)
                                                           │
                                                       success
                                                           ▼
                                                    UpdateAttribute
                                                           │
                                                       success
                                                           ▼
                                                     PutS3Object
                                                      │        │
                                                success(auto) failure(auto)
```

---

## Bước 6: Start Pipeline

### 6.1. Start tất cả processors

1. Click chuột phải trên canvas (khoảng trống) → **Start**
2. Tất cả processors sẽ chuyển sang icon ▶ (play) màu xanh

### 6.2. Monitor

Theo dõi trên canvas:
- **In/Out** trên mỗi connection hiển thị số FlowFiles đang queue
- **Tasks/Time** trên processor hiển thị số lần chạy
- Click chuột phải processor → **View status history** để xem metrics

### 6.3. Kiểm tra kết quả

```bash
# Kiểm tra file đã upload lên MinIO
mc ls napas-local/napas-datalake/bronze/transactions/ --recursive

# Expected output:
# [2026-06-22 15:00:01 +07]  XXX KiB date=2026-06-22/transactions-2026-06-22-150000.json
```

---

## Bước 7: Debug Khi Gặp Lỗi

### 7.1. Xem FlowFile content

1. Click vào connection (đường nối giữa 2 processors)
2. Click **List queue**
3. Click icon 👁 (eye) trên FlowFile → xem content
4. Tab **Attributes** → xem metadata

### 7.2. Xem Bulletin Board

1. Menu **Hamburger** (☰) góc trái trên → **Bulletin Board**
2. Hiển thị tất cả warnings/errors từ processors

### 7.3. Check NiFi logs

```bash
kubectl logs nifi-0 -n data-ingestion --tail=50 | grep -i error
```

### 7.4. Lỗi thường gặp

| Lỗi | Nguyên nhân | Fix |
|-----|-------------|-----|
| `Cannot create PoolableConnectionFactory` | JDBC driver thiếu hoặc sai URL | Kiểm tra driver đã copy + restart NiFi |
| `Access Denied` khi PutS3Object | Sai credentials MinIO | Kiểm tra Access Key/Secret Key |
| `No results` từ ExecuteSQL | Query không match data | Chạy query trực tiếp trong psql để test |
| `Connection refused` đến PostgreSQL | Service DNS sai | Verify: `kubectl get svc -n data-visualization` |

---

## Bước 8: Verify End-to-End

Sau khi pipeline chạy 1-2 phút, verify toàn bộ:

```bash
# 1. Check MinIO — có file mới không?
mc ls napas-local/napas-datalake/bronze/transactions/ --recursive

# 2. Check Dremio — query được data mới không?
# Vào Dremio UI → SQL Runner:
# SELECT COUNT(*) FROM "minio-datalake"."bronze"."transactions"

# 3. Check NiFi stats
# NiFi UI → Process Group "NAPAS-Bronze-Ingestion"
# → Xem counters: FlowFiles In/Out, Bytes Read/Written
```

---

## Tóm Tắt Cấu Hình

| Component | Config Key | Value |
|-----------|-----------|-------|
| **Source DB** | Host | `postgres-superset-postgresql.data-visualization.svc.cluster.local` |
| | Port | `5432` |
| | Database | `superset` |
| | User / Pass | `superset` / `SupersetPostgres2024` |
| **MinIO Target** | Endpoint | `http://minio.data-storage.svc.cluster.local:9000` |
| | Bucket | `napas-datalake` |
| | Path prefix | `bronze/transactions/date=YYYY-MM-DD/` |
| | Access / Secret | `napas-admin` / `napas-minio-s3cr3t-2024` |
| **NiFi Schedule** | Demo | `60 sec` |
| | Production | `0 0 2 * * ?` (CRON, 2:00 AM daily) |
