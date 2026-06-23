# Step 2: MinIO — Object Storage (S3-Compatible)

> **Deploy order:** 2/6
> **Namespace:** `data-storage`
> **Phụ thuộc:** Vault (để lưu MinIO credentials)

---

## 1. MinIO Là Gì?

MinIO là **high-performance object storage** tương thích 100% với Amazon S3 API. Đây là nền tảng lưu trữ trung tâm của Data Lake — toàn bộ dữ liệu (raw, cleaned, aggregated) đều nằm tại đây.

**Tại sao chọn MinIO thay vì SeaweedFS?**

| Tiêu chí           | MinIO                          | SeaweedFS                  |
|--------------------|--------------------------------|----------------------------|
| S3 API Compatibility | 100% — SDK S3 gốc hoạt động  | ~80% — một số API thiếu   |
| Hệ sinh thái       | Rất rộng (Spark, Dremio, NiFi, Trino...) | Hẹp hơn            |
| Community          | Lớn, tài liệu phong phú       | Nhỏ hơn                    |
| Production usage   | Netflix, Dropbox, Verizon...  | Ít case hơn               |
| paraline-platform  | ✅ Đang dùng MinIO             | ❌ Không dùng              |

---

## 2. Kiến Trúc MinIO

### 2.1. Thành Phần Cốt Lõi

```
┌─────────────────────────────────────────────────────────────────┐
│                     MinIO Server                                 │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    S3-compatible API Layer               │   │
│  │  Endpoint: :9000                                         │   │
│  │  Protocols: HTTP/HTTPS + WebSocket (event notifications) │   │
│  └─────────────────────────────┬────────────────────────────┘   │
│                                │                                 │
│  ┌─────────────────────────────▼────────────────────────────┐   │
│  │                   Erasure Coding Engine                   │   │
│  │  Distributed mode: dữ liệu được chia nhỏ + redundancy   │   │
│  │  N drives: N/2 parity → chịu được N/2 ổ cứng hỏng       │   │
│  └─────────────────────────────┬────────────────────────────┘   │
│                                │                                 │
│  ┌─────────────────────────────▼────────────────────────────┐   │
│  │                   Storage Backend                         │   │
│  │  Local filesystem (ổ cứng / SSD / NVMe / NFS)           │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │               Admin Console: :9001                        │   │
│  │  Web UI để quản lý buckets, users, policies              │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2. Deployment Modes

| Mode          | Khi nào dùng   | Cấu hình                              |
|---------------|----------------|---------------------------------------|
| **Standalone**    | Local dev      | 1 pod, 1 volume, không HA             |
| **Distributed**   | Production     | 4+ pods, N drives each, erasure coding|

### 2.3. Bucket Structure cho NAPAS

```
MinIO Server (:9000)
└── Bucket: napas-datalake/
    │
    ├── bronze/                           ← NiFi ghi vào đây
    │   └── transactions/
    │       └── date=2026-06-19/
    │           ├── part-00001.parquet
    │           └── part-00002.parquet
    │
    ├── silver/                           ← Dremio CTAS ghi vào đây
    │   └── transactions_clean/
    │       └── date=2026-06-19/
    │           └── part-00001.parquet
    │
    └── gold/                             ← Dremio aggregation ghi vào đây
        └── txn_summary_by_bank/
            └── date=2026-06-19/
                └── part-00001.parquet
```

---

## 3. MinIO Hoạt Động Như Thế Nào?

### 3.1. Object Storage vs Block Storage

```
Block Storage (HDFS, ổ cứng)        Object Storage (MinIO/S3)
─────────────────────────────        ──────────────────────────
Tư duy: thư mục / file tree         Tư duy: flat bucket + key
/data/transactions/2026/06/19/       napas-datalake/bronze/txn/date=2026-06-19/
part-001.parquet                      part-001.parquet

PUT /data/file                        PUT bucket/key
Phải mount filesystem                 Chỉ cần HTTP call
Khó scale ngang                       Scale ngang dễ dàng
```

### 3.2. NiFi → MinIO (Ghi dữ liệu)

```
NiFi (PutS3Object Processor)
  │
  │  HTTP PUT /napas-datalake/bronze/transactions/date=2026-06-19/part-001.parquet
  │  Headers: x-amz-content-sha256, Authorization (AWS Sig V4)
  │
  ▼
MinIO S3 API (:9000)
  │
  │  Validate signature, check ACL
  │
  ▼
Local filesystem: /data/napas-datalake/bronze/transactions/...
```

### 3.3. Dremio → MinIO (Đọc dữ liệu)

```
Dremio SQL Engine
  │
  │  SELECT * FROM s3.bronze.transactions WHERE date='2026-06-19'
  │
  ▼
Dremio S3 Source Connector
  │
  │  HTTP GET /napas-datalake/bronze/transactions/date=2026-06-19/*.parquet
  │  (parallel multi-part download nếu file lớn)
  │
  ▼
MinIO → trả về Parquet bytes
  │
  ▼
Dremio đọc Parquet column-by-column (columnar pushdown)
```

---

## 4. Tương Tác Với Các Component Khác

```
NiFi    ──(PutS3Object)──▶  MinIO ◀──(S3 Read)──  Dremio
                               │
                               │
                    MinIO Event Notifications
                    (bucket notifications → webhook)
                    → có thể trigger NiFi flow khi file đến
```

| Component | Tương tác với MinIO              | Protocol         |
|-----------|----------------------------------|------------------|
| NiFi      | Ghi raw data vào bronze/         | S3 API (PutS3Object) |
| Dremio    | Đọc/ghi tất cả 3 layers          | S3 API (GetObject, ListObjects) |
| Superset  | Không trực tiếp (qua Dremio)     | —                |
| Vault     | Cung cấp access_key/secret_key   | REST API         |

---

## 5. Ưu Điểm & Nhược Điểm

| Ưu điểm                                     | Nhược điểm                               |
|---------------------------------------------|------------------------------------------|
| S3 API 100% compatible — drop-in replacement | Không phải managed service (tự vận hành)|
| Hiệu năng cao: lên tới 325 GiB/s read       | Distributed mode cần tối thiểu 4 nodes  |
| Erasure coding: chịu lỗi N/2 drives         | Không có POSIX filesystem (không mount được như NFS) |
| MinIO Console (Web UI) đẹp, dễ dùng         | Versioning objects tốn dung lượng gấp đôi |
| Tích hợp native với mọi tool Big Data        |                                          |
| Self-hosted: dữ liệu không ra ngoài          |                                          |

---

## 6. Cài Đặt — Helm Values

### 6.1. Base values

```yaml
# platform/values/base/minio.yaml
image:
  repository: quay.io/minio/minio
  tag: RELEASE.2024-06-04T19-20-08Z
  pullPolicy: IfNotPresent

# Mode: standalone (dev) hoặc distributed (prod)
mode: standalone

# Root credentials — sẽ override từ env (lấy từ Vault trong prod)
rootUser: "napas-admin"
rootPassword: "napas-minio-s3cr3t-2024"

# Resources
resources:
  requests:
    memory: "512Mi"
    cpu: "250m"
  limits:
    memory: "2Gi"
    cpu: "1000m"

# Persistence
persistence:
  enabled: true
  storageClass: "platform-standard"
  size: 10Gi

# Service
service:
  type: ClusterIP
  port: 9000

# Console (Admin UI)
consoleService:
  type: ClusterIP
  port: 9001

# Tạo buckets tự động khi khởi động
buckets:
  - name: napas-datalake
    policy: none    # Private — chỉ authenticated users
    purge: false

# Tạo users (service accounts)
users:
  - accessKey: nifi-user
    secretKey: nifi-s3-secret-2024
    policy: readwrite

  - accessKey: dremio-user
    secretKey: dremio-s3-secret-2024
    policy: readwrite

# Environment
environment:
  MINIO_BROWSER_REDIRECT_URL: ""
  MINIO_PROMETHEUS_AUTH_TYPE: "public"
```

### 6.2. Environment overrides

```yaml
# platform/values/env/minio.yaml.gotmpl
mode: {{ .Values.minio.mode }}

persistence:
  size: {{ .Values.minio.storage }}

service:
  type: {{ .Values.minio.serviceType }}

{{ if .Values.minio.useExistingSecret }}
existingSecret: minio-credentials
{{ else }}
rootUser: {{ .Values.minio.auth.rootUser }}
rootPassword: {{ .Values.minio.auth.rootPassword }}
{{ end }}

resources:
  requests:
    cpu: {{ .Values.minio.resources.requests.cpu | quote }}
    memory: {{ .Values.minio.resources.requests.memory | quote }}
  limits:
    cpu: {{ .Values.minio.resources.limits.cpu | quote }}
    memory: {{ .Values.minio.resources.limits.memory | quote }}
```

### 6.3. Dev environment values

```yaml
# platform/environments/dev.yaml (phần minio)
minio:
  mode: standalone
  storage: 20Gi
  serviceType: NodePort
  auth:
    rootUser: napas-admin
    rootPassword: "napas-minio-s3cr3t-2024"
    useExistingSecret: false
  resources:
    requests: { cpu: "250m", memory: "512Mi" }
    limits:   { cpu: "1",    memory: "2Gi"   }
```

---

## 7. Deploy MinIO

### 7.1. Thêm vào Helmfile

```yaml
# platform/helmfile.yaml.gotmpl (layer 02-storage)
- name: minio
  namespace: data-storage
  chart: minio/minio
  version: "5.2.0"
  values:
    - values/base/minio.yaml
    - values/env/minio.yaml.gotmpl
  labels:
    layer: 02-storage
```

### 7.2. Thêm Helm repo và Deploy

```bash
# Thêm MinIO Helm repo
helm repo add minio https://charts.min.io/
helm repo update

# Deploy
./scripts/deploy.sh dev 02-storage

# Kiểm tra
kubectl get pods -n data-storage
# NAME                     READY   STATUS    RESTARTS   AGE
# minio-6d4f5c9d8b-xxxx   1/1     Running   0          2m
```

---

## 8. Cấu Hình Sau Deploy

```bash
# Port-forward để access local
kubectl port-forward -n data-storage svc/minio 9000:9000 &
kubectl port-forward -n data-storage svc/minio-console 9001:9001 &

# Cài MinIO client (mc)
# Windows: winget install MinIO.mc
# Linux:   wget https://dl.min.io/client/mc/release/linux-amd64/mc && chmod +x mc

# Cấu hình mc kết nối local
mc alias set napas-local http://localhost:9000 napas-admin napas-minio-s3cr3t-2024

# Tạo bucket structure
mc mb napas-local/napas-datalake
mc mb napas-local/napas-datalake/bronze
mc mb napas-local/napas-datalake/silver
mc mb napas-local/napas-datalake/gold

# Bật versioning (quan trọng cho bronze — không bao giờ xóa raw data)
mc version enable napas-local/napas-datalake

# Kiểm tra bucket đã tạo
mc ls napas-local/napas-datalake/
```

---

## 9. Validate MinIO Hoạt Động Đúng

```bash
# ✅ Check 1: Pod READY
kubectl get pods -n data-storage
# Expected: minio-xxxx   1/1   Running

# ✅ Check 2: S3 API health
kubectl port-forward -n data-storage svc/minio 9000:9000 &
curl -s http://localhost:9000/minio/health/live
# Expected: HTTP 200 OK (response body rỗng là đúng)

# ✅ Check 3: Upload và download object thử
echo "Hello NAPAS Data Platform" > /tmp/test-file.txt
mc cp /tmp/test-file.txt napas-local/napas-datalake/bronze/test-file.txt
mc ls napas-local/napas-datalake/bronze/
# Expected: [date] [size] test-file.txt

mc cat napas-local/napas-datalake/bronze/test-file.txt
# Expected: Hello NAPAS Data Platform

# ✅ Check 4: Xóa test file
mc rm napas-local/napas-datalake/bronze/test-file.txt

# ✅ Check 5: Console UI accessible
# Mở browser: http://localhost:9001
# Login: napas-admin / napas-minio-s3cr3t-2024
# Kiểm tra: Buckets > napas-datalake tồn tại

# ✅ Check 6: Kết nối từ trong cluster (simulate NiFi)
kubectl run minio-test \
  --image=minio/mc:latest \
  --rm -it \
  --restart=Never \
  -n data-ingestion \
  -- sh -c '
    mc alias set napas http://minio.data-storage.svc.cluster.local:9000 \
      napas-admin napas-minio-s3cr3t-2024
    mc ls napas/napas-datalake/
  '
# Expected: bronze/  silver/  gold/
```

### Kết Quả Validate Thành Công

```
[✅] minio pod: 1/1 Running
[✅] S3 API health: HTTP 200
[✅] Upload/Download object: OK
[✅] napas-datalake bucket tồn tại
[✅] bronze/silver/gold folders tồn tại
[✅] In-cluster access: OK
[✅] Console UI: accessible
→ MinIO sẵn sàng — Proceed to Step 3: Apache Ranger
```

---

## 10. Lưu Ý Quan Trọng

```
⚠️  Standalone mode (dev) = KHÔNG HA. Nếu pod crash, data sẽ mất
    nếu PVC bị xóa. Đảm bảo dùng reclaimPolicy: Retain cho prod.

⚠️  Bronze layer KHÔNG BAO GIỜ xóa data. Đây là source of truth.
    Dùng Object Lifecycle Policy để move sang cheaper storage sau 1 năm.

💡  Dùng mc mirror để sync data từ MinIO prod → MinIO dev khi cần
    debug với real data (anonymized trước):
    mc mirror prod/napas-datalake/bronze/date=2026-06-19 \
              local/napas-datalake/bronze/date=2026-06-19

💡  MinIO tương thích AWS SDK — mọi code Python dùng boto3 với endpoint
    override sẽ hoạt động ngay không cần sửa:
    s3 = boto3.client('s3',
      endpoint_url='http://minio.data-storage.svc.cluster.local:9000',
      aws_access_key_id='napas-admin',
      aws_secret_access_key='napas-minio-s3cr3t-2024'
    )
```
