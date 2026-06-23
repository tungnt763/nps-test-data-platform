# Step 4: Apache NiFi — Ingestion & Orchestration

> **Deploy order:** 4/6
> **Namespace:** `data-ingestion`
> **Phụ thuộc:** Vault (credentials), MinIO (storage target)
> **Vai trò đặc biệt:** NiFi thay thế Airflow — đảm nhận cả Ingestion lẫn Orchestration

---

## 1. Apache NiFi Là Gì?

Apache NiFi là **data flow automation platform** — công cụ kéo-thả để xây dựng, quản lý và giám sát luồng dữ liệu. NiFi xử lý dữ liệu theo mô hình **"data-in-motion"**: dữ liệu liên tục chạy qua các processor.

**Tại sao NiFi đảm nhận cả Orchestration (thay Airflow)?**

```
Airflow:          DAG Python → Task → Task → Task (batch job orchestrator)
                  Mạnh về: complex dependencies, retries, SLA
                  Nhược: cần viết code Python, nặng hơn

NiFi:             Flow → Processor → Processor → Processor (data-centric)
                  Mạnh về: data movement, transformation, scheduling built-in
                  + Retry, backpressure, monitoring đều có sẵn
                  → Đủ cho scope dự án batch ingestion của Napas
```

---

## 2. Kiến Trúc NiFi

### 2.1. Thành Phần Cốt Lõi

```
┌──────────────────────────────────────────────────────────────────────┐
│                         Apache NiFi                                   │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │                    Web UI (:8443 HTTPS)                          │ │
│  │  Drag & drop flow designer                                       │ │
│  │  Real-time monitoring, provenance                                │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│                                                                       │
│  ┌────────────────────┐    ┌────────────────────────────────────┐    │
│  │   FlowFile         │    │   Processor                         │    │
│  │                    │    │                                     │    │
│  │  Content:          │───▶│  QueryDatabaseTable                │    │
│  │  (raw bytes)       │    │  → Kết nối Oracle, chạy SQL        │    │
│  │                    │    │  → Sinh ra FlowFile mỗi batch row  │    │
│  │  Attributes:       │    └───────────────┬────────────────────┘    │
│  │  - filename        │                    │                          │
│  │  - mime.type       │    ┌───────────────▼────────────────────┐    │
│  │  - table.name      │    │  ConvertAvroToParquet               │    │
│  │  - timestamp       │    │  → Chuyển Avro bytes → Parquet     │    │
│  └────────────────────┘    └───────────────┬────────────────────┘    │
│                                            │                          │
│  ┌─────────────────────────────────────────▼──────────────────────┐ │
│  │  Process Group (nhóm processor thành 1 flow)                    │ │
│  │                                                                  │ │
│  │  [Oracle Source] → [Convert] → [Route on Success/Failure]       │ │
│  │                                     │           │                │ │
│  │                               [PutS3Object] [LogError]          │ │
│  └──────────────────────────────────────────────────────────────── ┘ │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │                    FlowFile Repository                           │ │
│  │  Lưu state của mọi FlowFile đang xử lý → đảm bảo exactly-once  │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│                                                                       │
│  ┌──────────────────┐  ┌────────────────────┐  ┌─────────────────┐  │
│  │  Provenance      │  │  Content Repository│  │  Connection Queue│  │
│  │  Repository      │  │  (raw bytes store) │  │  (backpressure)  │  │
│  └──────────────────┘  └────────────────────┘  └─────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
```

### 2.2. Các Khái Niệm Chính

| Khái niệm           | Giải thích                                                                 |
|---------------------|----------------------------------------------------------------------------|
| **FlowFile**        | Đơn vị dữ liệu trong NiFi. Gồm content (bytes) và attributes (metadata)  |
| **Processor**       | Xử lý FlowFile: đọc DB, chuyển đổi format, ghi S3, gửi HTTP...           |
| **Connection**      | Kết nối giữa 2 processors. Có queue với backpressure control              |
| **Process Group**   | Nhóm processors thành flow hoàn chỉnh (như 1 DAG trong Airflow)          |
| **Controller Service** | Shared service: JDBC Pool, S3 Client — dùng chung cho nhiều processors |
| **Provenance**      | Lịch sử đầy đủ của mỗi FlowFile: đến từ đâu, đi qua processor nào       |
| **Backpressure**    | Khi downstream chậm, queue đầy → upstream tự động dừng                   |

---

## 3. NiFi Đảm Nhận Orchestration Như Thế Nào?

### 3.1. Scheduling (thay Airflow Scheduler)

```
Mỗi Processor có scheduling riêng:

Processor: QueryDatabaseTable (Oracle)
├── Schedule strategy: CRON
├── Cron expression: 0 0 2 * * ?    (2h sáng mỗi ngày)
└── Concurrent tasks: 1              (không chạy song song)

Processor: PutS3Object (MinIO)
├── Schedule strategy: EVENT_DRIVEN  (chạy ngay khi có FlowFile)
└── Max concurrent: 4                (4 thread ghi song song)
```

### 3.2. Retry Logic (thay Airflow Retry)

```
[QueryDatabaseTable]
      │ success
      ▼
[PutS3Object]
      │              │
   success        failure
      │              │
      ▼              ▼
[LogSuccess]   [RetryFlowFile]
                    │
                    │ retryCount < 3? → back to PutS3Object
                    │ retryCount >= 3? → [LogError] + Alert
```

### 3.3. Monitoring (thay Airflow Web UI)

```
NiFi UI hiển thị real-time:
- FlowFiles queued tại mỗi connection
- Throughput: FlowFiles/second, bytes/second
- Errors: failed FlowFiles, processor errors
- Provenance: trace từng FlowFile từ source đến destination
```

---

## 4. Flow Design cho NAPAS

### 4.1. Full Batch Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│  Process Group: "NAPAS Daily Batch Ingestion"                        │
│                                                                      │
│  ┌──────────────────────┐                                            │
│  │ QueryDatabaseTable   │  Schedule: 0 0 2 * * ? (2h sáng)          │
│  │ (Oracle/MySQL)       │  SQL: SELECT * FROM transactions           │
│  │                      │       WHERE created_date = CURRENT_DATE-1  │
│  └──────────┬───────────┘                                            │
│             │ success (Avro format)                                   │
│             ▼                                                         │
│  ┌──────────────────────┐                                            │
│  │ SplitAvro            │  Tách 1 Avro batch → nhiều FlowFiles nhỏ  │
│  │                      │  (1 FlowFile = 10,000 rows)                │
│  └──────────┬───────────┘                                            │
│             │                                                         │
│             ▼                                                         │
│  ┌──────────────────────┐                                            │
│  │ ConvertAvroToParquet │  Chuyển Avro → Parquet columnar format     │
│  │                      │  (nhỏ hơn 3-5x, query nhanh hơn)          │
│  └──────────┬───────────┘                                            │
│             │                                                         │
│             ▼                                                         │
│  ┌──────────────────────┐                                            │
│  │ UpdateAttribute      │  Set S3 path:                              │
│  │                      │  key = bronze/transactions/                │
│  │                      │       date=${now():format('yyyy-MM-dd')}/  │
│  │                      │       part-${UUID()}.parquet               │
│  └──────────┬───────────┘                                            │
│             │                                                         │
│     ┌───────┴──────┐                                                 │
│     │ success      │ failure                                          │
│     ▼              ▼                                                  │
│  ┌──────────┐  ┌──────────────────┐                                  │
│  │PutS3Obj  │  │RetryFlowFile     │                                  │
│  │(MinIO)   │  │(max 3 retries)   │                                  │
│  │          │  └────────┬─────────┘                                  │
│  │Bucket:   │           │ exhausted                                   │
│  │napas-    │           ▼                                             │
│  │datalake  │     ┌──────────┐                                       │
│  │bronze/   │     │LogError  │                                       │
│  └──────────┘     │+ Alert   │                                       │
│                   └──────────┘                                        │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 5. Ưu Điểm & Nhược Điểm

| Ưu điểm                                        | Nhược điểm                                      |
|------------------------------------------------|-------------------------------------------------|
| Giao diện kéo-thả, không cần viết code        | Không mạnh bằng Airflow cho complex DAG dependencies |
| 300+ Processor có sẵn (DB, S3, HTTP, Kafka...) | Flow definition lưu trong XML, khó review trong Git |
| Built-in retry, backpressure, provenance       | NiFi Registry cần setup thêm cho version control  |
| Data lineage tracking mọi FlowFile            | Scale cluster phức tạp (NiFi Cluster với ZooKeeper)|
| Credentials lưu trong Vault — không hardcode  | Memory footprint cao cho large-scale deployments  |
| Exactly-once delivery guarantee               |                                                   |

---

## 6. Cài Đặt — Helm Values

### 6.1. Base values

```yaml
# platform/values/base/nifi.yaml
image:
  repository: apache/nifi
  tag: "1.25.0"
  pullPolicy: IfNotPresent

# Single-node deployment (dev)
replicaCount: 1

# NiFi configuration
nifi:
  # Security — TLS enabled
  security:
    # Dev: tự ký cert
    keystorePasswd: "napas-nifi-keystore-2024"
    truststorePasswd: "napas-nifi-truststore-2024"

  # Sensitive properties key (mã hóa credentials trong flow.xml)
  sensitivePropsKey: "NAPaSSensitiveKey2024MinLength12"

  # Admin user
  authentication:
    method: single-user
    username: "admin"
    password: "NiFiAdmin@2024"

# Resources
resources:
  requests:
    memory: "4Gi"
    cpu: "1000m"
  limits:
    memory: "8Gi"
    cpu: "2000m"

# Persistent volumes
persistence:
  enabled: true
  storageClass: "platform-standard"
  # Flow repository (lưu flow definition)
  flowRepo:
    size: 5Gi
  # Content repository (lưu FlowFile content)
  contentRepo:
    size: 20Gi
  # Provenance repository (lưu data lineage)
  provenanceRepo:
    size: 10Gi

service:
  type: ClusterIP
  httpsPort: 8443

# Environment variables
env:
  - name: NIFI_WEB_HTTPS_PORT
    value: "8443"
  - name: NIFI_JVM_HEAP_INIT
    value: "2g"
  - name: NIFI_JVM_HEAP_MAX
    value: "4g"
```

### 6.2. Environment overrides

```yaml
# platform/values/env/nifi.yaml.gotmpl
resources:
  requests:
    memory: {{ .Values.nifi.resources.requests.memory | quote }}
    cpu: {{ .Values.nifi.resources.requests.cpu | quote }}
  limits:
    memory: {{ .Values.nifi.resources.limits.memory | quote }}
    cpu: {{ .Values.nifi.resources.limits.cpu | quote }}
```

### 6.3. Dev environment values

```yaml
# platform/environments/dev.yaml (phần nifi)
nifi:
  resources:
    requests: { cpu: "500m", memory: "2Gi" }
    limits:   { cpu: "2",    memory: "4Gi" }
```

### 6.4. Helmfile configuration

```yaml
# platform/helmfile.yaml.gotmpl (layer 03-ingestion)
- name: nifi
  namespace: data-ingestion
  chart: dysnix/nifi
  version: "1.4.0"
  values:
    - values/base/nifi.yaml
    - values/env/nifi.yaml.gotmpl
  needs:
    - data-security/vault
    - data-storage/minio
  labels:
    layer: 03-ingestion
```

### 6.5. Deploy

```bash
./scripts/deploy.sh dev 03-ingestion

kubectl get pods -n data-ingestion -w
# NAME          READY   STATUS    RESTARTS   AGE
# nifi-0        1/1     Running   0          3m
```

---

## 7. Cấu Hình Controller Services (Sau Deploy)

```bash
# Port-forward
kubectl port-forward -n data-ingestion svc/nifi 8443:8443 &
# Mở browser: https://localhost:8443/nifi
# Login: admin / NiFiAdmin@2024
# Trust self-signed cert: proceed anyway
```

### 7.1. Tạo JDBC Connection Pool cho Oracle

**NiFi UI → Controller Settings → Controller Services → + Add**

```
Service: DBCPConnectionPool
Name: Oracle-Napas-Source

Properties:
  Database Connection URL: jdbc:oracle:thin:@oracle-host:1521:NAPASDB
  Database Driver Class: oracle.jdbc.OracleDriver
  Database Driver Location: /opt/nifi/nifi-current/drivers/ojdbc8.jar
  Database User: napas_reader
  Password: (lấy từ Vault)
```

### 7.2. Tạo S3 Connection cho MinIO

```
Service: AWSCredentialsProviderControllerService
Name: MinIO-Credentials

Properties:
  Access Key ID: napas-admin
  Secret Access Key: (lấy từ Vault)

→ Dùng service này trong PutS3Object processor:
  Endpoint Override URL: http://minio.data-storage.svc.cluster.local:9000
  Bucket: napas-datalake
  Object Key: bronze/transactions/date=${now():format('yyyy-MM-dd')}/${UUID()}.parquet
  Region: us-east-1    (MinIO không cần đúng region, để mặc định)
```

---

## 8. Validate NiFi Hoạt Động Đúng

```bash
# ✅ Check 1: Pod READY
kubectl get pods -n data-ingestion
# Expected: nifi-0   1/1   Running

# ✅ Check 2: UI accessible
kubectl port-forward -n data-ingestion svc/nifi 8443:8443 &
# Mở: https://localhost:8443/nifi
# Expected: NiFi Canvas load, login thành công

# ✅ Check 3: Controller Services healthy
# UI → Controller Settings → tất cả services ở trạng thái "Enabled" (lightning bolt icon)

# ✅ Check 4: Test đơn giản — GenerateFlowFile → PutS3Object
# Tạo flow test:
#   [GenerateFlowFile]  (generates 1 FlowFile với content "test")
#         │
#         ▼
#   [UpdateAttribute]   (set filename = "test-${UUID()}.txt")
#         │
#         ▼
#   [PutS3Object]       (bucket=napas-datalake, key=bronze/test/${filename})

# Chạy flow và kiểm tra MinIO:
mc ls napas-local/napas-datalake/bronze/test/
# Expected: 1 file với nội dung "test"

# ✅ Check 5: Provenance tracking
# NiFi UI → Menu → Provenance
# Expected: thấy event history của test FlowFile

# ✅ Check 6: Bulletin board không có errors
# NiFi UI → Menu → Summary → Bulleting Board
# Expected: No errors
```

### Kết Quả Validate Thành Công

```
[✅] nifi-0 pod: 1/1 Running
[✅] UI: accessible tại https://localhost:8443/nifi
[✅] Controller Services: Enabled
[✅] Test flow: GenerateFlowFile → PutS3Object hoạt động
[✅] File xuất hiện trong MinIO bronze/test/
[✅] Provenance: tracking hoạt động
[✅] Bulletin board: No errors
→ NiFi sẵn sàng — Proceed to Step 5: Dremio
```

---

## 9. Import Driver Oracle vào NiFi

```bash
# Oracle JDBC driver cần được thêm vào NiFi image
# Tải ojdbc8.jar từ Oracle (yêu cầu Oracle account)
# Sau đó copy vào NiFi pod:

kubectl cp ojdbc8.jar data-ingestion/nifi-0:/opt/nifi/nifi-current/drivers/ojdbc8.jar

# HOẶC: Tạo custom NiFi image với driver đã nhúng sẵn:
# Dockerfile:
# FROM apache/nifi:1.25.0
# COPY ojdbc8.jar /opt/nifi/nifi-current/drivers/
```

---

## 10. Lưu Ý

```
⚠️  NiFi lưu credentials trong flow.xml.gz dưới dạng mã hóa bằng
    sensitivePropsKey. Phải set key này TRƯỚC khi tạo bất kỳ credential nào.
    Thay đổi key = mất toàn bộ credentials đã lưu.

⚠️  NiFi không có hot-reload như Airflow. Thay đổi flow cần
    stop processor → edit → start lại.

💡  Dùng NiFi Registry để version control flows (commit/revert như Git).
    Setup NiFi Registry là bước nâng cao, không bắt buộc cho scope hiện tại.

💡  Process Group "NAPAS Daily Batch" có thể export thành template XML
    và import lại ở môi trường khác (UAT, Prod).
```
