# Step 3: Apache Ranger — Phân Quyền Tập Trung

> **Deploy order:** 3/6
> **Namespace:** `data-governance`
> **Phụ thuộc:** Vault (DB credentials), PostgreSQL (metadata store)

---

## 1. Apache Ranger Là Gì?

Apache Ranger là **centralized security framework** để quản lý quyền truy cập dữ liệu trên toàn bộ Data Platform. Thay vì mỗi service (Dremio, NiFi...) tự quản lý permission riêng, Ranger cung cấp **một giao diện duy nhất** để định nghĩa, enforce và audit tất cả access policies.

**Vấn đề Ranger giải quyết:**

```
❌ Không có Ranger:
   - Dremio có permission riêng
   - NiFi có permission riêng
   - MinIO có IAM riêng
   → Phân quyền bị rải rác, khó audit, dễ sai sót

✅ Có Ranger:
   - Định nghĩa policy 1 lần tại Ranger Admin
   - Ranger Plugin trong từng service enforce policy đó
   - Audit log tập trung: ai đọc gì, lúc nào, từ IP nào
```

---

## 2. Kiến Trúc Ranger

```
┌───────────────────────────────────────────────────────────────┐
│                    Apache Ranger                               │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │              Ranger Admin Server (:6080)                  │ │
│  │  Web UI + REST API để quản lý:                           │ │
│  │  • Services (Dremio, NiFi, MinIO...)                     │ │
│  │  • Policies (ai được phép làm gì)                        │ │
│  │  • Users & Groups (đồng bộ từ LDAP/AD)                   │ │
│  │  • Audit logs viewer                                      │ │
│  └─────────────────────┬───────────────────────────────────┘ │
│                         │ Policy distribution (pull)          │
│         ┌───────────────┼──────────────────────┐             │
│         │               │                      │             │
│         ▼               ▼                      ▼             │
│  ┌────────────┐  ┌────────────┐  ┌──────────────────────┐   │
│  │  Ranger    │  │  Ranger    │  │  Ranger              │   │
│  │  Plugin    │  │  Plugin    │  │  Plugin              │   │
│  │  (Dremio)  │  │  (NiFi)   │  │  (MinIO/S3)          │   │
│  └────────────┘  └────────────┘  └──────────────────────┘   │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │              Ranger Audit                                 │ │
│  │  Lưu audit logs vào: Solr / HDFS / Elasticsearch         │ │
│  │  Mỗi lần access dữ liệu → 1 audit record                │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │              PostgreSQL (Ranger Metadata)                 │ │
│  │  Lưu: policies, users, groups, service defs              │ │
│  └─────────────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────┘
```

### Các Thành Phần

| Thành phần       | Mô tả                                                           |
|------------------|-----------------------------------------------------------------|
| **Ranger Admin** | Web UI + REST API — nơi admin define policies                   |
| **Ranger Plugin**| Thư viện nhúng vào service (Dremio, NiFi), enforce policies local |
| **Ranger Audit** | Ghi lại mọi access attempt (allow/deny) vào storage            |
| **UserSync**     | Đồng bộ users/groups từ LDAP/Active Directory                  |

---

## 3. Ranger Hoạt Động Như Thế Nào?

### 3.1. Quy Trình Enforce Policy

```
Analyst (user: john) chạy SQL trong Superset:
SELECT * FROM gold.txn_summary WHERE bank_id = 'VCB'

        │
        ▼
Superset gửi query → Dremio

        │
        ▼
Dremio nhận query
        │
        │  [Ranger Plugin trong Dremio]
        │  1. Lấy user: john
        │  2. Resource: gold.txn_summary, bank_id column
        │  3. Action: SELECT
        │
        ▼
Ranger Plugin kiểm tra policy cache (local, không cần gọi Admin)

        │
        │  Policy: "analysts group có quyền SELECT trên gold.*
        │           nhưng KHÔNG được đọc cột card_number"
        │
        ├── john TRONG analysts group? → YES
        ├── gold.txn_summary có trong allowed resources? → YES
        ├── card_number có bị mask không? → YES (mask với ***)
        │
        ▼
Dremio execute query, trả về kết quả
        ├── bank_id: VCB (hiển thị bình thường)
        ├── amount: 1,500,000 (hiển thị bình thường)
        └── card_number: ****-****-****-1234 (MASKED)

        │
        ▼
Ranger Audit log:
  {user: john, resource: gold.txn_summary, action: SELECT,
   result: ALLOWED, timestamp: 2026-06-19T10:30:00, ip: 10.0.0.5}
```

### 3.2. Policy Types trong Ranger

| Policy Type       | Mô tả                                               | Ví dụ NAPAS                            |
|-------------------|-----------------------------------------------------|----------------------------------------|
| **Allow**         | Cho phép action cụ thể                              | analysts được SELECT gold.*            |
| **Deny**          | Từ chối tuyệt đối (override Allow)                  | contractors không được SELECT bronze.* |
| **Row Filter**    | Tự động thêm WHERE clause                           | analysts chỉ thấy bank_id của mình    |
| **Data Masking**  | Che giấu giá trị column nhạy cảm                   | card_number hiển thị là ****1234       |

---

## 4. Tương Tác Với Các Component Khác

```
LDAP/AD ─────(UserSync)────▶ Ranger Admin
                                   │
                    ┌──────────────┼────────────────┐
                    │              │                │
                    ▼              ▼                ▼
               Dremio          NiFi           MinIO/S3
             (plugin polls   (plugin polls  (plugin polls
              policies)       policies)      policies)
                    │              │                │
                    └──────────────┼────────────────┘
                                   │
                              Audit Logs
                              (Solr/ES)
```

| Component     | Ranger tác động                                               |
|---------------|---------------------------------------------------------------|
| Dremio        | SQL-level policies: table, column, row filter, masking        |
| NiFi          | Flow-level: ai được start/stop flow, access sensitive data    |
| MinIO (S3)    | Bucket/object-level: get, put, delete permissions             |

---

## 5. Ưu Điểm & Nhược Điểm

| Ưu điểm                                      | Nhược điểm                                    |
|----------------------------------------------|-----------------------------------------------|
| Policy tập trung — thay đổi 1 chỗ, áp dụng tất cả | Nặng về tài nguyên (Java app, cần 2-4GB RAM) |
| Audit log đầy đủ cho compliance              | Setup phức tạp, nhiều bước                    |
| Row-level & column-level masking             | Plugin cần tương thích phiên bản service      |
| LDAP integration (không cần tạo user tay)   | Ranger Admin là SPOF (cần HA cho prod)        |
| Tag-based policies (Ranger + Atlas)          |                                               |

---

## 6. Cài Đặt

### 6.1. Deploy PostgreSQL cho Ranger trước

```yaml
# platform/values/base/ranger-postgres.yaml
image:
  repository: postgres
  tag: "15.6"

auth:
  database: ranger
  username: ranger
  password: "Ranger@Postgres2024"   # Override từ Vault trong prod

primary:
  persistence:
    enabled: true
    size: 10Gi
    storageClass: "platform-standard"

  resources:
    requests:
      memory: "256Mi"
      cpu: "250m"
    limits:
      memory: "512Mi"
      cpu: "500m"
```

### 6.2. Ranger Helm Values

```yaml
# platform/values/base/ranger.yaml
# Dùng custom Helm chart (Ranger chưa có official chart)
# Sử dụng: https://github.com/nocturnal-naiad/ranger-helm

image:
  repository: apache/ranger
  tag: "2.4.0"
  pullPolicy: IfNotPresent

ranger:
  admin:
    port: 6080
    # Credentials
    adminPassword: "RangerAdmin@2024"

  # Kết nối PostgreSQL
  database:
    host: "postgres-ranger.data-governance.svc.cluster.local"
    port: 5432
    dbName: ranger
    username: ranger
    password: "Ranger@Postgres2024"

  # Audit config — ghi vào Solr (hoặc log file cho dev)
  audit:
    solr:
      enabled: false   # Tắt Solr cho dev
    log4j:
      enabled: true    # Dùng log file cho dev

resources:
  requests:
    memory: "2Gi"
    cpu: "500m"
  limits:
    memory: "4Gi"
    cpu: "2000m"

service:
  type: ClusterIP
  port: 6080

persistence:
  enabled: true
  size: 5Gi
  storageClass: "platform-standard"
```

### 6.3. Helmfile configuration

```yaml
# platform/helmfile.yaml.gotmpl (layer 04-governance)
- name: postgres-ranger
  namespace: data-governance
  chart: bitnami/postgresql
  version: "15.5.x"
  values:
    - values/base/ranger-postgres.yaml
  labels:
    layer: 04-governance
    component: postgres

- name: ranger
  namespace: data-governance
  chart: ./charts/ranger    # Custom chart
  values:
    - values/base/ranger.yaml
    - values/env/ranger.yaml.gotmpl
  needs:
    - data-governance/postgres-ranger
  labels:
    layer: 04-governance
    component: ranger
```

### 6.4. Deploy

```bash
./scripts/deploy.sh dev 04-governance

# Chờ pods sẵn sàng
kubectl get pods -n data-governance -w
# NAME                       READY   STATUS    RESTARTS   AGE
# postgres-ranger-0          1/1     Running   0          2m
# ranger-xxxxxx              1/1     Running   0          3m
```

---

## 7. Cấu Hình Policies Sau Deploy

```bash
# Port-forward Ranger Admin UI
kubectl port-forward -n data-governance svc/ranger 6080:6080 &

# Mở browser: http://localhost:6080
# Login: admin / RangerAdmin@2024
```

### 7.1. Tạo Dremio Service trong Ranger

Vào Ranger UI → **Access Manager → Resource Based Policies → + Add New Service**:

```
Service Type: DREMIO
Service Name: napas-dremio
Username: admin                    (Dremio admin user)
Password: Dremio@Admin2024
JDBC URL: jdbc:arrow-flight-sql://dremio.data-processing.svc.cluster.local:32010
```

### 7.2. Tạo Policies Mẫu (via REST API)

```bash
RANGER_ADMIN="http://localhost:6080"
AUTH="admin:RangerAdmin@2024"

# Policy 1: Data Engineers được full access tất cả layers
curl -s -u $AUTH -X POST "$RANGER_ADMIN/service/public/v2/api/policy" \
  -H "Content-Type: application/json" -d '{
  "name": "data-engineers-full-access",
  "service": "napas-dremio",
  "resources": {
    "schema": {"values": ["bronze", "silver", "gold"], "isExcludes": false},
    "table":  {"values": ["*"], "isExcludes": false},
    "column": {"values": ["*"], "isExcludes": false}
  },
  "policyItems": [{
    "users": ["nifi-service", "dremio-admin"],
    "groups": ["data-engineers"],
    "accesses": [{"type": "select", "isAllowed": true},
                 {"type": "create", "isAllowed": true},
                 {"type": "drop",   "isAllowed": true}]
  }],
  "isEnabled": true
}'

# Policy 2: Analysts chỉ xem Gold layer
curl -s -u $AUTH -X POST "$RANGER_ADMIN/service/public/v2/api/policy" \
  -H "Content-Type: application/json" -d '{
  "name": "analysts-gold-readonly",
  "service": "napas-dremio",
  "resources": {
    "schema": {"values": ["gold"], "isExcludes": false},
    "table":  {"values": ["*"],    "isExcludes": false},
    "column": {"values": ["*"],    "isExcludes": false}
  },
  "policyItems": [{
    "groups": ["analysts"],
    "accesses": [{"type": "select", "isAllowed": true}]
  }],
  "isEnabled": true
}'

# Policy 3: Column masking — che card_number với analysts
curl -s -u $AUTH -X POST "$RANGER_ADMIN/service/public/v2/api/policy" \
  -H "Content-Type: application/json" -d '{
  "name": "mask-card-number",
  "service": "napas-dremio",
  "policyType": 1,
  "resources": {
    "schema": {"values": ["gold", "silver"]},
    "table":  {"values": ["transactions", "transactions_clean"]},
    "column": {"values": ["card_number"]}
  },
  "dataMaskPolicyItems": [{
    "groups": ["analysts"],
    "dataMaskInfo": {"dataMaskType": "MASK_SHOW_LAST_4"}
  }],
  "isEnabled": true
}'
```

---

## 8. Validate Ranger Hoạt Động Đúng

```bash
# ✅ Check 1: Pods READY
kubectl get pods -n data-governance
# Expected:
# postgres-ranger-0   1/1   Running
# ranger-xxxx         1/1   Running

# ✅ Check 2: Ranger Admin API health
kubectl port-forward -n data-governance svc/ranger 6080:6080 &
curl -s -u admin:RangerAdmin@2024 http://localhost:6080/service/public/v2/api/servicedef | python3 -m json.tool | head -5
# Expected: JSON với list of service definitions

# ✅ Check 3: UI accessible
# Mở browser http://localhost:6080
# Login: admin / RangerAdmin@2024
# Kiểm tra: Access Manager tab có menu

# ✅ Check 4: Database connection OK
kubectl exec -n data-governance postgres-ranger-0 -- \
  psql -U ranger -d ranger -c "\dt"
# Expected: List of Ranger tables (x_portal_user, x_policy, etc.)

# ✅ Check 5: Policies API
curl -s -u admin:RangerAdmin@2024 \
  http://localhost:6080/service/public/v2/api/policy?serviceType=dremio | \
  python3 -c "import sys,json; print(f'Policies: {len(json.load(sys.stdin)[\"vXPolicies\"])}')"
# Expected: Policies: 3 (sau khi tạo 3 policies ở trên)
```

### Kết Quả Validate Thành Công

```
[✅] postgres-ranger: 1/1 Running
[✅] ranger: 1/1 Running
[✅] Admin API: HTTP 200
[✅] Database tables: tồn tại
[✅] 3 policies đã tạo
[✅] UI: accessible tại localhost:6080
→ Ranger sẵn sàng — Proceed to Step 4: Apache NiFi
```

---

## 9. Lưu Ý

```
⚠️  Ranger plugin cần được cài vào Dremio image. Xem file 05-dremio.md
    để biết cách build Dremio image có sẵn Ranger plugin.

💡  Ranger policies được cache local tại plugin (mặc định 30 giây).
    Thay đổi policy không có hiệu lực ngay lập tức — đợi 30 giây.

💡  Audit logs trong dev mode ghi vào /var/log/ranger/ trong pod.
    Production nên cấu hình Solr hoặc Elasticsearch để query audit.
```
