# Step 6: Apache Superset — Dashboard & Báo Cáo BI

> **Deploy order:** 6/6
> **Namespace:** `data-visualization`
> **Phụ thuộc:** Dremio (data source), Vault (credentials), PostgreSQL (metadata)

---

## 1. Apache Superset Là Gì?

Apache Superset là **Business Intelligence (BI) platform** mã nguồn mở — cho phép kết nối đến databases/query engines và tạo dashboard, chart, báo cáo tương tác không cần viết code. Superset là lớp cuối cùng trong pipeline: Business Users và Analysts sẽ làm việc tại đây.

**Vị trí trong NAPAS Platform:**

```
[NiFi] → [MinIO] → [Dremio Gold Layer] → [Superset] → [User]
                                              ▲
                          Superset kết nối Dremio qua Arrow Flight SQL
                          Chỉ đọc Gold layer — đã aggregated và cleaned
```

---

## 2. Kiến Trúc Superset

```
┌───────────────────────────────────────────────────────────────────┐
│                     Apache Superset                                │
│                                                                    │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                  Web Application (:8088)                     │  │
│  │                                                              │  │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────────────────┐ │  │
│  │  │ Dashboard  │  │  Charts    │  │  SQL Lab               │ │  │
│  │  │            │  │            │  │                        │ │  │
│  │  │ Tổng hợp   │  │ Bar chart  │  │ Ad-hoc SQL queries     │ │  │
│  │  │ nhiều chart│  │ Line chart │  │ Explore data interactively│ │  │
│  │  │ vào 1 view │  │ Pie chart  │  │ Download CSV/Excel     │ │  │
│  │  │            │  │ Table...   │  │                        │ │  │
│  │  └────────────┘  └────────────┘  └────────────────────────┘ │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                                                                    │
│  ┌─────────────────┐  ┌──────────────┐  ┌─────────────────────┐  │
│  │   Celery Worker │  │     Redis    │  │  PostgreSQL          │  │
│  │                 │  │    (Cache)   │  │  (Metadata DB)       │  │
│  │  Async query    │  │              │  │  - Users, roles      │  │
│  │  execution      │  │  Query cache │  │  - Dashboards        │  │
│  │  Email alerts   │  │  Session     │  │  - Charts            │  │
│  └─────────────────┘  └──────────────┘  │  - Datasources       │  │
│                                          └─────────────────────┘  │
│                                                                    │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │           SQLAlchemy Database Connections                    │  │
│  │                                                              │  │
│  │  Dremio (Arrow Flight) ← kết nối chính                      │  │
│  │  URL: dremioflight://admin:pass@dremio:32010/?UseEncryption=0│  │
│  └─────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────┘
```

### Các Thành Phần

| Thành phần       | Vai trò                                                             |
|------------------|---------------------------------------------------------------------|
| **Web App**      | Flask app — UI để tạo chart, dashboard, quản lý users             |
| **Celery Worker**| Chạy async jobs: scheduled reports, email alerts, cache warming    |
| **Redis**        | Broker cho Celery + cache kết quả query                            |
| **PostgreSQL**   | Lưu metadata: charts, dashboards, users, roles, datasources        |
| **SQLAlchemy**   | Database connector layer — kết nối đến Dremio qua Arrow Flight     |

---

## 3. Superset Hoạt Động Như Thế Nào?

### 3.1. Luồng Tạo Dashboard

```
1. Admin cấu hình Database Connection:
   - Type: Other (Arrow Flight)
   - SQLAlchemy URI: dremioflight://admin:pass@dremio.data-processing:32010/

2. Analyst tạo Dataset:
   - Chọn database: Dremio-Napas
   - Chọn schema: gold
   - Chọn table: txn_summary_by_bank

3. Analyst tạo Chart:
   - Chart type: Bar Chart
   - X-axis: txn_date
   - Y-axis: total_amount
   - Series: bank_id
   - Filter: last 30 days

4. Analyst tạo Dashboard:
   - Drag chart vào layout
   - Thêm filter panel (date range, bank_id selector)
   - Publish dashboard

5. Business User xem Dashboard:
   - Đăng nhập Superset
   - Mở dashboard "Tổng hợp giao dịch Napas"
   - Chọn date range, bank_id → chart tự update
```

### 3.2. Query Flow Superset → Dremio

```
User click chart → Superset generate SQL:
  SELECT bank_id, txn_date, SUM(total_amount) as total
  FROM gold.txn_summary_by_bank
  WHERE txn_date BETWEEN '2026-06-01' AND '2026-06-19'
  GROUP BY bank_id, txn_date
  ORDER BY txn_date DESC

  │
  ▼
SQLAlchemy → Arrow Flight Client
  grpc://dremio.data-processing.svc.cluster.local:32010

  │
  ▼
Dremio xử lý SQL (có Reflection → kết quả ngay <1s)

  │ Arrow columnar format
  ▼
Superset nhận kết quả → render chart → trả về browser
```

---

## 4. Tương Tác Với Các Component Khác

| Component | Tương tác với Superset               |
|-----------|--------------------------------------|
| Dremio    | Query Gold layer qua Arrow Flight SQL |
| Ranger    | Indirect — Ranger enforce tại Dremio (Superset chỉ thấy kết quả đã được filter) |
| Vault     | Inject Superset secret key và PostgreSQL password lúc startup |
| PostgreSQL| Superset metadata store (riêng, không dùng chung với Ranger) |
| Redis     | Cache query results, Celery message broker |

---

## 5. Ưu Điểm & Nhược Điểm

| Ưu điểm                                      | Nhược điểm                                    |
|----------------------------------------------|-----------------------------------------------|
| Miễn phí, OSS — không bị vendor lock-in      | UI/UX kém hơn Tableau/PowerBI cho non-tech users |
| SQL Lab: query trực tiếp, export CSV/Excel   | Không có self-service drag-drop như Tableau   |
| Arrow Flight: performance cao với Dremio     | Scheduled reports cần Celery + Redis setup     |
| RBAC tích hợp: phân quyền xem dashboard      | Alert/email feature cần cấu hình SMTP         |
| Jinja2 templating trong SQL (dynamic filter) | Mobile experience hạn chế                     |
| API đầy đủ: tự động hóa tạo chart/dashboard |                                               |

---

## 6. Cài Đặt — Helm Values

### 6.1. PostgreSQL cho Superset

```yaml
# platform/values/base/superset-postgres.yaml
image:
  repository: postgres
  tag: "15.6"

auth:
  database: superset
  username: superset
  password: "Superset@Postgres2024"

primary:
  persistence:
    enabled: true
    size: 10Gi
    storageClass: "platform-standard"

  resources:
    requests: { memory: "256Mi", cpu: "250m" }
    limits:   { memory: "512Mi", cpu: "500m"  }
```

### 6.2. Superset Helm Values

```yaml
# platform/values/base/superset.yaml
image:
  repository: apache/superset
  tag: "4.0.1"
  pullPolicy: IfNotPresent

# Secret key — PHẢI đổi trong production
configOverrides:
  secret_key: |
    SECRET_KEY = 'NAPASSupersetSecretKey2024ChangeMe'
  database: |
    SQLALCHEMY_DATABASE_URI = 'postgresql://superset:Superset@Postgres2024@postgres-superset.data-visualization.svc.cluster.local:5432/superset'

# Admin user
init:
  adminUser:
    username: admin
    password: "NapasSuperset@2024"
    email: admin@napas.com
    firstname: Admin
    lastname: Napas

# Resources
resources:
  requests: { memory: "1Gi", cpu: "500m" }
  limits:   { memory: "2Gi", cpu: "1000m" }

# Celery Worker
supersetWorker:
  enabled: true
  resources:
    requests: { memory: "512Mi", cpu: "250m" }
    limits:   { memory: "1Gi",   cpu: "500m"  }

# Redis (internal)
redis:
  enabled: true
  auth:
    enabled: false
  master:
    persistence:
      enabled: true
      size: 5Gi

# Service
service:
  type: ClusterIP
  port: 8088

# Extra pip packages
extraPackages:
  - pyarrow==14.0.1
  - sqlalchemy-dremio==3.0.5   # Dremio Arrow Flight connector

# Bootstrap script (chạy sau init)
bootstrapScript: |
  #!/bin/bash
  pip install pyarrow==14.0.1 sqlalchemy-dremio==3.0.5
```

### 6.3. Helmfile configuration

```yaml
# platform/helmfile.yaml.gotmpl (layer 06-visualization)
- name: postgres-superset
  namespace: data-visualization
  chart: bitnami/postgresql
  version: "15.5.x"
  values:
    - values/base/superset-postgres.yaml
  labels:
    layer: 06-visualization
    component: postgres

- name: superset
  namespace: data-visualization
  chart: superset/superset
  version: "0.12.9"
  values:
    - values/base/superset.yaml
    - values/env/superset.yaml.gotmpl
  needs:
    - data-visualization/postgres-superset
    - data-processing/dremio
  labels:
    layer: 06-visualization
    component: superset
```

### 6.4. Deploy

```bash
# Thêm Superset Helm repo
helm repo add superset https://apache.github.io/superset
helm repo update

./scripts/deploy.sh dev 06-visualization

kubectl get pods -n data-visualization -w
# NAME                         READY   STATUS    RESTARTS   AGE
# postgres-superset-0          1/1     Running   0          2m
# superset-xxxxxxxxx           1/1     Running   0          3m
# superset-worker-xxxxxxxxx    1/1     Running   0          3m
# superset-redis-master-0      1/1     Running   0          3m
```

---

## 7. Cấu Hình Sau Deploy

### 7.1. Kết Nối Dremio

```bash
kubectl port-forward -n data-visualization svc/superset 8088:8088 &
# Mở: http://localhost:8088
# Login: admin / NapasSuperset@2024
```

**Settings → Database Connections → + Database**

```
Database Type: Other (hoặc chọn Dremio nếu có)
Display Name: Dremio-Napas
SQLAlchemy URI: dremioflight://admin:Dremio%40Admin2024@dremio.data-processing.svc.cluster.local:32010/?UseEncryption=0

Advanced → SQL Lab:
  ✅ Allow DML
  ✅ Allow CREATE TABLE AS
  ✅ Allow Multi Schema Metadata Fetch

Test Connection → phải thấy "Connection looks good!"
```

### 7.2. Tạo Dataset từ Gold Layer

**Datasets → + Dataset**

```
Database: Dremio-Napas
Schema: gold
Table: txn_summary_by_bank
→ Save
```

### 7.3. Tạo Chart Mẫu — Biểu Đồ Giao Dịch Theo Ngân Hàng

**Charts → + Chart**

```
Dataset: gold.txn_summary_by_bank
Chart Type: Bar Chart

X-axis: txn_date
Metrics: SUM(total_amount)  [đặt tên: "Tổng giá trị GD"]
Series: bank_id
Sort: txn_date DESC
Time Range: Last 30 days

→ Run Query → Save → "Tổng GD theo Ngân Hàng"
```

### 7.4. Tạo Dashboard

**Dashboards → + Dashboard**

```
Title: "Tổng Quan Giao Dịch Napas"
→ Drag chart "Tổng GD theo Ngân Hàng" vào layout
→ Add Filter: Date Range (txn_date)
→ Add Filter: Bank (bank_id - multiple select)
→ Publish
```

---

## 8. Validate Superset Hoạt Động Đúng

```bash
# ✅ Check 1: Pods READY
kubectl get pods -n data-visualization
# Expected:
# postgres-superset-0        1/1   Running
# superset-xxxx              1/1   Running
# superset-worker-xxxx       1/1   Running
# superset-redis-master-0    1/1   Running

# ✅ Check 2: Web UI accessible
kubectl port-forward -n data-visualization svc/superset 8088:8088 &
# Mở: http://localhost:8088 → login thành công

# ✅ Check 3: Database connection hoạt động
# UI → Settings → Database Connections → Dremio-Napas → Test Connection
# Expected: "Connection looks good!"

# ✅ Check 4: SQL Lab query
# UI → SQL Lab → Database: Dremio-Napas → Schema: gold
# Query: SELECT * FROM txn_summary_by_bank LIMIT 5
# Expected: kết quả hiển thị trong table

# ✅ Check 5: Chart render
# Mở chart "Tổng GD theo Ngân Hàng"
# Expected: bar chart render, không lỗi

# ✅ Check 6: Dashboard accessible
# Mở dashboard "Tổng Quan Giao Dịch Napas"
# Expected: charts hiển thị, filters hoạt động

# ✅ Check 7: RBAC — tạo Analyst user
# Settings → List Users → + User
# Role: Alpha (read-only access)
# → Login với user mới → chỉ xem được dashboard, không có Settings
```

### Kết Quả Validate Thành Công

```
[✅] Tất cả pods: 1/1 Running (superset, worker, redis, postgres)
[✅] Web UI: accessible tại localhost:8088
[✅] Dremio connection: "Connection looks good!"
[✅] SQL Lab: query Gold layer trả về kết quả
[✅] Chart: render thành công
[✅] Dashboard: hiển thị và filter hoạt động
[✅] RBAC: phân quyền theo role hoạt động

🎉 NAPAS Data Platform END-TO-END HOÀN CHỈNH!

Flow hoàn chỉnh:
  Oracle/MySQL → NiFi → MinIO (bronze)
                → Dremio → MinIO (silver/gold)
                → Superset → Business Users
```

---

## 9. Lưu Ý

```
⚠️  SECRET_KEY phải được đặt cố định và không thay đổi.
    Nếu đổi SECRET_KEY, tất cả sessions bị invalidate và
    encrypted passwords trong DB không decrypt được.

💡  Superset cache kết quả query trong Redis theo mặc định 24h.
    Có thể force refresh bằng nút ↻ trong chart.

💡  Tạo Role "Napas Analyst" riêng với:
    - Quyền xem dashboards/charts
    - Không có quyền SQL Lab (nếu cần hạn chế)
    - Restricted data source (chỉ gold schema)

💡  Export dashboard sang JSON để backup và import sang UAT/Prod:
    Dashboards → Export → import ở môi trường khác
```
