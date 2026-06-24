# NAPAS Data Platform — Kiến Trúc Tổng Quan

> **Phiên bản:** 1.0 | **Ngày:** 2026-06-19
> **Pattern tham chiếu:** paraline-platform (Kubernetes + Helmfile + Layer-based namespaces)

---

## 1. Tổng Quan Hệ Thống

NAPAS Data Platform là **Modern Data Lake Stack** kết hợp 6 công nghệ OSS trên nền Kubernetes:

| Component          | Vai trò chính                                    | Namespace        |
|--------------------|--------------------------------------------------|------------------|
| HashiCorp Vault    | Quản lý secrets, credentials, API keys           | data-security    |
| MinIO              | Object Storage S3-compatible (Data Lake)         | data-storage     |
| Apache Ranger      | Phân quyền tập trung, audit log                  | data-governance  |
| Apache NiFi        | Ingestion batch + Orchestration pipeline         | data-ingestion   |
| Dremio             | SQL engine, xử lý dữ liệu 3-layer               | data-processing  |
| Apache Superset    | Dashboard, báo cáo BI                            | data-visualization|

> **NiFi = Ingestion + Orchestration:** Do scope dự án nhỏ, NiFi thay thế hoàn toàn Airflow.
> NiFi có khả năng schedule, retry, monitoring flow nên đủ đáp ứng nhu cầu orchestration.

---

## 2. Sơ Đồ Kiến Trúc Tổng Thể

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                        NAPAS DATA PLATFORM (Kubernetes)                     ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  ┌─────────────────────────────────────────────────────────────────────┐   ║
║  │  NGUỒN DỮ LIỆU (External — ngoài cluster)                           │   ║
║  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────────────┐  │   ║
║  │  │ Oracle   │  │ MySQL /  │  │ REST API │  │ Flat Files         │  │   ║
║  │  │ CoreBank │  │ MariaDB  │  │ (HTTP/S) │  │ CSV/JSON/SFTP      │  │   ║
║  │  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────────┬───────────┘  │   ║
║  └───────┼─────────────┼─────────────┼────────────────┼───────────────┘   ║
║          └─────────────┴─────────────┴────────────────┘                    ║
║                                      │                                      ║
║  ┌──────────────────────────────┐    │  ← NiFi fetch credentials từ Vault  ║
║  │  [01] data-security          │    │                                      ║
║  │  ┌──────────────────────┐    │    │                                      ║
║  │  │  HashiCorp Vault     │◄───┼────┘                                      ║
║  │  │  :8200               │    │                                           ║
║  │  │  • DB credentials    │    │                                           ║
║  │  │  • S3 access keys    │    │                                           ║
║  │  │  • API tokens        │    │                                           ║
║  │  └──────────┬───────────┘    │                                           ║
║  └─────────────┼────────────────┘                                           ║
║                │ secrets inject                                             ║
║                ▼                                                            ║
║  ┌─────────────────────────────────────────────────┐                       ║
║  │  [03] data-ingestion                             │                       ║
║  │  ┌───────────────────────────────────────────┐  │                       ║
║  │  │            Apache NiFi :8443              │  │                       ║
║  │  │                                           │  │                       ║
║  │  │  ┌─────────────┐   ┌────────────────────┐│  │                       ║
║  │  │  │ Processor   │──▶│ Processor          ││  │                       ║
║  │  │  │ QueryDB     │   │ ConvertToParquet   ││  │                       ║
║  │  │  │ FetchHTTP   │   │ PutS3Object        ││  │                       ║
║  │  │  │ GetSFTP     │   │ (→ MinIO)          ││  │                       ║
║  │  │  └─────────────┘   └────────────────────┘│  │                       ║
║  │  │                                           │  │                       ║
║  │  │  Orchestration:                           │  │                       ║
║  │  │  • Cron Schedule (0 2 * * *)              │  │                       ║
║  │  │  • Retry on failure                       │  │                       ║
║  │  │  • Alert on error                         │  │                       ║
║  │  │  • Flow monitoring dashboard              │  │                       ║
║  │  └───────────────────────────────────────────┘  │                       ║
║  └─────────────────────────────────────────────────┘                       ║
║                        │ PutS3Object                                        ║
║                        ▼                                                    ║
║  ┌─────────────────────────────────────────────────┐                       ║
║  │  [02] data-storage                               │                       ║
║  │  ┌───────────────────────────────────────────┐  │                       ║
║  │  │     MinIO (S3-Compatible)                 │  │                       ║
║  │  │     Console: :9001  |  S3 API: :9000      │  │                       ║
║  │  │                                           │  │                       ║
║  │  │  Bucket: napas-datalake/                  │  │                       ║
║  │  │  ├── bronze/   ← Raw data (Parquet/JSON)  │  │                       ║
║  │  │  ├── silver/   ← Cleaned & validated      │  │                       ║
║  │  │  └── gold/     ← Business aggregates      │  │                       ║
║  │  └───────────────────────────────────────────┘  │                       ║
║  └─────────────────────────────────────────────────┘                       ║
║                        │ S3 Read/Write                                      ║
║       ┌────────────────┼──────────────────────────────┐                    ║
║       │                ▼                              │                    ║
║  ┌────┴───────────────────────────────┐  ┌───────────┴────────────────┐   ║
║  │  [05] data-processing              │  │  [04] data-governance       │   ║
║  │  ┌────────────────────────────┐    │  │  ┌──────────────────────┐   │   ║
║  │  │        Dremio              │◄───┼──┼──│   Apache Ranger      │   │   ║
║  │  │  Coordinator: :9047        │    │  │  │   :6080 (Admin UI)   │   │   ║
║  │  │  Arrow Flight: :32010      │    │  │  │                      │   │   ║
║  │  │                            │    │  │  │ • Row-level security │   │   ║
║  │  │  • Bronze → Virtual DS     │    │  │  │ • Column masking     │   │   ║
║  │  │  • Silver → SQL Transform  │    │  │  │ • Tag-based policies │   │   ║
║  │  │  • Gold  → Reflections     │    │  │  │ • Audit to Solr      │   │   ║
║  │  └────────────────────────────┘    │  │  └──────────────────────┘   │   ║
║  └────────────────────────────────────┘  └────────────────────────────┘   ║
║                        │ Arrow Flight SQL                                   ║
║                        ▼                                                    ║
║  ┌─────────────────────────────────────────────────┐                       ║
║  │  [06] data-visualization                         │                       ║
║  │  ┌───────────────────────────────────────────┐  │                       ║
║  │  │     Apache Superset :8088                 │  │                       ║
║  │  │                                           │  │                       ║
║  │  │  • Dashboard tổng hợp                     │  │                       ║
║  │  │  • Báo cáo 3 layer (Bronze/Silver/Gold)   │  │                       ║
║  │  │  • SQL Lab ad-hoc queries                 │  │                       ║
║  │  │  • RBAC phân quyền xem dashboard          │  │                       ║
║  │  └───────────────────────────────────────────┘  │                       ║
║  └─────────────────────────────────────────────────┘                       ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

---

## 3. Luồng Dữ Liệu End-to-End

```
Nguồn Napas (Oracle/MySQL/API)
        │
        │  JDBC / HTTP / SFTP
        ▼
[NiFi] — lấy DB creds từ [Vault]
        │
        │  PutS3Object (Parquet)
        ▼
[MinIO] napas-datalake/bronze/
        │
        │  S3 source read
        ▼
[Dremio] Bronze Virtual Dataset
        │
        │  SQL: CTAS silver AS SELECT DISTINCT, CAST, COALESCE...
        ▼
[MinIO] napas-datalake/silver/
        │
        │  SQL: CTAS gold AS SELECT bank_id, COUNT(*), SUM(amount)...
        ▼
[MinIO] napas-datalake/gold/
        │
        │  Arrow Flight / ODBC
        ▼
[Superset] Dashboard
        │
        ▼
[Business User / Analyst]
```

---

## 4. Mô Hình 3-Layer (Medallion Architecture)

| Layer  | S3 Path                       | Tạo bởi    | Đặc điểm                              | Người dùng          |
|--------|-------------------------------|-----------|---------------------------------------|---------------------|
| Bronze | `s3://napas-datalake/bronze/` | NiFi      | Raw, immutable, partition by date     | Data Engineer       |
| Silver | `s3://napas-datalake/silver/` | Dremio    | Clean, validate, deduplicate          | Data Analyst        |
| Gold   | `s3://napas-datalake/gold/`   | Dremio    | Aggregated, KPI, business-ready       | Business + Superset |

**Ví dụ thực tế — giao dịch Napas:**

```
Bronze:  1 dòng / 1 giao dịch gốc từ Oracle lúc 2h sáng
         {txn_id, amount, merchant_id, bank_id, status_code, created_at, ...}

Silver:  Sau khi validate:
         - Loại bỏ txn_id trùng lặp
         - Cast amount từ VARCHAR sang DECIMAL(18,2)
         - Chuẩn hóa bank_id về format NAPAS (9 chữ số)
         - Lọc status_code = '00' (thành công)

Gold:    Tổng hợp:
         SELECT bank_id, DATE(created_at) as date,
                COUNT(*) as total_txns,
                SUM(amount) as total_amount
         FROM silver.transactions
         GROUP BY bank_id, DATE(created_at)
```

---

## 5. Kubernetes Namespace Layers

```
Layer   Namespace             Component
─────   ─────────────────     ────────────────────────────────────────
00      data-infra            Ingress NGINX, cert-manager (optional)
01      data-security         HashiCorp Vault (3-node Raft HA)
02      data-storage          MinIO (standalone dev / distributed prod)
03      data-ingestion        Apache NiFi
04      data-governance       Apache Ranger + PostgreSQL (Ranger metastore)
05      data-processing       Dremio (Coordinator + Executor)
06      data-visualization    Apache Superset + Redis + PostgreSQL
```

---

## 6. Thứ Tự Triển Khai

```
Step 1: Vault      → Secrets store cho tất cả services
Step 2: MinIO      → Storage trước khi NiFi ghi
Step 3: Ranger DB  → PostgreSQL metadata store
Step 4: Ranger     → Policies trước khi Dremio query
Step 5: NiFi       → Ingest → ghi Bronze vào MinIO
Step 6: Dremio     → SQL trên MinIO, enforce Ranger policies
Step 7: Superset   → BI kết nối Dremio Gold layer
```

---

## 7. Requirements

### Local Dev (Kind cluster)
```bash
kind     >= 0.23   kubectl  >= 1.29
helm     >= 3.14   helmfile >= 0.162
docker   >= 25.0
```

### Tài nguyên tối thiểu cho local
| Service   | CPU    | RAM   |
|-----------|--------|-------|
| Vault     | 0.5    | 256Mi |
| MinIO     | 0.5    | 512Mi |
| Ranger    | 1      | 2Gi   |
| NiFi      | 2      | 4Gi   |
| Dremio    | 2      | 8Gi   |
| Superset  | 0.5    | 1Gi   |
| **Total** | **~7** | **~16Gi** |

---

## 8. Tài Liệu Chi Tiết Từng Component

| File                      | Component         | Deploy Order |
|---------------------------|-------------------|:------------:|
| `01-hashicorp-vault.md`   | HashiCorp Vault   | 1            |
| `02-minio.md`             | MinIO             | 2            |
| `03-apache-ranger.md`     | Apache Ranger     | 3            |
| `04-apache-nifi.md`       | Apache NiFi       | 4            |
| `05-dremio.md`            | Dremio            | 5            |
| `06-apache-superset.md`   | Apache Superset   | 6            |
| `07-integration-guide.md` | End-to-End Flow   | Reference    |
| `08-nifi-pipeline-guide.md` | NiFi Pipeline Demo | Tutorial  |

> Mỗi file có cấu trúc: **Là gì → Kiến trúc → Hoạt động → Tương tác → Pros/Cons → Cài đặt → Validate**

### Metadata-Driven Pipeline (Advanced)

| File                      | Nội dung                          | Mục đích               |
|---------------------------|-----------------------------------|------------------------|
| `09-metadata-driven-strategy.md` | Chiến lược metadata-driven  | Architecture & Design  |
| `10-metadata-tables-design.md`   | Schema metadata tables      | DDL & Sample Data      |
| `11-nifi-dynamic-pipeline-setup.md` | NiFi Process Group setup | Build Pipeline         |
| `12-dynamic-sql-templates.md`    | SQL templates reference     | Bronze/Silver/Gold SQL |
| `13-pipeline-operations-runbook.md` | Operations guide         | Day-to-day Operations  |
