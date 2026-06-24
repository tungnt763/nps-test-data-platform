# Metadata-Driven Pipeline Strategy

> **Phiên bản:** 1.0 | **Ngày:** 2026-06-24
> **Áp dụng cho:** NiFi (Ingestion + Orchestration) + Dremio (Transform Engine)

---

## 1. Tại Sao Metadata-Driven?

### Vấn đề với pipeline truyền thống

```
Pipeline truyền thống (Hard-coded):
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ Pipeline A   │     │ Pipeline B   │     │ Pipeline C   │
│ transactions │     │ merchants    │     │ settlements  │
│              │     │              │     │              │
│ SELECT *     │     │ SELECT *     │     │ SELECT *     │
│ FROM txn     │     │ FROM merch   │     │ FROM settle  │
│ WHERE date > │     │ WHERE id >   │     │ (full load)  │
│ '2026-01-01' │     │ 1000         │     │              │
│              │     │              │     │              │
│ → bronze/txn │     │ → bronze/mer │     │ → bronze/set │
└──────────────┘     └──────────────┘     └──────────────┘
      ×50 bảng = 50 pipeline riêng biệt, mỗi pipeline cần sửa riêng
```

**Nhược điểm:**
- **50 bảng = 50 pipeline** → khó maintain, dễ sai sót
- Thay đổi logic (thêm column, đổi load type) → sửa từng pipeline
- Không có tính nhất quán, mỗi pipeline viết theo cách khác nhau
- Scale kém: thêm 10 bảng mới = xây 10 pipeline mới

### Giải pháp: Metadata-Driven

```
Metadata-Driven Pipeline:
┌─────────────────────────────────────────────────────────────┐
│                    METADATA CONFIG (Iceberg)                 │
│                                                              │
│  pipeline_id │ table_name   │ load_type   │ watermark_col   │
│  ────────────┼──────────────┼─────────────┼─────────────── │
│  P001        │ transactions │ incremental │ updated_at      │
│  P002        │ merchants    │ incremental │ merchant_id     │
│  P003        │ settlements  │ full        │ NULL            │
│  P004        │ bank_codes   │ full        │ NULL            │
│  ...         │ ...          │ ...         │ ...             │
└──────────────────────────┬──────────────────────────────────┘
                           │ READ CONFIG
                           ▼
┌──────────────────────────────────────────────────────────────┐
│              GENERIC PIPELINE (NiFi - 1 flow duy nhất)       │
│                                                              │
│  ┌────────────┐   ┌───────────┐   ┌─────────┐   ┌────────┐ │
│  │ Read       │──▶│ Generate  │──▶│ Execute │──▶│ Write  │ │
│  │ Metadata   │   │ Dynamic   │   │ SQL     │   │ to S3  │ │
│  │            │   │ SQL       │   │         │   │        │ │
│  └────────────┘   └───────────┘   └─────────┘   └────────┘ │
│                                                              │
│  1 flow xử lý TẤT CẢ bảng — chỉ cần thêm 1 dòng metadata  │
└──────────────────────────────────────────────────────────────┘
```

**Ưu điểm:**
- **Thêm bảng mới = INSERT 1 dòng vào metadata** (không cần sửa NiFi)
- Tính nhất quán cao: mọi bảng đều đi qua cùng 1 pipeline
- Dễ audit, monitor: tất cả execution log tập trung
- Scale tốt: 5 bảng hay 500 bảng — cùng 1 NiFi flow

---

## 2. Kiến Trúc Tổng Thể

```
╔══════════════════════════════════════════════════════════════════════════╗
║                    METADATA-DRIVEN DATA PLATFORM                        ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║  ┌────────────────────────────────────────────────────────────────────┐ ║
║  │                     METADATA LAYER (Dremio/Iceberg)                │ ║
║  │                                                                    │ ║
║  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────┐ │ ║
║  │  │ pipeline_    │ │ column_      │ │ transform_   │ │ dq_      │ │ ║
║  │  │ config       │ │ mapping      │ │ rules        │ │ rules    │ │ ║
║  │  │              │ │              │ │              │ │          │ │ ║
║  │  │ source info  │ │ src→tgt col  │ │ SQL template │ │ not_null │ │ ║
║  │  │ load_type    │ │ data_type    │ │ dedup/merge  │ │ unique   │ │ ║
║  │  │ schedule     │ │ transform    │ │ aggregate    │ │ range    │ │ ║
║  │  └──────┬───────┘ └──────┬───────┘ └──────┬───────┘ └────┬─────┘ │ ║
║  │         └────────────────┴────────────────┴──────────────┘       │ ║
║  └─────────────────────────────┬──────────────────────────────────────┘ ║
║                                │                                         ║
║                    ┌───────────┴───────────┐                            ║
║                    │    NiFi reads config  │                            ║
║                    │    via Dremio JDBC    │                            ║
║                    └───────────┬───────────┘                            ║
║                                │                                         ║
║  ┌─────────────────────────────┴──────────────────────────────────────┐ ║
║  │                  NIFI GENERIC PIPELINE ENGINE                      │ ║
║  │                                                                    │ ║
║  │  ┌─────────┐   ┌──────────┐   ┌──────────┐   ┌──────────────────┐│ ║
║  │  │ PG:     │──▶│ PG:      │──▶│ PG:      │──▶│ PG:              ││ ║
║  │  │ Meta    │   │ Bronze   │   │ Silver   │   │ Gold             ││ ║
║  │  │ Reader  │   │ Ingestion│   │ Transform│   │ Transform        ││ ║
║  │  └─────────┘   └──────────┘   └──────────┘   └──────────────────┘│ ║
║  │       │              │              │                │            │ ║
║  │       │ Read config  │ Source→MinIO │ Dremio SQL    │ Dremio SQL │ ║
║  └───────┼──────────────┼──────────────┼────────────────┼────────────┘ ║
║          │              │              │                │              ║
║          ▼              ▼              ▼                ▼              ║
║  ┌────────────────────────────────────────────────────────────────────┐ ║
║  │                     STORAGE LAYER (MinIO/Iceberg)                  │ ║
║  │                                                                    │ ║
║  │  napas-datalake/                                                   │ ║
║  │  ├── metadata/          ← Iceberg tables (pipeline_config, ...)   │ ║
║  │  ├── bronze/            ← Raw Parquet (NiFi writes)               │ ║
║  │  │   ├── transactions/dt=2026-06-24/                              │ ║
║  │  │   ├── merchants/dt=2026-06-24/                                 │ ║
║  │  │   └── settlements/dt=2026-06-24/                               │ ║
║  │  ├── silver/            ← Cleaned Iceberg (Dremio transforms)     │ ║
║  │  │   ├── transactions/                                            │ ║
║  │  │   ├── merchants/                                               │ ║
║  │  │   └── settlements/                                             │ ║
║  │  └── gold/              ← Aggregated Iceberg (Dremio transforms)  │ ║
║  │      ├── daily_summary/                                           │ ║
║  │      └── bank_kpis/                                               │ ║
║  └────────────────────────────────────────────────────────────────────┘ ║
║                                                                          ║
║  ┌────────────────────────────────────────────────────────────────────┐ ║
║  │                     QUERY ENGINE (Dremio)                          │ ║
║  │                                                                    │ ║
║  │  Vai trò kép:                                                     │ ║
║  │  1. Metadata Store: Host Iceberg metadata tables                  │ ║
║  │  2. Transform Engine: Chạy SQL bronze→silver→gold                 │ ║
║  │  3. Serving Layer: Expose gold tables cho Superset                │ ║
║  └────────────────────────────────────────────────────────────────────┘ ║
╚══════════════════════════════════════════════════════════════════════════╝
```

---

## 3. Nguyên Tắc Thiết Kế

### 3.1 ELT Pattern (không phải ETL)

```
ETL truyền thống:        ELT (Platform này):
Source → Transform → Load   Source → Load → Transform
         ↑ NiFi                      ↑ NiFi  ↑ Dremio
         (nặng)                      (nhẹ)   (mạnh)
```

- **NiFi** chỉ làm **E**xtract + **L**oad (raw data → bronze)
- **Dremio** làm **T**ransform (bronze → silver → gold) bằng SQL trên Iceberg
- NiFi đóng vai trò **orchestrator**: trigger Dremio SQL theo schedule

### 3.2 Single Responsibility per Layer

| Layer    | Engine | Trách nhiệm                           | Format          |
|----------|--------|----------------------------------------|-----------------|
| Bronze   | NiFi   | Ingest raw data, không transform       | Parquet files   |
| Silver   | Dremio | Clean, dedup, type cast, validate      | Iceberg tables  |
| Gold     | Dremio | Aggregate, KPI, business logic         | Iceberg tables  |
| Metadata | Dremio | Pipeline config, execution logs        | Iceberg tables  |

### 3.3 Convention over Configuration

Quy ước đặt tên tự động, giảm config:

```
Source table:  napas_core.transactions
  → Bronze path:  s3://napas-datalake/bronze/transactions/dt=YYYY-MM-DD/
  → Silver table:  silver.transactions
  → Gold table:    gold.daily_transactions_summary

Source table:  napas_core.merchants
  → Bronze path:  s3://napas-datalake/bronze/merchants/dt=YYYY-MM-DD/
  → Silver table:  silver.merchants
```

### 3.4 Idempotent Operations

Mọi pipeline đều có thể chạy lại mà không gây duplicate:

- **Full Load**: Overwrite partition hoặc `CREATE OR REPLACE TABLE`
- **Incremental**: `MERGE INTO` (upsert by primary key)
- **Dedup**: `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY updated_at DESC)`

---

## 4. Luồng Xử Lý Chi Tiết

### 4.1 Bronze Ingestion (NiFi)

```
                    ┌──────────────────────────────────┐
                    │    metadata.pipeline_config       │
                    │    WHERE target_layer = 'bronze'  │
                    │    AND is_active = true            │
                    └──────────────┬───────────────────┘
                                   │ (N rows = N tables)
                                   ▼
                    ┌──────────────────────────────────┐
                    │    SplitRecord: 1 FlowFile/table  │
                    └──────────────┬───────────────────┘
                                   │
                    ┌──────────────┴───────────────────┐
                    │    EvaluateJsonPath               │
                    │    → source_table, load_type,     │
                    │      watermark_col, primary_keys  │
                    └──────────────┬───────────────────┘
                                   │
                    ┌──────────────┴───────────────────┐
                    │    RouteOnAttribute               │
                    │    ${load_type}                   │
                    └──────┬──────────────┬────────────┘
                           │              │
                    ┌──────┴──────┐ ┌─────┴───────┐
                    │ full        │ │ incremental  │
                    │             │ │              │
                    │ SELECT *    │ │ SELECT *     │
                    │ FROM table  │ │ FROM table   │
                    │             │ │ WHERE col >  │
                    │             │ │ last_value   │
                    └──────┬──────┘ └──────┬───────┘
                           └───────┬───────┘
                                   ▼
                    ┌──────────────────────────────────┐
                    │    ExecuteSQL (Source DB)          │
                    │    Dynamic query from attributes  │
                    └──────────────┬───────────────────┘
                                   │ (Avro records)
                                   ▼
                    ┌──────────────────────────────────┐
                    │    UpdateAttribute                │
                    │    s3.key = bronze/${table}/      │
                    │    dt=${now():format('yyyy-MM-dd')}│
                    └──────────────┬───────────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────────┐
                    │    PutS3Object (MinIO)            │
                    │    → napas-datalake/bronze/...    │
                    └──────────────┬───────────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────────┐
                    │    Log Execution to metadata      │
                    │    INSERT pipeline_execution_log  │
                    └──────────────────────────────────┘
```

### 4.2 Silver/Gold Transform (NiFi orchestrate → Dremio execute)

```
                    ┌──────────────────────────────────┐
                    │    metadata.transform_rules       │
                    │    WHERE source_layer = 'bronze'  │
                    │    AND target_layer = 'silver'    │
                    │    ORDER BY execution_order       │
                    └──────────────┬───────────────────┘
                                   │
                    ┌──────────────┴───────────────────┐
                    │    SplitRecord: 1 rule/FlowFile   │
                    └──────────────┬───────────────────┘
                                   │
                    ┌──────────────┴───────────────────┐
                    │    EvaluateJsonPath               │
                    │    → sql_template, transform_type │
                    └──────────────┬───────────────────┘
                                   │
                    ┌──────────────┴───────────────────┐
                    │    ReplaceText: Render SQL        │
                    │    ${sql_template} with variables │
                    └──────────────┬───────────────────┘
                                   │
                    ┌──────────────┴───────────────────┐
                    │    ExecuteSQL (Dremio JDBC)       │
                    │    Run transform on Iceberg       │
                    └──────────────┬───────────────────┘
                                   │
                    ┌──────────────┴───────────────────┐
                    │    Log execution result           │
                    └──────────────────────────────────┘
```

---

## 5. Vai Trò Từng Component

| Component | Vai trò trong Metadata-Driven                               |
|-----------|---------------------------------------------------------------|
| **NiFi**  | Orchestrator: đọc metadata, generate SQL, trigger execution   |
|           | Ingestion: extract data từ source → load vào MinIO bronze     |
|           | Transform orchestrator: gọi Dremio SQL cho silver/gold        |
|           | Monitor: log execution, handle errors, retry                  |
| **Dremio**| Metadata store: host Iceberg metadata tables                  |
|           | Transform engine: chạy SQL trên Iceberg (dedup, merge, agg)  |
|           | Serving layer: expose gold tables cho Superset qua Flight SQL |
| **MinIO** | Storage: bronze (Parquet), silver/gold (Iceberg), metadata    |
| **Vault** | Secrets: source DB credentials, MinIO keys (future)           |

---

## 6. So Sánh với Airflow

| Tiêu chí                  | Airflow                    | NiFi Metadata-Driven       |
|---------------------------|----------------------------|-----------------------------|
| Dynamic pipeline          | Jinja template + DAG gen   | Expression Language + FlowFile attributes |
| Metadata config           | YAML/DB → generate DAGs    | Iceberg tables → read at runtime |
| Transform execution       | Call Spark/dbt/SQL          | Call Dremio SQL via JDBC    |
| Scheduling                | Cron in DAG definition     | Cron in NiFi processor     |
| Retry & error handling    | Task-level retry           | Processor-level retry + backpressure |
| Monitoring                | Airflow UI + logs          | NiFi UI + provenance + execution_log |
| Data routing              | XCom, branching            | FlowFile routing, attributes |
| Backpressure              | Pool slots                 | Native backpressure         |
| Visual pipeline           | Code-first                 | Visual drag-drop            |

**Kết luận**: NiFi metadata-driven đạt được hầu hết khả năng dynamic pipeline của Airflow, nhưng approach khác:
- Airflow: **generate** pipeline code từ metadata → static DAGs
- NiFi: **1 generic flow** đọc metadata **at runtime** → dynamic execution

---

## 7. Implementation Roadmap

```
Phase 1: Foundation (Doc 10)
├── Tạo metadata tables trong Dremio (Iceberg)
├── Insert sample config cho demo tables
└── Validate: query metadata từ Dremio UI

Phase 2: Bronze Pipeline (Doc 11)
├── Setup Dremio JDBC driver trong NiFi
├── Xây dựng NiFi Process Groups
├── Test với demo PostgreSQL → MinIO bronze
└── Validate: data xuất hiện trong MinIO bronze/

Phase 3: Silver/Gold Transform (Doc 12)
├── Define SQL templates trong metadata
├── NiFi orchestrate Dremio transforms
├── Test dedup, merge, aggregation
└── Validate: Iceberg tables trong silver/gold

Phase 4: Operations (Doc 13)
├── Onboard bảng mới qua metadata INSERT
├── Setup monitoring và alerting
├── Data quality checks
└── Runbook cho troubleshooting
```

---

## 8. Tài Liệu Liên Quan

| Doc | Nội dung | Mục đích |
|-----|----------|----------|
| [10-metadata-tables-design.md](10-metadata-tables-design.md) | Schema metadata tables | Tạo config tables |
| [11-nifi-dynamic-pipeline-setup.md](11-nifi-dynamic-pipeline-setup.md) | NiFi Process Group setup | Build pipeline |
| [12-dynamic-sql-templates.md](12-dynamic-sql-templates.md) | SQL templates | Bronze/Silver/Gold |
| [13-pipeline-operations-runbook.md](13-pipeline-operations-runbook.md) | Operations guide | Day-to-day ops |
