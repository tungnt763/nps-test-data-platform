# NAPAS Data Platform

Modern Data Lake Stack cho phân tích BigData thanh toán Napas.
Pattern: Kubernetes + Helmfile (tham chiếu từ paraline-platform).

## Stack

| Component        | Vai trò               | Namespace           |
|------------------|-----------------------|---------------------|
| HashiCorp Vault  | Secrets management    | data-security       |
| MinIO            | Object Storage (S3)   | data-storage        |
| Apache NiFi      | Ingestion + Orchestration | data-ingestion  |
| Apache Ranger    | Authorization + Audit | data-governance     |
| Dremio           | SQL Engine (3 layers) | data-processing     |
| Apache Superset  | BI Dashboard          | data-visualization  |

## Quickstart

```bash
cd napas-platform-infra
./scripts/setup.sh dev
./scripts/deploy.sh dev 01-security && ./scripts/init-vault.sh dev
./scripts/deploy.sh dev 02-storage
./scripts/deploy.sh dev 03-ingestion
./scripts/deploy.sh dev 04-governance
./scripts/deploy.sh dev 05-processing
./scripts/deploy.sh dev 06-visualization
./scripts/validate.sh dev
```

## Tài Liệu

| File                              | Nội dung                    |
|-----------------------------------|-----------------------------|
| `docs/00-architecture-overview.md` | Kiến trúc tổng quan        |
| `docs/01-hashicorp-vault.md`      | Vault setup + validate      |
| `docs/02-minio.md`                | MinIO setup + validate      |
| `docs/03-apache-ranger.md`        | Ranger setup + validate     |
| `docs/04-apache-nifi.md`          | NiFi setup + validate       |
| `docs/05-dremio.md`               | Dremio setup + validate     |
| `docs/06-apache-superset.md`      | Superset setup + validate   |
| `docs/07-integration-guide.md`    | End-to-end pipeline         |

## Dữ liệu Flow

```
Oracle/MySQL → NiFi (2h AM) → MinIO bronze/
                             → Dremio → silver/ → gold/
                                                 → Superset Dashboard
```

## Phân Quyền

- **Data Engineers**: full access bronze + silver + gold
- **Analysts**: read-only gold, card_number bị mask
- **Business Users**: chỉ xem Dashboard trong Superset
