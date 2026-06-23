# Step 1: HashiCorp Vault — Secrets Management

> **Deploy order:** 1/6 (PHẢI deploy đầu tiên — tất cả services khác lấy credentials từ đây)
> **Namespace:** `data-security`

---

## 1. HashiCorp Vault Là Gì?

HashiCorp Vault là một **secrets management platform** — hệ thống lưu trữ, truy cập, và quản lý vòng đời của thông tin nhạy cảm (secrets) như password, API key, certificate, và encryption key.

**Vấn đề Vault giải quyết:**

```
❌ Không dùng Vault — Anti-pattern:
   docker-compose.yml:
     environment:
       ORACLE_PASSWORD: "napas@Oracle2024!"   ← hardcode, lộ vào git
       MINIO_SECRET_KEY: "minio-secret-123"   ← tất cả dev biết prod password

✅ Dùng Vault:
   Mỗi service tự hỏi Vault lấy secret tại runtime
   Secret không bao giờ xuất hiện trong code/config file
   Rotation tự động không cần restart service
```

---

## 2. Kiến Trúc Vault

```
┌─────────────────────────────────────────────────────────────┐
│                    HashiCorp Vault                           │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │                  API Layer (:8200)                   │   │
│  │  HTTP REST API — mọi interaction đều qua đây        │   │
│  └─────────────────────────┬───────────────────────────┘   │
│                             │                               │
│  ┌──────────────────────────▼───────────────────────────┐  │
│  │               Security Barrier (AES-256-GCM)         │  │
│  │   Tất cả dữ liệu được mã hóa trước khi ghi storage  │  │
│  └──────────────────────────┬───────────────────────────┘  │
│                             │                               │
│  ┌─────────────┬────────────┴──────────┬─────────────────┐ │
│  │  Auth       │   Secret Engines      │  Audit Backends │ │
│  │  Methods    │                       │                  │ │
│  │  • Token    │  • KV v2 (key-value)  │  • File log     │ │
│  │  • K8s SA   │  • Database (dynamic) │  • Syslog       │ │
│  │  • AppRole  │  • AWS / GCP          │                  │ │
│  │  • LDAP     │  • PKI (certs)        │                  │ │
│  └─────────────┴───────────────────────┴─────────────────┘ │
│                             │                               │
│  ┌──────────────────────────▼───────────────────────────┐  │
│  │               Storage Backend                         │  │
│  │   Local dev: Integrated Raft (embedded etcd-like)    │  │
│  │   Production: Raft HA (3-node cluster)               │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Các Khái Niệm Chính

| Khái niệm   | Giải thích                                                                 |
|-------------|----------------------------------------------------------------------------|
| **Secret**  | Cặp key-value nhạy cảm: `{"db_password": "s3cr3t"}`                       |
| **Path**    | Địa chỉ của secret: `napas/nifi/oracle-credentials`                        |
| **Token**   | Chìa khóa truy cập Vault. Có TTL, có thể revoke                           |
| **Policy**  | Định nghĩa quyền: `path "napas/nifi/*" { capabilities = ["read"] }`       |
| **Seal/Unseal** | Vault khởi động ở trạng thái "sealed" (mã hóa hoàn toàn). Cần unseal trước khi dùng |
| **Lease**   | Thời gian sống của dynamic secret. Khi hết hạn, secret tự xóa            |

---

## 3. Vault Hoạt Động Như Thế Nào?

### 3.1. Quy Trình Đọc Secret (ví dụ NiFi lấy Oracle password)

```
NiFi Pod                    Vault Server             Oracle DB
    │                           │                        │
    │  1. Gửi K8s ServiceAccount│                        │
    │     token đến Vault       │                        │
    │──────────────────────────▶│                        │
    │                           │                        │
    │  2. Vault verify token    │                        │
    │     với K8s API Server    │                        │
    │                           │                        │
    │  3. Vault trả về Vault    │                        │
    │     Token (TTL: 1h)       │                        │
    │◀──────────────────────────│                        │
    │                           │                        │
    │  4. NiFi đọc secret       │                        │
    │     GET /v1/napas/nifi/   │                        │
    │         oracle-creds      │                        │
    │──────────────────────────▶│                        │
    │                           │                        │
    │  5. Trả về secret         │                        │
    │     {"username":"napas",  │                        │
    │      "password":"..."}    │                        │
    │◀──────────────────────────│                        │
    │                           │                        │
    │  6. NiFi kết nối Oracle   │                        │
    │  với credentials nhận được│                        │
    │──────────────────────────────────────────────────▶│
```

### 3.2. Vault Tương Tác Với Các Component Khác

| Component     | Cách lấy secret từ Vault          | Secret gì                        |
|---------------|-----------------------------------|----------------------------------|
| NiFi          | Vault Agent sidecar / REST API    | Oracle JDBC URL+password, MinIO keys |
| Dremio        | Environment variables inject      | MinIO access/secret key          |
| Ranger        | Init container đọc trước khi start| PostgreSQL password              |
| Superset      | Init container đọc trước khi start| PostgreSQL password, secret key  |

---

## 4. Ưu Điểm & Nhược Điểm

| Ưu điểm                                      | Nhược điểm                              |
|----------------------------------------------|-----------------------------------------|
| Zero-trust: không hardcode secrets anywhere   | Thêm dependency (nếu Vault down, service không start) |
| Dynamic secrets: tự sinh & xóa DB credentials| Phức tạp hơn khi setup lần đầu         |
| Audit log đầy đủ: ai đọc secret gì, lúc nào | Cần unseal thủ công sau mỗi lần restart |
| Secret rotation không restart service        | Raft HA cần tối thiểu 3 nodes cho prod |
| Tích hợp K8s native (ServiceAccount auth)    |                                         |

---

## 5. Cài Đặt — Local Dev (Docker Compose)

### 5.1. Thêm vào `docker-compose.yml` (layer 1)

```yaml
# napas-platform-infra/platform/values/base/vault.yaml
# Helm values cho HashiCorp Vault chart (official)

server:
  # Dev mode: auto-unseal, in-memory (KHÔNG dùng cho production)
  dev:
    enabled: false

  # Standalone mode với Raft storage (phù hợp dev + prod)
  standalone:
    enabled: true
    config: |
      ui = true

      listener "tcp" {
        tls_disable = 1
        address = "[::]:8200"
        cluster_address = "[::]:8201"
      }

      storage "raft" {
        path = "/vault/data"
        node_id = "vault-0"
      }

      service_registration "kubernetes" {}

  # Resources
  resources:
    requests:
      memory: "256Mi"
      cpu: "250m"
    limits:
      memory: "512Mi"
      cpu: "500m"

  # Persistent volume cho Raft storage
  dataStorage:
    enabled: true
    size: 10Gi
    storageClass: "platform-standard"

# UI mặc định bật
ui:
  enabled: true
  serviceType: "ClusterIP"

# Injector để inject secrets vào pods (Vault Agent)
injector:
  enabled: true
  resources:
    requests:
      memory: "256Mi"
      cpu: "250m"
```

### 5.2. Environment-specific overrides

```yaml
# platform/values/env/vault.yaml.gotmpl
server:
  standalone:
    config: |
      ui = true

      listener "tcp" {
        tls_disable = {{ if eq .Values.global.env "prod" }}0{{ else }}1{{ end }}
        address = "[::]:8200"
      }

      storage "raft" {
        path = "/vault/data"
        node_id = "vault-0"
      }

      service_registration "kubernetes" {}

  dataStorage:
    size: {{ .Values.vault.storage }}
```

```yaml
# platform/environments/dev.yaml (thêm vault section)
vault:
  storage: 10Gi
  rootTokenSecretName: ""  # sẽ tạo thủ công lúc init
```

---

## 6. Cài Đặt — Kubernetes (Helm)

### 6.1. Thêm Vault vào Helmfile

```yaml
# platform/helmfile.yaml.gotmpl (layer 01-security)
releases:
  - name: vault
    namespace: data-security
    chart: hashicorp/vault
    version: "0.28.1"
    values:
      - values/base/vault.yaml
      - values/env/vault.yaml.gotmpl
    labels:
      layer: 01-security
```

### 6.2. Deploy

```bash
# Deploy Vault
./scripts/deploy.sh dev 01-security

# Kiểm tra pod đã chạy
kubectl get pods -n data-security
# NAME       READY   STATUS    RESTARTS   AGE
# vault-0    0/1     Running   0          30s  ← 0/1 = chưa unseal, BÌNH THƯỜNG
```

---

## 7. Khởi Tạo & Unseal Vault (Bắt Buộc Sau Deploy Đầu Tiên)

```bash
# Bước 1: Khởi tạo Vault (CHỈ làm 1 lần duy nhất)
kubectl exec -n data-security vault-0 -- vault operator init \
  -key-shares=5 \
  -key-threshold=3 \
  -format=json > vault-init.json

# LƯU FILE NÀY LẠI! Mất là mất toàn bộ data!
# vault-init.json chứa:
# - 5 unseal keys (cần ít nhất 3 trong 5 để unseal)
# - 1 root token (dùng để admin ban đầu)

# Bước 2: Unseal (cần chạy mỗi lần Vault pod restart)
# Lấy 3 unseal keys từ file
UNSEAL_KEY_1=$(cat vault-init.json | python3 -c "import sys,json; print(json.load(sys.stdin)['unseal_keys_b64'][0])")
UNSEAL_KEY_2=$(cat vault-init.json | python3 -c "import sys,json; print(json.load(sys.stdin)['unseal_keys_b64'][1])")
UNSEAL_KEY_3=$(cat vault-init.json | python3 -c "import sys,json; print(json.load(sys.stdin)['unseal_keys_b64'][2])")

kubectl exec -n data-security vault-0 -- vault operator unseal $UNSEAL_KEY_1
kubectl exec -n data-security vault-0 -- vault operator unseal $UNSEAL_KEY_2
kubectl exec -n data-security vault-0 -- vault operator unseal $UNSEAL_KEY_3

# Kiểm tra trạng thái
kubectl exec -n data-security vault-0 -- vault status
```

---

## 8. Cấu Hình Vault Sau Khi Unseal

```bash
# Lấy root token
ROOT_TOKEN=$(cat vault-init.json | python3 -c "import sys,json; print(json.load(sys.stdin)['root_token'])")

# Port-forward để access local
kubectl port-forward -n data-security svc/vault 8200:8200 &

# Login
export VAULT_ADDR="http://localhost:8200"
vault login $ROOT_TOKEN

# Bước 3: Enable KV v2 secrets engine cho NAPAS
vault secrets enable -path=napas kv-v2

# Bước 4: Enable Kubernetes auth method
vault auth enable kubernetes

# Cấu hình K8s auth (cho phép pods dùng ServiceAccount để auth với Vault)
vault write auth/kubernetes/config \
  kubernetes_host="https://kubernetes.default.svc:443"

# Bước 5: Tạo secrets cho từng service

# MinIO credentials (NiFi và Dremio sẽ dùng)
vault kv put napas/minio/credentials \
  access_key="napas-admin" \
  secret_key="napas-minio-s3cr3t-2024"

# Oracle DB credentials (NiFi dùng để query source)
vault kv put napas/nifi/oracle-source \
  jdbc_url="jdbc:oracle:thin:@oracle-host:1521:NAPASDB" \
  username="napas_reader" \
  password="OracleR3ader@2024"

# PostgreSQL credentials (Ranger metastore)
vault kv put napas/ranger/postgres \
  host="postgres.data-governance.svc.cluster.local" \
  port="5432" \
  database="ranger" \
  username="ranger" \
  password="Ranger@Postgres2024"

# Superset secret key
vault kv put napas/superset/config \
  secret_key="$(openssl rand -base64 42)" \
  postgres_password="Superset@Postgres2024"

# Bước 6: Tạo policy cho từng service
vault policy write nifi-policy - <<EOF
path "napas/data/minio/*" { capabilities = ["read"] }
path "napas/data/nifi/*"  { capabilities = ["read"] }
EOF

vault policy write dremio-policy - <<EOF
path "napas/data/minio/*" { capabilities = ["read"] }
EOF

vault policy write ranger-policy - <<EOF
path "napas/data/ranger/*" { capabilities = ["read"] }
EOF

vault policy write superset-policy - <<EOF
path "napas/data/superset/*" { capabilities = ["read"] }
EOF

# Bước 7: Bind policy với K8s ServiceAccount
vault write auth/kubernetes/role/nifi-role \
  bound_service_account_names=nifi \
  bound_service_account_namespaces=data-ingestion \
  policies=nifi-policy \
  ttl=1h

vault write auth/kubernetes/role/dremio-role \
  bound_service_account_names=dremio \
  bound_service_account_namespaces=data-processing \
  policies=dremio-policy \
  ttl=1h

vault write auth/kubernetes/role/ranger-role \
  bound_service_account_names=ranger \
  bound_service_account_namespaces=data-governance \
  policies=ranger-policy \
  ttl=1h
```

---

## 9. Validate Vault Hoạt Động Đúng

### Checklist Validate

```bash
# ✅ Check 1: Vault pod READY (phải là 1/1, không phải 0/1)
kubectl get pods -n data-security
# Expected:
# NAME      READY   STATUS    RESTARTS   AGE
# vault-0   1/1     Running   0          5m

# ✅ Check 2: Vault status = Sealed: false
kubectl exec -n data-security vault-0 -- vault status
# Expected output:
# Key             Value
# ---             -----
# Seal Type       shamir
# Initialized     true
# Sealed          false    ← QUAN TRỌNG: phải là false
# Total Shares    5
# Threshold       3
# Version         1.16.x
# HA Enabled      false

# ✅ Check 3: Đọc được secret (dùng root token)
kubectl port-forward -n data-security svc/vault 8200:8200 &
export VAULT_ADDR="http://localhost:8200"
ROOT_TOKEN=$(cat vault-init.json | python3 -c "import sys,json; print(json.load(sys.stdin)['root_token'])")
vault login $ROOT_TOKEN
vault kv get napas/minio/credentials
# Expected:
# ====== Secret Path ======
# napas/data/minio/credentials
# ======= Metadata =======
# Key        Value
# version    1
# ====== Data ======
# Key           Value
# access_key    napas-admin
# secret_key    napas-minio-s3cr3t-2024

# ✅ Check 4: K8s auth hoạt động (simulate từ NiFi namespace)
kubectl run vault-test \
  --image=hashicorp/vault:1.16.0 \
  --rm -it \
  --restart=Never \
  -n data-ingestion \
  --serviceaccount=default \
  -- sh -c '
    KUBE_TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
    curl -s -X POST http://vault.data-security.svc.cluster.local:8200/v1/auth/kubernetes/login \
      -d "{\"jwt\": \"$KUBE_TOKEN\", \"role\": \"nifi-role\"}" | python3 -m json.tool
  '
# Expected: JSON với "client_token" field

# ✅ Check 5: UI accessible
kubectl port-forward -n data-security svc/vault 8200:8200
# Mở browser: http://localhost:8200
# Login bằng Method: Token, dùng root token
# Kiểm tra: Secrets Engines > napas/ có các keys đã tạo
```

### Kết Quả Validate Thành Công

```
[✅] vault-0 pod: 1/1 Running
[✅] Sealed: false
[✅] KV secrets đọc được
[✅] Kubernetes auth hoạt động
[✅] UI accessible tại localhost:8200
→ Vault sẵn sàng — Proceed to Step 2: MinIO
```

---

## 10. Tips & Lưu Ý Quan Trọng

```
⚠️  vault-init.json chứa root token và unseal keys — KHÔNG commit vào git!
    Thêm vào .gitignore: vault-init.json

⚠️  Sau mỗi lần Vault pod restart (node reboot, upgrade...) cần unseal lại.
    → Production: dùng Auto-unseal với AWS KMS hoặc GCP Cloud KMS.

⚠️  Root token chỉ dùng để setup ban đầu. Tạo admin token riêng với policy
    hạn chế hơn để dùng hàng ngày.

💡  Vault UI tại http://localhost:8200 rất tiện để kiểm tra secrets,
    policies, và audit logs trong quá trình development.
```
