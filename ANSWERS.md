# ANSWERS — Day 28 Track 2

Sinh viên: Trần Xuân Lộc (`2A202601671`)  
Nhánh: `ca-nhan-loc`  
Cách làm: cá nhân

## 1. Phần đã hoàn thành trên máy này

Đã hoàn thiện bốn hàm trong `src/lab28_platform/integration_tasks.py`. Chúng được
Kafka, Delta MERGE, Feast client và `/ready` gọi trực tiếp.

| Hàm | Boundary | Quyết định chính |
|---|---|---|
| `event_headers` | IP01 + IP10 | Luôn gửi `idempotency-key`. Chỉ gửi `traceparent` khi có giá trị thật; chuỗi rỗng bị bỏ. |
| `dedupe_latest` | IP03 | Một event / `idempotency_key`; thắng theo `(occurred_at, event_id)`; sort theo key. |
| `feast_online_request` | IP04 | `entities`, `FEATURE_REFS` từ `contracts.py`, `full_feature_names=false`. |
| `readiness_status` | IP07 + IP08 | `mandatory` fail → `not_ready`; chỉ optional fail → `degraded`; còn lại `ready`. |

Kiểm tra **offline**:

- `uv run pytest starter-tests tests -q` → **87 passed** (111.74s)
- `uv run ruff check .` → All checks passed
- `uv run python scripts/verify_matrix.py` → 245 checks passed
- `uv run python scripts/check_portability.py` → OK
- `uv run python scripts/validate_manifests.py` → Kubernetes/GitOps contracts passed
- `docker compose --env-file ports.template config --quiet` → OK (core + `--profile full`)

`uv run lab28 preflight` trên máy này: `profile=local-standard`, Docker daemon **up**,
`disk_free_gib=15.2`, 12 CPU.

### Core stack đã chạy

Đã prune Docker build cache (~14.5 GB), rồi:

```text
docker compose --env-file ports.template up -d --build --wait
uv run lab28 topics
uv run lab28 index --source file
$env:PYTHONUTF8="1"; uv run lab28 release
uv run lab28 seed --via-gateway   # 4 feedback bị 429 vì token bucket 10/s
uv run lab28 seed                 # API trực tiếp: 13 documents + 12 feedback accepted
```

Kết quả live:

- Kafka topics `data.raw`, `data.processed`, `model.events`, `data.raw.dlq` **created**
- Qdrant: **13 points** indexed (hybrid, model `paraphrase-multilingual-MiniLM-L12-v2`)
- MLflow: `lab28-rag-release` **v3 champion** (`run_id=1f2fc3871fc1487ea97238f97ccca06f`)
- API `/ready` (trong container, `LAB28_VLLM_REQUIRE_REAL=false`): **`degraded`** vì vLLM unreachable — đúng kỳ vọng core stack
- Host `lab28 ready` báo `not_ready` vì CLI đọc `require_real=true` mặc định; lấy verdict từ `http://localhost:8000/ready`

Workaround Qdrant: image `qdrant/qdrant:v1.19.0` có `./entrypoint.sh` **0 byte**, nên
`compose.yaml` chạy `command: ["./qdrant"]`. Không giả image khác.

Windows: `lab28 release` lần đầu fail khi MLflow in emoji ra stdout (`cp1252`).
Chạy lại với `$env:PYTHONUTF8="1"` thì promote thành công.

## 2. Trade-offs kỹ thuật

### Header Kafka: bỏ `traceparent` rỗng thay vì gửi placeholder

W3C `traceparent` có dạng cố định. Header rỗng hoặc giả sẽ làm collector/Jaeger
ghép sai span, trong khi IP10 đòi **cùng một trace ID** xuyên HTTP → Kafka →
Airflow → Delta → response. Chi phí: caller phải chủ động truyền context; lợi:
replay vẫn khớp được bằng `idempotency-key` dù không có trace.

Evidence live: `evidence/ip01-kafka-consume.json` — 46 message trên `data.raw`,
**46 có `traceparent`**. Trace `0e83f9a0945a5626afe7dad0ba46b883` xuất hiện cùng lúc
trên Kafka header, payload, và Jaeger (`lab28.gateway.request` + `lab28.api.ingest`
+ `lab28.kafka.produce`).

### Dedup ở Python trước MERGE, không để Spark tự “gộp”

Delta MERGE lỗi khi source có hai dòng cùng merge key. Dedup trong
`dedupe_latest` kiểm tra được bằng unit test, không cần JVM. Tie-break
`(occurred_at, event_id)` độc lập thứ tự Kafka partition. Sort theo key để
output deterministic. Chi phí: batch phải đọc hết một lần vào memory — đúng với
cỡ corpus lab, chưa phải pipeline hàng triệu event.

Core stack **chưa** chạy Spark/Airflow nên chưa có `_delta_log`. Hàm đã được
unit-test; MERGE live là việc của `--profile full`.

### Feature refs lấy từ `contracts.FEATURE_REFS`

Một nguồn sự thật cho registry Feast, request online và serving. Tránh lệch tên
`asker_activity_v1:*` giữa Python và `feature-repo/definitions.py`.
`full_feature_names=false` khớp contract serving (`AskerFeatures` dùng tên ngắn).

`POST /get-online-features` trả HTTP 200 với entity `asker-001` **PRESENT**, các
feature **NOT_FOUND** — Feast server sống, snapshot schema-only, chưa materialize
từ Delta.

### Readiness tách bắt buộc / không bắt buộc

Qdrant, Kafka, MLflow champion là đường serving cốt lõi → fail thì `not_ready`.
Feast lạnh và vLLM khi `require_real=false` chỉ `degraded`. Envoy health-check
**`/health` chứ không `/ready`**, để học viên nhìn breakdown thay vì “no healthy
upstream”.

Gateway rate limit 10 token/s: burst `/health` cho **200** (`x-request-id`) và
**429** (`x-lab28-rate-limited: true`). Seed qua gateway cũng bị 429 — đó là
policy thật, không phải lỗi API.

### Compose vs Kubernetes/GitOps

Compose đủ để chứng minh phần lớn boundary trên laptop. Manifest K8s mang
production shape: non-root, read-only rootfs, drop ALL caps, NetworkPolicy,
HPA/PDB, probes tách liveness/readiness/startup. Argo CD `selfHeal: true` +
`revisionHistoryLimit: 5` — rollback là đổi `targetRevision` (`refs/tags/v3.0.0`)
hoặc image tag trên Git, không `kubectl edit` live.
`uv run python scripts/validate_manifests.py` đã pass.

### GPU/vLLM

IP07 bắt buộc server thật (`/version` vLLM, `/v1/models`, metric `vllm:`). Máy
này không có endpoint; core stack `degraded` cho LLM. `/api/v1/ask` trả 503
`dependency_unavailable` — **không** mock OpenAI-compatible.
`evidence/ip07-vllm-identity.json` ghi `reachable: false` (UNVERIFIED trung thực).

## 3. Production gaps (cần nói khi demo)

1. **Kafka lab:** 1 broker, RF=1, plaintext — mất node là mất dữ liệu; production
   cần RF≥3, ACL, TLS.
2. **Gateway:** local rate limit 10 token/s, không auth/JWT. Đủ chứng minh 429,
   không đủ cho internet-facing API.
3. **Secret:** Grafana `admin/admin` trong Compose là lab default; không commit
   `.env`/token production.
4. **Quan sát:** Prometheus/Grafana/Jaeger local. LangSmith **UNVERIFIED** (không
   có `LANGSMITH_API_KEY`).
5. **Load test laptop ≠ capacity production.** P50/P95/P99 chưa chạy
   `load-tests/run_profile.py` trên máy này vì `/ask` không có vLLM.
6. **Disk/RAM:** ~15 GB trống; `--profile full` (Spark + Airflow) khuyến nghị
   20 GB — chưa bật trên máy này.
7. **IP07:** UNVERIFIED cho đến khi có vLLM GPU (local NVIDIA hoặc Kaggle T4 theo
   `KAGGLE_GPU_EXTENSION.md`).
8. **Corpus:** 13 documents + 12 feedback — đủ integration, không phải retrieval
   benchmark.
9. **Qdrant v1.19.0 entrypoint rỗng:** pin image lab bị lỗi packing; production
   phải verify digest/`entrypoint` trước khi roll.

## 4. Bằng chứng — file live, không bịa

`evidence/` được theo dõi trên Git để nộp cùng repo (không còn bị `.gitignore`).

| File | Trạng thái | Nguồn |
|---|---|---|
| `evidence/ip01-kafka-consume.json` | **live** | consume `data.raw`, 46 msg, header `traceparent` |
| `evidence/ip02-airflow-run.json` | **UNVERIFIED** | cần `--profile full` |
| `evidence/ip03-delta-history.json` | **UNVERIFIED** | `LakehouseUnavailable` — chưa Spark MERGE |
| `evidence/ip04-feast-online.json` | **live / lạnh** | Feast 200, features `NOT_FOUND` đến khi materialize |
| `evidence/ip05-qdrant-search.json` | **live** | 13 points, hybrid scores |
| `evidence/ip06-mlflow-release.json` | **live** | champion v3 |
| `evidence/ip07-vllm-identity.json` | **UNVERIFIED** | `ConnectError` — không giả server |
| `evidence/ip08-gateway.json` | **live** | 200 + 429 + `x-request-id` |
| `evidence/ip09-prometheus-targets.json` | **live** | mọi job `up` trừ `lab28-vllm-optional` `down` |
| `evidence/ip09-grafana-dashboards.json` | **live** | dashboard `lab28-platform`, datasource Prometheus |
| `evidence/ip10-trace.json` | **live một phần** | trace `0e83f9a0945a5626afe7dad0ba46b883`: gateway + ingest + kafka.produce. Thiếu consume/Airflow/Spark/ask/vLLM |
| `evidence/integration-report.json` | **live** | `lab28 evidence` |
| load profile P50/P95/P99 | **chưa chạy** | cần vLLM |

UI để đối chiếu khi demo:

| UI | URL |
|---|---|
| Gateway | http://localhost:8080/healthz |
| API `/ready` | http://localhost:8000/ready |
| Grafana | http://localhost:3000 (`admin`/`admin`) |
| Prometheus targets | http://localhost:9090/targets |
| Jaeger ingest trace | http://localhost:16686/trace/0e83f9a0945a5626afe7dad0ba46b883 |
| MLflow champion | http://localhost:5000 |
| Qdrant | http://localhost:6333/dashboard |

Sơ đồ kiến trúc: `docs/images/lab28-architecture-overview.png`.

## 5. Việc còn lại (máy đủ mạnh / GPU / giảng viên)

1. `docker compose --env-file ports.template --profile full up -d --build --wait`
2. `uv run pytest integration-tests/test_j1_golden_path.py` → IP02/IP03/IP04 đầy đủ
3. `uv run pytest integration-tests -m "not gpu and not langsmith"`
4. Nối vLLM thật (Kaggle T4 hoặc NVIDIA local) rồi `lab28 evidence` lại cho IP07
5. `uv run python load-tests/run_profile.py --requests 200 --workers 8`
6. Demo theo `docs/demo-runbook.md`: incident (stop Feast/Qdrant), MLflow rollback
   (`lab28 rollback`), GitOps self-heal

## 6. Contribution

Làm cá nhân. Adapter + fast suite + ruff + matrix/portability/manifest + core
Compose + evidence live IP01/IP04(lạnh)/IP05/IP06/IP08/IP09/IP10(một phần).
IP02/IP03/IP07 và J1–J5 đầy đủ chờ full profile + GPU.
