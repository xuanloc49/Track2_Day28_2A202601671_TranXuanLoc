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

- `uv run pytest starter-tests tests -q` → **87 passed**
- `uv run ruff check .` → All checks passed
- `uv run python scripts/verify_matrix.py` → 245 checks passed
- `uv run python scripts/check_portability.py` → OK
- `uv run python scripts/validate_manifests.py` → Kubernetes/GitOps contracts passed
- `docker compose --env-file ports.template config --quiet` → OK (core + `--profile full`)

### Core stack rồi `--profile full`

Đã prune Docker build cache (~14.5 GB), chạy core stack, sau đó:

```text
docker compose --env-file ports.template --profile full up -d --build --wait
```

Spark Connect và Airflow **healthy**. Integration live (`not gpu and not langsmith`):

- IT-J1: **12 passed**, 3 skipped (gpu)
- IT-J2 + J3 + J5 + gateway + prometheus + span contract: **35 passed**, 9 deselected
- IT-J4: **9 passed**, 4 deselected (gpu)
- Chi tiết: `evidence/pytest-integration.txt`

Happy-path IDs (IT-J1):

- DAG run `it-2ec2f014`, state `success`
- trace ID `6713481a7e19430b8851c66f2c771b46`
- Delta feedback **v1** (13 rows MERGE), documents **v1** (14 rows)
- Feast entity `it-j1-084d406b` với `feedback_count=1`, `delta_version=1`
- IT-J5 trace `45c8b110fbb94076841a0ef98e56bee4` mang gateway → ingest → Kafka produce/consume → Airflow DAG → Spark MERGE

Workaround Qdrant: image `qdrant/qdrant:v1.19.0` có `./entrypoint.sh` **0 byte**, nên
`compose.yaml` chạy `command: ["./qdrant"]`. Không giả image khác.

Windows: `lab28 release` cần `$env:PYTHONUTF8="1"` vì MLflow in emoji ra stdout (`cp1252`).

### LangSmith (OTLP thứ hai)

Collector export `otlphttp/langsmith` → `https://api.smith.langchain.com/otel/v1/traces`.
`compose.yaml` truyền `LANGSMITH_API_KEY` / `LANGSMITH_PROJECT` từ `.env` (không
ghi secret vào YAML). Khởi động:

```text
docker compose --env-file ports.template --env-file .env --profile full up -d --build --wait
```

Pytest: `uv run --env-file .env pytest integration-tests -m langsmith` → **1 passed**.
Prometheus thấy 3 span exporter, gồm `otlphttp/langsmith` (138 spans sent).


### GPU / vLLM / `/ask`

Image thật `vllm/vllm-openai:v0.28.0`. Overlay `compose.gpu.yaml`:
`VLLM_USE_V2_MODEL_RUNNER=0`, `VLLM_WSL2_ENABLE_PIN_MEMORY=1` (WSL không có UVA),
`--dtype half --max-model-len 1024 --gpu-memory-utilization 0.70 --enforce-eager
--max-num-seqs 1`. Model `Qwen/Qwen3-0.6B` vì Qwen3-1.7B không vừa 4 GB VRAM.

`POST /api/v1/ask` qua gateway **HTTP 200** trên vLLM thật:

- trace `4c48cdeed2354d0a8046bbf907b46847`
- champion `lab28-rag-release` v3, `vllm_model_id=Qwen/Qwen3-0.6B`
- `prompt_tokens=509`, `completion_tokens=64` (câu trả lời bị cắt ở token limit → `degraded`)
- Jaeger có đủ 6 span serving: `lab28.gateway.request`, `lab28.api.ask`,
  `lab28.feast.get_online_features`, `lab28.qdrant.query`,
  `lab28.mlflow.resolve_release`, `lab28.vllm.chat_completion`

Pytest `-m gpu` **không Spark** (Spark Connect đã OOM 137 trên Docker VM 8 GB):
**8 passed** (Prometheus scrape vLLM, IT-J3 serving theo alias, IT-J4 degraded
+ gateway eject). 7 test gpu còn lại cần DAG + `/ask` cùng một trace ID — chưa
chạy. Chi tiết: `evidence/pytest-gpu.txt`.

Envoy `health_check` dùng **`/ready`** (timeout 8s) và `healthy_panic_threshold: 0`
để một upstream `not_ready` bị đẩy khỏi rotation thay vì panic-mode.

## 2. Trade-offs kỹ thuật

### Header Kafka: bỏ `traceparent` rỗng thay vì gửi placeholder

W3C `traceparent` có dạng cố định. Header rỗng hoặc giả sẽ làm collector/Jaeger
ghép sai span, trong khi IP10 đòi **cùng một trace ID** xuyên HTTP → Kafka →
Airflow → Delta. Chi phí: caller phải truyền context; lợi: replay vẫn khớp
`idempotency-key` dù không có trace.

### Dedup ở Python trước MERGE, không để Spark tự “gộp”

Delta MERGE lỗi khi source có hai dòng cùng merge key. `dedupe_latest` unit-test
được, không cần JVM. Tie-break `(occurred_at, event_id)`. IT-J2 chứng minh
replay không nhân bản hàng; IT-J4 chứng minh poison message không kéo hàng tốt
xuống theo.

### Feature refs lấy từ `contracts.FEATURE_REFS`

Một nguồn sự thật cho registry Feast, request online và serving.
`full_feature_names=false` khớp `AskerFeatures`. Sau materialize (full profile),
online row có `PRESENT` + `delta_version` (xem `evidence/ip04-feast-online.json`).

### Readiness tách bắt buộc / không bắt buộc

Qdrant / Kafka / MLflow champion / vLLM là mandatory. Feast optional → stop Feast
không đưa pod ra `not_ready` (IT-J4 gpu). Stop Qdrant → API `/ready` 503; Envoy
health-check **`/ready`** nên gateway trả 503 *của Envoy* (không body `components`),
trong khi gọi thẳng API vẫn trả JSON breakdown.

Timeout health-check 8s vì `/ready` trên stack này ~3s (vLLM probe). Timeout 2s
sẽ flap host duy nhất.

Gateway 10 token/s: burst cho **200** và **429** (`x-lab28-rate-limited`).

### Compose vs Kubernetes/GitOps

Compose chứng minh 10 boundary trên laptop. Manifest K8s: non-root, read-only
rootfs, drop ALL caps, NetworkPolicy, HPA/PDB, probes tách. Argo CD
`selfHeal: true` + `revisionHistoryLimit: 5` — rollback đổi `targetRevision`
(`refs/tags/v3.0.0`) trên Git, không `kubectl edit`. Validate tĩnh đã pass;
**sync live trên cluster UNVERIFIED** (không có Argo CD local).

### GPU/vLLM trên 4 GB + Docker VM 8 GB

Qwen3-1.7B không vừa VRAM còn ~3.2/4.0 GiB. Serving dùng 0.6B. `max-model-len`
phải 1024: prompt RAG + template MLflow + 3 docs ~509 token; 512 làm vLLM trả
400 và `/ask` 503.

Không chạy đồng thời Spark Connect + vLLM: Spark 137, engine Docker 500. IP10
do đó **hai trace live** (ingest+pipeline, rồi serving) — union đủ 11 span
matrix, không phải một ID xuyên suốt.

## 3. Production gaps (cần nói khi demo)

1. **Kafka lab:** 1 broker, RF=1, plaintext.
2. **Gateway:** rate limit 10 token/s, không JWT. Health-check `/ready` nghĩa là
   ingest cũng bị eject khi vLLM/Qdrant down (một process vừa ingest vừa serve).
3. **Secret:** Grafana `admin/admin` là lab default; không commit `.env`.
4. **LangSmith live:** collector export `otlphttp/langsmith` + project `lab-28`.
   Pytest `-m langsmith` **1 passed**. Key chỉ nằm trong `.env` (gitignored).
5. **Load probe** bắn `/ready` (stdlib script), không phải `/ask`. P95/P99 bị
   token bucket 10 rps làm trễ — đó là bottleneck cạnh tranh cổng, không phải
   latency vLLM. `/ask` thật ~4.8s (llm ~3.6s) trên 0.6B.
6. **RAM:** laptop ~16 GB; `.wslconfig` `memory=10GB`. Spark + Airflow + vLLM
   cùng lúc vẫn dễ OOM. Session này chỉ bật `--profile full` (không GPU).
7. **Câu trả lời truncated** ở `max_tokens=64` → `degraded` dù inference thật.
8. **GitOps self-heal live:** chưa có cluster; chỉ validate manifest.
9. **Cùng một trace ID cho 11 span:** chưa chứng minh (cần Spark+vLLM cùng lúc).

## 4. Bằng chứng — file live, không bịa

`evidence/` được theo dõi trên Git để nộp cùng repo.

| File | Trạng thái | Nguồn |
|---|---|---|
| `evidence/ip01-kafka-consume.json` | **live** | IT-J1: key `it-j1-084d406b`, trace `6713481a7e19430b8851c66f2c771b46` |
| `evidence/ip02-airflow-run.json` | **live** | DAG `lab28_ingestion_pipeline` run `it-2ec2f014` success + asset events |
| `evidence/ip03-delta-history.json` | **live** | MERGE feedback v1 (13 rows), documents v1 (14 rows), time travel |
| `evidence/ip04-feast-online.json` | **live** | entity J1, features PRESENT, `delta_version=1` |
| `evidence/ip05-qdrant-search.json` | **live** | 14 points, hybrid scores |
| `evidence/ip06-mlflow-release.json` | **live** | IT-J3 promote v3→v4 (`run_id=19e1ae8944a745c09caf3b3fe725a6b1`) |
| `evidence/ip07-vllm-identity.json` | **live** | vLLM 0.28.0, `Qwen/Qwen3-0.6B`, 111 metric `vllm:` |
| `evidence/ip07-ask-serving.json` | **live** | `/ask` 200, model 0.6B, 6 serving spans trên Jaeger |
| `evidence/ip08-gateway.json` | **live** | 200 + 429 + `x-request-id` |
| `evidence/ip09-prometheus-targets.json` | **live** | targets + alert rules (snapshot trước GPU; scrape vLLM **up** lúc chạy gpu test) |
| `evidence/ip09-grafana-dashboards.json` | **live** | dashboard `lab28-platform` |
| `evidence/ip10-trace.json` | **live, hai trace** | ingest `45c8b110…` + serving `4c48cdee…`; union đủ 11 required spans |
| `evidence/ip10-langsmith.json` | **live** | project `lab-28`; 3 exporter (`otlp/jaeger`, `otlphttp/langsmith`, `debug`); 138 spans sent |
| `evidence/pytest-langsmith.txt` | **live** | `pytest -m langsmith` → 1 passed |
| `evidence/integration-report.json` | **live** | IP07 ready; score 100 trên 6 điểm CLI verify (IP02/08/09/10 unverified từ process) |
| `evidence/load-profile.json` | **live** | 200 req / 8 workers: P50 1414 ms, P95 5233 ms, P99 7598 ms, 200× HTTP 200 |
| `evidence/failure-recovery.json` | **live** | IT-J4 Feast/Qdrant/DLQ/replay |
| `evidence/rollback.json` | **live** | IT-J3 alias promotion + rollback |
| `evidence/gitops-validation.json` | **static pass / cluster UNVERIFIED** | `validate_manifests.py` |
| `evidence/pytest-gpu.txt` | **live** | 8 gpu tests passed; 7 chưa chạy vì thiếu Spark |

Sơ đồ kiến trúc: `docs/images/lab28-architecture-overview.png`.

## 5. Việc còn lại

1. Spark + vLLM cùng lúc để chứng minh **một** trace ID mang đủ 11 span
   (IT-J1/J5 gpu). Laptop 16 GB / WSL 10 GB chưa đủ an toàn.
2. Cluster Argo CD nếu lớp cấp → drift/self-heal live.
3. Commit/push wiring LangSmith + `evidence/ip10-langsmith.json` (không commit `.env`).

## 6. Contribution

Làm cá nhân: adapter, fast suite, Compose core + full profile, J1–J5 (phần
không gpu), 8 test gpu (Prometheus/J3/J4), `/ask` 200 trên vLLM 0.28.0,
evidence IP01–IP10 (IP10 = union hai trace), load profile, failure injection,
MLflow rollback, GitOps validate, LangSmith OTLP export (project `lab-28`).
Chưa: cùng-ID 11 span, GitOps sync live, demo trên lớp.
