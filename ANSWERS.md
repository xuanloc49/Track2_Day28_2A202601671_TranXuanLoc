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

Qdrant / Kafka / MLflow champion là mandatory. Feast optional → stop Feast không
đưa pod ra `not_ready` (IT-J4). Stop Qdrant → `/ready` 503. Envoy health-check
**`/health` chứ không `/ready`**.

Gateway 10 token/s: burst cho **200** và **429** (`x-lab28-rate-limited`).

### Compose vs Kubernetes/GitOps

Compose chứng minh 10 boundary trên laptop. Manifest K8s: non-root, read-only
rootfs, drop ALL caps, NetworkPolicy, HPA/PDB, probes tách. Argo CD
`selfHeal: true` + `revisionHistoryLimit: 5` — rollback đổi `targetRevision`
(`refs/tags/v3.0.0`) trên Git, không `kubectl edit`. Validate tĩnh đã pass;
**sync live trên cluster UNVERIFIED** (không có Argo CD local).

### GPU/vLLM

Máy có RTX 2050 (4 GB) + Docker Desktop/WSL. Image `vllm/vllm-openai:v0.28.0`
(~20 GB) đã pull sau khi dọn ổ. Không giả OpenAI-compatible.

vLLM 0.28 mặc định GPUModelRunnerV2 đòi UVA; trên WSL `pin_memory` tắt nên
engine crash `UVA is not available`. Overlay `compose.gpu.yaml` đặt
`VLLM_USE_V2_MODEL_RUNNER=0` và `VLLM_WSL2_ENABLE_PIN_MEMORY=1`, cộng
`--dtype half --max-model-len 512 --gpu-memory-utilization 0.70 --enforce-eager
--max-num-seqs 1`.

Qwen3-1.7B fp16 không vừa VRAM còn ~3.2/4.0 GiB (Windows chiếm GPU). Serving
live dùng `Qwen/Qwen3-0.6B` (`ports.template`). Probe: `/version` = `0.28.0`,
111 metric `vllm:`, `is_real_vllm: true` → `evidence/ip07-vllm-identity.json`.
`lab28 evidence` ghi IP07 **ready**.

Sau đó full profile + vLLM + pytest `-m gpu` làm Docker Desktop 500/OOM;
cổng 8001 mất, test gpu dừng ở `EEE` (fixture), **không** có `/ask` 200 hay
span serving. GitOps live cluster vẫn UNVERIFIED.

## 3. Production gaps (cần nói khi demo)

1. **Kafka lab:** 1 broker, RF=1, plaintext.
2. **Gateway:** rate limit 10 token/s, không JWT.
3. **Secret:** Grafana `admin/admin` là lab default; không commit `.env`.
4. **LangSmith UNVERIFIED** (không `LANGSMITH_API_KEY`).
5. **Load probe** bắn `/ready` (stdlib script), không phải `/ask`. P95/P99 bị
   token bucket 10 rps làm trễ — đó là bottleneck cạnh tranh cổng, không phải
   latency vLLM.
6. **RAM:** Docker VM 8 GB; Spark + Airflow + core stack dễ OOM (container 137).
7. **IP07 identity đã live**, nhưng `/ask` + test gpu-marked chưa pass vì
   full stack + vLLM vượt RAM Docker VM 8 GB (engine 500, cổng 8001 mất).
8. **GitOps self-heal live:** chưa có cluster; chỉ validate manifest.

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
| `evidence/ip08-gateway.json` | **live** | 200 + 429 + `x-request-id` |
| `evidence/ip09-prometheus-targets.json` | **live** | targets + alert rules (vLLM optional down) |
| `evidence/ip09-grafana-dashboards.json` | **live** | dashboard `lab28-platform` |
| `evidence/ip10-trace.json` | **live ingest+pipeline** | trace `45c8b110fbb94076841a0ef98e56bee4`; thiếu ask/vLLM/Feast/Qdrant query |
| `evidence/integration-report.json` | **live** | IP07 ready; score 100 trên 6 điểm CLI verify (IP02/08/09/10 unverified từ process) |
| `evidence/load-profile.json` | **live** | 200 req / 8 workers: P50 1414 ms, P95 5233 ms, P99 7598 ms, 200× HTTP 200 |
| `evidence/failure-recovery.json` | **live** | IT-J4 Feast/Qdrant/DLQ/replay |
| `evidence/rollback.json` | **live** | IT-J3 alias promotion + rollback |
| `evidence/gitops-validation.json` | **static pass / cluster UNVERIFIED** | `validate_manifests.py` |

Sơ đồ kiến trúc: `docs/images/lab28-architecture-overview.png`.

## 5. Việc còn lại

1. Restart Docker Desktop, lên **core + gpu** (không Spark/Airflow) rồi
   gọi `/ask` + pytest `-m gpu` khi RAM đủ; sau đó thu span serving IP10.
2. Cluster Argo CD nếu lớp cấp → drift/self-heal live.
3. Commit/push `compose.gpu.yaml`, `ports.template`, evidence IP07, `ANSWERS.md`
   khi bạn yêu cầu.

## 6. Contribution

Làm cá nhân: adapter, fast suite, Compose core + full profile, J1–J5 (trừ gpu),
evidence IP01–IP06/IP08–IP10 (ingest+pipeline), IP07 identity vLLM 0.28.0,
load profile, failure injection, MLflow rollback, GitOps validate. Còn `/ask`
end-to-end trên GPU (OOM) và GitOps sync live.
