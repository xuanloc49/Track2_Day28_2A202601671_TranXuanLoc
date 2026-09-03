"""Collect live evidence the CLI cannot write. Run against a running core stack."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "integration-tests"))
sys.path.insert(0, str(ROOT / "src"))

import stack  # noqa: E402
from lab28_platform.settings import Settings  # noqa: E402

OUT = ROOT / "evidence"
OUT.mkdir(exist_ok=True)


def dump(name: str, payload: object) -> None:
    path = OUT / name
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(f"wrote {path}")


def collect_ip01(settings: Settings) -> None:
    records = stack.read_topic(settings.kafka.bootstrap_servers, settings.kafka.topic_raw)
    with_trace = [r for r in records if r.traceparent and r.value]
    record = with_trace[0] if with_trace else (records[0] if records else None)
    if record is None:
        dump("ip01-kafka-consume.json", {"error": "no messages on data.raw"})
        return
    dump(
        "ip01-kafka-consume.json",
        {
            "topic": settings.kafka.topic_raw,
            "key": record.key,
            "partition": record.partition,
            "offset": record.offset,
            "headers": record.headers,
            "trace_id": record.trace_id,
            "value": record.value,
            "messages_seen": len(records),
            "messages_with_traceparent": len(with_trace),
        },
    )


def collect_ip08(settings: Settings) -> None:
    accepted = None
    rejected = None
    statuses: list[int] = []
    with httpx.Client(base_url=settings.gateway_url, timeout=10.0) as client:
        for _ in range(30):
            response = client.get("/health")
            statuses.append(response.status_code)
            if response.status_code == 200 and accepted is None:
                accepted = response
            if response.status_code == 429 and rejected is None:
                rejected = response
            if accepted is not None and rejected is not None:
                break
    dump(
        "ip08-gateway.json",
        {
            "gateway_url": settings.gateway_url,
            "route": "/health",
            "configured_rps": 10,
            "requests_sent": len(statuses),
            "accepted": sum(1 for status in statuses if status == 200),
            "rejected": sum(1 for status in statuses if status == 429),
            "sample_200": {
                "status": accepted.status_code if accepted else None,
                "x-request-id": accepted.headers.get("x-request-id") if accepted else None,
            },
            "sample_429": {
                "status": rejected.status_code if rejected else None,
                "x-request-id": rejected.headers.get("x-request-id") if rejected else None,
                "x-lab28-rate-limited": (
                    rejected.headers.get("x-lab28-rate-limited") if rejected else None
                ),
            },
        },
    )


def collect_ip09() -> None:
    prometheus = stack.Prometheus("http://localhost:9090")
    targets = prometheus.targets()
    dump(
        "ip09-prometheus-targets.json",
        {
            "targets": [
                {
                    "job": (target.get("labels") or {}).get("job"),
                    "url": target.get("scrapeUrl"),
                    "health": target.get("health"),
                    "last_scrape": target.get("lastScrape"),
                    "last_error": target.get("lastError"),
                }
                for target in targets
            ],
            "rules": [
                {
                    "name": rule.get("name"),
                    "type": rule.get("type"),
                    "health": rule.get("health"),
                    "duration": rule.get("duration"),
                    "labels": rule.get("labels"),
                    "annotations": rule.get("annotations"),
                }
                for rule in prometheus.rules()
            ],
        },
    )
    grafana_url = "http://localhost:3000"
    auth = ("admin", "admin")
    dashboards = httpx.get(f"{grafana_url}/api/search", params={"type": "dash-db"}, auth=auth, timeout=10)
    datasources = httpx.get(f"{grafana_url}/api/datasources", auth=auth, timeout=10)
    dump(
        "ip09-grafana-dashboards.json",
        {
            "grafana_url": grafana_url,
            "dashboards": [
                {"title": entry.get("title"), "uid": entry.get("uid"), "url": entry.get("url")}
                for entry in dashboards.json()
            ],
            "datasources": [
                {"name": entry.get("name"), "type": entry.get("type")} for entry in datasources.json()
            ],
        },
    )


def collect_ip04(settings: Settings) -> None:
    from lab28_platform.integration_tasks import feast_online_request

    request = feast_online_request("asker-001")
    response = httpx.post(
        f"{settings.feast.server_url.rstrip('/')}/get-online-features",
        json=request,
        timeout=10.0,
    )
    dump(
        "ip04-feast-online.json",
        {
            "request": request,
            "status_code": response.status_code,
            "body": response.json() if response.headers.get("content-type", "").startswith("application/json") else response.text[:2000],
            "note": "core stack: Feast is serving; rows stay empty until the full-profile Spark export/materialize runs",
        },
    )


def collect_ip10() -> None:
    required = [
        "lab28.gateway.request",
        "lab28.api.ingest",
        "lab28.kafka.produce",
        "lab28.kafka.consume",
        "lab28.airflow.dag",
        "lab28.spark.delta_merge",
        "lab28.api.ask",
        "lab28.feast.get_online_features",
        "lab28.qdrant.query",
        "lab28.mlflow.resolve_release",
        "lab28.vllm.chat_completion",
    ]
    traces = stack.TraceBackend("http://localhost:16686")
    payload: dict[str, object] = {"required_spans": required, "services": {}}
    for service in ("lab28-gateway", "lab28-api", "lab28-airflow"):
        try:
            response = httpx.get(
                "http://localhost:16686/api/traces",
                params={"service": service, "limit": 5},
                timeout=10.0,
            )
            found = list((response.json().get("data") or [])) if response.status_code == 200 else []
        except Exception as error:
            payload["services"][service] = {"error": f"{type(error).__name__}: {error}"}
            continue
        sample = []
        for trace in found[:3]:
            names = {span.get("operationName") for span in trace.get("spans") or []}
            sample.append(
                {
                    "trace_id": trace.get("traceID"),
                    "span_names": sorted(name for name in names if name),
                }
            )
        payload["services"][service] = {"count": len(found), "sample": sample}

    try:
        ask = httpx.post(
            "http://localhost:8080/api/v1/ask",
            json={
                "asker_id": "asker-001",
                "question": "Nền tảng dữ liệu của lab này gồm những thành phần nào?",
                "top_k": 3,
            },
            timeout=15.0,
        )
        ask_body = (
            ask.json()
            if ask.headers.get("content-type", "").startswith("application/json")
            else {"text": ask.text[:500]}
        )
        payload["ask"] = {"status_code": ask.status_code, "body": ask_body}
    except Exception as error:
        payload["ask"] = {
            "error": f"{type(error).__name__}: {error}",
            "note": "UNVERIFIED on core stack without a real vLLM endpoint",
        }
        ask_body = {}
    trace_id = (ask_body.get("evidence") or {}).get("trace_id") if isinstance(ask_body, dict) else None
    if not trace_id:
        gateway_sample = (payload.get("services") or {}).get("lab28-gateway") or {}
        samples = gateway_sample.get("sample") or []
        if samples:
            trace_id = samples[0].get("trace_id")
    if trace_id:
        seen = sorted(traces.span_names(str(trace_id)))
        payload["selected_trace"] = {
            "trace_id": trace_id,
            "span_names": seen,
            "missing_required": [name for name in required if name not in seen],
            "note": "core stack cannot produce Airflow/Spark/vLLM spans until the full profile and GPU path run",
        }
    dump("ip10-trace.json", payload)


def main() -> None:
    settings = Settings.from_env()
    collect_ip01(settings)
    collect_ip04(settings)
    collect_ip08(settings)
    collect_ip09()
    collect_ip10()


if __name__ == "__main__":
    main()
