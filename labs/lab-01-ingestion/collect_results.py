"""Сбор результатов лабы 1 в results.json: из него строится отчёт.

Источники: проверочная Spark-джоба (сверка с ground truth), XCom задач Airflow,
журнал генератора (бакет truth), Postgres dwh (версии Full Loader), топики Kafka.
Запуск с хоста при поднятом стенде: python labs/lab-01-ingestion/collect_results.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE))
import af  # noqa: E402

DATE = "2026-10-14"
VISITS_RUN, OLTP_RUNS = "lab01-visits", ["lab01-oltp", "lab01-oltp2"]
os.environ.setdefault("S3_ENDPOINT", "http://localhost:18333")
os.environ.setdefault("S3_ACCESS_KEY", "blogdev")
os.environ.setdefault("S3_SECRET_KEY", "blogdev-secret")
os.environ.setdefault("KAFKA_BOOTSTRAP", "localhost:19092")


def compose(*args: str) -> str:
    env = {**os.environ, "MSYS_NO_PATHCONV": "1"}
    return subprocess.run(["docker", "compose", "-f", str(ROOT / "docker/compose.yml"), *args],
                          capture_output=True, text=True, encoding="utf-8", env=env, check=True).stdout


def spark_result(script: str, *args: str) -> dict:
    out = compose("run", "--rm", "-q", "spark", "spark-submit", f"/opt/pipelines/spark/{script}", *args)
    line = [x for x in out.splitlines() if x.startswith("RESULT ")][-1]
    return json.loads(line[len("RESULT "):])


def result_from_log(dag_id: str, run_id: str, task_id: str, try_number: int):
    """Строка RESULT из лога задачи — для запусков, где в XCom попала другая последняя строка."""
    from urllib.parse import quote

    log = af.api("GET", f"/api/v2/dags/{dag_id}/dagRuns/{quote(run_id, safe='')}/taskInstances/{task_id}"
                        f"/logs/{try_number}?full_content=true")
    events = [e.get("event", "") if isinstance(e, dict) else str(e) for e in log.get("content", [])]
    line = next((e for e in reversed(events) if e.startswith("RESULT ")), None)
    return json.loads(line[len("RESULT "):]) if line else None


def last_xcom(dag_id: str, task_id: str, state: str = "success"):
    for run in reversed(af.runs(dag_id)):
        ti = {t["task_id"]: t for t in af.tasks(dag_id, run["dag_run_id"])}
        if ti.get(task_id, {}).get("state") == state:
            val = af.xcom(dag_id, run["dag_run_id"], task_id)
            if isinstance(val, str) and "RESULT " in val:
                val = json.loads(val.split("RESULT ", 1)[1])
            elif isinstance(val, str):
                val = result_from_log(dag_id, run["dag_run_id"], task_id, ti[task_id]["try_number"])
            return {"run_id": run["dag_run_id"], "value": val}
    return None


def s3_json(bucket: str, key: str):
    from blogsim.cli import s3_client
    from blogsim.config import Connections

    return json.loads(s3_client(Connections()).get_object(Bucket=bucket, Key=key)["Body"].read())


def topic_count(topic: str) -> int:
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"], "group.id": "lab01-collector"})
    md = c.list_topics(topic, timeout=10).topics[topic]
    total = 0
    for p in md.partitions:
        lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        total += hi - lo
    c.close()
    return total


def ingest_runs() -> dict:
    """По каждой партиции: чем закончился последний запуск часовой загрузки."""
    out = {}
    for run in af.runs("visits_hourly_ingest"):
        if not run["dag_run_id"].startswith("backfill__"):
            continue
        hour = run["logical_date"][11:13]
        tis = {t["task_id"]: t["state"] for t in af.tasks("visits_hourly_ingest", run["dag_run_id"])}
        out[hour] = {"run_state": run["state"], **tis}
    return dict(sorted(out.items()))


def dwh_versions() -> dict:
    import psycopg

    with psycopg.connect("postgresql://blog:blog@localhost:15432/dwh") as c, c.cursor() as cur:
        cur.execute("SELECT tablename FROM pg_tables WHERE tablename LIKE 'devices_v%' ORDER BY tablename")
        tables = [t for (t,) in cur.fetchall()]
        counts = {}
        for t in tables:
            cur.execute(f"SELECT count(*) FROM {t}")
            counts[t] = cur.fetchone()[0]
        cur.execute("SELECT definition FROM pg_views WHERE viewname = 'devices'")
        view = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM devices")
        view_rows = cur.fetchone()[0]
    return {"versions": counts, "view_points_to": next((t for t in tables if t in view), None), "view_rows": view_rows}


def main() -> None:
    res: dict = {"date": DATE}
    res["generator"] = {"visits": s3_json("truth", f"runs/{VISITS_RUN}/summary.json"),
                        "oltp": [s3_json("truth", f"runs/{r}/summary.json") for r in OLTP_RUNS],
                        "stream": s3_json("truth", "runs/lab01-stream/summary.json")}
    res["checks"] = spark_result("checks/lab01_checks.py", "--visits-run", VISITS_RUN,
                                 "--oltp-runs", ",".join(OLTP_RUNS), "--date", DATE)
    res["ingest_runs"] = ingest_runs()
    res["compactor"] = last_xcom("visits_compaction", "optimize_and_vacuum")
    res["compactor_source"] = last_xcom("visits_compaction", "stream_small_files")
    res["replication"] = {t: last_xcom("replicate_to_staging", t)
                          for t in ("passthrough_files", "passthrough_kafka", "transformation_users")}
    res["full_loader"] = {"delta_runs": [], "postgres": dwh_versions()}
    for run in af.runs("devices_full_load"):
        tis = {t["task_id"]: t["state"] for t in af.tasks("devices_full_load", run["dag_run_id"])}
        entry = {"run_id": run["dag_run_id"], "state": run["state"], "tasks": tis}
        if tis.get("full_load_delta") == "success":
            entry["delta"] = json.loads(af.xcom("devices_full_load", run["dag_run_id"], "full_load_delta")
                                        .split("RESULT ", 1)[1])
        if tis.get("full_load_postgres") == "success":
            entry["postgres"] = af.xcom("devices_full_load", run["dag_run_id"], "full_load_postgres")
        res["full_loader"]["delta_runs"].append(entry)
    res["external_trigger"] = {
        "events": topic_count("provider_events"),
        "dlq": topic_count("provider_events_dlq"),
        "external_runs": sum(r["dag_run_id"].startswith("external-") for r in af.runs("devices_full_load")),
    }
    (HERE / "results.json").write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: (v if k in ("external_trigger",) else "…") for k, v in res.items()}, ensure_ascii=False))


if __name__ == "__main__":
    main()
