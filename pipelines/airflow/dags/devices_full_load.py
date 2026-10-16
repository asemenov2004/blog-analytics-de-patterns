"""Full Loader (гл. 2, с. 30), запускается External Trigger'ом (с. 57).

schedule=None: планировщик DAG не трогает, запускает только внешний триггер с conf:
{"event": {...bucket, key...}, "trigger": {...}}. Две реализации Full Loader:
  A. Spark → Delta: перезапись одной транзакцией, откат через time travel;
  B. Postgres: COPY в версионную таблицу + переключение view (примеры 2.3–2.4 книги).
"""
from __future__ import annotations

import csv
import io
import json
from datetime import datetime

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, get_current_context, task

from lab_common.spark import SparkJob

KEEP_VERSIONS = 3


@dag(schedule=None, start_date=datetime(2026, 10, 14), catchup=False, max_active_runs=1,
     tags=["lab01", "full_loader", "external_trigger"], doc_md=__doc__)
def devices_full_load():
    @task
    def resolve_input() -> dict:
        ctx = get_current_context()
        conf = ctx["dag_run"].conf or {}
        event = conf.get("event")
        if not event or "key" not in event:
            raise ValueError("DAG запускается внешним триггером: в conf нужен event с bucket/key")
        print("триггер:", json.dumps(conf.get("trigger"), ensure_ascii=False))
        return {"bucket": event["bucket"], "key": event["key"], "s3a": f"s3a://{event['bucket']}/{event['key']}",
                "event_id": event.get("event_id"), "rows_announced": event.get("rows")}

    src = resolve_input()

    delta = SparkJob(task_id="full_load_delta", script="ingestion/full_load_devices.py",
                     args="--input {{ ti.xcom_pull(task_ids='resolve_input')['s3a'] }}")

    @task
    def full_load_postgres(source: dict) -> dict:
        raw = S3Hook(aws_conn_id="lake_s3").read_key(source["key"], bucket_name=source["bucket"])
        rows = [json.loads(line) for line in raw.splitlines() if line.strip()]
        buf = io.StringIO()
        w = csv.writer(buf)
        for r in rows:
            w.writerow([r["device_id"], r["type"], r["full_name"], r["version"]])
        buf.seek(0)
        version = f"devices_v{datetime.now():%Y%m%d%H%M%S}"
        hook = PostgresHook(postgres_conn_id="dwh")
        conn = hook.get_conn()
        with conn, conn.cursor() as cur:  # одна транзакция: таблица + данные + переключение view
            cur.execute(f"CREATE TABLE {version} (device_id bigint, type text, full_name text, version text)")
            with cur.copy(f"COPY {version} FROM STDIN WITH (FORMAT csv)") as cp:  # psycopg 3
                cp.write(buf.getvalue())
            cur.execute(f"CREATE OR REPLACE VIEW devices AS SELECT * FROM {version}")
            cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'devices_v%' "
                        "ORDER BY tablename DESC")
            old = [t for (t,) in cur.fetchall()][KEEP_VERSIONS:]
            for t in old:
                cur.execute(f"DROP TABLE {t}")
        return {"table": version, "rows": len(rows), "dropped_versions": old}

    src >> delta
    full_load_postgres(src)


devices_full_load()
