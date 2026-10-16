"""Incremental Loader + Readiness Marker (гл. 2): часовые партиции визитов → bronze.

Readiness Marker, сторона потребителя: не начинаем, пока поставщик не положил _SUCCESS
(S3KeySensor в режиме reschedule не держит слот воркера, пример 2.25 книги).
Readiness Marker, сторона производителя: последней задачей пишем свой маркер _COMPLETED,
на который ориентируются наши потребители (пример 2.26 книги).
Партиция определяется data interval'ом запуска — бэкфилл идёт ровно по одной партиции.
"""
from __future__ import annotations

import json
from datetime import datetime

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable

from lab_common.spark import SparkJob, parse_result

DATE = "{{ data_interval_start.strftime('%Y-%m-%d') }}"
HOUR = "{{ data_interval_start.strftime('%H') }}"
PART = f"date={DATE}/hour={HOUR}"

# явный интервал: запуск в 11:00 обрабатывает [10:00, 11:00); строка "@hourly" в Airflow 3 интервала не даёт
HOURLY = CronDataIntervalTimetable("@hourly", timezone="UTC")


# max_active_runs=1: Delta на S3 без внешней блокировки (DynamoDB LogStore) не гарантирует
# корректность при записи из нескольких драйверов, поэтому бэкфилл идёт по одной партиции
@dag(schedule=HOURLY, start_date=datetime(2026, 10, 14), catchup=False, max_active_runs=1,
     tags=["lab01", "incremental_loader", "readiness_marker"], doc_md=__doc__)
def visits_hourly_ingest():
    wait = S3KeySensor(
        task_id="wait_for_success_marker",
        bucket_name="landing",
        bucket_key=f"visits/{PART}/_SUCCESS",
        aws_conn_id="lake_s3",
        mode="reschedule",
        poke_interval=15,
        timeout=90,  # демо: в проде здесь был бы SLA поставщика
    )
    load = SparkJob(task_id="load_partition", script="ingestion/incremental_visits.py",
                    args=f"--date {DATE} --hour {HOUR}")

    @task
    def mark_completed(result_line: str) -> dict:
        ctx = get_current_context()
        result = parse_result(result_line)
        start = ctx["data_interval_start"]
        part = f"date={start:%Y-%m-%d}/hour={start:%H}"
        marker = {"partition": part, "rows": result["rows"], "run_id": ctx["run_id"],
                  "written_at": datetime.now().astimezone().isoformat(timespec="seconds")}
        S3Hook(aws_conn_id="lake_s3").load_string(json.dumps(marker), key=f"_markers/bronze_visits/{part}/_COMPLETED",
                                                  bucket_name="lake", replace=True)
        return marker

    wait >> load
    mark_completed(load.output)


visits_hourly_ingest()
