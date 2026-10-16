"""Потребитель bronze-визитов — «другая команда» из сценария Readiness Marker (гл. 2, с. 54).

Физически изолированный пайплайн: напрямую мы его не запускаем, он сам ждёт маркер
_COMPLETED, который пишет visits_hourly_ingest последней задачей.
"""
from __future__ import annotations

import json
from datetime import datetime

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable

PART = "date={{ data_interval_start.strftime('%Y-%m-%d') }}/hour={{ data_interval_start.strftime('%H') }}"

# явный интервал: запуск в 11:00 обрабатывает [10:00, 11:00); строка "@hourly" в Airflow 3 интервала не даёт
HOURLY = CronDataIntervalTimetable("@hourly", timezone="UTC")


@dag(schedule=HOURLY, start_date=datetime(2026, 10, 14), catchup=False, max_active_runs=4,
     tags=["lab01", "readiness_marker"], doc_md=__doc__)
def bronze_visits_consumer():
    wait = S3KeySensor(task_id="wait_for_completed_marker", bucket_name="lake",
                       bucket_key=f"_markers/bronze_visits/{PART}/_COMPLETED", aws_conn_id="lake_s3",
                       mode="reschedule", poke_interval=15, timeout=300)

    @task
    def consume() -> dict:
        start = get_current_context()["data_interval_start"]
        key = f"_markers/bronze_visits/date={start:%Y-%m-%d}/hour={start:%H}/_COMPLETED"
        marker = json.loads(S3Hook(aws_conn_id="lake_s3").read_key(key, bucket_name="lake"))
        print(f"партиция {marker['partition']} готова: {marker['rows']} строк — можно строить silver")
        return marker

    wait >> consume()


bronze_visits_consumer()
