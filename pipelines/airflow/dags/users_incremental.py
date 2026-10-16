"""Incremental Loader на дельта-колонке (гл. 2, с. 34): users из Postgres по updated_at.

Окно запуска = [data_interval_start, data_interval_end) — ограниченное окно из книги,
чтобы бэкфилл не превращался в полную загрузку.
"""
from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag
from airflow.timetables.interval import CronDataIntervalTimetable

from lab_common.spark import SparkJob

# явный интервал: запуск в 11:00 обрабатывает [10:00, 11:00); строка "@hourly" в Airflow 3 интервала не даёт
HOURLY = CronDataIntervalTimetable("@hourly", timezone="UTC")


@dag(schedule=HOURLY, start_date=datetime(2026, 10, 14), catchup=False, max_active_runs=1,
     tags=["lab01", "incremental_loader"], doc_md=__doc__)
def users_incremental():
    SparkJob(task_id="load_window", script="ingestion/incremental_table.py",
             args="--table users --from {{ data_interval_start.isoformat() }} --to {{ data_interval_end.isoformat() }}")


users_incremental()
