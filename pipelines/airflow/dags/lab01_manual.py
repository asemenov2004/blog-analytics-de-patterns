"""Ручные DAG'и лабы 1: CDC → bronze, Compactor, репликация в staging.

cdc_to_bronze      — вычитать накопленные события Debezium в bronze (в проде это постоянный стрим)
visits_compaction  — стрим visits мелкими микробатчами → OPTIMIZE + VACUUM
replicate_to_staging — Passthrough (файлы и Kafka) и Transformation (users без PII)
"""
from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG, Param

from lab_common.spark import SparkJob

COMMON = {"schedule": None, "start_date": datetime(2026, 10, 14), "catchup": False, "max_active_runs": 1}

with DAG("cdc_to_bronze", tags=["lab01", "cdc"], doc_md="Change Data Capture (гл. 2, с. 39)", **COMMON):
    SparkJob(task_id="cdc_to_bronze", script="ingestion/cdc_to_bronze.py")

with DAG("visits_compaction", tags=["lab01", "compactor"], doc_md="Compactor (гл. 2, с. 50)",
         params={"max_offsets": Param(100, type="integer"), "table": Param("visits_stream", type="string")},
         **COMMON):
    table = "s3a://lake/bronze/{{ params.table }}"
    stream = SparkJob(task_id="stream_small_files", script="ingestion/visits_stream.py",
                      args=f"--max-offsets {{{{ params.max_offsets }}}} --output {table}")
    compact = SparkJob(task_id="optimize_and_vacuum", script="ingestion/compact.py", args=f"--table {table}")
    stream >> compact

with DAG("replicate_to_staging", tags=["lab01", "replication"],
         doc_md="Passthrough Replicator (с. 43) и Transformation Replicator (с. 47)",
         params={"date": Param("2026-10-14", type="string", description="дата партиции landing/visits")}, **COMMON):
    SparkJob(task_id="passthrough_files", script="ingestion/replicate_files.py", args="--date {{ params.date }}")
    SparkJob(task_id="passthrough_kafka", script="ingestion/replicate_kafka.py")
    SparkJob(task_id="transformation_users", script="ingestion/replicate_users_pii.py")
