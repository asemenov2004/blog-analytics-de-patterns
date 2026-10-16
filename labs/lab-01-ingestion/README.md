# Лаба 1 — Паттерны приёма данных (гл. 2)

План и критерии проверки — [PLAN.md](PLAN.md). Полный прогон — [demo.sh](demo.sh), цифры — `results.json`,
отчёт — `python reports/lab-01/make_report.py`.

| Паттерн | Код | DAG |
|---|---|---|
| Full Loader | [full_load_devices.py](../../pipelines/spark/ingestion/full_load_devices.py) (Delta), задача `full_load_postgres` (версионная таблица + view) | `devices_full_load` |
| Incremental Loader (партиции) | [incremental_visits.py](../../pipelines/spark/ingestion/incremental_visits.py) | `visits_hourly_ingest` |
| Incremental Loader (дельта-колонка) | [incremental_table.py](../../pipelines/spark/ingestion/incremental_table.py) | `users_incremental` |
| Change Data Capture | Debezium [postgres-cdc.json](../../docker/connect/postgres-cdc.json), [cdc_to_bronze.py](../../pipelines/spark/ingestion/cdc_to_bronze.py) | `cdc_to_bronze` |
| Passthrough Replicator | [replicate_files.py](../../pipelines/spark/ingestion/replicate_files.py), [replicate_kafka.py](../../pipelines/spark/ingestion/replicate_kafka.py) | `replicate_to_staging` |
| Transformation Replicator | [replicate_users_pii.py](../../pipelines/spark/ingestion/replicate_users_pii.py) | `replicate_to_staging` |
| Compactor | [visits_stream.py](../../pipelines/spark/ingestion/visits_stream.py) → [compact.py](../../pipelines/spark/ingestion/compact.py) | `visits_compaction` |
| Readiness Marker | `S3KeySensor` на `_SUCCESS` + маркер `_COMPLETED` | `visits_hourly_ingest` → `bronze_visits_consumer` |
| External Trigger | [listener.py](../../pipelines/trigger_listener/listener.py) → Airflow REST API | `devices_full_load` |

Сверка с ground truth генератора — [lab01_checks.py](../../pipelines/spark/checks/lab01_checks.py).

## Стенд для лабы

```bash
docker compose -f docker/compose.yml --profile core --profile cdc --profile airflow up -d --wait
```

- Airflow: http://localhost:18088 (admin / admin)
- Kafka Connect REST: http://localhost:18083
- Redpanda Console: http://localhost:18080

## Что пошло не так и чему научило

Параллельный бэкфилл в одну Delta-таблицу на S3 сломал её журнал — см. [ADR-0005](../../docs/adr/0005-delta-single-writer-on-s3.md).
Пишущие DAG'и теперь работают по одному запуску.
