#!/usr/bin/env bash
# Лаба 1 — сценарий демонстрации паттернов приёма данных. Запуск из корня репозитория (Git Bash / Linux).
# Повторяет шаги, которыми получены цифры в results.json. Время прогона — около часа (бэкфилл идёт
# по одной партиции, сенсоры пустых партиций ждут таймаут).
set -euo pipefail
export MSYS_NO_PATHCONV=1
DC="docker compose -f docker/compose.yml"
GEN="$DC run --rm -q generator"
AF="python labs/lab-01-ingestion/af.py"

step() { printf '\n=== %s\n' "$*"; }

step "Стенд: core + cdc + airflow"
$DC --profile core --profile spark --profile tools --profile airflow build
$DC --profile core --profile cdc --profile airflow up -d --wait
$GEN bootstrap

step "CDC: регистрация коннектора Debezium (snapshot.mode=no_data — только изменения после старта)"
curl -fsS -X PUT -H 'Content-Type: application/json' --data @docker/connect/postgres-cdc.json \
  localhost:18083/connectors/blog-postgres-cdc/config >/dev/null
sleep 10

SPARK="$DC run --rm -q spark spark-submit /opt/pipelines/spark"
now() { date -u +%Y-%m-%dT%H:%M:%S+00:00; }

step "Данные: визиты за 6 часов (15% партиций без _SUCCESS)"
$GEN batch --start 2026-10-14T00:00:00Z --hours 6 --run-id lab01-visits --set defects.missing_success_pct=15

step "Incremental Loader (дельта-колонка): первичная загрузка users ДО изменений в источнике"
T0=$(now)
$SPARK/ingestion/incremental_table.py --table users --from 2020-01-01T00:00:00+00:00 --to "$T0"

step "Два прогона изменений OLTP, после каждого — загрузка своего окна по updated_at"
$GEN oltp --ops 300 --per-second 20 --seed 7 --run-id lab01-oltp
T1=$(now); $SPARK/ingestion/incremental_table.py --table users --from "$T0" --to "$T1"
$GEN oltp --ops 120 --per-second 20 --seed 8 --run-id lab01-oltp2
T2=$(now); $SPARK/ingestion/incremental_table.py --table users --from "$T1" --to "$T2"

step "Incremental Loader + Readiness Marker: бэкфилл 12 часовых партиций и потребитель"
for d in visits_hourly_ingest bronze_visits_consumer; do
  $DC exec -T airflow airflow backfill create --dag-id $d \
    --from-date 2026-10-14T00:00:00+00:00 --to-date 2026-10-14T11:00:00+00:00 --max-active-runs 1
done

step "External Trigger + Full Loader: две публикации справочника устройств"
$GEN provider --seed 101
sleep 60
$GEN provider --seed 202

step "CDC → bronze"
$AF trigger cdc_to_bronze '{}'

step "Compactor: поток visits мелкими микробатчами → OPTIMIZE + VACUUM"
$GEN stream --minutes 3 --speed 20 --run-id lab01-stream --config config/stream-demo.yaml
$AF trigger visits_compaction '{"max_offsets": 200, "table": "visits_stream"}'

step "Ждём DAG'и"
$AF wait visits_hourly_ingest bronze_visits_consumer devices_full_load cdc_to_bronze visits_compaction

step "Passthrough + Transformation Replicator"
$AF trigger replicate_to_staging '{"date": "2026-10-14"}'
$AF wait replicate_to_staging

step "External Trigger: отказ Airflow → DLQ → повторная обработка"
$DC --profile core --profile cdc --profile airflow stop airflow
$GEN provider --seed 303
sleep 15
$DC --profile core --profile cdc --profile airflow up -d --wait airflow
$DC --profile core --profile cdc --profile airflow run --rm -q trigger-listener --replay
$AF wait devices_full_load

step "Сбор результатов → results.json"
python labs/lab-01-ingestion/collect_results.py
