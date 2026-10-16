# Лаба 1 — Паттерны приёма данных (гл. 2, с. 29–62) — план

Ветка `lab-01-ingestion` → PR → merge → тег `lab-01`. Отчёт: `Отчет_Лаб1_ТКР_Семенов.docx`.

## 0. Что добавляется в стенд

| Компонент | Зачем | Профиль | Образ |
|---|---|---|---|
| Airflow (LocalExecutor, `standalone`) | DAG'и загрузки, сенсоры, REST API для триггера | `airflow` | см. вопрос 1 |
| Kafka Connect + Debezium | CDC из Postgres | `cdc` | `quay.io/debezium/connect:3.x` (на Docker Hub Debezium остановился на 3.0.0 в 2024) |
| Postgres JDBC jar | Spark читает Postgres | — | `postgresql-42.7.4.jar` в образ Spark (+ SHA-1 в `jars.sha1`) |
| БД `airflow` и `dwh` в том же Postgres | метаданные Airflow; витрина для варианта Full Loader «версия + view» | `core` | идемпотентный init |
| `trigger-listener` (Python) | обработчик уведомлений для External Trigger | `airflow` | на базе образа генератора |

Spark-джобы из Airflow запускаются **DockerOperator'ом**: на каждую задачу отдельный контейнер `blog-de/spark:local`.
Это локальный аналог `SparkKubernetesOperator` из примеров книги. Для этого в Airflow монтируется `docker.sock` (см. вопрос 2).

**Генератор:** новая команда `provider` — «внешний поставщик» публикует снимок справочника устройств
`providers/devices/<ts>.json` (без колонки «изменено», как в задаче книги для Full Loader) и событие в топик
`provider_events`. Снимки неидемпотентны: каждая публикация немного меняет справочник (как в задаче
Passthrough Replicator).

## 1. Паттерны: реализация и критерии проверки

### Full Loader ⭐ (с. 30)
- **A. Spark + Delta:** снимок `devices` → `lake/bronze/devices` с полной перезаписью (`overwrite`). Атомарность
  и откат дают транзакции и time travel Delta (пример 2.2 книги).
- **B. Airflow + Postgres:** `COPY` в версионную таблицу `dwh.devices_<run>` → `CREATE OR REPLACE VIEW dwh.devices`
  (примеры 2.3–2.4 книги). Потребитель всегда видит целую версию.
- **Проверка:** строк в Delta = строк в снимке; после 2-й загрузки `VERSION AS OF` предыдущей версии отдаёт
  прежний снимок; во время загрузки варианта B view отдаёт старую версию целиком, после — новую целиком.

### Incremental Loader ⭐ (с. 34)
- **По партициям:** часовые партиции `landing/visits/date=/hour=` → `lake/bronze/visits` (сырые строки + метаданные
  партиции). DAG раз в час грузит ровно партицию `{{ data_interval_start }}`, бэкфилл — по одной партиции.
- **По дельта-колонке:** `users` и `articles` из Postgres по `updated_at` в **ограниченном окне**
  `[data_interval_start, data_interval_end)` (совет книги про бэкфилл).
- **Проверка:** строк в bronze по каждой партиции = строк в файле генератора; повторный прогон партиции не плодит
  дубли. Демонстрация слабого места: после `blogsim oltp` с удалениями удалённые пользователи остаются в bronze —
  их ровно столько, сколько `delete_user` в `oltp_ops.jsonl`. Это мостик к CDC.

### Change Data Capture ⭐ (с. 39)
- Postgres (`wal_level=logical`, `pgoutput`, `REPLICA IDENTITY FULL` для `users`) → Debezium → топики
  `blog.public.users` / `articles` → Spark Structured Streaming → `lake/bronze/users_cdc` (append, с `op`, `ts_ms`, before/after).
- Дополнительно: Delta Change Data Feed на bronze-таблице (пример 2.11–2.12 книги).
- **Проверка:** число событий c/u/d в bronze = числу операций каждого типа в `oltp_ops.jsonl` прогона; удаления
  пойманы (в отличие от Incremental Loader); задержка «операция в Postgres → событие в топике» измерена (медиана, p95).

### Passthrough Replicator (с. 43)
- **Файлы:** `landing/visits/...` → бакет `staging` через `spark.read.text` → `write.text` (без разбора JSON, совет книги).
- **Kafka:** `visits` → `visits_staging` с сохранением ключей, заголовков и порядка внутри партиции (пример 2.15 книги:
  `sortWithinPartitions('offset')`).
- **Проверка:** множество строк каждой партиции побайтно совпадает (хеш отсортированных строк); в Kafka для каждой
  партиции последовательность (key, value, headers) совпадает 1:1.

### Transformation Replicator (с. 47)
- `users` → `staging`: удаляем `birth_date`, маскируем `email`, `first_name`, `last_name`; все поля читаются строками,
  чтобы не было скрытых преобразований типов (совет книги). Список PII-полей — в одном месте (мини-«контракт»).
- **Проверка:** в staging нет исходных значений PII (проверка по каждой строке); не-PII поля совпадают; строк столько же.

### Compactor ⭐ (с. 50)
- Стрим `visits` (Kafka) → `lake/bronze/visits_stream` с триггером раз в 10 с → много мелких файлов.
  Затем `OPTIMIZE` + `VACUUM` (с понятным порогом хранения), отдельная DAG.
- **Проверка:** число файлов и средний размер до и после; время `count()` и листинга до и после; данные те же
  (count + хеш); старые файлы удалены только после VACUUM.

### Readiness Marker ⭐ (с. 54)
- **Потребитель:** DAG ждёт `_SUCCESS` партиции через `S3KeySensor(mode="reschedule")`, пример 2.25 книги.
- **Производитель:** наш bronze-загрузчик последней задачей пишет маркер `_COMPLETED` (пример 2.26 книги), следующий DAG ждёт его.
- **Проверка:** партиции, которым генератор не поставил `_SUCCESS` (`missing_success` в ground truth), не загружены,
  а сенсор по ним упал по таймауту — список совпадает с журналом генератора 1:1.

### External Trigger (с. 57)
- `blogsim provider` → снимок devices + событие в `provider_events` → `trigger-listener` → Airflow REST API
  `dagRuns` с `conf` (событие, версия триггера, request id), `dag_run_id = external-<id>`; DAG с `schedule=None`
  грузит ровно указанный файл (Full Loader A). Ошибка вызова Airflow → событие в `provider_events_dlq`.
- Честная оговорка в отчёте: локально нет serverless-push, listener — долгоживущий consumer (то, что книга
  называет pull-реализацией). Обсуждаем компромисс.
- **Проверка:** N событий → N DAG run'ов с правильными файлами; при выключенном Airflow события в DLQ, после
  повторной обработки DLQ — все run'ы есть.

## 2. Отчёт

Титульник → цель → «Стенд» (кратко, лаба 0) → по разделу на паттерн (проблема, решение, реализация с листингом,
проверка с цифрами из `results.json`, компромиссы) → вывод. Скриншоты: Airflow (граф DAG'ов, сенсор в
reschedule), Redpanda Console (топики CDC), до/после Compactor.

## 3. Порядок работ

1. Стенд: Airflow, Kafka Connect, JDBC, init БД, генератор `provider`.
2. Full Loader → Incremental Loader → Readiness Marker (общий DAG загрузки visits).
3. CDC → Compactor.
4. Passthrough → Transformation Replicator.
5. External Trigger.
6. `demo.sh` (сценарий прогона всей лабы с нуля) → `results.json` → отчёт → PR.
