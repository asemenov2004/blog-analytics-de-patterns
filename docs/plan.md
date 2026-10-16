# План: лабораторные по «Инженерия данных. Паттерны проектирования» (Конечны, 2026)

**Идея:** один проект — платформа аналитики блогов (домен из гл. 1 книги).
Каждая лаба = одна глава книги = отдельная ветка + PR + отчёт (.docx). Проект растёт инкрементально.
Работаю один, репозиторий на GitHub.

**Порядок:** лабы идут от самых важных и интересных к менее приоритетным. Делаем, сколько успеем.
Внутри лабы ⭐ = ключевой паттерн, делается обязательно; остальные — если хватает времени.

Легенда покрытия: ✅ реализуем полноценно · 🟡 частично / эмуляция · 📝 только разбор в отчёте

---

## 1. Архитектура

```
                  ┌──────────────── Airflow (оркестрация, бэкфиллы) ────────────────┐
                  │                                                                 │
 Генератор ──┬─► Redpanda (Kafka API + Schema Registry) ──► Spark Structured Streaming ──┐
             │        ▲                                                                  │
             │        └── Debezium (CDC) ◄── Postgres OLTP (users, blogs, articles)      ▼
             │                                                          SeaweedFS (S3) + Delta Lake
             └─► файлы JSONL + _SUCCESS в S3 ────► Spark batch ──────────►  bronze → silver → gold
                                                                                         │
                                                       dbt ──► ClickHouse (gold-витрины, serving)
                                                                                         │
                                         Prometheus + Grafana, OpenLineage/Marquez (наблюдаемость)
```

### Стек

| Компонент | Зачем | Профиль compose | RAM (грубо, уточним в лабе 0) |
|---|---|---|---|
| Postgres 16 | OLTP-источник + метабаза Airflow (одна инстанция, разные БД) | `core` | ~0.3 ГБ |
| Redpanda + Console | Kafka-совместимый брокер, Schema Registry встроен, легче Kafka | `core` | ~1 ГБ |
| SeaweedFS | S3-хранилище для lakehouse (MinIO убран с Docker Hub, см. ADR-0003) | `core` | ~0.5 ГБ |
| Spark 3.5.8 + Delta Lake 3.3.3 (PySpark) | batch + streaming, как в книге | `spark` | ~2–3 ГБ |
| Airflow (LocalExecutor) | оркестрация, как в книге | `airflow` | ~1.5–2 ГБ |
| ClickHouse | gold-витрины, сравнение подходов к хранению | `ch` | ~1–1.5 ГБ |
| dbt (dbt-clickhouse) | gold-модели, тесты качества, контракты | `ch` | ~0.2 ГБ |
| Debezium (Kafka Connect) | CDC | `cdc` | ~1 ГБ |
| Prometheus + Grafana | метрики, лаг, алерты | `obs` | ~0.5 ГБ |
| Marquez (OpenLineage) | lineage | `lineage` | ~1 ГБ |
| Vault (dev mode) | секреты | `security` | ~0.2 ГБ |
| Flink | **не берём по умолчанию**: PyFlink неудобен, Spark закрывает те же паттерны | — | — |

**Ресурсы:** WSL2 уже ограничен 12 ГБ / 4 CPU (`.wslconfig`). Всё сразу не поднимаем:
каждая лаба = `core` + 1–3 нужных профиля. Версии образов фиксируем точно (без `latest`).

### Слои данных

- **bronze** — сырые события/выгрузки как есть (Delta в S3)
- **silver** — очищенное, дедуплицированное, обогащённое (Delta)
- **gold** — витрины для потребителей (Delta + ClickHouse через dbt)

---

## 2. Лаба 0 — стенд (в книге её нет, отчёт не пишется)

Подготовка к остальным лабам: репозиторий, compose, генератор, CI, генератор отчётов.
Результат фиксируется в README репозитория. В отчёте лабы 1 будет короткий раздел «Стенд».

### Генератор данных

Python + Faker, CLI, конфиг в YAML, фиксированный `seed` → воспроизводимые прогоны.

**Сущности:**
- **OLTP (Postgres):** `users` (с PII: email, имя, дата рождения, страна), `blogs`, `articles`
- **События визитов:** `event_id, user_id, article_id, event_time, ip, user_agent, referrer, schema_version, ...`

**Режимы:**
1. `stream` → события в топик Redpanda
2. `batch` → JSONL-файлы в S3 по партициям `date=/hour=` + маркер `_SUCCESS`
3. `oltp` → вставки/апдейты/удаления в Postgres (для CDC и инкрементальной загрузки)
4. большой объём для лабы по хранению — тот же `batch` с повышенной интенсивностью (~30 тыс. записей/с). Этим заменяем TPC-H.

**Управляемые дефекты (главное):**

| Параметр | Что даёт | Для каких паттернов |
|---|---|---|
| `duplicates_pct` | повторная отправка того же события | Windowed Deduplicator, Keyed Idempotency |
| `late_pct`, `max_delay` | события с event_time в прошлом | Late Data Detector / Integrators |
| `out_of_order_pct` | перемешивание порядка | Sequencers, Orderers |
| `malformed_pct` | битый JSON, нет обязательных полей, не тот тип | Dead-Letter, Constraints Enforcer |
| `hot_keys` | перекос по пользователю/статье | Skew Detector, Local Aggregator |
| `schema_version` | v1 → v2 (совместимо) → v3 (несовместимо) | Schema Compatibility Enforcer, Schema Migrator |
| `pauses` | обрывы потока на N минут | Flow Interruption Detector, Lag Detector |
| `late_batch` | файл приходит после дедлайна / без `_SUCCESS` | Readiness Marker, SLA Misses Detector |

**Ground truth:** генератор пишет журнал внесённых дефектов (какие `event_id` дублированы, опоздали, битые).
В отчёте это даёт измеримую проверку: «внесли 1000 дублей, дедупликатор убрал 1000».

### Генератор отчётов (.docx)

Та же схема, что в отчётах по VBA (`VBA/Лаб8/automation`): python-docx + `template_title.docx`
(титульник ЮФУ, поля 3 / 1,5 / 2 / 2 см, Times New Roman 14, подписи «Рисунок N – …»)
+ `meta.py` для свойств файла. Общая библиотека `reports/lib/`, на каждую лабу свой `make_report.py`.
Цифры в отчёт попадают из `results.json` реального прогона, а не вписываются руками.

---

## 3. Лабы и паттерны (в порядке приоритета)

Всего 69 уникальных паттернов (Vertical Partitioner есть и в гл. 7, и в гл. 8).
По плану (по уникальным): ✅ 65 · 🟡 4 · 📝 0.

| № | Тема | Глава | Почему на этом месте |
|---|---|---|---|
| 1 | Приём данных | 2 | фундамент: без него нечего обрабатывать |
| 2 | Обработка ошибок | 3 | самое «живое» в DE: DLQ, дубли, опоздания |
| 3 | Идемпотентность | 4 | перезапуски и бэкфиллы, ежедневная боль |
| 4 | Обогащение и агрегация | 5 (ч. 1) | первая бизнес-ценность |
| 5 | Сессии и упорядочивание | 5 (ч. 2) | самое интересное в стриминге |
| 6 | Качество данных (+ dbt) | 9 | важно, dbt-тесты и контракты |
| 7 | Потоки данных (Airflow) | 6 | оркестрация, частично уже затронута в 1–3 |
| 8 | Наблюдаемость | 10 | метрики и алерты поверх готового |
| 9 | Хранение (+ dbt, ClickHouse) | 8 | замеры «до/после» на большом объёме |
| 10 | Безопасность | 7 | 3 из 4 частичных паттернов здесь, кандидат на сокращение |

### Лаба 1 — Приём данных (гл. 2)
| Паттерн | Реализация | |
|---|---|---|
| Full Loader ⭐ | `blogs`/`articles` из Postgres → bronze целиком (справочник маленький) | ✅ |
| Incremental Loader ⭐ | `batch`-файлы визитов по партициям `date/hour` → bronze | ✅ |
| Change Data Capture ⭐ | Debezium: `users` → Redpanda → bronze | ✅ |
| Passthrough Replicator | копия топика / bronze-таблицы в «тестовое окружение» без изменений | ✅ |
| Transformation Replicator | та же репликация, но с вырезанием PII | ✅ |
| Compactor ⭐ | мелкие файлы от стриминга → `OPTIMIZE` Delta | ✅ |
| Readiness Marker ⭐ | Airflow ждёт `_SUCCESS` перед загрузкой партиции | ✅ |
| External Trigger | появление файла → запуск DAG через Airflow REST API / Datasets | ✅ |

### Лаба 2 — Обработка ошибок (гл. 3)
| Паттерн | Реализация | |
|---|---|---|
| Dead-Letter ⭐ | битые события → DLQ-топик + таблица с причиной | ✅ |
| Windowed Deduplicator ⭐ | дедуп в стриме по `event_id` в окне (watermark) | ✅ |
| Late Data Detector ⭐ | watermark, учёт и вывод опоздавших событий | ✅ |
| Static Late Data Integrator | ежедневный перепрогон последних N дней | ✅ |
| Dynamic Late Data Integrator ⭐ | перепрогон только тех партиций, куда реально пришли опоздавшие | ✅ |
| Filter Interceptor | фильтрация со счётчиками по причинам отбраковки | ✅ |
| Checkpointer ⭐ | чекпоинты стрима, демо: kill → рестарт без потерь и дублей | ✅ |

### Лаба 3 — Идемпотентность (гл. 4)
| Паттерн | Реализация | |
|---|---|---|
| Fast Metadata Cleaner | `DROP PARTITION`/`TRUNCATE` вместо `DELETE` (ClickHouse/Postgres) | ✅ |
| Data Overwrite ⭐ | Delta `replaceWhere` по партиции при перезапуске | ✅ |
| Merger ⭐ | `MERGE` изменений `users` из CDC в silver-измерение | ✅ |
| Stateful Merger | MERGE, корректный при бэкфилле (учёт версий и состояния) | ✅ |
| Keyed Idempotency ⭐ | детерминированные ключи результатов (сессий/агрегатов) | ✅ |
| Transactional Writer ⭐ | атомарная публикация: Delta-коммит / транзакция Postgres | ✅ |
| Proxy | view поверх версионных таблиц, переключение версии | ✅ |

### Лаба 4 — Ценность данных: обогащение и агрегация (гл. 5, ч. 1)
| Паттерн | Реализация | |
|---|---|---|
| Static Joiner ⭐ | визиты × справочник статей | ✅ |
| Dynamic Joiner ⭐ | stream-stream: визиты × поток изменений `users` (CDC) | ✅ |
| Wrapper | конверт: исходный payload + технические поля | ✅ |
| Metadata Decorator | `run_id`, версия джобы и т.п. в метаданных результата | ✅ |
| Distributed Aggregator ⭐ | агрегаты по статьям с shuffle | ✅ |
| Local Aggregator ⭐ | агрегация без shuffle: топик заранее партиционирован по ключу | ✅ |

### Лаба 5 — Ценность данных: сессии и упорядочивание (гл. 5, ч. 2)
| Паттерн | Реализация | |
|---|---|---|
| Incremental Sessionizer ⭐ | batch-сессии по часам с переносом незакрытых сессий | ✅ |
| Stateful Sessionizer ⭐ | streaming-сессии на состоянии (`applyInPandasWithState`) | ✅ |
| Bin Pack Orderer | отправка событий во внешний API (мок) пачками с сохранением порядка | ✅ |
| FIFO Orderer | то же, строго по одному | ✅ |

### Лаба 6 — Качество (гл. 9) + dbt
| Паттерн | Реализация | |
|---|---|---|
| Audit-Write-Audit-Publish ⭐ | staging → проверки (dbt tests) → публикация (swap) | ✅ |
| Constraints Enforcer ⭐ | Delta `CHECK`, ClickHouse `CONSTRAINT`, dbt model contracts | ✅ |
| Schema Compatibility Enforcer ⭐ | Schema Registry в режиме BACKWARD отбивает v3 генератора | ✅ |
| Schema Migrator | переименование поля с переходным периодом | ✅ |
| Offline Observer | отдельная ночная джоба профилирования качества | ✅ |
| Online Observer ⭐ | метрики качества внутри пайплайна | ✅ |

### Лаба 7 — Потоки данных (гл. 6), всё на Airflow
| Паттерн | Реализация | |
|---|---|---|
| Local Sequencer | цепочка задач внутри DAG | ✅ |
| Isolated Sequencer ⭐ | зависимость между DAG'ами (Datasets / сенсор) | ✅ |
| Aligned Fan-In ⭐ | дневной агрегат ждёт все 24 часовые партиции | ✅ |
| Unaligned Fan-In ⭐ | дневной агрегат идёт с неполными данными + флаг полноты | ✅ |
| Parallel Split | одна выгрузка → несколько независимых веток | ✅ |
| Exclusive Choice ⭐ | ветвление по версии схемы / объёму данных | ✅ |
| Single Runner | `max_active_runs=1` для зависимых от прошлого запусков | ✅ |
| Concurrent Runner | параллельный бэкфилл независимых партиций | ✅ |

### Лаба 8 — Наблюдаемость (гл. 10)
| Паттерн | Реализация | |
|---|---|---|
| Flow Interruption Detector ⭐ | нет данных N минут → алерт (генератор с `pauses`) | ✅ |
| Skew Detector | перекос размеров партиций / горячие ключи | ✅ |
| Lag Detector ⭐ | consumer lag Redpanda → Prometheus → алерт | ✅ |
| SLA Misses Detector ⭐ | витрина не готова к дедлайну → алерт (механизм Airflow или свой сенсор) | ✅ |
| Dataset Tracker | lineage на уровне таблиц: OpenLineage → Marquez | ✅ |
| Fine-Grained Tracker | lineage на уровне колонок (column lineage в OpenLineage из Spark) | 🟡 |

### Лаба 9 — Хранение (гл. 8) + dbt/ClickHouse, на большом объёме из `batch`
| Паттерн | Реализация | |
|---|---|---|
| Horizontal Partitioner ⭐ | партиции по дате (Delta `partitionBy`, ClickHouse `PARTITION BY`) | ✅ |
| Vertical Partitioner | широкая таблица → горячие и холодные колонки | ✅ |
| Bucket | `bucketBy` в Spark, join без shuffle | ✅ |
| Sorter ⭐ | Z-order в Delta vs `ORDER BY` в ClickHouse | ✅ |
| Metadata Enhancer | статистики и data skipping (Parquet min/max, Delta stats) | ✅ |
| Dataset Materializer ⭐ | материализованная gold-витрина (dbt-модель / CH materialized view) | ✅ |
| Manifest | загрузка по манифесту-списку файлов вместо листинга | ✅ |
| Normalizer | silver в нормализованной модели | ✅ |
| Denormalizer ⭐ | широкая gold-таблица в ClickHouse (dbt) | ✅ |

Все паттерны лабы 9 сопровождаются замерами «до/после» (время запроса, прочитанные байты).

### Лаба 10 — Безопасность (гл. 7)
| Паттерн | Реализация | |
|---|---|---|
| Vertical Partitioner ⭐ | PII `users` в отдельную таблицу | ✅ |
| In-Place Overwriter ⭐ | запрос «удалить пользователя» → Delta `DELETE` + `VACUUM` | ✅ |
| Fine-Grained Accessor for Tables ⭐ | GRANT на колонки, row policies в ClickHouse / RLS в Postgres | ✅ |
| Fine-Grained Accessor for Resources | identities SeaweedFS S3 с правами на бакеты (аналог IAM) | 🟡 |
| Encryptor | шифрование at-rest в S3 + колоночное (`pgcrypto`) | 🟡 |
| Anonymizer | удаление/замена PII через Faker | ✅ |
| Pseudo-Anonymizer ⭐ | хеширование с солью / токенизация `user_id`, email | ✅ |
| Secrets Pointer ⭐ | креды в Vault, в коде только ссылки | ✅ |
| Secretless Connector | облачный паттерн (IAM-роли). Локально: Vault dynamic secrets / сертификаты | 🟡 |

---

## 4. Коллективная разработка (под название дисциплины)

- **Git-flow:** `main` защищена; ветка на лабу `lab-NN-<тема>` → PR → self-review → merge → тег `lab-NN`
- **Шаблон PR:** реализованные паттерны, как запустить, как проверить, ссылка на отчёт
- **CI (GitHub Actions):** `ruff` + `pytest`: тесты генератора, юнит-тесты трансформаций на PySpark local
- **ADR** (`docs/adr/`) на ключевые решения: Redpanda вместо Kafka, Spark вместо Flink, SeaweedFS вместо MinIO и т.д.
- **Conventional commits**

## 5. Структура репозитория

```
blog-analytics-de-patterns/
├── docker/                 compose.yml с профилями, конфиги сервисов
├── generator/              генератор + config/*.yaml + tests
├── pipelines/
│   ├── spark/              джобы по главам (ingestion/, errors/, idempotency/, ...)
│   ├── airflow/dags/
│   └── dbt/                dbt-проект (ClickHouse)
├── sql/                    postgres/, clickhouse/
├── labs/
│   └── lab-NN-<тема>/      README.md, demo.sh, results.json, img/
├── reports/
│   ├── lib/                общая библиотека docx + template_title.docx + meta.py
│   └── lab-NN/make_report.py
├── docs/                   architecture.md, adr/
└── .github/                workflows/, pull_request_template.md
```

## 6. Структура отчёта (.docx)

Титульник ЮФУ, далее: тема → цель → «Ход работы» → по разделу на паттерн → вывод.
Раздел на паттерн:
1. **Проблема:** что ломается без паттерна (демо на дефектах генератора)
2. **Паттерн:** суть, ссылка на главу и страницу книги
3. **Реализация:** листинг, схема
4. **Проверка:** цифры из ground truth генератора / метрик (из `results.json`)
5. **Компромиссы:** чем платим, когда выбирать альтернативу

## 7. Открытые вопросы

1. Должность преподавателя (Чернышев-Придворов Ю. Г.) и форма «Проверил/Проверила» для титульника.
