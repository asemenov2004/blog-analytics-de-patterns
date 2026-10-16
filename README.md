# Blog Analytics — паттерны инженерии данных

Лабораторные по дисциплине «Технологии коллективной разработки» на материале книги
Б. Конечны «Инженерия данных. Паттерны проектирования» (Алист, 2026; ориг. *Data Engineering Design Patterns*, O'Reilly, 2025).

Один проект на весь курс — платформа аналитики блогов (сценарий из гл. 1 книги).
Каждая лаба реализует паттерны одной главы поверх того, что уже построено.
План, порядок лаб и покрытие паттернов — [docs/plan.md](docs/plan.md), решения по стеку — [docs/adr/](docs/adr/).

## Стенд

| Сервис | Профиль | Адрес с хоста |
|---|---|---|
| Postgres 16 (OLTP: `users`, `blogs`, `articles`) | `core` | `localhost:15432`, БД `blog` |
| Redpanda (Kafka API) | `core` | `localhost:19092` |
| Schema Registry | `core` | `http://localhost:18081` |
| Redpanda Console | `core` | http://localhost:18080 |
| SeaweedFS S3 (бакеты `landing`, `lake`, `truth`) | `core` | `http://localhost:18333` |
| Spark 3.5.8 + Delta 3.3.3 | `spark` | Spark UI `http://localhost:4040` во время джобы |
| Генератор `blogsim` | `tools` | запускается через `docker compose run` |

Логины и пароли — dev-значения по умолчанию (см. [docker/.env.example](docker/.env.example)).

### Запуск

Требуется Docker Desktop с WSL2. Под стенд нужно 6+ ГБ свободной памяти.

Образ Spark собирается с jar'ами из [docker/spark/jars.sha1](docker/spark/jars.sha1). Из РФ Maven Central
отдаёт около 65 КБ/с, поэтому jar'ы лучше положить в `docker/spark/jars/` заранее (ссылки —
`docker/spark/jars/URLS.txt`). Чего там нет, сборка докачает сама. SHA-1 проверяется в обоих случаях.

```bash
docker compose -f docker/compose.yml --profile core up -d --wait
docker compose -f docker/compose.yml run --rm generator bootstrap
docker compose -f docker/compose.yml run --rm generator batch --start 2026-10-01T00:00:00Z --hours 6
docker compose -f docker/compose.yml run --rm spark spark-submit /opt/pipelines/spark/smoke/delta_s3_smoke.py
```

Остановить стенд с сохранением данных — `down`, сбросить всё — `down -v`:

```bash
docker compose -f docker/compose.yml --profile core --profile spark down
```

> **Git Bash на Windows** переписывает аргументы, похожие на абсолютные пути (`/app/...`).
> Пути к конфигам генератора передавай относительно `/app`, например `--config config/stream-demo.yaml`.
> Для `spark-submit /opt/...` это не мешает: MSYS не трогает пути, которых нет в Windows. Если тронет — `MSYS_NO_PATHCONV=1`.

## Генератор `blogsim`

Модель трафика (сессии пользователей) → доставка с дефектами → приёмники. Детерминирован от `seed`.

| Команда | Что делает |
|---|---|
| `bootstrap` | бакеты S3, топик `visits`, сидинг Postgres. Повторный запуск безопасен |
| `batch --start ... --hours N` | JSONL по часам доставки `landing/visits/date=/hour=` + `_SUCCESS` |
| `stream --minutes N [--speed K]` | визиты в топик `visits` в реальном времени, ключ — `user_id` |
| `oltp --ops N --per-second R` | вставки, обновления и удаления в `users` / `articles` |

Дефекты настраиваются в [defaults.yaml](generator/blogsim/defaults.yaml), своими YAML (`--config`) или точечно (`--set`):

```bash
docker compose -f docker/compose.yml run --rm generator batch --start 2026-10-01T00:00:00Z --hours 24 --set defects.duplicates_pct=5 --set "defects.pauses=[{at_min: 120, duration_min: 30}]"
```

Дубли, опоздания, перемешивание, битые записи, боты, «висячие» ссылки, горячие ключи, паузы,
смена версии схемы (v1 → v2 совместимо, v3 несовместимо), партиции без `_SUCCESS`.

**Ground truth:** каждый прогон пишет в бакет `truth` файлы `runs/<run_id>/defects.jsonl` (каждая дефектная
запись) и `summary.json` (счётчики). С ними сверяются результаты паттернов.

## Разработка

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -e "generator[dev]"
.venv/Scripts/ruff check .
.venv/Scripts/python -m pytest generator/tests -q
```

Процесс: ветка `lab-NN-<тема>` → PR по [шаблону](.github/pull_request_template.md) → CI (ruff, pytest,
проверка compose) → merge → тег `lab-NN`.

## Отчёты

Отчёты .docx собираются скриптом из результатов реального прогона: `reports/lab-NN/make_report.py`
на общей библиотеке [reports/lib](reports/lib/docx_report.py) (титульник ЮФУ, ГОСТ-подписи).

```bash
.venv/Scripts/python -m pip install -r reports/requirements.txt
```

## Структура

```
docker/        compose.yml (профили), образы Spark и генератора, init Postgres
generator/     blogsim: генератор данных + тесты
pipelines/     spark/ (джобы по главам), далее airflow/, dbt/
labs/          lab-NN-<тема>/: README, demo-сценарий, results.json
reports/       lib/ (общая сборка docx), lab-NN/make_report.py
docs/          plan.md, adr/
```
