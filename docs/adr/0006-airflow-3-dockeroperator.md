# ADR-0006. Airflow 3.3 и запуск Spark через DockerOperator

**Статус:** принято · 2026-10-16

## Контекст
Книга написана под Airflow 2: API `/api/v1`, `schedule_interval`. В кэше был образ 2.10.5,
актуальная версия — 3.3.2. Spark-джобы нужно запускать из DAG'ов, а в образе Airflow нет ни Java, ни Spark.

## Решение
- **Airflow 3.3.2**, `standalone` (LocalExecutor), simple auth manager с dev-учёткой admin/admin.
  Учебный проект на неподдерживаемой ветке 2.x выглядел бы хуже, а расхождения с книгой небольшие.
- **DockerOperator** (`lab_common.SparkJob`): каждая Spark-задача — отдельный контейнер `blog-de/spark:local`
  в сети стенда. Это локальный аналог `SparkKubernetesOperator` из книги. Каталог `pipelines` монтируется с хоста;
  путь на хосте берётся из монтирований контейнера Airflow через Docker API.
- Результат джобы — строка `RESULT {json}`. `SparkJob` ищет её среди всех строк лога: JVM при остановке может
  напечатать что-то после неё.
- Часовые DAG'и используют явный `CronDataIntervalTimetable`. Строка `"@hourly"` в Airflow 3 даёт расписание без
  интервала данных (`data_interval_start == data_interval_end`).
- Логи задач — в томе `airflow_logs`: без него они пропадают при пересоздании контейнера.

## Последствия
- Airflow получает доступ к `docker.sock`, то есть полный контроль над Docker хоста. Для локального стенда
  допустимо, в проде так не делают.
- Отличия от книги (API v2 с JWT-токеном, `airflow.sdk`, `backfill create`) описываются в отчётах.
