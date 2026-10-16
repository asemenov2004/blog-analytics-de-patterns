# ADR-0003. SeaweedFS вместо MinIO как S3-хранилище

**Статус:** принято · 2026-10-13

## Контекст
Изначально в плане был MinIO. Проверка 2026-10-13: репозиторий `minio/minio` на Docker Hub
отдаёт 404. Старый образ есть только в локальном кэше, на чистой машине или в CI он не скачается.

## Решение
SeaweedFS (`chrislusf/seaweedfs:4.48`) в режиме `weed server -s3`: master, volume, filer и S3 API
в одном контейнере. Доступ — identities в s3.json, собирается из окружения при старте.

## Последствия
- Воспроизводимо: образ с версионным тегом доступен в Docker Hub.
- Spark (S3A, path-style) и boto3 работают без изменений.
- Лаба 10 (Fine-Grained Accessor for Resources): права задаются через identities SeaweedFS вместо
  политик MinIO. По-прежнему эмуляция IAM, статус 🟡.
