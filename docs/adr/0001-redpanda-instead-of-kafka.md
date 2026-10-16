# ADR-0001. Redpanda вместо Apache Kafka

**Статус:** принято · 2026-10-13

## Контекст
Нужен брокер с Kafka API и Schema Registry (лаба 6: Schema Compatibility Enforcer).
Стенд крутится на ноутбуке с 16 ГБ RAM, под WSL2 выделено 12 ГБ.

## Решение
Redpanda в режиме `dev-container`: один процесс, Kafka API и встроенный Schema Registry.
Клиенты (confluent-kafka, Spark Kafka connector, Debezium) работают с ним как с Kafka.

## Последствия
- Экономия памяти и одного сервиса: отдельный Schema Registry не нужен.
- Внутренности брокера отличаются от Kafka (нет ZooKeeper/KRaft-настроек), для паттернов книги это не важно.
