"""External Trigger (гл. 2, с. 57): обработчик уведомлений о публикации наборов данных.

Три шага паттерна: подписка на канал уведомлений (топик provider_events) → реакция
(фильтр по типу события и набору данных) → запуск DAG через REST API Airflow с контекстом
(событие + метаданные триггера). Ошибка запуска не теряет событие: оно уходит в DLQ.

    python listener.py            слушать provider_events
    python listener.py --replay   повторно обработать накопленное в DLQ и выйти
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import sys

import requests
from confluent_kafka import Consumer, Producer

VERSION = "1.0.0"
TOPIC = os.getenv("PROVIDER_TOPIC", "provider_events")
DLQ = os.getenv("PROVIDER_DLQ_TOPIC", "provider_events_dlq")
AIRFLOW = os.getenv("AIRFLOW_URL", "http://airflow:8080")
# какой DAG запускать на какой набор данных
ROUTES = {"devices": "devices_full_load"}

log = logging.getLogger("trigger")


class Airflow:
    def __init__(self, url: str, user: str, password: str):
        self.url, self.user, self.password = url.rstrip("/"), user, password
        self.token: str | None = None

    def _auth(self) -> None:
        r = requests.post(f"{self.url}/auth/token", json={"username": self.user, "password": self.password},
                          timeout=10)
        r.raise_for_status()
        self.token = r.json()["access_token"]

    def trigger(self, dag_id: str, run_id: str, conf: dict) -> str:
        """'created' | 'exists' (повторное событие — запуск уже был, это не ошибка)."""
        for attempt in (1, 2):
            if self.token is None:
                self._auth()
            r = requests.post(f"{self.url}/api/v2/dags/{dag_id}/dagRuns",
                              headers={"Authorization": f"Bearer {self.token}"},
                              json={"dag_run_id": run_id, "logical_date": None, "conf": conf}, timeout=15)
            if r.status_code == 401 and attempt == 1:  # токен истёк
                self.token = None
                continue
            if r.status_code == 409:
                return "exists"
            r.raise_for_status()
            return "created"
        raise RuntimeError("не удалось авторизоваться в Airflow")


def handle(airflow: Airflow, msg, producer: Producer) -> str:
    raw = msg.value()
    try:
        event = json.loads(raw)
        if event.get("type") != "dataset_published" or event.get("dataset") not in ROUTES:
            return "skipped"
        dag_id = ROUTES[event["dataset"]]
        conf = {
            "event": event,
            "trigger": {"name": "trigger-listener", "version": VERSION, "host": socket.gethostname(),
                        "topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset()},
        }
        return airflow.trigger(dag_id, f"external-{event['event_id']}", conf)
    except Exception as exc:  # событие не теряем ни при какой ошибке
        producer.produce(DLQ, key=msg.key(), value=raw, headers=[
            ("error", f"{type(exc).__name__}: {exc}"[:500].encode()),
            ("source_topic", msg.topic().encode()), ("source_offset", str(msg.offset()).encode()),
        ])
        producer.flush(10)
        log.warning("в DLQ: offset=%s %s", msg.offset(), exc)
        return "dlq"


def run(topic: str, group: str, stop_at_end: bool) -> dict:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": group, "auto.offset.reset": "earliest",
                         "enable.auto.commit": False, "enable.partition.eof": stop_at_end})
    producer = Producer({"bootstrap.servers": bootstrap})
    airflow = Airflow(AIRFLOW, os.getenv("AIRFLOW_USER", "admin"), os.getenv("AIRFLOW_PASSWORD", "admin"))
    consumer.subscribe([topic])
    stats: dict[str, int] = {}
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if stop_at_end and msg.error().code() == -191:  # _PARTITION_EOF
                    break
                log.error("kafka: %s", msg.error())
                continue
            result = handle(airflow, msg, producer)
            stats[result] = stats.get(result, 0) + 1
            log.info("offset=%s → %s", msg.offset(), result)
            consumer.commit(msg, asynchronous=False)  # коммит только после обработки: at-least-once
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
    return stats


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    p = argparse.ArgumentParser()
    p.add_argument("--replay", action="store_true", help="переобработать DLQ и выйти")
    args = p.parse_args()
    if args.replay:
        print(json.dumps(run(DLQ, "trigger-listener-dlq-replay", stop_at_end=True)))
    else:
        run(TOPIC, "trigger-listener", stop_at_end=False)


if __name__ == "__main__":
    main()
