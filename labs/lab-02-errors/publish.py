"""Публикация изолированной партии в одноимённый топик Redpanda."""
import argparse
import json
import tempfile
from pathlib import Path

from bench import generate
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

p = argparse.ArgumentParser()
p.add_argument("topic")
p.add_argument("size", type=int)
p.add_argument("--seed", type=int, default=42)
args = p.parse_args()
config = {"bootstrap.servers": "redpanda:9092"}
# Существующий топик запрещён: повтор эксперимента получает собственные offsets.
admin = AdminClient(config)
for future in admin.create_topics([NewTopic(args.topic, 1, 1), NewTopic(args.topic + "_dlq", 1, 1)]).values():
    future.result()
producer = Producer(config)
errors = []


def delivered(err, msg):
    if err:
        errors.append(str(err))


with tempfile.TemporaryDirectory() as tmp:
    path = Path(tmp) / "visits.jsonl"
    counts, hours = generate(path, args.size, args.seed)
    for raw in path.read_bytes().splitlines():
        while True:
            try:
                producer.produce(args.topic, value=raw, on_delivery=delivered)
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)
    if producer.flush(60) or errors:
        raise RuntimeError(errors)
    print("TRUTH " + json.dumps({"outcomes": counts, "hours": hours}))

