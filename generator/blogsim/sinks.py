"""Приёмники записей и журнал ground truth.

Storage — куда класть байты (локальный каталог или S3). PartitionedSink раскладывает
записи по партициям date=/hour= времени доставки и после закрытия партиции ставит _SUCCESS.
"""
from __future__ import annotations

import json
import random
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from .delivery import Record, iso


class Storage(Protocol):
    def put(self, path: str, data: bytes) -> None: ...


class DirStorage:
    def __init__(self, root: str | Path):
        self.root = Path(root)

    def put(self, path: str, data: bytes) -> None:
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)

    def __str__(self) -> str:
        return str(self.root)


class S3Storage:
    def __init__(self, client, bucket: str, prefix: str = ""):
        self.client, self.bucket, self.prefix = client, bucket, prefix.strip("/")

    def put(self, path: str, data: bytes) -> None:
        key = f"{self.prefix}/{path}" if self.prefix else path
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data)

    def __str__(self) -> str:
        return f"s3://{self.bucket}/{self.prefix}"


def partition_of(t: float) -> str:
    dt = datetime.fromtimestamp(t, UTC)
    return f"date={dt:%Y-%m-%d}/hour={dt:%H}"


class PartitionedSink:
    """JSONL-файлы по часам доставки. Записи приходят в порядке arrival, поэтому смена
    партиции означает, что предыдущая закрыта и её можно публиковать."""

    def __init__(self, storage: Storage, dataset: str, missing_success_pct: float = 0.0,
                 rng: random.Random | None = None, truth: TruthLog | None = None):
        self.storage, self.dataset = storage, dataset
        self.missing_success_pct = missing_success_pct
        self.rng = rng or random.Random(0)
        self.truth = truth
        self.current: str | None = None
        self.buf: list[bytes] = []
        self.partitions: list[str] = []

    def write(self, rec: Record) -> None:
        part = partition_of(rec.arrival)
        if part != self.current:
            self._flush()
            self.current = part
        self.buf.append(rec.value)

    def _flush(self) -> None:
        if self.current is None:
            return
        base = f"{self.dataset}/{self.current}"
        self.storage.put(f"{base}/part-00000.jsonl", b"\n".join(self.buf) + b"\n")
        if self.rng.random() * 100 < self.missing_success_pct:
            if self.truth:
                self.truth.partition_defect(base, "missing_success")
        else:
            self.storage.put(f"{base}/_SUCCESS", b"")
        self.partitions.append(base)
        self.buf = []

    def close(self) -> None:
        self._flush()
        self.current = None


class KafkaSink:
    def __init__(self, producer, topic: str):
        self.producer, self.topic = producer, topic
        self.errors: list[str] = []

    def _on_delivery(self, err, _msg) -> None:
        if err is not None:
            self.errors.append(str(err))

    def write(self, rec: Record) -> None:
        while True:
            try:
                self.producer.produce(self.topic, key=rec.key, value=rec.value,
                                      timestamp=int(rec.arrival * 1000), on_delivery=self._on_delivery)
                break
            except BufferError:
                self.producer.poll(0.5)
        self.producer.poll(0)

    def close(self) -> None:
        self.producer.flush(30)
        if self.errors:
            raise RuntimeError(f"Kafka: {len(self.errors)} ошибок доставки, первая: {self.errors[0]}")


class TruthLog:
    """Журнал внесённых дефектов: defects.jsonl (по записи на дефектную запись) + summary.json."""

    def __init__(self, storage: Storage, run_id: str, meta: dict):
        self.storage, self.run_id, self.meta = storage, run_id, meta
        self.lines: list[bytes] = []
        self.counts: Counter[str] = Counter()
        self.records = 0
        self.originals = 0
        self.versions: Counter[int] = Counter()
        self.first_arrival = self.last_arrival = None
        self.partition_defects: list[dict] = []

    def add(self, rec: Record) -> None:
        self.records += 1
        self.versions[rec.schema_version] += 1
        if "duplicate" not in rec.defects:
            self.originals += 1
        self.first_arrival = self.first_arrival or rec.arrival
        self.last_arrival = rec.arrival
        if rec.defects:
            self.counts.update(rec.defects)
            self.lines.append(json.dumps({
                "seq": rec.seq, "event_id": rec.event_id, "defects": list(rec.defects),
                "event_time": iso(rec.event_time), "arrival_time": iso(rec.arrival), **rec.detail,
            }, separators=(",", ":")).encode())

    def partition_defect(self, partition: str, kind: str) -> None:
        self.counts[kind] += 1
        self.partition_defects.append({"partition": partition, "defect": kind})

    def summary(self) -> dict:
        return {
            "run_id": self.run_id, **self.meta,
            "records": self.records,
            "original_records": self.originals,
            "first_arrival": iso(self.first_arrival) if self.first_arrival else None,
            "last_arrival": iso(self.last_arrival) if self.last_arrival else None,
            "schema_versions": {str(k): v for k, v in sorted(self.versions.items())},
            "defects": dict(sorted(self.counts.items())),
            "partition_defects": self.partition_defects,
        }

    def close(self) -> dict:
        base = f"runs/{self.run_id}"
        self.storage.put(f"{base}/defects.jsonl", b"\n".join(self.lines) + (b"\n" if self.lines else b""))
        s = self.summary()
        self.storage.put(f"{base}/summary.json", json.dumps(s, ensure_ascii=False, indent=2).encode())
        return s
