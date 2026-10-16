"""Доставка визитов: задержки, дубли, битые записи, версии схемы.

Каждому визиту назначается arrival_time >= event_time. Записи отдаются строго в порядке
arrival_time: запись из буфера безопасно отдавать, как только её arrival <= event_time
очередного визита (у всех следующих arrival не раньше их event_time).
"""
from __future__ import annotations

import heapq
import json
import random
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime

from .config import Config
from .sim import TrafficModel, Visit

MALFORMED_KINDS = ("truncated_json", "missing_event_id", "missing_event_time", "user_id_not_int", "bad_timestamp")


@dataclass(slots=True)
class Record:
    seq: int
    arrival: float
    event_time: float
    key: bytes
    value: bytes
    event_id: str | None
    schema_version: int
    defects: tuple[str, ...]
    detail: dict


def iso(t: float) -> str:
    return datetime.fromtimestamp(t, UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def payload(v: Visit, version: int) -> dict:
    """v1 — базовая; v2 — +device_type (совместимо); v3 — article_id→content_id,
    event_time в epoch ms (несовместимо)."""
    p = {
        "event_id": v.event_id,
        "event_time": iso(v.event_time),
        "user_id": v.user_id,
        "article_id": v.article_id,
        "blog_id": v.blog_id,
        "event_type": v.event_type,
        "ip": v.ip,
        "user_agent": v.user_agent,
        "referrer": v.referrer,
        "schema_version": version,
    }
    if version >= 2:
        p["device_type"] = v.device_type
    if version >= 3:
        p["content_id"] = p.pop("article_id")
        p["event_time"] = int(round(v.event_time * 1000))
    return p


def corrupt(p: dict, kind: str) -> bytes:
    p = dict(p)
    if kind == "truncated_json":
        raw = json.dumps(p, separators=(",", ":")).encode()
        return raw[: len(raw) // 2]
    if kind == "missing_event_id":
        p.pop("event_id")
    elif kind == "missing_event_time":
        p.pop("event_time")
    elif kind == "user_id_not_int":
        p["user_id"] = f"u-{p['user_id']}"
    elif kind == "bad_timestamp":
        p["event_time"] = "yesterday"
    return json.dumps(p, separators=(",", ":")).encode()


class Delivery:
    def __init__(self, cfg: Config, model: TrafficModel, rng: random.Random | None = None):
        self.cfg = cfg
        self.model = model
        self.rng = rng or random.Random(cfg.seed + 1)

    def _latency(self) -> tuple[float, tuple[str, ...], dict]:
        d, r = self.cfg.defects, self.rng.random() * 100
        if r < d.late_pct:
            lat = self.rng.uniform(d.late_min_delay_s, d.late_max_delay_s)
            return lat, ("late",), {"latency_s": round(lat, 3)}
        if r < d.late_pct + d.out_of_order_pct:
            lat = self.rng.uniform(0, d.out_of_order_max_s)
            return lat, ("out_of_order",), {"latency_s": round(lat, 3)}
        return self.rng.uniform(0, 1.0), (), {}

    def _arrival(self, t: float) -> float:
        # пока источник «лежит», доставка копится и уходит пачкой после паузы
        end = self.model.in_pause(t)
        return t if end is None else end + self.rng.uniform(0, 5)

    def records(self, end: float | None = None, drain: bool = True) -> Iterator[Record]:
        """drain=False — не выдавать записи с arrival >= end (режим stream)."""
        d = self.cfg.defects
        heap: list[tuple[float, int, Record]] = []
        n = seq = 0

        def push(rec: Record) -> None:
            nonlocal n
            n += 1
            heapq.heappush(heap, (rec.arrival, n, rec))

        def pop() -> Record:
            nonlocal seq
            rec = heapq.heappop(heap)[2]
            rec.seq, seq = seq, seq + 1
            return rec

        for v in self.model.visits(end):
            while heap and heap[0][0] <= v.event_time:
                yield pop()
            lat, lat_defects, detail = self._latency()
            arrival = self._arrival(v.event_time + lat)
            version = self.cfg.schema.version_at(arrival - self.model.start)
            p = payload(v, version)
            defects = tuple(v.defects) + lat_defects
            if self.rng.random() < d.malformed_pct / 100:
                kind = self.rng.choice(MALFORMED_KINDS)
                value = corrupt(p, kind)
                defects += ("malformed",)
                detail = {**detail, "malformed_kind": kind}
            else:
                value = json.dumps(p, separators=(",", ":")).encode()
            rec = Record(0, arrival, v.event_time, str(v.user_id).encode(), value, v.event_id,
                         version, defects, detail)
            push(rec)
            if self.rng.random() < d.duplicates_pct / 100:
                dup_arrival = self._arrival(arrival + self.rng.uniform(0, d.duplicate_max_delay_s))
                push(Record(0, dup_arrival, v.event_time, rec.key, value, v.event_id, version,
                            defects + ("duplicate",), {**detail, "first_arrival": iso(arrival)}))
        while heap:
            if not drain and end is not None and heap[0][0] >= end:
                return
            yield pop()
