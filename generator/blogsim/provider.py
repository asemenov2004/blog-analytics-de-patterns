"""Внешний поставщик справочника устройств (сценарий Full Loader / External Trigger из гл. 2).

Справочник публикуется целым снимком без колонки «изменено» — только полная загрузка.
Поставщик неидемпотентен: каждая публикация немного отличается от предыдущей
(часть устройств меняет версию, появляются новые), поэтому копировать нужно именно
опубликованный снимок, а не перезапрашивать его (сценарий Passthrough Replicator).
"""
from __future__ import annotations

import json
import random
import uuid
from datetime import UTC, datetime

from .config import Config

TYPES = ("mobile", "mobile", "mobile", "desktop", "desktop", "tablet", "tv")
BRANDS = {
    "mobile": ("Pixel", "Galaxy S", "iPhone", "Redmi Note", "Xperia", "Moto G"),
    "desktop": ("ThinkCentre", "OptiPlex", "iMac", "Mac mini", "Pavilion"),
    "tablet": ("iPad", "Galaxy Tab", "MatePad", "Tab P"),
    "tv": ("Bravia", "Smart TV", "Fire TV"),
}


def base_devices(cfg: Config) -> list[dict]:
    rng = random.Random(cfg.seed + 41)
    rows = []
    for did in range(1, cfg.provider.devices + 1):
        rows.append(_device(rng, did))
    return rows


def _device(rng: random.Random, did: int) -> dict:
    tp = rng.choice(TYPES)
    return {
        "device_id": did,
        "type": tp,
        "full_name": f"{rng.choice(BRANDS[tp])} {rng.randint(1, 20)}{rng.choice(('', ' Pro', ' Lite', ' Max'))}",
        "version": f"{rng.randint(1, 18)}.{rng.randint(0, 9)}",
    }


def snapshot(cfg: Config, rng: random.Random) -> tuple[list[dict], dict]:
    """Снимок = базовый справочник + случайные изменения этого вызова."""
    rows = base_devices(cfg)
    p = cfg.provider
    changed = 0
    for r in rows:
        if rng.random() * 100 < p.change_pct:
            r["version"] = f"{rng.randint(1, 18)}.{rng.randint(0, 9)}"
            changed += 1
    added = int(len(rows) * p.new_pct / 100)
    next_id = len(rows) + 1 + rng.randint(0, 10_000) * 10  # у разных снимков новые устройства разные
    rows += [_device(rng, next_id + i) for i in range(added)]
    return rows, {"changed": changed, "added": added}


def publish(cfg: Config, storage, producer, rng: random.Random, send_event: bool = True) -> dict:
    now = datetime.now(UTC)
    event_id = str(uuid.UUID(int=rng.getrandbits(128), version=4))
    rows, stats = snapshot(cfg, rng)
    # id в пути: две публикации в одну секунду не затирают друг друга
    key = f"providers/devices/published_at={now:%Y%m%dT%H%M%S}_{event_id[:8]}/devices.jsonl"
    storage.put(key, b"\n".join(json.dumps(r, separators=(",", ":")).encode() for r in rows) + b"\n")
    event = {
        "event_id": event_id,
        "type": "dataset_published",
        "dataset": "devices",
        "bucket": cfg.storage.landing_bucket,
        "key": key,
        "rows": len(rows),
        "published_at": now.isoformat(timespec="seconds"),
    }
    if send_event:
        producer.produce(cfg.storage.provider_topic, key=b"devices", value=json.dumps(event).encode())
        producer.flush(30)
    return {**event, **stats, "event_sent": send_event}
