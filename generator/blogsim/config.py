"""Конфигурация генератора: defaults.yaml + пользовательские YAML + точечные --set."""
from __future__ import annotations

import os
from dataclasses import dataclass, field, fields, is_dataclass
from pathlib import Path
from typing import Any, get_args, get_origin, get_type_hints

import yaml

DEFAULTS = Path(__file__).with_name("defaults.yaml")


@dataclass
class Catalog:
    users: int
    blogs: int
    articles: int


@dataclass
class Traffic:
    sessions_per_minute: float
    mean_pageviews: float
    mean_gap_s: float
    max_gap_s: float
    click_share: float
    same_blog_share: float


@dataclass
class Pause:
    at_min: float
    duration_min: float


@dataclass
class Defects:
    duplicates_pct: float
    duplicate_max_delay_s: float
    late_pct: float
    late_min_delay_s: float
    late_max_delay_s: float
    out_of_order_pct: float
    out_of_order_max_s: float
    malformed_pct: float
    bots_pct: float
    orphan_refs_pct: float
    hot_users: int
    hot_users_share: float
    hot_articles: int
    hot_articles_share: float
    missing_success_pct: float
    pauses: list[Pause] = field(default_factory=list)


@dataclass
class SchemaChange:
    at_min: float
    version: int


@dataclass
class Schema:
    version: int
    changes: list[SchemaChange] = field(default_factory=list)

    def version_at(self, offset_s: float) -> int:
        """Версия схемы, которую использует продюсер через offset_s секунд от старта."""
        version = self.version
        for ch in sorted(self.changes, key=lambda c: c.at_min):
            if offset_s >= ch.at_min * 60:
                version = ch.version
        return version


@dataclass
class Storage:
    landing_bucket: str
    truth_bucket: str
    lake_bucket: str
    visits_topic: str
    visits_partitions: int


@dataclass
class Config:
    seed: int
    catalog: Catalog
    traffic: Traffic
    defects: Defects
    schema: Schema
    storage: Storage


@dataclass
class Connections:
    """Адреса сервисов берутся только из окружения, в YAML их нет."""
    kafka_bootstrap: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP", "localhost:19092"))
    s3_endpoint: str = field(default_factory=lambda: os.getenv("S3_ENDPOINT", "http://localhost:18333"))
    s3_access_key: str = field(default_factory=lambda: os.getenv("S3_ACCESS_KEY", ""))
    s3_secret_key: str = field(default_factory=lambda: os.getenv("S3_SECRET_KEY", ""))
    pg_dsn: str = field(default_factory=lambda: os.getenv("PG_DSN", ""))


def _build(cls: type, data: Any, path: str) -> Any:
    if not is_dataclass(cls):
        return data
    if not isinstance(data, dict):
        raise ValueError(f"{path or 'config'}: ожидался словарь, получено {data!r}")
    hints = get_type_hints(cls)
    known = {f.name for f in fields(cls)}
    unknown = set(data) - known
    if unknown:
        raise ValueError(f"{path or 'config'}: неизвестные ключи {sorted(unknown)}")
    kwargs = {}
    for name in known & set(data):
        tp, value = hints[name], data[name]
        if get_origin(tp) is list:
            (item,) = get_args(tp)
            kwargs[name] = [_build(item, v, f"{path}.{name}[{i}]") for i, v in enumerate(value or [])]
        elif is_dataclass(tp):
            kwargs[name] = _build(tp, value, f"{path}.{name}".lstrip("."))
        else:
            kwargs[name] = tp(value) if tp in (int, float, str) else value
    return cls(**kwargs)


def _merge(base: dict, over: dict) -> dict:
    out = dict(base)
    for k, v in over.items():
        out[k] = _merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out


def _apply_set(raw: dict, assignment: str) -> None:
    key, sep, value = assignment.partition("=")
    if not sep:
        raise ValueError(f"--set ожидает ключ=значение, получено {assignment!r}")
    node = raw
    *parents, leaf = key.strip().split(".")
    for p in parents:
        node = node.setdefault(p, {})
    node[leaf] = yaml.safe_load(value)


def load_config(paths: list[str | Path] = (), overrides: list[str] = ()) -> Config:
    raw = yaml.safe_load(DEFAULTS.read_text(encoding="utf-8"))
    for p in paths:
        raw = _merge(raw, yaml.safe_load(Path(p).read_text(encoding="utf-8")) or {})
    for o in overrides:
        _apply_set(raw, o)
    return _build(Config, raw, "")
