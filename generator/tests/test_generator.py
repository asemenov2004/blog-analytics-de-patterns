import json
import random
from collections import Counter
from pathlib import Path

import pytest

from blogsim.cli import run_batch
from blogsim.config import load_config
from blogsim.delivery import Delivery
from blogsim.sim import TrafficModel
from blogsim.sinks import DirStorage

START = 1_790_000_000.0  # 2026-09-21T12:53:20Z
HOUR = 3600


def records(cfg, hours=2.0, drain=True):
    model = TrafficModel(cfg, START, random.Random(cfg.seed))
    return list(Delivery(cfg, model, random.Random(cfg.seed + 1)).records(START + hours * HOUR, drain=drain))


def valid(value: bytes) -> dict | None:
    """Минимальная проверка, которую сделает потребитель (как будущий Dead-Letter)."""
    try:
        p = json.loads(value)
    except json.JSONDecodeError:
        return None
    if not p.get("event_id") or "event_time" not in p or not isinstance(p.get("user_id"), int):
        return None
    if isinstance(p["event_time"], str) and not p["event_time"][:4].isdigit():
        return None
    return p


# ------------------------------------------------------------------ конфиг
def test_config_overrides_and_validation():
    cfg = load_config(overrides=["defects.duplicates_pct=7.5", "schema.changes=[{at_min: 10, version: 3}]"])
    assert cfg.defects.duplicates_pct == 7.5
    assert cfg.schema.version_at(0) == 1 and cfg.schema.version_at(600) == 3
    with pytest.raises(ValueError, match="неизвестные ключи"):
        load_config(overrides=["defects.dupliactes_pct=1"])


# ------------------------------------------------------------------ модель и доставка
def test_deterministic_by_seed():
    cfg = load_config()
    a, b = records(cfg, 0.5), records(cfg, 0.5)
    assert [r.value for r in a] == [r.value for r in b]
    c = records(load_config(overrides=["seed=7"]), 0.5)
    assert [r.value for r in a] != [r.value for r in c]


def test_arrival_order_and_latency_bounds():
    cfg = load_config()
    recs = records(cfg)
    assert len(recs) > 5_000
    assert all(x.arrival <= y.arrival for x, y in zip(recs, recs[1:]))
    assert [r.seq for r in recs] == list(range(len(recs)))
    d = cfg.defects
    for r in recs:
        lat = r.arrival - r.event_time
        assert lat >= 0
        if "late" in r.defects and "duplicate" not in r.defects:
            assert d.late_min_delay_s <= lat <= d.late_max_delay_s
        elif "out_of_order" in r.defects and "duplicate" not in r.defects:
            assert lat <= d.out_of_order_max_s


def test_defect_rates_close_to_config():
    cfg = load_config(overrides=["defects.duplicates_pct=5", "defects.malformed_pct=2", "defects.late_pct=3"])
    recs = records(cfg, 3)
    originals = [r for r in recs if "duplicate" not in r.defects]
    n = len(originals)
    share = lambda kind: sum(kind in r.defects for r in originals) / n * 100  # noqa: E731
    assert abs(sum("duplicate" in r.defects for r in recs) / n * 100 - 5) < 1
    assert abs(share("malformed") - 2) < 0.6
    # late в окне [start, end) частично не долетает — проверяем снизу и сверху мягко
    assert 1.5 < share("late") < 3.6


def test_duplicates_are_exact_copies_delivered_later():
    recs = records(load_config(overrides=["defects.duplicates_pct=5"]))
    first: dict[str, tuple[int, bytes]] = {}
    for r in recs:
        if "duplicate" in r.defects:
            seq, value = first[r.event_id]
            assert seq < r.seq and value == r.value
        else:
            assert r.event_id not in first, "event_id оригинала уникален"
            first[r.event_id] = (r.seq, r.value)


def test_malformed_detectable_and_clean_valid():
    recs = records(load_config(overrides=["defects.malformed_pct=3"]))
    for r in recs:
        if "malformed" in r.defects:
            assert valid(r.value) is None, r.detail
        else:
            assert valid(r.value) is not None


def test_schema_versions_switch_by_arrival():
    cfg = load_config(overrides=["schema.changes=[{at_min: 30, version: 2}, {at_min: 60, version: 3}]",
                                 "defects.malformed_pct=0"])
    recs = records(cfg, 1.5)
    for r in recs:
        p = json.loads(r.value)
        if "duplicate" in r.defects:  # повторная отправка тех же байтов — версия оригинала
            continue
        offset = r.arrival - START
        expected = 1 if offset < 1800 else 2 if offset < 3600 else 3
        assert p["schema_version"] == r.schema_version == expected
        if expected == 3:
            assert "content_id" in p and "article_id" not in p and isinstance(p["event_time"], int)
        else:
            assert "article_id" in p and ("device_type" in p) == (expected == 2)


def test_sessions_respect_gap_and_bots_flagged():
    cfg = load_config(overrides=["defects.malformed_pct=0", "defects.duplicates_pct=0"])
    model = TrafficModel(cfg, START, random.Random(cfg.seed))
    visits = list(model.visits(START + HOUR))
    assert all(a.event_time <= b.event_time for a, b in zip(visits, visits[1:]))
    bots = [v for v in visits if "bot" in v.defects]
    assert bots and all(v.device_type == "bot" for v in bots)
    assert all(v.device_type != "bot" for v in visits if "bot" not in v.defects)


def test_pause_has_no_events_inside():
    cfg = load_config(overrides=["defects.pauses=[{at_min: 20, duration_min: 15}]"])
    recs = records(cfg, 1)
    a, b = START + 20 * 60, START + 35 * 60
    assert not [r for r in recs if a <= r.event_time < b]
    assert not [r for r in recs if a <= r.arrival < b]
    assert [r for r in recs if r.arrival >= b]


def test_hot_users_skew():
    cfg = load_config(overrides=["defects.hot_users=5", "defects.hot_users_share=0.5", "defects.bots_pct=0"])
    model = TrafficModel(cfg, START, random.Random(cfg.seed))
    users = Counter(v.user_id for v in model.visits(START + HOUR))
    hot = sum(users[u] for u in range(1, 6)) / sum(users.values())
    assert 0.4 < hot < 0.6


# ------------------------------------------------------------------ batch в каталог
def test_batch_to_dir_matches_truth(tmp_path: Path):
    cfg = load_config(overrides=["defects.missing_success_pct=20"])
    summary = run_batch(cfg, START, 3, DirStorage(tmp_path / "landing"), DirStorage(tmp_path / "truth"), "t1")
    parts = sorted((tmp_path / "landing" / "visits").glob("date=*/hour=*"))
    assert len(parts) == summary["partitions"] >= 3
    lines = sum(len((p / "part-00000.jsonl").read_bytes().splitlines()) for p in parts)
    assert lines == summary["records"]
    missing = [p for p in parts if not (p / "_SUCCESS").exists()]
    assert len(missing) == summary["defects"].get("missing_success", 0)
    truth = (tmp_path / "truth" / "runs" / "t1" / "defects.jsonl").read_text().splitlines()
    dup_in_truth = sum("duplicate" in json.loads(x)["defects"] for x in truth)
    assert dup_in_truth == summary["defects"]["duplicate"] == summary["records"] - summary["original_records"]


# ------------------------------------------------------------------ внешний поставщик
def test_provider_snapshots_differ_but_share_base(tmp_path: Path):
    from blogsim.provider import publish

    cfg = load_config()
    storage = DirStorage(tmp_path)
    a = publish(cfg, storage, None, random.Random(1), send_event=False)
    b = publish(cfg, storage, None, random.Random(2), send_event=False)
    rows_a = [json.loads(x) for x in (tmp_path / a["key"]).read_text().splitlines()]
    assert len(rows_a) == a["rows"] == cfg.provider.devices + a["added"]
    assert not any("updated" in k or "modified" in k for k in rows_a[0])  # нет колонки «изменено»
    assert (a["changed"], a["added"]) != (0, 0)
    rows_b = [json.loads(x) for x in (tmp_path / b["key"]).read_text().splitlines()]
    assert rows_a != rows_b, "поставщик неидемпотентен: снимки различаются"
    base = {r["device_id"] for r in rows_a if r["device_id"] <= cfg.provider.devices}
    assert len(base) == cfg.provider.devices
