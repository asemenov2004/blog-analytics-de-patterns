"""Измеряемый приём визитов из JSONL. Python 3.10+, стандартная библиотека."""
import argparse
import hashlib
import json
import os
import platform
import random
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
from collections import Counter
from contextlib import closing
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS checkpoint(pos INTEGER, watermark_max INTEGER);
INSERT INTO checkpoint SELECT 0,-1 WHERE NOT EXISTS(SELECT 1 FROM checkpoint);
CREATE TABLE IF NOT EXISTS seen(id INTEGER PRIMARY KEY, ts INTEGER);
CREATE INDEX IF NOT EXISTS expires ON seen(ts);
CREATE TABLE IF NOT EXISTS visits(id INTEGER PRIMARY KEY, hour INTEGER, late INTEGER);
CREATE INDEX IF NOT EXISTS partitions ON visits(hour);
CREATE TABLE IF NOT EXISTS dlq(pos INTEGER PRIMARY KEY, raw TEXT, reason TEXT);
CREATE TABLE IF NOT EXISTS audit(outcome TEXT PRIMARY KEY, n INTEGER);
"""
WINDOW = 600


def generate(path, size, seed):
    rng = random.Random(seed)
    counts, hours = Counter(), Counter()
    previous = None
    with path.open("w", encoding="utf8") as out:
        for i in range(size):
            ts = 86400 + i * 2
            row = {"event_id": i, "ts": ts, "article_id": rng.randrange(200), "bot": False}
            kind = rng.choices(["ok", "json", "schema", "bot", "duplicate", "late"],
                               weights=[84, 2, 2, 5, 4, 3])[0]
            if kind == "duplicate" and previous is None:
                kind = "ok"
            if kind == "duplicate":
                row = previous
            elif kind == "late":
                # Смещение относительно последней нормальной записи гарантирует позднюю доставку.
                if previous is None:
                    kind = "ok"
                else:
                    row["ts"] = previous["ts"] - 7200
            elif kind == "bot":
                row["bot"] = True
            elif kind == "schema":
                del row["article_id"]
            if kind == "ok":
                previous = row.copy()
            if kind in ("late", "ok"):
                hours[row["ts"] // 3600] += 1
            counts[kind] += 1
            out.write(("{broken" if kind == "json" else json.dumps(row)) + "\n")
    return counts, hours


def classify(db, raw, offset, maximum):
    try:
        e = json.loads(raw)
    except ValueError:
        reason = "json"
    else:
        reason = "schema"
        if (isinstance(e, dict) and all(type(e.get(k)) is int for k in ("event_id", "ts", "article_id"))
                and type(e.get("bot")) is bool):
            if e["bot"]:
                return "bot", maximum
            key, ts = e["event_id"], e["ts"]
            if ts < maximum - WINDOW:
                db.execute("INSERT OR IGNORE INTO visits VALUES(?,?,1)", (key, ts // 3600))
                return "late", maximum
            if ts > maximum:
                db.execute("DELETE FROM seen WHERE ts < ?", (ts - WINDOW,))
            if db.execute("INSERT OR IGNORE INTO seen VALUES(?,?)", (key, ts)).rowcount == 0:
                return "duplicate", maximum
            db.execute("INSERT INTO visits VALUES(?,?,0)", (key, ts // 3600))
            return "ok", max(ts, maximum)
    db.execute("INSERT INTO dlq VALUES(?,?,?)", (offset, raw, reason))
    return reason, maximum


def consume(path, batch, crash=False):
    with closing(sqlite3.connect(path.with_suffix(".db"))) as db, path.open("rb") as src:
        db.executescript(SCHEMA)
        pos, maximum = db.execute("SELECT * FROM checkpoint").fetchone()
        src.seek(pos)
        while True:
            counts = Counter()
            with db:
                for _ in range(batch):
                    offset = src.tell()
                    line = src.readline()
                    if not line:
                        break
                    kind, maximum = classify(db, line.decode().rstrip(), offset, maximum)
                    counts[kind] += 1
                    if crash and offset >= path.stat().st_size // 2:
                        os._exit(75)  # намеренный обрыв внутри транзакции дочернего процесса
                for key, n in counts.items():
                    db.execute("INSERT INTO audit VALUES(?,?) ON CONFLICT(outcome) DO UPDATE SET n=n+excluded.n",
                               (key, n))
                db.execute("UPDATE checkpoint SET pos=?,watermark_max=?", (src.tell(), maximum))
            if not counts:
                return


def reconcile(db, mode, periods, baseline):
    view = baseline.copy()
    started = time.perf_counter()
    for hour in periods:
        view[hour] = db.execute("SELECT count(*) FROM visits WHERE hour=?", (hour,)).fetchone()[0]
    return {k: v for k, v in view.items() if v}, {
        "strategy": mode, "hours": len(periods), "ms": (time.perf_counter() - started) * 1000}


def experiment(size, seed, batch, recovery):
    with tempfile.TemporaryDirectory() as temp:
        path = Path(temp) / "visits.jsonl"
        truth, hours = generate(path, size, seed)
        started = time.perf_counter()
        if recovery:
            r = subprocess.run([sys.executable, __file__, "--crash-worker", str(path), "--batch", str(batch)])
            if r.returncode != 75:
                raise RuntimeError(f"worker: {r.returncode}")
            with closing(sqlite3.connect(path.with_suffix(".db"))) as db:
                resume = db.execute("SELECT pos FROM checkpoint").fetchone()[0]
        consume(path, batch)
        elapsed = time.perf_counter() - started
        with closing(sqlite3.connect(path.with_suffix(".db"))) as db:
            outcomes = dict(db.execute("SELECT * FROM audit"))
            base = dict(db.execute("SELECT hour,count(*) FROM visits WHERE late=0 GROUP BY hour"))
            touched = [r[0] for r in db.execute("SELECT DISTINCT hour FROM visits WHERE late=1")]
            # Статический горизонт включает всю поставку и два предшествующих часа.
            fixed = list(range(22, 25 + ((size - 1) * 2) // 3600))
            static, sm = reconcile(db, "static", fixed, base)
            dynamic, dm = reconcile(db, "dynamic", touched, base)
            pos = db.execute("SELECT pos FROM checkpoint").fetchone()[0]
            dlq = dict(db.execute("SELECT reason,count(*) FROM dlq GROUP BY reason"))
            if not (outcomes == truth and static == dynamic == hours and pos == path.stat().st_size
                    and all(dlq.get(k, 0) == truth[k] for k in ("json", "schema"))):
                raise RuntimeError("Контрольная сверка не прошла")
            digest = hashlib.sha256()
            for table, order in (("visits", "id"), ("dlq", "pos"), ("audit", "outcome"),
                                 ("checkpoint", "pos"), ("seen", "id")):
                for row in db.execute(f"SELECT * FROM {table} ORDER BY {order}"):
                    digest.update(json.dumps(row).encode())
            example = None
            if touched:
                h = min(touched)
                example = {"hour": h, "before": base.get(h, 0), "after": dynamic[h], "expected": hours[h]}
        return {"events": size, "bytes": pos, "seconds": elapsed, "rows_s": size / elapsed,
                "outcomes": outcomes, "static": sm, "dynamic": dm, "example": example,
                "resume_byte": resume if recovery else None, "digest": digest.hexdigest()}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--sizes", nargs="+", type=int, default=[1000, 10000, 50000])
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--batch", type=int, default=250)
    p.add_argument("--out", type=Path, default=Path(__file__).with_name("results.json"))
    p.add_argument("--crash-worker", type=Path, help=argparse.SUPPRESS)
    args = p.parse_args()
    if min(*args.sizes, args.repeats, args.batch) < 1:
        p.error("Размеры, число повторов и батч должны быть положительными")
    if args.crash_worker:
        consume(args.crash_worker, args.batch, crash=True)
        return
    series = []
    for n in args.sizes:
        runs = [experiment(n, args.seed, args.batch, False) for _ in range(args.repeats)]
        series.append({"runs": runs, "median_s": statistics.median(r["seconds"] for r in runs)})
    recovered = experiment(args.sizes[-1], args.seed, args.batch, True)
    if recovered["digest"] != series[-1]["runs"][0]["digest"]:
        raise RuntimeError("Восстановленное состояние отличается от контрольного")
    result = {"python": sys.version.split()[0], "platform": platform.platform(), "sqlite": sqlite3.sqlite_version,
              "seed": args.seed, "batch": args.batch, "series": series, "recovery": recovered}
    args.out.write_text(json.dumps(result, indent=2), encoding="utf8")
    for group in series:
        print(group["runs"][0]["events"], round(group["median_s"], 4), "s")
    print("Recovery verified:", recovered["resume_byte"], "->", recovered["bytes"])


if __name__ == "__main__":
    main()

