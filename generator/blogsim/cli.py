"""CLI генератора.

  blogsim bootstrap                         бакеты S3, топики, сидинг Postgres (идемпотентно)
  blogsim batch --start ... --hours 24      JSONL-файлы визитов по часам + _SUCCESS
  blogsim stream --minutes 10 --speed 1     визиты в Kafka в реальном времени
  blogsim oltp --ops 500 --per-second 5     поток изменений в Postgres

Журнал дефектов (ground truth) — в бакет truth (или --out dir:<путь> для локального прогона).
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
from datetime import UTC, datetime

from .config import Config, Connections, load_config
from .delivery import Delivery
from .sim import TrafficModel
from .sinks import DirStorage, KafkaSink, PartitionedSink, S3Storage, TruthLog


def s3_client(conn: Connections):
    import boto3
    from botocore.config import Config as BotoConfig

    return boto3.client("s3", endpoint_url=conn.s3_endpoint, aws_access_key_id=conn.s3_access_key,
                        aws_secret_access_key=conn.s3_secret_key, region_name="us-east-1",
                        config=BotoConfig(s3={"addressing_style": "path"}))


def storages(out: str, cfg: Config, conn: Connections, run_id: str):
    """(хранилище данных, хранилище truth) для --out s3 | dir:<путь>."""
    if out == "s3":
        client = s3_client(conn)
        return S3Storage(client, cfg.storage.landing_bucket), S3Storage(client, cfg.storage.truth_bucket)
    if out.startswith("dir:"):
        root = out[4:]
        return DirStorage(f"{root}/landing"), DirStorage(f"{root}/truth")
    raise SystemExit(f"--out: ожидается s3 или dir:<путь>, получено {out!r}")


def parse_ts(s: str) -> float:
    dt = datetime.fromisoformat(s)
    return (dt if dt.tzinfo else dt.replace(tzinfo=UTC)).timestamp()


def default_run_id(mode: str) -> str:
    return f"{mode}-{datetime.now(UTC):%Y%m%dT%H%M%SZ}"


# ------------------------------------------------------------------ команды
def cmd_bootstrap(cfg: Config, conn: Connections, args) -> dict:
    result: dict = {}
    if not args.skip_s3:
        client = s3_client(conn)
        existing = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
        wanted = [cfg.storage.landing_bucket, cfg.storage.truth_bucket, cfg.storage.lake_bucket]
        for b in wanted:
            if b not in existing:
                client.create_bucket(Bucket=b)
        result["buckets"] = wanted
    if not args.skip_kafka:
        from confluent_kafka.admin import AdminClient, NewTopic

        admin = AdminClient({"bootstrap.servers": conn.kafka_bootstrap})
        topic = cfg.storage.visits_topic
        if topic not in admin.list_topics(timeout=15).topics:
            fut = admin.create_topics([NewTopic(topic, cfg.storage.visits_partitions, 1)])[topic]
            fut.result(timeout=30)
        result["topics"] = [topic]
    if not args.skip_pg:
        import psycopg

        from .oltp import seed

        with psycopg.connect(conn.pg_dsn) as pg:
            result["postgres"] = seed(pg, cfg)
    return result


def run_batch(cfg: Config, start: float, hours: float, data, truth_storage, run_id: str) -> dict:
    end = start + hours * 3600
    model = TrafficModel(cfg, start, random.Random(cfg.seed))
    delivery = Delivery(cfg, model, random.Random(cfg.seed + 1))
    truth = TruthLog(truth_storage, run_id, {"mode": "batch", "start": start, "hours": hours, "seed": cfg.seed})
    sink = PartitionedSink(data, "visits", cfg.defects.missing_success_pct, random.Random(cfg.seed + 2), truth)
    for rec in delivery.records(end, drain=True):
        sink.write(rec)
        truth.add(rec)
    sink.close()
    summary = truth.close()
    summary["partitions"] = len(sink.partitions)
    return summary


def cmd_batch(cfg: Config, conn: Connections, args) -> dict:
    run_id = args.run_id or default_run_id("batch")
    data, truth = storages(args.out, cfg, conn, run_id)
    summary = run_batch(cfg, parse_ts(args.start), args.hours, data, truth, run_id)
    summary["data"], summary["truth"] = str(data), str(truth)
    return summary


def cmd_stream(cfg: Config, conn: Connections, args) -> dict:
    from confluent_kafka import Producer

    run_id = args.run_id or default_run_id("stream")
    _, truth_storage = storages(args.out, cfg, conn, run_id)
    start = time.time()
    end = start + args.minutes * 60 * args.speed  # в «симулированных» секундах
    model = TrafficModel(cfg, start, random.Random(cfg.seed))
    delivery = Delivery(cfg, model, random.Random(cfg.seed + 1))
    truth = TruthLog(truth_storage, run_id, {"mode": "stream", "start": start, "speed": args.speed,
                                             "minutes": args.minutes, "seed": cfg.seed,
                                             "topic": cfg.storage.visits_topic})
    sink = KafkaSink(Producer({"bootstrap.servers": conn.kafka_bootstrap, "linger.ms": 50}),
                     cfg.storage.visits_topic)
    try:
        for rec in delivery.records(end, drain=False):
            wait = start + (rec.arrival - start) / args.speed - time.time()
            if wait > 0:
                time.sleep(wait)
            sink.write(rec)
            truth.add(rec)
    except KeyboardInterrupt:
        print("остановлено, сохраняю журнал", file=sys.stderr)
    finally:
        sink.close()
    return truth.close()


def cmd_oltp(cfg: Config, conn: Connections, args) -> dict:
    import psycopg

    from .oltp import Mutator

    run_id = args.run_id or default_run_id("oltp")
    _, truth_storage = storages(args.out, cfg, conn, run_id)
    log: list[bytes] = []
    with psycopg.connect(conn.pg_dsn) as pg:
        counts = Mutator(pg, cfg, random.Random(args.seed if args.seed is not None else time.time_ns())) \
            .run(args.ops, args.per_second, log)
    truth_storage.put(f"runs/{run_id}/oltp_ops.jsonl", b"\n".join(log) + b"\n")
    summary = {"run_id": run_id, "mode": "oltp", "ops": counts}
    truth_storage.put(f"runs/{run_id}/summary.json", json.dumps(summary, ensure_ascii=False, indent=2).encode())
    return summary


# ------------------------------------------------------------------ разбор аргументов
def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", action="append", default=[], help="YAML поверх defaults (можно несколько)")
    common.add_argument("--set", action="append", default=[], metavar="KEY=VALUE",
                        help="точечная правка, напр. defects.duplicates_pct=5")
    common.add_argument("--run-id", help="идентификатор прогона (по умолчанию <режим>-<время>)")
    common.add_argument("--out", default="s3", help="s3 (по умолчанию) или dir:<путь>")

    p = argparse.ArgumentParser(prog="blogsim", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("bootstrap", parents=[common], help="бакеты, топики, сидинг Postgres")
    b.add_argument("--skip-s3", action="store_true")
    b.add_argument("--skip-kafka", action="store_true")
    b.add_argument("--skip-pg", action="store_true")
    b.set_defaults(func=cmd_bootstrap)

    bt = sub.add_parser("batch", parents=[common], help="файлы визитов за период")
    bt.add_argument("--start", required=True, help="начало, ISO 8601 (UTC по умолчанию)")
    bt.add_argument("--hours", type=float, default=24)
    bt.set_defaults(func=cmd_batch)

    st = sub.add_parser("stream", parents=[common], help="визиты в Kafka в реальном времени")
    st.add_argument("--minutes", type=float, default=10, help="длительность по часам стены")
    st.add_argument("--speed", type=float, default=1.0, help="ускорение модельного времени")
    st.set_defaults(func=cmd_stream)

    ol = sub.add_parser("oltp", parents=[common], help="поток изменений в Postgres")
    ol.add_argument("--ops", type=int, default=500)
    ol.add_argument("--per-second", type=float, default=5)
    ol.add_argument("--seed", type=int, help="seed изменений (по умолчанию случайный)")
    ol.set_defaults(func=cmd_oltp)
    return p


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    cfg = load_config(args.config, args.set)
    result = args.func(cfg, Connections(), args)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
