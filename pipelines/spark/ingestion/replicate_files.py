"""Passthrough Replicator (гл. 2, с. 43), файлы: landing → staging без изменений.

Копируем строки текстовым API, а не JSON-ридером: никакого разбора, никаких скрытых
преобразований типов (совет книги). Сравнение — по мультимножеству строк каждой партиции.
Число и имена файлов Spark не сохраняет — ограничение, которое книга называет явно.
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import functions as F  # noqa: E402

from common import LANDING, STAGING, emit, spark_session  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--date", required=True)
args = p.parse_args()

spark = spark_session("replicate_files")
src = f"{LANDING}/visits/date={args.date}"
dst = f"{STAGING}/landing/visits/date={args.date}"
spark.read.text(src).write.mode("overwrite").partitionBy("hour").text(dst)


def fingerprint(path: str) -> dict:
    rows = (spark.read.text(path).groupBy("hour")
            .agg(F.count("*").alias("n"), F.sum(F.xxhash64("value")).alias("h")).collect())
    return {f"{int(r['hour']):02d}": (r["n"], str(r["h"])) for r in rows}


a, b = fingerprint(src), fingerprint(dst)
emit({"pattern": "passthrough_replicator_files", "partitions": len(a),
      "rows": sum(n for n, _ in a.values()), "identical": a == b,
      "mismatched": sorted(k for k in a.keys() | b.keys() if a.get(k) != b.get(k))})
