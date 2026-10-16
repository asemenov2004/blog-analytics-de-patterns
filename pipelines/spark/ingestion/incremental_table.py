"""Incremental Loader, реализация на дельта-колонке (гл. 2, с. 34).

Из Postgres берём строки, у которых updated_at попадает в ОГРАНИЧЕННОЕ окно
[from, to) — совет книги: при бэкфилле объём не взрывается, окна можно гнать параллельно.
Окно целиком заменяется при перезапуске (replaceWhere по началу окна).
Слабое место (и повод для CDC): физически удалённые строки этим способом не увидеть.
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import functions as F  # noqa: E402

from common import LAKE, emit, jdbc_options, spark_session  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--table", required=True, choices=["users", "articles"])
p.add_argument("--from", dest="date_from", required=True, help="начало окна, ISO 8601")
p.add_argument("--to", dest="date_to", required=True, help="конец окна (не включительно)")
args = p.parse_args()

spark = spark_session(f"incremental_{args.table}")
query = (f"SELECT * FROM {args.table} "
         f"WHERE updated_at >= '{args.date_from}'::timestamptz AND updated_at < '{args.date_to}'::timestamptz")
rows = (spark.read.format("jdbc").options(**jdbc_options()).option("query", query).load()
        .withColumn("_window_start", F.lit(args.date_from).cast("timestamp"))
        .withColumn("_window_end", F.lit(args.date_to).cast("timestamp"))
        .withColumn("_loaded_at", F.current_timestamp()))
out = f"{LAKE}/bronze/{args.table}_incremental"
(rows.write.format("delta").mode("overwrite")
 .option("replaceWhere", f"_window_start = '{args.date_from}'")
 .save(out))
loaded = spark.read.format("delta").load(out).where(F.col("_window_start") == F.lit(args.date_from).cast("timestamp"))
emit({"pattern": "incremental_loader_delta_column", "table": args.table,
      "window": [args.date_from, args.date_to], "rows": loaded.count()})
