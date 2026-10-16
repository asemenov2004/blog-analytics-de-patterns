"""Change Data Capture (гл. 2, с. 39): события Debezium из топиков blog.public.* → bronze.

Debezium читает журнал WAL Postgres (logical replication, pgoutput) и пишет каждое
изменение строки в топик. Здесь мы сохраняем конверт как есть: тип операции (c/u/d),
время в источнике, образы строки до и после — потребители сами решают, хранить ли
историю или только последнее состояние.
trigger(availableNow) — вычитать всё накопленное и остановиться (удобно для демо и Airflow).
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import functions as F  # noqa: E402

from common import KAFKA, LAKE, emit, spark_session  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--topics", default="blog\\.public\\..*", help="regex топиков Debezium")
p.add_argument("--output", default=f"{LAKE}/bronze/cdc")
args = p.parse_args()

spark = spark_session("cdc_to_bronze")
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA)
       .option("subscribePattern", args.topics)
       .option("startingOffsets", "earliest")
       .load())
v = F.col("value").cast("string")
events = raw.select(
    F.regexp_extract("topic", r"\.([^.]+)$", 1).alias("table"),
    F.get_json_object(v, "$.op").alias("op"),
    # время коммита в Postgres и время обработки коннектором — из них считаем задержку CDC
    (F.get_json_object(v, "$.source.ts_ms").cast("long") / 1000).cast("timestamp").alias("commit_ts"),
    (F.get_json_object(v, "$.ts_ms").cast("long") / 1000).cast("timestamp").alias("captured_ts"),
    F.get_json_object(v, "$.source.lsn").cast("long").alias("source_lsn"),
    F.get_json_object(v, "$.before").alias("before"),
    F.get_json_object(v, "$.after").alias("after"),
    F.col("key").cast("string").alias("key"),
    "topic", "partition", "offset",
    F.col("timestamp").alias("kafka_ts"),
)
query = (events.writeStream.format("delta")
         .option("checkpointLocation", f"{LAKE}/_checkpoints/bronze_cdc")
         .partitionBy("table")
         .trigger(availableNow=True)
         .start(args.output))
query.awaitTermination()

df = spark.read.format("delta").load(args.output)
by_op = {f"{r['table']}.{r['op']}": r["count"] for r in df.groupBy("table", "op").count().collect()}
emit({"pattern": "cdc", "rows": df.count(), "by_table_op": by_op})
