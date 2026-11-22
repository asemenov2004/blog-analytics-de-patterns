"""Конечная поставка одного топика: streaming checkpoint и пакетная сверка по offsets."""
import argparse
import json
import sys
import time
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import KAFKA, LAKE, spark_session  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("topic")
args = p.parse_args()
spark = spark_session("lab02_errors")
base = f"{LAKE}/lab02/{args.topic}"
source = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA)
          .option("subscribe", args.topic).option("startingOffsets", "earliest").load())


def ingest():
    (source.selectExpr("offset", "CAST(value AS STRING) AS raw").writeStream.format("delta")
     .option("checkpointLocation", base + "/checkpoint").trigger(availableNow=True)
     .start(base + "/raw").awaitTermination())


start = time.perf_counter()
ingest()
raw = spark.read.format("delta").load(base + "/raw")
n = raw.count()
ingested = time.perf_counter()
ingest()
if raw.select("offset").distinct().count() != n or spark.read.format("delta").load(base + "/raw").count() != n:
    raise RuntimeError("Повторное чтение нарушило контракт checkpoint")
processed = time.perf_counter()
parsed = (raw.withColumn("e", F.from_json("raw", "event_id LONG, ts LONG, article_id LONG, bot BOOLEAN, "
                                          "_corrupt_record STRING")).select("*", "e.*"))
# Для эксперимента один раздел Kafka задаёт однозначный порядок доставки.
previous = Window.orderBy("offset").rowsBetween(Window.unboundedPreceding, -1)
bad = F.col("_corrupt_record").isNotNull()
missing = F.expr("event_id IS NULL OR ts IS NULL OR article_id IS NULL OR bot IS NULL")
valid = ~bad & ~missing & ~F.col("bot")
seq = Window.partitionBy("event_id").orderBy("offset")
classified = (parsed.withColumn("frontier", F.max(F.when(valid, F.col("ts"))).over(previous) - 600)
              .withColumn("previous_id", F.max(F.when(valid, F.col("ts"))).over(
                  seq.rowsBetween(Window.unboundedPreceding, -1)))
              .withColumn("kind", F.when(bad, "json").when(missing, "schema").when(F.col("bot"), "bot")
                          .when(F.col("ts") < F.col("frontier"), "late")
                          .when(F.col("previous_id") >= F.coalesce("frontier", F.lit(-1)), "duplicate")
                          .otherwise("ok")).withColumn("hour", F.floor(F.col("ts") / 3600)).cache())
counts = {r.kind: r.n for r in classified.groupBy("kind").agg(F.count("*").alias("n")).collect()}
dead = classified.where("kind IN ('json','schema')").select("offset", "raw", "kind")
dead.write.format("delta").mode("overwrite").save(base + "/dlq")
(dead.selectExpr("CAST(offset AS STRING) AS key", "to_json(struct(*)) AS value").write.format("kafka")
 .option("kafka.bootstrap.servers", KAFKA).option("topic", args.topic + "_dlq").save())
clean = classified.where("kind IN ('ok','late')").dropDuplicates(["event_id"])
online = classified.where("kind='ok'").groupBy("hour").count()
online.write.format("delta").mode("overwrite").save(base + "/online")
dirty = classified.where("kind='late'").select("hour").distinct()
static = clean.where(f"hour BETWEEN 22 AND {24 + (n-1)*2//3600}").groupBy("hour").count()
dynamic = online.join(dirty, "hour", "left_anti").unionByName(
    clean.join(dirty, "hour").groupBy("hour").count())
views = {}
for name, data in (("static", static), ("dynamic", dynamic)):
    data.write.format("delta").mode("overwrite").save(base + "/" + name)
    views[name] = {str(r.hour): r["count"] for r in spark.read.format("delta").load(base + "/" + name).collect()}
print("RESULT " + json.dumps({"events": n, "outcomes": counts, "views": views, "topic": args.topic,
                             "ingest_s": ingested-start, "checkpoint_retry_s": processed-ingested,
                             "processing_s": time.perf_counter()-processed}), flush=True)
spark.stop()
