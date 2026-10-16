"""Smoke-проверка стенда: Spark читает landing (S3) и Kafka, пишет и читает Delta в lake.

spark-submit /opt/pipelines/spark/smoke/delta_s3_smoke.py [--topic visits]
Печатает JSON со счётчиками — их сверяем с summary.json генератора.
"""
import argparse
import json
import os

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--landing", default="s3a://landing/visits")
parser.add_argument("--target", default="s3a://lake/_smoke/visits_raw")
parser.add_argument("--topic", default="visits")
args = parser.parse_args()

spark = SparkSession.builder.appName("smoke").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (spark.read.text(args.landing)  # партиции date=/hour= Spark распознаёт сам
       .withColumn("is_json_object", F.get_json_object("value", "$.event_id").isNotNull()))
raw.write.format("delta").mode("overwrite").save(args.target)

delta = spark.read.format("delta").load(args.target)
history = DeltaTable.forPath(spark, args.target).history(1).select("version", "operation").first()

kafka = (spark.read.format("kafka")
         .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092"))
         .option("subscribe", args.topic)
         .option("startingOffsets", "earliest")
         .load())

print("SMOKE_RESULT " + json.dumps({
    "landing_lines": raw.count(),
    "landing_partitions": raw.select("date", "hour").distinct().count(),
    "delta_rows": delta.count(),
    "delta_rows_with_event_id": delta.filter("is_json_object").count(),
    "delta_last_commit": {"version": history["version"], "operation": history["operation"]},
    "kafka_messages": kafka.count(),
}))
spark.stop()
