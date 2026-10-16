"""Passthrough Replicator (гл. 2, с. 43), Kafka: visits → visits_staging.

Реплика должна сохранить не только key/value, но и заголовки, номер партиции и порядок
внутри партиции (пример 2.15 книги: sortWithinPartitions('offset') перед записью).
Сверка: для каждой партиции последовательности (key, value, headers) совпадают 1:1.
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common import KAFKA, emit, spark_session  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--source", default="visits")
p.add_argument("--target", default="visits_staging")
args = p.parse_args()

spark = spark_session("replicate_kafka")


def read(topic: str):
    return (spark.read.format("kafka").option("kafka.bootstrap.servers", KAFKA)
            .option("subscribe", topic).option("startingOffsets", "earliest")
            .option("includeHeaders", "true").load())


src = read(args.source).select("key", "value", "headers", "partition", "offset")
# одна партиция Spark = одна партиция Kafka; внутри — строго по offset
(src.repartition("partition").sortWithinPartitions("partition", "offset").drop("offset")
 .write.format("kafka").option("kafka.bootstrap.servers", KAFKA)
 .option("topic", args.target).option("includeHeaders", "true").save())


def ranked(df):
    w = Window.partitionBy("partition").orderBy("offset")
    record = F.concat_ws("|", F.col("key").cast("string"), F.col("value").cast("string"), F.to_json("headers"))
    return df.withColumn("rn", F.row_number().over(w)).select("partition", "rn", F.sha2(record, 256).alias("h"))


a, b = ranked(read(args.source)), ranked(read(args.target))
diff = (a.alias("a").join(b.alias("b"), ["partition", "rn"], "full_outer")
        .where(~F.col("a.h").eqNullSafe(F.col("b.h"))))
per_part = {r["partition"]: r["count"] for r in a.groupBy("partition").count().collect()}
emit({"pattern": "passthrough_replicator_kafka", "messages": sum(per_part.values()), "per_partition": per_part,
      "mismatches": diff.count()})
