"""Incremental Loader, реализация на партициях (гл. 2, с. 34).

Грузим ровно одну часовую партицию landing/visits/date=/hour= в bronze. Партицию
определяет время выполнения (data interval в Airflow), бэкфилл идёт по одной партиции.
Строки копируются как есть: разбор и чистка — дело следующих слоёв.
Перезапуск той же партиции заменяет её (replaceWhere), а не дописывает дубли.
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import functions as F  # noqa: E402

from common import LAKE, LANDING, emit, spark_session  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--date", required=True)
p.add_argument("--hour", required=True)
p.add_argument("--output", default=f"{LAKE}/bronze/visits_raw")
args = p.parse_args()
hour = f"{int(args.hour):02d}"

spark = spark_session("incremental_visits")
src = f"{LANDING}/visits/date={args.date}/hour={hour}/"
rows = (spark.read.text(src)
        .withColumn("source_file", F.input_file_name())
        .withColumn("loaded_at", F.current_timestamp())
        .withColumn("date", F.lit(args.date))
        .withColumn("hour", F.lit(hour)))
(rows.write.format("delta").mode("overwrite")
 .option("replaceWhere", f"date = '{args.date}' AND hour = '{hour}'")
 .partitionBy("date", "hour")
 .save(args.output))

loaded = spark.read.format("delta").load(args.output).where(f"date = '{args.date}' AND hour = '{hour}'").count()
emit({"pattern": "incremental_loader", "partition": f"date={args.date}/hour={hour}", "rows": loaded})
