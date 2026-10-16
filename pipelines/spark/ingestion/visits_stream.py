"""Синхронизация топика visits с bronze в режиме микробатчей — источник «проблемы маленьких
файлов» для паттерна Compactor (гл. 2, с. 50).

Каждый микробатч дописывает в Delta свои файлы. Маленький maxOffsetsPerTrigger имитирует
частые короткие батчи потокового приёма (данные доступны через минуты, файлов — тысячи).
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import KAFKA, LAKE, emit, list_files, spark_session  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--max-offsets", type=int, default=100, help="maxOffsetsPerTrigger (размер микробатча)")
p.add_argument("--output", default=f"{LAKE}/bronze/visits_stream")
args = p.parse_args()

spark = spark_session("visits_stream")
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA)
       .option("subscribe", "visits")
       .option("startingOffsets", "earliest")
       .option("maxOffsetsPerTrigger", args.max_offsets)
       .load()
       .selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value",
                   "partition", "offset", "timestamp AS kafka_ts"))
query = (raw.writeStream.format("delta")
         .option("checkpointLocation", f"{LAKE}/_checkpoints/bronze_{args.output.rstrip('/').rsplit('/', 1)[-1]}")
         .trigger(availableNow=True)
         .start(args.output))
query.awaitTermination()

files = list_files(spark, args.output)
emit({"pattern": "compactor_source", "rows": spark.read.format("delta").load(args.output).count(),
      "data_files": len(files), "avg_file_kb": round(sum(s for _, s in files) / max(len(files), 1) / 1024, 1)})
