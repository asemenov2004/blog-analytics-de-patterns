"""Full Loader (гл. 2, с. 30): снимок справочника устройств целиком → Delta.

У поставщика нет колонки «изменено», поэтому каждый раз грузим всё. Согласованность
для читателей и откат обеспечивает Delta: перезапись — одна транзакция, предыдущая
версия доступна через time travel (пример 2.2 книги).
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from delta.tables import DeltaTable  # noqa: E402

from common import LAKE, emit, spark_session  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--input", required=True, help="s3a://landing/providers/devices/.../devices.jsonl")
p.add_argument("--output", default=f"{LAKE}/bronze/devices")
args = p.parse_args()

spark = spark_session("full_load_devices")
schema = "device_id BIGINT, type STRING, full_name STRING, version STRING"
devices = spark.read.schema(schema).json(args.input)
devices.write.format("delta").mode("overwrite").save(args.output)

last = DeltaTable.forPath(spark, args.output).history(1).first()
emit({"pattern": "full_loader", "input": args.input, "rows": spark.read.format("delta").load(args.output).count(),
      "delta_version": last["version"], "operation": last["operation"]})
