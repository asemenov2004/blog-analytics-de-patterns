"""Compactor (гл. 2, с. 50): OPTIMIZE сливает мелкие файлы в крупные, VACUUM удаляет старые.

Меряем до и после: файлов в таблице (по журналу Delta) и физически в хранилище, время
листинга и чтения; проверяем, что данные не изменились (count + сумма хешей строк).
VACUUM с нулевым порогом хранения допустим только в демо: в проде он ломает time travel
и может удалить файлы незавершённой записи (книга, «Очистка»).
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from delta.tables import DeltaTable  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common import LAKE, emit, list_files, spark_session, timer  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--table", default=f"{LAKE}/bronze/visits_stream")
p.add_argument("--vacuum-hours", type=float, default=0.0)
args = p.parse_args()

spark = spark_session("compact")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
dt = DeltaTable.forPath(spark, args.table)


def measure(stage: str) -> dict:
    m: dict = {}
    detail = dt.detail().first()
    m["table_files"] = detail["numFiles"]
    m["table_mb"] = round(detail["sizeInBytes"] / 2**20, 2)
    with timer(m, "list_s"):
        files = list_files(spark, args.table)
    m["storage_files"] = len(files)
    df = spark.read.format("delta").load(args.table)
    runs = []
    for _ in range(3):
        t = time.perf_counter()
        df.count()
        runs.append(time.perf_counter() - t)
    m["count_s_median"] = round(sorted(runs)[1], 3)
    fp = df.agg(F.count("*").alias("n"), F.sum(F.xxhash64("value")).alias("h")).first()
    m["rows"], m["fingerprint"] = fp["n"], str(fp["h"])
    return m


before = measure("before")
with timer(before, "optimize_s"):
    stats = dt.optimize().executeCompaction().select("metrics.numFilesAdded", "metrics.numFilesRemoved").first()
after_optimize = measure("after_optimize")
dt.vacuum(args.vacuum_hours)
after_vacuum = measure("after_vacuum")

unchanged = (before["fingerprint"], before["rows"]) == (after_vacuum["fingerprint"], after_vacuum["rows"])
emit({"pattern": "compactor", "files_added": stats[0], "files_removed": stats[1],
      "before": before, "after_optimize": after_optimize, "after_vacuum": after_vacuum, "data_unchanged": unchanged})
