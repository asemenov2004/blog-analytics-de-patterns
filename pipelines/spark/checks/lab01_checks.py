"""Сверка результатов лабы 1 с ground truth генератора. Печатает RESULT {json}.

spark-submit lab01_checks.py --visits-run lab01-visits --oltp-runs lab01-oltp,lab01-oltp2 --date 2026-10-14
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from delta.tables import DeltaTable  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common import LAKE, LANDING, emit, list_files, spark_session  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--visits-run", required=True)
p.add_argument("--oltp-runs", required=True, help="прогоны OLTP через запятую, по порядку")
p.add_argument("--date", required=True)
args = p.parse_args()
spark = spark_session("lab01_checks")
res: dict = {}

# ---------------------------------------------------------------- Incremental Loader + Readiness Marker
landing = (spark.read.text(f"{LANDING}/visits/date={args.date}")
           .groupBy("hour").count().collect())
landing_rows = {f"{int(r['hour']):02d}": r["count"] for r in landing}
success = {f.split("hour=")[1][:2] for f, _ in list_files(spark, f"{LANDING}/visits/date={args.date}")
           if f.endswith("/_SUCCESS")}
bronze = (spark.read.format("delta").load(f"{LAKE}/bronze/visits_raw").where(F.col("date") == args.date)
          .groupBy("hour").count().collect())
bronze_rows = {r["hour"]: r["count"] for r in bronze}
markers = {f.split("hour=")[1][:2] for f, _ in list_files(spark, f"{LAKE}/_markers/bronze_visits/date={args.date}")
           if f.endswith("/_COMPLETED")}
truth = json.loads(spark.sparkContext.wholeTextFiles(f"s3a://truth/runs/{args.visits_run}/summary.json").first()[1])
missing_truth = sorted(d["partition"].split("hour=")[1] for d in truth["partition_defects"]
                       if d["defect"] == "missing_success" and f"date={args.date}" in d["partition"])
res["incremental"] = {
    "landing_partitions": len(landing_rows),
    "loaded_partitions": sorted(bronze_rows),
    "rows_match_per_partition": {h: bronze_rows[h] == n for h, n in sorted(landing_rows.items())
                                 if h in bronze_rows},
    "rows_loaded": sum(bronze_rows.values()),
    "rows_landing_ready": sum(n for h, n in landing_rows.items() if h in success),
}
res["readiness"] = {
    "without_success_in_storage": sorted(set(landing_rows) - success),
    "without_success_in_truth": missing_truth,
    "not_loaded": sorted(set(landing_rows) - set(bronze_rows)),
    "completed_markers": sorted(markers),
}

# ---------------------------------------------------------------- Incremental (дельта-колонка) vs CDC: удаления
runs = args.oltp_runs.split(",")
ops = spark.read.json([f"s3a://truth/runs/{r}/oltp_ops.jsonl" for r in runs])
deleted = [r["id"] for r in ops.where("op = 'delete_user'").select("id").collect()]
# удаления последнего прогона: эти пользователи точно были загружены в bronze до удаления
last_ops = spark.read.json(f"s3a://truth/runs/{runs[-1]}/oltp_ops.jsonl")
deleted_last = [r["id"] for r in last_ops.where("op = 'delete_user'").select("id").collect()]
users_inc = spark.read.format("delta").load(f"{LAKE}/bronze/users_incremental")
res["users_incremental"] = {
    "rows": users_inc.count(),
    "windows": [[str(r["_window_start"]), str(r["_window_end"]), r["count"]] for r in
                users_inc.groupBy("_window_start", "_window_end").count().orderBy("_window_start").collect()],
    "deleted_in_source": len(deleted_last),
    "deleted_still_in_bronze": (users_inc.where(F.col("user_id").isin(deleted_last))
                                .select("user_id").distinct().count()),
}

expected = {r["op"]: r["count"] for r in ops.groupBy("op").count().collect()}
cdc = spark.read.format("delta").load(f"{LAKE}/bronze/cdc")
got = {f"{r['table']}.{r['op']}": r["count"] for r in cdc.groupBy("table", "op").count().collect()}
mapping = {"insert_user": "users.c", "update_user": "users.u", "delete_user": "users.d",
           "insert_article": "articles.c", "update_article": "articles.u"}
lat = cdc.select(((F.col("kafka_ts").cast("double") - F.col("commit_ts").cast("double"))).alias("s"))
q = lat.approxQuantile("s", [0.5, 0.95, 1.0], 0.001)
deleted_caught = cdc.where("table = 'users' AND op = 'd'").select(
    F.get_json_object("before", "$.user_id").cast("long").alias("id")).where(F.col("id").isin(deleted)).count()
res["cdc"] = {
    "per_op": {k: {"oltp": expected.get(k, 0), "cdc": got.get(v, 0)} for k, v in mapping.items()},
    "all_match": all(expected.get(k, 0) == got.get(v, 0) for k, v in mapping.items()),
    "deleted_caught": deleted_caught,
    "latency_s": {"p50": round(q[0], 3), "p95": round(q[1], 3), "max": round(q[2], 3)},
}

# ---------------------------------------------------------------- Full Loader: версии и time travel
dev_path = f"{LAKE}/bronze/devices"
hist = DeltaTable.forPath(spark, dev_path).history().select("version", "operation").orderBy("version").collect()
versions = []
for h in hist:
    df = spark.read.format("delta").option("versionAsOf", h["version"]).load(dev_path)
    versions.append({"version": h["version"], "operation": h["operation"], "rows": df.count(),
                     "fingerprint": str(df.agg(F.sum(F.xxhash64("device_id", "version"))).first()[0])})
res["full_loader"] = {"versions": versions}

emit(res)
