"""Общее для Spark-джоб: сессия, адреса, вывод результата.

Джоба печатает последней строкой `RESULT {json}`. DockerOperator кладёт последнюю
строку лога в XCom, demo-скрипты вытаскивают её grep'ом.
"""
from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager

from pyspark.sql import SparkSession

LAKE = "s3a://lake"
LANDING = "s3a://landing"
STAGING = "s3a://staging"
KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")


def spark_session(app: str) -> SparkSession:
    spark = SparkSession.builder.appName(app).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def jdbc_options() -> dict:
    return {
        "url": os.environ.get("PG_JDBC_URL", "jdbc:postgresql://postgres:5432/blog"),
        "user": os.environ.get("PG_USER", "blog"),
        "password": os.environ.get("PG_PASSWORD", "blog"),
        "driver": "org.postgresql.Driver",
    }


def emit(result: dict) -> None:
    print("RESULT " + json.dumps(result, ensure_ascii=False, default=str), flush=True)


@contextmanager
def timer(store: dict, key: str):
    t = time.perf_counter()
    yield
    store[key] = round(time.perf_counter() - t, 3)


def list_files(spark: SparkSession, path: str) -> list[tuple[str, int]]:
    """Рекурсивный листинг через Hadoop FS: (путь, размер). Служебные _delta_log не считаем."""
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(path)
    fs = p.getFileSystem(conf)
    out = []
    it = fs.listFiles(p, True)
    while it.hasNext():
        st = it.next()
        s = st.getPath().toString()
        if "/_delta_log/" not in s:
            out.append((s, st.getLen()))
    return out
