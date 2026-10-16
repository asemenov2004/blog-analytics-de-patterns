"""SparkJob — запуск Spark-джобы отдельным контейнером blog-de/spark:local.

Локальный аналог SparkKubernetesOperator из примеров книги: каждая задача — свой
контейнер в сети стенда. Каталог pipelines монтируется с хоста; путь к нему на хосте
узнаём у Docker, посмотрев на монтирования собственного контейнера Airflow.
"""
from __future__ import annotations

import functools
import json
import os
import socket

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

PIPELINES_IN_AIRFLOW = "/opt/airflow/pipelines"
PUBLIC_ENV = ("KAFKA_BOOTSTRAP", "PG_JDBC_URL")
SECRET_ENV = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "PG_USER", "PG_PASSWORD")


@functools.cache
def host_pipelines_dir() -> str:
    import docker

    me = docker.from_env().containers.get(socket.gethostname())
    return next(m["Source"] for m in me.attrs["Mounts"] if m["Destination"] == PIPELINES_IN_AIRFLOW)


class SparkJob(DockerOperator):
    """script — путь относительно pipelines/spark, args — строка аргументов (шаблонизируется)."""

    def __init__(self, *, script: str, args: str = "", **kwargs):
        super().__init__(
            image=os.environ.get("SPARK_IMAGE", "blog-de/spark:local"),
            command=f"spark-submit /opt/pipelines/spark/{script} {args}".strip(),
            network_mode=os.environ.get("SPARK_NETWORK", "blog-de_default"),
            environment={k: os.environ.get(k, "") for k in PUBLIC_ENV},
            private_environment={k: os.environ.get(k, "") for k in SECRET_ENV},  # не светятся в UI
            docker_url="unix://var/run/docker.sock",
            auto_remove="success",
            mount_tmp_dir=False,
            mem_limit="3g",
            xcom_all=True,
            **kwargs,
        )

    def execute(self, context):
        self.mounts = [Mount(target="/opt/pipelines", source=host_pipelines_dir(), type="bind", read_only=True)]
        lines = super().execute(context) or []
        # JVM при остановке может напечатать что-то после RESULT — ищем саму строку результата
        lines = lines if isinstance(lines, list) else [lines]
        return next((line for line in reversed(lines) if "RESULT " in str(line)), lines[-1] if lines else None)


def parse_result(line: str | None) -> dict:
    """Последняя строка лога джобы: 'RESULT {...}' → dict."""
    if not line or "RESULT " not in line:
        raise ValueError(f"джоба не вернула RESULT: {line!r}")
    return json.loads(line.split("RESULT ", 1)[1])
