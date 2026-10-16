# Spark 3.5.8 + Delta Lake 3.3.3 + S3A (Hadoop 3.3.4) + коннектор Kafka.
# Jar'ы зашиты в образ, чтобы spark-submit не ходил в Maven при каждом запуске.
# Список и SHA-1 — в jars.sha1. Если файл уже лежит в docker/spark/jars/ (скачан вручную),
# берётся он, иначе качается из MAVEN. Контрольная сумма проверяется в обоих случаях.
FROM apache/spark:3.5.8

USER root
ARG MAVEN=https://repo1.maven.org/maven2
COPY jars.sha1 /tmp/jars.sha1
COPY jars/ /tmp/jars/
RUN cd /opt/spark/jars \
 && while read -r sha path; do \
      f=$(basename "$path"); \
      if [ -f "/tmp/jars/$f" ]; then cp "/tmp/jars/$f" .; else wget -q "$MAVEN/$path" || exit 1; fi; \
      echo "$sha  $f" | sha1sum -c - || exit 1; \
    done < /tmp/jars.sha1 \
 && rm -rf /tmp/jars /tmp/jars.sha1 \
 && pip install --no-cache-dir --no-deps delta-spark==3.3.3 \
 && mkdir -p /opt/work && chown spark /opt/work

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY log4j2.properties /opt/spark/conf/log4j2.properties
ENV PATH="/opt/spark/bin:${PATH}"
USER spark
WORKDIR /opt/work
