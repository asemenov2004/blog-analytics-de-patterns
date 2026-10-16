"""Transformation Replicator (гл. 2, с. 47): users из «прода» (Postgres) → staging без PII.

Все колонки читаются строками — никаких скрытых преобразований дат и чисел (совет книги).
Правила для PII собраны в одном месте (PII_RULES) — это мини-контракт данных: новое
чувствительное поле добавляется сюда, а не ищется по коду.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import functions as F  # noqa: E402

from common import STAGING, emit, jdbc_options, spark_session  # noqa: E402

PII_RULES = {
    "birth_date": "drop",
    "email": "mask_email",   # a***@domain
    "first_name": "initial",  # A.
    "last_name": "initial",
}
MASKS = {
    "mask_email": lambda c: F.concat(F.substring(c, 1, 1), F.lit("***@"), F.substring_index(c, "@", -1)),
    "initial": lambda c: F.concat(F.substring(c, 1, 1), F.lit(".")),
}

spark = spark_session("replicate_users_pii")
src = (spark.read.format("jdbc").options(**jdbc_options())
       .option("query", "SELECT user_id::text, email, first_name, last_name, birth_date::text, country::text, "
                        "registered_at::text, updated_at::text FROM users")
       .load()
       .cache())  # один снимок источника и для записи, и для сверки
src.count()
out = src
for col, rule in PII_RULES.items():
    out = out.drop(col) if rule == "drop" else out.withColumn(col, MASKS[rule](F.col(col)))
target = f"{STAGING}/users"
out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target)

# проверка: ни одно исходное значение PII не доехало; остальные поля совпадают
rep = spark.read.format("delta").load(target)
j = src.alias("s").join(rep.alias("r"), "user_id")
leaks = {c: j.where(F.col(f"s.{c}") == F.col(f"r.{c}")).count()
         for c, r in PII_RULES.items() if r != "drop"}
plain = [c for c in src.columns if c not in PII_RULES and c != "user_id"]
mismatch = j.where(" OR ".join(f"NOT (s.{c} <=> r.{c})" for c in plain)).count()
emit({"pattern": "transformation_replicator", "rows_source": src.count(), "rows_replica": rep.count(),
      "dropped_present": [c for c, r in PII_RULES.items() if r == "drop" and c in rep.columns],
      "pii_leaks": leaks, "non_pii_mismatches": mismatch, "replica_columns": rep.columns})
