"""OLTP-нагрузка на Postgres: начальный сидинг и поток изменений (для CDC и инкрементальной загрузки)."""
from __future__ import annotations

import json
import random
import time
from datetime import UTC, datetime

from faker import Faker

from . import catalog
from .config import Config

USER_COLS = ("user_id", "email", "first_name", "last_name", "birth_date", "country", "registered_at")
BLOG_COLS = ("blog_id", "name", "owner_user_id", "created_at")
ARTICLE_COLS = ("article_id", "blog_id", "title", "category", "published_at")

# операция → вес
OPS = {"insert_user": 30, "update_user": 40, "delete_user": 5, "insert_article": 15, "update_article": 10}


def _copy(cur, table: str, cols: tuple[str, ...], rows: list[dict]) -> None:
    with cur.copy(f"COPY {table} ({', '.join(cols)}) FROM STDIN") as cp:
        for r in rows:
            cp.write_row(tuple(r[c] for c in cols))


def seed(conn, cfg: Config) -> dict:
    """Заливает справочники, если таблицы пустые. Повторный вызов ничего не делает."""
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM users")
        if cur.fetchone()[0]:
            return {"seeded": False, "reason": "users не пустая"}
        users, blogs, articles = catalog.users(cfg), catalog.blogs(cfg), catalog.articles(cfg)
        _copy(cur, "users", USER_COLS, users)
        _copy(cur, "blogs", BLOG_COLS, blogs)
        _copy(cur, "articles", ARTICLE_COLS, articles)
    conn.commit()
    return {"seeded": True, "users": len(users), "blogs": len(blogs), "articles": len(articles)}


class Mutator:
    def __init__(self, conn, cfg: Config, rng: random.Random):
        self.conn, self.cfg, self.rng = conn, cfg, rng
        self.fake = Faker("en_US")
        self.fake.seed_instance(rng.randint(0, 2**31))
        self.next_user = self._max("users", "user_id") + 1
        self.next_article = self._max("articles", "article_id") + 1

    def _max(self, table: str, col: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT coalesce(max({col}), 0) FROM {table}")
            return cur.fetchone()[0]

    def _random_id(self, table: str, col: str) -> int | None:
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT {col} FROM {table} WHERE {col} >= %s ORDER BY {col} LIMIT 1",
                        (self.rng.randint(1, max(self._max(table, col), 1)),))
            row = cur.fetchone()
            return row[0] if row else None

    def apply(self, op: str) -> dict:
        rng, fake = self.rng, self.fake
        with self.conn.cursor() as cur:
            if op == "insert_user":
                uid, self.next_user = self.next_user, self.next_user + 1
                row = catalog.users(self.cfg, start_id=uid, count=1, seed_shift=uid)[0]
                _copy(cur, "users", USER_COLS, [row])
                change = {"email": row["email"], "country": row["country"]}
                key = uid
            elif op == "update_user":
                key = self._random_id("users", "user_id")
                if rng.random() < 0.5:
                    change = {"email": f"{fake.user_name()}.{key}@{fake.free_email_domain()}".lower()}
                else:
                    change = {"country": fake.country_code()}
                col, val = next(iter(change.items()))
                cur.execute(f"UPDATE users SET {col} = %s WHERE user_id = %s", (val, key))
            elif op == "delete_user":
                key = self._random_id("users", "user_id")
                cur.execute("DELETE FROM users WHERE user_id = %s", (key,))
                change = {}
            elif op == "insert_article":
                key, self.next_article = self.next_article, self.next_article + 1
                change = {"blog_id": rng.randint(1, self.cfg.catalog.blogs),
                          "title": fake.sentence(nb_words=6).rstrip("."),
                          "category": rng.choice(catalog.CATEGORIES)}
                cur.execute("INSERT INTO articles (article_id, blog_id, title, category, published_at) "
                            "VALUES (%s, %s, %s, %s, now())",
                            (key, change["blog_id"], change["title"], change["category"]))
            elif op == "update_article":
                key = self._random_id("articles", "article_id")
                change = {"title": fake.sentence(nb_words=6).rstrip(".")}
                cur.execute("UPDATE articles SET title = %s WHERE article_id = %s", (change["title"], key))
            else:
                raise ValueError(op)
        self.conn.commit()
        return {"ts": datetime.now(UTC).isoformat(timespec="milliseconds"), "op": op,
                "id": key, "change": change}

    def run(self, ops: int, per_second: float, log) -> dict:
        names, weights = list(OPS), list(OPS.values())
        counts: dict[str, int] = {}
        for _ in range(ops):
            op = self.rng.choices(names, weights)[0]
            entry = self.apply(op)
            log.append(json.dumps(entry, default=str, separators=(",", ":")).encode())
            counts[op] = counts.get(op, 0) + 1
            if per_second > 0:
                time.sleep(1 / per_second)
        return counts
