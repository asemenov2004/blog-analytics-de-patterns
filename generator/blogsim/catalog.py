"""Справочники OLTP: пользователи, блоги, статьи.

Всё детерминировано от seed: генератор событий и сидинг Postgres получают одинаковые id
и одинаковую привязку статей к блогам, не обращаясь к базе.
"""
from __future__ import annotations

import random
from datetime import UTC, date, datetime, timedelta

from faker import Faker

from .config import Config

CATEGORIES = ("data", "backend", "frontend", "devops", "ml", "career", "databases", "streaming")
EPOCH = datetime(2024, 1, 1, tzinfo=UTC)


def article_blogs(cfg: Config) -> list[int]:
    """blog_id для каждой статьи; индекс списка = article_id - 1."""
    rng = random.Random(cfg.seed * 1_000_003 + 1)
    return [rng.randint(1, cfg.catalog.blogs) for _ in range(cfg.catalog.articles)]


def _faker(seed: int) -> Faker:
    fake = Faker("en_US")
    fake.seed_instance(seed)
    return fake


def users(cfg: Config, start_id: int = 1, count: int | None = None, seed_shift: int = 0) -> list[dict]:
    count = cfg.catalog.users if count is None else count
    fake = _faker(cfg.seed + 11 + seed_shift)
    rng = random.Random(cfg.seed + 12 + seed_shift)
    rows = []
    for uid in range(start_id, start_id + count):
        first, last = fake.first_name(), fake.last_name()
        rows.append({
            "user_id": uid,
            "email": f"{first}.{last}.{uid}@{fake.free_email_domain()}".lower(),
            "first_name": first,
            "last_name": last,
            "birth_date": date(1960, 1, 1) + timedelta(days=rng.randint(0, 45 * 365)),
            "country": fake.country_code(),
            "registered_at": EPOCH + timedelta(seconds=rng.randint(0, 600 * 86400)),
        })
    return rows


def blogs(cfg: Config) -> list[dict]:
    fake = _faker(cfg.seed + 21)
    rng = random.Random(cfg.seed + 22)
    return [{
        "blog_id": bid,
        "name": fake.catch_phrase(),
        "owner_user_id": rng.randint(1, cfg.catalog.users),
        "created_at": EPOCH + timedelta(seconds=rng.randint(0, 300 * 86400)),
    } for bid in range(1, cfg.catalog.blogs + 1)]


def articles(cfg: Config) -> list[dict]:
    fake = _faker(cfg.seed + 31)
    rng = random.Random(cfg.seed + 32)
    rows = []
    for aid, bid in enumerate(article_blogs(cfg), start=1):
        rows.append({
            "article_id": aid,
            "blog_id": bid,
            "title": fake.sentence(nb_words=6).rstrip("."),
            "category": rng.choice(CATEGORIES),
            "published_at": EPOCH + timedelta(seconds=rng.randint(0, 640 * 86400)),
        })
    return rows
