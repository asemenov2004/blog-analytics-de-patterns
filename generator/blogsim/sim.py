"""Модель трафика: сессии пользователей → «чистые» визиты в порядке event_time.

Начала сессий — пуассоновский поток. Внутри сессии события идут с экспоненциальными
интервалами, не больше max_gap_s (меньше таймаута сессии), так что границы сессий
восстанавливаются однозначно. Время — float-секунды UNIX.
"""
from __future__ import annotations

import heapq
import random
import uuid
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass, field

from .catalog import article_blogs
from .config import Config

USER_AGENTS = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",  # noqa: E501
    "Mozilla/5.0 (X11; Linux x86_64; rv:143.0) Gecko/20100101 Firefox/143.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
    "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPad; CPU OS 18_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
)
DEVICE_BY_UA = ("desktop", "desktop", "desktop", "mobile", "mobile", "tablet")
BOT_AGENTS = (
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
    "python-requests/2.32.3",
)
REFERRERS = (None, None, "https://www.google.com/", "https://duckduckgo.com/", "https://x.com/", "https://news.ycombinator.com/")


@dataclass(slots=True)
class Visit:
    event_id: str
    event_time: float
    user_id: int
    article_id: int
    blog_id: int
    event_type: str
    ip: str
    user_agent: str
    device_type: str
    referrer: str | None
    defects: list[str] = field(default_factory=list)


@dataclass(slots=True)
class _Session:
    user_id: int
    left: int
    article_id: int
    bot: bool
    ip: str
    ua: str
    device: str
    referrer: str | None


def user_ip(seed: int, user_id: int) -> str:
    r = random.Random(seed * 7_919 + user_id)
    return f"{r.randint(11, 223)}.{r.randint(0, 255)}.{r.randint(0, 255)}.{r.randint(1, 254)}"


class TrafficModel:
    def __init__(self, cfg: Config, start: float, rng: random.Random | None = None):
        self.cfg = cfg
        self.start = start
        self.rng = rng or random.Random(cfg.seed)
        self.blog_of = article_blogs(cfg)
        self.by_blog: dict[int, list[int]] = defaultdict(list)
        for aid, bid in enumerate(self.blog_of, start=1):
            self.by_blog[bid].append(aid)
        self.pauses = [(start + p.at_min * 60, start + (p.at_min + p.duration_min) * 60)
                       for p in cfg.defects.pauses]

    def in_pause(self, t: float) -> float | None:
        """Конец паузы, если t в неё попадает."""
        for a, b in self.pauses:
            if a <= t < b:
                return b
        return None

    # -- выбор сущностей
    def _pick(self, n: int, hot: int, share: float) -> int:
        if hot and self.rng.random() < share:
            return self.rng.randint(1, min(hot, n))
        return self.rng.randint(1, n)

    def _next_article(self, current: int) -> int:
        tr = self.cfg.traffic
        if self.rng.random() < tr.same_blog_share:
            return self.rng.choice(self.by_blog[self.blog_of[current - 1]])
        d = self.cfg.defects
        return self._pick(self.cfg.catalog.articles, d.hot_articles, d.hot_articles_share)

    def _new_session(self) -> _Session:
        cat, tr, d, rng = self.cfg.catalog, self.cfg.traffic, self.cfg.defects, self.rng
        user = self._pick(cat.users, d.hot_users, d.hot_users_share)
        bot = rng.random() < d.bots_pct / 100
        if bot:
            pageviews = rng.randint(20, 60)
            ua, device = rng.choice(BOT_AGENTS), "bot"
        else:
            pageviews = 1 + int(rng.expovariate(1 / max(tr.mean_pageviews - 1, 1e-9)))
            k = (user * 31 + self.cfg.seed) % len(USER_AGENTS)
            ua, device = USER_AGENTS[k], DEVICE_BY_UA[k]
        return _Session(user_id=user, left=pageviews,
                        article_id=self._pick(cat.articles, d.hot_articles, d.hot_articles_share),
                        bot=bot, ip=user_ip(self.cfg.seed, user), ua=ua, device=device,
                        referrer=rng.choice(REFERRERS))

    def _gap(self, s: _Session) -> float:
        if s.bot:
            return self.rng.uniform(0.5, 5)
        tr = self.cfg.traffic
        return min(self.rng.expovariate(1 / tr.mean_gap_s), tr.max_gap_s)

    def _visit(self, s: _Session, t: float, first: bool) -> Visit:
        rng, d = self.rng, self.cfg.defects
        if not first:
            s.article_id = self._next_article(s.article_id)
        v = Visit(
            event_id=str(uuid.UUID(int=rng.getrandbits(128), version=4)),
            event_time=t,
            user_id=s.user_id,
            article_id=s.article_id,
            blog_id=self.blog_of[s.article_id - 1],
            event_type="click" if not first and rng.random() < self.cfg.traffic.click_share else "page_view",
            ip=s.ip, user_agent=s.ua, device_type=s.device,
            referrer=s.referrer if first else f"/articles/{s.article_id}",
        )
        if s.bot:
            v.defects.append("bot")
        if rng.random() < d.orphan_refs_pct / 100:
            v.article_id = self.cfg.catalog.articles + rng.randint(1, 1000)
            v.defects.append("orphan_article")
        return v

    def visits(self, end: float | None = None) -> Iterator[Visit]:
        """Визиты в порядке event_time на [start, end). end=None — бесконечно."""
        rate = self.cfg.traffic.sessions_per_minute / 60
        next_start = self.start + self.rng.expovariate(rate)
        heap: list[tuple[float, int, _Session, bool]] = []
        n = 0
        while True:
            if heap and heap[0][0] <= next_start:
                t, _, s, first = heapq.heappop(heap)
            else:
                t, s, first = next_start, self._new_session(), True
                next_start += self.rng.expovariate(rate)
            if end is not None and t >= end:
                return
            if self.in_pause(t) is None:
                yield self._visit(s, t, first)
            s.left -= 1
            if s.left > 0:
                n += 1
                heapq.heappush(heap, (t + self._gap(s), n, s, False))
