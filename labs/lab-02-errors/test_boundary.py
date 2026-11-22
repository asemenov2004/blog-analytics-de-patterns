"""Проверка границы watermark и срока жизни ключа без Docker."""
import sqlite3
import unittest
from contextlib import closing

from bench import SCHEMA, classify


class Windows(unittest.TestCase):
    def test_boundary_and_expiry(self):
        with closing(sqlite3.connect(":memory:")) as db:
            db.executescript(SCHEMA)
            maximum = -1
            for i, (key, ts, expected) in enumerate([
                (1, 1000, "ok"), (2, 1600, "ok"), (1, 1000, "duplicate"),
                (3, 1601, "ok"), (1, 1000, "late"),
            ]):
                raw = f'{{"event_id":{key},"ts":{ts},"article_id":1,"bot":false}}'
                actual, maximum = classify(db, raw, i, maximum)
                self.assertEqual(actual, expected)
            self.assertIsNone(db.execute("SELECT id FROM seen WHERE id=1").fetchone())


if __name__ == "__main__":
    unittest.main()
