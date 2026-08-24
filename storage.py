"""SQLite-backed message activity store.

Schema is a rolled-up daily fact table — one row per (user, channel, day)
counting messages. This keeps the DB small (thousands of rows even for busy
servers) and makes arbitrary window queries fast:

    SELECT user_id, SUM(count)
    FROM activity
    WHERE day >= :cutoff
    GROUP BY user_id
    HAVING SUM(count) < :threshold

We also track backfill progress per channel so /backfill is resumable — if
Railway restarts mid-scan, we pick up from where we left off instead of
re-processing thousands of messages.
"""

from __future__ import annotations

import datetime as dt
import logging
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Optional

log = logging.getLogger(__name__)


class Storage:
    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        # sqlite3 connections are thread-affine — we open one and guard it
        # with a lock, which is fine for our low-contention workload.
        self._conn = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self._lock = threading.Lock()
        self._migrate()

    # -- schema --------------------------------------------------------------------

    def _migrate(self) -> None:
        with self._locked() as cur:
            cur.executescript("""
                CREATE TABLE IF NOT EXISTS activity (
                    user_id     INTEGER NOT NULL,
                    channel_id  INTEGER NOT NULL,
                    day         TEXT    NOT NULL,   -- ISO date YYYY-MM-DD (UTC)
                    count       INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (user_id, channel_id, day)
                );
                CREATE INDEX IF NOT EXISTS idx_activity_day        ON activity(day);
                CREATE INDEX IF NOT EXISTS idx_activity_user_day   ON activity(user_id, day);

                CREATE TABLE IF NOT EXISTS backfill_progress (
                    channel_id       INTEGER PRIMARY KEY,
                    last_message_id  INTEGER,       -- most recent message we ingested
                    oldest_reached   INTEGER,       -- oldest message id we've walked back to
                    complete         INTEGER NOT NULL DEFAULT 0,
                    updated_at       TEXT
                );

                CREATE TABLE IF NOT EXISTS meta (
                    key   TEXT PRIMARY KEY,
                    value TEXT
                );
            """)

    @contextmanager
    def _locked(self):
        with self._lock:
            cur = self._conn.cursor()
            try:
                yield cur
            finally:
                cur.close()

    # -- writes --------------------------------------------------------------------

    def bump(self, *, user_id: int, channel_id: int, day: str, delta: int = 1) -> None:
        """Increment the counter for one (user, channel, day) tuple. Used by live capture."""
        with self._locked() as cur:
            cur.execute("""
                INSERT INTO activity (user_id, channel_id, day, count)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id, channel_id, day) DO UPDATE SET count = count + excluded.count
            """, (user_id, channel_id, day, delta))

    def bulk_bump(self, rows: Iterable[tuple[int, int, str, int]]) -> None:
        """Batch upsert (user_id, channel_id, day, count) tuples. Used by backfill."""
        rows = list(rows)
        if not rows:
            return
        with self._locked() as cur:
            cur.execute("BEGIN")
            cur.executemany("""
                INSERT INTO activity (user_id, channel_id, day, count)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id, channel_id, day) DO UPDATE SET count = count + excluded.count
            """, rows)
            cur.execute("COMMIT")

    # -- queries -------------------------------------------------------------------

    def message_counts_since(self, cutoff_day: str) -> dict[int, int]:
        """Return {user_id: total_messages} for all activity on/after cutoff_day."""
        with self._locked() as cur:
            cur.execute("""
                SELECT user_id, SUM(count) AS total
                FROM activity
                WHERE day >= ?
                GROUP BY user_id
            """, (cutoff_day,))
            return {row[0]: row[1] for row in cur.fetchall()}

    def per_user_stats(self, user_id: int, cutoff_day: str) -> dict:
        """Detailed stats for one user: total, last_day_seen, top channel."""
        with self._locked() as cur:
            cur.execute("""
                SELECT SUM(count), MAX(day)
                FROM activity WHERE user_id = ? AND day >= ?
            """, (user_id, cutoff_day))
            total, last_day = cur.fetchone()
            cur.execute("""
                SELECT channel_id, SUM(count) AS c
                FROM activity WHERE user_id = ? AND day >= ?
                GROUP BY channel_id ORDER BY c DESC LIMIT 1
            """, (user_id, cutoff_day))
            top = cur.fetchone()
            top_channel_id, top_count = (top if top else (None, 0))
        return {
            "total": total or 0,
            "last_day": last_day,
            "top_channel_id": top_channel_id,
            "top_channel_count": top_count or 0,
        }

    # -- backfill bookkeeping ------------------------------------------------------

    def get_progress(self, channel_id: int) -> Optional[tuple[int, int, bool]]:
        """Return (last_message_id, oldest_reached, complete) or None."""
        with self._locked() as cur:
            cur.execute("""
                SELECT last_message_id, oldest_reached, complete
                FROM backfill_progress WHERE channel_id = ?
            """, (channel_id,))
            row = cur.fetchone()
        if not row:
            return None
        return (row[0] or 0, row[1] or 0, bool(row[2]))

    def save_progress(self, *, channel_id: int, last_message_id: Optional[int],
                      oldest_reached: Optional[int], complete: bool) -> None:
        with self._locked() as cur:
            cur.execute("""
                INSERT INTO backfill_progress
                    (channel_id, last_message_id, oldest_reached, complete, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(channel_id) DO UPDATE SET
                    last_message_id = COALESCE(excluded.last_message_id, backfill_progress.last_message_id),
                    oldest_reached  = COALESCE(excluded.oldest_reached,  backfill_progress.oldest_reached),
                    complete        = excluded.complete,
                    updated_at      = excluded.updated_at
            """, (channel_id, last_message_id, oldest_reached, 1 if complete else 0,
                  dt.datetime.now(dt.timezone.utc).isoformat()))

    def all_progress(self) -> list[dict]:
        with self._locked() as cur:
            cur.execute("""
                SELECT channel_id, last_message_id, oldest_reached, complete, updated_at
                FROM backfill_progress
            """)
            rows = cur.fetchall()
        return [
            {"channel_id": r[0], "last_message_id": r[1], "oldest_reached": r[2],
             "complete": bool(r[3]), "updated_at": r[4]}
            for r in rows
        ]

    # -- maintenance ---------------------------------------------------------------

    def purge_older_than(self, cutoff_day: str) -> int:
        with self._locked() as cur:
            cur.execute("DELETE FROM activity WHERE day < ?", (cutoff_day,))
            return cur.rowcount

    def total_rows(self) -> int:
        with self._locked() as cur:
            cur.execute("SELECT COUNT(*) FROM activity")
            return cur.fetchone()[0]

    def meta_get(self, key: str) -> Optional[str]:
        with self._locked() as cur:
            cur.execute("SELECT value FROM meta WHERE key = ?", (key,))
            row = cur.fetchone()
        return row[0] if row else None

    def meta_set(self, key: str, value: str) -> None:
        with self._locked() as cur:
            cur.execute("""
                INSERT INTO meta (key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """, (key, value))
