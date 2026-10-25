"""Env-driven configuration for the inactivity bot."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v and v.strip() else default


@dataclass(frozen=True)
class Config:
    discord_token: str
    guild_id: int

    # Defaults for /inactive when arguments are omitted.
    default_threshold: int      # max messages to be considered "inactive"
    default_days: int           # lookback window in days

    # Monthly digest — set digest_channel_id to 0 to disable.
    digest_channel_id: int
    digest_day_of_month: int    # 1-28 (28 to avoid month-length edge cases)
    digest_hour_utc: int        # 0-23
    digest_threshold: int       # 0 = fall back to default_threshold
    digest_days: int            # 0 = fall back to default_days

    # Only this Discord user ID can confirm destructive prunes (0 = disabled).
    # Anyone with Manage Server can *run* /prune_inactive to see previews,
    # but the kick button is only attached when this user runs it.
    authorized_pruner_id: int

    # How many days of history to keep. Older rows are periodically purged.
    retention_days: int

    # Backfill knobs.
    backfill_batch_size: int    # rows to flush per SQLite transaction
    backfill_concurrency: int   # channels scanned in parallel

    db_path: str        # SQLite file, used when database_url is empty
    database_url: str   # Postgres URL (Heroku sets DATABASE_URL automatically)

    @classmethod
    def from_env(cls) -> "Config":
        required = ("DISCORD_TOKEN", "GUILD_ID")
        missing = [k for k in required if not os.getenv(k)]
        if missing:
            raise RuntimeError("Missing required env vars: " + ", ".join(missing))

        return cls(
            discord_token=os.environ["DISCORD_TOKEN"],
            guild_id=int(os.environ["GUILD_ID"]),
            default_threshold=_int("DEFAULT_THRESHOLD", 10),
            default_days=_int("DEFAULT_DAYS", 60),
            digest_channel_id=_int("DIGEST_CHANNEL_ID", 0),
            digest_day_of_month=min(28, max(1, _int("DIGEST_DAY_OF_MONTH", 1))),
            digest_hour_utc=min(23, max(0, _int("DIGEST_HOUR_UTC", 14))),
            digest_threshold=_int("DIGEST_THRESHOLD", 0),
            digest_days=_int("DIGEST_DAYS", 0),
            authorized_pruner_id=_int("AUTHORIZED_PRUNER_ID", 0),
            retention_days=_int("RETENTION_DAYS", 365),
            backfill_batch_size=max(1, _int("BACKFILL_BATCH_SIZE", 500)),
            backfill_concurrency=min(10, max(1, _int("BACKFILL_CONCURRENCY", 3))),
            db_path=os.getenv("DB_PATH", "activity.db"),
            database_url=os.getenv("DATABASE_URL", ""),
        )
