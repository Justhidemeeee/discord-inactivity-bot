# Discord Inactivity Bot

Answers "who in my server has posted fewer than N messages in the last M days?" — with real, accurate numbers.

## Commands

All commands are mod-only (require Manage Server permission).

- **`/inactive [threshold] [days] [only_role] [exclude_role] [min_join_age_days]`**
  Main query. Lists members below the threshold in the window. Returns an embed with mentions, message counts, join dates. If >40 matches, attaches a text file with the full list.
- **`/activity user:@someone [days]`**
  Deep dive on one member — total messages, last active day, most-active channel.
- **`/backfill [days] [channel]`**
  Walks channel history to seed the DB. Default 180 days back, all readable text channels. Resumable — if the bot restarts mid-scan, it picks up where it left off.
- **`/backfill_status`**
  Shows how the backfill is progressing.
- **`/digest`**
  Manually posts the monthly digest to the configured channel right now (bypasses schedule). Useful for testing.

## Monthly digest

If you set `DIGEST_CHANNEL_ID` in the env, the bot auto-posts an inactivity report to that channel once a month at your configured day/hour:

- `DIGEST_CHANNEL_ID` — target channel (0 = disabled)
- `DIGEST_DAY_OF_MONTH` — 1-28 (defaults to 1st of the month)
- `DIGEST_HOUR_UTC` — 0-23 UTC (defaults to 14 = 10am ET / 9am CT / 7am PT)
- `DIGEST_THRESHOLD` — 0 to use the `DEFAULT_THRESHOLD`, or override just for the digest
- `DIGEST_DAYS` — 0 to use the `DEFAULT_DAYS`, or override (e.g. `30` for "last month only")

The scheduler checks hourly, posts once per calendar month (deduped in the DB so bot restarts don't double-post). Test it any time with `/digest`.

## How it works (hybrid)

- **Live capture (`on_message`):** every new message increments a per-day, per-channel, per-user counter in SQLite. Cost per message: one SQLite upsert (µs-scale).
- **Backfill:** one-time walk of channel history to seed the counters with older messages. Resumable and idempotent — safe to re-run.
- **Query:** `/inactive` runs `SELECT user_id, SUM(count) ... WHERE day >= cutoff GROUP BY user_id`. Instant even for large servers.

## What it can't tell you

- **Voice-channel activity** — Discord's API doesn't give bots access to voice presence history. Someone who chats in voice all day but never types a message will look inactive.
- **Reaction-only activity** — reactions aren't messages. If you want reactions to count as engagement, that's a separate change (ask and I'll add it).
- **Deleted messages** — Discord's message history doesn't return them, so they don't count.
- **Threads inside forum posts** — counted, but grouped by thread ID rather than parent forum. If you want them rolled up to the forum, small tweak in `on_message`.

## Setup

### 1. Create the Discord app

1. https://discord.com/developers/applications → New Application
2. Bot tab → Reset Token → copy
3. Enable **Server Members Intent** and **Message Content Intent** (we don't actually use content, but having it on lets the intents system deliver `message.created_at` reliably for backfill)
4. OAuth2 → URL Generator → scopes: `bot`, `applications.commands`. Bot permissions: **View Channels, Read Message History, Send Messages, Embed Links**
5. Invite the bot to your server

### 2. Get your IDs

Enable Developer Mode (User Settings → Advanced), then right-click your server icon → Copy Server ID.

### 3. Deploy on Railway

1. Push this folder to a GitHub repo (make sure `.env` is gitignored)
2. New Railway project → Deploy from repo
3. **Add a Volume** (Settings tab → Volumes → New Volume → mount path `/data`). This is **required** or your DB gets wiped on every redeploy.
4. Variables tab → Raw Editor:
   ```
   DISCORD_TOKEN=your-token
   GUILD_ID=your-server-id
   DB_PATH=/data/activity.db
   DEFAULT_THRESHOLD=10
   DEFAULT_DAYS=60
   RETENTION_DAYS=365
   BACKFILL_BATCH_SIZE=500
   ```
5. Deploy. Watch logs for `Logged in as ...`.

### 4. Seed the DB

In Discord, run `/backfill days:90` (or however far back you care about). Then `/backfill_status` to watch progress. For a busy server this can take 5-20 minutes because of Discord's rate limits.

Once complete, `/inactive` returns accurate results going back that many days.

### 5. Just query

`/inactive threshold:5 days:30 exclude_role:@Mods` — lists non-mod members who posted 5 or fewer messages in the last 30 days.

## Gotchas

- **First-time DB is empty.** Until you run `/backfill`, `/inactive` will show everyone as inactive because no counts exist. Run backfill first.
- **Backfill runs in the background.** You can use other commands while it's going; the bot won't block on it.
- **Retention default is 365 days.** Bump `RETENTION_DAYS` higher if you want longer memory.
- **Bots are always excluded.** Bot accounts don't get counted or listed.

## Optional next steps (ask and I'll add)

- Auto-DM warnings to inactive users N days before pruning
- Prune command that kicks everyone matching a query
- Weekly inactive-report posted to a channel automatically
- Reactions as activity signal
- CSV export of full activity history
