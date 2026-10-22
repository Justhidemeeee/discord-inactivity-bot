# Discord Inactivity Bot

Answers "who in my server has posted fewer than N messages in the last M days?" — with real, accurate numbers.

## Commands

All commands are mod-only (require Manage Server permission).

- **`/inactive [threshold] [days] [only_role] [exclude_role] [min_join_age_days]`**
  Main query. Lists members below the threshold in the window. Returns an embed with mentions, message counts, join dates. If >40 matches, attaches a text file with the full list.
- **`/activity user:@someone [days]`**
  Deep dive on one member — total messages, last active day, most-active channel.
- **`/backfill [days] [channel]`**
  Walks history to seed the DB. Default 180 days back (capped at `RETENTION_DAYS`), covering every readable text/voice/stage channel plus threads in text and forum channels (including archived ones). Resumable — if the bot restarts mid-scan, it picks up where it left off. Re-running with a larger `days` extends the history instead of being ignored.
- **`/backfill_status`**
  Shows how the backfill is progressing.
- **`/digest`**
  Manually posts the monthly digest to the configured channel right now (bypasses schedule). Useful for testing.
- **`/prune_announce channel [days_until_prune] [threshold] [days_lookback]`**
  Posts an `@everyone` warning describing exactly who will be removed (members with `threshold` or fewer messages) and when.
- **`/prune_inactive [threshold] [days] [only_role] [exclude_role] [min_join_age_days] [dry_run]`**
  Previews who would be kicked. With `dry_run:false`, run by the user in `AUTHORIZED_PRUNER_ID`, it adds a two-click kick button. Activity is re-checked at confirm time, so anyone who posted since the preview is spared. Kicks are logged to the digest channel if one is set.

## Monthly digest

If you set `DIGEST_CHANNEL_ID` in the env, the bot auto-posts an inactivity report to that channel once a month at your configured day/hour:

- `DIGEST_CHANNEL_ID` — target channel (0 = disabled)
- `DIGEST_DAY_OF_MONTH` — 1-28 (defaults to 1st of the month)
- `DIGEST_HOUR_UTC` — 0-23 UTC (defaults to 14 = 10am ET / 9am CT / 7am PT)
- `DIGEST_THRESHOLD` — 0 to use the `DEFAULT_THRESHOLD`, or override just for the digest
- `DIGEST_DAYS` — 0 to use the `DEFAULT_DAYS`, or override (e.g. `30` for "last month only")

The scheduler checks hourly and posts once per calendar month, at or after the configured day/hour (deduped in the DB so restarts don't double-post, and a bot that was down at the scheduled time catches up when it comes back — including right after first deploy if that day has already passed). Test it any time with `/digest`.

## How it works (hybrid)

- **Live capture (`on_message`):** every new message increments a per-day, per-channel, per-user counter. Counts are buffered in memory and written to SQLite every 5 seconds (and on shutdown), so a busy server costs one transaction per flush rather than one per message.
- **Backfill:** walks channel history older than the moment live capture first started, so a message is never counted by both. Each batch and its checkpoint are written in one transaction, so a crash mid-scan can't double-count on resume. Channels are scanned in parallel (`BACKFILL_CONCURRENCY`, default 3).
- **Query:** `/inactive` runs `SELECT user_id, SUM(count) ... WHERE day >= cutoff GROUP BY user_id`. Instant even for large servers.

## What it can't tell you

- **Voice-channel activity** — Discord's API doesn't give bots access to voice presence history. Someone who chats in voice all day but never types a message will look inactive.
- **Reaction-only activity** — reactions aren't messages. If you want reactions to count as engagement, that's a separate change (ask and I'll add it).
- **Deleted messages** — Discord's message history doesn't return them, so they don't count.
- **Threads and forum posts** — counted (live and backfill), but grouped by thread ID rather than parent channel. If you want them rolled up to the parent, small tweak in `on_message` and `_backfill_channel`.
- **Messages sent while the bot is offline** — live capture misses them, and backfill only walks history from before live capture first started, so downtime gaps stay uncounted. Keep downtime short.

## Setup

### 1. Create the Discord app

1. https://discord.com/developers/applications → New Application
2. Bot tab → Reset Token → copy
3. Enable **Server Members Intent**. (Message Content Intent is *not* needed — the bot only reads message metadata.)
4. OAuth2 → URL Generator → scopes: `bot`, `applications.commands`. Bot permissions: **View Channels, Read Message History, Send Messages, Embed Links, Attach Files**. For pruning also add **Kick Members** and **Mention @everyone** (for `/prune_announce`), and **Manage Threads** if you want private archived threads backfilled. The bot's role must sit above anyone it should be able to kick.
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
   BACKFILL_CONCURRENCY=3
   AUTHORIZED_PRUNER_ID=your-user-id
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
- Reactions as activity signal
- CSV export of full activity history
