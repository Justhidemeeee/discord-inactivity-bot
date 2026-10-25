"""Discord bot that tracks per-member message activity and answers 'who is inactive?'.

Hybrid design:
  - Live capture (`on_message`) increments a per-day counter on every new message.
  - `/backfill` walks channel history to seed those counters with historical data.
  - `/inactive` queries the counters for anyone below a threshold in a time window.

Storage (see storage.py) is Postgres when DATABASE_URL is set — Heroku sets it
when you attach Heroku Postgres — and SQLite otherwise. With SQLite on Railway,
attach a Volume mounted at /data and set DB_PATH=/data/activity.db, otherwise
the DB is wiped on every redeploy and you'd have to re-backfill.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import io
import logging
import signal
from collections import Counter
from pathlib import Path
from typing import Optional, Union

import discord
from discord import app_commands
from discord.ext import tasks

import sitehealth
from config import Config
from storage import Storage, iso_utc, parse_iso_utc

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("inactivity-bot")

# Slash-command argument bounds — Discord enforces these client-side.
Threshold = app_commands.Range[int, 0, 1_000_000]
Days = app_commands.Range[int, 1, 3650]
JoinAge = app_commands.Range[int, 0, 3650]

# Anything whose history we can walk.
HistoryChannel = Union[discord.TextChannel, discord.VoiceChannel, discord.StageChannel, discord.Thread]

EMBED_LIST_LIMIT = 40


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------


def utc_day(ts: dt.datetime) -> str:
    """YYYY-MM-DD in UTC — our storage day key."""
    return ts.astimezone(dt.timezone.utc).strftime("%Y-%m-%d")


def cutoff_day(days_back: int) -> str:
    return utc_day(dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days_back))


def text_file(text: str, filename: str) -> discord.File:
    return discord.File(io.BytesIO(text.encode()), filename=filename)


def add_footer(embed: discord.Embed, text: str) -> None:
    """Append to the embed footer instead of overwriting it."""
    existing = embed.footer.text
    embed.set_footer(text=f"{existing} · {text}" if existing else text)


# --------------------------------------------------------------------------------------
# Bot
# --------------------------------------------------------------------------------------


def make_intents() -> discord.Intents:
    intents = discord.Intents.default()
    intents.members = True          # for member lookup / join dates
    intents.messages = True         # required for on_message
    intents.guilds = True
    intents.message_content = False  # we don't need content, only metadata
    return intents


class InactivityBot(discord.Client):
    def __init__(self, config: Config):
        super().__init__(intents=make_intents())
        self.config = config
        self.tree = app_commands.CommandTree(self)
        self.storage = Storage(Path(config.db_path), database_url=config.database_url)
        self._backfill_task: Optional[asyncio.Task] = None
        self._shutdown_task: Optional[asyncio.Task] = None
        # Live counts are buffered in memory and flushed every few seconds,
        # so a busy server costs one SQLite transaction per flush, not per message.
        self._live_buffer: Counter[tuple[int, int, str]] = Counter()
        # When live capture first started. Backfill only walks history older than
        # this, so it never re-counts messages on_message already counted.
        self._live_since = parse_iso_utc(self.storage.meta_setdefault(
            "live_since", iso_utc(dt.datetime.now(dt.timezone.utc))))
        self._register_commands()

    # -- lifecycle ------------------------------------------------------------------

    async def setup_hook(self) -> None:
        guild = discord.Object(id=self.config.guild_id)
        self.tree.copy_global_to(guild=guild)
        await self.tree.sync(guild=guild)
        self.live_flush.start()
        self.nightly_maintenance.start()
        if self.config.digest_channel_id:
            self.monthly_digest.start()
        # Railway stops containers with SIGTERM; close cleanly so buffered counts are saved.
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._request_shutdown)
            except (NotImplementedError, RuntimeError):
                pass  # e.g. Windows

    def _request_shutdown(self) -> None:
        if self._shutdown_task is None:
            self._shutdown_task = asyncio.create_task(self.close())

    async def close(self) -> None:
        await self._flush_live()
        await super().close()

    async def on_ready(self) -> None:
        total = await asyncio.to_thread(self.storage.total_rows)
        log.info("Logged in as %s — tracking guild %s (DB=%s, %d rows)",
                 self.user, self.config.guild_id, self.storage.description, total)

    # -- live capture ---------------------------------------------------------------

    async def on_message(self, message: discord.Message) -> None:
        if message.guild is None or message.guild.id != self.config.guild_id:
            return
        if message.author.bot:
            return
        # Threads: message.channel is the thread; message.channel.parent_id is the
        # parent forum/text channel. Storing the thread id groups activity by thread,
        # which is what most people want. If you'd rather roll thread activity up to
        # the parent, replace channel.id with getattr(channel, "parent_id", channel.id)
        # here and in _backfill_channel.
        self._live_buffer[(message.author.id, message.channel.id, utc_day(message.created_at))] += 1

    async def _flush_live(self) -> None:
        if not self._live_buffer:
            return
        buf, self._live_buffer = self._live_buffer, Counter()
        try:
            await asyncio.to_thread(self.storage.bulk_bump,
                                    [(u, c, d, n) for (u, c, d), n in buf.items()])
        except Exception:
            log.exception("Live flush failed — will retry")
            self._live_buffer.update(buf)

    @tasks.loop(seconds=5)
    async def live_flush(self):
        await self._flush_live()

    # -- nightly maintenance --------------------------------------------------------

    @tasks.loop(hours=24)
    async def nightly_maintenance(self):
        cutoff = cutoff_day(self.config.retention_days)
        deleted = await asyncio.to_thread(self.storage.purge_older_than, cutoff)
        if deleted:
            log.info("Purged %d activity rows older than %s", deleted, cutoff)

    @nightly_maintenance.before_loop
    async def _wait_ready(self):
        await self.wait_until_ready()

    # -- monthly digest -------------------------------------------------------------

    @tasks.loop(hours=1)
    async def monthly_digest(self):
        """Fires hourly; posts once per month at or after the configured day/hour.

        Using "at or after" means a bot that was down at the scheduled hour
        catches up when it comes back instead of skipping the month.
        """
        now = dt.datetime.now(dt.timezone.utc)
        scheduled = now.replace(day=self.config.digest_day_of_month,
                                hour=self.config.digest_hour_utc,
                                minute=0, second=0, microsecond=0)
        if now < scheduled:
            return
        # Dedup: don't post twice in the same month across restarts.
        stamp_key = "last_digest_month"
        this_month = now.strftime("%Y-%m")
        last = await asyncio.to_thread(self.storage.meta_get, stamp_key)
        if last == this_month:
            return
        log.info("Posting monthly digest for %s", this_month)
        try:
            await self._post_digest(triggered_by=None)
            await asyncio.to_thread(self.storage.meta_set, stamp_key, this_month)
        except Exception:
            log.exception("Monthly digest failed — will retry next hour")

    @monthly_digest.before_loop
    async def _wait_ready_digest(self):
        await self.wait_until_ready()

    # -- shared helper: compute inactive members list -------------------------------

    async def _compute_inactive(
        self,
        guild: discord.Guild,
        *,
        threshold: int,
        days: int,
        only_role: Optional[discord.Role] = None,
        exclude_role: Optional[discord.Role] = None,
        min_join_age_days: int = 0,
    ) -> list[tuple[discord.Member, int]]:
        await self._flush_live()
        counts = await asyncio.to_thread(self.storage.message_counts_since, cutoff_day(days))
        now = dt.datetime.now(dt.timezone.utc)
        min_age = dt.timedelta(days=min_join_age_days)
        out: list[tuple[discord.Member, int]] = []
        for member in guild.members:
            if member.bot:
                continue
            # get_role is a binary search on the member's role ids; `in member.roles`
            # would build a list of Role objects for every member.
            if only_role and member.get_role(only_role.id) is None:
                continue
            if exclude_role and member.get_role(exclude_role.id) is not None:
                continue
            if min_join_age_days > 0 and member.joined_at is not None:
                if now - member.joined_at < min_age:
                    continue
            c = counts.get(member.id, 0)
            if c <= threshold:
                out.append((member, c))
        out.sort(key=lambda t: (t[1], t[0].joined_at or now))
        return out

    def _build_inactive_embed(
        self,
        inactive: list[tuple[discord.Member, int]],
        *,
        threshold: int,
        days: int,
        only_role: Optional[discord.Role],
        exclude_role: Optional[discord.Role],
        min_join_age_days: int,
        title_prefix: str = "😴 Inactive members",
    ) -> tuple[discord.Embed, list[discord.File]]:
        title = f"{title_prefix} ({len(inactive)} found)"
        desc = (f"**≤ {threshold} messages** in the last **{days} days**\n"
                f"Filters: {'only ' + only_role.mention if only_role else 'all members'}"
                f"{', excl. ' + exclude_role.mention if exclude_role else ''}"
                f"{', join age ≥ ' + str(min_join_age_days) + 'd' if min_join_age_days else ''}")
        if days > self.config.retention_days:
            desc += (f"\n⚠️ Only {self.config.retention_days} days of history are kept "
                     f"(`RETENTION_DAYS`), so older activity isn't counted.")

        if not inactive:
            embed = discord.Embed(
                title=title,
                description=desc + "\n\n_No matches — everyone is above the threshold._",
                color=discord.Color.green(),
            )
            return embed, []

        lines_head = []
        for member, count in inactive[:EMBED_LIST_LIMIT]:
            joined = member.joined_at.strftime("%Y-%m-%d") if member.joined_at else "?"
            lines_head.append(f"• {member.mention} — **{count}** msg · joined {joined}")

        embed = discord.Embed(
            title=title,
            description=desc + "\n\n" + "\n".join(lines_head),
            color=discord.Color.orange(),
        )

        files: list[discord.File] = []
        if len(inactive) > EMBED_LIST_LIMIT:
            lines = [f"# Inactive members — ≤{threshold} msgs in last {days}d"]
            for member, count in inactive:
                joined = member.joined_at.strftime("%Y-%m-%d") if member.joined_at else "?"
                lines.append(f"{member} ({member.id})\t{count} msg\tjoined {joined}")
            files.append(text_file("\n".join(lines) + "\n", "inactive_members.txt"))
            embed.set_footer(text=f"Showing first {EMBED_LIST_LIMIT} of {len(inactive)} — "
                                  f"full list attached.")
        return embed, files

    async def _digest_channel(self) -> discord.abc.Messageable:
        """Resolve DIGEST_CHANNEL_ID, raising a readable error if it's unusable."""
        cid = self.config.digest_channel_id
        channel = self.get_channel(cid)
        if channel is None:
            try:
                channel = await self.fetch_channel(cid)
            except discord.HTTPException as e:
                raise RuntimeError(f"digest channel {cid} not found or not visible to the bot") from e
        if not isinstance(channel, (discord.TextChannel, discord.Thread, discord.VoiceChannel)):
            raise RuntimeError(f"digest channel {cid} is not a channel the bot can post in")
        return channel

    async def _post_digest(self, *, triggered_by: Optional[discord.Interaction]) -> discord.abc.Messageable:
        """Compute + post the monthly digest to the configured channel. Raises on failure."""
        guild = self.get_guild(self.config.guild_id)
        if guild is None:
            raise RuntimeError(f"guild {self.config.guild_id} not found")
        channel = await self._digest_channel()

        th = self.config.digest_threshold or self.config.default_threshold
        dy = self.config.digest_days or self.config.default_days
        inactive = await self._compute_inactive(guild, threshold=th, days=dy)

        now = dt.datetime.now(dt.timezone.utc)
        prefix = f"📊 Monthly inactivity digest — {now.strftime('%B %Y')}"
        embed, files = self._build_inactive_embed(
            inactive,
            threshold=th, days=dy,
            only_role=None, exclude_role=None, min_join_age_days=0,
            title_prefix=prefix,
        )
        if triggered_by is not None:
            add_footer(embed, f"Triggered manually by {triggered_by.user}")
        await channel.send(embed=embed, files=files)
        return channel

    # -- slash commands -------------------------------------------------------------

    def _register_commands(self) -> None:

        @self.tree.command(name="inactive",
                           description="List members below a message-count threshold in a time window.")
        @app_commands.describe(
            threshold="Max messages to count as inactive (default from env).",
            days="Look-back window in days (default from env).",
            only_role="Only check members with this role.",
            exclude_role="Skip members with this role (e.g. mods, VIPs).",
            min_join_age_days="Ignore members who joined more recently than this.",
        )
        @app_commands.default_permissions(manage_guild=True)
        async def inactive_cmd(
            interaction: discord.Interaction,
            threshold: Optional[Threshold] = None,
            days: Optional[Days] = None,
            only_role: Optional[discord.Role] = None,
            exclude_role: Optional[discord.Role] = None,
            min_join_age_days: Optional[JoinAge] = None,
        ):
            await interaction.response.defer(thinking=True, ephemeral=True)
            th = threshold if threshold is not None else self.config.default_threshold
            dy = days if days is not None else self.config.default_days
            min_age = min_join_age_days if min_join_age_days is not None else 0

            guild = interaction.guild
            if guild is None:
                await interaction.followup.send("Server context missing.", ephemeral=True)
                return

            inactive = await self._compute_inactive(
                guild,
                threshold=th, days=dy,
                only_role=only_role, exclude_role=exclude_role,
                min_join_age_days=min_age,
            )
            embed, files = self._build_inactive_embed(
                inactive,
                threshold=th, days=dy,
                only_role=only_role, exclude_role=exclude_role,
                min_join_age_days=min_age,
            )
            await interaction.followup.send(embed=embed, files=files, ephemeral=True)

        @self.tree.command(name="activity",
                           description="Show recent activity for one member.")
        @app_commands.describe(user="The member to inspect.",
                               days="Look-back window (default from env).")
        @app_commands.default_permissions(manage_guild=True)
        async def activity_cmd(interaction: discord.Interaction,
                               user: discord.Member,
                               days: Optional[Days] = None):
            await interaction.response.defer(thinking=True, ephemeral=True)
            dy = days if days is not None else self.config.default_days
            await self._flush_live()
            stats = await asyncio.to_thread(self.storage.per_user_stats, user.id, cutoff_day(dy))

            top_ch = "-"
            if stats["top_channel_id"]:
                ch = interaction.guild.get_channel_or_thread(stats["top_channel_id"]) if interaction.guild else None
                top_ch = ch.mention if ch else f"`{stats['top_channel_id']}`"

            joined = user.joined_at.strftime("%Y-%m-%d") if user.joined_at else "?"

            embed = discord.Embed(
                title=f"Activity — {user}",
                color=discord.Color.blurple(),
            )
            embed.add_field(name=f"Messages (last {dy}d)", value=str(stats["total"]), inline=True)
            embed.add_field(name="Last seen (UTC day)", value=stats["last_day"] or "never", inline=True)
            embed.add_field(name="Joined server", value=joined, inline=True)
            embed.add_field(name="Most-active channel",
                            value=f"{top_ch} ({stats['top_channel_count']} msgs)", inline=False)
            await interaction.followup.send(embed=embed, ephemeral=True)

        @self.tree.command(
            name="prune_announce",
            description="Post an @everyone warning that inactive members will be removed soon.",
        )
        @app_commands.describe(
            channel="Channel to post the announcement in (usually #general or #announcements).",
            days_until_prune="How many days from now the prune will happen.",
            threshold="Members with this many messages or fewer will be removed.",
            days_lookback="How far back the message count is measured.",
        )
        @app_commands.default_permissions(manage_guild=True)
        async def prune_announce_cmd(
            interaction: discord.Interaction,
            channel: discord.TextChannel,
            days_until_prune: app_commands.Range[int, 1, 365] = 7,
            threshold: Optional[Threshold] = None,
            days_lookback: Optional[Days] = None,
        ):
            th = threshold if threshold is not None else self.config.default_threshold
            dy = days_lookback if days_lookback is not None else self.config.default_days

            perms = channel.permissions_for(channel.guild.me)
            if not (perms.send_messages and perms.mention_everyone):
                await interaction.response.send_message(
                    f"❌ I need **Send Messages** and **Mention @everyone** in {channel.mention} "
                    f"to post this announcement.", ephemeral=True)
                return

            deadline = dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=days_until_prune)
            # Discord dynamic timestamp — shows in each viewer's local timezone.
            deadline_ts = int(deadline.timestamp())

            # Wording must match _compute_inactive exactly: count <= threshold is removed.
            if th == 0:
                rule = f"Members who haven't posted **any messages in the last {dy} days**"
                stay = "Even a single message keeps you in."
            else:
                rule = f"Members with **{th} or fewer messages in the last {dy} days**"
                stay = (f"You need at least **{th + 1} messages** in the {dy} days "
                        f"leading up to the deadline.")
            body = (
                f"@everyone\n\n"
                f"🚨 **Inactivity cleanup on <t:{deadline_ts}:F>** (<t:{deadline_ts}:R>)\n\n"
                f"{rule} will be removed from the server on that date.\n\n"
                f"**To stay:** post in any channel before the deadline. {stay}\n\n"
                f"_Automated cleanup — no action needed if you've been active._"
            )
            try:
                await channel.send(
                    body,
                    allowed_mentions=discord.AllowedMentions(everyone=True),
                )
            except discord.Forbidden:
                await interaction.response.send_message(
                    f"❌ Missing permission to post in {channel.mention}.", ephemeral=True)
                return
            await interaction.response.send_message(
                f"✅ Announcement posted in {channel.mention}. "
                f"Run `/prune_inactive threshold:{th} days:{dy} dry_run:false` after "
                f"{deadline:%Y-%m-%d %H:%M UTC} to actually kick.",
                ephemeral=True,
            )

        @self.tree.command(
            name="prune_inactive",
            description="Preview and (with confirm) kick inactive members.",
        )
        @app_commands.describe(
            threshold="Max messages to be pruned (default from env).",
            days="Look-back window (default from env).",
            only_role="Only prune members with this role.",
            exclude_role="Skip members with this role (mods, VIPs, etc.).",
            min_join_age_days="Skip members who joined more recently than this.",
            dry_run="true = preview only (default). false = show kick button.",
        )
        @app_commands.default_permissions(manage_guild=True)
        async def prune_inactive_cmd(
            interaction: discord.Interaction,
            threshold: Optional[Threshold] = None,
            days: Optional[Days] = None,
            only_role: Optional[discord.Role] = None,
            exclude_role: Optional[discord.Role] = None,
            min_join_age_days: Optional[JoinAge] = None,
            dry_run: bool = True,
        ):
            await interaction.response.defer(thinking=True, ephemeral=True)
            th = threshold if threshold is not None else self.config.default_threshold
            dy = days if days is not None else self.config.default_days
            min_age = min_join_age_days if min_join_age_days is not None else 0

            guild = interaction.guild
            if guild is None:
                await interaction.followup.send("Server context missing.", ephemeral=True)
                return
            if not guild.me.guild_permissions.kick_members:
                await interaction.followup.send(
                    "❌ I don't have the **Kick Members** permission, so I can't prune. "
                    "Grant it to my role and try again.", ephemeral=True)
                return

            inactive = await self._compute_inactive(
                guild,
                threshold=th, days=dy,
                only_role=only_role, exclude_role=exclude_role,
                min_join_age_days=min_age,
            )

            # Filter out members the bot can't kick, so the preview is honest.
            kickable, skipped = self._split_kickable(guild, [m for m, _ in inactive])
            kickable_ids = {m.id for m in kickable}

            embed, files = self._build_inactive_embed(
                [(m, c) for m, c in inactive if m.id in kickable_ids],
                threshold=th, days=dy,
                only_role=only_role, exclude_role=exclude_role, min_join_age_days=min_age,
                title_prefix="🔨 Prune preview" if not dry_run else "🧪 Prune dry-run",
            )
            if skipped:
                embed.description = (embed.description or "") + (
                    f"\n\n⚠️ **{len(skipped)} members can't be kicked** "
                    f"(higher role than the bot, or server owner) — they're excluded from the count."
                )

            view: Optional[discord.ui.View] = None
            pruner_id = self.config.authorized_pruner_id
            if dry_run or not kickable:
                add_footer(embed, "Dry-run mode — no one has been kicked. "
                                  "Re-run with dry_run:false to enable the kick button.")
            elif pruner_id == 0:
                add_footer(embed, "⚠️ AUTHORIZED_PRUNER_ID env var isn't set — "
                                  "no one can confirm the kick. Set it in Railway first.")
            elif interaction.user.id != pruner_id:
                # This reply is ephemeral, so only the invoker could ever see a button.
                pruner = guild.get_member(pruner_id)
                name = pruner.display_name if pruner else f"user {pruner_id}"
                add_footer(embed, f"Only {name} can confirm a kick — ask them to run "
                                  f"this command with dry_run:false.")
            else:
                view = PruneConfirmView(bot=self, target_ids=list(kickable_ids),
                                        threshold=th, days=dy)
                add_footer(embed, "Click the kick button twice to confirm. Counts are "
                                  "re-checked at confirm time.")
            await interaction.followup.send(embed=embed, files=files, view=view, ephemeral=True)

        @self.tree.command(name="digest",
                           description="Post the monthly inactivity digest right now (bypasses schedule).")
        @app_commands.default_permissions(manage_guild=True)
        async def digest_cmd(interaction: discord.Interaction):
            if not self.config.digest_channel_id:
                await interaction.response.send_message(
                    "Digest is disabled — set `DIGEST_CHANNEL_ID` in the bot's env vars first.",
                    ephemeral=True)
                return
            await interaction.response.defer(thinking=True, ephemeral=True)
            try:
                channel = await self._post_digest(triggered_by=interaction)
            except Exception as e:
                log.exception("Manual digest failed")
                await interaction.followup.send(f"❌ Digest failed: {e}", ephemeral=True)
                return
            await interaction.followup.send(f"✅ Digest posted to {channel.mention}.", ephemeral=True)

        @self.tree.command(name="sitehealth",
                           description="Check whether the wiki is up right now.")
        # Per-channel cooldown so the command can't be used to hammer the site.
        @app_commands.checks.cooldown(1, 20.0, key=lambda i: i.channel_id)
        async def sitehealth_cmd(interaction: discord.Interaction):
            await interaction.response.defer(thinking=True)
            report = await sitehealth.probe(self.config.site_health_url)
            color, icon = {
                sitehealth.UP: (discord.Color.green(), "🟢"),
                sitehealth.MAINTENANCE: (discord.Color.gold(), "🟡"),
                sitehealth.DOWN: (discord.Color.red(), "🔴"),
            }[report.state]
            embed = discord.Embed(
                title=f"{icon} {self.config.site_health_url.removeprefix('https://')} is {report.state}",
                description=report.reason,
                color=color,
                timestamp=dt.datetime.now(dt.timezone.utc),
            )
            for c in report.checks:
                timing = f"{c.ms} ms" if c.ms is not None else "—"
                embed.add_field(name=f"{'✅' if c.ok else '❌'} {c.path}",
                                value=f"{c.detail} · {timing}", inline=False)
            await interaction.followup.send(embed=embed,
                                            allowed_mentions=discord.AllowedMentions.none())

        @self.tree.error
        async def on_tree_error(interaction: discord.Interaction,
                                error: app_commands.AppCommandError):
            if isinstance(error, app_commands.CommandOnCooldown):
                msg = f"⏳ Slow down — try again in {error.retry_after:.0f} s."
            else:
                log.exception("Command %s failed",
                              interaction.command.name if interaction.command else "?",
                              exc_info=error)
                msg = "❌ Something went wrong running that command."
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)

        @self.tree.command(name="backfill",
                           description="Walk channel history to seed the DB. Safe to re-run — it resumes.")
        @app_commands.describe(days="How many days back to walk (default 180).",
                               channel="Only backfill this channel and its threads "
                                       "(default: every readable channel and thread).")
        @app_commands.default_permissions(manage_guild=True)
        async def backfill_cmd(interaction: discord.Interaction,
                               days: Optional[Days] = None,
                               channel: Optional[Union[discord.TextChannel, discord.ForumChannel,
                                                       discord.VoiceChannel, discord.Thread]] = None):
            if self._backfill_task and not self._backfill_task.done():
                await interaction.response.send_message(
                    "A backfill is already running. Use `/backfill_status` to check progress.",
                    ephemeral=True)
                return
            guild = interaction.guild
            if guild is None:
                await interaction.response.send_message("Server context missing.", ephemeral=True)
                return
            days_back = days if days is not None else 180
            note = ""
            if days_back > self.config.retention_days:
                days_back = self.config.retention_days
                note = (f" (capped at `RETENTION_DAYS`={days_back} — anything older "
                        f"would be purged tonight)")
            where = channel.mention if channel else "every readable channel and thread"
            await interaction.response.send_message(
                f"⏳ Backfill started — {where}, going back {days_back} days{note}. "
                f"Use `/backfill_status` to check progress. This can take several minutes.",
                ephemeral=True)
            self._backfill_task = asyncio.create_task(self._run_backfill(guild, channel, days_back))

        @self.tree.command(name="backfill_status",
                           description="Show progress of the current or last backfill.")
        @app_commands.default_permissions(manage_guild=True)
        async def backfill_status_cmd(interaction: discord.Interaction):
            rows = await asyncio.to_thread(self.storage.all_progress)
            total_rows = await asyncio.to_thread(self.storage.total_rows)
            running = self._backfill_task and not self._backfill_task.done()
            done = sum(1 for r in rows if r["complete"])
            in_progress = len(rows) - done
            lines = [
                f"**Running:** {'yes' if running else 'no'}",
                f"**Channels & threads completed:** {done}/{len(rows)}"
                + (f" ({in_progress} still in progress)" if in_progress else ""),
                f"**Activity rows in DB:** {total_rows:,}",
            ]

            files: list[discord.File] = []
            if rows:
                # Show up to 10 most recently updated in the message body.
                recent = sorted(rows, key=lambda r: r["updated_at"] or "", reverse=True)[:10]
                lines.append("\n**Recently updated channels:**")
                for r in recent:
                    ch = self.get_channel(r["channel_id"])
                    name = f"#{ch.name}" if ch else f"`{r['channel_id']}`"
                    lines.append(f"• {name} — {'✅ done' if r['complete'] else '⏳ in progress'}")
                if len(rows) > 10:
                    # Attach the full list as a text file.
                    out = [f"# Backfill status — {done}/{len(rows)} channels complete\n"]
                    all_sorted = sorted(rows, key=lambda r: (not r["complete"], (r["updated_at"] or "")), reverse=True)
                    for r in all_sorted:
                        ch = self.get_channel(r["channel_id"])
                        name = f"#{ch.name}" if ch else f"channel_id={r['channel_id']}"
                        status = "done" if r["complete"] else "in progress"
                        out.append(f"{status}\t{name}\t(updated {r['updated_at'] or '?'})")
                    files.append(text_file("\n".join(out) + "\n", "backfill_channels.txt"))
                    lines.append(f"\n_Full list of all {len(rows)} channels attached._")

            await interaction.response.send_message(
                "\n".join(lines), files=files, ephemeral=True)

    # -- prune helpers --------------------------------------------------------------

    def _split_kickable(self, guild: discord.Guild,
                        members: list[discord.Member]) -> tuple[list[discord.Member],
                                                                 list[discord.Member]]:
        """Split a member list into (kickable, skipped).

        Discord will refuse to kick:
          - anyone, if the bot lacks Kick Members
          - the server owner
          - members whose highest role is >= the bot's highest role
          - the bot itself (obvious)
        """
        me = guild.me
        if not me.guild_permissions.kick_members:
            return [], list(members)
        my_top = me.top_role
        kickable, skipped = [], []
        for m in members:
            if m.id == guild.owner_id or m.id == me.id or m.top_role >= my_top:
                skipped.append(m)
            else:
                kickable.append(m)
        return kickable, skipped

    async def recheck_prune_targets(self, guild: discord.Guild, target_ids: list[int], *,
                                    threshold: int, days: int) -> tuple[list[discord.Member], int]:
        """Re-validate a preview's targets right before kicking.

        Returns (still_kickable, became_active). Members who left are dropped silently.
        """
        await self._flush_live()
        counts = await asyncio.to_thread(self.storage.message_counts_since, cutoff_day(days))
        still: list[discord.Member] = []
        became_active = 0
        for uid in target_ids:
            member = guild.get_member(uid)
            if member is None:
                continue
            if counts.get(uid, 0) > threshold:
                became_active += 1
                continue
            still.append(member)
        kickable, _ = self._split_kickable(guild, still)
        return kickable, became_active

    async def perform_prune(self, members: list[discord.Member],
                            *, reason: str) -> tuple[int, int, list[str]]:
        """Kick each member, one at a time with a small delay.
        Returns (kicked, failed, error_lines)."""
        kicked = 0
        failed = 0
        errors: list[str] = []
        for member in members:
            try:
                await member.kick(reason=reason[:512])
                kicked += 1
            except discord.NotFound:
                pass  # left on their own in the meantime
            except discord.Forbidden:
                failed += 1
                errors.append(f"{member} ({member.id}) — Forbidden (role too high?)")
            except discord.HTTPException as e:
                failed += 1
                errors.append(f"{member} ({member.id}) — {e}")
            # discord.py handles hard rate limits; this keeps mass kicks gentle.
            await asyncio.sleep(0.5)
        return kicked, failed, errors

    # -- backfill engine ------------------------------------------------------------

    async def _backfill_targets(
        self,
        guild: discord.Guild,
        only: Optional[Union[discord.TextChannel, discord.ForumChannel, discord.VoiceChannel, discord.Thread]],
        cutoff: dt.datetime,
    ) -> list[HistoryChannel]:
        """Everything whose history should be walked: channels plus their threads.

        Live capture counts thread messages, so backfill has to walk threads too,
        including archived ones that still have messages inside the window.
        """
        me = guild.me

        def readable(ch: discord.abc.GuildChannel) -> bool:
            p = ch.permissions_for(me)
            return p.view_channel and p.read_message_history

        if isinstance(only, (discord.Thread, discord.VoiceChannel)):
            return [only]
        if only is not None:
            parents = [only]
            direct: list[HistoryChannel] = [only] if isinstance(only, discord.TextChannel) else []
        else:
            parents = [ch for ch in (*guild.text_channels, *guild.forums) if readable(ch)]
            direct = [ch for ch in (*guild.text_channels, *guild.voice_channels, *guild.stage_channels)
                      if readable(ch)]

        parent_ids = {p.id for p in parents}
        threads: dict[int, discord.Thread] = {}
        try:
            active = await guild.active_threads()
        except discord.HTTPException:
            active = list(guild.threads)
        for t in active:
            if t.parent_id in parent_ids:
                threads[t.id] = t

        for parent in parents:
            archives = [parent.archived_threads(limit=None)]
            if isinstance(parent, discord.TextChannel) and parent.permissions_for(me).manage_threads:
                archives.append(parent.archived_threads(private=True, limit=None))
            for archive in archives:
                try:
                    # Newest-archived first; a thread archived before the cutoff
                    # can't contain messages inside the window.
                    async for t in archive:
                        if t.archive_timestamp < cutoff:
                            break
                        threads[t.id] = t
                except discord.Forbidden:
                    log.warning("Backfill: can't list archived threads in #%s", parent.name)

        return [*direct, *threads.values()]

    async def _run_backfill(self, guild: discord.Guild, only, days_back: int) -> None:
        """Walk each channel's history in reverse (newest→oldest) and count messages.

        Channels are scanned a few at a time in parallel — Discord rate-limits
        history requests per channel, so concurrency is close to a free speedup.
        """
        cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days_back)
        try:
            targets = await self._backfill_targets(guild, only, cutoff)
        except Exception:
            log.exception("Backfill: failed to list channels")
            return
        log.info("Backfill: %d channel(s)/thread(s), cutoff=%s, live_since=%s",
                 len(targets), cutoff.isoformat(), self._live_since.isoformat())

        sem = asyncio.Semaphore(self.config.backfill_concurrency)

        async def scan(ch: HistoryChannel) -> None:
            async with sem:
                try:
                    await self._backfill_channel(ch, cutoff)
                except discord.Forbidden:
                    log.warning("Backfill: no permission for #%s", ch.name)
                except Exception:
                    log.exception("Backfill: error on #%s", ch.name)

        await asyncio.gather(*(scan(ch) for ch in targets))
        log.info("Backfill: finished")

    async def _backfill_channel(self, ch: HistoryChannel, cutoff: dt.datetime) -> None:
        """Count every message in [cutoff, live_since) that isn't counted yet.

        Progress per channel:
          - oldest_reached: oldest message id counted so far; resume just before it.
          - covered_since:  once a scan finishes, every message at/after this time
                            is counted. A later run with an older cutoff extends it.
        """
        cutoff_iso = iso_utc(cutoff)
        # Never walk messages newer than live_since — on_message already counted those.
        before: Union[discord.Object, dt.datetime] = self._live_since
        progress = await asyncio.to_thread(self.storage.get_progress, ch.id)
        if progress:
            covered = progress["covered_since"]
            if progress["complete"] and covered is not None and covered <= cutoff_iso:
                log.info("Backfill: #%s already covers this window — skipping", ch.name)
                return
            if progress["oldest_reached"]:
                before = discord.Object(id=progress["oldest_reached"])
            elif covered is not None:
                before = min(parse_iso_utc(covered), self._live_since)
            elif progress["complete"]:
                # Pre-upgrade row with nothing ingested: we can't tell where its
                # coverage starts, so rescanning could double-count. Leave it.
                log.info("Backfill: #%s complete (legacy) — skipping", ch.name)
                return

        batch: list[tuple[int, int, str, int]] = []
        counted = 0
        newest_seen: Optional[int] = None
        oldest_seen: Optional[int] = None

        log.info("Backfill: scanning #%s (before=%s)", ch.name,
                 before.id if isinstance(before, discord.Object) else before.isoformat())

        try:
            async for msg in ch.history(limit=None, before=before, oldest_first=False):
                if msg.created_at < cutoff:
                    break  # walked past the window; done.
                if msg.author.bot:
                    continue
                if newest_seen is None:
                    newest_seen = msg.id
                oldest_seen = msg.id
                batch.append((msg.author.id, ch.id, utc_day(msg.created_at), 1))
                counted += 1

                # Flush + checkpoint together so a crash never loses or double-counts a batch.
                if len(batch) >= self.config.backfill_batch_size:
                    await asyncio.to_thread(
                        self.storage.flush_backfill, batch,
                        channel_id=ch.id,
                        last_message_id=newest_seen,
                        oldest_reached=oldest_seen,
                        complete=False,
                    )
                    batch = []
        except discord.Forbidden:
            log.warning("Backfill: forbidden mid-scan on #%s", ch.name)
            return

        await asyncio.to_thread(
            self.storage.flush_backfill, batch,
            channel_id=ch.id,
            last_message_id=newest_seen,
            oldest_reached=oldest_seen,
            complete=True,
            covered_since=cutoff_iso,
        )
        log.info("Backfill: #%s done — %d messages ingested", ch.name, counted)


# --------------------------------------------------------------------------------------
# Prune confirmation UI (two-step, locked to AUTHORIZED_PRUNER_ID)
# --------------------------------------------------------------------------------------


class PruneConfirmView(discord.ui.View):
    """First button asks 'are you sure?'; second click actually kicks.

    The view times out after 10 minutes to prevent stale confirm buttons
    from being reused hours later.
    """
    def __init__(self, *, bot: InactivityBot, target_ids: list[int],
                 threshold: int, days: int):
        super().__init__(timeout=600)
        self.bot = bot
        self.target_ids = target_ids
        self.threshold = threshold
        self.days = days
        self._armed = False   # flips to True after first click

    async def _check_authorized(self, interaction: discord.Interaction) -> bool:
        allowed = self.bot.config.authorized_pruner_id
        if allowed == 0 or interaction.user.id != allowed:
            await interaction.response.send_message(
                f"❌ Only <@{allowed}> can confirm this action." if allowed
                else "❌ AUTHORIZED_PRUNER_ID isn't configured.",
                ephemeral=True,
            )
            return False
        return True

    @discord.ui.button(label="🔨 Kick these members", style=discord.ButtonStyle.danger)
    async def step1(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._check_authorized(interaction):
            return
        if not self._armed:
            self._armed = True
            button.label = f"⚠️ CONFIRM — kick up to {len(self.target_ids)} members"
            await interaction.response.edit_message(
                content=f"⚠️ Click the button **again** to confirm — this will kick up to "
                        f"**{len(self.target_ids)} members** and cannot be undone.",
                view=self,
            )
            return

        # Second click — actually kick.
        # Disable the button immediately so it can't fire twice while we work.
        for child in self.children:
            child.disabled = True
        self.stop()
        await interaction.response.edit_message(
            content="⏳ Re-checking activity and kicking… this may take a few minutes.",
            view=self,
        )
        guild = interaction.guild
        if guild is None:
            return
        members, became_active = await self.bot.recheck_prune_targets(
            guild, self.target_ids, threshold=self.threshold, days=self.days,
        )
        reason = (f"Inactivity prune by {interaction.user} — "
                  f"≤{self.threshold} msgs in last {self.days}d")
        kicked, failed, errors = await self.bot.perform_prune(members, reason=reason)

        summary = f"✅ Kicked **{kicked}** members. Failed: {failed}."
        if became_active:
            summary += f" Spared **{became_active}** who became active since the preview."
        errors_text = "# Prune errors\n" + "".join(line + "\n" for line in errors) if errors else None

        def attachments() -> list[discord.File]:
            # discord.File can't be re-sent, so build fresh ones per attempt.
            return [text_file(errors_text, "prune_errors.txt")] if errors_text else []

        try:
            await interaction.followup.send(summary, files=attachments(), ephemeral=True)
        except discord.HTTPException:
            # Interaction tokens expire after 15 minutes; very large prunes outlive them.
            try:
                await interaction.user.send(summary, files=attachments())
            except discord.HTTPException:
                log.warning("Prune summary could not be delivered: %s", summary)

        # Also post to the digest channel as a public audit trail if configured.
        if self.bot.config.digest_channel_id:
            try:
                ch = await self.bot._digest_channel()
                await ch.send(
                    f"🔨 **Inactivity prune** by {interaction.user.mention} — "
                    f"kicked **{kicked}**, failed **{failed}** "
                    f"(criteria: ≤{self.threshold} msgs in last {self.days}d).",
                    allowed_mentions=discord.AllowedMentions.none(),
                )
            except (RuntimeError, discord.HTTPException):
                log.warning("Prune audit post failed", exc_info=True)


# --------------------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------------------


def main() -> None:
    config = Config.from_env()
    bot = InactivityBot(config)
    bot.run(config.discord_token, log_handler=None)


if __name__ == "__main__":
    main()
