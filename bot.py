"""Discord bot that tracks per-member message activity and answers 'who is inactive?'.

Hybrid design:
  - Live capture (`on_message`) increments a per-day counter on every new message.
  - `/backfill` walks channel history to seed those counters with historical data.
  - `/inactive` queries the counters for anyone below a threshold in a time window.

Storage is SQLite (see storage.py). On Railway make sure to attach a Volume
mounted at /data and set DB_PATH=/data/activity.db, otherwise the DB is wiped
on every redeploy and you'd have to re-backfill.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import io
import logging
from pathlib import Path
from typing import Optional

import discord
from discord import app_commands
from discord.ext import tasks

from config import Config
from storage import Storage

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("inactivity-bot")


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------


def utc_day(ts: dt.datetime) -> str:
    """YYYY-MM-DD in UTC — our storage day key."""
    return ts.astimezone(dt.timezone.utc).strftime("%Y-%m-%d")


def cutoff_day(days_back: int) -> str:
    return utc_day(dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days_back))


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
        self.storage = Storage(Path(config.db_path))
        self._backfill_task: Optional[asyncio.Task] = None
        self._register_commands()

    # -- lifecycle ------------------------------------------------------------------

    async def setup_hook(self) -> None:
        guild = discord.Object(id=self.config.guild_id)
        self.tree.copy_global_to(guild=guild)
        await self.tree.sync(guild=guild)
        self.nightly_maintenance.start()
        if self.config.digest_channel_id:
            self.monthly_digest.start()

    async def on_ready(self) -> None:
        log.info("Logged in as %s — tracking guild %s (DB=%s, %d rows)",
                 self.user, self.config.guild_id, self.config.db_path,
                 self.storage.total_rows())

    # -- live capture ---------------------------------------------------------------

    async def on_message(self, message: discord.Message) -> None:
        if message.guild is None or message.guild.id != self.config.guild_id:
            return
        if message.author.bot:
            return
        # Threads: message.channel is the thread; message.channel.parent_id is the
        # parent forum/text channel. Storing the thread id groups activity by thread,
        # which is what most people want. If you'd rather roll thread activity up to
        # the parent, replace channel.id with getattr(channel, "parent_id", channel.id).
        await asyncio.to_thread(
            self.storage.bump,
            user_id=message.author.id,
            channel_id=message.channel.id,
            day=utc_day(message.created_at),
        )

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
        """Fires hourly; posts a digest at the configured day/hour, once per month."""
        now = dt.datetime.now(dt.timezone.utc)
        if now.day != self.config.digest_day_of_month or now.hour != self.config.digest_hour_utc:
            return
        # Dedup: don't post twice if the bot restarts within the hour.
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
            log.exception("Monthly digest failed")

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
        counts = await asyncio.to_thread(self.storage.message_counts_since, cutoff_day(days))
        now = dt.datetime.now(dt.timezone.utc)
        out: list[tuple[discord.Member, int]] = []
        for member in guild.members:
            if member.bot:
                continue
            if only_role and only_role not in member.roles:
                continue
            if exclude_role and exclude_role in member.roles:
                continue
            if member.joined_at is not None and min_join_age_days > 0:
                if (now - member.joined_at).days < min_join_age_days:
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

        if not inactive:
            embed = discord.Embed(
                title=title,
                description=desc + "\n\n_No matches — everyone is above the threshold._",
                color=discord.Color.green(),
            )
            return embed, []

        lines_head = []
        for member, count in inactive[:40]:
            joined = member.joined_at.strftime("%Y-%m-%d") if member.joined_at else "?"
            lines_head.append(f"• {member.mention} — **{count}** msg · joined {joined}")

        embed = discord.Embed(
            title=title,
            description=desc + "\n\n" + "\n".join(lines_head),
            color=discord.Color.orange(),
        )

        files: list[discord.File] = []
        if len(inactive) > 40:
            buf = io.StringIO()
            buf.write(f"# Inactive members — ≤{threshold} msgs in last {days}d\n")
            for member, count in inactive:
                joined = member.joined_at.strftime("%Y-%m-%d") if member.joined_at else "?"
                buf.write(f"{member} ({member.id})\t{count} msg\tjoined {joined}\n")
            files.append(discord.File(io.BytesIO(buf.getvalue().encode()),
                                      filename="inactive_members.txt"))
            embed.set_footer(text=f"Showing first 40 of {len(inactive)} — full list attached.")
        return embed, files

    async def _post_digest(self, *, triggered_by: Optional[discord.Interaction]) -> None:
        """Compute + post the monthly digest to the configured channel."""
        guild = self.get_guild(self.config.guild_id)
        if guild is None:
            log.warning("Digest: guild %s not found", self.config.guild_id)
            return
        channel = self.get_channel(self.config.digest_channel_id)
        if not isinstance(channel, discord.TextChannel):
            log.warning("Digest: channel %s is not a text channel", self.config.digest_channel_id)
            return

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
            embed.set_footer(text=(embed.footer.text + " · " if embed.footer.text else "")
                             + f"Triggered manually by {triggered_by.user}")
        await channel.send(embed=embed, files=files)

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
            threshold: Optional[int] = None,
            days: Optional[int] = None,
            only_role: Optional[discord.Role] = None,
            exclude_role: Optional[discord.Role] = None,
            min_join_age_days: Optional[int] = None,
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
                               days: Optional[int] = None):
            await interaction.response.defer(thinking=True, ephemeral=True)
            dy = days if days is not None else self.config.default_days
            stats = await asyncio.to_thread(self.storage.per_user_stats, user.id, cutoff_day(dy))

            top_ch = "-"
            if stats["top_channel_id"]:
                ch = interaction.guild.get_channel(stats["top_channel_id"]) if interaction.guild else None
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
            threshold="Message count under which people will be removed.",
            days_lookback="How far back the message count is measured.",
        )
        @app_commands.default_permissions(manage_guild=True)
        async def prune_announce_cmd(
            interaction: discord.Interaction,
            channel: discord.TextChannel,
            days_until_prune: int = 7,
            threshold: Optional[int] = None,
            days_lookback: Optional[int] = None,
        ):
            th = threshold if threshold is not None else self.config.default_threshold
            dy = days_lookback if days_lookback is not None else self.config.default_days
            deadline = dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=days_until_prune)
            # Discord dynamic timestamp — shows in each viewer's local timezone.
            deadline_ts = int(deadline.timestamp())

            body = (
                f"@everyone\n\n"
                f"🚨 **Inactivity cleanup on <t:{deadline_ts}:F>** (<t:{deadline_ts}:R>)\n\n"
                f"Members who have posted **fewer than {th} messages in the last {dy} days** "
                f"will be automatically removed from the server on that date.\n\n"
                f"**To stay:** post something in any channel before the deadline. "
                f"Even a single message keeps you in.\n\n"
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
                f"Run `/prune_inactive threshold:{th} days:{dy}` after {deadline:%Y-%m-%d %H:%M UTC} "
                f"to actually kick.",
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
            threshold: Optional[int] = None,
            days: Optional[int] = None,
            only_role: Optional[discord.Role] = None,
            exclude_role: Optional[discord.Role] = None,
            min_join_age_days: Optional[int] = None,
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

            inactive = await self._compute_inactive(
                guild,
                threshold=th, days=dy,
                only_role=only_role, exclude_role=exclude_role,
                min_join_age_days=min_age,
            )

            # Filter out members the bot legally can't kick, so the preview is honest.
            kickable, skipped = self._split_kickable(guild, [m for m, _ in inactive])
            skipped_note = ""
            if skipped:
                skipped_note = (
                    f"\n\n⚠️ **{len(skipped)} members can't be kicked** "
                    f"(higher role than the bot, or server owner) — they're excluded from the count."
                )

            embed, files = self._build_inactive_embed(
                [(m, c) for m, c in inactive if m in set(kickable)],
                threshold=th, days=dy,
                only_role=only_role, exclude_role=exclude_role, min_join_age_days=min_age,
                title_prefix="🔨 Prune preview" if not dry_run else "🧪 Prune dry-run",
            )
            embed.description = (embed.description or "") + skipped_note

            view: Optional[discord.ui.View] = None
            if dry_run or not kickable:
                embed.set_footer(text="Dry-run mode — no one has been kicked. "
                                 "Re-run with dry_run:false to enable the kick button.")
            else:
                if self.config.authorized_pruner_id == 0:
                    embed.set_footer(text="⚠️ AUTHORIZED_PRUNER_ID env var isn't set — "
                                     "no one can confirm the kick. Set it in Railway first.")
                else:
                    view = PruneConfirmView(bot=self, target_ids=[m.id for m in kickable],
                                            threshold=th, days=dy)
                    embed.set_footer(
                        text=f"Only <@{self.config.authorized_pruner_id}> can confirm this kick."
                    )
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
                await self._post_digest(triggered_by=interaction)
                ch = self.get_channel(self.config.digest_channel_id)
                where = ch.mention if isinstance(ch, discord.TextChannel) else f"channel `{self.config.digest_channel_id}`"
                await interaction.followup.send(f"✅ Digest posted to {where}.", ephemeral=True)
            except Exception as e:
                log.exception("Manual digest failed")
                await interaction.followup.send(f"Failed: {e}", ephemeral=True)

        @self.tree.command(name="backfill",
                           description="Walk channel history to seed the DB. Safe to re-run — it resumes.")
        @app_commands.describe(days="How many days back to walk (default 180).",
                               channel="Only backfill this channel (default: all readable text channels).")
        @app_commands.default_permissions(manage_guild=True)
        async def backfill_cmd(interaction: discord.Interaction,
                               days: Optional[int] = None,
                               channel: Optional[discord.TextChannel] = None):
            if self._backfill_task and not self._backfill_task.done():
                await interaction.response.send_message(
                    "A backfill is already running. Use `/backfill_status` to check progress.",
                    ephemeral=True)
                return
            days_back = days if days is not None else 180
            channels = [channel] if channel else self._readable_text_channels(interaction.guild)
            await interaction.response.send_message(
                f"⏳ Backfill started — {len(channels)} channel(s), going back {days_back} days. "
                f"Use `/backfill_status` to check progress. This can take several minutes.",
                ephemeral=True)
            self._backfill_task = asyncio.create_task(self._run_backfill(channels, days_back))

        @self.tree.command(name="backfill_status",
                           description="Show progress of the current or last backfill.")
        @app_commands.default_permissions(manage_guild=True)
        async def backfill_status_cmd(interaction: discord.Interaction):
            rows = await asyncio.to_thread(self.storage.all_progress)
            running = self._backfill_task and not self._backfill_task.done()
            done = sum(1 for r in rows if r["complete"])
            in_progress = sum(1 for r in rows if not r["complete"])
            total_rows = self.storage.total_rows()
            lines = [
                f"**Running:** {'yes' if running else 'no'}",
                f"**Channels completed:** {done}/{len(rows)}"
                + (f" ({in_progress} still in progress)" if in_progress else ""),
                f"**Activity rows in DB:** {total_rows:,}",
            ]

            files: list[discord.File] = []
            if rows:
                # Show up to 10 most recently updated in the embed body.
                recent = sorted(rows, key=lambda r: r["updated_at"] or "", reverse=True)[:10]
                lines.append("\n**Recently updated channels:**")
                for r in recent:
                    ch = self.get_channel(r["channel_id"])
                    name = f"#{ch.name}" if ch else f"`{r['channel_id']}`"
                    lines.append(f"• {name} — {'✅ done' if r['complete'] else '⏳ in progress'}")
                if len(rows) > 10:
                    # Attach the full list as a text file.
                    buf = io.StringIO()
                    buf.write(f"# Backfill status — {done}/{len(rows)} channels complete\n\n")
                    all_sorted = sorted(rows, key=lambda r: (not r["complete"], (r["updated_at"] or "")), reverse=True)
                    for r in all_sorted:
                        ch = self.get_channel(r["channel_id"])
                        name = f"#{ch.name}" if ch else f"channel_id={r['channel_id']}"
                        status = "done" if r["complete"] else "in progress"
                        buf.write(f"{status}\t{name}\t(updated {r['updated_at'] or '?'})\n")
                    files.append(discord.File(io.BytesIO(buf.getvalue().encode()),
                                              filename="backfill_channels.txt"))
                    lines.append(f"\n_Full list of all {len(rows)} channels attached._")

            await interaction.response.send_message(
                "\n".join(lines), files=files, ephemeral=True)

    # -- backfill engine ------------------------------------------------------------

    def _readable_text_channels(self, guild: Optional[discord.Guild]) -> list[discord.TextChannel]:
        if guild is None:
            return []
        me = guild.me
        out = []
        for ch in guild.text_channels:
            perms = ch.permissions_for(me)
            if perms.view_channel and perms.read_message_history:
                out.append(ch)
        return out

    # -- prune helpers --------------------------------------------------------------

    def _split_kickable(self, guild: discord.Guild,
                        members: list[discord.Member]) -> tuple[list[discord.Member],
                                                                 list[discord.Member]]:
        """Split a member list into (kickable, skipped).

        Discord will refuse to kick:
          - the server owner
          - members whose highest role is >= the bot's highest role
          - the bot itself (obvious)
        """
        me = guild.me
        my_top = me.top_role if me else None
        kickable, skipped = [], []
        for m in members:
            if m.id == guild.owner_id:
                skipped.append(m); continue
            if me and m.id == me.id:
                skipped.append(m); continue
            if my_top and m.top_role >= my_top:
                skipped.append(m); continue
            kickable.append(m)
        return kickable, skipped

    async def perform_prune(self, guild: discord.Guild, target_ids: list[int],
                            *, reason: str) -> tuple[int, int, list[str]]:
        """Kick each target, one at a time with a small delay.
        Returns (kicked, failed, error_lines)."""
        kicked = 0
        failed = 0
        errors: list[str] = []
        for uid in target_ids:
            member = guild.get_member(uid)
            if member is None:
                # Left between preview and confirm — nothing to do.
                continue
            try:
                await member.kick(reason=reason[:512])
                kicked += 1
            except discord.Forbidden:
                failed += 1
                errors.append(f"{member} ({uid}) — Forbidden (role too high?)")
            except discord.HTTPException as e:
                failed += 1
                errors.append(f"{member} ({uid}) — {e}")
            # Pace ourselves so Discord doesn't rate-limit us; ~2/s is safe.
            await asyncio.sleep(0.5)
        return kicked, failed, errors

    async def _run_backfill(self, channels: list[discord.TextChannel], days_back: int) -> None:
        """Walk each channel's history in reverse (newest→oldest) and count messages.

        Resumability: for each channel we track (last_message_id, oldest_reached).
        On restart, if we haven't reached the cutoff yet, we resume from oldest_reached.
        """
        cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days_back)
        log.info("Backfill: %d channel(s), cutoff=%s", len(channels), cutoff.isoformat())

        for ch in channels:
            try:
                await self._backfill_channel(ch, cutoff)
            except discord.Forbidden:
                log.warning("Backfill: no permission for #%s", ch.name)
            except Exception:
                log.exception("Backfill: error on #%s", ch.name)
        log.info("Backfill: finished")

    async def _backfill_channel(self, ch: discord.TextChannel, cutoff: dt.datetime) -> None:
        progress = await asyncio.to_thread(self.storage.get_progress, ch.id)
        # Resume: if oldest_reached is set and older than cutoff, we're already done.
        if progress:
            _, oldest, complete = progress
            if complete:
                log.info("Backfill: #%s already complete — skipping", ch.name)
                return
            # Resume from `oldest` (before that snowflake) if we have one.
            before = discord.Object(id=oldest) if oldest else None
        else:
            before = None

        batch: list[tuple[int, int, str, int]] = []
        counted = 0
        newest_seen: Optional[int] = None
        oldest_seen: Optional[int] = None

        log.info("Backfill: scanning #%s (before=%s)", ch.name, before.id if before else "now")

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

                # Flush + checkpoint periodically so a crash doesn't lose everything.
                if len(batch) >= self.config.backfill_batch_size:
                    await asyncio.to_thread(self.storage.bulk_bump, batch)
                    await asyncio.to_thread(
                        self.storage.save_progress,
                        channel_id=ch.id,
                        last_message_id=newest_seen,
                        oldest_reached=oldest_seen,
                        complete=False,
                    )
                    batch = []
        except discord.Forbidden:
            log.warning("Backfill: forbidden mid-scan on #%s", ch.name)
            return

        if batch:
            await asyncio.to_thread(self.storage.bulk_bump, batch)

        await asyncio.to_thread(
            self.storage.save_progress,
            channel_id=ch.id,
            last_message_id=newest_seen,
            oldest_reached=oldest_seen,
            complete=True,
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

    @discord.ui.button(label="🔨 Kick these members", style=discord.ButtonStyle.danger,
                       custom_id="prune:step1")
    async def step1(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._check_authorized(interaction):
            return
        if not self._armed:
            self._armed = True
            button.label = f"⚠️ CONFIRM — kick {len(self.target_ids)} members"
            button.style = discord.ButtonStyle.danger
            await interaction.response.edit_message(
                content=f"⚠️ Click the button **again** to confirm — this will kick "
                        f"**{len(self.target_ids)} members** and cannot be undone.",
                view=self,
            )
            return

        # Second click — actually kick.
        # Disable the button immediately so it can't fire twice while we work.
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(
            content=f"⏳ Kicking {len(self.target_ids)} members… this may take a minute.",
            view=self,
        )
        guild = interaction.guild
        if guild is None:
            return
        reason = (f"Inactivity prune by {interaction.user} — "
                  f"≤{self.threshold} msgs in last {self.days}d")
        kicked, failed, errors = await self.bot.perform_prune(
            guild, self.target_ids, reason=reason,
        )
        summary = f"✅ Kicked **{kicked}** members. Failed: {failed}."
        files: list[discord.File] = []
        if errors:
            buf = io.StringIO()
            buf.write("# Prune errors\n")
            for line in errors:
                buf.write(line + "\n")
            files.append(discord.File(io.BytesIO(buf.getvalue().encode()),
                                      filename="prune_errors.txt"))
        try:
            await interaction.followup.send(summary, files=files, ephemeral=True)
        except discord.HTTPException:
            pass
        # Also post to the digest channel as a public audit trail if configured.
        if self.bot.config.digest_channel_id:
            ch = self.bot.get_channel(self.bot.config.digest_channel_id)
            if isinstance(ch, discord.TextChannel):
                try:
                    await ch.send(
                        f"🔨 **Inactivity prune** by {interaction.user.mention} — "
                        f"kicked **{kicked}**, failed **{failed}** "
                        f"(criteria: ≤{self.threshold} msgs in last {self.days}d)."
                    )
                except discord.HTTPException:
                    pass


# --------------------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------------------


def main() -> None:
    config = Config.from_env()
    bot = InactivityBot(config)
    bot.run(config.discord_token, log_handler=None)


if __name__ == "__main__":
    main()
