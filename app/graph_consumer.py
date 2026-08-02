"""
Graph consumer.

Consumes `following-pages`. For every (source_user -> discovered_account)
pair:
  1. Upsert the discovered account into `users` (creating it if new).
  2. Create the edge source -> discovered in `edges`.
  3. If the account is new, bump discovery_score, do a *cheap* follower-count
     lookup (HypeAuditor's public suggest endpoint — see app/enrichment.py),
     and enqueue it in crawl_queue (queue_type=discovery) ONLY if it clears
     the follower threshold.

This intentionally does NOT call Instagram's own resolve_user() here — that
endpoint doesn't return follower counts on the following-list response
anyway (see the caveat in app/scraper.py), so hitting IG per-discovered-
account just to check a threshold would be paying your scarce, single-
session IG rate-limit budget for something a public, unauthenticated,
much-more-parallelizable endpoint can answer instead.

We deliberately leave insta_id / is_verified / profile_pic unresolved here.
Those only matter once an account is actually about to be crawled, and
worker.py already does a real IG resolve_user() at that point (needed to
get the numeric insta_id for pagination) — see the follow-up threshold
check added there, which is the actual gate: a HypeAuditor number here can
drift from Instagram's live number by the time the job runs, and seed rows
skip this consumer entirely, so worker.py's check is the one that can't be
bypassed.

Run: python -m app.graph_consumer
"""

import asyncio
import logging

import httpx
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.config import settings
from app.db import CrawlQueue, Edge, SessionLocal, User
from app.enrichment import lookup_follower_count
from app.kafka_client import make_consumer
from app.priority import compute_priority

logging.basicConfig(level=logging.INFO, format="%(asctime)s [graph-consumer] %(message)s")
log = logging.getLogger(__name__)


async def process_page(http_client: httpx.AsyncClient, session, page: dict):
    source_user_id = page["source_user_id"]
    for acc in page["accounts"]:
        username = acc["username"]

        # Look up first; if new, always insert a row into `users` regardless
        # of follower count — we want the full set of discovered accounts on
        # record (e.g. in case one crosses the threshold later on a re-check).
        # Only the EDGE is gated on the threshold, since edges is what would
        # actually balloon toward 1B+ rows if we recorded every following
        # relationship instead of just the ones to accounts worth crawling.
        user = (await session.execute(select(User).where(User.username == username))).scalar_one_or_none()

        if user is None:
            try:
                enrichment = await lookup_follower_count(http_client, username)
            except httpx.HTTPError as e:
                log.warning("hypeauditor lookup failed for %s: %s", username, e)
                enrichment = None

            stmt = (
                pg_insert(User)
                .values(
                    insta_id=enrichment["insta_id"],
                    username=username,
                    follower_count=enrichment["follower_count"] if enrichment else None,
                    is_verified=enrichment["is_verified"] if enrichment else None,
                )
                .on_conflict_do_nothing(index_elements=["username"])
                .returning(User)
            )
            result = await session.execute(stmt)
            await session.commit()
            user = result.scalar_one_or_none()
            if user is None:
                # lost a race to another consumer instance inserting the same user
                user = (await session.execute(select(User).where(User.username == username))).scalar_one()

        if (user.follower_count or 0) < settings.follower_threshold:
            continue  # row stays in `users`, but no edge, no discovery_score, no queueing

        # Threshold cleared. Insert the edge and check whether it was
        # actually new. Kafka gives at-least-once delivery — if this
        # consumer crashes after committing DB writes but before committing
        # the Kafka offset, this exact page gets redelivered on restart.
        # Gating discovery_score on "did the edge insert actually happen"
        # (not "does the user already exist") is what makes that redelivery
        # a safe no-op instead of a silent double-count.
        edge_stmt = (
            pg_insert(Edge)
            .values(from_user_id=source_user_id, to_user_id=user.id)
            .on_conflict_do_nothing(index_elements=["from_user_id", "to_user_id"])
        )
        edge_result = await session.execute(edge_stmt)
        await session.commit()
        edge_is_new = edge_result.rowcount > 0

        if not edge_is_new:
            continue  # already recorded this exact relationship, nothing left to do

        user.discovery_score = (user.discovery_score or 0) + 1
        await session.merge(user)
        await session.commit()

        if user.crawl_state is not None and user.crawl_state != "never_crawled":
            continue  # already crawled or in flight, don't re-enqueue

        existing_queue = (
            await session.execute(
                select(CrawlQueue).where(
                    CrawlQueue.user_id == user.id, CrawlQueue.status.in_(["pending", "processing"])
                )
            )
        ).scalar_one_or_none()
        if existing_queue:
            continue

        priority = compute_priority(user.follower_count, user.seed_association_score, user.discovery_score)
        session.add(
            CrawlQueue(
                user_id=user.id,
                priority=priority,
                source_type="discovered",
                queue_type="discovery",
            )
        )
        user.crawl_state = "queued"
        await session.commit()


async def run():
    consumer = make_consumer(settings.topic_following_pages, group_id="graph-consumers")
    await consumer.start()
    async with httpx.AsyncClient(timeout=10.0) as http_client:
        try:
            async for msg in consumer:
                page = msg.value
                async with SessionLocal() as session:
                    await process_page(http_client, session, page)
                await consumer.commit()
                log.info(
                    "processed page %d from %s (%d accounts)",
                    page["page_num"], page["source_username"], len(page["accounts"]),
                )
        finally:
            await consumer.stop()


if __name__ == "__main__":
    asyncio.run(run())