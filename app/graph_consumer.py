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
from sqlalchemy import or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError

from app.config import settings
from app.db import CrawlQueue, Edge, SessionLocal, User
from app.enrichment import lookup_follower_count
from app.kafka_client import make_consumer
from app.priority import compute_priority
from app.proxy_pool import ProxyPool

logging.basicConfig(level=logging.INFO, format="%(asctime)s [graph-consumer] %(message)s")
log = logging.getLogger(__name__)


async def process_page(http_client: httpx.AsyncClient, proxy_pool: ProxyPool, session, page: dict):
    source_user_id = page["source_user_id"]
    for acc in page["accounts"]:
        username = acc["username"]
        insta_id = acc.get("insta_id")
        is_private = acc["is_private"]
        profile_pic = acc["profile_pic"]

        # Look up by insta_id first if available, or by username
        if insta_id:
            user = (await session.execute(
                select(User).where(or_(User.insta_id == insta_id, User.username == username))
            )).scalar_one_or_none()
        else:
            user = (await session.execute(
                select(User).where(User.username == username)
            )).scalar_one_or_none()

        enrichment = None
        exhausted_retries = False
        crawl_state = user.crawl_state if user else None

        if user is None or crawl_state is None or crawl_state == "enrichment_failed":
            try:
                if not is_private:
                    enrichment, exhausted_retries = await lookup_follower_count(http_client, proxy_pool, username)
                    if enrichment:
                        follower_count = enrichment.get("follower_count") or 0
                        if follower_count < settings.follower_threshold:
                            crawl_state = "skipped_below_threshold"
                        else:
                            crawl_state = "never_crawled"
            except Exception as e:
                log.warning("Enrichment lookup failed for %s: %s", username, e)
                enrichment = None
                crawl_state = "enrichment_failed"

            if enrichment is None and not is_private:
                if exhausted_retries:
                    crawl_state = "enrichment_failed"
                    log.warning("Request exhausted for %s", username)
                else:
                    crawl_state = "not_present_in_enrichment_list"
                    log.warning("No enrichment match for %s", username)
            elif enrichment is None and is_private:
                log.info("Private account %s, skipping enrichment", username)
                crawl_state = "never_crawled"
                enrichment = None

            if user is None:
                # Target insta_id constraint if present, fallback to username
                effective_insta_id = insta_id or (enrichment.get("insta_id") if enrichment else None)
                effective_name = acc.get("name") or (enrichment.get("name") if enrichment else None)
                effective_is_verified = acc.get("is_verified", False) or (enrichment.get("is_verified") if enrichment else False)
                effective_profile_pic = profile_pic or (enrichment.get("profile_pic") if enrichment else None)
                effective_following_count = enrichment.get("following_count") if enrichment else None
                effective_country = enrichment.get("country") if enrichment else None
                effective_category = enrichment.get("category") if enrichment else None

                index_elem = ["insta_id"] if effective_insta_id else ["username"]
                stmt = (
                    pg_insert(User)
                    .values(
                        insta_id=effective_insta_id,
                        username=username,
                        follower_count=enrichment["follower_count"] if enrichment else None,
                        following_count=effective_following_count,
                        is_verified=effective_is_verified,
                        name=effective_name,
                        is_private=is_private,
                        country=effective_country,
                        category=effective_category,
                        discovered_by=source_user_id,
                        crawl_state=crawl_state,
                        profile_pic=effective_profile_pic,
                    )
                    .on_conflict_do_nothing(index_elements=index_elem)
                    .returning(User)
                )
                try:
                    result = await session.execute(stmt)
                    await session.commit()
                    user = result.scalar_one_or_none()
                except IntegrityError:
                    await session.rollback()
                    user = None

                if user is None:
                    # Race condition with parallel worker or alternate unique constraint
                    if effective_insta_id:
                        user = (await session.execute(
                            select(User).where(or_(User.insta_id == effective_insta_id, User.username == username))
                        )).scalar_one_or_none()
                    else:
                        user = (await session.execute(
                            select(User).where(User.username == username)
                        )).scalar_one_or_none()
            else:
                # Existing user row updating state
                if enrichment:
                    if user.follower_count is None and enrichment.get("follower_count") is not None:
                        user.follower_count = enrichment["follower_count"]
                    if user.following_count is None and enrichment.get("following_count") is not None:
                        user.following_count = enrichment["following_count"]
                    if user.country is None and enrichment.get("country") is not None:
                        user.country = enrichment["country"]
                    if user.category is None and enrichment.get("category") is not None:
                        user.category = enrichment["category"]
                    if user.profile_pic is None and enrichment.get("profile_pic") is not None:
                        user.profile_pic = enrichment["profile_pic"]
                    if user.name is None and enrichment.get("name") is not None:
                        user.name = enrichment["name"]
                    if user.is_verified is None and enrichment.get("is_verified") is not None:
                        user.is_verified = enrichment["is_verified"]
                if user.username != username:
                    user.username = username
                if user.insta_id is None and insta_id:
                    user.insta_id = insta_id
                user.crawl_state = crawl_state
                await session.merge(user)
                await session.commit()

        if user is None:
            log.warning("Could not resolve or insert user %s (insta_id: %s), skipping", username, insta_id)
            continue

        


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

        log.info("user already existed %s, adding discovery score", user.username)

        user.discovery_score = (user.discovery_score or 0) + 1
        await session.merge(user)
        await session.commit()


        if (user.follower_count or 0) < settings.follower_threshold:
            if user.crawl_state == "never_crawled":
                user.crawl_state = "skipped_below_threshold"
                await session.merge(user)
                await session.commit()
            continue  # row stays in `users`, but no queueing, only discovery_score
        
        if user.crawl_state is not None and user.crawl_state != "never_crawled":
            continue  # already crawled or in flight, don't re-enqueue

        # existing_queue = (
        #     await session.execute(
        #         select(CrawlQueue).where(
        #             CrawlQueue.user_id == user.id, CrawlQueue.status.in_(["pending", "processing"])
        #         )
        #     )
        # ).scalar_one_or_none()
        # if existing_queue:
        #     continue

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
        proxy_pool = ProxyPool()
        await proxy_pool.refresh(http_client) 
        try:
            async for msg in consumer:
                page = msg.value
                async with SessionLocal() as session:
                    await process_page(http_client, proxy_pool ,session, page)
                await consumer.commit()
                log.info(
                    "processed chunck %d of page %d from %s (%d accounts)",
                    page["chunk_index"], page["instagram_page"], page["source_username"], len(page["accounts"]),
                )
        finally:
            await consumer.stop()


if __name__ == "__main__":
    asyncio.run(run())