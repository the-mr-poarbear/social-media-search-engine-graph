"""
Dedicated Priority Enrichment Worker with Concurrent SKIP LOCKED Claiming.

Continuously claims and enriches users at 'pending_enrichment' or 'enrichment_failed'.
Uses PostgreSQL `FOR UPDATE SKIP LOCKED` so multiple parallel workers can run concurrently
without duplicate lookups or lock contention.

Prioritizes users with the highest `discovery_score` (most connected in the graph)
and breaks ties by discovery order (`id ASC`).

Routes enriched accounts:
  - follower_count >= threshold -> enqueues in `crawl_queue` and sets crawl_state='queued'
  - follower_count < threshold  -> sets crawl_state='skipped_below_threshold'
  - not found in API            -> sets crawl_state='not_present_in_enrichment_list'
  - transient proxy/net error   -> sets crawl_state='enrichment_failed' (auto-retried)

Run: python -m app.enrichment_worker
"""

import asyncio
from datetime import datetime, timedelta, timezone
import logging

import httpx
from sqlalchemy import select, update

from app.config import settings
from app.db import CrawlQueue, SessionLocal, User
from app.enrichment import lookup_follower_count
from app.priority import compute_priority
from app.proxy_pool import ProxyPool

logging.basicConfig(level=logging.INFO, format="%(asctime)s [enrichment-worker] %(message)s")
log = logging.getLogger(__name__)


async def _claim_batch(session, limit: int = 5) -> list[int]:
    """Atomically claims up to `limit` pending accounts using FOR UPDATE SKIP LOCKED.

    Also recovers any stalled 'enriching' claims from crashed workers (> 5 minutes old).
    """
    now = datetime.now(timezone.utc)
    stale_cutoff = now - timedelta(minutes=5)

    # 1. Recover stalled claims from crashed workers
    await session.execute(
        update(User)
        .where(User.crawl_state == "enriching", User.updated_at < stale_cutoff)
        .values(crawl_state="pending_enrichment")
    )

    # 2. Select and lock next batch using SKIP LOCKED
    users = (
        await session.execute(
            select(User)
            .where(User.crawl_state.in_(["pending_enrichment", "enrichment_failed"]))
            .order_by(User.discovery_score.desc(), User.id.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
    ).scalars().all()

    if not users:
        await session.commit()
        return []

    user_ids = [u.id for u in users]

    # 3. Mark claimed rows as 'enriching'
    await session.execute(
        update(User)
        .where(User.id.in_(user_ids))
        .values(crawl_state="enriching")
    )
    await session.commit()
    return user_ids


async def enrich_single_user(http_client: httpx.AsyncClient, proxy_pool: ProxyPool, user_id: int):
    async with SessionLocal() as session:
        try:
            user = await session.get(User, user_id)
            if not user or user.crawl_state not in ("enriching", "pending_enrichment", "enrichment_failed"):
                return

            username = user.username
            is_private = user.is_private

            # Private accounts cannot have their following crawled; skip enrichment
            if is_private:
                user.crawl_state = "never_crawled"
                await session.commit()
                log.info("Private account %s, skipping enrichment", username)
                return

            try:
                enrichment, exhausted_retries = await lookup_follower_count(http_client, proxy_pool, username)
            except Exception as e:
                log.warning("Lookup exception for %s: %s", username, e)
                user.crawl_state = "enrichment_failed"
                await session.commit()
                return

            # Lookup failure handling
            if not enrichment:
                if exhausted_retries:
                    log.warning("Requests exhausted for %s, keeping enrichment_failed", username)
                    user.crawl_state = "enrichment_failed"
                else:
                    log.info("User %s not present in enrichment database", username)
                    user.crawl_state = "not_present_in_enrichment_list"
                await session.commit()
                return

            # Populate metadata from enrichment
            user.follower_count = enrichment.get("follower_count")
            if enrichment.get("following_count") is not None:
                user.following_count = enrichment["following_count"]
            if enrichment.get("country") is not None:
                user.country = enrichment["country"]
            if enrichment.get("category") is not None:
                user.category = enrichment["category"]
            if enrichment.get("profile_pic") is not None:
                user.profile_pic = enrichment["profile_pic"]
            if enrichment.get("name") is not None:
                user.name = enrichment["name"]
            if enrichment.get("is_verified") is not None:
                user.is_verified = enrichment["is_verified"]
            if enrichment.get("is_private") is not None:
                user.is_private = enrichment["is_private"]
            if user.insta_id is None and enrichment.get("insta_id") is not None:
                user.insta_id = enrichment["insta_id"]

            # Follower threshold gate
            follower_count = user.follower_count or 0
            if follower_count < settings.follower_threshold:
                user.crawl_state = "skipped_below_threshold"
                await session.commit()
                log.info(
                    "User %s below threshold (%d < %d), marked skipped_below_threshold",
                    username,
                    follower_count,
                    settings.follower_threshold,
                )
                return

            # Cleared threshold: enqueue in CrawlQueue
            existing_queue = (
                await session.execute(
                    select(CrawlQueue.id).where(
                        CrawlQueue.user_id == user.id,
                        CrawlQueue.status.in_(["pending", "processing"]),
                    )
                )
            ).scalar_one_or_none()

            priority = compute_priority(user.follower_count, user.seed_association_score, user.discovery_score)
            if not existing_queue:
                session.add(
                    CrawlQueue(
                        user_id=user.id,
                        priority=priority,
                        source_type="discovered",
                        queue_type="discovery",
                        status="pending",
                    )
                )

            user.crawl_state = "queued"
            await session.commit()
            log.info("Enriched %s: %d followers, score %d, priority %.2f -> queued", username, follower_count, user.discovery_score, priority)

        except Exception as e:
            await session.rollback()
            log.error("Failed processing user ID %d: %s", user_id, e)


async def run():
    log.info("Starting enrichment worker with SKIP LOCKED claiming...")
    async with httpx.AsyncClient(timeout=10.0) as http_client:
        proxy_pool = ProxyPool(state_file=settings.enrichment_proxy_state_file)
        await proxy_pool.refresh(http_client)

        try:
            while True:
                async with SessionLocal() as session:
                    user_ids = await _claim_batch(session, limit=5)

                if not user_ids:
                    await asyncio.sleep(2.0)
                    continue

                log.info("Claimed batch of %d pending users for enrichment", len(user_ids))
                for uid in user_ids:
                    await enrich_single_user(http_client, proxy_pool, uid)

        finally:
            await proxy_pool.aclose()


if __name__ == "__main__":
    asyncio.run(run())
