"""
Retries enrichment for `users` rows stuck at crawl_state='enrichment_failed'.

Run periodically (cron / scheduled task):
  python -m app.retry_failed_enrichment
"""

import asyncio
import logging

import httpx
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.config import settings
from app.db import CrawlQueue, Edge, SessionLocal, User
from app.enrichment import lookup_follower_count
from app.priority import compute_priority
from app.proxy_pool import ProxyPool

logging.basicConfig(level=logging.INFO, format="%(asctime)s [retry-enrichment] %(message)s")
log = logging.getLogger(__name__)


async def retry_user(http_client: httpx.AsyncClient, proxy_pool: ProxyPool, user_id: int):
    async with SessionLocal() as session:
        try:
            user = await session.get(User, user_id)
            if not user or user.crawl_state != "enrichment_failed":
                return

            username = user.username

            try:
                enrichment, exhausted_retries = await lookup_follower_count(http_client, proxy_pool, username)
            except Exception as e:
                log.warning("Lookup failed again for %s: %s", username, e)
                return

            # Handle lookup failure
            if not enrichment:
                if exhausted_retries:
                    # Temporary network / proxy failure: leave as 'enrichment_failed'
                    log.warning("Requests exhausted for %s, keeping enrichment_failed", username)
                    return
                else:
                    # Permanent absence: user does not exist in enrichment database
                    log.info("User %s not present in enrichment database", username)
                    user.crawl_state = "not_present_in_enrichment_list"
                    await session.commit()
                    return

            # Populate metadata from enrichment
            user.follower_count = enrichment.get("follower_count")
            if enrichment.get("is_verified") is not None:
                user.is_verified = enrichment["is_verified"]
            if enrichment.get("is_private") is not None:
                user.is_private = enrichment["is_private"]
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
            if user.insta_id is None and enrichment.get("insta_id") is not None:
                user.insta_id = enrichment["insta_id"]

            # Ensure edge from discovered_by exists (if missing)
            if user.discovered_by:
                edge_result = await session.execute(
                    pg_insert(Edge)
                    .values(from_user_id=user.discovered_by, to_user_id=user.id)
                    .on_conflict_do_nothing(index_elements=["from_user_id", "to_user_id"])
                )
                if edge_result.rowcount > 0:
                    user.discovery_score = (user.discovery_score or 0) + 1

            # Check follower threshold
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

            # Cleared threshold: check if already in queue or add it
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
            log.info("Recovered %s: %d followers, priority %.2f, queued", username, follower_count, priority)

        except Exception as e:
            await session.rollback()
            log.warning("Failed processing user ID %d: %s", user_id, e)


async def run():
    async with httpx.AsyncClient(timeout=10.0) as http_client:
        proxy_pool = ProxyPool()
        await proxy_pool.refresh(http_client)
        async with SessionLocal() as session:
            user_ids = (
                await session.execute(select(User.id).where(User.crawl_state == "enrichment_failed"))
            ).scalars().all()

        log.info("Retrying %d failed-enrichment users", len(user_ids))
        for uid in user_ids:
            await retry_user(http_client, proxy_pool, uid)

        await proxy_pool.aclose()


if __name__ == "__main__":
    asyncio.run(run())