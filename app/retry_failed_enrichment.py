"""
Retries HypeAuditor enrichment for `users` rows stuck at
crawl_state='enrichment_failed' that were never re-discovered by a later
crawl (graph_consumer.py already retries inline on every re-encounter —
this script is specifically for the ones that never got a second encounter).

If a retry now succeeds and clears the follower threshold, this recreates
the edge using `discovered_by` (who originally found them) — the one edge
that would otherwise be permanently lost, since every subsequent encounter
of an 'enrichment_failed' user is what recovers the *other* edges
automatically.

Run periodically (cron / scheduled task), not continuously:
  python -m scripts.retry_failed_enrichment
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


async def retry_one(http_client: httpx.AsyncClient, proxy_pool: ProxyPool, session, user: User):
    try:
        enrichment = await lookup_follower_count(http_client, proxy_pool, user.username)
    except httpx.HTTPError as e:
        log.warning("retry failed again for %s: %s", user.username, e)
        return

    if not enrichment:
        return  # still failed, leave crawl_state='enrichment_failed' for next run

    user.follower_count = enrichment["follower_count"]
    user.is_verified = enrichment["is_verified"]
    user.is_private = enrichment["is_private"]
    user.crawl_state = "never_crawled"
    await session.merge(user)
    await session.commit()

    if (user.follower_count or 0) < settings.follower_threshold or user.discovered_by is None:
        return  # now genuinely known-low, or no source to attribute an edge to — nothing more to do

    edge_result = await session.execute(
        pg_insert(Edge)
        .values(from_user_id=user.discovered_by, to_user_id=user.id)
        .on_conflict_do_nothing(index_elements=["from_user_id", "to_user_id"])
    )
    await session.commit()
    if edge_result.rowcount == 0:
        return  # edge already exists (recovered via a later re-encounter in the meantime)

    user.discovery_score = (user.discovery_score or 0) + 1
    await session.merge(user)
    await session.commit()

    if user.crawl_state != "never_crawled":
        return

    priority = compute_priority(user.follower_count, user.seed_association_score, user.discovery_score)
    queue_result = await session.execute(
        pg_insert(CrawlQueue).values(
            user_id=user.id, priority=priority, source_type="discovered",
            queue_type="discovery", status="pending",
        )
    )
    if queue_result.rowcount:
        user.crawl_state = "queued"
        await session.commit()

    log.info("recovered %s: now %s followers, edge + queue entry created", user.username, user.follower_count)


async def run():
    async with httpx.AsyncClient(timeout=10.0) as http_client:
        proxy_pool = ProxyPool()
        await proxy_pool.refresh(http_client)
        async with SessionLocal() as session:
            failed_users = (
                await session.execute(select(User).where(User.crawl_state == "enrichment_failed"))
            ).scalars().all()
            log.info("retrying %d failed-enrichment users", len(failed_users))
            for user in failed_users:
                await retry_one(http_client, proxy_pool, session, user)
        await proxy_pool.aclose()


if __name__ == "__main__":
    asyncio.run(run())