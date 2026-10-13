"""
Instagram Crawl Worker (GraphQL Edition).

Consumes `crawl-jobs` from Kafka, resolves the target's following graph using
Instagram's web GraphQL endpoint (edge_follow), chunks the discovered accounts,
and publishes them to `following-pages` for graph_consumer ingestion.

Features:
- GraphQL Pagination: Uses query_hash with `after` pagination cursors.
- Isolated Proxy Pool: Claims, monitors, and benches proxies from `data/worker/proxies_state.json`.
- Lease Heartbeating: Extends DB and proxy leases throughout long pagination crawls.
- Resilient Error Handling: Automatically benches proxies on HTTP 429 rate-limits.

Run:
  python -m app.worker
"""

import asyncio
import logging
import time
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, update

from app.config import settings
from app.db import CrawlQueue, SessionLocal, User
from app.kafka_client import get_producer, make_consumer
from app.proxy_pool import ProxyPool, _Proxy
from app.scraper import AuthExpired, InstagramSession, RateLimited, ScrapeError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [worker] %(message)s")
log = logging.getLogger(__name__)

WORKER_ID = f"worker-{uuid.uuid4().hex[:8]}"


async def _mark_failed(session, queue_id: int, error: str, requeue_delay_s: int = 60):
    row = (await session.execute(select(CrawlQueue).where(CrawlQueue.id == queue_id))).scalar_one_or_none()
    if not row:
        return
    if row.status not in ("processing", "pending"):
        return
    row.attempts += 1
    row.last_error = error[:2000]
    if row.attempts >= settings.max_attempts:
        row.status = "failed"
    else:
        row.status = "pending"
        row.available_at = datetime.now(timezone.utc) + timedelta(
            seconds=requeue_delay_s * row.attempts
        )
    row.locked_by = None
    row.locked_until = None
    await session.commit()


async def _mark_completed(session, queue_id: int, user_id: int, duration: float):
    await session.execute(
        update(CrawlQueue).where(CrawlQueue.id == queue_id).values(
            status="completed", locked_by=None, locked_until=None, last_crawl_duration=duration
        )
    )
    await session.execute(
        update(User)
        .where(User.id == user_id)
        .values(
            crawl_state="completed",
            last_crawled_at=datetime.now(timezone.utc),
            crawl_count=User.crawl_count + 1,
        )
    )
    await session.commit()


async def handle_job(
    ig_holder: list[InstagramSession | None],
    proxy_holder: list[_Proxy | None],
    proxy_pool: ProxyPool,
    producer,
    job: dict,
):
    queue_id = job["queue_id"]
    user_id = job["user_id"]
    username = job["username"]
    insta_id = job.get("insta_id")

    if not insta_id:
        log.warning("%s: missing numeric insta_id for job queue_id=%d, skipping", username, queue_id)
        return

    start = time.monotonic()
    async with SessionLocal() as session:
        try:
            now = datetime.now(timezone.utc)
            row = (
                await session.execute(select(CrawlQueue).where(CrawlQueue.id == queue_id))
            ).scalar_one_or_none()

            if not row:
                log.warning("%s: queue_id=%d not found in crawl_queue, skipping", username, queue_id)
                return

            if row.status == "completed":
                log.info("%s: queue_id=%d already completed, skipping duplicate message", username, queue_id)
                return

            if row.status != "processing" or (row.locked_until and row.locked_until < now):
                log.warning(
                    "%s: queue_id=%d lease expired or status changed (status=%s, locked_until=%s), skipping stale job",
                    username,
                    queue_id,
                    row.status,
                    row.locked_until,
                )
                return

            # Claim active worker ownership & renew initial DB lease
            row.locked_by = WORKER_ID
            row.locked_until = now + timedelta(seconds=settings.lease_seconds)
            await session.commit()

            # Acquire or verify active proxy lease
            active_proxy = await proxy_pool.get_proxy()
            proxy_holder[0] = active_proxy
            proxy_url = active_proxy.url if active_proxy else None

            # Re-initialize InstagramSession if proxy changed or not created yet
            current_ig = ig_holder[0]
            if current_ig is None or current_ig.proxy != proxy_url:
                if current_ig:
                    await current_ig.aclose()
                current_ig = InstagramSession(proxy=proxy_url)
                ig_holder[0] = current_ig

            page_num = 0
            cursor = None
            chunk_size = settings.worker_message_size
            total_accounts_crawled = 0

            log.info("Starting GraphQL crawl for %s (id=%s) via proxy=%s", username, insta_id, proxy_url or "direct")

            while True:
                req_start = time.monotonic()
                accounts, has_next_page, end_cursor = await current_ig.get_following_graphql(
                    insta_id=insta_id,
                    after_cursor=cursor,
                    first=50,
                )
                req_duration = time.monotonic() - req_start

                if active_proxy:
                    active_proxy.record_success(req_duration)

                total_accounts_crawled += len(accounts)

                # Split accounts into chunks and publish to Kafka following-pages
                for chunk_index, start_idx in enumerate(range(0, len(accounts), chunk_size)):
                    chunk = accounts[start_idx : start_idx + chunk_size]
                    await producer.send_and_wait(
                        settings.topic_following_pages,
                        {
                            "source_user_id": user_id,
                            "source_username": username,
                            "source_insta_id": insta_id,
                            "instagram_page": page_num,
                            "chunk_index": chunk_index,
                            "message_id": f"{insta_id}:{page_num}:{chunk_index}",
                            "accounts": [
                                {
                                    "insta_id": a.insta_id,
                                    "username": a.username,
                                    "name": a.name,
                                    "is_verified": a.is_verified,
                                    "profile_pic": a.profile_pic,
                                    "is_private": a.is_private,
                                }
                                for a in chunk
                            ],
                        },
                    )

                log.info(
                    "%s: page=%d accounts=%d has_next=%s cursor=%s (total=%d)",
                    username,
                    page_num,
                    len(accounts),
                    has_next_page,
                    (end_cursor[:16] + "...") if end_cursor else None,
                    total_accounts_crawled,
                )

                if not has_next_page or not end_cursor:
                    break

                cursor = end_cursor
                page_num += 1

                # Heartbeat: Touch both DB job lease and proxy lease during long crawls
                await session.execute(
                    update(CrawlQueue)
                    .where(CrawlQueue.id == queue_id, CrawlQueue.locked_by == WORKER_ID)
                    .values(locked_until=datetime.now(timezone.utc) + timedelta(seconds=settings.lease_seconds))
                )
                await session.commit()

                if active_proxy:
                    active_proxy = await proxy_pool.get_proxy()
                    proxy_holder[0] = active_proxy

            duration = time.monotonic() - start
            await _mark_completed(session, queue_id, user_id, duration)
            log.info("Finished crawl for %s: %d total accounts in %.1fs", username, total_accounts_crawled, duration)

        except RateLimited as e:
            log.warning("Rate limited on %s: %s", username, e)
            await session.rollback()
            if proxy_holder[0]:
                proxy_holder[0].bench_immediately(reason=f"rate_limited: {e}")
                proxy_pool.release_proxy(proxy_holder[0])
                proxy_holder[0] = None
            if ig_holder[0]:
                await ig_holder[0].aclose()
                ig_holder[0] = None
            await _mark_failed(session, queue_id, str(e), requeue_delay_s=300)

        except AuthExpired as e:
            log.error("Auth expired: %s — refresh IG_SESSIONID and restart the worker", e)
            await session.rollback()
            await _mark_failed(session, queue_id, str(e), requeue_delay_s=600)
            raise

        except ScrapeError as e:
            log.warning("Scrape error on %s: %s", username, e)
            await session.rollback()
            if proxy_holder[0]:
                proxy_holder[0].record_failure(reason=f"scrape_error: {e}")
            await _mark_failed(session, queue_id, str(e))

        except Exception as e:
            log.exception("Unexpected error on %s", username)
            await session.rollback()
            if proxy_holder[0]:
                proxy_holder[0].record_failure(reason=f"unexpected_error: {e}")
            await _mark_failed(session, queue_id, repr(e))


async def run(override_proxy: str | None = None):
    log.info("Starting Instagram GraphQL Crawl Worker [%s]...", WORKER_ID)

    proxy_pool = ProxyPool(state_file=settings.worker_proxy_state_file)
    ig_holder: list[InstagramSession | None] = [None]
    proxy_holder: list[_Proxy | None] = [None]

    if override_proxy:
        log.info("Using CLI override proxy: %s", override_proxy)
        ig_holder[0] = InstagramSession(proxy=override_proxy)

    consumer = make_consumer(settings.topic_crawl_jobs, group_id="workers")
    await consumer.start()

    try:
        async with get_producer() as producer:
            log.info("Worker listening for crawl jobs on topic '%s'...", settings.topic_crawl_jobs)
            async for msg in consumer:
                job = msg.value
                await handle_job(ig_holder, proxy_holder, proxy_pool, producer, job)
                await consumer.commit()
    finally:
        await consumer.stop()
        if ig_holder[0]:
            await ig_holder[0].aclose()
        await proxy_pool.aclose()
        log.info("Worker [%s] shutdown cleanly.", WORKER_ID)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Instagram GraphQL Crawl Worker")
    parser.add_argument(
        "--proxy",
        type=str,
        default=None,
        help="Override proxy URL (e.g. socks5://127.0.0.1:2080). Overrides shared proxy pool.",
    )
    args = parser.parse_args()
    asyncio.run(run(override_proxy=args.proxy))
