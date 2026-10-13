"""
[LEGACY] Crawl worker (pre-GraphQL deprecation).

This is the previous crawl worker implementation that used mobile v1
endpoints (friendships/{id}/following) without dynamic ProxyPool leasing.
Preserved for historical reference and backward fallback.
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
from app.scraper import AuthExpired, InstagramSession, RateLimited, ScrapeError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [worker-legacy] %(message)s")
log = logging.getLogger(__name__)

WORKER_ID = f"worker-legacy-{uuid.uuid4().hex[:8]}"


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
            seconds=requeue_delay_s * row.attempts  # simple exponential-ish backoff
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


async def handle_job(ig: InstagramSession, producer, job: dict):
    queue_id = job["queue_id"]
    user_id = job["user_id"]
    username = job["username"]
    insta_id = job.get("insta_id")

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

            # Claim active worker ownership & renew initial lease
            row.locked_by = WORKER_ID
            row.locked_until = now + timedelta(seconds=settings.lease_seconds)
            await session.commit()

            page_num = 0
            page_size = 200
            chunk_size = settings.worker_message_size

            while True:
                accounts, hasmore = await ig.get_following_page(insta_id, page=page_num, count=page_size)

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
                        "%s: instagram_page=%d chunk=%d accounts=%d done=%s",
                        username,
                        page_num,
                        chunk_index,
                        len(chunk),
                        not hasmore,
                    )

                page_num += 1

                if not hasmore:
                    break

            await _mark_completed(session, queue_id, user_id, time.monotonic() - start)

        except RateLimited as e:
            log.warning("rate limited on %s: %s", username, e)
            await session.rollback()
            await _mark_failed(session, queue_id, str(e), requeue_delay_s=300)
        except AuthExpired as e:
            log.error("auth expired: %s — refresh IG_SESSIONID and restart the worker", e)
            await session.rollback()
            await _mark_failed(session, queue_id, str(e), requeue_delay_s=600)
            raise  # no point continuing this worker with a dead session
        except ScrapeError as e:
            log.warning("scrape error on %s: %s", username, e)
            await session.rollback()
            await _mark_failed(session, queue_id, str(e))
        except Exception as e:  # noqa: BLE001
            log.exception("unexpected error on %s", username)
            await session.rollback()
            await _mark_failed(session, queue_id, repr(e))


async def run(proxy: str | None = None):
    effective_proxy = proxy or settings.worker_proxy or None
    if effective_proxy:
        log.info("Starting worker with proxy: %s", effective_proxy)
    else:
        log.info("Starting worker without proxy (direct connection)")

    ig = InstagramSession(proxy=effective_proxy)
    consumer = make_consumer(settings.topic_crawl_jobs, group_id="workers-legacy")
    await consumer.start()
    try:
        async with get_producer() as producer:
            async for msg in consumer:
                job = msg.value
                await handle_job(ig, producer, job)
                await consumer.commit()
    finally:
        await consumer.stop()
        await ig.aclose()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Instagram Crawl Worker (Legacy)")
    parser.add_argument(
        "--proxy",
        type=str,
        default=None,
        help="Proxy URL (e.g. socks5://127.0.0.1:2080 or http://127.0.0.1:8080). Overrides WORKER_PROXY environment variable.",
    )
    args = parser.parse_args()
    asyncio.run(run(proxy=args.proxy))
