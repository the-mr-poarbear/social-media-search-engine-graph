"""
Crawl worker.

Consumes `crawl-jobs`, resolves the target's numeric insta_id if we don't
have it yet, paginates their following list, and publishes each page
verbatim to `following-pages` for the graph consumer to process. Updates
the user row + crawl_queue row itself (workers own the "did the crawl
succeed" bookkeeping; the graph consumer only ever inserts/updates based
on discovered accounts).

Run multiple of these in parallel for real throughput:
  python -m app.worker
  python -m app.worker   # (another terminal/process — same consumer group)

NOTE: since this is a single-session POC (no proxy/account rotation),
running many workers in parallel against the *same* IG session just gets
you rate-limited faster, not more throughput. Keep it to 1 worker unless
you've wired up multiple sessions.
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [worker] %(message)s")
log = logging.getLogger(__name__)

WORKER_ID = f"worker-{uuid.uuid4().hex[:8]}"


async def _mark_failed(session, queue_id: int, error: str, requeue_delay_s: int = 60):
    row = (await session.execute(select(CrawlQueue).where(CrawlQueue.id == queue_id))).scalar_one()
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
            # if not insta_id:
            #     profile = await ig.resolve_user(insta_id)
            #     insta_id = profile["insta_id"]
            #     await session.execute(
            #         update(User)
            #         .where(User.id == user_id)
            #         .values(
            #             insta_id=insta_id,
            #             follower_count=profile["follower_count"],
            #             following_count=profile["following_count"],
            #             is_verified=profile["is_verified"],
            #             name=profile["name"],
            #             profile_pic=profile["profile_pic"],
            #         )
            #     )
            #     await session.commit()

                # THE actual gate: whatever got this row into crawl_queue — a
                # HypeAuditor estimate that's since drifted, a seed row that
                # was never checked at all, a stale queue entry — this is the
                # live IG number, checked right before we'd otherwise spend a
                # full pagination run on their following list.
            #     if (profile["follower_count"] or 0) < settings.follower_threshold:
            #         log.info(
            #             "%s: %s followers, below threshold (%s) — skipping crawl",
            #             username, profile["follower_count"], settings.follower_threshold,
            #         )
            #         await session.execute(
            #             update(CrawlQueue)
            #             .where(CrawlQueue.id == queue_id)
            #             .values(status="completed", locked_by=None, locked_until=None)
            #         )
            #         await session.execute(
            #             update(User).where(User.id == user_id).values(crawl_state="skipped_below_threshold")
            #         )
            #         await session.commit()
            #         return
            # else:
            #     # insta_id was already known (e.g. re-queued for refresh) —
            #     # still worth a fresh check in case follower count dropped.
            #     existing_user = (
            #         await session.execute(select(User).where(User.id == user_id))
            #     ).scalar_one()
            #     if (existing_user.follower_count or 0) < settings.follower_threshold:
            #         log.info("%s: below threshold on recheck — skipping crawl", username)
            #         await session.execute(
            #             update(CrawlQueue)
            #             .where(CrawlQueue.id == queue_id)
            #             .values(status="completed", locked_by=None, locked_until=None)
            #         )
            #         await session.execute(
            #             update(User).where(User.id == user_id).values(crawl_state="skipped_below_threshold")
            #         )
            #         await session.commit()
            #         return

            page_num = 0
            while True:
                accounts, hasmore = await ig.get_following_page(insta_id, page=page_num )
                page_num += 1
                await producer.send_and_wait(
                    settings.topic_following_pages,
                    {
                        "source_user_id": user_id,
                        "source_username": username,
                        "source_insta_id": insta_id,
                        "page_num": page_num,
                        "accounts": [
                            {
                                "insta_id": a.insta_id,
                                "username": a.username,
                                "name": a.name,
                                "is_verified": a.is_verified,
                                "profile_pic": a.profile_pic,
                                "is_private": a.is_private,
                            }
                            for a in accounts
                        ],
                    },
                )
                log.info("%s: page %d, %d accounts, done=%s", username, page_num, len(accounts), not hasmore)
                if not hasmore:
                    break

            await _mark_completed(session, queue_id, user_id, time.monotonic() - start)

        except RateLimited as e:
            log.warning("rate limited on %s: %s", username, e)
            await _mark_failed(session, queue_id, str(e), requeue_delay_s=300)
        except AuthExpired as e:
            log.error("auth expired: %s — refresh IG_SESSIONID and restart the worker", e)
            await _mark_failed(session, queue_id, str(e), requeue_delay_s=600)
            raise  # no point continuing this worker with a dead session
        except ScrapeError as e:
            log.warning("scrape error on %s: %s", username, e)
            await _mark_failed(session, queue_id, str(e))
        except Exception as e:  # noqa: BLE001 — POC-grade catch-all so one bad job doesn't kill the worker
            log.exception("unexpected error on %s", username)
            await _mark_failed(session, queue_id, repr(e))


async def run():
    ig = InstagramSession()
    consumer = make_consumer(settings.topic_crawl_jobs, group_id="workers")
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
    asyncio.run(run())
