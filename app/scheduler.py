"""
Scheduler service.

Polls crawl_queue for pending work, respecting a discovery/seed dispatch
ratio (default 80/20), locks the rows it picks (status -> processing,
locked_until -> now + lease), and publishes one Kafka message per job to
`crawl-jobs`. Workers pick those up independently.

Run: python -m app.scheduler
"""

import asyncio
import logging
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, update

from app.config import settings
from app.db import CrawlQueue, SessionLocal, User
from app.kafka_client import get_producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [scheduler] %(message)s")
log = logging.getLogger(__name__)

SCHEDULER_ID = f"scheduler-{uuid.uuid4().hex[:8]}"


async def _claim_batch(session, queue_type: str, limit: int) -> list[CrawlQueue]:
    if limit <= 0:
        return []
    now = datetime.now(timezone.utc)
    rows = (
        await session.execute(
            select(CrawlQueue)
            .where(
                CrawlQueue.status == "pending",
                CrawlQueue.queue_type == queue_type,
                CrawlQueue.available_at <= now,
            )
            .order_by(CrawlQueue.priority.desc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
    ).scalars().all()

    if rows:
        ids = [r.id for r in rows]
        lease_until = now + timedelta(seconds=settings.lease_seconds)
        await session.execute(
            update(CrawlQueue)
            .where(CrawlQueue.id.in_(ids))
            .values(status="processing", locked_by=SCHEDULER_ID, locked_until=lease_until)
        )
        await session.commit()
    return rows


async def _requeue_expired_leases(session):
    """Rows whose lease expired (worker crashed / never ack'd) go back to pending."""
    now = datetime.now(timezone.utc)
    result = await session.execute(
        update(CrawlQueue)
        .where(CrawlQueue.status == "processing", CrawlQueue.locked_until < now)
        .values(status="pending", locked_by=None, locked_until=None)
    )
    await session.commit()
    if result.rowcount:
        log.info("requeued %d jobs with expired leases", result.rowcount)


async def run():
    async with get_producer() as producer:
        while True:
            async with SessionLocal() as session:
                await _requeue_expired_leases(session)

                discovery_n = round(settings.scheduler_batch_size * settings.discovery_queue_share)
                seed_n = settings.scheduler_batch_size - discovery_n

                discovery_rows = await _claim_batch(session, "discovery", discovery_n)
                seed_rows = await _claim_batch(session, "seed", seed_n)
                # top up from the other queue if one is empty, so the batch doesn't shrink
                if not discovery_rows:
                    seed_rows += await _claim_batch(session, "seed", discovery_n)
                if not seed_rows:
                    discovery_rows += await _claim_batch(session, "discovery", seed_n)

                batch = discovery_rows + seed_rows
                if not batch:
                    await asyncio.sleep(settings.scheduler_poll_interval)
                    continue

                user_ids = [r.user_id for r in batch]
                users = (
                    await session.execute(select(User).where(User.id.in_(user_ids)))
                ).scalars().all()
                users_by_id = {u.id: u for u in users}

                for row in batch:
                    user = users_by_id.get(row.user_id)
                    if not user:
                        continue
                    await producer.send_and_wait(
                        settings.topic_crawl_jobs,
                        {
                            "queue_id": row.id,
                            "user_id": user.id,
                            "username": user.username,
                            "insta_id": user.insta_id,
                            "queue_type": row.queue_type,
                            "source_type": row.source_type,
                        },
                    )
                log.info("dispatched %d jobs (%d discovery, %d seed)", len(batch), len(discovery_rows), len(seed_rows))

            await asyncio.sleep(settings.scheduler_poll_interval)


if __name__ == "__main__":
    asyncio.run(run())
