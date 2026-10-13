"""
Quick helper to enqueue stranded users who have follower_count >= settings.follower_threshold
and are sitting at crawl_state='never_crawled'.

Usage:
  python -m scripts.enqueue_stranded_users
"""

import asyncio
import logging

from sqlalchemy import select

from app.config import settings
from app.db import CrawlQueue, SessionLocal, User
from app.priority import compute_priority

logging.basicConfig(level=logging.INFO, format="%(asctime)s [enqueue-stranded] %(message)s")
log = logging.getLogger(__name__)


async def main():
    async with SessionLocal() as session:
        users = (
            await session.execute(
                select(User).where(
                    User.crawl_state == "never_crawled",
                    User.follower_count >= settings.follower_threshold,
                )
            )
        ).scalars().all()

        log.info("Found %d stranded high-follower accounts to enqueue", len(users))
        enqueued_count = 0

        for user in users:
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
            enqueued_count += 1

        await session.commit()
        log.info("Successfully enqueued %d users into crawl_queue with status='queued'", enqueued_count)


if __name__ == "__main__":
    asyncio.run(main())
