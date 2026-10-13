"""
Graph consumer.

Consumes `following-pages` from Kafka and ingests the social graph in high-throughput batches:
  1. Bulk upserts discovered accounts into `users` with crawl_state='pending_enrichment' (or 'never_crawled' if private).
  2. Bulk inserts relationships into `edges` (with on_conflict_do_nothing).
  3. Bulk increments `discovery_score` for all accounts with newly confirmed edges.
  4. Commits Kafka offset in milliseconds.

Profile enrichment and follower-count thresholding are decoupled and handled asynchronously
by `app.enrichment_worker`.

Run: python -m app.graph_consumer
"""

import asyncio
import logging

from sqlalchemy import or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.config import settings
from app.db import Edge, SessionLocal, User
from app.kafka_client import make_consumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [graph-consumer] %(message)s")
log = logging.getLogger(__name__)


async def process_page(session, page: dict):
    source_user_id = page["source_user_id"]
    accounts = page.get("accounts", [])
    if not accounts:
        return

    # Step 1: Batch upsert user stubs (existing users are untouched via on_conflict_do_nothing)
    user_values = []
    for acc in accounts:
        username = acc.get("username")
        if not username:
            continue
        is_private = acc.get("is_private", False)
        crawl_state = "never_crawled" if is_private else "pending_enrichment"

        user_values.append({
            "username": username,
            "insta_id": acc.get("insta_id"),
            "name": acc.get("name"),
            "is_private": is_private,
            "is_verified": acc.get("is_verified", False),
            "profile_pic": acc.get("profile_pic"),
            "discovered_by": source_user_id,
            "crawl_state": crawl_state,
        })

    if not user_values:
        return

    user_stmt = (
        pg_insert(User)
        .values(user_values)
        .on_conflict_do_nothing()
    )
    await session.execute(user_stmt)

    # Step 2: Query IDs for all accounts in this chunk by username or insta_id in 1 query
    usernames = [u["username"] for u in user_values if u.get("username")]
    insta_ids = [u["insta_id"] for u in user_values if u.get("insta_id")]

    conditions = []
    if usernames:
        conditions.append(User.username.in_(usernames))
    if insta_ids:
        conditions.append(User.insta_id.in_(insta_ids))

    user_rows = (await session.execute(
        select(User.id, User.username, User.insta_id).where(or_(*conditions))
    )).all()

    user_id_by_username = {row.username: row.id for row in user_rows if row.username}
    user_id_by_insta_id = {row.insta_id: row.id for row in user_rows if row.insta_id}

    # Step 3: Batch insert edges from source_user_id to all target accounts
    edge_target_ids = set()
    for u in user_values:
        uid = user_id_by_username.get(u["username"]) or (user_id_by_insta_id.get(u["insta_id"]) if u.get("insta_id") else None)
        if uid:
            edge_target_ids.add(uid)

    edge_values = [
        {"from_user_id": source_user_id, "to_user_id": uid}
        for uid in edge_target_ids
    ]

    if edge_values:
        edge_stmt = (
            pg_insert(Edge)
            .values(edge_values)
            .on_conflict_do_nothing(index_elements=["from_user_id", "to_user_id"])
            .returning(Edge.to_user_id)
        )
        edge_result = await session.execute(edge_stmt)
        new_edge_to_user_ids = edge_result.scalars().all()

        # Step 4: Batch increment discovery_score for all accounts that gained a new incoming edge
        if new_edge_to_user_ids:
            await session.execute(
                update(User)
                .where(User.id.in_(new_edge_to_user_ids))
                .values(discovery_score=User.discovery_score + 1)
            )

    await session.commit()


async def run():
    consumer = make_consumer(settings.topic_following_pages, group_id="graph-consumers")
    await consumer.start()
    log.info("Graph consumer started. Listening for following-pages...")
    try:
        async for msg in consumer:
            page = msg.value
            async with SessionLocal() as session:
                await process_page(session, page)
            await consumer.commit()
            log.info(
                "Processed chunk %d of page %d from %s (%d accounts)",
                page["chunk_index"],
                page["instagram_page"],
                page["source_username"],
                len(page.get("accounts", [])),
            )
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(run())