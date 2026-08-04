"""
Loads your seed dataset CSV into `users` (is_seed=True) and `crawl_queue`
(queue_type=seed).

Expected CSV columns (extra columns are ignored):
  username, name, category, country, follower_count, seed_association_score

Usage:
  python -m scripts.load_seed data/seed.csv
"""

import asyncio
import csv
import sys

from sqlalchemy import func, literal_column
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.db import CrawlQueue, SessionLocal, User
from app.priority import compute_priority


async def load(csv_path: str):
    inserted, skipped = 0, 0
    async with SessionLocal() as session:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                username = row["username"].strip().lstrip("@")
                insta_id = None
                if row.get("insta_id"):
                    try:
                        insta_id = int(row["insta_id"].strip())
                    except ValueError:
                        continue  # skip malformed IDs
                if not username or not insta_id:
                    continue

                follower_count = int(row["follower_count"]) if row.get("follower_count") else None
                seed_score = float(row["seed_association_score"]) if row.get("seed_association_score") else 0.0

                stmt = (
                    pg_insert(User)
                    .values(
                        username=username,
                        name=row.get("name") or None,
                        category=row.get("category") or None,
                        country=row.get("country") or None,
                        follower_count=follower_count,
                        is_seed=True,
                        seed_association_score=seed_score,
                        insta_id=insta_id,
                    )
                    .on_conflict_do_update(
                        index_elements=["insta_id"],
                        set_={
                            "is_seed": True,
                            # if a user shows up on multiple seed lists, keep the higher score
                            "seed_association_score": func.greatest(
                                User.seed_association_score, pg_insert(User).excluded.seed_association_score
                            ),
                        },
                    )
                    .returning(User.id, literal_column("(xmax = 0)").label("is_new_row"))
                )
                result = await session.execute(stmt)
                row = result.one_or_none()
                if row is None:
                    skipped += 1
                    continue
                user_id, is_new_row = row
                if not is_new_row:
                    # already existed (and already has a queue entry from a prior load) — skip re-queueing
                    skipped += 1
                    continue

                priority = compute_priority(follower_count, seed_score, discovery_score=0)
                session.add(
                    CrawlQueue(
                        user_id=user_id,
                        priority=priority,
                        source_type="seed",
                        queue_type="seed",
                    )
                )
                inserted += 1

                if inserted % 1000 == 0:
                    await session.commit()
                    print(f"  ...{inserted} seed users loaded")

        await session.commit()
    print(f"done. inserted={inserted} skipped(dupe)={skipped}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: python -m scripts.load_seed <path-to-seed.csv>")
        sys.exit(1)
    asyncio.run(load(sys.argv[1]))
