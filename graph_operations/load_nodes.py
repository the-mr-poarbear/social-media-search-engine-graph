"""
load_nodes.py

Syncs display/LOD attributes (username, display_name, follower_count,
pfp_url) from your Postgres users table onto Neo4j User nodes.

Run this BEFORE load_edges.py on first load, and re-run it any time on
its own to refresh follower counts / pfps / usernames without touching
graph structure (e.g. after your enrichment worker updates users).

Run:
    python load_nodes.py
"""
import asyncio
import time

from app.config import settings
from .neo4j_client import Neo4jLoader
from .pg_reader import stream_users


async def main():
    loader = Neo4jLoader(settings.neo4j_uri, settings.neo4j_user, settings.neo4j_password)
    await loader.create_constraints()

    start = time.monotonic()
    total = 0

    async for batch in stream_users(settings.pg_dsn, settings.batch_size):
        await loader.upsert_users_batch(batch)
        total += len(batch)
        elapsed = time.monotonic() - start
        rate = total / elapsed if elapsed > 0 else 0
        print(f"Synced {total:,} user nodes  ({rate:,.0f}/sec)")

    await loader.close()
    print(f"Done. {total:,} user nodes synced in {time.monotonic() - start:.1f}s")


if __name__ == "__main__":
    asyncio.run(main())