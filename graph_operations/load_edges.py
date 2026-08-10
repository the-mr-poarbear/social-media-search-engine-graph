"""
load_edges.py

Streams your (from_user_id, to_user_id) table out of Postgres and loads
it into the Dockerized Neo4j as (:User)-[:FOLLOWS]->(:User).

Run:
    python load_edges.py

Config comes from env vars (see config.py for defaults):
    PG_DSN, PG_EDGES_TABLE, PG_FROM_COL, PG_TO_COL,
    NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, BATCH_SIZE
"""
import asyncio
import time

from app.config import settings
from .neo4j_client import Neo4jLoader
from .pg_reader import stream_edges

# Run load_nodes.py first (or alongside) to populate username/display_name/
# follower_count/pfp_url -- this script only creates graph structure.


async def main():
    loader = Neo4jLoader(settings.neo4j_uri, settings.neo4j_user, settings.neo4j_password)
    await loader.create_constraints()

    start = time.monotonic()
    total = 0

    async for batch in stream_edges(settings.pg_dsn, settings.batch_size):
        await loader.upsert_edges_batch(batch)
        total += len(batch)
        elapsed = time.monotonic() - start
        rate = total / elapsed if elapsed > 0 else 0
        print(f"Loaded {total:,} edges  ({rate:,.0f}/sec)")

    await loader.close()
    print(f"Done. {total:,} edges loaded in {time.monotonic() - start:.1f}s")


if __name__ == "__main__":
    asyncio.run(main())