"""
Async generators streaming rows out of Postgres in batches, using
SQLAlchemy's async engine + a server-side cursor (conn.stream) so this
stays low-memory regardless of table size.

Table/column names are hardcoded to match app/models.py directly -- no
need to parameterize a schema that isn't going to change per-environment.
"""
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

EDGES_QUERY = text("SELECT from_user_id, to_user_id FROM edges")

USERS_QUERY = text(
    "SELECT id, insta_id, username, name, follower_count, "
    "profile_pic, is_verified, discovery_score FROM users"
)


async def stream_edges(pg_dsn: str, batch_size: int = 5000):
    """Yields lists of {"from_id": ..., "to_id": ...} dicts."""
    engine = create_async_engine(pg_dsn)
    try:
        async with engine.connect() as conn:
            result = await conn.stream(EDGES_QUERY, execution_options={"yield_per": batch_size})
            batch = []
            async for row in result:
                batch.append({"from_id": str(row[0]), "to_id": str(row[1])})
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch
    finally:
        await engine.dispose()


async def stream_users(pg_dsn: str, batch_size: int = 5000):
    """
    Yields lists of dicts with the fields synced onto Neo4j :User nodes:
    id (node identity, matches users.id / the Edge FKs), insta_id,
    username, name, follower_count, profile_pic, is_verified, discovery_score.
    """
    engine = create_async_engine(pg_dsn)
    try:
        async with engine.connect() as conn:
            result = await conn.stream(USERS_QUERY, execution_options={"yield_per": batch_size})
            batch = []
            async for row in result:
                batch.append({
                    "id": str(row[0]),
                    "insta_id": str(row[1]) if row[1] is not None else None,
                    "username": row[2],
                    "name": row[3],
                    # Left as None (not coerced to 0) when unenriched --
                    # "unknown" and "confirmed 0 followers" shouldn't be
                    # the same thing for LOD culling. Neo4j drops a
                    # property entirely when SET to null.
                    "follower_count": row[4],
                    "profile_pic": row[5],
                    "is_verified": row[6],
                    "discovery_score": row[7],
                })
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch
    finally:
        await engine.dispose()