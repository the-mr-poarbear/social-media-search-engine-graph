"""
Async Neo4j driver wrapper. Two write paths:
  - upsert_edges_batch: creates (bare, if new) User nodes + FOLLOWS rels
  - upsert_users_batch: SETs display/LOD attributes onto User nodes
    (username, display_name, follower_count, pfp_url)

Run node attribute sync independently of edge loading whenever you want
to refresh follower counts / pfps / usernames without touching the graph
structure.
"""
from neo4j import AsyncGraphDatabase

EDGE_UPSERT_QUERY = """
UNWIND $rows AS row
MERGE (a:User {id: row.from_id})
MERGE (b:User {id: row.to_id})
MERGE (a)-[:FOLLOWS]->(b)
"""

# follower_count is indexed since it's the field you filter/sort on for
# LOD culling at render time. insta_id also indexed (non-unique here --
# uniqueness is already enforced in Postgres) since it's a natural lookup
# key when enrichment data arrives keyed by Instagram's own id.
USER_UPSERT_QUERY = """
UNWIND $rows AS row
MERGE (u:User {id: row.id})
SET u.insta_id = row.insta_id,
    u.username = row.username,
    u.name = row.name,
    u.follower_count = row.follower_count,
    u.profile_pic = row.profile_pic,
    u.is_verified = row.is_verified,
    u.discovery_score = row.discovery_score
"""


class Neo4jLoader:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = AsyncGraphDatabase.driver(uri, auth=(user, password))

    async def close(self):
        await self.driver.close()

    async def create_constraints(self):
        async with self.driver.session() as session:
            await session.run(
                "CREATE CONSTRAINT user_id IF NOT EXISTS "
                "FOR (u:User) REQUIRE u.id IS UNIQUE"
            )
            # Range index for fast "WHERE u.follower_count > $threshold"
            # queries -- this is what your LOD/zoom culling will run.
            await session.run(
                "CREATE INDEX user_follower_count IF NOT EXISTS "
                "FOR (u:User) ON (u.follower_count)"
            )
            await session.run(
                "CREATE INDEX user_insta_id IF NOT EXISTS "
                "FOR (u:User) ON (u.insta_id)"
            )

    async def upsert_edges_batch(self, rows: list[dict]):
        async with self.driver.session() as session:
            await session.run(EDGE_UPSERT_QUERY, rows=rows)

    async def upsert_users_batch(self, rows: list[dict]):
        async with self.driver.session() as session:
            await session.run(USER_UPSERT_QUERY, rows=rows)