# Neo4j Loader Service

Loads your Instagram graph into Neo4j (Docker), split into two entrypoints:

- **`load_nodes.py`** -- syncs attributes from Postgres `users` onto
  `:User` nodes: `insta_id`, `username`, `name`, `follower_count`,
  `profile_pic`, `is_verified`, `discovery_score`. Re-run independently
  any time to refresh these without touching graph structure.
- **`load_edges.py`** -- streams `edges` (`from_user_id, to_user_id`) and
  creates `(:User)-[:FOLLOWS]->(:User)` relationships.

Run `load_nodes.py` before (or alongside) `load_edges.py` on first load.

## Node identity: Postgres `users.id`, not `username` or `insta_id`

Neo4j node id = the internal Postgres `id` (bigint PK) -- same value your
`Edge` table's foreign keys reference. `username` is mutable (renames would
orphan relationships on re-sync), and `insta_id` is nullable in your schema
(a user discovered only via an edge but not yet enriched won't have one
yet), so neither is safe as the join key. `id` is the one guaranteed to
exist and never change.

## Why attributes live in Neo4j, not just ids

`follower_count` is indexed (`user_follower_count`) because it's the field
driving LOD/zoom culling -- filtering has to happen in the same Cypher call
that returns the viewport subgraph, not as a second round-trip to Postgres
per render. The rest (`username`, `name`, `profile_pic`, `is_verified`,
`discovery_score`) are denormalized too since every rendered node needs
them immediately. `insta_id` is also indexed as a natural lookup key for
when enrichment data arrives keyed by Instagram's own id rather than your
internal one.

Everything else in `users` (crawl_state, seed_association_score,
last_crawled_at, etc.) stays Postgres-only -- it's never touched by a graph
traversal or a render decision.

`follower_count` is left `null` rather than coerced to `0` when
unenriched -- "unknown" and "confirmed zero followers" shouldn't collapse
into the same value for LOD purposes. Setting a property to `null` in
Cypher just drops it from the node.

## 1. Start Neo4j in Docker

```bash
docker compose up -d
docker compose ps   # wait for healthy
```

## 2. Install deps

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

## 3. Configure

```bash
export PG_DSN="postgresql+asyncpg://igpoc:igpoc@localhost:5432/igpoc"

export PG_EDGES_TABLE="edges"
export PG_FROM_COL="from_user_id"
export PG_TO_COL="to_user_id"

export PG_USERS_TABLE="users"
export PG_USER_ID_COL="id"
export PG_INSTA_ID_COL="insta_id"
export PG_USERNAME_COL="username"
export PG_NAME_COL="name"
export PG_FOLLOWER_COUNT_COL="follower_count"
export PG_PROFILE_PIC_COL="profile_pic"
export PG_IS_VERIFIED_COL="is_verified"
export PG_DISCOVERY_SCORE_COL="discovery_score"

export NEO4J_URI="bolt://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="your_password_here"

export BATCH_SIZE=5000
```

Defaults already match your actual schema, so this step is only needed if
you rename columns later.

## 4. Run the loaders

```bash
python load_nodes.py    # attributes
python load_edges.py    # graph structure
```

Both are safe to re-run (`MERGE` + unique constraint on `id`) -- re-running
`load_nodes.py` after every enrichment cycle to refresh follower counts,
pfps, and verification status is fine.

## File map

```
docker-compose.yml   # Neo4j + APOC + GDS, Dockerized (DB only)
config.py            # env-driven settings: pg_dsn, table/column names, neo4j creds
pg_reader.py         # async generators streaming batches from Postgres
neo4j_client.py       # async Neo4j driver wrapper (constraints, indexes, node/edge upserts)
load_nodes.py         # entrypoint: sync user attributes -> Neo4j
load_edges.py         # entrypoint: sync graph structure -> Neo4j
neo4j_import/         # unused for now -- only needed if you switch to LOAD CSV later
```

## Notes

- `profile_pic`: if this is a raw Instagram CDN link, it will expire. If
  you're not already mirroring/proxying images somewhere stable, nodes
  will start showing broken images a few days after crawl.
- At ~20M nodes, these extra properties add roughly 3-4GB to Neo4j's
  footprint -- trivial next to what it already holds for relationships.
- If UNWIND/MERGE batch throughput isn't fast enough at full scale, the
  next step up is CSV export + `apoc.periodic.iterate` with `LOAD CSV`, or
  `neo4j-admin database import full` for a one-shot cold import.