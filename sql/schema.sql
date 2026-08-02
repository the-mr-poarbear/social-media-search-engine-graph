-- IG Graph POC schema
-- Simplified from the full design: no partitioning, no sharding, just enough
-- to prove the priority-driven crawl loop works end to end.

CREATE TABLE IF NOT EXISTS users (
    id                      BIGSERIAL PRIMARY KEY,
    insta_id                BIGINT UNIQUE,              -- NULL until we resolve username -> id
    username                TEXT UNIQUE NOT NULL,
    name                    TEXT,
    follower_count          INTEGER,
    following_count         INTEGER,
    profile_pic             TEXT,
    category                TEXT,
    country                 TEXT,
    is_seed                 BOOLEAN NOT NULL DEFAULT FALSE,
    seed_association_score  DOUBLE PRECISION DEFAULT 0,
    discovery_score         INTEGER NOT NULL DEFAULT 0,   -- incremented each time re-discovered
    crawl_state             TEXT NOT NULL DEFAULT 'never_crawled',
    is_verified             BOOLEAN,
    last_crawled_at         TIMESTAMPTZ,
    crawl_count             INTEGER NOT NULL DEFAULT 0,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_users_insta_id ON users (insta_id);
CREATE INDEX IF NOT EXISTS idx_users_crawl_state ON users (crawl_state);

CREATE TABLE IF NOT EXISTS edges (
    from_user_id  BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    to_user_id    BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    state         TEXT NOT NULL DEFAULT 'confirmed',
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (from_user_id, to_user_id)
);

CREATE INDEX IF NOT EXISTS idx_edges_to ON edges (to_user_id);

CREATE TABLE IF NOT EXISTS crawl_queue (
    id                    BIGSERIAL PRIMARY KEY,
    user_id               BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    priority              DOUBLE PRECISION NOT NULL DEFAULT 0,
    source_type           TEXT NOT NULL,          -- seed | discovered | refresh | manual
    queue_type            TEXT NOT NULL,          -- seed | discovery | refresh
    attempts              INTEGER NOT NULL DEFAULT 0,
    status                TEXT NOT NULL DEFAULT 'pending', -- pending | processing | completed | failed
    available_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by             TEXT,
    locked_until          TIMESTAMPTZ,
    last_error            TEXT,
    last_crawl_duration   DOUBLE PRECISION
);

-- app-level dedup: before inserting a new queue row for a user, check for an
-- existing pending/processing row. Enforced in code (see app/graph_consumer.py)
-- rather than a DB constraint, since "one active row per user" is easy to express
-- in a query but awkward as a raw UNIQUE constraint across a mutable status column.

CREATE INDEX IF NOT EXISTS idx_queue_dispatch
    ON crawl_queue (status, queue_type, priority DESC, available_at);
