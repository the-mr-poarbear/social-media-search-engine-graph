# IG Graph POC

Proves out the pipeline shape: seed dataset -> priority queue -> Kafka-dispatched
crawl workers -> Postgres graph. Not built for scale or survival (no proxy
rotation, no account pooling, no partitioning) — that's deliberately out of
scope for now.

## Stack
- Postgres 16
- Redpanda (Kafka-wire-protocol compatible, single container — swap for real
  Kafka+Zookeeper in `docker-compose.yml` if you specifically need Apache Kafka)
- Python 3.11, SQLAlchemy 2.x async (asyncpg), aiokafka, httpx

## Setup

```bash
docker compose up -d          # postgres:5432, redpanda:9092, console:8080
pip install -r requirements.txt --break-system-packages
alembic upgrade head           # applies the schema (see "Migrations" below)
```

## Migrations (Alembic)

Schema is managed by Alembic now, autogenerating off the SQLAlchemy models
in `app/db.py` — not by hand-editing `sql/schema.sql` (that file is kept
only as a human-readable snapshot; it's no longer applied automatically).

**Fresh setup:** `alembic upgrade head` — reads `PG_DSN` from the same env
var/config as the rest of the app (`app/config.py`), no separate DB URL to
keep in sync.

**Changing the schema:** edit the models in `app/db.py`, then:

```bash
alembic revision --autogenerate -m "add whatever_field to users"
# review the generated file in alembic/versions/ — autogenerate is good
# but not perfect (it won't catch things like column renames, it'll see
# those as a drop+add and lose data unless you edit the migration by hand)
alembic upgrade head
```

**Useful commands:**
```bash
alembic current              # what revision is the DB actually on
alembic history               # full migration chain
alembic check                 # does the DB match the models right now? (no drift = clean exit)
alembic downgrade -1          # roll back one migration
```

**If you already had a database from before Alembic was added** (i.e. it
was bootstrapped via the old `schema.sql` auto-init and already has the
three tables): don't run `alembic upgrade head` blindly, it'll try to
`CREATE TABLE` things that already exist and fail. Instead:

```bash
alembic stamp head    # tells alembic "the DB is already at this revision", no DDL runs
```


## Instagram auth

No password/2FA flow implemented. Instead: log into instagram.com in a
real browser, pull three cookies from DevTools -> Application -> Cookies,
and export them:

```bash
export IG_SESSIONID="..."
export IG_CSRFTOKEN="..."
export IG_DS_USER_ID="..."
```

Session cookies expire / get invalidated by IG's automation detection
faster than you'd like. When a worker logs `AuthExpired`, refresh these
and restart it.

## Running the pipeline

Load seed data first (try the bundled sample before pointing at your real
34K-row CSV):

```bash
python -m scripts.load_seed data/seed_sample.csv
```

Then, in separate terminals:

```bash
python -m app.scheduler          # 1 instance
python -m app.worker              # 1 instance — see note below on why not more, yet
python -m app.graph_consumer      # 1 instance
```

Watch topics/consumer lag at http://localhost:8080 (Redpanda console).

## Known POC limitations (intentional, matches what you asked for)

1. **Single Instagram session.** Running multiple `app.worker` processes
   against one session cookie just gets you rate-limited faster, not more
   throughput. Real parallelism needs a pool of authenticated sessions —
   out of scope here.
2. **Follower-count filtering is expensive.** The following-list endpoint
   doesn't return follower counts, so checking each newly-discovered
   account against the 100K threshold costs one extra profile lookup per
   account (see the caveat in `app/graph_consumer.py`). For a page of 200
   mostly-new accounts, that's 200 calls before you know which ones matter.
   If this makes the loop too slow to be useful, the doc in that file
   outlines the lazy-filtering alternative.
3. **No proxy rotation, no DB sizing/partitioning.** As agreed — this is
   about proving the loop closes (seed -> crawl -> discover -> re-queue -> crawl),
   not surviving IG's abuse detection or scaling to 1B edges.
4. **Backoff is naive.** `worker.py`'s retry delay is `60 * attempts`
   seconds, not real exponential backoff with jitter. Fine for a POC,
   worth revisiting before this runs unattended for days.

## What "success" looks like for this POC

Run it for an hour against a handful of seed accounts and check:

```sql
select count(*) from users;
select count(*) from edges;
select queue_type, status, count(*) from crawl_queue group by 1,2;
select username, follower_count, discovery_score
  from users order by discovery_score desc limit 20;
```

If the graph is growing, discovery_score is climbing on hub accounts, and
the queue is churning through both seed and discovery jobs roughly 20/80 —
the architecture holds. Then it's a conversation about proxies, session
pooling, and partitioning before touching real scale.