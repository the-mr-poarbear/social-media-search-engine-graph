<p align="right">
  <a href="./README.md">English</a> |
  <a href="./README.fa.md">فارسی</a>
</p>

<h1 align="">SocialGraph Engine</h1>

<p align="">
  <i>A high-throughput, event-driven distributed crawler and graph mining pipeline for large-scale social network discovery.</i>
</p>

<p align="">
  An end-to-end social network intelligence platform powered by Redpanda (Kafka), PostgreSQL, Neo4j Graph Data Science (GDS), and an automated self-healing proxy network.
</p>

---

## Overview

Mapping and analyzing large-scale social networks is notoriously difficult. Social platforms enforce stringent rate limits, deploy aggressive bot detection, and present massive graph densities where single profiles can branch into tens of thousands of connections. Traditional single-process scrapers quickly get rate-limited, stall on API lookups, or crash under the memory overhead of rendering millions of graph relationships.

**SocialGraph Engine** solves these challenges through a distributed, decoupled, and event-driven architecture designed to discover, crawl, enrich, and visualize large-scale follower networks:

- **Distributed Priority Crawling:** Kafka-orchestrated crawl workers traverse 1-hop and 2-hop follower graphs using logarithmic multi-objective priority queues.
- **Decoupled Asynchronous Ingestion & Enrichment:** Kafka-backed event streams separate real-time graph edge ingestion from slow external profile enrichment, preventing pipeline bottlenecks.
- **Self-Healing Proxy Orchestration:** Dynamic multi-protocol proxy rotation (Sing-box, V2Ray, SOCKS5) with automated latency benchmarking, cooldown benches, and file-based state synchronization.
- **Graph Intelligence & Layout Baking:** High-throughput streaming into Neo4j with Graph Data Science (GDS) Louvain Community Detection, FastRP structural embeddings, and precomputed 2D Sunflower phyllotaxis layout baking for instant Level-of-Detail (LOD) web visualization.

Whether exploring influencer clusters or mapping vast community topologies, SocialGraph Engine demonstrates how modern data engineering and distributed systems principles conquer the challenges of large-scale social data mining.

## About Me

I'm Arsam, a Computer Engineering student with a passion for Data Engineering, Distributed Systems, Graph Mining, and Scalable Backend Architectures.

This project represents the convergence of these interests: engineering fault-tolerant scrapers, coordinating distributed workers over message brokers, designing concurrent transactional database queues, and running graph algorithms at scale.

Explore more of my work on my [GitHub profile](https://github.com/the-mr-poarbear).

## System Workflow

The crawler operates as a closed continuous feedback loop: seed datasets initialize the queue, workers extract follower connections, new accounts are discovered and ranked, and high-impact profiles are dynamically requeued for deep exploration.

```mermaid
flowchart TD
    subgraph S1["1. Scheduling & Dispatch"]
        A[("PostgreSQL\ncrawl_queue")] -->|SKIP LOCKED\nTimed Lease| B["Priority Scheduler\napp.scheduler"]
        B -->|"crawl-jobs\n(Topic)"| C{{"Redpanda / Kafka\nMessage Broker"}}
    end

    subgraph S2["2. Distributed Crawling"]
        C -->|Consume Job| D["GraphQL Crawl Worker\napp.worker"]
        P[("Sing-box / V2Ray\nProxy Pool")] -.->|Rotated Requests| D
        D -->|"Query GraphQL\nedge_follow"| IG["Instagram Web API"]
        IG -->|"Discovered Followers"| D
        D -->|"following-pages\n(Topic)"| C
    end

    subgraph S3["3. High-Throughput Ingestion"]
        C -->|Consume Pages| E["Graph Consumer\napp.graph_consumer"]
        E -->|"Batch Upsert Users\nBulk Insert Edges"| F[("PostgreSQL\nusers & edges")]
        E -->|"Increment discovery_score"| F
    end

    subgraph S4["4. Decoupled Enrichment"]
        F -->|"Pending Profiles\n(SKIP LOCKED)"| G["Enrichment Worker\napp.enrichment_worker"]
        G -->|"Adaptive Delay Lookups"| APIS["Public Metrics APIs\n(HypeAuditor / TrendHero)"]
        APIS -->|Follower Counts & Metadata| G
        G -->|"follower_count >= 100K"| A
        G -->|"follower_count < 100K"| H["Mark Skipped\n(Edge Preserved)"]
    end

    subgraph S5["5. Graph Analytics & LOD Layout"]
        F -->|"Async Stream Batches"| I["Neo4j Loader\ngraph_operations"]
        I -->|"(:User)-[:FOLLOWS]->(:User)"| J[("Neo4j Database\nAPOC + GDS")]
        J -->|"Louvain Community Detection\nFastRP Structural Embeddings"| K["Layout Baker\nbake_layout.py"]
        K -->|"Bake (x, y) Coordinates"| J
    end
```

---

## Core Engineering & Architectural Components

### 1. Multi-Objective Priority Scheduling

Rather than naive breadth-first (BFS) or depth-first (DFS) traversal, crawling priorities are calculated using a logarithmic multi-objective score balancing celebrity scale, seed relevance, and real-time in-degree network centrality:

$$\text{Priority} = W_{\text{followers}} \cdot \ln(1 + \text{followers}) + W_{\text{seed}} \cdot \text{seed\_score} + W_{\text{discovery}} \cdot \ln(1 + \text{discovery\_score})$$

- **Logarithmic Scaling ($\ln(1 + x)$):** Prevents accounts with 100M+ followers from starving accounts with 500K followers, while ensuring early discovery in-degrees matter exponentially more than later hops.
- **Discovery vs. Seed Balance:** The scheduler maintains an 80/20 dispatch ratio (80% newly discovered viral accounts, 20% verified seed accounts) to continuously expand new network frontiers while deepening core hubs.
- **Contention-Free Distributed Locking:** Implemented with PostgreSQL `FOR UPDATE SKIP LOCKED`. Multiple scheduler instances can run simultaneously without race conditions or deadlock hazards.
- **Self-Healing Lease Management:** Workers receive timed leases (`locked_until`). If a worker crashes or drops offline, expired leases are automatically reclaimed and returned to the queue with exponential backoff and jitter.

### 2. High-Throughput GraphQL Crawl Workers

Crawl workers consume dispatch tasks from Kafka and extract follow lists directly from Instagram's web GraphQL API:

- **Cursor-Based Pagination:** Crawls paginated `edge_follow` connections utilizing `after` cursors and web query hashes.
- **Continuous Lease Heartbeating:** As long-running pagination crawls proceed across thousands of followers, workers continuously renew their database and proxy leases to avoid stale reclamation.
- **Instant Chunk Dispatching:** As soon as a chunk of 50–200 accounts is extracted, it is immediately emitted to the Kafka `following-pages` topic, keeping downstream ingestion saturated without waiting for multi-page crawls to finish.

### 3. Decoupled Real-Time Ingestion vs. Metadata Enrichment

A fundamental bottleneck in large-scale social crawlers is resolving profile metadata. Instagram's following endpoints provide user handles and IDs but omit follower counts. Resolving follower counts directly in the crawl loop creates a 200:1 request amplification that triggers rate limits instantly.

SocialGraph Engine decouples ingestion into two distinct stages:

1. **Instant Edge Ingestion (`app.graph_consumer`):** Consumes `following-pages` from Kafka at thousands of records per second. It performs bulk `ON CONFLICT DO NOTHING` user stub upserts, writes edges, increments network `discovery_score`, and commits Kafka offsets in milliseconds.
2. **Asynchronous Enrichment Workers (`app.enrichment_worker`):** Independent workers claim batches of pending users with `FOR UPDATE SKIP LOCKED`, prioritized by `discovery_score`. Using external intelligence APIs (HypeAuditor and TrendHero) with adaptive PID-like cooldown pacing (`AdaptiveDelay`), workers verify true follower counts and verification badges.
   - **Above Threshold ($\ge 100\text{k}$):** Enqueued into `crawl_queue` for 2nd-hop crawl expansion.
   - **Below Threshold ($< 100\text{k}$):** Marked as `skipped_below_threshold`. The connection edge remains in the graph, but crawler resources are preserved for high-influence nodes.

### 4. Self-Healing Multi-Protocol Proxy Network

To sustain high request volumes without IP bans, the platform incorporates an automated proxy orchestrator:

- **Multi-Protocol Support:** Dynamically connects through Sing-box and V2Ray outbounds, routing traffic through local SOCKS5 ports (`2080`, `2081`, `2082`, etc.).
- **Automated Health Benchmarking:** Continuously tests IP connectivity and latency against external endpoints (`api.ipify.org`).
- **Cooldown Benches:** Automatically detects HTTP 429 rate-limiting, socket timeouts, and handshake drops. Impaired proxies are placed on a cooldown quarantine bench before being re-tested.
- **Process-Safe State Synchronization:** Proxy leases and failure metrics are tracked in JSON state files protected by cross-process file locks.

### 5. Graph Intelligence & 2D Layout Baking for LOD Web Rendering

Loading a 20M+ node graph directly into a browser client crashes WebGL and Canvas visualizers if the client has to compute force-directed layouts (ForceAtlas2) in real time.

SocialGraph Engine solves this with **offline layout baking** directly inside Neo4j:

1. **Neo4j Graph Data Science (GDS) Louvain Community Detection:** Partitions the undirected social graph into modular clusters in $O(E)$ linear time.
2. **GDS FastRP (Fast Random Projection):** Generates structural graph embeddings capturing 1-hop and 2-hop neighborhood proximity in $O(E)$ time.
3. **Hierarchical Sunflower Phyllotaxis Projection:**
   - **Macro-level:** Community centroids are positioned along the 2D plane using a golden-ratio sunflower phyllotaxis spiral, proportionally scaled by cluster size.
   - **Micro-level:** Individual accounts are arranged around their community centroid using polar projections derived from their FastRP embeddings and centrality radius.
   - **Asteroid Belt Isolation:** Disconnected and peripheral nodes are organized along an outer perimeter boundary so they never clutter dense communities.
4. **Precomputed Coordinate Ingestion:** Computed $(x, y)$ coordinates are batch-written back to Neo4j `:User` nodes. Web clients can stream viewport-clipped nodes with Level-of-Detail (LOD) follower count filtering in single-digit milliseconds.

---

## Database Architecture

### PostgreSQL (Transactional Core)

PostgreSQL serves as the primary system of record, maintaining strict ACID guarantees for queue states, relationships, and user stubs:

```
┌─────────────────────────────────┐       ┌─────────────────────────────────┐
│              users              │       │              edges              │
├─────────────────────────────────┤       ├─────────────────────────────────┤
│ id (PK, BigInt)                 │◄──┐   │ from_user_id (PK, FK -> users)  │
│ insta_id (Unique, BigInt)       │   ├───┤ to_user_id   (PK, FK -> users)  │
│ username (Unique, String)       │   │   │ state (String)                  │
│ name, profile_pic (Text)        │   │   │ discovered_at (Timestamp)       │
│ follower_count, following_count │   │   └─────────────────────────────────┘
│ is_seed, is_verified, is_private│   │
│ seed_association_score (Float)  │   │   ┌─────────────────────────────────┐
│ discovery_score (Integer)       │   │   │           crawl_queue           │
│ crawl_state (Enum/String)       │   │   ├─────────────────────────────────┤
│ discovered_by (FK -> users.id)  │   │   │ id (PK, BigInt)                 │
│ last_crawled_at, crawl_count    │   └───┤ user_id (FK -> users.id)        │
│ created_at, updated_at          │       │ priority (Float)                │
└─────────────────────────────────┘       │ queue_type, status, attempts    │
                                          │ available_at, locked_until      │
                                          │ locked_by, last_error           │
                                          │ [Partial Unique Index: active]  │
                                          └─────────────────────────────────┘
```

- **Partial Unique Index:** `ux_crawl_queue_active_user` ensures that at most one active job (`status IN ('pending', 'processing')`) exists per user, while preserving historical audit logs.
- **Alembic Migrations:** Fully version-controlled database schema with explicit deterministic naming conventions (`ix_`, `uq_`, `ck_`, `fk_`, `pk_`).

### Neo4j (Graph Analytics & Visualization)

- **Node Identity:** Maps internal PostgreSQL `users.id` directly as the unique constraint key, preserving integrity against mutable Instagram handles.
- **Indexed Viewport LOD:** Range-indexed `user_follower_count` and `user_insta_id` enable high-speed Cypher queries for zoom-based filtering:
  ```cypher
  MATCH (u:User)
  WHERE u.follower_count >= $min_followers
    AND u.x >= $min_x AND u.x <= $max_x
    AND u.y >= $min_y AND u.y <= $max_y
  RETURN u.id, u.username, u.x, u.y, u.follower_count
  ```

---

## Getting Started

### 1. Prerequisites & Infrastructure Setup

Start the complete infrastructure stack (PostgreSQL, Redpanda, Redpanda Console, pgAdmin, and Neo4j with APOC & GDS plugins):

```bash
# Clone the repository
git clone https://github.com/the-mr-poarbear/social-media-search-engine-graph.git
cd social-media-search-engine-graph

# Spin up containers
docker compose up -d
```

Verify services:
- **PostgreSQL:** `localhost:5432`
- **Redpanda (Kafka):** `localhost:19092` (Console UI at `http://localhost:8080`)
- **Neo4j (Bolt/HTTP):** `localhost:7687` / `http://localhost:7474`
- **pgAdmin:** `http://localhost:5050`

### 2. Environment & Dependency Installation

Create a virtual environment and install the required packages:

```bash
python -m venv venv
venv\Scripts\activate   # Windows (or: source venv/bin/activate on Linux/macOS)
pip install -r requirements.txt
```

Configure your `.env` file:

```env
# Database & Kafka
PG_DSN=postgresql+asyncpg://igpoc:igpoc@localhost:5432/igpoc
KAFKA_BOOTSTRAP=localhost:19092

# Instagram Session Authentication (DevTools -> Application -> Cookies)
IG_SESSIONID=your_session_id_here
IG_DS_USER_ID=your_numeric_user_id

# Crawl & Threshold Settings
FOLLOWER_THRESHOLD=100000
DISCOVERY_QUEUE_SHARE=0.8

# Neo4j Graph Settings
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password_here
```

Apply database migrations:

```bash
alembic upgrade head
```

### 3. Seeding the Graph

Load initial influencer seed datasets into PostgreSQL:

```bash
python -m scripts.load_seed data/seed_sample.csv
```

### 4. Running the Pipeline

You can launch the entire distributed architecture in a single command using the built-in Windows Terminal tab manager:

```bash
python start_pipeline.py --consumers 4 --enrichers 2 --workers 1
```

Or run each service individually across separate terminals:

```bash
# 1. Start Singbox proxy pool
python -m scripts.run_singbox_proxies

# 2. Start graph consumers (scale horizontally)
python -m app.graph_consumer

# 3. Start enrichment workers (scale horizontally)
python -m app.enrichment_worker

# 4. Start crawl worker
python -m app.worker

# 5. Start priority scheduler
python -m app.scheduler
```

### 5. Syncing & Baking Neo4j Graph Analytics

Once crawling has collected graph connections, sync the data into Neo4j and precompute the 2D visualization layout:

```bash
# Stream nodes and edges into Neo4j
python -m graph_operations.load_nodes
python -m graph_operations.load_edges

# Execute Louvain community detection, FastRP embeddings, and sunflower layout baking
python -m graph_operations.bake_layout --scaling-factor 3000 --dimension 16
```

---

## Key Technical Highlights

- **Event-Driven Decoupling:** Redpanda/Kafka message streaming decouples network-bound scraping from graph persistence and API enrichment.
- **Concurrency Without Contention:** Leverages PostgreSQL `FOR UPDATE SKIP LOCKED` for zero-overhead work claiming across parallel schedulers and enrichment workers.
- **Self-Healing Lease Management:** Resilient timed leases with automated stale recovery, handling worker dropouts without orphaned queue jobs.
- **Adaptive PID Pacing:** Custom `AdaptiveDelay` controllers dynamically adapt request intervals based on external API rate-limit responses.
- **Logarithmic Frontier Exploration:** Multi-objective priority formulas prevent hub starvation and ensure discovery of virally emerging influencers.
- **Sub-Second Web Graph Visualization:** Offline layout baking using Louvain community detection and FastRP structural embeddings eliminates browser-side physics computations for 20M+ node graphs.
- **Automated Proxy Rotation:** Resilient Sing-box and V2Ray routing with live latency benchmarks and cooldown quarantine.

## Technologies

**Python · Redpanda / Apache Kafka · PostgreSQL · SQLAlchemy 2.0 (asyncpg) · Alembic · Neo4j & Graph Data Science (GDS) · Docker · Sing-box / V2Ray · NetworkX · NumPy · Scikit-Learn**

## Author

**Arsam Amirsoleimani**

Computer Engineering student focused on Data Engineering, Distributed Systems, Graph Analytics, and Machine Learning.

- [GitHub Profile](https://github.com/the-mr-poarbear)