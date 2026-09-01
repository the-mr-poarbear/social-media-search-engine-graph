"""
bake_layout.py

Precomputes a 2D layout for the graph and writes x, y back onto
every :User node as properties -- so the frontend reads baked positions
instead of running a layout algorithm on every render/zoom.

Architecture for 20M+ Users:
- Neo4j Graph Data Science (GDS) Louvain Community Detection:
  Partitions the graph into modular clusters in O(E) time, ensuring
  direct neighbors and dense follower/followee communities share the
  same visual territory.
- Neo4j GDS FastRP (Fast Random Projection):
  Generates structural graph embeddings in O(E) time with diffusion weights
  tuned specifically for 1-hop and 2-hop neighborhood proximity.
- Hierarchical Sunflower / Polar Projection in Python:
  1. Macro-level: Community centroids are placed across the 2D plane using
     an optimal Sunflower phyllotaxis disk distribution, scaled by community size.
  2. Micro-level: Members within each community are positioned around their
     community centroid using circular projection of their FastRP embedding vectors
     and centrality radius.
  3. Isolated nodes (zero edges) are gracefully placed along an outer perimeter
     asteroid belt so they never clutter connected communities.
  4. Vectorized with NumPy for near-instant execution and streamed back in batches.

Run:
    python -m graph_operations.bake_layout
    python -m graph_operations.bake_layout --scaling-factor 3000 --dimension 16
"""
import argparse
import asyncio
import math
import time
import numpy as np

from app.config import settings
from .neo4j_client import Neo4jLoader


async def verify_gds(driver) -> str:
    """Verifies that the Neo4j GDS plugin is available and returns its version."""
    async with driver.session() as session:
        try:
            result = await session.run("CALL gds.version()")
            record = await result.single()
            if record:
                return str(record[0])
        except Exception as e:
            raise RuntimeError(
                f"Neo4j Graph Data Science (GDS) plugin is not available: {e}. "
                "Ensure NEO4J_PLUGINS=[\"apoc\", \"graph-data-science\"] is configured in Docker."
            ) from e
    raise RuntimeError("Neo4j GDS plugin did not return a valid version.")


async def project_graph(driver, graph_name: str) -> dict:
    """
    Projects the :User nodes and :FOLLOWS edges into GDS in-memory graph catalog.
    All :User nodes (including isolated ones) are included.
    Treats FOLLOWS as UNDIRECTED for symmetric community and neighborhood discovery.
    """
    await drop_projected_graph(driver, graph_name)
    async with driver.session() as session:
        result = await session.run(
            """
            CALL gds.graph.project(
                $graph_name,
                'User',
                {
                    FOLLOWS: {
                        type: 'FOLLOWS',
                        orientation: 'UNDIRECTED'
                    }
                }
            )
            YIELD graphName, nodeCount, relationshipCount, projectMillis
            """,
            graph_name=graph_name,
        )
        record = await result.single()
        return dict(record) if record else {}


async def run_louvain(
    driver,
    graph_name: str,
    max_levels: int = 10,
    max_iterations: int = 15,
) -> dict:
    """
    Executes Louvain Community Detection inside GDS.
    Groups tightly connected neighbors into modular communities.
    """
    async with driver.session() as session:
        result = await session.run(
            """
            CALL gds.louvain.mutate(
                $graph_name,
                {
                    mutateProperty: 'communityId',
                    maxLevels: $max_levels,
                    maxIterations: $max_iterations
                }
            )
            YIELD communityCount, modularity, modularities, ranLevels, computeMillis
            """,
            graph_name=graph_name,
            max_levels=max_levels,
            max_iterations=max_iterations,
        )
        record = await result.single()
        return dict(record) if record else {}


async def run_fastrp(
    driver,
    graph_name: str,
    dimension: int = 16,
    iteration_weights: list[float] | None = None,
    random_seed: int = 42,
) -> dict:
    """
    Executes FastRP algorithm inside Neo4j GDS.
    Iteration weights prioritize 1-hop direct neighbors [0.0, 1.0, 0.4, 0.1].
    """
    if iteration_weights is None:
        # Weight 1-hop connections most heavily for tight neighbor proximity
        iteration_weights = [0.0, 1.0, 0.4, 0.1]

    async with driver.session() as session:
        result = await session.run(
            """
            CALL gds.fastRP.mutate(
                $graph_name,
                {
                    embeddingDimension: $dimension,
                    iterationWeights: $iteration_weights,
                    mutateProperty: 'fastrp_embedding',
                    randomSeed: $random_seed
                }
            )
            YIELD nodePropertiesWritten, computeMillis
            """,
            graph_name=graph_name,
            dimension=dimension,
            iteration_weights=iteration_weights,
            random_seed=random_seed,
        )
        record = await result.single()
        return dict(record) if record else {}


async def stream_graph_data(driver, graph_name: str) -> tuple[list[str], np.ndarray, np.ndarray]:
    """
    Streams node IDs, community IDs, and FastRP embeddings from the GDS projection.
    """
    node_ids: list[str] = []
    community_ids: list[int] = []
    embeddings: list[list[float]] = []

    # Stream community IDs
    async with driver.session() as session:
        result = await session.run(
            """
            CALL gds.graph.nodeProperty.stream($graph_name, 'communityId')
            YIELD nodeId, propertyValue AS communityId
            RETURN nodeId, gds.util.asNode(nodeId).id AS id, communityId
            ORDER BY nodeId ASC
            """,
            graph_name=graph_name,
        )
        async for record in result:
            node_ids.append(str(record["id"]))
            community_ids.append(record["communityId"])

    # Stream FastRP embeddings
    async with driver.session() as session:
        result = await session.run(
            """
            CALL gds.graph.nodeProperty.stream($graph_name, 'fastrp_embedding')
            YIELD nodeId, propertyValue AS embedding
            RETURN embedding
            ORDER BY nodeId ASC
            """,
            graph_name=graph_name,
        )
        async for record in result:
            embeddings.append(record["embedding"])

    comm_arr = np.array(community_ids, dtype=np.int64)
    emb_arr = np.array(embeddings, dtype=np.float32)
    return node_ids, comm_arr, emb_arr


def compute_hierarchical_2d_positions(
    node_ids: list[str],
    community_ids: np.ndarray,
    embeddings: np.ndarray,
    scaling_factor: float = 2000.0,
    random_seed: int = 42,
) -> np.ndarray:
    """
    Computes 2D (x, y) coordinates using a fully-vectorized hierarchical approach:
    1. Macro-level: Community centroids are placed using a Sunflower phyllotaxis disk layout.
    2. Micro-level: Nodes within each community are placed around their centroid using
       FastRP circular projections and degree/centrality radius.
    3. Isolated nodes are positioned in an outer asteroid belt.
    """
    n_nodes = len(node_ids)
    coords_2d = np.zeros((n_nodes, 2), dtype=np.float32)
    if n_nodes == 0:
        return coords_2d

    rng = np.random.RandomState(random_seed)

    # 1. Identify isolated nodes (zero embedding norm)
    norms = np.linalg.norm(embeddings, axis=1)
    isolated_mask = norms < 1e-6
    connected_mask = ~isolated_mask

    # 2. Position connected communities and nodes
    if np.any(connected_mask):
        conn_communities = community_ids[connected_mask]
        unique_comms, comm_counts = np.unique(conn_communities, return_counts=True)

        # Sort communities by size descending (largest placed at center)
        sort_order = np.argsort(-comm_counts)
        sorted_comms = unique_comms[sort_order]
        sorted_counts = comm_counts[sort_order]

        n_comms = len(sorted_comms)
        golden_angle = np.float32(math.pi * (3.0 - math.sqrt(5.0)))  # ~137.5 degrees

        # Vectorized Sunflower phyllotaxis for community centroids
        ranks = np.arange(n_comms, dtype=np.float32)
        if n_comms == 1:
            cx_comms = np.zeros(1, dtype=np.float32)
            cy_comms = np.zeros(1, dtype=np.float32)
        else:
            r_comms = (scaling_factor * 0.75 * np.sqrt((ranks + 0.5) / n_comms)).astype(np.float32)
            theta_comms = ranks * golden_angle
            cx_comms = r_comms * np.cos(theta_comms)
            cy_comms = r_comms * np.sin(theta_comms)

        avg_count = float(np.mean(sorted_counts)) if n_comms > 0 else 1.0
        radii_comms = scaling_factor * 0.18 * np.sqrt(sorted_counts.astype(np.float32) / max(avg_count, 1.0))
        radii_comms = np.clip(radii_comms, 25.0, scaling_factor * 0.4).astype(np.float32)

        # Fast lookup from community ID to centroid index
        comm_to_idx = {cid: idx for idx, cid in enumerate(sorted_comms)}
        node_comm_idx = np.array([comm_to_idx[c] for c in conn_communities], dtype=np.int32)

        node_cx = cx_comms[node_comm_idx]
        node_cy = cy_comms[node_comm_idx]
        node_cr = radii_comms[node_comm_idx]

        # FastRP circular harmonic projection for intra-cluster angle
        dim = embeddings.shape[1]
        harmonics = np.arange(dim, dtype=np.float32)
        cos_harmonics = np.cos(2.0 * np.pi * harmonics / dim).astype(np.float32)
        sin_harmonics = np.sin(2.0 * np.pi * harmonics / dim).astype(np.float32)

        conn_embs = embeddings[connected_mask]
        u = np.dot(conn_embs, cos_harmonics)
        v = np.dot(conn_embs, sin_harmonics)
        angles = np.arctan2(v, u)

        # Radial distance within cluster: sigmoid mapping of embedding norm
        conn_norms = norms[connected_mask]
        norm_factor = 1.0 / (1.0 + np.exp(-np.minimum(conn_norms, 10.0)))
        r_intra = node_cr * (0.15 + 0.80 * norm_factor)

        # Deterministic jitter using node indices
        conn_indices = np.arange(np.sum(connected_mask), dtype=np.int64)
        jitter_angle = ((conn_indices % 10000).astype(np.float32) / 10000.0) * (2.0 * np.pi)
        jitter_r = 1.5 + (conn_indices % 5).astype(np.float32) * 0.8

        coords_2d[connected_mask, 0] = node_cx + r_intra * np.cos(angles) + jitter_r * np.cos(jitter_angle)
        coords_2d[connected_mask, 1] = node_cy + r_intra * np.sin(angles) + jitter_r * np.sin(jitter_angle)

    # 3. Position isolated nodes
    n_isolated = int(np.sum(isolated_mask))
    if n_isolated > 0:
        iso_angles = rng.uniform(0, 2 * np.pi, size=n_isolated).astype(np.float32)
        iso_radii = rng.uniform(scaling_factor * 1.35, scaling_factor * 1.75, size=n_isolated).astype(np.float32)
        coords_2d[isolated_mask, 0] = iso_radii * np.cos(iso_angles)
        coords_2d[isolated_mask, 1] = iso_radii * np.sin(iso_angles)

    return coords_2d


async def drop_projected_graph(driver, graph_name: str):
    """Frees the in-memory graph from Neo4j memory if it exists."""
    async with driver.session() as session:
        try:
            result = await session.run(
                "CALL gds.graph.exists($graph_name) YIELD exists",
                graph_name=graph_name,
            )
            record = await result.single()
            if record and record.get("exists", False):
                await session.run("CALL gds.graph.drop($graph_name) YIELD graphName", graph_name=graph_name)
        except Exception:
            pass


async def write_positions(loader: Neo4jLoader, node_ids: list[str], coords_2d: np.ndarray, batch_size: int):
    """Writes computed (x, y) coordinates to Neo4j in parameterized batches."""
    total = len(node_ids)
    for i in range(0, total, batch_size):
        end = min(i + batch_size, total)
        batch = [
            {
                "id": str(node_ids[j]),
                "x": round(float(coords_2d[j, 0]), 3),
                "y": round(float(coords_2d[j, 1]), 3),
            }
            for j in range(i, end)
        ]
        await loader.upsert_positions_batch(batch)
        print(f"Wrote positions for {end:,}/{total:,} nodes")


async def main(
    dimension: int = 16,
    scaling_factor: float = 2000.0,
    batch_size: int = 5000,
    graph_name: str = "layout_graph",
    random_seed: int = 42,
    max_louvain_levels: int = 10,
    max_louvain_iterations: int = 15,
):
    loader = Neo4jLoader(settings.neo4j_uri, settings.neo4j_user, settings.neo4j_password)
    t0 = time.perf_counter()

    try:
        # 1. Verify GDS availability
        print("Checking Neo4j Graph Data Science (GDS) plugin...")
        gds_ver = await verify_gds(loader.driver)
        print(f"Neo4j GDS active (v{gds_ver})")

        # 2. Project in-memory graph
        print(f"Projecting graph into GDS memory catalog ('{graph_name}')...")
        proj_stats = await project_graph(loader.driver, graph_name)
        node_count = proj_stats.get("nodeCount", 0)
        rel_count = proj_stats.get("relationshipCount", 0)
        proj_ms = proj_stats.get("projectMillis", 0)
        print(
            f"Graph projected in {proj_ms:,}ms: {node_count:,} nodes, {rel_count:,} relationships"
        )

        if node_count == 0:
            print("No :User nodes found in database. Exiting.")
            return

        # 3. Run Louvain Community Detection (O(E))
        print("Running Louvain Community Detection (grouping direct neighbors)...")
        louvain_stats = await run_louvain(
            loader.driver,
            graph_name=graph_name,
            max_levels=max_louvain_levels,
            max_iterations=max_louvain_iterations,
        )
        comm_count = louvain_stats.get("communityCount", 0)
        louvain_ms = louvain_stats.get("computeMillis", 0)
        print(f"Louvain completed in {louvain_ms:,}ms: {comm_count:,} communities discovered")

        # 4. Run FastRP Embedding (O(E)) with 1-hop neighbor priority
        print(f"Running FastRP structural embedding (dim={dimension}, seed={random_seed})...")
        fastrp_stats = await run_fastrp(
            loader.driver,
            graph_name=graph_name,
            dimension=dimension,
            random_seed=random_seed,
        )
        compute_ms = fastrp_stats.get("computeMillis", 0)
        print(f"FastRP completed in {compute_ms:,}ms")

        # 5. Stream data from Neo4j
        print("Streaming graph metadata and embeddings from Neo4j...")
        node_ids, community_ids, embeddings = await stream_graph_data(loader.driver, graph_name)
        print(
            f"Received data for {len(node_ids):,} nodes "
            f"({embeddings.nbytes / (1024*1024):.2f} MB)"
        )

        # 6. Compute Hierarchical 2D coordinates (Vectorized NumPy)
        print(f"Computing hierarchical multi-scale 2D coordinates (scale={scaling_factor})...")
        coords_2d = compute_hierarchical_2d_positions(
            node_ids,
            community_ids,
            embeddings,
            scaling_factor=scaling_factor,
            random_seed=random_seed,
        )

        # 7. Write positions back to Neo4j
        print(f"Writing (x, y) positions back to Neo4j in batches of {batch_size:,}...")
        await write_positions(loader, node_ids, coords_2d, batch_size)

    finally:
        # 8. Clean up in-memory GDS projection
        print("Cleaning up in-memory GDS projection...")
        await drop_projected_graph(loader.driver, graph_name)
        await loader.close()

    total_time = time.perf_counter() - t0
    print(f"Done in {total_time:.2f}s! Every :User node now has baked .x / .y properties.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Precompute 2D graph layout via Neo4j GDS Louvain + FastRP Hierarchical Layout"
    )
    parser.add_argument(
        "--dimension",
        type=int,
        default=16,
        help="FastRP embedding dimension (default: 16).",
    )
    parser.add_argument(
        "--scaling-factor",
        type=float,
        default=2000.0,
        help="Coordinate spread factor / canvas radius (default: 2000.0).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=settings.batch_size or 5000,
        help="Batch size for writing coordinates back to Neo4j.",
    )
    parser.add_argument(
        "--graph-name",
        type=str,
        default="layout_graph",
        help="In-memory GDS projected graph name.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible layout.",
    )
    parser.add_argument(
        "--louvain-levels",
        type=int,
        default=10,
        help="Max levels for Louvain community detection (default: 10).",
    )
    parser.add_argument(
        "--louvain-iterations",
        type=int,
        default=15,
        help="Max iterations per level for Louvain (default: 15).",
    )
    args = parser.parse_args()

    asyncio.run(
        main(
            dimension=args.dimension,
            scaling_factor=args.scaling_factor,
            batch_size=args.batch_size,
            graph_name=args.graph_name,
            random_seed=args.seed,
            max_louvain_levels=args.louvain_levels,
            max_louvain_iterations=args.louvain_iterations,
        )
    )