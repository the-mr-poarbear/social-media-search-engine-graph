"""
Priority score used to order crawl_queue.

score = W_FOLLOWERS * log1p(follower_count)
      + W_SEED       * seed_association_score
      + W_DISCOVERY  * log1p(discovery_score)

- log1p on follower_count so a jump from 100K -> 1M doesn't dominate a
  jump from 1M -> 10M as much as raw counts would.
- discovery_score (in-degree proxy) also log-scaled: being found once vs
  twice should matter more than being found 50 vs 51 times.
- seed_association_score is already a small bounded number from the seed
  dataset (how many ranking lists a user appeared on), used as-is.

These weights are a starting point, not tuned — the point of the POC is to
see whether the shape of the resulting crawl order looks sane, then adjust.
"""

import math

W_FOLLOWERS = 1.0
W_SEED = 2.0
W_DISCOVERY = 1.5


def compute_priority(
    follower_count: int | None,
    seed_association_score: float | None,
    discovery_score: int | None,
) -> float:
    followers_term = W_FOLLOWERS * math.log1p(max(follower_count or 0, 0))
    seed_term = W_SEED * (seed_association_score or 0.0)
    discovery_term = W_DISCOVERY * math.log1p(max(discovery_score or 0, 0))
    return followers_term + seed_term + discovery_term
