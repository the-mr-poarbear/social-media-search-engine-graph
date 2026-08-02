"""
Cheap follower-count lookups via HypeAuditor's public suggest endpoint —
the same one their own site's search box calls. No auth required.

    GET https://pdata.hypeauditor.com/suggest/?search={username}&st=ig&excl_st=sn
    -> {"success": true, "list": [{"username": ..., "followers_count": ..., ...}]}

This is a *substring* search, not an exact lookup — searching "poo_bon" can
also return "poo_bong.9" — so we filter the response for an exact username
match ourselves.

Why this exists: Instagram's own following-list endpoint doesn't return
follower counts per account (see the caveat in app/scraper.py), so checking
the >100K threshold against IG directly means one full `resolve_user` call
per newly-discovered account. This endpoint is meant to be hit from a public
search box, so it tolerates far more volume/parallelism than an authenticated
IG session does — use it to decide "worth queueing at all" cheaply, and save
the real IG resolve_user() call for the moment an account is actually about
to be crawled (see app/worker.py).

NOT wired up here: trendhero's get_er_reports endpoint. A quick check
showed it returning data for a *different* username than the one requested
(looked like a cached/shared response, not a per-username lookup) — verify
that behaves correctly against your own account before trusting it for
anything.
"""

import httpx

HYPEAUDITOR_SUGGEST_URL = "https://pdata.hypeauditor.com/suggest/"


async def lookup_follower_count(client: httpx.AsyncClient, username: str) -> dict | None:
    """Returns {follower_count, is_verified, is_private, hypeauditor_user_id} or None if not found."""
    resp = await client.get(
        HYPEAUDITOR_SUGGEST_URL,
        params={"search": username, "st": "ig", "excl_st": "sn"},
    )
    resp.raise_for_status()
    data = resp.json()
    for item in data.get("list", []):
        if item.get("username", "").lower() == username.lower():
            return {
                "follower_count": item.get("followers_count"),
                "is_verified": item.get("is_verified"),
                "is_private": item.get("is_private"),
                "insta_id": item.get("user_id"),
            }
    return None
