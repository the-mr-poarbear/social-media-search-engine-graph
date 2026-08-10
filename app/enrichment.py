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

import logging
import httpx
import asyncio
import random
from app.adaptive_delay import AdaptiveDelay
from app.proxy_pool import ProxyPool
from app.config import settings
import socksio.exceptions
from json import JSONDecodeError


log = logging.getLogger(__name__)


HYPEAUDITOR_SUGGEST_URL = "https://pdata.hypeauditor.com/suggest/"

adaptive_delay = AdaptiveDelay(
    target=settings.enrich_target_interval,
    initial_sleep=0
)


async def lookup_follower_count(
    http_client: httpx.AsyncClient,
    proxy_pool: ProxyPool,
    username: str,
    max_attempts: int = 3,
) -> tuple[dict | None, bool]:
    """Returns {follower_count, is_verified, is_private, hypeauditor_user_id} or None if not found.

    http_client is used unproxied for two things: refreshing the proxy list
    itself, and as a last-resort fallback if the pool has no available
    proxy right now (all benched) — better to occasionally eat a direct
    request than to fail the lookup outright.
    """
    params = {"search": username, "st": "ig", "excl_st": "sn"}

    exhausted_retries = False

    for attempt in range(max_attempts):
        resp = None
        proxy = await proxy_pool.get_proxy(http_client)
        # await asyncio.sleep(random.uniform(10, 10))
        await adaptive_delay.wait()
        start = asyncio.get_event_loop().time()

        try:
            if proxy is None:
                resp = await http_client.get(HYPEAUDITOR_SUGGEST_URL, params=params)
            else:
                proxied_client = proxy_pool.get_client_for(proxy)
                resp = await proxied_client.get(HYPEAUDITOR_SUGGEST_URL, params=params)
            resp.raise_for_status()

            
            response_time = asyncio.get_event_loop().time() - start
            total_time = adaptive_delay.sleep_time + response_time
            adaptive_delay.record(total_time)

        except httpx.HTTPError as e:
            response = getattr(e, "response", None)
            proxy_pool.release_proxy(proxy)

            if response is not None and response.status_code == 403:
                log.warning("403 forbidden for %s via %s, benching proxy", username, proxy.url if proxy else "direct")

            elif proxy is not None:
                should_release = proxy.record_failure()

                # proxy_pool.release_proxy(proxy)    
                    
                log.warning("attempt %d/%d failed for %s via %s: %s",
                            attempt + 1, max_attempts, username, proxy.url if proxy else "direct", e)
            continue

        except (socksio.exceptions.ProtocolError) as e:
            log.warning(
                "Proxy %s returned malformed SOCKS reply, removing immediately.",
                proxy.url,
            )
            # proxy.benched_until = time.monotonic() + BENCH_SECONDS
            # proxy.record_failure()
            proxy_pool.release_proxy(proxy)
            continue
            
 
        
        try:
            if resp is None:
                log.warning("resp is None for %s ! releasing proxy", username)
                proxy_pool.release_proxy(proxy)
                continue
                
            elif resp.status_code == 200 and proxy is not None:
                proxy.record_success()

            data = resp.json()
            
        except JSONDecodeError:
            log.warning(
                "Invalid JSON response. status=%s body=%s",
                resp.status_code,
                resp.text[:500]
            )
            proxy_pool.release_proxy(proxy)
            continue
        
        for item in data.get("list", []):
            if item.get("username", "").lower() == username.lower():
                return {
                    "follower_count": item.get("followers_count"),
                    "is_verified": item.get("is_verified"),
                    "is_private": item.get("is_private"),
                    "insta_id": int(item.get("user_id")),
                    "name": item.get("full_name"),     
                } , exhausted_retries
            
        return None , exhausted_retries  # request succeeded, username just wasn't in the results

    log.warning("all %d attempts exhausted for %s", max_attempts, username)
    exhausted_retries = True
    return None , exhausted_retries