"""
Cheap follower-count and profile lookups via public endpoints:
  1. HypeAuditor suggest: https://pdata.hypeauditor.com/suggest/?search={username}&st=ig&excl_st=sn
  2. TrendHero ER reports: https://trendhero.io/api/get_er_reports?username={username}

Supported modes (configured via ENRICHMENT_PROVIDER):
  - "mixed" (default): Interleaves requests between HypeAuditor and TrendHero based on
    whichever provider has the shortest cooldown / is ready first. Maximizes single-worker
    throughput without overloading either external service.
  - "trendhero": Direct queries to TrendHero only.
  - "hypeauditor": Direct queries to HypeAuditor only.

No fallbacks are performed for accounts missing from an enrichment response — if an account
is not found, it is considered not prominent enough for that provider's public index.
"""

import asyncio
from json import JSONDecodeError
import logging
import ssl
import time

import httpcore
import httpx
import socksio.exceptions

from app.adaptive_delay import AdaptiveDelay
from app.config import settings
from app.proxy_pool import ProxyPool

log = logging.getLogger(__name__)

hypeauditor_delay = AdaptiveDelay(
    target=settings.enrich_target_interval,
    initial_sleep=settings.enrich_target_interval,
    min_sleep=settings.enrich_min_sleep,
    max_sleep=settings.enrich_max_sleep,
)

trendhero_delay = AdaptiveDelay(
    target=settings.trendhero_target_interval,
    initial_sleep=settings.trendhero_target_interval,
    min_sleep=settings.trendhero_min_sleep,
    max_sleep=settings.trendhero_max_sleep,
)


async def _fetch_json_with_proxy(
    http_client: httpx.AsyncClient,
    proxy_pool: ProxyPool,
    url: str,
    params: dict,
    username: str,
    delay_tracker: AdaptiveDelay,
    wait_duration: float,
    max_attempts: int = 3,
) -> tuple[dict | None, bool]:
    """Internal helper to execute a proxied GET request with retry, timeout,

    error-handling, and adaptive delay recording.
    """
    for attempt in range(max_attempts):
        resp = None
        proxy = await proxy_pool.get_proxy(http_client)
        start = time.monotonic()

        try:
            req_timeout = httpx.Timeout(
                timeout=settings.proxy_timeout,
                connect=settings.proxy_connect_timeout,
                read=settings.proxy_timeout,
            )

            if proxy is None:
                req_coro = http_client.get(
                    url,
                    params=params,
                    timeout=req_timeout,
                )
            else:
                proxied_client = proxy_pool.get_client_for(proxy)
                req_coro = proxied_client.get(
                    url,
                    params=params,
                    timeout=req_timeout,
                )

            resp = await asyncio.wait_for(req_coro, timeout=settings.proxy_timeout)
            latency = time.monotonic() - start
            resp.raise_for_status()

            if resp is None or not resp.content:
                log.warning("Empty response received for %s via %s", username, proxy.url if proxy else "direct")
                if proxy is not None:
                    if proxy.record_failure("empty_response"):
                        proxy_pool.release_proxy(proxy)
                continue

            data = resp.json()

        except (socksio.exceptions.SOCKSError, httpx.ProxyError, ssl.SSLError) as e:
            latency = time.monotonic() - start
            log.warning(
                "Fatal proxy / SSL error for %s via %s (%.2fs): %s",
                username,
                proxy.url if proxy else "direct",
                latency,
                e,
            )
            if proxy is not None:
                proxy.bench_immediately(f"Proxy/SSL/SOCKS error: {e}")
                proxy_pool.release_proxy(proxy)
            continue

        except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.TimeoutException, TimeoutError, asyncio.TimeoutError) as e:
            latency = time.monotonic() - start
            log.warning(
                "Timeout on attempt %d/%d for %s via %s after %.2fs",
                attempt + 1,
                max_attempts,
                username,
                proxy.url if proxy else "direct",
                latency,
            )
            if proxy is not None:
                if proxy.record_failure("timeout"):
                    proxy_pool.release_proxy(proxy)
            continue

        except httpx.HTTPStatusError as e:
            latency = time.monotonic() - start
            status_code = e.response.status_code
            if status_code in (403, 429):
                log.warning(
                    "HTTP %d (blocked/rate-limited) for %s via %s",
                    status_code,
                    username,
                    proxy.url if proxy else "direct",
                )
                if proxy is not None:
                    proxy.bench_immediately(f"HTTP {status_code}")
                    proxy_pool.release_proxy(proxy)
            elif status_code == 503:
                # Check if TrendHero returned a structured "report_generation_failed" response
                try:
                    err_json = e.response.json()
                except Exception:
                    err_json = None

                if isinstance(err_json, dict) and err_json.get("error_code") == "report_generation_failed":
                    log.info(
                        "TrendHero report unavailable (report_generation_failed) for %s via %s (%.2fs) — not in database",
                        username,
                        proxy.url if proxy else "direct",
                        latency,
                    )
                    if proxy is not None:
                        proxy.record_success(latency)
                    total_time = wait_duration + latency
                    delay_tracker.record(total_time)
                    return None, False

                log.warning(
                    "HTTP 503 (Service Unavailable) for %s via %s (%.2fs) — upstream server error, proxy is unaffected",
                    username,
                    proxy.url if proxy else "direct",
                    latency,
                )
                # Do not record failure on the proxy since 503 is an upstream server issue
            elif status_code == 404:
                log.info(
                    "HTTP 404 for %s via %s (%.2fs) — not found",
                    username,
                    proxy.url if proxy else "direct",
                    latency,
                )
                if proxy is not None:
                    proxy.record_success(latency)
                total_time = wait_duration + latency
                delay_tracker.record(total_time)
                return None, False
            else:
                log.warning(
                    "HTTP %d error for %s via %s (%.2fs): %s",
                    status_code,
                    username,
                    proxy.url if proxy else "direct",
                    latency,
                    e,
                )
                if proxy is not None:
                    if proxy.record_failure(f"HTTP {status_code}"):
                        proxy_pool.release_proxy(proxy)
            continue

        except (httpx.HTTPError, httpcore.NetworkError, httpcore.ProtocolError, OSError, ConnectionError, JSONDecodeError) as e:
            latency = time.monotonic() - start
            log.warning(
                "Request/network error on attempt %d/%d for %s via %s (%.2fs): %s",
                attempt + 1,
                max_attempts,
                username,
                proxy.url if proxy else "direct",
                latency,
                e,
            )
            if proxy is not None:
                if proxy.record_failure(f"Error: {e}"):
                    proxy_pool.release_proxy(proxy)
            continue

        except Exception as e:
            latency = time.monotonic() - start
            log.warning(
                "Unexpected error on attempt %d/%d for %s via %s (%.2fs): %s",
                attempt + 1,
                max_attempts,
                username,
                proxy.url if proxy else "direct",
                latency,
                e,
            )
            if proxy is not None:
                proxy.bench_immediately(f"Unexpected error: {e}")
                proxy_pool.release_proxy(proxy)
            continue

        # Successful response
        if proxy is not None:
            if latency > settings.proxy_max_latency:
                log.warning(
                    "Proxy %s was too slow for %s (%.2fs > max %.2fs threshold)",
                    proxy.url,
                    username,
                    latency,
                    settings.proxy_max_latency,
                )
                if proxy.record_slow(latency):
                    proxy_pool.release_proxy(proxy)
            else:
                proxy.record_success(latency)

        total_time = wait_duration + latency
        delay_tracker.record(total_time)
        return data, False

    log.warning("All %d attempts exhausted for %s", max_attempts, username)
    return None, True


async def lookup_hypeauditor(
    http_client: httpx.AsyncClient,
    proxy_pool: ProxyPool,
    username: str,
    max_attempts: int = 3,
) -> tuple[dict | None, bool]:
    """Look up follower count and basic info via HypeAuditor's suggest endpoint."""
    wait_duration = await hypeauditor_delay.wait()
    params = {"search": username, "st": "ig", "excl_st": "sn"}

    data, exhausted = await _fetch_json_with_proxy(
        http_client=http_client,
        proxy_pool=proxy_pool,
        url=settings.hypeauditor_url,
        params=params,
        username=username,
        delay_tracker=hypeauditor_delay,
        wait_duration=wait_duration,
        max_attempts=max_attempts,
    )

    if data is None:
        return None, exhausted

    for item in data.get("list", []):
        if item.get("username", "").lower() == username.lower():
            user_id = item.get("user_id")
            return {
                "follower_count": item.get("followers_count"),
                "is_verified": item.get("is_verified"),
                "is_private": item.get("is_private"),
                "insta_id": int(user_id) if user_id and str(user_id).isdigit() else None,
                "name": item.get("full_name") or None,
            }, False

    return None, False


async def lookup_trendhero(
    http_client: httpx.AsyncClient,
    proxy_pool: ProxyPool,
    username: str,
    max_attempts: int = 3,
) -> tuple[dict | None, bool]:
    """Look up follower count and profile metadata via TrendHero's get_er_reports endpoint."""
    wait_duration = await trendhero_delay.wait()
    params = {"username": username}

    data, exhausted = await _fetch_json_with_proxy(
        http_client=http_client,
        proxy_pool=proxy_pool,
        url=settings.trendhero_url,
        params=params,
        username=username,
        delay_tracker=trendhero_delay,
        wait_duration=wait_duration,
        max_attempts=max_attempts,
    )

    if data is None:
        return None, exhausted

    preview = data.get("preview")
    if not isinstance(preview, dict):
        log.warning("TrendHero response missing 'preview' object for %s", username)
        return None, False

    user_info = preview.get("user_info")
    if not isinstance(user_info, dict):
        log.warning("TrendHero response missing 'user_info' object for %s", username)
        return None, False

    # Exact username match verification
    ret_username = user_info.get("username", "")
    if ret_username.lower() != username.lower():
        log.warning("TrendHero returned mismatched username '%s' (expected '%s')", ret_username, username)
        return None, False

    pk = user_info.get("pk") or user_info.get("id")
    insta_id = int(pk) if pk and str(pk).isdigit() else None
    category = user_info.get("overall_category_name") or user_info.get("business_category_name") or None
    country = user_info.get("country") or None

    return {
        "follower_count": user_info.get("follower_count"),
        "following_count": user_info.get("following_count"),
        "is_verified": user_info.get("is_verified"),
        "is_private": user_info.get("is_private"),
        "insta_id": insta_id,
        "name": user_info.get("full_name") or None,
        "profile_pic": user_info.get("profile_pic_url") or None,
        "category": category,
        "country": country,
    }, False


async def lookup_follower_count(
    http_client: httpx.AsyncClient,
    proxy_pool: ProxyPool,
    username: str,
    provider: str | None = None,
    max_attempts: int = 3,
) -> tuple[dict | None, bool]:
    """Primary enrichment entry point. Supports 'mixed', 'trendhero', and 'hypeauditor'.

    In 'mixed' mode, it automatically selects whichever provider has the shortest
    remaining cooldown (ready soonest), waits for it, and returns the result.
    """
    active_provider = (provider or settings.enrichment_provider).lower()

    if active_provider == "trendhero":
        return await lookup_trendhero(http_client, proxy_pool, username, max_attempts=max_attempts)
    elif active_provider == "hypeauditor":
        return await lookup_hypeauditor(http_client, proxy_pool, username, max_attempts=max_attempts)
    elif active_provider == "mixed":
        wait_hype = hypeauditor_delay.time_until_ready()
        wait_trend = trendhero_delay.time_until_ready()

        if wait_hype <= wait_trend:
            return await lookup_hypeauditor(http_client, proxy_pool, username, max_attempts=max_attempts)
        else:
            return await lookup_trendhero(http_client, proxy_pool, username, max_attempts=max_attempts)
    else:
        log.warning("Unknown enrichment provider '%s', defaulting to HypeAuditor", active_provider)
        return await lookup_hypeauditor(http_client, proxy_pool, username, max_attempts=max_attempts)