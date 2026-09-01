"""
Quick verification script for testing enrichment lookups with HypeAuditor,
TrendHero, or Mixed mode.

Usage:
  python -m scripts.test_enrichment --provider mixed --usernames poo_bon,instagram
  python -m scripts.test_enrichment --provider trendhero --usernames poo_bon
  python -m scripts.test_enrichment --provider hypeauditor --usernames poo_bon
"""

import argparse
import asyncio
import json
import logging
import time

import httpx

from app.config import settings
from app.enrichment import (
    hypeauditor_delay,
    lookup_follower_count,
    lookup_hypeauditor,
    lookup_trendhero,
    trendhero_delay,
)
from app.proxy_pool import ProxyPool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("test-enrichment")


async def main():
    parser = argparse.ArgumentParser(description="Test Instagram account enrichment providers.")
    parser.add_argument(
        "--provider",
        type=str,
        default=settings.enrichment_provider,
        choices=["mixed", "trendhero", "hypeauditor"],
        help="Enrichment provider to test (default: settings.enrichment_provider)",
    )
    parser.add_argument(
        "--usernames",
        type=str,
        default="poo_bon",
        help="Comma-separated usernames to lookup (default: 'poo_bon')",
    )
    args = parser.parse_args()

    usernames = [u.strip() for u in args.usernames.split(",") if u.strip()]
    log.info("Starting enrichment test | Provider: %s | Users: %s", args.provider, usernames)

    async with httpx.AsyncClient(timeout=15.0) as http_client:
        proxy_pool = ProxyPool()
        await proxy_pool.refresh(http_client)

        for i, username in enumerate(usernames):
            start = time.monotonic()
            log.info(
                "[%d/%d] Requesting lookup for '%s' (Provider mode: %s)...",
                i + 1,
                len(usernames),
                username,
                args.provider,
            )

            result, exhausted = await lookup_follower_count(
                http_client=http_client,
                proxy_pool=proxy_pool,
                username=username,
                provider=args.provider,
            )
            elapsed = time.monotonic() - start

            log.info(
                "[%d/%d] Finished '%s' in %.2fs | Exhausted: %s | Result:\n%s",
                i + 1,
                len(usernames),
                username,
                elapsed,
                exhausted,
                json.dumps(result, indent=2, ensure_ascii=False) if result else "None",
            )
            log.info(
                "Provider cooldowns -> HypeAuditor remaining: %.2fs | TrendHero remaining: %.2fs",
                hypeauditor_delay.time_until_ready(),
                trendhero_delay.time_until_ready(),
            )

        await proxy_pool.aclose()


if __name__ == "__main__":
    asyncio.run(main())
