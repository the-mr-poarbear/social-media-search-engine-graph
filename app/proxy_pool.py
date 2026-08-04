import asyncio
import logging
import random
import time

import httpx

log = logging.getLogger(__name__)

PROXYSCRAPE_URL = "https://api.proxyscrape.com/v4/free-proxy-list/get"

REFRESH_INTERVAL_SECONDS = 5 * 60
MAX_CONSECUTIVE_FAILURES = 3
BENCH_SECONDS = 10 * 60
MIN_ANONYMITY = "elite"
PROTOCOL="socks5"


class _Proxy:
    __slots__ = ("url", "consecutive_failures", "benched_until")

    def __init__(self, url: str):
        self.url = url
        self.consecutive_failures = 0
        self.benched_until = 0.0

    @property
    def is_available(self) -> bool:
        return time.monotonic() >= self.benched_until

    def record_success(self):
        self.consecutive_failures = 0

    def record_failure(self):
        self.consecutive_failures += 1

        if self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            self.benched_until = time.monotonic() + BENCH_SECONDS

            log.info(
                "benching proxy %s for %ds after %d consecutive failures",
                self.url,
                BENCH_SECONDS,
                self.consecutive_failures,
            )

            return True  # tell caller it failed permanently for now

        return False



class ProxyPool:
    def __init__(self):
        self._proxies: list[_Proxy] = []
        self._clients: dict[str, httpx.AsyncClient] = {}
        self._current_proxy: _Proxy | None = None
        self._last_refresh = 0.0
        self._lock = asyncio.Lock()

    async def refresh(self, http_client: httpx.AsyncClient):
        async with self._lock:
            resp = await http_client.get(
                PROXYSCRAPE_URL,
                params={
                    "request": "display_proxies",
                    "proxy_format": "protocolipport",
                    "format": "json",
                    "protocol":PROTOCOL
                },
            )
            resp.raise_for_status()
            data = resp.json()

            live_urls = [
                p["proxy"]
                for p in data.get("proxies", [])
                if p.get("alive") and p.get("anonymity") == MIN_ANONYMITY
            ]

            existing = {p.url: p for p in self._proxies}
            self._proxies = [existing.get(url, _Proxy(url)) for url in live_urls]

            # Close clients for proxies that no longer exist
            live_set = set(live_urls)
            for url, client in list(self._clients.items()):
                if url not in live_set:
                    try:
                        await client.aclose()
                    except Exception:
                        pass
                    del self._clients[url]

            self._last_refresh = time.monotonic()

            log.info("proxy pool refreshed: %d live proxies", len(self._proxies))

    async def _maybe_refresh(self, http_client: httpx.AsyncClient):
        if (
            not self._proxies
            or (time.monotonic() - self._last_refresh)
            > REFRESH_INTERVAL_SECONDS
        ):
            await self.refresh(http_client)

    async def get_proxy(
        self,
        http_client: httpx.AsyncClient
    ) -> "_Proxy | None":

        await self._maybe_refresh(http_client)

        # Keep current proxy if still alive
        if self._current_proxy and self._current_proxy.is_available:
            return self._current_proxy


        available = [
            p for p in self._proxies
            if p.is_available
        ]

        if not available:
            return None


        self._current_proxy = random.choice(available)

        log.info(
            "new proxy assigned: %s",
            self._current_proxy.url
        )

        return self._current_proxy

    def release_proxy(self, proxy: _Proxy):
        if self._current_proxy == proxy:
            self._current_proxy = None
            
    def get_client_for(self, proxy: _Proxy) -> httpx.AsyncClient:
        """
        Returns a cached AsyncClient configured for this proxy.
        """
        client = self._clients.get(proxy.url)

        if client is None:
            client = httpx.AsyncClient(
                proxy=proxy.url,
                timeout=httpx.Timeout(20.0),
                follow_redirects=True,
                limits=httpx.Limits(
                    max_connections=10,
                    max_keepalive_connections=5,
                ),
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/139.0.0.0 Safari/537.36"
                    )
                },
            )

            self._clients[proxy.url] = client

        return client

    async def close(self):
        """
        Close all cached AsyncClients.
        """
        for client in self._clients.values():
            try:
                await client.aclose()
            except Exception:
                pass

        self._clients.clear()