"""
Instagram scraping via the same internal endpoints the web client itself
calls, authenticated with cookies lifted from a real logged-in browser
session (sessionid / csrftoken / ds_user_id). This is the "authenticated
session, not browser fetch()" approach — plain HTTP calls with the right
headers/cookies, no headless browser needed.

Two endpoints:
  - web_profile_info: username -> numeric user id + basic stats
  - friendships/{id}/following: paginated following list

NOTE: these are undocumented/reverse-engineered endpoints. They can change
shape or get more aggressive about detection without notice. For this POC
there's no proxy rotation and no multi-account pooling — a single session
WILL get rate limited or challenged if you push it hard. That's expected;
the point right now is to prove the pipeline shape, not to survive at scale.
"""

import asyncio
import json
import logging
import random
from dataclasses import dataclass

import httpx

from app.config import settings

log = logging.getLogger(__name__)

BASE_URL = "https://i.instagram.com/api/v1"
WEB_BASE_URL = "https://www.instagram.com/api/v1"

USER_AGENT = (
"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
)


class RateLimited(Exception):
    pass


class AuthExpired(Exception):
    pass


class ScrapeError(Exception):
    pass


@dataclass
class FollowedAccount:
    insta_id: int
    username: str
    name: str | None
    is_verified: bool
    profile_pic: str | None
    follower_count: int | None = None  # not present on the following list itself
    is_private: bool | None = None 


class InstagramSession:
    def __init__(self, proxy: str | None = None):
        if not settings.ig_sessionid:
            raise RuntimeError(
                "Missing IG_SESSIONID env var. "
                "Pull this from your browser's cookie jar for instagram.com "
                "while logged in, then export it before running a worker."
            )
        
        self.proxy = proxy if proxy is not None else (settings.worker_proxy or None)

        cookies = {"sessionid": settings.ig_sessionid}
        if settings.ig_ds_user_id:
            cookies["ds_user_id"] = settings.ig_ds_user_id

        self._client = httpx.AsyncClient(
            proxy=self.proxy,
            headers={
                "User-Agent": USER_AGENT,
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "Pragma": "no-cache",
                "Cache-Control": "no-cache",
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Origin": "https://www.instagram.com",
                "Content-Type": "application/x-www-form-urlencoded",
                "Connection": "keep-alive",
                "Referer": "https://www.instagram.com/",
                "X-Requested-With": "XMLHttpRequest",
                "x-ig-app-id": settings.ig_app_id,
            },
            cookies=cookies,
            timeout=15.0,
            follow_redirects=True,
        )

    async def aclose(self):
        await self._client.aclose()

    async def _throttled_post(self, url: str , payload: dict | None = None, params: dict | None = None) -> httpx.Response:
        # Randomized delay before every call — cheap POC-grade politeness.
        await asyncio.sleep(random.uniform(settings.request_min_delay, settings.request_max_delay))
        resp = await self._client.post(url, json=payload, params=params)
        if resp.status_code == 429:
            raise RateLimited(f"429 on {url}")
        if resp.status_code in (401, 403) or "/accounts/login" in str(resp.url):
            raise AuthExpired(f"{resp.status_code} on {url} — session cookie likely expired/challenged")
        if resp.status_code != 200:
            raise ScrapeError(f"unexpected status {resp.status_code} on {url}: {resp.text[:200]}")
        return resp

    async def _throttled_get(self, url: str , params: dict | None = None) -> httpx.Response:
        # Randomized delay before every call — cheap POC-grade politeness.
        await asyncio.sleep(random.uniform(settings.request_min_delay, settings.request_max_delay))
        resp = await self._client.get(url, params=params)
        if resp.status_code == 429:
            raise RateLimited(f"429 on {url}")
        if resp.status_code in (401, 403) or "/accounts/login" in str(resp.url):
            raise AuthExpired(f"{resp.status_code} on {url} — session cookie likely expired/challenged")
        if "checkpoint_required" in resp.text:
            raise AuthExpired("checkpoint_required detected — account challenged")
        if resp.status_code != 200:
            raise ScrapeError(f"unexpected status {resp.status_code} on {url}: {resp.text[:200]}")
        return resp

    async def resolve_user(self, insta_id: str) -> dict:
        """insta_id -> {insta_id, follower_count, following_count, is_verified, ...}"""
        resp = await self._throttled_post(
            f"{WEB_BASE_URL}/graphql/", payload={
                'fb_dtsg': settings.fb_dtsg,
                'variables': f'{{"id":{insta_id},"__relay_internal__pv__PolarisCannesGuardianExperienceEnabledrelayprovider":true,"__relay_internal__pv__PolarisCASB976ProfileEnabledrelayprovider":false,"__relay_internal__pv__PolarisWebSchoolsEnabledrelayprovider":false,"__relay_internal__pv__PolarisRepostsConsumptionEnabledrelayprovider":true,"__relay_internal__pv__PolarisShortDramaEnabledrelayprovider":false}}',
                'doc_id': settings.doc_id
            }
        )
        data = resp.json()
        user = data.get("data", {}).get("user")
        if not user:
            raise ScrapeError(f"no profile data for {insta_id} (private/deleted/blocked?)")
        return {
            "insta_id": int(user["id"]),
            "username": user["username"],
            "name": user.get("full_name"),
            "follower_count": user.get("follower_count"),
            "following_count": user.get("following_count"),
            "profile_pic": user.get("profile_pic_url"),
            "is_verified": user.get("is_verified", False),
            "is_private": user.get("is_private", False),
        }

    async def get_following_graphql(
        self,
        insta_id: str | int,
        after_cursor: str | None = None,
        first: int = 50,
    ) -> tuple[list[FollowedAccount], bool, str | None]:
        """
        Fetches one page of following accounts using Instagram's web GraphQL query endpoint.
        URL: https://www.instagram.com/graphql/query/?query_hash={QUERY_HASH}&variables={...}

        Returns (accounts, has_next_page, end_cursor).
        """
        variables = {
            "id": str(insta_id),
            "first": first,
        }
        if after_cursor:
            variables["after"] = after_cursor

        params = {
            "query_hash": settings.following_query_hash,
            "variables": json.dumps(variables, separators=(",", ":")),
        }

        url = "https://www.instagram.com/graphql/query/"
        resp = await self._throttled_get(url, params=params)

        try:
            data = resp.json()
        except Exception as e:
            raise ScrapeError(f"Invalid JSON response from GraphQL on {url}: {e}")

        if data.get("status") == "fail" or "message" in data:
            msg = data.get("message", "")
            if "rate" in msg.lower() or "wait" in msg.lower():
                raise RateLimited(f"GraphQL rate limited on {insta_id}: {msg}")
            raise ScrapeError(f"GraphQL error on {insta_id}: {msg}")

        user_data = data.get("data", {}).get("user")
        if not user_data:
            log.warning("No user data returned for insta_id=%s (status=%s)", insta_id, data.get("status"))
            return [], False, None

        edge_follow = user_data.get("edge_follow") or {}
        page_info = edge_follow.get("page_info") or {}
        has_next_page = bool(page_info.get("has_next_page", False))
        end_cursor = page_info.get("end_cursor")

        edges = edge_follow.get("edges") or []
        accounts: list[FollowedAccount] = []
        for edge in edges:
            node = edge.get("node") or {}
            node_id = node.get("id")
            username = node.get("username")
            if not node_id or not username:
                continue

            accounts.append(
                FollowedAccount(
                    insta_id=int(node_id),
                    username=username,
                    name=node.get("full_name"),
                    is_verified=bool(node.get("is_verified", False)),
                    profile_pic=node.get("profile_pic_url"),
                    is_private=bool(node.get("is_private", False)),
                )
            )

        return accounts, has_next_page, end_cursor

    async def get_following_page(
        self, insta_id: int, page: int | None = None , count: int = 200
    ) -> tuple[list[FollowedAccount], str | None]:
        """[Deprecated] One page of who `insta_id` follows via mobile v1 friendships.

        Returns (accounts, next_max_id). next_max_id is None when done.
        """
        params = {"count": count}
        if page and page > 1:
            params["max_id"] = (page -1) * count 
        resp = await self._throttled_get(f"{BASE_URL}/friendships/{insta_id}/following/", params=params)
        data = resp.json()
        accounts = [
            FollowedAccount(
                insta_id=int(u["id"]),
                username=u["username"],
                name=u.get("full_name"),
                is_verified=u.get("is_verified", False),
                profile_pic=u.get("profile_pic_url"),
                is_private=u.get("is_private", False),
            )
            for u in data.get("users", [])
        ]
        hasmore = data.get("has_more")
        return accounts, hasmore
