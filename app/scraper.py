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
import random
from dataclasses import dataclass

import httpx

from app.config import settings

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
        if not (settings.ig_sessionid and settings.doc_id and settings.fb_dtsg):
            raise RuntimeError(
                "Missing IG_SESSIONID / DOC_ID / FB_DTSG env vars. "
                "Pull these from your browser's cookie jar for instagram.com "
                "while logged in, then export them before running a worker."
            )
        
        self.proxy = proxy if proxy is not None else (settings.worker_proxy or None)

        self._client = httpx.AsyncClient(
            proxy=self.proxy,
            headers={
                "User-Agent": USER_AGENT,
                # "X-IG-App-ID": settings.ig_app_id,
                # "X-CSRFToken": settings.ig_csrftoken,
                # "X-Requested-With": "XMLHttpRequest",
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'Pragma': 'no-cache',
                'Cache-Control': 'no-cache',
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Origin': 'https://www.instagram.com',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Connection': 'keep-alive',
                "Referer": "https://www.instagram.com/",
                'x-ig-app-id': settings.ig_app_id,
            },
            cookies={
                "sessionid": settings.ig_sessionid,
                # "csrftoken": settings.ig_csrftoken,
                # "ds_user_id": settings.ig_ds_user_id,
            },
            timeout=15.0,
        )

    async def aclose(self):
        await self._client.aclose()

    async def _throttled_post(self, url: str , payload: dict | None = None, params: dict | None = None) -> httpx.Response:
        # Randomized delay before every call — cheap POC-grade politeness.
        await asyncio.sleep(random.uniform(settings.request_min_delay, settings.request_max_delay))
        resp = await self._client.post(url, json=payload, params=params)
        if resp.status_code == 429:
            raise RateLimited(f"429 on {url}")
        if resp.status_code in (401, 403):
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
        if resp.status_code in (401, 403):
            raise AuthExpired(f"{resp.status_code} on {url} — session cookie likely expired/challenged")
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

    async def get_following_page(
        self, insta_id: int, page: int | None = None , count: int = 200
    ) -> tuple[list[FollowedAccount], str | None]:
        """One page (~200 accounts) of who `insta_id` follows.

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
