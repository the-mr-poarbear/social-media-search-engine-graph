import atexit
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
import csv
import json
import logging
import os
from pathlib import Path
import random
import sys
import time
import uuid

from typing import Any

import httpx

from app.config import settings

log = logging.getLogger(__name__)

PROXYSCRAPE_URL = "https://api.proxyscrape.com/v4/free-proxy-list/get"


def is_pid_alive(pid: int | None) -> bool:
    """
    Check if a process with given PID is still running on the system.
    """
    if pid is None or pid <= 0:
        return False
    if pid == os.getpid():
        return True
    try:
        if sys.platform == "win32":
            import ctypes
            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            SYNCHRONIZE = 0x00100000
            handle = ctypes.windll.kernel32.OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION | SYNCHRONIZE,
                False,
                pid,
            )
            if handle:
                ctypes.windll.kernel32.CloseHandle(handle)
                return True
            return False
        else:
            os.kill(pid, 0)
            return True
    except (OSError, ProcessLookupError, PermissionError):
        return False


@contextmanager
def file_lock(lock_path: Path, timeout: float = 20.0):
    """
    Cross-platform inter-process file lock using msvcrt on Windows and fcntl on Unix.
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    start = time.monotonic()
    lock_file = None

    while True:
        try:
            lock_file = open(lock_path, "a+")
            if sys.platform == "win32":
                import msvcrt
                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except (IOError, OSError):
            if lock_file:
                try:
                    lock_file.close()
                except Exception:
                    pass
            if time.monotonic() - start > timeout:
                raise TimeoutError(f"Could not acquire file lock on {lock_path} within {timeout}s")
            time.sleep(0.05)

    try:
        yield lock_file
    finally:
        try:
            if sys.platform == "win32":
                import msvcrt
                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            lock_file.close()
        except Exception:
            pass


def extract_proxy_name(config_str: str) -> str:
    """
    Extracts a human-readable unique identifier from a proxy URL or V2Ray link.
    Example: '185.143.235.201:443 (🇺🇸 D'AV | آمریکا)' or '185.143.235.201:443_#US_D'AV'
    """
    import urllib.parse
    if "://" not in config_str:
        return config_str
    try:
        parsed = urllib.parse.urlparse(config_str)
        if "@" in parsed.netloc:
            host_port = parsed.netloc.split("@", 1)[1]
        else:
            host_port = parsed.netloc

        remark = urllib.parse.unquote(parsed.fragment) if parsed.fragment else ""
        if remark:
            return f"{host_port} ({remark.strip()})"

        tail = (parsed.query or config_str)[-5:]
        return f"{host_port}_{tail}"
    except Exception:
        return config_str[-20:]


def export_configs_status_csv(
    csv_path: Path | str,
    configs_data: list[dict],
    current_proxies_state: dict | None = None,
):
    """
    Exports or updates a CSV status report mapping V2Ray configs to their health and metrics.
    Uses UTF-8 with BOM (utf-8-sig) for seamless viewing in Excel/VSCode without corrupting Persian/emojis.
    """
    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    temp_file = csv_path.with_name(f"{csv_path.stem}_{os.getpid()}_{uuid.uuid4().hex[:6]}.tmp")

    fieldnames = [
        "name",
        "protocol",
        "available_on_init",
        "available_now",
        "consecutive_failures",
        "avg_latency",
        "total_requests",
        "successful_requests",
        "init_error_or_reason",
        "local_port",
        "config",
    ]

    state_proxies = current_proxies_state or {}

    try:
        with open(temp_file, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for item in configs_data:
                local_url = item.get("local_url", "")
                state = state_proxies.get(local_url, {})
                is_in_state = local_url in state_proxies

                avail_on_init = item.get("available_on_init", False)
                avail_now = state.get("is_available", False) if is_in_state else False
                consecutive_fails = state.get("consecutive_failures", 0) if is_in_state else (0 if avail_on_init else 4)
                avg_lat = state.get("avg_latency", item.get("init_latency", 0.0))
                tot_req = state.get("total_requests", 0)
                succ_req = state.get("successful_requests", 0)
                reason = item.get("init_reason", "") or ("OK" if avail_on_init else "Failed")

                row = {
                    "name": item.get("name", ""),
                    "protocol": item.get("protocol", ""),
                    "available_on_init": avail_on_init,
                    "available_now": avail_now,
                    "consecutive_failures": consecutive_fails,
                    "avg_latency": round(avg_lat, 3),
                    "total_requests": tot_req,
                    "successful_requests": succ_req,
                    "init_error_or_reason": reason,
                    "local_port": local_url,
                    "config": item.get("config", ""),
                }
                writer.writerow(row)

        replaced = False
        for _ in range(10):
            try:
                os.replace(temp_file, csv_path)
                replaced = True
                break
            except (PermissionError, OSError):
                time.sleep(0.05)

        if not replaced:
            with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
                with open(temp_file, "r", encoding="utf-8-sig") as tf:
                    f.write(tf.read())
    finally:
        if temp_file.exists():
            try:
                temp_file.unlink()
            except Exception:
                pass


class _Proxy:
    __slots__ = (
        "url",
        "name",
        "consecutive_failures",
        "benched_until",
        "total_requests",
        "successful_requests",
        "last_latency",
        "avg_latency",
        "_sync_callback",
    )

    def __init__(self, url: str, name: str = ""):
        self.url = url
        self.name = name or extract_proxy_name(url)
        self.consecutive_failures = 0
        self.benched_until = 0.0
        self.total_requests = 0
        self.successful_requests = 0
        self.last_latency = 0.0
        self.avg_latency = 0.0
        self._sync_callback = None

    @property
    def is_available(self) -> bool:
        return time.time() >= self.benched_until

    def _sync(self):
        if self._sync_callback:
            try:
                self._sync_callback()
            except Exception as e:
                log.debug("Error during proxy sync callback: %s", e)

    def record_success(self, latency: float = 0.0):
        self.consecutive_failures = 0
        self.total_requests += 1
        self.successful_requests += 1
        self.last_latency = latency
        if self.avg_latency == 0.0:
            self.avg_latency = latency
        else:
            self.avg_latency = 0.7 * self.avg_latency + 0.3 * latency
        self._sync()

    def record_slow(self, latency: float) -> bool:
        """
        Record a response that exceeded max acceptable latency.
        Counts as a failure towards benching. Returns True if benched.
        """
        self.total_requests += 1
        self.last_latency = latency
        if self.avg_latency == 0.0:
            self.avg_latency = latency
        else:
            self.avg_latency = 0.7 * self.avg_latency + 0.3 * latency
        self.consecutive_failures += 1

        benched = False
        if self.consecutive_failures >= settings.proxy_max_failures:
            self.benched_until = time.time() + settings.proxy_bench_seconds
            log.info(
                "benching slow proxy %s (%s) for %ds after %d consecutive slow/failed requests (last latency: %.2fs)",
                self.name,
                self.url,
                int(settings.proxy_bench_seconds),
                self.consecutive_failures,
                latency,
            )
            benched = True

        self._sync()
        return benched

    def record_failure(self, reason: str = "") -> bool:
        self.total_requests += 1
        self.consecutive_failures += 1

        benched = False
        if self.consecutive_failures >= settings.proxy_max_failures:
            self.benched_until = time.time() + settings.proxy_bench_seconds
            log.info(
                "benching proxy %s (%s) for %ds after %d consecutive failures (reason: %s)",
                self.name,
                self.url,
                int(settings.proxy_bench_seconds),
                self.consecutive_failures,
                reason,
            )
            benched = True

        self._sync()
        return benched

    def bench_immediately(self, reason: str = ""):
        self.consecutive_failures = settings.proxy_max_failures
        self.benched_until = time.time() + settings.proxy_bench_seconds
        log.warning(
            "immediately benching proxy %s (%s) for %ds (reason: %s)",
            self.name,
            self.url,
            int(settings.proxy_bench_seconds),
            reason,
        )
        self._sync()

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "url": self.url,
            "is_available": self.is_available,
            "benched_until": self.benched_until,
            "consecutive_failures": self.consecutive_failures,
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "last_latency": round(self.last_latency, 3),
            "avg_latency": round(self.avg_latency, 3),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "_Proxy":
        name = data.get("name", "")
        p = cls(data["url"], name=name)
        p.consecutive_failures = data.get("consecutive_failures", 0)
        p.benched_until = data.get("benched_until", 0.0)
        p.total_requests = data.get("total_requests", 0)
        p.successful_requests = data.get("successful_requests", 0)
        p.last_latency = data.get("last_latency", 0.0)
        p.avg_latency = data.get("avg_latency", 0.0)
        return p


def test_proxy_health(
    proxy_url: str,
    name: str = "",
    test_url: str | None = None,
    max_attempts: int | None = None,
    timeout: float | None = None,
) -> tuple[bool, float, str]:
    """
    Tests proxy connectivity against test_url with retry attempts and live progress logging.
    Returns (is_healthy, latency_seconds, error_reason).
    """
    target_url = test_url or settings.proxy_test_url
    attempts = max_attempts if max_attempts is not None else settings.proxy_test_max_attempts
    to = timeout if timeout is not None else settings.proxy_test_timeout
    display_name = name or proxy_url

    last_error = ""
    for attempt in range(1, attempts + 1):
        start = time.monotonic()
        try:
            log.info("  Testing %s [%s] (attempt %d/%d)...", display_name, proxy_url, attempt, attempts)
            with httpx.Client(
                proxy=proxy_url,
                timeout=httpx.Timeout(timeout=to, connect=min(to, 3.0), read=to),
                follow_redirects=True,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/139.0.0.0 Safari/537.36"
                    )
                },
            ) as client:
                resp = client.get(target_url)
                if resp.status_code < 400:
                    latency = round(time.monotonic() - start, 3)
                    log.info("  [PASS] %s [%s] -> HTTP %d (latency: %.2fs)", display_name, proxy_url, resp.status_code, latency)
                    return True, latency, ""
                else:
                    last_error = f"HTTP {resp.status_code}"
                    log.warning("  [FAIL] %s [%s] attempt %d/%d: %s", display_name, proxy_url, attempt, attempts, last_error)
        except Exception as e:
            last_error = str(e)
            log.warning("  [FAIL] %s [%s] attempt %d/%d: %s", display_name, proxy_url, attempt, attempts, last_error)

        if attempt < attempts:
            time.sleep(0.3)

    return False, 0.0, last_error


class ProxyStateManager:
    """
    Manages shared inter-process proxy state in a JSON file (data/proxies_state.json).
    Coordinates worker claiming, heartbeat leasing, crash recovery, and performance syncing.
    """

    def __init__(self, state_file: str | None = None):
        self.state_file_path = Path(state_file or settings.proxy_state_file).resolve()
        self.lock_path = self.state_file_path.with_suffix(".json.lock")

    def _read_data_unlocked(self) -> dict:
        if not self.state_file_path.exists():
            return {"updated_at": time.time(), "proxies": {}}
        try:
            with open(self.state_file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log.warning("Could not read proxy state file %s: %s", self.state_file_path, e)
            return {"updated_at": time.time(), "proxies": {}}

    def _write_data_unlocked(self, data: dict):
        data["updated_at"] = time.time()
        # Use process-unique temp file to prevent concurrent temp file collisions
        temp_file = self.state_file_path.with_name(
            f"{self.state_file_path.stem}_{os.getpid()}_{uuid.uuid4().hex[:6]}.tmp"
        )
        try:
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            # Retry os.replace in case of transient Windows file locks
            replaced = False
            for _ in range(10):
                try:
                    os.replace(temp_file, self.state_file_path)
                    replaced = True
                    break
                except (PermissionError, OSError):
                    time.sleep(0.05)

            if not replaced:
                # Fallback to direct write (safe because we are inside file_lock)
                with open(self.state_file_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
        finally:
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except Exception:
                    pass

    def init_file(
        self,
        static_urls: tuple[str, ...] | list[str],
        names: dict[str, str] | None = None,
        test_proxies: bool = True,
    ):
        """
        Initializes or reconciles the JSON state file with configured static proxy URLs and names.
        If test_proxies is True, tests proxies against PROXY_TEST_URL and benches failing ones.
        """
        names_dict = names or {}
        health_results: dict[str, tuple[bool, float, str]] = {}

        if test_proxies and static_urls:
            log.info(
                "Testing initial health for %d proxy(ies) against %s (max %d attempts)...",
                len(static_urls),
                settings.proxy_test_url,
                settings.proxy_test_max_attempts,
            )
            with ThreadPoolExecutor(max_workers=min(len(static_urls), 10)) as executor:
                futures = {
                    executor.submit(
                        test_proxy_health,
                        url,
                        names_dict.get(url, extract_proxy_name(url)),
                    ): url for url in static_urls
                }
                for fut in as_completed(futures):
                    u = futures[fut]
                    try:
                        health_results[u] = fut.result()
                    except Exception as e:
                        health_results[u] = (False, 0.0, str(e))

        with file_lock(self.lock_path):
            data = self._read_data_unlocked()
            proxies = data.get("proxies", {})
            now = time.time()

            # Filter only healthy passing proxies if test_proxies is True
            valid_urls = []
            excluded_count = 0
            for url in static_urls:
                if test_proxies and url in health_results:
                    is_healthy, latency, reason = health_results[url]
                    if not is_healthy:
                        excluded_count += 1
                        if url in proxies:
                            del proxies[url]
                        continue
                valid_urls.append(url)

            for url in valid_urls:
                proxy_name = names_dict.get(url, extract_proxy_name(url))
                latency = health_results[url][1] if (url in health_results and health_results[url][0]) else 0.0

                if url not in proxies:
                    proxies[url] = {
                        "name": proxy_name,
                        "url": url,
                        "is_available": True,
                        "benched_until": 0.0,
                        "consecutive_failures": 0,
                        "total_requests": 0,
                        "successful_requests": 0,
                        "last_latency": latency,
                        "avg_latency": latency,
                        "assigned_worker": None,
                        "worker_pid": None,
                        "lease_expires_at": 0.0,
                        "last_heartbeat": 0.0,
                    }
                else:
                    proxies[url]["name"] = proxy_name
                    proxies[url]["is_available"] = True
                    proxies[url]["benched_until"] = 0.0
                    proxies[url]["consecutive_failures"] = 0
                    if latency > 0:
                        proxies[url]["last_latency"] = latency
                        if proxies[url].get("avg_latency", 0.0) == 0.0:
                            proxies[url]["avg_latency"] = latency

            # Remove proxies no longer in valid config set
            valid_set = set(valid_urls)
            for url in list(proxies.keys()):
                if url not in valid_set:
                    del proxies[url]

            data["proxies"] = proxies
            self._write_data_unlocked(data)
            log.info(
                "Initialized shared proxy state at %s (%d healthy proxy(ies) included, %d broken excluded)",
                self.state_file_path,
                len(valid_urls),
                excluded_count,
            )
            return health_results

    def claim_available_proxy(
        self,
        worker_id: str,
        pid: int,
        preferred_url: str | None = None,
    ) -> _Proxy | None:
        """
        Atomically inspects state, frees dead/stale worker leases, and claims an available proxy.
        Fast in-memory operation (sub-millisecond) with no blocking network calls.
        """
        with file_lock(self.lock_path):
            data = self._read_data_unlocked()
            proxies = data.get("proxies", {})
            now = time.time()

            # 1. Clean up stale leases / crashed worker assignments
            for url, info in proxies.items():
                assigned_worker = info.get("assigned_worker")
                if assigned_worker is not None and assigned_worker != worker_id:
                    worker_pid = info.get("worker_pid")
                    lease_expires = info.get("lease_expires_at", 0.0)

                    is_stale = lease_expires < now
                    is_dead = not is_pid_alive(worker_pid)

                    if is_stale or is_dead:
                        reason = "lease expired" if is_stale else f"worker PID {worker_pid} dead"
                        log.info("Reclaiming proxy %s from %s (%s)", url, assigned_worker, reason)
                        info["assigned_worker"] = None
                        info["worker_pid"] = None
                        info["lease_expires_at"] = 0.0

            # 2. Inspect proxies and find eligible unbenched & unassigned proxies
            available_urls = []
            for url, info in proxies.items():
                benched_until = info.get("benched_until", 0.0)
                assigned_worker = info.get("assigned_worker")

                is_unbenched = benched_until <= now
                info["is_available"] = is_unbenched
                if is_unbenched and info.get("consecutive_failures", 0) >= settings.proxy_max_failures:
                    info["consecutive_failures"] = 0

                if is_unbenched and (assigned_worker is None or assigned_worker == worker_id):
                    available_urls.append(url)

            if not available_urls:
                self._write_data_unlocked(data)
                return None

            # Choose preferred proxy or proxy with best average latency
            chosen_url = preferred_url if preferred_url in available_urls else None
            if not chosen_url:
                sorted_urls = sorted(
                    available_urls,
                    key=lambda u: proxies[u].get("avg_latency", 0.0) or random.uniform(0.01, 0.05),
                )
                chosen_url = sorted_urls[0]

            # Claim the chosen proxy
            chosen_info = proxies[chosen_url]
            chosen_info["assigned_worker"] = worker_id
            chosen_info["worker_pid"] = pid
            chosen_info["lease_expires_at"] = now + settings.proxy_lease_seconds
            chosen_info["last_heartbeat"] = now

            self._write_data_unlocked(data)

            proxy_obj = _Proxy.from_dict(chosen_info)
            return proxy_obj

    def touch_lease(self, url: str, worker_id: str):
        """
        Extends the lease expiration for the actively used proxy.
        """
        with file_lock(self.lock_path):
            data = self._read_data_unlocked()
            proxies = data.get("proxies", {})
            if url in proxies:
                info = proxies[url]
                if info.get("assigned_worker") == worker_id:
                    now = time.time()
                    info["lease_expires_at"] = now + settings.proxy_lease_seconds
                    info["last_heartbeat"] = now
                    self._write_data_unlocked(data)

    def release_proxy(self, url: str, worker_id: str):
        """
        Frees a claimed proxy so other workers can use it immediately.
        """
        with file_lock(self.lock_path):
            data = self._read_data_unlocked()
            proxies = data.get("proxies", {})
            if url in proxies:
                info = proxies[url]
                if info.get("assigned_worker") == worker_id:
                    info["assigned_worker"] = None
                    info["worker_pid"] = None
                    info["lease_expires_at"] = 0.0
                    self._write_data_unlocked(data)
                    log.info("Proxy %s released back to shared pool", url)

    def sync_proxy(self, proxy: _Proxy, worker_id: str):
        """
        Syncs failure/latency/benching state of a proxy back to the shared JSON.
        """
        with file_lock(self.lock_path):
            data = self._read_data_unlocked()
            proxies = data.get("proxies", {})
            if proxy.url in proxies:
                info = proxies[proxy.url]
                info["consecutive_failures"] = proxy.consecutive_failures
                info["benched_until"] = proxy.benched_until
                info["is_available"] = proxy.is_available
                info["total_requests"] = proxy.total_requests
                info["successful_requests"] = proxy.successful_requests
                info["last_latency"] = round(proxy.last_latency, 3)
                info["avg_latency"] = round(proxy.avg_latency, 3)

                # If proxy is now benched, clear worker assignment so it doesn't stay locked
                if not proxy.is_available and info.get("assigned_worker") == worker_id:
                    info["assigned_worker"] = None
                    info["worker_pid"] = None
                    info["lease_expires_at"] = 0.0

                self._write_data_unlocked(data)

    def check_and_recover_benched_proxies(
        self,
        test_url: str | None = None,
        max_attempts: int = 2,
        timeout: float = 3.0,
    ) -> list[str]:
        """
        Scans proxies_state.json for unavailable/benched proxies whose bench cooldown has elapsed,
        tests them in parallel, and restores any recovering proxies to is_available=True.
        Applies exponential backoff to persistent failing proxies to avoid test loops.
        """
        with file_lock(self.lock_path):
            data = self._read_data_unlocked()
            proxies = data.get("proxies", {})
            now = time.time()
            failing = [
                (url, info.get("name", url))
                for url, info in proxies.items()
                if not info.get("is_available", True) and info.get("benched_until", 0.0) <= now
            ]

        if not failing:
            return []

        log.info("Supervisor testing recovery for %d benched proxy(ies) whose cooldown expired...", len(failing))
        recovered_urls: list[str] = []
        recovery_results: dict[str, tuple[bool, float, str]] = {}

        with ThreadPoolExecutor(max_workers=min(len(failing), 10)) as executor:
            futures = {
                executor.submit(
                    test_proxy_health,
                    url,
                    name,
                    test_url,
                    max_attempts,
                    timeout,
                ): url
                for url, name in failing
            }
            for fut in as_completed(futures):
                u = futures[fut]
                try:
                    recovery_results[u] = fut.result()
                except Exception as e:
                    recovery_results[u] = (False, 0.0, str(e))

        with file_lock(self.lock_path):
            data = self._read_data_unlocked()
            proxies = data.get("proxies", {})
            now = time.time()

            for url, (is_healthy, latency, reason) in recovery_results.items():
                if url in proxies:
                    proxy_name = proxies[url].get("name", url)
                    if is_healthy:
                        proxies[url]["is_available"] = True
                        proxies[url]["benched_until"] = 0.0
                        proxies[url]["consecutive_failures"] = 0
                        proxies[url]["last_latency"] = latency
                        if proxies[url].get("avg_latency", 0.0) == 0.0:
                            proxies[url]["avg_latency"] = latency
                        else:
                            proxies[url]["avg_latency"] = 0.7 * proxies[url]["avg_latency"] + 0.3 * latency
                        recovered_urls.append(url)
                        log.info(
                            "  [RECOVERED] %s [%s] is healthy again (latency: %.2fs)! Restored to pool.",
                            proxy_name,
                            url,
                            latency,
                        )
                    else:
                        # Exponential backoff on repeatedly failing proxies (60s -> 120s -> 240s -> max 600s)
                        curr_fails = proxies[url].get("consecutive_failures", settings.proxy_max_failures) + 1
                        proxies[url]["consecutive_failures"] = curr_fails
                        fail_multiplier = min(2 ** min(curr_fails - settings.proxy_max_failures, 4), 10)
                        backoff = min(settings.proxy_bench_seconds * fail_multiplier, 600.0)
                        proxies[url]["is_available"] = False
                        proxies[url]["benched_until"] = now + backoff
                        log.debug("Proxy %s [%s] still down; benched for %ds (failures: %d)", proxy_name, url, int(backoff), curr_fails)

            self._write_data_unlocked(data)
            if recovered_urls:
                log.info("Supervisor restored %d proxy(ies) to available pool.", len(recovered_urls))

        return recovered_urls

    def get_pool_status(self) -> dict:
        """
        Returns real-time proxy counts: total, available_free, in_use, benched_or_failed, benched_ready_to_test.
        Fast in-memory read.
        """
        with file_lock(self.lock_path):
            data = self._read_data_unlocked()
            proxies = data.get("proxies", {})
            now = time.time()

            total = len(proxies)
            available_free = 0
            in_use = 0
            benched_or_failed = 0
            benched_ready_to_test = 0

            for url, info in proxies.items():
                is_avail = info.get("is_available", False)
                assigned = info.get("assigned_worker")
                lease_expires = info.get("lease_expires_at", 0.0)
                benched_until = info.get("benched_until", 0.0)

                if is_avail:
                    if assigned and lease_expires >= now:
                        in_use += 1
                    else:
                        available_free += 1
                else:
                    benched_or_failed += 1
                    if benched_until <= now:
                        benched_ready_to_test += 1

            return {
                "total": total,
                "available_free": available_free,
                "in_use": in_use,
                "benched_or_failed": benched_or_failed,
                "benched_ready_to_test": benched_ready_to_test,
            }


class ProxyPool:
    def __init__(self):
        self._worker_id = f"worker_{os.getpid()}_{uuid.uuid4().hex[:6]}"
        self._state_manager = ProxyStateManager()
        self._proxies: list[_Proxy] = []
        self._clients: dict[str, httpx.AsyncClient] = {}
        self._current_proxy: _Proxy | None = None
        self._last_refresh = 0.0
        self._lock = asyncio.Lock()

        # Register exit handler to release proxy lease on process termination
        atexit.register(self._cleanup_sync)

    def _cleanup_sync(self):
        if self._current_proxy and settings.proxy_mode in ("singbox", "static"):
            try:
                self._state_manager.release_proxy(self._current_proxy.url, self._worker_id)
            except Exception:
                pass

    async def refresh(self, http_client: httpx.AsyncClient):
        if settings.proxy_mode != "dynamic":
            return

        async with self._lock:
            try:
                resp = await http_client.get(
                    PROXYSCRAPE_URL,
                    params={
                        "request": "display_proxies",
                        "proxy_format": "protocolipport",
                        "format": "json",
                        "protocol": settings.proxy_protocol,
                    },
                    timeout=httpx.Timeout(10.0),
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                log.warning("failed to refresh proxy pool from %s: %s", PROXYSCRAPE_URL, e)
                return

            live_urls = [
                p["proxy"]
                for p in data.get("proxies", [])
                if p.get("alive") and p.get("anonymity") == settings.proxy_min_anonymity
            ]

            existing = {p.url: p for p in self._proxies}
            new_proxies = [existing.get(url, _Proxy(url)) for url in live_urls]

            # Preserve current active proxy if healthy
            if self._current_proxy and self._current_proxy.is_available:
                if self._current_proxy.url not in {p.url for p in new_proxies}:
                    new_proxies.append(self._current_proxy)

            self._proxies = new_proxies

            # Close clients for proxies that no longer exist in our pool
            active_urls = {p.url for p in self._proxies}
            for url, client in list(self._clients.items()):
                if url not in active_urls:
                    try:
                        await client.aclose()
                    except Exception:
                        pass
                    del self._clients[url]

            self._last_refresh = time.monotonic()
            log.info("proxy pool refreshed: %d live proxies", len(self._proxies))

    async def _maybe_refresh(self, http_client: httpx.AsyncClient):
        if settings.proxy_mode == "dynamic":
            if (
                not self._proxies
                or (time.monotonic() - self._last_refresh)
                > settings.proxy_refresh_interval_seconds
            ):
                await self.refresh(http_client)

    async def get_proxy(
        self,
        http_client: httpx.AsyncClient | None = None,
    ) -> "_Proxy | None":
        if settings.proxy_mode == "direct":
            return None

        if settings.proxy_mode in ("singbox", "static"):
            # 1. Keep current proxy if still unbenched
            if self._current_proxy and self._current_proxy.is_available:
                self._state_manager.touch_lease(self._current_proxy.url, self._worker_id)
                return self._current_proxy

            # 2. Release previous proxy if benched
            if self._current_proxy:
                self._state_manager.release_proxy(self._current_proxy.url, self._worker_id)
                self._current_proxy = None

            # 3. Claim an available proxy from shared state
            claimed = self._state_manager.claim_available_proxy(
                worker_id=self._worker_id,
                pid=os.getpid(),
            )

            # 4. If all proxies are benched or in use, wait non-blocking with asyncio.sleep
            #    (Async yield keeps the event loop & Kafka heartbeats active!)
            warned = False
            while claimed is None:
                if not warned:
                    log.warning(
                        "[%s] No available proxies in pool (all benched or in use). Waiting for proxy supervisor...",
                        self._worker_id,
                    )
                    warned = True
                await asyncio.sleep(2.5)
                claimed = self._state_manager.claim_available_proxy(
                    worker_id=self._worker_id,
                    pid=os.getpid(),
                )

            claimed._sync_callback = lambda: self._state_manager.sync_proxy(claimed, self._worker_id)
            self._current_proxy = claimed
            log.info("[%s] assigned proxy: %s (%s)", self._worker_id, self._current_proxy.name, self._current_proxy.url)
            return self._current_proxy

        # Dynamic Mode
        await self._maybe_refresh(http_client)

        if self._current_proxy and self._current_proxy.is_available:
            return self._current_proxy

        self._current_proxy = None
        available = [p for p in self._proxies if p.is_available]

        if not available:
            log.warning("No available proxies in pool (all benched or empty).")
            return None

        self._current_proxy = random.choice(available)
        log.info("new dynamic proxy assigned: %s", self._current_proxy.url)
        return self._current_proxy

    def release_proxy(self, proxy: _Proxy | None):
        if proxy and self._current_proxy == proxy:
            log.info("releasing current proxy: %s", proxy.url)
            if settings.proxy_mode in ("singbox", "static"):
                self._state_manager.release_proxy(proxy.url, self._worker_id)
            self._current_proxy = None

    def get_client_for(self, proxy: _Proxy) -> httpx.AsyncClient:
        """
        Returns a cached AsyncClient configured for this proxy with strict timeouts.
        """
        client = self._clients.get(proxy.url)

        if client is None or client.is_closed:
            client = httpx.AsyncClient(
                proxy=proxy.url,
                timeout=httpx.Timeout(
                    timeout=settings.proxy_timeout,
                    connect=settings.proxy_connect_timeout,
                    read=settings.proxy_timeout,
                    write=5.0,
                    pool=5.0,
                ),
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
        Close all cached AsyncClients, release proxy lease, and stop SingBox processes.
        """
        self._cleanup_sync()

        for client in self._clients.values():
            try:
                await client.aclose()
            except Exception:
                pass

        self._clients.clear()

    # Alias for convenience
    aclose = close