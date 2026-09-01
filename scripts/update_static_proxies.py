"""
One-off utility to update or initialize data/proxies_state.json with static proxies.

Usage:
    python -m scripts.update_static_proxies
"""

import logging
import sys
from pathlib import Path

from app.config import settings
from app.proxy_pool import ProxyStateManager, extract_proxy_name

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def main():
    state_manager = ProxyStateManager()
    proxies = settings.static_proxies

    if not proxies:
        log.warning("STATIC_PROXIES is empty in .env!")
        sys.exit(1)

    log.info("Initializing/updating %s with %d static proxies:", settings.proxy_state_file, len(proxies))
    names_map = {}
    for p in proxies:
        name = extract_proxy_name(p)
        names_map[p] = name
        log.info("  - %s [%s]", p, name)

    state_manager.init_file(proxies, names=names_map, test_proxies=True)
    log.info("Done! State file is ready for graph_consumer workers.")


if __name__ == "__main__":
    main()
