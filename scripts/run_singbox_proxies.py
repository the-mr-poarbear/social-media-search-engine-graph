"""
Standalone background proxy runner for V2Ray / Sing-box configs.
Reads configs from data/v2ray_configs.txt, spins up SingBoxProxy listeners
on local ports (e.g. 2080, 2081, 2082...), and writes them to data/proxies_state.json.

Run in a separate terminal:
    python -m scripts.run_singbox_proxies
"""

import logging
import os
import signal
import sys
import time
from pathlib import Path

from app.config import settings
from app.proxy_pool import ProxyStateManager, extract_proxy_name, export_configs_status_csv

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [singbox-runner] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def find_singbox_executable():
    try:
        from singbox2proxy import SingBoxBatch
        from singbox2proxy.base import SingBoxCore
    except ImportError:
        log.error("singbox2proxy library not found. Install it with: pip install singbox2proxy")
        sys.exit(1)

    # Locate sing-box.exe binary if present in project or virtualenv
    candidate_exes = [
        Path("bin/sing-box.exe").resolve(),
        Path("venv/Scripts/sing-box.exe").resolve(),
    ]
    core_exe = None
    for cand in candidate_exes:
        if cand.exists():
            core_exe = str(cand)
            cand_dir = str(cand.parent)
            if cand_dir not in os.environ.get("PATH", ""):
                os.environ["PATH"] = f"{cand_dir};{os.environ.get('PATH', '')}"
            break
    try:
        return SingBoxCore(executable=core_exe) if core_exe else SingBoxCore()
    except Exception as e:
        log.error("Could not initialize SingBoxCore: %s", e)
        sys.exit(1)


def main():
    config_file = Path("data/v2ray_configs.txt")
    csv_report_file = Path("data/v2ray_configs_status.csv")
    if not config_file.exists():
        log.error("Config file not found: %s", config_file)
        sys.exit(1)

    with open(config_file, "r", encoding="utf-8") as f:
        configs = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    if not configs:
        log.error("No valid V2Ray configs found in %s", config_file)
        sys.exit(1)

    log.info("Found %d V2Ray config(s) in %s", len(configs), config_file)

    core = find_singbox_executable()
    log.info("Starting sing-box batch engine...")

    batch = None

    def shutdown(*_):
        log.info("Shutting down sing-box proxy engine...")
        if batch:
            try:
                batch.stop()
            except Exception as e:
                log.debug("Error stopping batch: %s", e)
        log.info("All sing-box proxy instances stopped.")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        from singbox2proxy import SingBoxBatch
        batch = SingBoxBatch(urls=configs, core=core, start=True)
    except Exception as e:
        log.error("Failed to start SingBoxBatch: %s", e)
        sys.exit(1)

    local_urls = []
    names_map = {}
    for p in batch:
        local_url = p.socks_url
        name = extract_proxy_name(p.url)
        local_urls.append(local_url)
        names_map[local_url] = name
        log.info("  [Node #%d READY] -> %s (%s) [%s]", p.index + 1, local_url, p.protocol, name)

    if not local_urls:
        log.error("No proxies available in batch. Exiting.")
        shutdown()

    # Update shared state JSON with initial health testing
    state_manager = ProxyStateManager()
    health_results = state_manager.init_file(local_urls, names=names_map, test_proxies=True) or {}
    log.info("Successfully updated shared proxy state at %s", settings.proxy_state_file)

    # Build metadata list for all configs to export initial status CSV
    nodes_meta = []
    for p in batch:
        local_url = p.socks_url
        name = extract_proxy_name(p.url)
        hr = health_results.get(local_url, (False, 0.0, ""))
        nodes_meta.append({
            "name": name,
            "protocol": p.protocol,
            "local_url": local_url,
            "config": p.url,
            "available_on_init": hr[0],
            "init_latency": hr[1],
            "init_reason": hr[2],
        })

    # Export initial CSV report
    current_proxies = state_manager._read_data_unlocked().get("proxies", {})
    export_configs_status_csv(csv_report_file, nodes_meta, current_proxies)
    log.info("Exported V2Ray configs status report -> %s", csv_report_file)
    log.info("Proxy manager is running! Workers can now consume proxies from %s.", settings.proxy_state_file)

    shortage_cooldown = 15.0  # min seconds between shortage checks
    idle_interval = 300.0     # 5 minutes slow routine check when healthy
    last_recovery_check = time.time()
    last_csv_sync = time.time()

    log.info("Hybrid proxy supervisor active:")
    log.info("  - Shortage check: Instant (when 0 free available proxies, min %ds cooldown)", int(shortage_cooldown))
    log.info("  - Routine check: Every %ds when healthy proxies exist", int(idle_interval))
    log.info("  - Status CSV sync: Auto-updates %s", csv_report_file)
    log.info("Press Ctrl+C to stop.")

    try:
        while True:
            time.sleep(2.0)
            now = time.time()
            elapsed = now - last_recovery_check

            status = state_manager.get_pool_status()
            has_ready_proxies = status["benched_ready_to_test"] > 0
            has_shortage = status["available_free"] == 0

            if has_ready_proxies:
                should_check = False
                if has_shortage and elapsed >= shortage_cooldown:
                    log.warning(
                        "Proxy shortage (0 free, %d in use, %d ready to test). Running recovery check...",
                        status["in_use"],
                        status["benched_ready_to_test"],
                    )
                    should_check = True
                elif elapsed >= idle_interval:
                    log.info(
                        "Routine recovery check (%d free, %d in use, %d ready to test)...",
                        status["available_free"],
                        status["in_use"],
                        status["benched_ready_to_test"],
                    )
                    should_check = True

                if should_check:
                    last_recovery_check = now
                    state_manager.check_and_recover_benched_proxies()
                    current_proxies = state_manager._read_data_unlocked().get("proxies", {})
                    export_configs_status_csv(csv_report_file, nodes_meta, current_proxies)
                    last_csv_sync = now

            # Periodically sync CSV report with worker activity metrics every 30s
            if now - last_csv_sync >= 30.0:
                last_csv_sync = now
                current_proxies = state_manager._read_data_unlocked().get("proxies", {})
                export_configs_status_csv(csv_report_file, nodes_meta, current_proxies)
    except KeyboardInterrupt:
        shutdown()


if __name__ == "__main__":
    main()
