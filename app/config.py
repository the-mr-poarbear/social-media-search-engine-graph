import os
from dataclasses import dataclass
from pathlib import Path
 
from dotenv import load_dotenv


load_dotenv(Path(__file__).resolve().parent.parent / ".env")

@dataclass(frozen=True)
class Settings:
    # Postgres
    pg_dsn: str = os.getenv(
        "PG_DSN", "postgresql+asyncpg://igpoc:igpoc@localhost:5432/igpoc"
    )

    # Kafka / Redpanda
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
    topic_crawl_jobs: str = os.getenv("TOPIC_CRAWL_JOBS", "crawl-jobs")
    topic_following_pages: str = os.getenv("TOPIC_FOLLOWING_PAGES", "following-pages")

    # Instagram auth — pull from your logged-in browser session's cookie jar.
    # DevTools -> Application -> Cookies -> instagram.com
    ig_sessionid: str = os.getenv("IG_SESSIONID", "")
    # ig_csrftoken: str = os.getenv("IG_CSRFTOKEN", "")
    ig_ds_user_id: str = os.getenv("IG_DS_USER_ID", "")
    fb_dtsg: str = os.getenv("FB_DTSG", "")
    ig_app_id: str = os.getenv("IG_APP_ID", "936619743392459")  # public web app id
    doc_id: str = os.getenv("DOC_ID", "37095153540132433")  # public web doc id
    
    # Crawl behaviour
    follower_threshold: int = int(os.getenv("FOLLOWER_THRESHOLD", "100000"))
    discovery_queue_share: float = float(os.getenv("DISCOVERY_QUEUE_SHARE", "0.8"))
    scheduler_batch_size: int = int(os.getenv("SCHEDULER_BATCH_SIZE", "10"))
    scheduler_poll_interval: float = float(os.getenv("SCHEDULER_POLL_INTERVAL", "900.0"))
    lease_seconds: int = int(os.getenv("LEASE_SECONDS", "18000"))
    request_min_delay: float = float(os.getenv("REQUEST_MIN_DELAY", "7.0"))
    request_max_delay: float = float(os.getenv("REQUEST_MAX_DELAY", "11.0"))
    max_attempts: int = int(os.getenv("MAX_ATTEMPTS", "3"))

    # Enrichment behaviour
    enrichment_provider: str = os.getenv("ENRICHMENT_PROVIDER", "mixed")  # "mixed", "trendhero", "hypeauditor"
    hypeauditor_url: str = os.getenv("HYPEAUDITOR_URL", "https://pdata.hypeauditor.com/suggest/")
    trendhero_url: str = os.getenv("TRENDHERO_URL", "https://trendhero.io/api/get_er_reports")
    enrich_target_interval: float = float(os.getenv("ENRICH_TARGET_INTERVAL", "10.0"))
    enrich_min_sleep: float = float(os.getenv("ENRICH_MIN_SLEEP", "0"))
    enrich_max_sleep: float = float(os.getenv("ENRICH_MAX_SLEEP", "10.0"))
    trendhero_target_interval: float = float(os.getenv("TRENDHERO_TARGET_INTERVAL", "1.0"))
    trendhero_min_sleep: float = float(os.getenv("TRENDHERO_MIN_SLEEP", "0"))
    trendhero_max_sleep: float = float(os.getenv("TRENDHERO_MAX_SLEEP", "10.0"))

    # Proxy behaviour
    proxy_mode: str = os.getenv("PROXY_MODE", "static")  # "static", "dynamic", or "direct"
    v2ray_configs_file: str = os.getenv("V2RAY_CONFIGS_FILE", "data/v2ray_configs.txt")
    singbox_start_port: int = int(os.getenv("SINGBOX_START_PORT", "2080"))
    static_proxies: tuple[str, ...] = tuple(
        p.strip() for p in os.getenv(
            "STATIC_PROXIES",
            "socks5://127.0.0.1:2080,socks5://127.0.0.1:2081,socks5://127.0.0.1:2082"
        ).split(",") if p.strip()
    )
    proxy_state_file: str = os.getenv("PROXY_STATE_FILE", "data/proxies_state.json")
    proxy_lease_seconds: float = float(os.getenv("PROXY_LEASE_SECONDS", "30.0"))
    proxy_timeout: float = float(os.getenv("PROXY_TIMEOUT", "12.0"))
    proxy_connect_timeout: float = float(os.getenv("PROXY_CONNECT_TIMEOUT", "9"))
    proxy_max_latency: float = float(os.getenv("PROXY_MAX_LATENCY", "7"))
    proxy_max_failures: int = int(os.getenv("PROXY_MAX_FAILURES", "4"))
    proxy_bench_seconds: float = float(os.getenv("PROXY_BENCH_SECONDS", "60.0"))
    proxy_refresh_interval_seconds: float = float(os.getenv("PROXY_REFRESH_INTERVAL_SECONDS", "300.0"))
    proxy_protocol: str = os.getenv("PROXY_PROTOCOL", "socks5")
    proxy_min_anonymity: str = os.getenv("PROXY_MIN_ANONYMITY", "elite")

    # Proxy Health Testing
    proxy_test_url: str = os.getenv("PROXY_TEST_URL", "https://api.ipify.org?format=json")
    proxy_test_max_attempts: int = int(os.getenv("PROXY_TEST_MAX_ATTEMPTS", "3"))
    proxy_test_timeout: float = float(os.getenv("PROXY_TEST_TIMEOUT", "5.0"))

    def get_v2ray_configs(self) -> list[str]:
        configs: list[str] = []
        file_path = Path(self.v2ray_configs_file)
        if file_path.exists():
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        configs.append(line)

        env_configs = os.getenv("V2RAY_CONFIGS", "")
        if env_configs:
            for c in env_configs.split(","):
                c = c.strip()
                if c and c not in configs:
                    configs.append(c)

        return configs

    # Worker behaviour
    worker_message_size: int = int(os.getenv("WORKER_MESSAGE_SIZE", "10"))
    worker_proxy: str = os.getenv("WORKER_PROXY", "socks5://127.0.0.1:34832")


    neo4j_uri: str = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
    neo4j_user: str = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password: str = os.getenv("NEO4J_PASSWORD", "your_password_here")

    batch_size: int = int(os.getenv("BATCH_SIZE", "5000"))


 



settings = Settings()
