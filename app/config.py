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
    scheduler_batch_size: int = int(os.getenv("SCHEDULER_BATCH_SIZE", "20"))
    scheduler_poll_interval: float = float(os.getenv("SCHEDULER_POLL_INTERVAL", "2.0"))
    lease_seconds: int = int(os.getenv("LEASE_SECONDS", "600"))
    request_min_delay: float = float(os.getenv("REQUEST_MIN_DELAY", "3.0"))
    request_max_delay: float = float(os.getenv("REQUEST_MAX_DELAY", "7.0"))
    max_attempts: int = int(os.getenv("MAX_ATTEMPTS", "3"))

    #Enrichment behaviour
    enrich_target_interval: float = float(os.getenv("ENRICH_TARGET_INTERVAL", "10.0"))
    enrich_min_sleep: float = float(os.getenv("ENRICH_MIN_SLEEP", "0"))
    enrich_max_sleep: float = float(os.getenv("ENRICH_MAX_SLEEP", "10.0"))

    # Worker behaviour
    worker_message_size: int = int(os.getenv("WORKER_MESSAGE_SIZE", "10"))


settings = Settings()
