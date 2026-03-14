from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "Scrapper Zuzikus"
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    app_reload: bool = False
    app_host_port: int = 8000
    postgres_db: str = "scrapperzuzikus"
    postgres_user: str = "scrapper"
    postgres_password: str = "change-me-postgres"
    postgres_host_port: int = 5433
    database_url: str = "postgresql+psycopg://postgres:postgres@db:5432/scrapperzuzikus"
    redis_password: str = "change-me-redis"
    redis_host_port: int = 6380
    redis_url: str = "redis://redis:6379/0"
    overpass_url: str = "http://overpass/api/interpreter"
    overpass_data_path: str = "/overpassdb"
    overpass_daily_query_cap: int = 0
    overpass_connect_retries: int = 4
    overpass_retry_backoff_seconds: float = 1.5
    discovery_cooldown_hours: int = 168
    crawl_recrawl_hours: int = 168
    region_catalog_countries: str = "TH"
    user_agent: str = "ScrapperZuzikusBot/0.1 (+contact@example.com)"
    request_timeout_seconds: int = 20
    max_pages_per_site: int = 12
    max_emails_per_company: int = 10
    crawler_ignore_robots: bool = True
    crawler_insecure_ssl_fallback: bool = True
    crawler_proxy_url: str | None = None
    browser_fallback_enabled: bool = True
    browser_max_pages_per_site: int = 6
    browser_navigation_timeout_seconds: int = 30
    browser_wait_after_load_ms: int = 2500
    browser_retry_attempts: int = 2
    browser_stealth_scroll_steps: int = 3
    browser_proxy_url: str | None = None
    browser_proxy_bypass: str = ""
    browser_stealth_plugin_enabled: bool = True
    worker_processes: int = 1
    worker_threads: int = 1
    browser_worker_processes: int = 1
    browser_worker_threads: int = 1


@lru_cache
def get_settings() -> Settings:
    return Settings()
