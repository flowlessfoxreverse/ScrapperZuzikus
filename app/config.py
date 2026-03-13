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
    overpass_daily_query_cap: int = 0
    discovery_cooldown_hours: int = 168
    crawl_recrawl_hours: int = 168
    user_agent: str = "ScrapperZuzikusBot/0.1 (+contact@example.com)"
    request_timeout_seconds: int = 20
    max_pages_per_site: int = 12
    max_emails_per_company: int = 10
    worker_processes: int = 1
    worker_threads: int = 1


@lru_cache
def get_settings() -> Settings:
    return Settings()
