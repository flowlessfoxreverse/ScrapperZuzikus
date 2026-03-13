from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    app_name: str = "Scrapper Zuzikus"
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    database_url: str = "postgresql+psycopg://postgres:postgres@db:5432/scrapperzuzikus"
    redis_url: str = "redis://redis:6379/0"
    overpass_url: str = "https://overpass-api.de/api/interpreter"
    overpass_daily_query_cap: int = 80
    user_agent: str = "ScrapperZuzikusBot/0.1 (+contact@example.com)"
    request_timeout_seconds: int = 20
    max_pages_per_site: int = 12
    max_emails_per_company: int = 10


@lru_cache
def get_settings() -> Settings:
    return Settings()

