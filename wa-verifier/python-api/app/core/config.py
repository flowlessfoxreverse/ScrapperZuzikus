from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    # Database
    DATABASE_URL: str = "postgresql+asyncpg://waverifier:password@postgres:5432/waverifier"

    # Redis
    REDIS_URL: str = "redis://redis:6379/0"

    # Baileys service
    BAILEYS_URL: str = "http://baileys:3001"
    BAILEYS_TIMEOUT: int = 30  # seconds per request

    # Worker config
    WORKER_CONCURRENCY: int = 5       # parallel jobs
    BATCH_SIZE: int = 25              # numbers per Baileys bulk call
    CHECK_DELAY_MS: int = 500         # ms between batches
    MAX_RETRIES: int = 3

    # API
    CORS_ORIGINS: List[str] = ["http://localhost:3000", "http://localhost:8080"]
    API_KEY: str = "change-me-in-production"

    class Config:
        env_file = ".env"


settings = Settings()
