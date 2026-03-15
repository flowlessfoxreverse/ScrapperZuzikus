import asyncio
import httpx
import logging
from typing import Any

from app.core.config import settings

logger = logging.getLogger(__name__)


class BaileysClient:
    """
    Async HTTP client for the Baileys WhatsApp microservice.
    Handles retries, timeouts, and error normalization.
    """

    def __init__(self):
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=settings.BAILEYS_URL,
                timeout=httpx.Timeout(settings.BAILEYS_TIMEOUT),
            )
        return self._client

    async def close(self):
        if self._client:
            await self._client.aclose()

    async def health(self) -> dict[str, Any]:
        client = await self._get_client()
        resp = await client.get("/health")
        resp.raise_for_status()
        return resp.json()

    async def get_qr(self) -> dict[str, Any]:
        client = await self._get_client()
        resp = await client.get("/qr")
        resp.raise_for_status()
        return resp.json()

    async def check_number(self, phone: str) -> dict[str, Any]:
        """Check a single phone number."""
        client = await self._get_client()
        for attempt in range(settings.MAX_RETRIES):
            try:
                resp = await client.post("/check", json={"phone": phone})
                if resp.status_code == 429:
                    wait = 2 ** attempt
                    logger.warning(f"Rate limited, waiting {wait}s")
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json()
            except httpx.TimeoutException:
                if attempt == settings.MAX_RETRIES - 1:
                    raise
                await asyncio.sleep(2 ** attempt)
        raise RuntimeError(f"Failed to check {phone} after {settings.MAX_RETRIES} retries")

    async def check_bulk(self, phones: list[str]) -> list[dict[str, Any]]:
        """
        Check a batch of phone numbers.
        Automatically splits into chunks if over limit.
        """
        all_results = []
        chunk_size = settings.BATCH_SIZE
        client = await self._get_client()

        for i in range(0, len(phones), chunk_size):
            chunk = phones[i : i + chunk_size]
            for attempt in range(settings.MAX_RETRIES):
                try:
                    resp = await client.post("/check/bulk", json={"phones": chunk})
                    if resp.status_code == 429:
                        wait = 2 ** attempt
                        logger.warning(f"Rate limited on bulk, waiting {wait}s")
                        await asyncio.sleep(wait)
                        continue
                    resp.raise_for_status()
                    data = resp.json()
                    all_results.extend(data.get("results", []))
                    break
                except httpx.TimeoutException:
                    if attempt == settings.MAX_RETRIES - 1:
                        # Mark all in chunk as error
                        all_results.extend([
                            {"phone": p, "exists": False, "jid": None, "error": "timeout"}
                            for p in chunk
                        ])
                    else:
                        await asyncio.sleep(2 ** attempt)

            # Delay between chunks
            if i + chunk_size < len(phones):
                await asyncio.sleep(settings.CHECK_DELAY_MS / 1000)

        return all_results


# Singleton
baileys_client = BaileysClient()
