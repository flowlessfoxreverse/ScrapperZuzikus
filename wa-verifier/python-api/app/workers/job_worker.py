"""
Job worker: pulls pending jobs from Redis queue,
processes phone numbers in batches via Baileys,
writes results back to Postgres.

Run with: python -m app.workers.job_worker
"""

import asyncio
import json
import logging
from datetime import datetime, timezone

import redis.asyncio as aioredis
from sqlalchemy import select, update

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.models.models import Job, PhoneNumber, JobStatus, NumberStatus
from app.services.baileys_client import baileys_client

logger = logging.getLogger(__name__)

QUEUE_KEY = "wa_verifier:jobs:queue"
PROCESSING_KEY = "wa_verifier:jobs:processing"


class JobWorker:
    def __init__(self):
        self.redis: aioredis.Redis | None = None
        self.running = False

    async def start(self):
        self.redis = await aioredis.from_url(settings.REDIS_URL, decode_responses=True)
        self.running = True
        logger.info(f"Worker started. Concurrency: {settings.WORKER_CONCURRENCY}")

        # Run N concurrent workers
        tasks = [self._worker_loop(f"worker-{i}") for i in range(settings.WORKER_CONCURRENCY)]
        await asyncio.gather(*tasks)

    async def stop(self):
        self.running = False
        if self.redis:
            await self.redis.aclose()
        await baileys_client.close()

    async def _worker_loop(self, worker_id: str):
        """Each worker continuously pulls jobs from the queue."""
        logger.info(f"{worker_id}: started")
        while self.running:
            try:
                # Blocking pop with 5s timeout
                item = await self.redis.blpop(QUEUE_KEY, timeout=5)
                if not item:
                    continue

                _, job_id = item
                logger.info(f"{worker_id}: picked up job {job_id}")
                await self._process_job(job_id, worker_id)

            except Exception as e:
                logger.error(f"{worker_id}: error in loop: {e}", exc_info=True)
                await asyncio.sleep(2)

    async def _process_job(self, job_id: str, worker_id: str):
        async with AsyncSessionLocal() as db:
            # Fetch job
            result = await db.execute(select(Job).where(Job.id == job_id))
            job = result.scalar_one_or_none()

            if not job:
                logger.warning(f"Job {job_id} not found")
                return

            if job.status != JobStatus.PENDING:
                logger.warning(f"Job {job_id} already in status {job.status}, skipping")
                return

            # Mark as processing
            job.status = JobStatus.PROCESSING
            job.updated_at = datetime.now(timezone.utc)
            await db.commit()

            try:
                await self._run_checks(job, db, worker_id)
            except Exception as e:
                logger.error(f"Job {job_id} failed: {e}", exc_info=True)
                job.status = JobStatus.FAILED
                job.error_message = str(e)
                job.updated_at = datetime.now(timezone.utc)
                await db.commit()

    async def _run_checks(self, job: Job, db, worker_id: str):
        """Fetch pending numbers and process them in batches."""
        batch_size = settings.BATCH_SIZE
        offset = 0

        while True:
            # Fetch next batch of pending numbers
            result = await db.execute(
                select(PhoneNumber)
                .where(
                    PhoneNumber.job_id == job.id,
                    PhoneNumber.status == NumberStatus.PENDING,
                )
                .limit(batch_size)
                .offset(offset)
            )
            numbers = result.scalars().all()

            if not numbers:
                break

            phones = [n.phone for n in numbers]
            logger.info(f"{worker_id}: checking {len(phones)} numbers for job {job.id}")

            # Call Baileys bulk check
            results = await baileys_client.check_bulk(phones)

            # Map results back
            result_map = {r["original"]: r for r in results}

            now = datetime.now(timezone.utc)
            for number in numbers:
                r = result_map.get(number.phone)
                if not r:
                    number.status = NumberStatus.ERROR
                    number.error_message = "No result returned"
                    job.error_count += 1
                elif r.get("error"):
                    number.status = NumberStatus.ERROR
                    number.error_message = r["error"]
                    number.retry_count += 1
                    job.error_count += 1
                elif r.get("exists"):
                    number.status = NumberStatus.ACTIVE
                    number.whatsapp_jid = r.get("jid")
                    job.active_count += 1
                else:
                    number.status = NumberStatus.INACTIVE
                    job.inactive_count += 1

                number.checked_at = now
                number.phone_normalized = r.get("phone") if r else None
                job.processed_count += 1

            job.updated_at = now
            await db.commit()

            logger.info(
                f"{worker_id}: job {job.id} progress "
                f"{job.processed_count}/{job.total_numbers} "
                f"({job.progress_pct}%)"
            )

            offset += batch_size

        # Mark job complete
        job.status = JobStatus.COMPLETED
        job.completed_at = datetime.now(timezone.utc)
        job.updated_at = job.completed_at
        await db.commit()

        logger.info(
            f"✅ Job {job.id} completed. "
            f"Active: {job.active_count}, "
            f"Inactive: {job.inactive_count}, "
            f"Errors: {job.error_count}"
        )


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    worker = JobWorker()
    try:
        await worker.start()
    except KeyboardInterrupt:
        logger.info("Shutting down worker...")
        await worker.stop()


if __name__ == "__main__":
    asyncio.run(main())
