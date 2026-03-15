import csv
import io
import uuid
import json
import logging
from datetime import datetime, timezone

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query
from pydantic import BaseModel, field_validator
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import get_db
from app.models.models import Job, PhoneNumber, JobStatus, NumberStatus

logger = logging.getLogger(__name__)
router = APIRouter()

QUEUE_KEY = "wa_verifier:jobs:queue"


# ─── Schemas ────────────────────────────────────────────────────

class CreateJobRequest(BaseModel):
    name: str
    phones: list[str]

    @field_validator("phones")
    @classmethod
    def validate_phones(cls, v):
        if not v:
            raise ValueError("phones list cannot be empty")
        if len(v) > 100_000:
            raise ValueError("Max 100,000 numbers per job")
        return v


class JobResponse(BaseModel):
    id: str
    name: str
    status: str
    total_numbers: int
    processed_count: int
    active_count: int
    inactive_count: int
    error_count: int
    progress_pct: float
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None = None
    error_message: str | None = None

    class Config:
        from_attributes = True


# ─── Redis helper ───────────────────────────────────────────────

async def get_redis():
    r = await aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    try:
        yield r
    finally:
        await r.aclose()


# ─── Routes ─────────────────────────────────────────────────────

@router.post("/", response_model=JobResponse, status_code=201)
async def create_job(
    payload: CreateJobRequest,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    """Create a verification job from a JSON list of phone numbers."""
    job = Job(
        id=str(uuid.uuid4()),
        name=payload.name,
        status=JobStatus.PENDING,
        total_numbers=len(payload.phones),
    )
    db.add(job)
    await db.flush()  # Get the ID

    # Bulk insert numbers
    numbers = [
        PhoneNumber(job_id=job.id, phone=phone.strip())
        for phone in payload.phones
        if phone.strip()
    ]
    db.add_all(numbers)
    await db.commit()

    # Push job to Redis queue
    await redis.rpush(QUEUE_KEY, job.id)

    logger.info(f"Created job {job.id} with {len(numbers)} numbers")
    return job


@router.post("/upload", response_model=JobResponse, status_code=201)
async def create_job_from_csv(
    name: str = Query(..., description="Job name"),
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    """Create a verification job by uploading a CSV file (one number per line or column)."""
    if not file.filename.endswith(".csv"):
        raise HTTPException(400, "Only CSV files are accepted")

    content = await file.read()
    text = content.decode("utf-8-sig")  # Handle BOM

    phones = []
    reader = csv.reader(io.StringIO(text))
    for row in reader:
        if not row:
            continue
        # Accept first column as phone number
        phone = row[0].strip().strip('"')
        if phone and not phone.lower().startswith("phone"):  # skip header
            phones.append(phone)

    if not phones:
        raise HTTPException(400, "No valid phone numbers found in CSV")

    if len(phones) > 100_000:
        raise HTTPException(400, "Max 100,000 numbers per upload")

    job = Job(
        id=str(uuid.uuid4()),
        name=name,
        status=JobStatus.PENDING,
        total_numbers=len(phones),
    )
    db.add(job)
    await db.flush()

    db.add_all([PhoneNumber(job_id=job.id, phone=p) for p in phones])
    await db.commit()
    await redis.rpush(QUEUE_KEY, job.id)

    return job


@router.get("/", response_model=list[JobResponse])
async def list_jobs(
    skip: int = 0,
    limit: int = 20,
    status: JobStatus | None = None,
    db: AsyncSession = Depends(get_db),
):
    query = select(Job).order_by(Job.created_at.desc()).offset(skip).limit(limit)
    if status:
        query = query.where(Job.status == status)
    result = await db.execute(query)
    return result.scalars().all()


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(job_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(404, "Job not found")
    return job


@router.delete("/{job_id}", status_code=204)
async def cancel_job(job_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(404, "Job not found")
    if job.status == JobStatus.PROCESSING:
        raise HTTPException(400, "Cannot cancel a job that is currently processing")
    job.status = JobStatus.CANCELLED
    await db.commit()


@router.get("/{job_id}/results")
async def get_job_results(
    job_id: str,
    status: NumberStatus | None = None,
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
):
    """Get phone number results for a job, filterable by status (active/inactive/error/pending)."""
    # Verify job exists
    job_result = await db.execute(select(Job).where(Job.id == job_id))
    if not job_result.scalar_one_or_none():
        raise HTTPException(404, "Job not found")

    query = (
        select(PhoneNumber)
        .where(PhoneNumber.job_id == job_id)
        .order_by(PhoneNumber.phone)
        .offset(skip)
        .limit(limit)
    )
    if status:
        query = query.where(PhoneNumber.status == status)

    result = await db.execute(query)
    numbers = result.scalars().all()

    return {
        "job_id": job_id,
        "filter": status,
        "skip": skip,
        "limit": limit,
        "results": [
            {
                "phone": n.phone,
                "phone_normalized": n.phone_normalized,
                "status": n.status,
                "whatsapp_jid": n.whatsapp_jid,
                "checked_at": n.checked_at,
                "error_message": n.error_message,
            }
            for n in numbers
        ],
    }


@router.get("/{job_id}/export")
async def export_job_results(
    job_id: str,
    status: NumberStatus | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Export all results as CSV."""
    from fastapi.responses import StreamingResponse

    job_result = await db.execute(select(Job).where(Job.id == job_id))
    job = job_result.scalar_one_or_none()
    if not job:
        raise HTTPException(404, "Job not found")

    query = select(PhoneNumber).where(PhoneNumber.job_id == job_id)
    if status:
        query = query.where(PhoneNumber.status == status)
    result = await db.execute(query)
    numbers = result.scalars().all()

    def generate():
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["phone", "phone_normalized", "status", "whatsapp_jid", "checked_at"])
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)

        for n in numbers:
            writer.writerow([
                n.phone, n.phone_normalized or "",
                n.status, n.whatsapp_jid or "",
                n.checked_at.isoformat() if n.checked_at else "",
            ])
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)

    filename = f"job_{job_id}_{status or 'all'}.csv"
    return StreamingResponse(
        generate(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
