from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.models.models import PhoneNumber, NumberStatus

router = APIRouter()


@router.get("/search")
async def search_number(
    phone: str = Query(..., description="Phone number to look up (partial match)"),
    db: AsyncSession = Depends(get_db),
):
    """Search for a phone number across all jobs."""
    result = await db.execute(
        select(PhoneNumber)
        .where(PhoneNumber.phone.contains(phone))
        .limit(50)
    )
    numbers = result.scalars().all()
    return [
        {
            "phone": n.phone,
            "status": n.status,
            "whatsapp_jid": n.whatsapp_jid,
            "job_id": n.job_id,
            "checked_at": n.checked_at,
        }
        for n in numbers
    ]
