from fastapi import APIRouter
from app.services.baileys_client import baileys_client

router = APIRouter()


@router.get("/health")
async def health():
    try:
        wa_status = await baileys_client.health()
    except Exception as e:
        wa_status = {"status": "unreachable", "error": str(e)}

    return {
        "api": "healthy",
        "whatsapp": wa_status,
    }


@router.get("/whatsapp/qr")
async def get_qr():
    """Get the WhatsApp QR code for session authentication."""
    return await baileys_client.get_qr()
