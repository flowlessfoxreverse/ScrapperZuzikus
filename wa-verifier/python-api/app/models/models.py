import uuid
from datetime import datetime
from sqlalchemy import (
    String, Integer, Boolean, DateTime, ForeignKey,
    Enum as SAEnum, Text, JSON
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import UUID
import enum

from app.core.database import Base


class JobStatus(str, enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class NumberStatus(str, enum.Enum):
    PENDING = "pending"
    ACTIVE = "active"       # Exists on WhatsApp
    INACTIVE = "inactive"   # Not on WhatsApp
    ERROR = "error"         # Check failed


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[JobStatus] = mapped_column(
        SAEnum(JobStatus), default=JobStatus.PENDING, index=True
    )
    total_numbers: Mapped[int] = mapped_column(Integer, default=0)
    processed_count: Mapped[int] = mapped_column(Integer, default=0)
    active_count: Mapped[int] = mapped_column(Integer, default=0)
    inactive_count: Mapped[int] = mapped_column(Integer, default=0)
    error_count: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    meta: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    numbers: Mapped[list["PhoneNumber"]] = relationship(
        "PhoneNumber", back_populates="job", cascade="all, delete-orphan"
    )

    @property
    def progress_pct(self) -> float:
        if self.total_numbers == 0:
            return 0.0
        return round((self.processed_count / self.total_numbers) * 100, 2)


class PhoneNumber(Base):
    __tablename__ = "phone_numbers"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    job_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), ForeignKey("jobs.id", ondelete="CASCADE"), index=True
    )
    phone: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    phone_normalized: Mapped[str | None] = mapped_column(String(30), nullable=True)
    status: Mapped[NumberStatus] = mapped_column(
        SAEnum(NumberStatus), default=NumberStatus.PENDING, index=True
    )
    whatsapp_jid: Mapped[str | None] = mapped_column(String(100), nullable=True)
    checked_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    job: Mapped["Job"] = relationship("Job", back_populates="numbers")
