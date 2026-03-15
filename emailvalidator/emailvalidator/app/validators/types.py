"""
Shared types used across all validation layers.

Each layer returns its own result dataclass. The pipeline collects them
all into a ValidationResult, which is what the API serializes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class EmailStatus(str, Enum):
    VALID = "valid"
    INVALID = "invalid"
    RISKY = "risky"          # catch-all, role-based, or low-confidence
    UNKNOWN = "unknown"      # SMTP timeout / greylisted / inconclusive


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """Top-level result returned by the full pipeline."""

    email: str
    status: EmailStatus
    score: int                  # 0–100, higher = more deliverable
    normalized: str | None

    # Per-layer verdicts
    syntax_valid: bool
    domain_exists: bool | None
    mx_found: bool | None
    is_disposable: bool | None
    is_role_based: bool | None
    typo_suggestion: str | None
    smtp_verdict: str | None    # "valid" | "invalid" | "catch_all" | "unknown"

    details: dict = field(default_factory=dict)
