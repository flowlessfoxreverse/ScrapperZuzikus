"""
Layer 1 — Syntax Validation

Checks that an email address is structurally valid before any network call.
Uses the email-validator library (RFC 5322 compliant) with a lightweight
regex pre-check to fail fast on obvious garbage.

Returns a SyntaxResult dataclass so callers never parse raw strings.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from email_validator import EmailNotValidError, ValidatedEmail, validate_email


# Quick pre-filter: rejects strings that can't possibly be emails.
# email-validator is thorough but slow-ish; this fails obvious cases in <1µs.
_FAST_REJECT = re.compile(
    r"^[^@\s]{1,64}"   # local part: 1–64 chars, no @ or whitespace
    r"@"
    r"[^@\s]{1,255}$"  # domain part: up to 255 chars
)


@dataclass(frozen=True, slots=True)
class SyntaxResult:
    valid: bool
    normalized: str | None      # lowercase, unicode-normalized form
    local: str | None           # part before @
    domain: str | None          # part after @
    error: str | None           # human-readable reason if invalid


def validate_syntax(email: str) -> SyntaxResult:
    """
    Validate the syntax of an email address.

    Steps:
    1. Fast regex pre-check (rejects clear non-emails instantly)
    2. email-validator full RFC check (handles unicode, quoting, length rules)

    Args:
        email: Raw email string from user input.

    Returns:
        SyntaxResult with valid=True and normalized fields on success,
        or valid=False with an error message on failure.

    Examples:
        >>> validate_syntax("User+tag@Example.COM")
        SyntaxResult(valid=True, normalized='user+tag@example.com', ...)

        >>> validate_syntax("notanemail")
        SyntaxResult(valid=False, ..., error='...')
    """
    if not isinstance(email, str):
        return SyntaxResult(
            valid=False,
            normalized=None,
            local=None,
            domain=None,
            error="Email must be a string",
        )

    email = email.strip()

    if not email:
        return SyntaxResult(
            valid=False,
            normalized=None,
            local=None,
            domain=None,
            error="Email is empty",
        )

    if not _FAST_REJECT.match(email):
        return SyntaxResult(
            valid=False,
            normalized=None,
            local=None,
            domain=None,
            error="Invalid email format",
        )

    try:
        result: ValidatedEmail = validate_email(
            email,
            check_deliverability=False,  # no DNS here — that's Layer 2
        )
        normalized = result.normalized.lower()
        local, domain = normalized.split("@", 1)
        return SyntaxResult(
            valid=True,
            normalized=normalized,
            local=local,
            domain=domain,
            error=None,
        )
    except EmailNotValidError as exc:
        return SyntaxResult(
            valid=False,
            normalized=None,
            local=None,
            domain=None,
            error=str(exc),
        )
