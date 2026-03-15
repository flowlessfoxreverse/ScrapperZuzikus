"""
Layer 6 — Catch-all Domain Detection

A catch-all domain accepts email for any address, even fake ones:

    RCPT TO: sjd83h3h2j@company.com  →  250 OK   ← fake address, still accepted

This makes it impossible to confirm individual mailbox existence via SMTP.
We detect it by probing 2 randomly-generated addresses alongside the real one.

── 3-probe algorithm ────────────────────────────────────────────────────────

    Probe 1: target email          user@company.com
    Probe 2: random fake           xk7q2m9p@company.com
    Probe 3: random fake           zr4n8w1v@company.com

    All three accepted  → catch_all
    Only target accepted → valid (strong signal)
    Target rejected      → invalid (regardless of fakes)
    Any probe greylisted → unknown (retry later)

── Why two fake probes? ─────────────────────────────────────────────────────

    Some anti-bot servers accept the first probe then reject subsequent ones.
    Two fakes reduces false-positive catch-all classification.
    If fake1=accept, fake2=reject → inconclusive → we mark catch_all conservatively
    (1/2 fakes accepted is enough to flag — better safe than a bad send list).

── Fake address generation ──────────────────────────────────────────────────

    Fakes are random 8-char alphanumeric strings — long enough to be
    implausible as real addresses, short enough to be unremarkable.
    We avoid dictionary words to prevent accidentally hitting a real mailbox.
"""

from __future__ import annotations

import asyncio
import random
import string
from dataclasses import dataclass
from enum import Enum

from app.validators.smtp import (
    SMTPProbeResult,
    SMTPVerdict,
    probe_mailbox,
    DEFAULT_HELO_HOSTNAME,
    DEFAULT_FROM_ADDRESS,
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_RCPT_TIMEOUT,
)


# ── Result type ───────────────────────────────────────────────────────────────

class CatchAllVerdict(str, Enum):
    VALID      = "valid"       # target accepted, fakes rejected
    INVALID    = "invalid"     # target rejected
    CATCH_ALL  = "catch_all"   # fakes accepted — domain accepts everything
    UNKNOWN    = "unknown"     # greylisting, timeout, or inconclusive


@dataclass(frozen=True, slots=True)
class CatchAllResult:
    verdict: CatchAllVerdict
    is_catch_all: bool
    can_retry: bool
    target_probe: SMTPProbeResult
    fake_probes: list[SMTPProbeResult]
    error: str | None


# ── Fake address generation ───────────────────────────────────────────────────

_FAKE_CHARS = string.ascii_lowercase + string.digits
_FAKE_LENGTH = 10   # long enough to be implausible as a real address


def _generate_fake_local() -> str:
    """Generate a random local part that is very unlikely to be a real mailbox."""
    return "".join(random.choices(_FAKE_CHARS, k=_FAKE_LENGTH))


def _make_fake_email(domain: str) -> str:
    return f"{_generate_fake_local()}@{domain}"


# ── Core algorithm ────────────────────────────────────────────────────────────

def _decide_verdict(
    target: SMTPProbeResult,
    fakes: list[SMTPProbeResult],
) -> tuple[CatchAllVerdict, bool]:
    """
    Apply the 3-probe decision matrix.
    Returns (verdict, can_retry).

    Decision tree:
    1. Target rejected (INVALID)     → INVALID, no retry
    2. Target greylisted/unknown     → UNKNOWN, retry
    3. Any fake accepted             → CATCH_ALL, no retry
    4. Any fake greylisted           → UNKNOWN (can't tell), retry
    5. All fakes rejected + target accepted → VALID, no retry
    """
    # Step 1 — target outcome drives everything
    if target.verdict == SMTPVerdict.INVALID:
        return CatchAllVerdict.INVALID, False

    if target.verdict in (SMTPVerdict.UNKNOWN, SMTPVerdict.GREYLISTED):
        return CatchAllVerdict.UNKNOWN, target.can_retry

    # Target is VALID (250/251) — now check fakes
    fakes_accepted   = [f for f in fakes if f.verdict == SMTPVerdict.VALID]
    fakes_greylisted = [f for f in fakes if f.verdict in (
        SMTPVerdict.GREYLISTED, SMTPVerdict.UNKNOWN
    )]

    if fakes_accepted:
        # At least one fake was accepted → catch-all
        return CatchAllVerdict.CATCH_ALL, False

    if fakes_greylisted:
        # Can't determine — server is being evasive with fake addresses
        return CatchAllVerdict.UNKNOWN, True

    # All fakes rejected, target accepted → definitively valid mailbox
    return CatchAllVerdict.VALID, False


async def detect_catch_all(
    email: str,
    mx_host: str,
    *,
    helo_hostname: str = DEFAULT_HELO_HOSTNAME,
    from_address: str = DEFAULT_FROM_ADDRESS,
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
    rcpt_timeout: float = DEFAULT_RCPT_TIMEOUT,
    num_fakes: int = 2,
    fake_locals: list[str] | None = None,   # inject for testing
) -> CatchAllResult:
    """
    Run the 3-probe catch-all detection algorithm.

    Fires all 3 probes concurrently (target + fakes) using asyncio.gather.
    Total time ≈ max(individual probe times), not sum.

    Args:
        email:       Target email to validate.
        mx_host:     MX server to probe (from Layer 2 primary_mx).
        num_fakes:   How many fake probes to send (default 2).
        fake_locals: Override fake local parts (used in tests for determinism).

    Returns:
        CatchAllResult with verdict and all raw probe results.
    """
    domain = email.split("@", 1)[1]

    # Build fake email addresses
    if fake_locals is not None:
        fake_emails = [f"{local}@{domain}" for local in fake_locals]
    else:
        fake_emails = [_make_fake_email(domain) for _ in range(num_fakes)]

    probe_kwargs = dict(
        mx_host=mx_host,
        helo_hostname=helo_hostname,
        from_address=from_address,
        connect_timeout=connect_timeout,
        rcpt_timeout=rcpt_timeout,
    )

    # Fire all probes concurrently
    all_emails = [email] + fake_emails
    results: list[SMTPProbeResult] = await asyncio.gather(
        *[probe_mailbox(addr, **probe_kwargs) for addr in all_emails]
    )

    target_result = results[0]
    fake_results  = results[1:]

    verdict, can_retry = _decide_verdict(target_result, fake_results)

    return CatchAllResult(
        verdict=verdict,
        is_catch_all=(verdict == CatchAllVerdict.CATCH_ALL),
        can_retry=can_retry,
        target_probe=target_result,
        fake_probes=fake_results,
        error=None,
    )
