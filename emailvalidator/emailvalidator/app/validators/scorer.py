"""
Layer 7 — Risk Scorer

Combines all layer signals into:
  - A score 0–100  (higher = more likely deliverable)
  - A status:  valid | risky | invalid | unknown

── Scoring model ────────────────────────────────────────────────────────────

Points are awarded for positive signals and deducted for negative ones.
The model is additive — each layer contributes independently.

Base points (what a clean email earns at each layer):

    Syntax valid        +20   zero-infra check
    MX record found     +20   zero-infra check
    Not disposable      +15   zero-infra check
    Not role-based      +10   zero-infra check (soft signal)
    No typo detected     +5   informational only
    SMTP: valid         +30   requires clean IP

    Max possible        100

Deductions (applied on top of base):

    SMTP: invalid       -60   strong negative — hard to recover from
    SMTP: catch-all     -20   deliverability uncertain
    Is disposable       -25   clear intent to avoid tracking
    Is role-based       -10   shared inbox, low personal value
    SMTP: unknown        -5   couldn't probe — mild penalty

── Status thresholds ────────────────────────────────────────────────────────

    score >= 80          valid      high confidence, send
    score 50–79          risky      send with caution (catch-all, role-based)
    score  1–49          invalid    likely bad, don't send
    score   0            invalid    failed syntax or no MX

── No SMTP probe path ───────────────────────────────────────────────────────

    When SMTP is skipped (smtp_verdict=None), the max achievable score is 70.
    Thresholds shift:
      score >= 60  → risky    (we can't confirm but layers 1-4 look good)
      score < 60   → invalid
    This prevents over-confidence without SMTP data.

── Design notes ─────────────────────────────────────────────────────────────

    The scorer is a pure function — no I/O, fully testable.
    All inputs come from the dataclasses produced by layers 1–6.
    The API layer assembles these inputs and calls score().
"""

from __future__ import annotations

from dataclasses import dataclass

from app.validators.catchall import CatchAllVerdict
from app.validators.smtp import SMTPVerdict
from app.validators.types import EmailStatus


# ── Score weights ─────────────────────────────────────────────────────────────

# Positive contributions
_W_SYNTAX    = 20
_W_MX        = 20
_W_NOT_DISP  = 15
_W_NOT_ROLE  = 10
_W_NO_TYPO   =  5
_W_SMTP_VALID = 30

# Deductions
_D_SMTP_INVALID   = 60
_D_SMTP_CATCH_ALL = 20
_D_IS_DISPOSABLE  = 25
_D_IS_ROLE        = 10
_D_SMTP_UNKNOWN   =  5

# Status thresholds (with SMTP)
_VALID_THRESHOLD = 80
_RISKY_THRESHOLD = 50

# Status thresholds (without SMTP — max score is 70)
_NO_SMTP_VALID_THRESHOLD = 60


@dataclass(frozen=True, slots=True)
class ScoreInput:
    """
    Collected results from layers 1–6.
    Pass None for any layer that was skipped.
    """
    # Layer 1
    syntax_valid: bool

    # Layer 2
    domain_exists: bool | None
    mx_found: bool | None

    # Layer 3
    is_disposable: bool | None
    is_role_based: bool | None

    # Layer 4
    has_typo: bool | None

    # Layer 5 + 6 combined
    smtp_verdict: SMTPVerdict | CatchAllVerdict | None

    # Metadata
    normalized_email: str | None


@dataclass(frozen=True, slots=True)
class ScoreResult:
    score: int                  # 0–100
    status: EmailStatus
    reasons: list[str]          # human-readable explanation of each deduction


def score(inputs: ScoreInput) -> ScoreResult:
    """
    Compute a deliverability score and status from all layer signals.

    This is a pure function — deterministic given the same inputs.
    """
    points = 0
    reasons: list[str] = []
    smtp_was_probed = inputs.smtp_verdict is not None

    # ── Syntax ────────────────────────────────────────────────────────────────
    if inputs.syntax_valid:
        points += _W_SYNTAX
    else:
        reasons.append("Invalid email syntax")
        # Syntax failure is fatal — no further checks matter
        return ScoreResult(score=0, status=EmailStatus.INVALID, reasons=reasons)

    # ── Domain / MX ───────────────────────────────────────────────────────────
    if inputs.mx_found is False:
        reasons.append("No MX records — domain does not accept email")
        return ScoreResult(score=points, status=EmailStatus.INVALID, reasons=reasons)

    if inputs.mx_found:
        points += _W_MX
    # mx_found=None means layer 2 was skipped — no deduction, no credit

    # ── Disposable ────────────────────────────────────────────────────────────
    if inputs.is_disposable is True:
        points -= _D_IS_DISPOSABLE
        reasons.append("Disposable email domain")
    elif inputs.is_disposable is False:
        points += _W_NOT_DISP

    # ── Role-based ────────────────────────────────────────────────────────────
    if inputs.is_role_based is True:
        points -= _D_IS_ROLE
        reasons.append("Role-based address (shared inbox)")
    elif inputs.is_role_based is False:
        points += _W_NOT_ROLE

    # ── Typo ──────────────────────────────────────────────────────────────────
    if inputs.has_typo is False:
        points += _W_NO_TYPO
    elif inputs.has_typo is True:
        reasons.append("Domain looks like a typo — check the suggestion")
        # No deduction for typo — it's informational, not a hard negative

    # ── SMTP / catch-all ──────────────────────────────────────────────────────
    verdict = inputs.smtp_verdict

    if verdict == SMTPVerdict.VALID or verdict == CatchAllVerdict.VALID:
        points += _W_SMTP_VALID

    elif verdict == SMTPVerdict.INVALID or verdict == CatchAllVerdict.INVALID:
        points -= _D_SMTP_INVALID
        reasons.append("SMTP probe: mailbox does not exist")

    elif verdict == CatchAllVerdict.CATCH_ALL:
        points -= _D_SMTP_CATCH_ALL
        reasons.append("Catch-all domain: individual mailbox existence unconfirmed")

    elif verdict in (SMTPVerdict.GREYLISTED, CatchAllVerdict.UNKNOWN):
        points -= _D_SMTP_UNKNOWN
        reasons.append("SMTP probe inconclusive (greylisted or timed out)")

    elif verdict == SMTPVerdict.UNKNOWN:
        points -= _D_SMTP_UNKNOWN
        reasons.append("SMTP probe inconclusive")

    # elif None: SMTP skipped — no credit, no deduction

    # ── Clamp to 0–100 ────────────────────────────────────────────────────────
    points = max(0, min(100, points))

    # ── Status classification ─────────────────────────────────────────────────
    status = _classify_status(points, smtp_was_probed, verdict)

    return ScoreResult(score=points, status=status, reasons=reasons)


def _classify_status(
    score: int,
    smtp_was_probed: bool,
    verdict: SMTPVerdict | CatchAllVerdict | None,
) -> EmailStatus:
    """Map a numeric score + SMTP context to a final EmailStatus."""

    # Explicit INVALID signals always override score
    if verdict in (SMTPVerdict.INVALID, CatchAllVerdict.INVALID):
        return EmailStatus.INVALID

    # Inconclusive SMTP → unknown
    if verdict in (SMTPVerdict.GREYLISTED, SMTPVerdict.UNKNOWN, CatchAllVerdict.UNKNOWN):
        return EmailStatus.UNKNOWN

    # Catch-all is always risky regardless of score
    if verdict == CatchAllVerdict.CATCH_ALL:
        return EmailStatus.RISKY

    # Score-based classification
    if smtp_was_probed:
        if score >= _VALID_THRESHOLD:
            return EmailStatus.VALID
        if score >= _RISKY_THRESHOLD:
            return EmailStatus.RISKY
        return EmailStatus.INVALID
    else:
        # No SMTP data — be more conservative
        if score >= _NO_SMTP_VALID_THRESHOLD:
            return EmailStatus.RISKY    # can't confirm without SMTP
        return EmailStatus.INVALID
