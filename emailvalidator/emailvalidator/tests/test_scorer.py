"""
Tests for Layer 7 — Risk Scorer

Pure function tests — no mocking needed.
We verify every scoring path, threshold boundary, and status classification.
"""

from __future__ import annotations

import pytest

from app.validators.catchall import CatchAllVerdict
from app.validators.scorer import ScoreInput, ScoreResult, score
from app.validators.smtp import SMTPVerdict
from app.validators.types import EmailStatus


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_input(
    syntax_valid: bool = True,
    domain_exists: bool | None = True,
    mx_found: bool | None = True,
    is_disposable: bool | None = False,
    is_role_based: bool | None = False,
    has_typo: bool | None = False,
    smtp_verdict=SMTPVerdict.VALID,
    normalized_email: str | None = "user@example.com",
) -> ScoreInput:
    return ScoreInput(
        syntax_valid=syntax_valid,
        domain_exists=domain_exists,
        mx_found=mx_found,
        is_disposable=is_disposable,
        is_role_based=is_role_based,
        has_typo=has_typo,
        smtp_verdict=smtp_verdict,
        normalized_email=normalized_email,
    )


# ── Perfect valid email ───────────────────────────────────────────────────────

class TestPerfectScore:
    def test_perfect_email_scores_100(self):
        result = score(make_input())
        assert result.score == 100

    def test_perfect_email_status_valid(self):
        result = score(make_input())
        assert result.status == EmailStatus.VALID

    def test_perfect_email_no_reasons(self):
        result = score(make_input())
        assert result.reasons == []


# ── Syntax failure ────────────────────────────────────────────────────────────

class TestSyntaxFailure:
    def test_invalid_syntax_scores_zero(self):
        result = score(make_input(syntax_valid=False))
        assert result.score == 0

    def test_invalid_syntax_status_invalid(self):
        result = score(make_input(syntax_valid=False))
        assert result.status == EmailStatus.INVALID

    def test_invalid_syntax_has_reason(self):
        result = score(make_input(syntax_valid=False))
        assert any("syntax" in r.lower() for r in result.reasons)

    def test_invalid_syntax_stops_pipeline(self):
        # Even if other signals are positive, syntax=False → score=0
        result = score(make_input(
            syntax_valid=False,
            smtp_verdict=SMTPVerdict.VALID,
            is_disposable=False,
        ))
        assert result.score == 0


# ── MX failure ────────────────────────────────────────────────────────────────

class TestMXFailure:
    def test_no_mx_returns_invalid(self):
        result = score(make_input(mx_found=False, smtp_verdict=None))
        assert result.status == EmailStatus.INVALID

    def test_no_mx_has_reason(self):
        result = score(make_input(mx_found=False, smtp_verdict=None))
        assert any("mx" in r.lower() or "domain" in r.lower() for r in result.reasons)

    def test_mx_skipped_gets_no_credit(self):
        result_with_mx    = score(make_input(mx_found=True))
        result_without_mx = score(make_input(mx_found=None))
        assert result_with_mx.score > result_without_mx.score


# ── Disposable domain ─────────────────────────────────────────────────────────

class TestDisposableDomain:
    def test_disposable_reduces_score(self):
        clean    = score(make_input(is_disposable=False))
        disposable = score(make_input(is_disposable=True))
        assert disposable.score < clean.score

    def test_disposable_has_reason(self):
        result = score(make_input(is_disposable=True))
        assert any("disposable" in r.lower() for r in result.reasons)

    def test_disposable_with_smtp_valid_is_risky(self):
        result = score(make_input(is_disposable=True, smtp_verdict=SMTPVerdict.VALID))
        assert result.status == EmailStatus.RISKY


# ── Role-based address ────────────────────────────────────────────────────────

class TestRoleBased:
    def test_role_based_reduces_score(self):
        clean = score(make_input(is_role_based=False))
        role  = score(make_input(is_role_based=True))
        assert role.score < clean.score

    def test_role_based_has_reason(self):
        result = score(make_input(is_role_based=True))
        assert any("role" in r.lower() for r in result.reasons)

    def test_role_based_alone_doesnt_cause_invalid(self):
        result = score(make_input(is_role_based=True, smtp_verdict=SMTPVerdict.VALID))
        assert result.status in (EmailStatus.VALID, EmailStatus.RISKY)


# ── Typo detected ─────────────────────────────────────────────────────────────

class TestTypo:
    def test_typo_adds_reason(self):
        result = score(make_input(has_typo=True))
        assert any("typo" in r.lower() for r in result.reasons)

    def test_no_typo_gives_bonus_points(self):
        with_typo    = score(make_input(has_typo=True))
        without_typo = score(make_input(has_typo=False))
        assert without_typo.score > with_typo.score


# ── SMTP verdicts ─────────────────────────────────────────────────────────────

class TestSMTPVerdict:
    def test_smtp_valid_adds_points(self):
        no_smtp  = score(make_input(smtp_verdict=None))
        with_smtp = score(make_input(smtp_verdict=SMTPVerdict.VALID))
        assert with_smtp.score > no_smtp.score

    def test_smtp_invalid_makes_invalid(self):
        result = score(make_input(smtp_verdict=SMTPVerdict.INVALID))
        assert result.status == EmailStatus.INVALID

    def test_smtp_invalid_has_reason(self):
        result = score(make_input(smtp_verdict=SMTPVerdict.INVALID))
        assert any("smtp" in r.lower() or "mailbox" in r.lower() for r in result.reasons)

    def test_smtp_greylisted_returns_unknown(self):
        result = score(make_input(smtp_verdict=SMTPVerdict.GREYLISTED))
        assert result.status == EmailStatus.UNKNOWN

    def test_smtp_unknown_returns_unknown(self):
        result = score(make_input(smtp_verdict=SMTPVerdict.UNKNOWN))
        assert result.status == EmailStatus.UNKNOWN

    def test_smtp_unknown_has_reason(self):
        result = score(make_input(smtp_verdict=SMTPVerdict.UNKNOWN))
        assert any("inconclusive" in r.lower() for r in result.reasons)


# ── Catch-all verdicts ────────────────────────────────────────────────────────

class TestCatchAllVerdict:
    def test_catch_all_is_risky(self):
        result = score(make_input(smtp_verdict=CatchAllVerdict.CATCH_ALL))
        assert result.status == EmailStatus.RISKY

    def test_catch_all_has_reason(self):
        result = score(make_input(smtp_verdict=CatchAllVerdict.CATCH_ALL))
        assert any("catch" in r.lower() for r in result.reasons)

    def test_catch_all_smtp_valid_is_valid(self):
        result = score(make_input(smtp_verdict=CatchAllVerdict.VALID))
        assert result.status == EmailStatus.VALID

    def test_catch_all_smtp_invalid_is_invalid(self):
        result = score(make_input(smtp_verdict=CatchAllVerdict.INVALID))
        assert result.status == EmailStatus.INVALID


# ── No SMTP probe ─────────────────────────────────────────────────────────────

class TestNoSMTPProbe:
    def test_no_smtp_clean_email_is_risky(self):
        # Without SMTP, even a clean email can only reach 'risky'
        result = score(make_input(smtp_verdict=None))
        assert result.status == EmailStatus.RISKY

    def test_no_smtp_disposable_is_invalid(self):
        result = score(make_input(
            smtp_verdict=None,
            is_disposable=True,
        ))
        assert result.status == EmailStatus.INVALID

    def test_no_smtp_score_capped_below_smtp_full(self):
        with_smtp    = score(make_input(smtp_verdict=SMTPVerdict.VALID))
        without_smtp = score(make_input(smtp_verdict=None))
        assert with_smtp.score > without_smtp.score


# ── Score clamping ────────────────────────────────────────────────────────────

class TestScoreClamping:
    def test_score_never_exceeds_100(self):
        result = score(make_input())
        assert result.score <= 100

    def test_score_never_below_zero(self):
        # Pile on every negative signal
        result = score(make_input(
            is_disposable=True,
            is_role_based=True,
            smtp_verdict=SMTPVerdict.INVALID,
        ))
        assert result.score >= 0

    def test_result_is_frozen(self):
        result = score(make_input())
        with pytest.raises((AttributeError, TypeError)):
            result.score = 0  # type: ignore[misc]


# ── Combined realistic scenarios ─────────────────────────────────────────────

class TestRealisticScenarios:
    def test_throwaway_mailinator_address(self):
        result = score(make_input(
            is_disposable=True,
            smtp_verdict=SMTPVerdict.VALID,
        ))
        # Disposable with confirmed mailbox → risky
        assert result.status == EmailStatus.RISKY

    def test_corporate_support_inbox(self):
        result = score(make_input(
            is_role_based=True,
            smtp_verdict=SMTPVerdict.VALID,
        ))
        # Role-based but confirmed → risky (shared inbox)
        assert result.status in (EmailStatus.VALID, EmailStatus.RISKY)

    def test_catch_all_with_disposable(self):
        result = score(make_input(
            is_disposable=True,
            smtp_verdict=CatchAllVerdict.CATCH_ALL,
        ))
        assert result.status == EmailStatus.RISKY
        assert result.score < 70

    def test_typo_domain_no_smtp(self):
        result = score(make_input(
            has_typo=True,
            smtp_verdict=None,
        ))
        # Typo is informational — doesn't force invalid
        assert result.status in (EmailStatus.RISKY, EmailStatus.INVALID)
        assert any("typo" in r.lower() for r in result.reasons)
