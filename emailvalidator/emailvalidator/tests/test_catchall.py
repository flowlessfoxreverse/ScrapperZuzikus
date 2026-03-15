"""
Tests for Layer 6 — Catch-all Detection

All SMTP probes are mocked. We test the decision logic exhaustively,
the concurrent probe dispatch, and the fake-address injection path.
"""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from app.validators.catchall import (
    CatchAllResult,
    CatchAllVerdict,
    _decide_verdict,
    _generate_fake_local,
    _make_fake_email,
    detect_catch_all,
)
from app.validators.smtp import SMTPProbeResult, SMTPVerdict


# ── Helpers ───────────────────────────────────────────────────────────────────

def smtp_result(verdict: SMTPVerdict, code: int = 250) -> SMTPProbeResult:
    return SMTPProbeResult(
        verdict=verdict,
        smtp_code=code,
        smtp_message="test",
        mx_host="mx.example.com",
        can_retry=(verdict in (SMTPVerdict.GREYLISTED, SMTPVerdict.UNKNOWN)),
        error=None,
    )


VALID   = smtp_result(SMTPVerdict.VALID, 250)
INVALID = smtp_result(SMTPVerdict.INVALID, 550)
GREY    = smtp_result(SMTPVerdict.GREYLISTED, 451)
UNKNOWN = smtp_result(SMTPVerdict.UNKNOWN, 252)


# ── _decide_verdict unit tests ────────────────────────────────────────────────

class TestDecideVerdict:
    def test_target_valid_fakes_both_rejected(self):
        verdict, retry = _decide_verdict(VALID, [INVALID, INVALID])
        assert verdict == CatchAllVerdict.VALID
        assert retry is False

    def test_target_valid_fakes_both_accepted_catch_all(self):
        verdict, retry = _decide_verdict(VALID, [VALID, VALID])
        assert verdict == CatchAllVerdict.CATCH_ALL
        assert retry is False

    def test_target_valid_one_fake_accepted_catch_all(self):
        verdict, retry = _decide_verdict(VALID, [VALID, INVALID])
        assert verdict == CatchAllVerdict.CATCH_ALL

    def test_target_invalid_is_invalid_regardless_of_fakes(self):
        verdict, retry = _decide_verdict(INVALID, [VALID, VALID])
        assert verdict == CatchAllVerdict.INVALID
        assert retry is False

    def test_target_greylisted_is_unknown_retriable(self):
        verdict, retry = _decide_verdict(GREY, [INVALID, INVALID])
        assert verdict == CatchAllVerdict.UNKNOWN
        assert retry is True

    def test_target_valid_fakes_greylisted_is_unknown(self):
        # Can't determine catch-all when fakes are greylisted
        verdict, retry = _decide_verdict(VALID, [GREY, GREY])
        assert verdict == CatchAllVerdict.UNKNOWN
        assert retry is True

    def test_target_valid_one_fake_greylisted_is_unknown(self):
        verdict, retry = _decide_verdict(VALID, [GREY, INVALID])
        assert verdict == CatchAllVerdict.UNKNOWN
        assert retry is True

    def test_target_unknown_is_unknown(self):
        verdict, _ = _decide_verdict(UNKNOWN, [INVALID, INVALID])
        assert verdict == CatchAllVerdict.UNKNOWN

    def test_all_accepted_is_catch_all(self):
        """Classic catch-all: everything accepted."""
        verdict, _ = _decide_verdict(VALID, [VALID, VALID])
        assert verdict == CatchAllVerdict.CATCH_ALL


# ── Fake address generation ───────────────────────────────────────────────────

class TestFakeAddressGeneration:
    def test_fake_local_length(self):
        local = _generate_fake_local()
        assert len(local) == 10

    def test_fake_local_alphanumeric(self):
        for _ in range(20):
            local = _generate_fake_local()
            assert local.isalnum(), f"{local!r} contains non-alphanumeric chars"

    def test_fake_local_lowercase(self):
        for _ in range(20):
            local = _generate_fake_local()
            assert local == local.lower()

    def test_fake_locals_are_random(self):
        locals_ = {_generate_fake_local() for _ in range(50)}
        # Extremely unlikely to have all 50 identical
        assert len(locals_) > 1

    def test_make_fake_email_format(self):
        email = _make_fake_email("example.com")
        assert "@" in email
        assert email.endswith("@example.com")
        local = email.split("@")[0]
        assert len(local) == 10


# ── detect_catch_all integration ─────────────────────────────────────────────

PATCH_PROBE = "app.validators.catchall.probe_mailbox"


class TestDetectCatchAll:
    @pytest.mark.asyncio
    async def test_valid_mailbox(self):
        # target=250, fake1=550, fake2=550
        responses = [VALID, INVALID, INVALID]
        with patch(PATCH_PROBE, new=AsyncMock(side_effect=responses)):
            result = await detect_catch_all(
                "user@example.com",
                "mx.example.com",
                fake_locals=["fakeone123", "faketwo456"],
            )
        assert result.verdict == CatchAllVerdict.VALID
        assert result.is_catch_all is False
        assert result.can_retry is False
        assert result.error is None

    @pytest.mark.asyncio
    async def test_catch_all_domain(self):
        responses = [VALID, VALID, VALID]
        with patch(PATCH_PROBE, new=AsyncMock(side_effect=responses)):
            result = await detect_catch_all(
                "user@example.com",
                "mx.example.com",
                fake_locals=["fakeone123", "faketwo456"],
            )
        assert result.verdict == CatchAllVerdict.CATCH_ALL
        assert result.is_catch_all is True

    @pytest.mark.asyncio
    async def test_invalid_mailbox(self):
        responses = [INVALID, INVALID, INVALID]
        with patch(PATCH_PROBE, new=AsyncMock(side_effect=responses)):
            result = await detect_catch_all(
                "user@example.com",
                "mx.example.com",
                fake_locals=["fakeone123", "faketwo456"],
            )
        assert result.verdict == CatchAllVerdict.INVALID

    @pytest.mark.asyncio
    async def test_greylisted_target_is_unknown(self):
        responses = [GREY, INVALID, INVALID]
        with patch(PATCH_PROBE, new=AsyncMock(side_effect=responses)):
            result = await detect_catch_all(
                "user@example.com",
                "mx.example.com",
                fake_locals=["fakeone123", "faketwo456"],
            )
        assert result.verdict == CatchAllVerdict.UNKNOWN
        assert result.can_retry is True

    @pytest.mark.asyncio
    async def test_probes_fired_concurrently(self):
        """All 3 probes should be dispatched in a single asyncio.gather call."""
        call_order = []

        async def mock_probe(email, **kwargs):
            call_order.append(email)
            return VALID if "user@" in email else INVALID

        with patch(PATCH_PROBE, side_effect=mock_probe):
            await detect_catch_all(
                "user@example.com",
                "mx.example.com",
                fake_locals=["fakeaaa111", "fakebbb222"],
            )
        # All 3 emails should have been probed
        assert len(call_order) == 3
        assert "user@example.com" in call_order
        assert "fakeaaa111@example.com" in call_order
        assert "fakebbb222@example.com" in call_order

    @pytest.mark.asyncio
    async def test_fake_emails_use_target_domain(self):
        """Fakes must use the same domain as the target email."""
        probed = []

        async def mock_probe(email, **kwargs):
            probed.append(email)
            return INVALID

        with patch(PATCH_PROBE, side_effect=mock_probe):
            await detect_catch_all(
                "user@company.io",
                "mx.company.io",
                fake_locals=["fakeaaa111", "fakebbb222"],
            )

        for email in probed:
            assert email.endswith("@company.io"), f"{email} uses wrong domain"

    @pytest.mark.asyncio
    async def test_target_probe_captured_in_result(self):
        responses = [VALID, INVALID, INVALID]
        with patch(PATCH_PROBE, new=AsyncMock(side_effect=responses)):
            result = await detect_catch_all(
                "user@example.com",
                "mx.example.com",
                fake_locals=["fakeone123", "faketwo456"],
            )
        assert result.target_probe.verdict == SMTPVerdict.VALID
        assert len(result.fake_probes) == 2

    @pytest.mark.asyncio
    async def test_partial_catch_all_one_fake_accepted(self):
        """One fake accepted is enough to flag as catch-all."""
        responses = [VALID, VALID, INVALID]
        with patch(PATCH_PROBE, new=AsyncMock(side_effect=responses)):
            result = await detect_catch_all(
                "user@example.com",
                "mx.example.com",
                fake_locals=["fakeone123", "faketwo456"],
            )
        assert result.verdict == CatchAllVerdict.CATCH_ALL


# ── CatchAllResult helpers ────────────────────────────────────────────────────

class TestCatchAllResult:
    def test_result_is_frozen(self):
        result = CatchAllResult(
            verdict=CatchAllVerdict.VALID,
            is_catch_all=False,
            can_retry=False,
            target_probe=VALID,
            fake_probes=[INVALID, INVALID],
            error=None,
        )
        with pytest.raises((AttributeError, TypeError)):
            result.verdict = CatchAllVerdict.INVALID  # type: ignore[misc]
