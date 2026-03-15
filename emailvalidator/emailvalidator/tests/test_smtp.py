"""
Tests for Layer 5 — SMTP Mailbox Probe

Strategy:
  - All tests mock smtplib.SMTP so no real network calls are made.
  - We test _probe_sync directly (synchronous) for simplicity,
    plus probe_mailbox (async wrapper) for contract tests.
  - Every SMTP code category is tested.
  - All exception paths (timeout, disconnect, OS error) are tested.
  - probe_with_fallback MX failover logic is tested end-to-end.
"""

from __future__ import annotations

import smtplib
import socket
import pytest
from unittest.mock import MagicMock, patch, call

from app.validators.smtp import (
    SMTPProbeResult,
    SMTPVerdict,
    _classify_code,
    _probe_sync,
    probe_mailbox,
    probe_with_fallback,
)
from app.validators.domain import MXRecord


# ── Helpers ───────────────────────────────────────────────────────────────────

PATCH_SMTP = "app.validators.smtp.smtplib.SMTP"

def make_smtp_mock(
    mail_response: tuple[int, bytes] = (250, b"OK"),
    rcpt_response: tuple[int, bytes] = (250, b"OK"),
) -> MagicMock:
    """Build a smtplib.SMTP mock with configurable MAIL FROM and RCPT TO responses."""
    smtp = MagicMock()
    smtp.connect.return_value = (220, b"Service ready")
    smtp.ehlo.return_value = (250, b"Hello")
    smtp.mail.return_value = mail_response
    smtp.rcpt.return_value = rcpt_response
    smtp.quit.return_value = (221, b"Bye")
    return smtp


def probe(
    email="user@example.com",
    mx_host="mx.example.com",
    smtp_mock=None,
) -> SMTPProbeResult:
    """Run _probe_sync with a mock SMTP instance."""
    if smtp_mock is None:
        smtp_mock = make_smtp_mock()
    with patch(PATCH_SMTP, return_value=smtp_mock):
        return _probe_sync(
            email=email,
            mx_host=mx_host,
            helo_hostname="probe.test.com",
            from_address="probe@test.com",
            connect_timeout=5.0,
            rcpt_timeout=10.0,
            port=25,
        )


# ── _classify_code unit tests ─────────────────────────────────────────────────

class TestClassifyCode:
    def test_250_is_valid(self):
        verdict, can_retry = _classify_code(250, "OK")
        assert verdict == SMTPVerdict.VALID
        assert can_retry is False

    def test_251_is_valid(self):
        verdict, _ = _classify_code(251, "User not local, forwarding")
        assert verdict == SMTPVerdict.VALID

    def test_252_is_unknown(self):
        verdict, can_retry = _classify_code(252, "Cannot verify")
        assert verdict == SMTPVerdict.UNKNOWN
        assert can_retry is False

    def test_550_is_invalid(self):
        verdict, can_retry = _classify_code(550, "No such user")
        assert verdict == SMTPVerdict.INVALID
        assert can_retry is False

    def test_551_is_invalid(self):
        verdict, _ = _classify_code(551, "User not local")
        assert verdict == SMTPVerdict.INVALID

    def test_553_is_invalid(self):
        verdict, _ = _classify_code(553, "Mailbox name not allowed")
        assert verdict == SMTPVerdict.INVALID

    def test_554_is_invalid(self):
        verdict, _ = _classify_code(554, "Transaction failed")
        assert verdict == SMTPVerdict.INVALID

    def test_451_is_greylisted(self):
        verdict, can_retry = _classify_code(451, "Requested action aborted")
        assert verdict == SMTPVerdict.GREYLISTED
        assert can_retry is True

    def test_421_is_greylisted(self):
        verdict, can_retry = _classify_code(421, "Service not available")
        assert verdict == SMTPVerdict.GREYLISTED
        assert can_retry is True

    def test_450_is_greylisted(self):
        verdict, can_retry = _classify_code(450, "Mailbox unavailable")
        assert verdict == SMTPVerdict.GREYLISTED
        assert can_retry is True

    def test_greylist_phrase_in_message_overrides_code(self):
        # Some servers return 5xx with "try again later" — we treat as greylist
        verdict, can_retry = _classify_code(550, "Rate limit - try again later")
        assert verdict == SMTPVerdict.GREYLISTED
        assert can_retry is True

    def test_greylist_phrase_greylisted_in_message(self):
        verdict, can_retry = _classify_code(451, "Greylisted, please retry")
        assert verdict == SMTPVerdict.GREYLISTED
        assert can_retry is True

    def test_other_4xx_is_unknown_retriable(self):
        verdict, can_retry = _classify_code(452, "Insufficient storage")
        assert verdict == SMTPVerdict.GREYLISTED
        assert can_retry is True

    def test_unknown_5xx_is_unknown_not_retriable(self):
        verdict, can_retry = _classify_code(521, "Server does not accept mail")
        assert verdict == SMTPVerdict.UNKNOWN
        assert can_retry is False


# ── Successful probe ──────────────────────────────────────────────────────────

class TestSuccessfulProbe:
    def test_250_returns_valid(self):
        r = probe(smtp_mock=make_smtp_mock(rcpt_response=(250, b"OK")))
        assert r.verdict == SMTPVerdict.VALID
        assert r.smtp_code == 250
        assert r.error is None
        assert r.can_retry is False

    def test_251_returns_valid(self):
        r = probe(smtp_mock=make_smtp_mock(rcpt_response=(251, b"User forwarded")))
        assert r.verdict == SMTPVerdict.VALID

    def test_valid_result_is_definitive(self):
        r = probe(smtp_mock=make_smtp_mock(rcpt_response=(250, b"OK")))
        assert r.is_definitive is True

    def test_mx_host_captured_in_result(self):
        r = probe(mx_host="mx1.gmail.com")
        assert r.mx_host == "mx1.gmail.com"

    def test_smtp_sequence_correct(self):
        """Verify the probe sends EHLO → MAIL FROM → RCPT TO in order."""
        smtp_mock = make_smtp_mock()
        probe(email="user@example.com", smtp_mock=smtp_mock)
        smtp_mock.ehlo.assert_called_once()
        smtp_mock.mail.assert_called_once_with("probe@test.com")
        smtp_mock.rcpt.assert_called_once_with("user@example.com")

    def test_quit_called_on_success(self):
        smtp_mock = make_smtp_mock()
        probe(smtp_mock=smtp_mock)
        smtp_mock.quit.assert_called_once()


# ── Invalid mailbox ───────────────────────────────────────────────────────────

class TestInvalidMailbox:
    def test_550_returns_invalid(self):
        r = probe(smtp_mock=make_smtp_mock(rcpt_response=(550, b"No such user")))
        assert r.verdict == SMTPVerdict.INVALID
        assert r.is_definitive is True
        assert r.can_retry is False

    def test_551_returns_invalid(self):
        r = probe(smtp_mock=make_smtp_mock(rcpt_response=(551, b"User not local")))
        assert r.verdict == SMTPVerdict.INVALID

    def test_554_returns_invalid(self):
        r = probe(smtp_mock=make_smtp_mock(rcpt_response=(554, b"Transaction failed")))
        assert r.verdict == SMTPVerdict.INVALID

    def test_smtp_message_captured(self):
        r = probe(smtp_mock=make_smtp_mock(rcpt_response=(550, b"No such user here")))
        assert "no such user" in r.smtp_message.lower()


# ── Greylisting ───────────────────────────────────────────────────────────────

class TestGreylisting:
    def test_451_returns_greylisted(self):
        r = probe(smtp_mock=make_smtp_mock(rcpt_response=(451, b"Greylisted")))
        assert r.verdict == SMTPVerdict.GREYLISTED
        assert r.can_retry is True

    def test_421_returns_greylisted(self):
        r = probe(smtp_mock=make_smtp_mock(rcpt_response=(421, b"Service unavailable")))
        assert r.verdict == SMTPVerdict.GREYLISTED
        assert r.can_retry is True

    def test_greylisted_is_not_definitive(self):
        r = probe(smtp_mock=make_smtp_mock(rcpt_response=(451, b"Try later")))
        assert r.is_definitive is False


# ── Mail FROM rejection ───────────────────────────────────────────────────────

class TestMailFromRejection:
    def test_mail_from_rejected_550_returns_invalid(self):
        r = probe(smtp_mock=make_smtp_mock(mail_response=(550, b"Sender rejected")))
        assert r.verdict == SMTPVerdict.INVALID
        assert r.error is not None
        assert "MAIL FROM" in r.error


# ── Exception handling ────────────────────────────────────────────────────────

class TestExceptionHandling:
    def test_connect_error_returns_unknown_retriable(self):
        smtp_mock = MagicMock()
        smtp_mock.connect.side_effect = smtplib.SMTPConnectError(421, b"Connection refused")
        r = probe(smtp_mock=smtp_mock)
        assert r.verdict == SMTPVerdict.UNKNOWN
        assert r.can_retry is True
        assert r.error is not None

    def test_server_disconnected_returns_unknown_retriable(self):
        smtp_mock = make_smtp_mock()
        smtp_mock.rcpt.side_effect = smtplib.SMTPServerDisconnected("Connection lost")
        r = probe(smtp_mock=smtp_mock)
        assert r.verdict == SMTPVerdict.UNKNOWN
        assert r.can_retry is True

    def test_timeout_returns_unknown_retriable(self):
        smtp_mock = make_smtp_mock()
        smtp_mock.connect.side_effect = socket.timeout("timed out")
        r = probe(smtp_mock=smtp_mock)
        assert r.verdict == SMTPVerdict.UNKNOWN
        assert r.can_retry is True
        assert "Timeout" in r.error

    def test_os_error_returns_unknown_retriable(self):
        smtp_mock = make_smtp_mock()
        smtp_mock.connect.side_effect = OSError("Network unreachable")
        r = probe(smtp_mock=smtp_mock)
        assert r.verdict == SMTPVerdict.UNKNOWN
        assert r.can_retry is True

    def test_quit_always_called_even_on_rcpt_exception(self):
        smtp_mock = make_smtp_mock()
        smtp_mock.rcpt.side_effect = smtplib.SMTPServerDisconnected("bye")
        probe(smtp_mock=smtp_mock)
        smtp_mock.quit.assert_called_once()

    def test_quit_exception_does_not_propagate(self):
        smtp_mock = make_smtp_mock()
        smtp_mock.quit.side_effect = Exception("QUIT failed")
        # Should not raise
        r = probe(smtp_mock=smtp_mock)
        assert r is not None

    def test_ehlo_falls_back_to_helo(self):
        smtp_mock = make_smtp_mock()
        smtp_mock.ehlo.side_effect = smtplib.SMTPHeloError(500, b"EHLO not supported")
        smtp_mock.helo.return_value = (250, b"Hello")
        r = probe(smtp_mock=smtp_mock)
        smtp_mock.helo.assert_called_once()
        assert r.verdict == SMTPVerdict.VALID


# ── Async wrapper ─────────────────────────────────────────────────────────────

class TestProbeMailboxAsync:
    @pytest.mark.asyncio
    async def test_async_wrapper_returns_result(self):
        smtp_mock = make_smtp_mock(rcpt_response=(250, b"OK"))
        with patch(PATCH_SMTP, return_value=smtp_mock):
            result = await probe_mailbox(
                "user@example.com",
                "mx.example.com",
                helo_hostname="probe.test.com",
                from_address="probe@test.com",
            )
        assert result.verdict == SMTPVerdict.VALID

    @pytest.mark.asyncio
    async def test_async_wrapper_passes_timeout(self):
        smtp_mock = make_smtp_mock()
        with patch(PATCH_SMTP, return_value=smtp_mock) as smtp_cls:
            await probe_mailbox(
                "user@example.com",
                "mx.example.com",
                connect_timeout=3.0,
            )
        smtp_cls.assert_called_with(timeout=3.0)


# ── probe_with_fallback ───────────────────────────────────────────────────────

class TestProbeWithFallback:
    def _mx(self, host: str, pref: int = 10) -> MXRecord:
        return MXRecord(host=host, preference=pref)

    @pytest.mark.asyncio
    async def test_returns_primary_result_when_valid(self):
        smtp_mock = make_smtp_mock(rcpt_response=(250, b"OK"))
        with patch(PATCH_SMTP, return_value=smtp_mock):
            result = await probe_with_fallback(
                "user@example.com",
                [self._mx("mx1.example.com"), self._mx("mx2.example.com", 20)],
            )
        assert result.verdict == SMTPVerdict.VALID
        assert result.mx_host == "mx1.example.com"

    @pytest.mark.asyncio
    async def test_falls_back_to_secondary_on_connect_failure(self):
        call_count = 0

        def smtp_factory(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            mock = MagicMock()
            if call_count == 1:
                mock.connect.side_effect = smtplib.SMTPConnectError(421, b"Refused")
                mock.quit.return_value = (221, b"Bye")
            else:
                mock.connect.return_value = (220, b"Ready")
                mock.ehlo.return_value = (250, b"Hello")
                mock.mail.return_value = (250, b"OK")
                mock.rcpt.return_value = (250, b"OK")
                mock.quit.return_value = (221, b"Bye")
            return mock

        with patch(PATCH_SMTP, side_effect=smtp_factory):
            result = await probe_with_fallback(
                "user@example.com",
                [self._mx("mx1.example.com"), self._mx("mx2.example.com", 20)],
            )
        assert result.verdict == SMTPVerdict.VALID
        assert result.mx_host == "mx2.example.com"

    @pytest.mark.asyncio
    async def test_stops_at_definitive_invalid(self):
        """Don't try secondary MX if primary gives definitive INVALID."""
        smtp_mock = make_smtp_mock(rcpt_response=(550, b"No such user"))
        with patch(PATCH_SMTP, return_value=smtp_mock) as smtp_cls:
            result = await probe_with_fallback(
                "user@example.com",
                [self._mx("mx1.example.com"), self._mx("mx2.example.com", 20)],
            )
        # SMTP should only be instantiated once (primary only)
        assert smtp_cls.call_count == 1
        assert result.verdict == SMTPVerdict.INVALID

    @pytest.mark.asyncio
    async def test_empty_mx_list_returns_unknown(self):
        result = await probe_with_fallback("user@example.com", [])
        assert result.verdict == SMTPVerdict.UNKNOWN
        assert result.error is not None

    @pytest.mark.asyncio
    async def test_greylist_does_not_try_next_mx(self):
        """Greylisting is a per-email issue, not per-MX — don't waste time."""
        smtp_mock = make_smtp_mock(rcpt_response=(451, b"Greylisted"))
        with patch(PATCH_SMTP, return_value=smtp_mock) as smtp_cls:
            result = await probe_with_fallback(
                "user@example.com",
                [self._mx("mx1.example.com"), self._mx("mx2.example.com", 20)],
            )
        assert smtp_cls.call_count == 1
        assert result.verdict == SMTPVerdict.GREYLISTED


# ── SMTPProbeResult helpers ───────────────────────────────────────────────────

class TestSMTPProbeResult:
    def test_valid_is_definitive(self):
        r = SMTPProbeResult(
            verdict=SMTPVerdict.VALID, smtp_code=250, smtp_message="OK",
            mx_host="mx.example.com", can_retry=False, error=None,
        )
        assert r.is_definitive is True

    def test_invalid_is_definitive(self):
        r = SMTPProbeResult(
            verdict=SMTPVerdict.INVALID, smtp_code=550, smtp_message="No such user",
            mx_host="mx.example.com", can_retry=False, error=None,
        )
        assert r.is_definitive is True

    def test_unknown_is_not_definitive(self):
        r = SMTPProbeResult(
            verdict=SMTPVerdict.UNKNOWN, smtp_code=None, smtp_message=None,
            mx_host="mx.example.com", can_retry=True, error="timeout",
        )
        assert r.is_definitive is False

    def test_greylisted_is_not_definitive(self):
        r = SMTPProbeResult(
            verdict=SMTPVerdict.GREYLISTED, smtp_code=451, smtp_message="Try later",
            mx_host="mx.example.com", can_retry=True, error=None,
        )
        assert r.is_definitive is False

    def test_result_is_frozen(self):
        r = SMTPProbeResult(
            verdict=SMTPVerdict.VALID, smtp_code=250, smtp_message="OK",
            mx_host="mx.example.com", can_retry=False, error=None,
        )
        with pytest.raises((AttributeError, TypeError)):
            r.verdict = SMTPVerdict.INVALID  # type: ignore[misc]
