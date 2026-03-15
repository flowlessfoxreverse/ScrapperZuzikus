"""
Layer 5 — SMTP Mailbox Probe

Connects to the target domain's MX server and issues a RCPT TO probe
to determine whether a mailbox exists, without actually sending mail.

── Probe sequence ────────────────────────────────────────────────────────────

    CONNECT  mx.domain.com:25
    EHLO     probe.yourdomain.com
    MAIL FROM: <probe@yourdomain.com>
    RCPT TO:  <target@domain.com>        ← verdict lives here
    QUIT

── SMTP verdicts ────────────────────────────────────────────────────────────

    250/251  → VALID      mailbox accepted
    252      → UNKNOWN    server won't verify but will try to deliver
    421/450/
    451/452  → UNKNOWN    greylisted or temporarily unavailable (retry later)
    550/551/
    553/554  → INVALID    mailbox does not exist (permanent rejection)
    5xx other→ UNKNOWN    conservative: don't mark invalid on unknown 5xx

── Infrastructure reality ────────────────────────────────────────────────────

  This module requires one clean IP that isn't on major blocklists.
  Gmail, Outlook, and Yahoo block residential IPs. A $5/mo VPS from
  Hetzner, DigitalOcean, or Vultr is enough to start.

  You MUST set SMTP_HELO_HOSTNAME and SMTP_FROM_ADDRESS to values that
  match your VPS's rDNS (PTR record). Mail servers reject probes from
  mismatched HELO hostnames.

── Anti-detection mitigations ────────────────────────────────────────────────

  - Configurable HELO hostname (rotate if you have multiple IPs)
  - Configurable sender address (bounce@yourdomain.com)
  - Connection timeout: 10s (fast fail, don't hang)
  - RCPT timeout: 15s (some servers are slow to respond)
  - We always QUIT cleanly — abrupt drops are flagged by spam filters
  - We never pipeline RCPT calls in the same connection (catch-all is Layer 6)

── Greylisting ───────────────────────────────────────────────────────────────

  4xx responses are temporary. The UNKNOWN verdict signals the worker
  layer to enqueue a retry. Layer 5 itself does NOT retry — that's the
  queue's job so we don't block the event loop.
"""

from __future__ import annotations

import asyncio
import logging
import smtplib
import socket
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


# ── Configuration ─────────────────────────────────────────────────────────────

DEFAULT_HELO_HOSTNAME = "mail.validator.example.com"  # override via env
DEFAULT_FROM_ADDRESS  = "probe@validator.example.com"  # override via env
DEFAULT_CONNECT_TIMEOUT = 10.0   # seconds — connection to MX
DEFAULT_RCPT_TIMEOUT    = 15.0   # seconds — waiting for RCPT TO response
DEFAULT_PORT            = 25


# ── Result types ──────────────────────────────────────────────────────────────

class SMTPVerdict(str, Enum):
    VALID    = "valid"      # 250/251: mailbox accepted
    INVALID  = "invalid"   # 550/551/553/554: permanent rejection
    UNKNOWN  = "unknown"   # 252 / 4xx / connection issues / inconclusive
    GREYLISTED = "greylisted"  # 421/450/451/452: explicitly temporary


@dataclass(frozen=True, slots=True)
class SMTPProbeResult:
    verdict: SMTPVerdict
    smtp_code: int | None          # raw SMTP response code
    smtp_message: str | None       # raw SMTP response message
    mx_host: str | None            # which MX server was probed
    can_retry: bool                # True if a retry might yield a better result
    error: str | None              # connection/protocol error description

    @property
    def is_definitive(self) -> bool:
        """True when the verdict is reliable and retrying won't help."""
        return self.verdict in (SMTPVerdict.VALID, SMTPVerdict.INVALID)


# ── SMTP code classification ──────────────────────────────────────────────────

# Codes that definitively confirm existence
_VALID_CODES: frozenset[int] = frozenset({250, 251})

# Codes that definitively deny existence
_INVALID_CODES: frozenset[int] = frozenset({550, 551, 553, 554})

# Codes that indicate greylisting / temporary unavailability
_GREYLIST_CODES: frozenset[int] = frozenset({421, 450, 451, 452})


def _classify_code(code: int, message: str) -> tuple[SMTPVerdict, bool]:
    """
    Map an SMTP response code to (verdict, can_retry).

    Some servers return 550 for valid addresses on a bad day, so we
    also check the message text for common greylisting phrases.
    """
    msg_lower = message.lower()

    # Greylisting phrases override the code classification
    greylist_phrases = (
        "greylist", "greylisted", "try again", "try later",
        "temporarily", "come back", "too fast",
    )
    if any(p in msg_lower for p in greylist_phrases):
        return SMTPVerdict.GREYLISTED, True

    if code in _VALID_CODES:
        return SMTPVerdict.VALID, False

    if code in _INVALID_CODES:
        return SMTPVerdict.INVALID, False

    if code in _GREYLIST_CODES:
        return SMTPVerdict.GREYLISTED, True

    if code == 252:
        # "Cannot verify user, will forward" — common on catch-all configs
        return SMTPVerdict.UNKNOWN, False

    if 400 <= code < 500:
        # Other 4xx — temporary, retry
        return SMTPVerdict.UNKNOWN, True

    if 500 <= code < 600:
        # Other 5xx — permanent but unknown reason; don't mark invalid
        # conservatively (some servers return 500 for anti-harvesting)
        return SMTPVerdict.UNKNOWN, False

    return SMTPVerdict.UNKNOWN, False


# ── Core probe ────────────────────────────────────────────────────────────────

async def probe_mailbox(
    email: str,
    mx_host: str,
    *,
    helo_hostname: str = DEFAULT_HELO_HOSTNAME,
    from_address: str = DEFAULT_FROM_ADDRESS,
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
    rcpt_timeout: float = DEFAULT_RCPT_TIMEOUT,
    port: int = DEFAULT_PORT,
) -> SMTPProbeResult:
    """
    Probe whether a mailbox exists by connecting to the MX server.

    This is a pure async wrapper around the synchronous smtplib probe.
    We run smtplib in a thread executor so it never blocks the event loop.

    Args:
        email:           Full email address to probe, e.g. user@domain.com
        mx_host:         MX server hostname from Layer 2 (primary MX).
        helo_hostname:   Hostname to send in EHLO/HELO. Must match your
                         server's rDNS PTR record for deliverability.
        from_address:    MAIL FROM address. Use a real address on your domain.
        connect_timeout: Seconds before giving up on TCP connection.
        rcpt_timeout:    Seconds to wait for RCPT TO response.
        port:            SMTP port (default 25; some VPS block 25, try 587).

    Returns:
        SMTPProbeResult with verdict, raw code, and retry guidance.
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None,  # default thread pool
        lambda: _probe_sync(
            email=email,
            mx_host=mx_host,
            helo_hostname=helo_hostname,
            from_address=from_address,
            connect_timeout=connect_timeout,
            rcpt_timeout=rcpt_timeout,
            port=port,
        )
    )


def _probe_sync(
    *,
    email: str,
    mx_host: str,
    helo_hostname: str,
    from_address: str,
    connect_timeout: float,
    rcpt_timeout: float,
    port: int,
) -> SMTPProbeResult:
    """
    Synchronous SMTP probe. Runs in a thread executor.

    Broken out for testability — the async wrapper is trivial.
    """
    smtp: smtplib.SMTP | None = None

    try:
        logger.debug("SMTP probe: connecting to %s:%d for %s", mx_host, port, email)

        smtp = smtplib.SMTP(timeout=connect_timeout)
        smtp.connect(mx_host, port)

        # EHLO — identify ourselves. Use HELO as fallback for old servers.
        try:
            smtp.ehlo(helo_hostname)
        except smtplib.SMTPHeloError:
            smtp.helo(helo_hostname)

        # MAIL FROM — required before RCPT TO
        code, msg = smtp.mail(from_address)
        if code != 250:
            verdict, can_retry = _classify_code(code, msg.decode(errors="replace"))
            return SMTPProbeResult(
                verdict=verdict,
                smtp_code=code,
                smtp_message=msg.decode(errors="replace"),
                mx_host=mx_host,
                can_retry=can_retry,
                error=f"MAIL FROM rejected: {code}",
            )

        # RCPT TO — the actual probe
        code, msg = smtp.rcpt(email)
        msg_str = msg.decode(errors="replace") if isinstance(msg, bytes) else str(msg)
        verdict, can_retry = _classify_code(code, msg_str)

        logger.debug(
            "SMTP probe result for %s via %s: %d %s → %s",
            email, mx_host, code, msg_str[:80], verdict
        )

        return SMTPProbeResult(
            verdict=verdict,
            smtp_code=code,
            smtp_message=msg_str,
            mx_host=mx_host,
            can_retry=can_retry,
            error=None,
        )

    except smtplib.SMTPConnectError as exc:
        logger.warning("SMTP connect error for %s via %s: %s", email, mx_host, exc)
        return SMTPProbeResult(
            verdict=SMTPVerdict.UNKNOWN,
            smtp_code=None,
            smtp_message=None,
            mx_host=mx_host,
            can_retry=True,
            error=f"Connection failed: {exc}",
        )

    except smtplib.SMTPServerDisconnected as exc:
        # Server closed the connection — common anti-probe measure
        logger.warning("SMTP server disconnected for %s via %s: %s", email, mx_host, exc)
        return SMTPProbeResult(
            verdict=SMTPVerdict.UNKNOWN,
            smtp_code=None,
            smtp_message=None,
            mx_host=mx_host,
            can_retry=True,
            error=f"Server disconnected: {exc}",
        )

    except smtplib.SMTPException as exc:
        logger.warning("SMTP protocol error for %s via %s: %s", email, mx_host, exc)
        return SMTPProbeResult(
            verdict=SMTPVerdict.UNKNOWN,
            smtp_code=getattr(exc, "smtp_code", None),
            smtp_message=getattr(exc, "smtp_error", str(exc)),
            mx_host=mx_host,
            can_retry=False,
            error=f"SMTP error: {exc}",
        )

    except (socket.timeout, TimeoutError) as exc:
        logger.warning("SMTP timeout for %s via %s: %s", email, mx_host, exc)
        return SMTPProbeResult(
            verdict=SMTPVerdict.UNKNOWN,
            smtp_code=None,
            smtp_message=None,
            mx_host=mx_host,
            can_retry=True,
            error=f"Timeout: {exc}",
        )

    except OSError as exc:
        # Covers ConnectionRefusedError, network unreachable, etc.
        logger.warning("SMTP OS error for %s via %s: %s", email, mx_host, exc)
        return SMTPProbeResult(
            verdict=SMTPVerdict.UNKNOWN,
            smtp_code=None,
            smtp_message=None,
            mx_host=mx_host,
            can_retry=True,
            error=f"Network error: {exc}",
        )

    finally:
        # Always QUIT cleanly — abrupt drops get flagged
        if smtp is not None:
            try:
                smtp.quit()
            except Exception:
                pass


# ── Convenience: probe with MX fallback ──────────────────────────────────────

async def probe_with_fallback(
    email: str,
    mx_records: list,   # list of MXRecord from domain.py
    *,
    helo_hostname: str = DEFAULT_HELO_HOSTNAME,
    from_address: str = DEFAULT_FROM_ADDRESS,
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
    rcpt_timeout: float = DEFAULT_RCPT_TIMEOUT,
    max_mx_attempts: int = 2,
) -> SMTPProbeResult:
    """
    Probe using primary MX first, falling back to secondary on connection
    failure. Stops after max_mx_attempts tries or the first definitive result.

    This is the function callers should use — it handles MX failover
    so the caller doesn't need to know about MX priority ordering.
    """
    last_result: SMTPProbeResult | None = None

    for mx in mx_records[:max_mx_attempts]:
        result = await probe_mailbox(
            email=email,
            mx_host=mx.host,
            helo_hostname=helo_hostname,
            from_address=from_address,
            connect_timeout=connect_timeout,
            rcpt_timeout=rcpt_timeout,
        )

        if result.is_definitive:
            return result

        # Connection-level failure → try next MX
        if result.error and "Connection" in (result.error or ""):
            logger.info(
                "MX %s unreachable for %s, trying next MX", mx.host, email
            )
            last_result = result
            continue

        # Non-connection UNKNOWN (greylist, etc.) — no point trying another MX
        return result

    return last_result or SMTPProbeResult(
        verdict=SMTPVerdict.UNKNOWN,
        smtp_code=None,
        smtp_message=None,
        mx_host=None,
        can_retry=True,
        error="No reachable MX servers",
    )
