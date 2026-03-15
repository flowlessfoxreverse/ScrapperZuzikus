"""
Phase 3 — 8b: IP Rotation via SOCKS5 Proxy Pool

Manages a pool of SOCKS5 proxies (or bare VPS IPs) and routes each
SMTP probe through a different exit IP, preventing any single IP from
accumulating blocks.

── Why SOCKS5 and not HTTP proxy? ───────────────────────────────────────────

    HTTP proxies work at the application layer and understand HTTP only.
    SMTP is a raw TCP protocol — it needs to tunnel through SOCKS5,
    which works at the transport layer and forwards any TCP traffic.

── Proxy source options (ranked by reliability) ─────────────────────────────

    1. Specialist SMTP proxies (proxy4smtp.com)
       — SOCKS5, port 25 explicitly allowed, ~$49/proxy/month
       — 10k verifications/day per proxy
       — Best IP reputation for email verification

    2. VPS with open port 25 (Hetzner, Contabo, OVH)
       — Full control, configure PTR/rDNS yourself
       — $5–10/month per IP
       — Best for MVP and low-to-medium volume

    3. Standard residential/datacenter proxies
       — PORT 25 is blocked by virtually all mainstream providers
       — NOT suitable for SMTP verification

── Rotation strategy ────────────────────────────────────────────────────────

    round_robin     — cycles through proxies in order (default)
    least_used      — routes to the proxy with the fewest recent uses
    random          — picks a random proxy each time

── HELO rotation ────────────────────────────────────────────────────────────

    Each proxy should have its own HELO hostname that matches its PTR record.
    Mismatched HELO/PTR is a strong spam signal and gets probes rejected.
    Configure per-proxy helo_hostname when adding proxies.

── Rate limiting ─────────────────────────────────────────────────────────────

    Each proxy has a configurable daily limit (default: 8000 — conservative
    below the ~10k threshold where providers start noticing patterns).
    The manager automatically skips exhausted proxies.
"""

from __future__ import annotations

import logging
import random
import smtplib
import socket
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from threading import Lock
from typing import Generator

logger = logging.getLogger(__name__)

try:
    import socks  # PySocks library
    SOCKS5_AVAILABLE = True
except ImportError:
    SOCKS5_AVAILABLE = False
    logger.warning(
        "PySocks not installed — SOCKS5 proxy support disabled. "
        "Install with: pip install PySocks"
    )


# ── Proxy configuration ───────────────────────────────────────────────────────

class RotationStrategy(str, Enum):
    ROUND_ROBIN = "round_robin"
    LEAST_USED  = "least_used"
    RANDOM      = "random"


@dataclass
class ProxyConfig:
    """
    Configuration for a single SOCKS5 proxy or bare VPS IP.

    For a bare VPS (no SOCKS5), set host/port to None — the SMTP
    connection will use the server's own IP directly.
    """
    name: str                          # identifier, e.g. "proxy1" or "vps-hetzner-1"
    host: str | None                   # SOCKS5 host, or None for direct connection
    port: int | None                   # SOCKS5 port (default 1080)
    username: str | None = None        # optional SOCKS5 auth
    password: str | None = None        # optional SOCKS5 auth
    helo_hostname: str = "mail.validator.example.com"   # must match PTR record
    from_address: str = "probe@validator.example.com"   # must be valid on your domain
    daily_limit: int = 8_000           # conservative — raise if proxy4smtp confirms higher
    is_direct: bool = False            # True = no proxy, use server's own IP

    # Runtime counters (not part of config, managed by ProxyPool)
    _uses_today: int = field(default=0, repr=False, compare=False)
    _last_reset: float = field(default_factory=time.time, repr=False, compare=False)

    @property
    def uses_today(self) -> int:
        self._maybe_reset_counter()
        return self._uses_today

    @property
    def is_exhausted(self) -> bool:
        return self.uses_today >= self.daily_limit

    def increment(self) -> None:
        self._maybe_reset_counter()
        self._uses_today += 1

    def _maybe_reset_counter(self) -> None:
        """Reset daily counter at midnight."""
        now = time.time()
        if now - self._last_reset > 86400:
            self._uses_today = 0
            self._last_reset = now

    @classmethod
    def direct(
        cls,
        name: str = "direct",
        helo_hostname: str = "mail.validator.example.com",
        from_address: str = "probe@validator.example.com",
        daily_limit: int = 8_000,
    ) -> "ProxyConfig":
        """Create a config for a direct connection (no SOCKS5 proxy)."""
        return cls(
            name=name,
            host=None,
            port=None,
            helo_hostname=helo_hostname,
            from_address=from_address,
            daily_limit=daily_limit,
            is_direct=True,
        )


# ── Proxy pool ────────────────────────────────────────────────────────────────

class ProxyPool:
    """
    Thread-safe pool of SOCKS5 proxies with rotation and rate limiting.

    Usage:
        pool = ProxyPool([
            ProxyConfig("proxy1", "socks.proxy4smtp.com", 1080,
                        username="user", password="pass",
                        helo_hostname="mail.myapp.com",
                        from_address="probe@myapp.com"),
            ProxyConfig.direct("vps-hetzner", helo_hostname="mail.myapp.com"),
        ])

        proxy = pool.get_proxy()
        # use proxy.helo_hostname, proxy.from_address in SMTP probe
        pool.record_use(proxy.name)
    """

    def __init__(
        self,
        proxies: list[ProxyConfig],
        strategy: RotationStrategy = RotationStrategy.ROUND_ROBIN,
    ) -> None:
        if not proxies:
            raise ValueError("ProxyPool requires at least one proxy")
        self._proxies = proxies
        self._strategy = strategy
        self._index = 0
        self._lock = Lock()

    @property
    def size(self) -> int:
        return len(self._proxies)

    @property
    def available(self) -> list[ProxyConfig]:
        """Return proxies that haven't hit their daily limit."""
        return [p for p in self._proxies if not p.is_exhausted]

    def get_proxy(self) -> ProxyConfig | None:
        """
        Return the next proxy according to the rotation strategy.
        Returns None if all proxies are exhausted.
        """
        with self._lock:
            available = self.available
            if not available:
                logger.warning("All proxies exhausted for today")
                return None

            if self._strategy == RotationStrategy.ROUND_ROBIN:
                proxy = self._round_robin(available)
            elif self._strategy == RotationStrategy.LEAST_USED:
                proxy = min(available, key=lambda p: p.uses_today)
            else:
                proxy = random.choice(available)

            return proxy

    def record_use(self, proxy_name: str) -> None:
        """Increment the use counter for a proxy after a successful probe."""
        with self._lock:
            for proxy in self._proxies:
                if proxy.name == proxy_name:
                    proxy.increment()
                    return

    def _round_robin(self, available: list[ProxyConfig]) -> ProxyConfig:
        proxy = available[self._index % len(available)]
        self._index = (self._index + 1) % len(available)
        return proxy

    def stats(self) -> list[dict]:
        """Return usage stats for all proxies — useful for monitoring."""
        return [
            {
                "name": p.name,
                "uses_today": p.uses_today,
                "daily_limit": p.daily_limit,
                "is_exhausted": p.is_exhausted,
                "is_direct": p.is_direct,
                "host": p.host,
            }
            for p in self._proxies
        ]


# ── SOCKS5-aware SMTP connection ──────────────────────────────────────────────

def make_smtp_via_proxy(
    proxy: ProxyConfig,
    connect_timeout: float = 10.0,
) -> smtplib.SMTP:
    """
    Create an smtplib.SMTP instance that connects through a SOCKS5 proxy.

    For direct connections (proxy.is_direct=True), returns a standard SMTP.
    For SOCKS5 connections, patches the socket to route through the proxy.

    The returned SMTP object is NOT yet connected — caller must call
    smtp.connect(mx_host, port) after receiving it.
    """
    if proxy.is_direct or proxy.host is None:
        return smtplib.SMTP(timeout=connect_timeout)

    if not SOCKS5_AVAILABLE:
        raise RuntimeError(
            "PySocks is required for SOCKS5 proxy support. "
            "Install with: pip install PySocks"
        )

    # Create a SOCKS5 socket and wrap smtplib around it
    sock = socks.socksocket()
    sock.set_proxy(
        proxy_type=socks.SOCKS5,
        addr=proxy.host,
        port=proxy.port or 1080,
        username=proxy.username,
        password=proxy.password,
    )
    sock.settimeout(connect_timeout)

    # Inject the socket into smtplib via _get_socket override
    smtp = smtplib.SMTP(timeout=connect_timeout)
    smtp.sock = sock
    smtp._get_socket = lambda *a, **kw: sock   # type: ignore[assignment]

    return smtp


@contextmanager
def smtp_connection(
    mx_host: str,
    mx_port: int,
    proxy: ProxyConfig,
    connect_timeout: float = 10.0,
) -> Generator[smtplib.SMTP, None, None]:
    """
    Context manager that yields a connected, EHLO'd SMTP session
    routed through the given proxy. Always QUITs cleanly on exit.

    Usage:
        with smtp_connection("mx.gmail.com", 25, proxy) as smtp:
            code, msg = smtp.rcpt("user@gmail.com")
    """
    smtp: smtplib.SMTP | None = None
    try:
        smtp = make_smtp_via_proxy(proxy, connect_timeout=connect_timeout)
        smtp.connect(mx_host, mx_port)
        try:
            smtp.ehlo(proxy.helo_hostname)
        except smtplib.SMTPHeloError:
            smtp.helo(proxy.helo_hostname)
        yield smtp
    finally:
        if smtp is not None:
            try:
                smtp.quit()
            except Exception:
                pass


# ── Pool builder from environment ─────────────────────────────────────────────

def pool_from_env() -> ProxyPool:
    """
    Build a ProxyPool from environment variables.

    Reads PROXY_1_HOST, PROXY_1_PORT, PROXY_1_USER, PROXY_1_PASS,
    PROXY_1_HELO, PROXY_1_FROM up to PROXY_9_*.

    Falls back to a single direct connection if no env vars are set.
    This makes local development work without any proxy configured.

    Example docker-compose environment section:
        PROXY_1_HOST: socks.proxy4smtp.com
        PROXY_1_PORT: 1080
        PROXY_1_USER: myusername
        PROXY_1_PASS: mypassword
        PROXY_1_HELO: mail.myapp.com
        PROXY_1_FROM: probe@myapp.com
        PROXY_2_HOST: socks2.proxy4smtp.com
        ...
    """
    import os

    proxies: list[ProxyConfig] = []

    for i in range(1, 10):
        host = os.getenv(f"PROXY_{i}_HOST")
        if not host:
            continue
        proxies.append(ProxyConfig(
            name=f"proxy{i}",
            host=host,
            port=int(os.getenv(f"PROXY_{i}_PORT", "1080")),
            username=os.getenv(f"PROXY_{i}_USER"),
            password=os.getenv(f"PROXY_{i}_PASS"),
            helo_hostname=os.getenv(f"PROXY_{i}_HELO", "mail.validator.example.com"),
            from_address=os.getenv(f"PROXY_{i}_FROM", "probe@validator.example.com"),
            daily_limit=int(os.getenv(f"PROXY_{i}_DAILY_LIMIT", "8000")),
        ))

    if not proxies:
        logger.info("No proxy env vars set — using direct connection")
        proxies = [ProxyConfig.direct(
            helo_hostname=os.getenv("SMTP_HELO_HOSTNAME", "mail.validator.example.com"),
            from_address=os.getenv("SMTP_FROM_ADDRESS", "probe@validator.example.com"),
        )]

    strategy = RotationStrategy(
        os.getenv("PROXY_ROTATION_STRATEGY", RotationStrategy.ROUND_ROBIN)
    )

    pool = ProxyPool(proxies, strategy=strategy)
    logger.info(
        "Proxy pool initialised: %d proxies, strategy=%s",
        pool.size, strategy
    )
    return pool
