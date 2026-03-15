"""
Tests for Phase 3 — 8b: Proxy Rotation

Tests ProxyPool rotation strategies, rate limiting, daily counters,
SOCKS5 socket construction, and the env-based pool builder.
"""

from __future__ import annotations

import os
import time
import pytest
from unittest.mock import MagicMock, patch

from app.validators.proxy import (
    ProxyConfig,
    ProxyPool,
    RotationStrategy,
    make_smtp_via_proxy,
    pool_from_env,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_proxy(name: str = "p1", daily_limit: int = 100) -> ProxyConfig:
    return ProxyConfig(
        name=name,
        host="proxy.test.com",
        port=1080,
        helo_hostname="mail.test.com",
        from_address="probe@test.com",
        daily_limit=daily_limit,
    )


def make_direct(name: str = "direct") -> ProxyConfig:
    return ProxyConfig.direct(name=name)


# ── ProxyConfig ───────────────────────────────────────────────────────────────

class TestProxyConfig:
    def test_direct_factory(self):
        p = ProxyConfig.direct("vps1")
        assert p.is_direct is True
        assert p.host is None
        assert p.port is None

    def test_uses_today_starts_at_zero(self):
        p = make_proxy()
        assert p.uses_today == 0

    def test_is_not_exhausted_initially(self):
        p = make_proxy(daily_limit=100)
        assert p.is_exhausted is False

    def test_exhausted_after_limit(self):
        p = make_proxy(daily_limit=3)
        p.increment(); p.increment(); p.increment()
        assert p.is_exhausted is True

    def test_increment_increases_count(self):
        p = make_proxy()
        p.increment()
        assert p.uses_today == 1

    def test_counter_resets_after_24h(self):
        p = make_proxy()
        p.increment()
        assert p.uses_today == 1
        # Simulate time passing (reset threshold is 86400s)
        p._last_reset = time.time() - 90_000
        assert p.uses_today == 0   # reset triggered


# ── ProxyPool ─────────────────────────────────────────────────────────────────

class TestProxyPool:
    def test_requires_at_least_one_proxy(self):
        with pytest.raises(ValueError):
            ProxyPool([])

    def test_size_reflects_proxy_count(self):
        pool = ProxyPool([make_proxy("p1"), make_proxy("p2")])
        assert pool.size == 2

    def test_available_excludes_exhausted(self):
        exhausted = make_proxy("p1", daily_limit=0)
        fresh = make_proxy("p2", daily_limit=100)
        pool = ProxyPool([exhausted, fresh])
        assert len(pool.available) == 1
        assert pool.available[0].name == "p2"

    def test_get_proxy_returns_none_when_all_exhausted(self):
        pool = ProxyPool([make_proxy("p1", daily_limit=0)])
        assert pool.get_proxy() is None

    def test_record_use_increments_correct_proxy(self):
        p1 = make_proxy("p1")
        p2 = make_proxy("p2")
        pool = ProxyPool([p1, p2])
        pool.record_use("p2")
        assert p2.uses_today == 1
        assert p1.uses_today == 0

    def test_stats_returns_all_proxies(self):
        pool = ProxyPool([make_proxy("p1"), make_proxy("p2")])
        stats = pool.stats()
        assert len(stats) == 2
        assert stats[0]["name"] == "p1"


class TestRoundRobinRotation:
    def test_cycles_through_proxies(self):
        p1, p2, p3 = make_proxy("p1"), make_proxy("p2"), make_proxy("p3")
        pool = ProxyPool([p1, p2, p3], strategy=RotationStrategy.ROUND_ROBIN)
        names = [pool.get_proxy().name for _ in range(6)]
        assert names == ["p1", "p2", "p3", "p1", "p2", "p3"]

    def test_skips_exhausted_proxy(self):
        p1 = make_proxy("p1", daily_limit=0)
        p2 = make_proxy("p2", daily_limit=100)
        pool = ProxyPool([p1, p2], strategy=RotationStrategy.ROUND_ROBIN)
        # p1 is exhausted, should always get p2
        for _ in range(5):
            assert pool.get_proxy().name == "p2"


class TestLeastUsedRotation:
    def test_returns_least_used(self):
        p1, p2 = make_proxy("p1"), make_proxy("p2")
        p1.increment(); p1.increment()   # p1 has 2 uses
        pool = ProxyPool([p1, p2], strategy=RotationStrategy.LEAST_USED)
        assert pool.get_proxy().name == "p2"

    def test_returns_any_when_tied(self):
        p1, p2 = make_proxy("p1"), make_proxy("p2")
        pool = ProxyPool([p1, p2], strategy=RotationStrategy.LEAST_USED)
        result = pool.get_proxy()
        assert result.name in ("p1", "p2")


class TestRandomRotation:
    def test_returns_a_proxy(self):
        pool = ProxyPool([make_proxy("p1"), make_proxy("p2")], strategy=RotationStrategy.RANDOM)
        result = pool.get_proxy()
        assert result is not None
        assert result.name in ("p1", "p2")


# ── SMTP socket construction ──────────────────────────────────────────────────

class TestMakeSMTPViaProxy:
    def test_direct_returns_standard_smtp(self):
        proxy = ProxyConfig.direct()
        import smtplib
        smtp = make_smtp_via_proxy(proxy)
        assert isinstance(smtp, smtplib.SMTP)

    def test_socks5_proxy_configures_socket(self):
        proxy = make_proxy()
        mock_sock = MagicMock()
        mock_sock_cls = MagicMock(return_value=mock_sock)

        with patch("app.validators.proxy.socks") as mock_socks:
            mock_socks.socksocket.return_value = mock_sock
            mock_socks.SOCKS5 = 2  # socks.SOCKS5 constant
            smtp = make_smtp_via_proxy(proxy)

        mock_socks.socksocket.assert_called_once()
        mock_sock.set_proxy.assert_called_once_with(
            proxy_type=2,
            addr="proxy.test.com",
            port=1080,
            username=None,
            password=None,
        )

    def test_socks5_without_pysocks_raises(self):
        proxy = make_proxy()
        with patch("app.validators.proxy.SOCKS5_AVAILABLE", False):
            with pytest.raises(RuntimeError, match="PySocks"):
                make_smtp_via_proxy(proxy)


# ── pool_from_env ─────────────────────────────────────────────────────────────

class TestPoolFromEnv:
    def test_no_env_vars_returns_direct_pool(self):
        env = {k: v for k, v in os.environ.items() if not k.startswith("PROXY_")}
        with patch.dict(os.environ, env, clear=True):
            pool = pool_from_env()
        assert pool.size == 1
        assert pool._proxies[0].is_direct is True

    def test_reads_proxy_1_from_env(self):
        env = {
            "PROXY_1_HOST": "socks.provider.com",
            "PROXY_1_PORT": "1080",
            "PROXY_1_USER": "user1",
            "PROXY_1_PASS": "pass1",
            "PROXY_1_HELO": "mail.myapp.com",
            "PROXY_1_FROM": "probe@myapp.com",
        }
        with patch.dict(os.environ, env, clear=True):
            pool = pool_from_env()
        assert pool.size == 1
        p = pool._proxies[0]
        assert p.host == "socks.provider.com"
        assert p.port == 1080
        assert p.username == "user1"
        assert p.helo_hostname == "mail.myapp.com"

    def test_reads_multiple_proxies(self):
        env = {
            "PROXY_1_HOST": "proxy1.com",
            "PROXY_1_PORT": "1080",
            "PROXY_2_HOST": "proxy2.com",
            "PROXY_2_PORT": "1081",
        }
        with patch.dict(os.environ, env, clear=True):
            pool = pool_from_env()
        assert pool.size == 2

    def test_strategy_from_env(self):
        env = {
            "PROXY_1_HOST": "proxy1.com",
            "PROXY_1_PORT": "1080",
            "PROXY_ROTATION_STRATEGY": "least_used",
        }
        with patch.dict(os.environ, env, clear=True):
            pool = pool_from_env()
        assert pool._strategy == RotationStrategy.LEAST_USED

    def test_daily_limit_from_env(self):
        env = {
            "PROXY_1_HOST": "proxy1.com",
            "PROXY_1_PORT": "1080",
            "PROXY_1_DAILY_LIMIT": "5000",
        }
        with patch.dict(os.environ, env, clear=True):
            pool = pool_from_env()
        assert pool._proxies[0].daily_limit == 5000
