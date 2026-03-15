"""
Tests for Layer 2 — Domain + MX Validation

Strategy:
  - Patch _make_resolver to inject a mock resolver — avoids any resolv.conf
    dependency so tests run identically in Docker, CI, and local sandboxes.
  - Tests cover logic (caching, sorting, error handling), not the DNS library.
  - Integration tests are marked skip and can be run locally: pytest -m integration
"""

from __future__ import annotations

import pytest
import dns.resolver
import dns.exception
from unittest.mock import AsyncMock, MagicMock, patch

from app.validators.domain import (
    DomainResult,
    MXRecord,
    clear_cache,
    validate_domain,
)

PATCH_RESOLVER = "app.validators.domain._make_resolver"


def _mock_resolver(side_effects: list) -> MagicMock:
    resolver = MagicMock()
    resolver.resolve = AsyncMock(side_effect=side_effects)
    return resolver


def make_mx_answer(pairs: list) -> list:
    records = []
    for preference, exchange in pairs:
        rdata = MagicMock()
        rdata.preference = preference
        rdata.exchange = exchange + "."
        records.append(rdata)
    return records


def make_a_answer() -> list:
    return [MagicMock()]


@pytest.fixture(autouse=True)
def clear_domain_cache():
    clear_cache()
    yield
    clear_cache()


class TestValidDomain:
    @pytest.mark.asyncio
    async def test_simple_valid_domain(self):
        mx = make_mx_answer([(10, "mail.example.com")])
        with patch(PATCH_RESOLVER, return_value=_mock_resolver([make_a_answer(), mx])):
            result = await validate_domain("example.com", use_cache=False)
        assert result.valid is True
        assert result.domain_exists is True
        assert result.mx_found is True
        assert result.error is None

    @pytest.mark.asyncio
    async def test_mx_records_sorted_by_preference(self):
        mx = make_mx_answer([(30, "mx3.example.com"), (10, "mx1.example.com"), (20, "mx2.example.com")])
        with patch(PATCH_RESOLVER, return_value=_mock_resolver([make_a_answer(), mx])):
            result = await validate_domain("example.com", use_cache=False)
        assert result.mx_records[0].host == "mx1.example.com"
        assert result.mx_records[1].host == "mx2.example.com"
        assert result.mx_records[2].host == "mx3.example.com"

    @pytest.mark.asyncio
    async def test_primary_mx_is_lowest_preference(self):
        mx = make_mx_answer([(20, "backup.example.com"), (10, "primary.example.com")])
        with patch(PATCH_RESOLVER, return_value=_mock_resolver([make_a_answer(), mx])):
            result = await validate_domain("example.com", use_cache=False)
        assert result.primary_mx == "primary.example.com"

    @pytest.mark.asyncio
    async def test_trailing_dot_stripped_from_mx_host(self):
        mx = make_mx_answer([(10, "mail.example.com")])
        with patch(PATCH_RESOLVER, return_value=_mock_resolver([make_a_answer(), mx])):
            result = await validate_domain("example.com", use_cache=False)
        assert not result.mx_records[0].host.endswith(".")

    @pytest.mark.asyncio
    async def test_mx_host_lowercased(self):
        mx = make_mx_answer([(10, "MX.EXAMPLE.COM")])
        with patch(PATCH_RESOLVER, return_value=_mock_resolver([make_a_answer(), mx])):
            result = await validate_domain("example.com", use_cache=False)
        assert result.mx_records[0].host == "mx.example.com"

    @pytest.mark.asyncio
    async def test_domain_input_normalised_lowercase(self):
        mx = make_mx_answer([(10, "mail.example.com")])
        with patch(PATCH_RESOLVER, return_value=_mock_resolver([make_a_answer(), mx])):
            result = await validate_domain("EXAMPLE.COM", use_cache=False)
        assert result.domain == "example.com"


class TestInvalidDomain:
    @pytest.mark.asyncio
    async def test_nxdomain(self):
        with patch(PATCH_RESOLVER, return_value=_mock_resolver([dns.resolver.NXDOMAIN()])):
            result = await validate_domain("definitelynotareal.xyz", use_cache=False)
        assert result.valid is False
        assert result.domain_exists is False
        assert result.error is not None

    @pytest.mark.asyncio
    async def test_domain_exists_but_no_mx(self):
        with patch(PATCH_RESOLVER, return_value=_mock_resolver([make_a_answer(), dns.resolver.NoAnswer()])):
            result = await validate_domain("example.com", use_cache=False)
        assert result.valid is False
        assert result.domain_exists is True
        assert result.mx_found is False

    @pytest.mark.asyncio
    async def test_mx_timeout_returns_error(self):
        with patch(PATCH_RESOLVER, return_value=_mock_resolver([make_a_answer(), dns.exception.Timeout()])):
            result = await validate_domain("example.com", use_cache=False)
        assert result.valid is False
        assert "timeout" in result.error.lower()

    @pytest.mark.asyncio
    async def test_no_nameservers_returns_error(self):
        with patch(PATCH_RESOLVER, return_value=_mock_resolver([make_a_answer(), dns.resolver.NoNameservers()])):
            result = await validate_domain("example.com", use_cache=False)
        assert result.valid is False
        assert result.error is not None


class TestCaching:
    @pytest.mark.asyncio
    async def test_second_call_uses_cache(self):
        mx = make_mx_answer([(10, "mail.example.com")])
        mock_r = _mock_resolver([make_a_answer(), mx])
        with patch(PATCH_RESOLVER, return_value=mock_r):
            r1 = await validate_domain("example.com")
            r2 = await validate_domain("example.com")
        assert mock_r.resolve.call_count == 2   # A + MX, not 4
        assert r1 == r2

    @pytest.mark.asyncio
    async def test_use_cache_false_bypasses_cache(self):
        mx1 = make_mx_answer([(10, "mail.example.com")])
        mx2 = make_mx_answer([(10, "mail2.example.com")])
        mock_r = _mock_resolver([make_a_answer(), mx1, make_a_answer(), mx2])
        with patch(PATCH_RESOLVER, return_value=mock_r):
            r1 = await validate_domain("example.com", use_cache=False)
            r2 = await validate_domain("example.com", use_cache=False)
        assert r1.primary_mx == "mail.example.com"
        assert r2.primary_mx == "mail2.example.com"

    @pytest.mark.asyncio
    async def test_different_domains_cached_separately(self):
        mx_a = make_mx_answer([(10, "mail.a.com")])
        mx_b = make_mx_answer([(10, "mail.b.com")])
        mock_r = _mock_resolver([make_a_answer(), mx_a, make_a_answer(), mx_b])
        with patch(PATCH_RESOLVER, return_value=mock_r):
            ra = await validate_domain("a.com")
            rb = await validate_domain("b.com")
        assert ra.primary_mx == "mail.a.com"
        assert rb.primary_mx == "mail.b.com"

    @pytest.mark.asyncio
    async def test_clear_cache_forces_refetch(self):
        mx = make_mx_answer([(10, "mail.example.com")])
        mock_r = _mock_resolver([make_a_answer(), mx, make_a_answer(), make_mx_answer([(10, "mail.example.com")])])
        with patch(PATCH_RESOLVER, return_value=mock_r):
            await validate_domain("example.com")
            clear_cache()
            await validate_domain("example.com")
        assert mock_r.resolve.call_count == 4


class TestDomainResultHelpers:
    def test_primary_mx_none_when_no_records(self):
        result = DomainResult(valid=False, domain="x.com", domain_exists=False, mx_found=False, mx_records=[], error="no mx")
        assert result.primary_mx is None

    def test_primary_mx_returns_lowest_preference(self):
        result = DomainResult(
            valid=True, domain="x.com", domain_exists=True, mx_found=True,
            mx_records=[MXRecord(host="mx1.x.com", preference=10), MXRecord(host="mx2.x.com", preference=20)],
            error=None,
        )
        assert result.primary_mx == "mx1.x.com"

    def test_result_is_frozen(self):
        result = DomainResult(valid=True, domain="x.com", domain_exists=True, mx_found=True, mx_records=[], error=None)
        with pytest.raises((AttributeError, TypeError)):
            result.valid = False  # type: ignore[misc]


class TestAAAAFallback:
    @pytest.mark.asyncio
    async def test_aaaa_used_when_a_missing(self):
        mx = make_mx_answer([(10, "mail.example.com")])
        mock_r = _mock_resolver([dns.resolver.NXDOMAIN(), make_a_answer(), mx])
        with patch(PATCH_RESOLVER, return_value=mock_r):
            result = await validate_domain("ipv6only.example.com", use_cache=False)
        assert result.domain_exists is True
        assert result.valid is True


@pytest.mark.skip(reason="Requires live network — run locally with: pytest -m integration")
class TestIntegration:
    @pytest.mark.asyncio
    async def test_gmail_has_mx(self):
        result = await validate_domain("gmail.com", use_cache=False)
        assert result.valid is True
        assert result.primary_mx is not None

    @pytest.mark.asyncio
    async def test_fake_domain_fails(self):
        result = await validate_domain("thisdoesnotexist12345xyz.com", use_cache=False)
        assert result.valid is False
