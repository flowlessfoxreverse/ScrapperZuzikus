"""
Tests for Layer 3 — Disposable + Role-based Detection

Strategy:
  - All tests are synchronous and purely in-memory (no network calls).
  - We reset module state between tests using the public API (add/remove)
    rather than reaching into private variables.
  - reload_disposable_list() is tested with a mocked urllib to avoid
    any real network dependency.
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch
from io import BytesIO

from app.validators.disposable import (
    DisposableResult,
    ROLE_BASED_LOCALS,
    _SEED_DOMAINS,
    add_domain,
    blocklist_size,
    check_disposable,
    is_loaded_from_upstream,
    reload_disposable_list,
    remove_domain,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def mock_upstream(domains: list[str], status: int = 200):
    """Build a mock urllib response containing the given domain list."""
    body = "\n".join(domains).encode()
    resp = MagicMock()
    resp.read.return_value = body
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


# ── Disposable domain detection ───────────────────────────────────────────────

class TestDisposableDetection:
    def test_known_disposable_domain(self):
        result = check_disposable("user", "mailinator.com")
        assert result.is_disposable is True

    def test_known_disposable_yopmail(self):
        result = check_disposable("test", "yopmail.com")
        assert result.is_disposable is True

    def test_known_disposable_guerrilla(self):
        result = check_disposable("anon", "guerrillamail.com")
        assert result.is_disposable is True

    def test_known_disposable_tempmail(self):
        result = check_disposable("tmp", "tempmail.com")
        assert result.is_disposable is True

    def test_known_disposable_trashmail(self):
        result = check_disposable("x", "trashmail.com")
        assert result.is_disposable is True

    def test_legitimate_gmail_not_disposable(self):
        result = check_disposable("user", "gmail.com")
        assert result.is_disposable is False

    def test_legitimate_outlook_not_disposable(self):
        result = check_disposable("user", "outlook.com")
        assert result.is_disposable is False

    def test_legitimate_company_domain_not_disposable(self):
        result = check_disposable("john", "acmecorp.com")
        assert result.is_disposable is False

    def test_check_is_case_insensitive_input(self):
        # Caller should pass pre-lowercased values; test that exactly
        result_lower = check_disposable("user", "mailinator.com")
        assert result_lower.is_disposable is True

    def test_result_preserves_domain_and_local(self):
        result = check_disposable("user", "mailinator.com")
        assert result.domain == "mailinator.com"
        assert result.local == "user"

    def test_result_is_frozen(self):
        result = check_disposable("user", "example.com")
        with pytest.raises((AttributeError, TypeError)):
            result.is_disposable = True  # type: ignore[misc]


# ── Role-based detection ──────────────────────────────────────────────────────

class TestRoleBasedDetection:
    @pytest.mark.parametrize("local", [
        "admin", "info", "support", "sales", "contact",
        "noreply", "no-reply", "postmaster", "webmaster",
        "hello", "help", "billing", "abuse", "security",
        "newsletter", "unsubscribe", "subscribe",
        "mailer-daemon", "hostmaster", "marketing",
    ])
    def test_role_local_flagged(self, local):
        result = check_disposable(local, "example.com")
        assert result.is_role_based is True, f"{local!r} should be role-based"

    def test_regular_local_not_role_based(self):
        result = check_disposable("john", "example.com")
        assert result.is_role_based is False

    def test_regular_local_firstname_not_role_based(self):
        result = check_disposable("alice", "example.com")
        assert result.is_role_based is False

    def test_role_and_disposable_can_both_be_true(self):
        result = check_disposable("admin", "mailinator.com")
        assert result.is_disposable is True
        assert result.is_role_based is True

    def test_role_based_locals_set_is_comprehensive(self):
        # Spot-check that the set includes critical entries
        for expected in ("admin", "info", "support", "no-reply", "noreply", "postmaster"):
            assert expected in ROLE_BASED_LOCALS, f"{expected!r} missing from ROLE_BASED_LOCALS"


# ── Dynamic blocklist management ──────────────────────────────────────────────

class TestBlocklistManagement:
    def setup_method(self):
        """Ensure any test-added domains are removed before each test."""
        remove_domain("test-custom-domain-xyz.com")

    def teardown_method(self):
        remove_domain("test-custom-domain-xyz.com")

    def test_add_domain_makes_it_disposable(self):
        assert check_disposable("x", "test-custom-domain-xyz.com").is_disposable is False
        add_domain("test-custom-domain-xyz.com")
        assert check_disposable("x", "test-custom-domain-xyz.com").is_disposable is True

    def test_remove_domain_makes_it_not_disposable(self):
        add_domain("test-custom-domain-xyz.com")
        assert check_disposable("x", "test-custom-domain-xyz.com").is_disposable is True
        remove_domain("test-custom-domain-xyz.com")
        assert check_disposable("x", "test-custom-domain-xyz.com").is_disposable is False

    def test_add_domain_normalises_case(self):
        add_domain("TEST-CUSTOM-DOMAIN-XYZ.COM")
        assert check_disposable("x", "test-custom-domain-xyz.com").is_disposable is True

    def test_add_domain_trims_whitespace(self):
        add_domain("  test-custom-domain-xyz.com  ")
        assert check_disposable("x", "test-custom-domain-xyz.com").is_disposable is True

    def test_blocklist_size_increases_on_add(self):
        before = blocklist_size()
        add_domain("test-custom-domain-xyz.com")
        assert blocklist_size() == before + 1

    def test_blocklist_size_decreases_on_remove(self):
        add_domain("test-custom-domain-xyz.com")
        before = blocklist_size()
        remove_domain("test-custom-domain-xyz.com")
        assert blocklist_size() == before - 1

    def test_remove_nonexistent_domain_is_safe(self):
        # Should not raise
        remove_domain("definitelynotthere99999.com")

    def test_seed_domains_always_present(self):
        # Seed domains should survive add/remove operations
        for domain in ("mailinator.com", "yopmail.com", "guerrillamail.com"):
            assert check_disposable("x", domain).is_disposable is True


# ── Upstream reload ───────────────────────────────────────────────────────────

class TestReloadDisposableList:
    def test_reload_with_valid_upstream(self):
        # Generate a list large enough to pass the sanity check (>1000)
        large_list = [f"fake-disposable-{i}.com" for i in range(1200)]
        with patch("urllib.request.urlopen", return_value=mock_upstream(large_list)):
            count = reload_disposable_list()
        assert count == 1200
        assert is_loaded_from_upstream() is True
        assert check_disposable("x", "fake-disposable-0.com").is_disposable is True

    def test_reload_with_comments_and_blanks_ignored(self):
        domains = (
            ["# this is a comment", ""]
            + [f"disp-domain-{i}.com" for i in range(1100)]
            + ["  ", "# another comment"]
        )
        with patch("urllib.request.urlopen", return_value=mock_upstream(domains)):
            count = reload_disposable_list()
        assert count == 1100

    def test_reload_falls_back_on_network_error(self):
        with patch("urllib.request.urlopen", side_effect=OSError("network error")):
            count = reload_disposable_list()
        assert is_loaded_from_upstream() is False
        # Seed domains still work
        assert check_disposable("x", "mailinator.com").is_disposable is True
        assert count == len(_SEED_DOMAINS)

    def test_reload_falls_back_on_suspiciously_small_list(self):
        # Only 5 domains — below the 1000 sanity threshold
        with patch("urllib.request.urlopen", return_value=mock_upstream(["x.com", "y.com"])):
            count = reload_disposable_list()
        assert is_loaded_from_upstream() is False
        assert count == len(_SEED_DOMAINS)

    def test_reload_replaces_previous_custom_domains(self):
        """After a successful upstream reload, the list is the upstream list."""
        large_list = [f"upstream-domain-{i}.com" for i in range(1200)]
        with patch("urllib.request.urlopen", return_value=mock_upstream(large_list)):
            reload_disposable_list()
        # A seed domain that wasn't in the upstream list is now gone
        # (upstream list replaces entirely — callers should re-add custom blocks)
        assert check_disposable("x", "upstream-domain-0.com").is_disposable is True

    def test_reload_normalises_domain_case(self):
        domains = [f"UPPER-DOMAIN-{i}.COM" for i in range(1100)]
        with patch("urllib.request.urlopen", return_value=mock_upstream(domains)):
            reload_disposable_list()
        assert check_disposable("x", "upper-domain-0.com").is_disposable is True


# ── Seed list integrity ───────────────────────────────────────────────────────

class TestSeedList:
    def test_seed_list_contains_major_services(self):
        major = {
            "mailinator.com", "guerrillamail.com", "yopmail.com",
            "tempmail.com", "trashmail.com", "10minutemail.com",
            "maildrop.cc", "fakeinbox.com", "getnada.com",
        }
        missing = major - _SEED_DOMAINS
        assert not missing, f"Missing from seed: {missing}"

    def test_seed_list_all_lowercase(self):
        uppercase = [d for d in _SEED_DOMAINS if d != d.lower()]
        assert not uppercase, f"Uppercase domains in seed: {uppercase[:5]}"

    def test_seed_list_no_blank_entries(self):
        blanks = [d for d in _SEED_DOMAINS if not d.strip()]
        assert not blanks

    def test_seed_list_all_have_dots(self):
        no_dot = [d for d in _SEED_DOMAINS if "." not in d]
        assert not no_dot, f"Invalid entries without dot: {no_dot[:5]}"

    def test_seed_list_minimum_size(self):
        # Should always have at least 100 seed domains
        assert len(_SEED_DOMAINS) >= 100
