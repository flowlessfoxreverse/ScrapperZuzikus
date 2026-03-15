"""
Tests for Layer 4 — Typo Detection

Coverage:
  - Classic transposition typos (gmial, hotmial)
  - Substitution typos (gnail)
  - Insertion typos (gmaill)
  - Deletion typos (outlok, yaho)
  - TLD-only typos (gmail.cmo, gmail.ocm)
  - No suggestion for correct domains
  - No suggestion for unrelated domains
  - Short-domain threshold (aol, me)
  - Full email suggestion format
  - Distance attribute accuracy
  - Result immutability
  - _dl_distance and _split_domain unit tests
"""

from __future__ import annotations

import pytest

from app.validators.typo import (
    TypoResult,
    _dl_distance,
    _split_domain,
    check_typo,
    get_popular_domains,
)


# ── _dl_distance unit tests ───────────────────────────────────────────────────

class TestDLDistance:
    def test_identical_strings(self):
        assert _dl_distance("gmail", "gmail") == 0

    def test_empty_vs_nonempty(self):
        assert _dl_distance("", "gmail") == 5
        assert _dl_distance("gmail", "") == 5

    def test_both_empty(self):
        assert _dl_distance("", "") == 0

    def test_single_substitution(self):
        assert _dl_distance("gnail", "gmail") == 1

    def test_single_deletion(self):
        assert _dl_distance("gmai", "gmail") == 1

    def test_single_insertion(self):
        assert _dl_distance("gmaill", "gmail") == 1

    def test_transposition(self):
        # Transposition is 1 edit in DL, 2 in plain Levenshtein
        assert _dl_distance("gmial", "gmail") == 1
        assert _dl_distance("hotmial", "hotmail") == 1

    def test_two_edits(self):
        assert _dl_distance("gmal", "gmail") == 1   # deletion only
        assert _dl_distance("gmaok", "gmail") == 2  # two substitutions: o→i, k→l

    def test_completely_different(self):
        d = _dl_distance("yahoo", "gmail")
        assert d >= 4

    def test_tld_transposition(self):
        assert _dl_distance("cmo", "com") == 1
        assert _dl_distance("ocm", "com") == 1


# ── _split_domain unit tests ──────────────────────────────────────────────────

class TestSplitDomain:
    def test_simple(self):
        assert _split_domain("gmail.com") == ("gmail", "com")

    def test_two_part_tld(self):
        assert _split_domain("yahoo.co.uk") == ("yahoo", "co.uk")

    def test_no_dot(self):
        assert _split_domain("gmail") == ("gmail", "")

    def test_subdomain_preserved(self):
        # We only split on the first dot
        assert _split_domain("mail.google.com") == ("mail", "google.com")


# ── Transposition typos ───────────────────────────────────────────────────────

class TestTranspositionTypos:
    def test_gmial(self):
        r = check_typo("user", "gmial.com")
        assert r.has_typo is True
        assert r.suggested_domain == "gmail.com"
        assert r.suggestion == "user@gmail.com"

    def test_hotmial(self):
        r = check_typo("user", "hotmial.com")
        assert r.has_typo is True
        assert r.suggested_domain == "hotmail.com"

    def test_yahooo(self):
        r = check_typo("user", "yahooo.com")
        assert r.has_typo is True
        assert r.suggested_domain == "yahoo.com"

    def test_outloko(self):
        r = check_typo("user", "otulook.com")
        assert r.has_typo is True
        assert r.suggested_domain == "outlook.com"


# ── Substitution typos ────────────────────────────────────────────────────────

class TestSubstitutionTypos:
    def test_gnail(self):
        r = check_typo("user", "gnail.com")
        assert r.has_typo is True
        assert r.suggested_domain == "gmail.com"

    def test_hotmsil(self):
        r = check_typo("user", "hotmsil.com")
        assert r.has_typo is True
        assert r.suggested_domain == "hotmail.com"


# ── Deletion typos ────────────────────────────────────────────────────────────

class TestDeletionTypos:
    def test_outlok(self):
        r = check_typo("user", "outlok.com")
        assert r.has_typo is True
        assert r.suggested_domain == "outlook.com"

    def test_yaho(self):
        r = check_typo("user", "yaho.com")
        assert r.has_typo is True
        assert r.suggested_domain == "yahoo.com"

    def test_homail(self):
        r = check_typo("user", "homail.com")
        assert r.has_typo is True
        assert r.suggested_domain == "hotmail.com"


# ── Insertion typos ───────────────────────────────────────────────────────────

class TestInsertionTypos:
    def test_gmaill(self):
        r = check_typo("user", "gmaill.com")
        assert r.has_typo is True
        assert r.suggested_domain == "gmail.com"

    def test_outlookk(self):
        r = check_typo("user", "outlookk.com")
        assert r.has_typo is True
        assert r.suggested_domain == "outlook.com"


# ── TLD typos ─────────────────────────────────────────────────────────────────

class TestTLDTypos:
    def test_gmail_cmo(self):
        r = check_typo("user", "gmail.cmo")
        assert r.has_typo is True
        assert r.suggested_domain == "gmail.com"

    def test_gmail_ocm(self):
        r = check_typo("user", "gmail.ocm")
        assert r.has_typo is True
        assert r.suggested_domain == "gmail.com"

    def test_gmail_con(self):
        r = check_typo("user", "gmail.con")
        assert r.has_typo is True
        assert r.suggested_domain == "gmail.com"


# ── No suggestion for correct domains ────────────────────────────────────────

class TestCorrectDomains:
    @pytest.mark.parametrize("domain", [
        "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
        "icloud.com", "protonmail.com", "fastmail.com",
        "yahoo.co.uk", "hotmail.co.uk",
    ])
    def test_no_suggestion_for_correct_domain(self, domain):
        r = check_typo("user", domain)
        assert r.has_typo is False
        assert r.suggestion is None

    def test_result_distance_is_none_when_no_typo(self):
        r = check_typo("user", "gmail.com")
        assert r.distance is None


# ── No suggestion for unrelated domains ──────────────────────────────────────

class TestUnrelatedDomains:
    def test_company_domain_no_suggestion(self):
        r = check_typo("john", "acmecorp.com")
        assert r.has_typo is False

    def test_completely_different_no_suggestion(self):
        r = check_typo("user", "verylongdifferentdomain.org")
        assert r.has_typo is False

    def test_random_string_no_suggestion(self):
        r = check_typo("user", "xyzxyzxyz.com")
        assert r.has_typo is False


# ── Short domain threshold ────────────────────────────────────────────────────

class TestShortDomainThreshold:
    def test_aol_correct_no_suggestion(self):
        r = check_typo("user", "aol.com")
        assert r.has_typo is False

    def test_aol_one_edit_suggests(self):
        # 'aol' has threshold 1 — single edit should still be caught
        r = check_typo("user", "aal.com")
        assert r.has_typo is True
        assert r.suggested_domain == "aol.com"

    def test_me_correct_no_suggestion(self):
        r = check_typo("user", "me.com")
        assert r.has_typo is False


# ── Suggestion format ─────────────────────────────────────────────────────────

class TestSuggestionFormat:
    def test_suggestion_contains_original_local(self):
        r = check_typo("john.doe+tag", "gmial.com")
        assert r.suggestion == "john.doe+tag@gmail.com"

    def test_suggestion_is_full_email(self):
        r = check_typo("user", "hotmial.com")
        assert "@" in r.suggestion
        assert r.suggestion.startswith("user@")

    def test_original_domain_preserved(self):
        r = check_typo("user", "gmial.com")
        assert r.original_domain == "gmial.com"

    def test_distance_is_set_when_typo_found(self):
        r = check_typo("user", "gmial.com")
        assert isinstance(r.distance, int)
        assert r.distance >= 1

    def test_result_is_frozen(self):
        r = check_typo("user", "gmail.com")
        with pytest.raises((AttributeError, TypeError)):
            r.has_typo = True  # type: ignore[misc]


# ── Popular domains set ───────────────────────────────────────────────────────

class TestPopularDomainsSet:
    def test_returns_frozenset(self):
        domains = get_popular_domains()
        assert isinstance(domains, frozenset)

    def test_contains_major_providers(self):
        domains = get_popular_domains()
        for expected in ("gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com"):
            assert expected in domains, f"{expected} missing from popular domains"

    def test_all_lowercase(self):
        domains = get_popular_domains()
        for d in domains:
            assert d == d.lower(), f"{d!r} not lowercase"

    def test_all_contain_dot(self):
        domains = get_popular_domains()
        for d in domains:
            assert "." in d, f"{d!r} has no TLD separator"
