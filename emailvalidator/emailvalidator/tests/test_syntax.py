"""
Tests for Layer 1 — Syntax Validation

Covers:
- Valid standard emails
- Valid edge cases (tags, subdomains, unicode)
- Invalid format cases
- Edge cases (empty, non-string, whitespace)
- Normalization behaviour
"""

import pytest

from app.validators.syntax import SyntaxResult, validate_syntax


# ── Helpers ──────────────────────────────────────────────────────────────────

def valid(email: str) -> SyntaxResult:
    result = validate_syntax(email)
    assert result.valid, f"Expected valid, got error: {result.error!r} for {email!r}"
    return result


def invalid(email: str) -> SyntaxResult:
    result = validate_syntax(email)
    assert not result.valid, f"Expected invalid, but got valid for {email!r}"
    return result


# ── Valid emails ──────────────────────────────────────────────────────────────

class TestValidEmails:
    def test_simple(self):
        r = valid("user@example.com")
        assert r.local == "user"
        assert r.domain == "example.com"

    def test_plus_tag(self):
        r = valid("user+newsletter@example.com")
        assert r.local == "user+newsletter"

    def test_subdomain(self):
        r = valid("user@mail.example.co.uk")
        assert r.domain == "mail.example.co.uk"

    def test_uppercase_normalised(self):
        r = valid("User@Example.COM")
        assert r.normalized == "user@example.com"

    def test_dot_in_local(self):
        valid("first.last@example.com")

    def test_numeric_local(self):
        valid("12345@example.com")

    def test_hyphen_in_domain(self):
        valid("user@my-company.com")

    def test_leading_whitespace_stripped(self):
        r = valid("  user@example.com  ")
        assert r.normalized == "user@example.com"

    def test_new_tld(self):
        valid("user@example.io")

    def test_long_local_part(self):
        # 64 chars is the max local part length per RFC
        local = "a" * 64
        valid(f"{local}@example.com")


# ── Invalid emails ────────────────────────────────────────────────────────────

class TestInvalidEmails:
    def test_missing_at(self):
        r = invalid("userexample.com")
        assert r.error is not None

    def test_missing_domain(self):
        invalid("user@")

    def test_missing_local(self):
        invalid("@example.com")

    def test_double_at(self):
        invalid("user@@example.com")

    def test_spaces_in_middle(self):
        invalid("user @example.com")

    def test_no_tld(self):
        # email-validator rejects bare hostnames without a dot
        invalid("user@localhost")

    def test_local_too_long(self):
        local = "a" * 65
        invalid(f"{local}@example.com")

    def test_plain_string(self):
        invalid("notanemail")

    def test_empty_string(self):
        r = invalid("")
        assert "empty" in r.error.lower()

    def test_only_whitespace(self):
        r = invalid("   ")
        assert r.error is not None

    def test_consecutive_dots_local(self):
        invalid("user..name@example.com")

    def test_leading_dot_local(self):
        invalid(".user@example.com")

    def test_trailing_dot_local(self):
        invalid("user.@example.com")


# ── Non-string inputs ─────────────────────────────────────────────────────────

class TestNonStringInputs:
    def test_none(self):
        r = invalid(None)  # type: ignore[arg-type]
        assert "string" in r.error.lower()

    def test_integer(self):
        r = invalid(123)  # type: ignore[arg-type]
        assert r.error is not None

    def test_list(self):
        r = invalid(["user@example.com"])  # type: ignore[arg-type]
        assert r.error is not None


# ── Result fields ─────────────────────────────────────────────────────────────

class TestResultFields:
    def test_valid_result_has_no_error(self):
        r = valid("user@example.com")
        assert r.error is None

    def test_invalid_result_has_error_string(self):
        r = invalid("bad")
        assert isinstance(r.error, str)
        assert len(r.error) > 0

    def test_invalid_result_fields_are_none(self):
        r = invalid("bad")
        assert r.normalized is None
        assert r.local is None
        assert r.domain is None

    def test_valid_result_is_frozen(self):
        r = valid("user@example.com")
        with pytest.raises((AttributeError, TypeError)):
            r.valid = False  # type: ignore[misc]
