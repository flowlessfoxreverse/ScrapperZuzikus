"""Shared pytest fixtures for the email validator test suite."""

import pytest


# Reusable email sets for parametrized tests across layers

VALID_EMAILS = [
    "user@example.com",
    "user+tag@example.com",
    "first.last@subdomain.example.com",
    "user@example.co.uk",
    "12345@example.org",
]

INVALID_EMAILS = [
    "notanemail",
    "@example.com",
    "user@",
    "user@@example.com",
    "",
    "   ",
]

DISPOSABLE_DOMAINS = [
    "mailinator.com",
    "guerrillamail.com",
    "10minutemail.com",
    "tempmail.com",
    "throwam.com",
]

ROLE_BASED_LOCALS = [
    "admin",
    "info",
    "support",
    "contact",
    "sales",
    "noreply",
    "no-reply",
    "postmaster",
    "webmaster",
    "hello",
    "help",
    "billing",
    "abuse",
    "security",
]
