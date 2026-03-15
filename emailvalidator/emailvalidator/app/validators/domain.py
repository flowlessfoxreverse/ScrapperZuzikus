"""
Layer 2 — Domain + MX Validation

Checks that:
  1. The domain exists (A or AAAA record resolves)
  2. The domain has MX records (it actually accepts email)

Key design decisions:
  - In-process TTL cache (TTLCache) per domain — MX records change rarely.
    A warm cache means zero DNS calls for repeat queries on popular domains.
  - Async-first: uses dns.asyncresolver so it never blocks the event loop.
  - Configurable resolver timeout + nameservers (defaults to system resolver).
  - Returns a structured DomainResult — no raw exceptions leak to callers.

Typical latency: 20–80ms first call, <1ms cached.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

import dns.asyncresolver
import dns.exception
import dns.rdatatype
from cachetools import TTLCache

logger = logging.getLogger(__name__)

# ── Cache ─────────────────────────────────────────────────────────────────────
# Keyed by domain string. TTL=3600s (1 hour) — MX records are stable.
# maxsize=4096 covers most production workloads without meaningful RAM cost.
_domain_cache: TTLCache[str, "DomainResult"] = TTLCache(maxsize=4096, ttl=3600)

# ── Configuration ─────────────────────────────────────────────────────────────
DEFAULT_TIMEOUT = 5.0       # seconds per DNS query
DEFAULT_LIFETIME = 8.0      # total time across retries


@dataclass(frozen=True, slots=True)
class MXRecord:
    host: str
    preference: int     # lower = higher priority


@dataclass(frozen=True, slots=True)
class DomainResult:
    valid: bool
    domain: str
    domain_exists: bool         # A/AAAA resolves
    mx_found: bool              # at least one MX record
    mx_records: list[MXRecord]  # sorted by preference
    error: str | None

    @property
    def primary_mx(self) -> str | None:
        """Return the hostname of the highest-priority MX record."""
        if not self.mx_records:
            return None
        return self.mx_records[0].host


async def validate_domain(
    domain: str,
    *,
    timeout: float = DEFAULT_TIMEOUT,
    lifetime: float = DEFAULT_LIFETIME,
    nameservers: list[str] | None = None,
    use_cache: bool = True,
) -> DomainResult:
    """
    Validate that a domain exists and accepts email.

    Args:
        domain:       Domain portion of the email (after @).
        timeout:      Per-query DNS timeout in seconds.
        lifetime:     Total lifetime across retries in seconds.
        nameservers:  Override DNS resolvers (e.g. ["8.8.8.8", "1.1.1.1"]).
                      Defaults to system resolver.
        use_cache:    Whether to use/populate the in-process TTL cache.

    Returns:
        DomainResult with mx_records sorted ascending by preference.
    """
    domain = domain.lower().strip().rstrip(".")

    if use_cache and domain in _domain_cache:
        logger.debug("domain cache hit: %s", domain)
        return _domain_cache[domain]

    result = await _resolve_domain(domain, timeout=timeout, lifetime=lifetime, nameservers=nameservers)

    if use_cache:
        _domain_cache[domain] = result

    return result


def _make_resolver(
    timeout: float,
    lifetime: float,
    nameservers: list[str] | None,
) -> dns.asyncresolver.Resolver:
    """
    Build a configured async resolver.

    Falls back to public DNS (1.1.1.1 / 8.8.8.8) when no system
    resolv.conf is available — common in Docker containers and CI.
    """
    try:
        resolver = dns.asyncresolver.Resolver()
    except dns.resolver.NoResolverConfiguration:
        resolver = dns.asyncresolver.Resolver(configure=False)
        resolver.nameservers = ["1.1.1.1", "8.8.8.8"]

    resolver.timeout = timeout
    resolver.lifetime = lifetime
    if nameservers:
        resolver.nameservers = nameservers
    return resolver


async def _resolve_domain(
    domain: str,
    *,
    timeout: float,
    lifetime: float,
    nameservers: list[str] | None,
) -> DomainResult:
    resolver = _make_resolver(timeout, lifetime, nameservers)

    # Step 1 — check domain existence via A or AAAA
    domain_exists = await _check_domain_exists(resolver, domain)

    if not domain_exists:
        return DomainResult(
            valid=False,
            domain=domain,
            domain_exists=False,
            mx_found=False,
            mx_records=[],
            error=f"Domain '{domain}' does not exist",
        )

    # Step 2 — fetch MX records
    mx_records, mx_error = await _fetch_mx_records(resolver, domain)

    if not mx_records:
        # Some domains have no MX but do have A records (implicit MX).
        # Per RFC 5321 §5.1 this is technically valid, but deliverability
        # is uncertain — we mark it as invalid for email purposes.
        return DomainResult(
            valid=False,
            domain=domain,
            domain_exists=True,
            mx_found=False,
            mx_records=[],
            error=mx_error or f"No MX records found for '{domain}'",
        )

    return DomainResult(
        valid=True,
        domain=domain,
        domain_exists=True,
        mx_found=True,
        mx_records=mx_records,
        error=None,
    )


async def _check_domain_exists(resolver: dns.asyncresolver.Resolver, domain: str) -> bool:
    """Return True if the domain has at least one A or AAAA record."""
    for rdtype in ("A", "AAAA"):
        try:
            await resolver.resolve(domain, rdtype)
            return True
        except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer):
            continue
        except (dns.exception.Timeout, dns.resolver.NoNameservers) as exc:
            logger.warning("DNS timeout/error checking %s %s: %s", rdtype, domain, exc)
            # Timeout is not the same as non-existence — treat as exists
            # to avoid false negatives (we'll catch it in MX step)
            return True
        except Exception as exc:  # noqa: BLE001
            logger.warning("Unexpected DNS error for %s: %s", domain, exc)
    return False


async def _fetch_mx_records(
    resolver: dns.asyncresolver.Resolver,
    domain: str,
) -> tuple[list[MXRecord], str | None]:
    """
    Fetch and sort MX records for a domain.

    Returns (records, error_string). On success error is None.
    On failure records is [] and error describes why.
    """
    try:
        answers = await resolver.resolve(domain, "MX")
        records = sorted(
            [
                MXRecord(
                    host=str(rdata.exchange).rstrip(".").lower(),
                    preference=rdata.preference,
                )
                for rdata in answers
            ],
            key=lambda r: r.preference,
        )
        return records, None

    except dns.resolver.NoAnswer:
        return [], f"No MX records for '{domain}'"

    except dns.resolver.NXDOMAIN:
        return [], f"Domain '{domain}' does not exist"

    except dns.exception.Timeout:
        return [], f"DNS timeout resolving MX for '{domain}'"

    except dns.resolver.NoNameservers:
        return [], f"No nameservers available for '{domain}'"

    except Exception as exc:  # noqa: BLE001
        logger.error("Unexpected MX lookup error for %s: %s", domain, exc)
        return [], f"DNS error: {exc}"


def clear_cache() -> None:
    """Clear the domain TTL cache. Useful in tests."""
    _domain_cache.clear()
