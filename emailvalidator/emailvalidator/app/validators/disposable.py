"""
Layer 3 — Disposable + Role-based Detection

Two independent checks, both O(1) lookups against in-memory sets:

  1. Disposable domain detection
     Loads from the disposable-email-domains blocklist (GitHub). The list
     is fetched once at startup and cached in memory. A curated seed list
     is bundled as a fallback so the validator works fully offline or when
     the upstream list is temporarily unavailable.

  2. Role-based address detection
     Flags locals like admin@, info@, noreply@ that are shared inboxes,
     not individual people. These addresses have lower deliverability value
     for outreach — most validation services mark them as "risky".

Design choices:
  - Both checks are synchronous — no I/O, pure set membership.
  - The domain blocklist is a frozenset after loading (immutable, slightly
    faster membership tests than a regular set).
  - Reload is explicit (call reload_disposable_list()) — no background threads.
"""

from __future__ import annotations

import logging
import urllib.request
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Blocklist source ──────────────────────────────────────────────────────────

BLOCKLIST_URL = (
    "https://raw.githubusercontent.com/disposable-email-domains/"
    "disposable-email-domains/master/disposable_email_blocklist.conf"
)

# Seed list — ~150 well-known disposable/throwaway services.
# Used as fallback when the upstream list can't be fetched.
# The full GitHub list has 100k+ entries; this covers the most common ones.
_SEED_DOMAINS: frozenset[str] = frozenset({
    # ── High-volume throwaway services ────────────────────────────────────────
    "mailinator.com", "guerrillamail.com", "guerrillamail.net",
    "guerrillamail.org", "guerrillamail.de", "guerrillamail.info",
    "guerrillamailblock.com", "grr.la", "sharklasers.com",
    "guerrillamailblock.com", "spam4.me", "trashmail.com",
    "trashmail.at", "trashmail.io", "trashmail.me", "trashmail.net",
    "trashmail.org", "trashmail.xyz", "trashmailer.com",
    "10minutemail.com", "10minutemail.net", "10minutemail.org",
    "10minutemail.de", "10minutemail.co.za", "10minutemail.ru",
    "tempmail.com", "tempmail.net", "tempmail.org", "temp-mail.org",
    "temp-mail.io", "tempmail.de", "tempinbox.com", "tempr.email",
    "throwam.com", "throwam.net", "throwaway.email",
    "dispostable.com", "discard.email", "discardmail.com",
    "discardmail.de", "spamgourmet.com", "spamgourmet.net",
    "spamgourmet.org", "spamoff.de", "spamspot.com",
    "yopmail.com", "yopmail.fr", "yopmail.net",
    "cool.fr.nf", "jetable.fr.nf", "nospam.ze.tc",
    "nomail.xl.cx", "mega.zik.dj", "speed.1s.fr",
    "courriel.fr.nf", "moncourrier.fr.nf", "monemail.fr.nf",
    "monmail.fr.nf", "jetable.net", "jetable.org",
    "mailnull.com", "maildrop.cc", "mailnew.com",
    "mailcatch.com", "mailnesia.com", "mailnull.com",
    "mailexpire.com", "mailme.lv", "mailme24.com",
    "mailmetrash.com", "mailmoat.com", "mailnull.com",
    "mailscrap.com", "mailseal.de", "mailtemp.info",
    "mailtome.de", "mailtothis.com", "mailzilla.com",
    "spamfree24.org", "spamfree24.de", "spamfree24.eu",
    "spamfree24.info", "spamfree24.net", "spamfree.eu",
    "spamgob.com", "spamhereplease.com", "spamhole.com",
    "spamify.com", "spamkill.info", "spaml.com",
    "spaml.de", "spammotel.com", "spammotels.com",
    "spammy.host", "spamok.com", "spampa.com",
    "spamslicer.com", "spamstack.net", "spamthis.co.uk",
    "spamthisplease.com", "spamtrail.com", "spamtroll.net",
    "spamwc.de", "spamwc.cf", "spamwc.ga", "spamwc.gq",
    "fakeinbox.com", "fake-box.com", "fakedemail.com",
    "fakemailz.com", "fakeemails.com",
    "getnada.com", "getnada.me",
    "getairmail.com", "getonemail.com",
    "disposableaddress.com", "disposableemailaddresses.com",
    "disposableemailaddresses.emailmiser.com",
    "dispomail.eu", "dispostable.com",
    "dodgit.com", "dodgit.org",
    "dontreg.com", "dontsendmespam.de",
    "drdrb.com", "drdrb.net",
    "dumpmail.de", "dumpyemail.com",
    "e4ward.com", "easytrashmail.com",
    "einrot.com", "emailgo.de",
    "emailias.com", "emaillime.com",
    "emailmiser.com", "emailsensei.com",
    "emailtemporanea.com", "emailtemporanea.net",
    "emailtemporanea.org", "emailthe.net",
    "emailtmp.com", "emailwarden.com",
    "emailx.at.hm", "emailxfer.com",
    "emz.net", "enterto.com",
    "ephemail.net", "etranquil.com",
    "etranquil.net", "etranquil.org",
    "evopo.com", "explodemail.com",
    "eyepaste.com", "f4k.es",
    "fast-email.com", "fast-mail.fr",
    "fastem.com", "fastemail.us",
    "fastemailer.com", "faster-email.com",
    "fastimap.com", "fastmazda.com",
    "fastmessaging.com", "fastsfwmail.com",
    "fastswift.com", "fastyandex.com",
    "fightallspam.com", "fiifke.de",
    "filzmail.com", "fixmail.tk",
    "fizmail.com", "fleckens.hu",
    "flurred.com", "flyspam.com",
    "frapmail.com", "friendlymail.co.uk",
    "front14.org", "fudgerub.com",
    "fun2.biz", "furzauflunge.de",
    "fux0ringduh.com", "fw2.me",
    "getairmail.com", "getmails.eu",
    "getnowtoday.cf", "gishpuppy.com",
    "gmailom.co", "goemailgo.com",
    "gotmail.com", "gotmail.net",
    "gotmail.org",
    # ── One-time-use / anonymous ──────────────────────────────────────────────
    "anonaddy.com", "anonaddy.me",
    "33mail.com", "spamgourmet.com",
    "guerrillamail.biz", "spam.la",
    "binkmail.com", "bob.email",
    "bofthew.com", "boun.cr",
    "bouncr.com", "breakthru.com",
    "brefmail.com", "brn.ee",
    "bspamfree.org", "bugmenot.com",
    "bumpymail.com",
})

# ── Role-based locals ─────────────────────────────────────────────────────────
# These are shared/departmental inboxes, not individual people.
# Full list is comprehensive but not exhaustive.
ROLE_BASED_LOCALS: frozenset[str] = frozenset({
    "abuse", "admin", "administrator", "all", "billing",
    "contact", "contactus", "cs", "customercare", "customersupport",
    "customerservice", "devnull", "dns", "ftp", "hello", "help",
    "helpdesk", "hostmaster", "info", "information", "invoice",
    "it", "legal", "mailer-daemon", "marketing", "media",
    "news", "newsletter", "no-reply", "nobody", "noreply",
    "null", "operations", "orders", "postmaster", "press",
    "privacy", "recruiting", "recruitment", "refunds", "register",
    "registration", "reply", "root", "sales", "security",
    "service", "services", "spam", "sso", "subscribe",
    "support", "sysadmin", "tech", "techsupport", "test",
    "unsubscribe", "usenet", "uucp", "webmaster", "www",
})


# ── State ─────────────────────────────────────────────────────────────────────

_disposable_domains: frozenset[str] = _SEED_DOMAINS
_loaded_from_upstream: bool = False


# ── Public API ────────────────────────────────────────────────────────────────

@dataclass(frozen=True, slots=True)
class DisposableResult:
    is_disposable: bool
    is_role_based: bool
    domain: str
    local: str


def check_disposable(local: str, domain: str) -> DisposableResult:
    """
    Check whether an email is disposable or role-based.

    Both checks are O(1) set lookups — safe to call in the hot path.

    Args:
        local:  The local part of the email (before @), already lowercased.
        domain: The domain part of the email (after @), already lowercased.

    Returns:
        DisposableResult with is_disposable and is_role_based flags.
    """
    return DisposableResult(
        is_disposable=domain in _disposable_domains,
        is_role_based=local in ROLE_BASED_LOCALS,
        domain=domain,
        local=local,
    )


def reload_disposable_list(*, timeout: int = 10) -> int:
    """
    Fetch the latest disposable domain blocklist from GitHub and replace
    the in-memory set. Falls back to the seed list on any error.

    Returns the number of domains loaded.
    Call this once at application startup (e.g. in a FastAPI lifespan event).
    """
    global _disposable_domains, _loaded_from_upstream

    try:
        req = urllib.request.Request(
            BLOCKLIST_URL,
            headers={"User-Agent": "emailvalidator/1.0"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")

        domains = frozenset(
            line.strip().lower()
            for line in raw.splitlines()
            if line.strip() and not line.startswith("#")
        )

        if len(domains) < 1000:
            # Sanity check — the real list has 100k+ entries.
            # A tiny response means something went wrong.
            raise ValueError(f"Suspiciously small blocklist: {len(domains)} entries")

        _disposable_domains = domains
        _loaded_from_upstream = True
        logger.info("Loaded %d disposable domains from upstream", len(domains))
        return len(domains)

    except Exception as exc:
        logger.warning(
            "Failed to load upstream disposable list, using seed (%d domains): %s",
            len(_SEED_DOMAINS),
            exc,
        )
        _disposable_domains = _SEED_DOMAINS
        _loaded_from_upstream = False
        return len(_SEED_DOMAINS)


def add_domain(domain: str) -> None:
    """
    Add a single domain to the in-memory blocklist.
    Useful for customer-specific blocks or test overrides.
    """
    global _disposable_domains
    _disposable_domains = _disposable_domains | {domain.lower().strip()}


def remove_domain(domain: str) -> None:
    """Remove a single domain from the in-memory blocklist."""
    global _disposable_domains
    _disposable_domains = _disposable_domains - {domain.lower().strip()}


def blocklist_size() -> int:
    """Return the current number of blocked domains."""
    return len(_disposable_domains)


def is_loaded_from_upstream() -> bool:
    """Return True if the upstream list was fetched successfully."""
    return _loaded_from_upstream
