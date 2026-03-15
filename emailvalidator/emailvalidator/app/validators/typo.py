"""
Layer 4 — Typo Detection

Suggests corrections for common email domain typos:
    gmial.com     → gmail.com
    hotmial.com   → hotmail.com
    outlok.com    → outlook.com
    yaho.com      → yahoo.com

Algorithm (adapted from mailcheck.js, ported to Python):
  1. Split the email domain into label (e.g. 'gmail') and TLD (e.g. 'com').
  2. Score each candidate domain in the popular-domains list using
     Damerau-Levenshtein distance separately on the label and the TLD.
  3. The candidate with the lowest combined score wins, provided it's
     below the suggestion threshold.

Why Damerau-Levenshtein (not plain Levenshtein)?
  - DL handles transpositions as a single edit (gmial→gmail = 1, not 2).
  - Transpositions are the single most common human typing error.

Threshold design:
  - Domain label: max distance 2 for labels ≥ 5 chars, 1 for shorter ones.
    Rationale: 'gmal' (4 chars) at distance 1 from 'gmail' (5 chars) is
    ambiguous — it could be intentional. 'hotmial' at distance 1 from
    'hotmail' is almost certainly a typo.
  - TLD: max distance 1 (com/net/org are short; any more = different TLD).
  - We never suggest the same domain back (distance 0 = no typo).

Performance:
  - The popular-domains list has ~50 entries.
  - DL distance is O(m×n) where m,n are string lengths (≤15 chars each).
  - Total cost per email: ~50 × 2 × O(15²) ≈ microseconds.
  - No caching needed — fast enough on every call.
"""

from __future__ import annotations

from dataclasses import dataclass


# ── Popular domain list ───────────────────────────────────────────────────────
# Covers >95% of real-world email domains.
# Format: ("label", "tld")  →  the full domain is label + "." + tld
#
# Kept as tuples so we can score label and TLD independently.

_POPULAR_DOMAINS: tuple[tuple[str, str], ...] = (
    # ── Global giants ────────────────────────────────────────────────────────
    ("gmail",     "com"),
    ("googlemail","com"),
    ("yahoo",     "com"),
    ("yahoo",     "co.uk"),
    ("yahoo",     "co.in"),
    ("yahoo",     "com.br"),
    ("yahoo",     "com.ar"),
    ("yahoo",     "com.mx"),
    ("yahoo",     "com.au"),
    ("yahoo",     "fr"),
    ("yahoo",     "de"),
    ("yahoo",     "it"),
    ("yahoo",     "es"),
    ("hotmail",   "com"),
    ("hotmail",   "co.uk"),
    ("hotmail",   "fr"),
    ("hotmail",   "de"),
    ("hotmail",   "it"),
    ("hotmail",   "es"),
    ("hotmail",   "com.br"),
    ("outlook",   "com"),
    ("live",      "com"),
    ("live",      "co.uk"),
    ("live",      "fr"),
    ("live",      "de"),
    ("msn",       "com"),
    ("icloud",    "com"),
    ("me",        "com"),
    ("mac",       "com"),
    # ── Regional / popular ────────────────────────────────────────────────────
    ("aol",       "com"),
    ("protonmail","com"),
    ("protonmail","ch"),
    ("proton",    "me"),
    ("pm",        "me"),
    ("zoho",      "com"),
    ("fastmail",  "com"),
    ("fastmail",  "fm"),
    ("hey",       "com"),
    ("tutanota",  "com"),
    ("tutamail",  "com"),
    ("tuta",      "io"),
    ("gmx",       "com"),
    ("gmx",       "de"),
    ("gmx",       "net"),
    ("web",       "de"),
    ("mail",      "ru"),
    ("yandex",    "com"),
    ("yandex",    "ru"),
    ("rambler",   "ru"),
    ("inbox",     "com"),
    ("inbox",     "lv"),
    ("rediffmail","com"),
    ("comcast",   "net"),
    ("verizon",   "net"),
    ("att",       "net"),
    ("sbcglobal", "net"),
    ("bellsouth",  "net"),
    ("cox",       "net"),
    ("charter",   "net"),
    ("earthlink", "net"),
    ("optonline", "net"),
    # ── TLDs to recognise ────────────────────────────────────────────────────
    # These help suggest correct TLD when only that part is mistyped:
    # user@gmail.cmo → user@gmail.com
)

# Pre-build full domain strings for fast membership check
_POPULAR_DOMAIN_SET: frozenset[str] = frozenset(
    f"{label}.{tld}" for label, tld in _POPULAR_DOMAINS
)


# ── Core algorithm ────────────────────────────────────────────────────────────

def _dl_distance(s1: str, s2: str) -> int:
    """
    Damerau-Levenshtein distance between two strings.
    Counts insertions, deletions, substitutions, and transpositions,
    each as a single edit operation.
    """
    if s1 == s2:
        return 0
    l1, l2 = len(s1), len(s2)
    if not l1:
        return l2
    if not l2:
        return l1

    # Build the DP matrix
    m = [[0] * (l2 + 1) for _ in range(l1 + 1)]
    for i in range(l1 + 1):
        m[i][0] = i
    for j in range(l2 + 1):
        m[0][j] = j

    for i in range(1, l1 + 1):
        for j in range(1, l2 + 1):
            cost = 0 if s1[i - 1] == s2[j - 1] else 1
            m[i][j] = min(
                m[i - 1][j] + 1,        # deletion
                m[i][j - 1] + 1,        # insertion
                m[i - 1][j - 1] + cost, # substitution
            )
            # Transposition (Damerau extension)
            if i > 1 and j > 1 and s1[i - 1] == s2[j - 2] and s1[i - 2] == s2[j - 1]:
                m[i][j] = min(m[i][j], m[i - 2][j - 2] + cost)

    return m[l1][l2]


def _split_domain(domain: str) -> tuple[str, str]:
    """
    Split 'gmail.com' → ('gmail', 'com').
    Split 'yahoo.co.uk' → ('yahoo', 'co.uk').
    Split 'gmail' (no dot) → ('gmail', '').
    """
    parts = domain.split(".", 1)
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]


def _label_threshold(label: str) -> int:
    """
    Max allowable DL distance for the domain label.
    Shorter labels need a tighter threshold to avoid false positives.

    Examples:
        'aol'  (3) → 1   (any 2-edit change on 3 chars is a different word)
        'live' (4) → 1
        'gmail' (5) → 2  (gmial, gmaill, gnail all correctly caught)
        'hotmail' (7) → 2
    """
    return 1 if len(label) <= 4 else 2


# ── Public API ────────────────────────────────────────────────────────────────

@dataclass(frozen=True, slots=True)
class TypoResult:
    has_typo: bool
    suggestion: str | None      # full corrected email, e.g. "user@gmail.com"
    original_domain: str
    suggested_domain: str | None
    distance: int | None        # combined label+tld distance


def check_typo(local: str, domain: str) -> TypoResult:
    """
    Check whether the domain looks like a typo of a known popular domain
    and return a correction suggestion if so.

    Args:
        local:  Local part of the email (before @), already normalised.
        domain: Domain part of the email (after @), already lowercased.

    Returns:
        TypoResult. If has_typo is True, suggestion contains the full
        corrected email address. Otherwise suggestion is None.

    Examples:
        >>> check_typo("user", "gmial.com")
        TypoResult(has_typo=True, suggestion="user@gmail.com", ...)

        >>> check_typo("user", "gmail.com")
        TypoResult(has_typo=False, suggestion=None, ...)
    """
    # If it's already a known popular domain, no typo
    if domain in _POPULAR_DOMAIN_SET:
        return TypoResult(
            has_typo=False,
            suggestion=None,
            original_domain=domain,
            suggested_domain=None,
            distance=None,
        )

    input_label, input_tld = _split_domain(domain)
    best_domain: str | None = None
    best_score: int = 999

    for cand_label, cand_tld in _POPULAR_DOMAINS:
        label_dist = _dl_distance(input_label, cand_label)
        tld_dist   = _dl_distance(input_tld, cand_tld)

        # Hard-reject on per-part thresholds before combining
        if label_dist > _label_threshold(cand_label):
            continue
        if tld_dist > 1:
            continue

        combined = label_dist + tld_dist

        # Prefer lowest combined distance; break ties by shorter label distance
        if combined < best_score or (
            combined == best_score
            and label_dist < _dl_distance(input_label, (best_domain or ".").split(".")[0])
        ):
            best_score = combined
            best_domain = f"{cand_label}.{cand_tld}"

    if best_domain is None or best_score == 0:
        return TypoResult(
            has_typo=False,
            suggestion=None,
            original_domain=domain,
            suggested_domain=None,
            distance=None,
        )

    return TypoResult(
        has_typo=True,
        suggestion=f"{local}@{best_domain}",
        original_domain=domain,
        suggested_domain=best_domain,
        distance=best_score,
    )


def get_popular_domains() -> frozenset[str]:
    """Return the current set of popular domains used for typo comparison."""
    return _POPULAR_DOMAIN_SET
