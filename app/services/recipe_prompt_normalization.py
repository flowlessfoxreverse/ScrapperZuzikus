from __future__ import annotations

import re

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Region


PROMPT_REPLACEMENTS: tuple[tuple[str, str], ...] = (
    ("rent a car", "car rental"),
    ("car hire", "car rental"),
    ("motorbike hire", "motorbike rental"),
    ("motorcycle hire", "motorbike rental"),
    ("bike hire", "bike rental"),
    ("quad bike", "quad"),
    ("atv", "quad"),
    ("haircut", "hair"),
    ("hairstylist", "hair"),
    ("nails", "nail"),
    ("manicure", "nail"),
    ("pedicure", "nail"),
    ("eyelashes", "lash"),
    ("eyebrow", "brow"),
    ("wellness", "spa"),
    ("scuba", "diving"),
    ("lawyer", "law"),
    ("attorney", "law"),
    ("course", "training"),
    ("academy", "training"),
    ("estate agent", "property agency"),
    ("real estate", "property"),
)

PROMPT_STOPWORDS = {
    "a",
    "an",
    "and",
    "best",
    "business",
    "businesses",
    "complete",
    "company",
    "companies",
    "experience",
    "for",
    "full",
    "in",
    "local",
    "of",
    "service",
    "services",
    "the",
    "with",
}


def normalize_prompt_text(prompt: str) -> str:
    normalized = prompt.strip().lower()
    normalized = normalized.replace("&", " and ")
    normalized = normalized.replace("/", " ")
    normalized = normalized.replace("-", " ")
    normalized = re.sub(r"[^a-z0-9\s]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    for source, target in PROMPT_REPLACEMENTS:
        normalized = normalized.replace(source, target)
    tokens: list[str] = []
    for token in normalized.split():
        if token in PROMPT_STOPWORDS:
            continue
        if len(token) > 4 and token.endswith("ies"):
            token = f"{token[:-3]}y"
        elif len(token) > 4 and token.endswith("s") and not token.endswith("ss"):
            token = token[:-1]
        tokens.append(token)
    return " ".join(tokens)


def resolve_prompt_country_code(session: Session, prompt: str) -> str | None:
    normalized_prompt = normalize_prompt_text(prompt)
    if not normalized_prompt:
        return None

    countries = session.scalars(
        select(Region)
        .where(Region.osm_admin_level <= 2, Region.is_active.is_(True))
        .order_by(Region.country_code, Region.name)
    ).all()
    seen_codes: set[str] = set()
    for region in countries:
        if not region.country_code or region.country_code in seen_codes:
            continue
        seen_codes.add(region.country_code)
        country_code = region.country_code.lower()
        candidate_names = {
            normalize_prompt_text(region.name),
            country_code,
        }
        stripped_name = re.sub(r"\s*\[[A-Z0-9-]+\].*$", "", region.name).strip()
        if stripped_name:
            candidate_names.add(normalize_prompt_text(stripped_name))
        for candidate in candidate_names:
            if not candidate:
                continue
            if candidate == normalized_prompt:
                return region.country_code
            if f" {candidate} " in f" {normalized_prompt} ":
                return region.country_code
    return None
