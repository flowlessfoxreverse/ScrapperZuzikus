from __future__ import annotations

import re


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
