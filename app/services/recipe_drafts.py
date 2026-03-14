from __future__ import annotations

from dataclasses import dataclass
import re

from app.models import RecipeAdapter, Vertical


@dataclass
class DraftProposal:
    prompt: str
    slug: str
    label: str
    description: str
    vertical: Vertical
    adapter: RecipeAdapter
    osm_tags: list[dict[str, str]]
    exclude_tags: list[dict[str, str]]
    search_terms: list[str]
    website_keywords: list[str]
    language_hints: list[str]
    rationale: list[str]


KEYWORD_RECIPES: list[dict[str, object]] = [
    {
        "match": ("car rental", "rent a car", "car hire"),
        "vertical": Vertical.VEHICLE,
        "label": "Car Rental",
        "osm_tags": [{"amenity": "car_rental"}],
        "exclude_tags": [],
        "search_terms": ["car rental", "rent a car", "car hire"],
        "website_keywords": ["car rental", "fleet", "booking"],
    },
    {
        "match": ("motorbike rental", "motorcycle rental", "scooter rental", "bike rental", "atv rental", "quad rental"),
        "vertical": Vertical.VEHICLE,
        "label": "Motorbike Rental",
        "osm_tags": [{"shop": "motorcycle_rental"}, {"amenity": "bicycle_rental"}],
        "exclude_tags": [{"shop": "travel_agency"}],
        "search_terms": ["motorbike rental", "motorcycle rental", "scooter rental", "atv rental"],
        "website_keywords": ["rent", "booking", "bike", "motorbike", "scooter", "atv"],
    },
    {
        "match": ("travel agency", "travel agent", "tour agency", "tour operator", "excursion"),
        "vertical": Vertical.TOURISM,
        "label": "Travel Agency",
        "osm_tags": [{"shop": "travel_agency"}, {"office": "travel_agent"}],
        "exclude_tags": [{"tourism": "information"}],
        "search_terms": ["travel agency", "tour operator", "excursion"],
        "website_keywords": ["tour", "travel", "package", "excursion"],
    },
    {
        "match": ("tour guide", "tour guide service", "guide service"),
        "vertical": Vertical.TOURISM,
        "label": "Tour Guide Service",
        "osm_tags": [{"tourism": "information"}],
        "exclude_tags": [{"shop": "travel_agency"}],
        "search_terms": ["tour guide", "guide service"],
        "website_keywords": ["guide", "tour", "private tour"],
    },
    {
        "match": ("elephant sanctuary", "elephant camp", "diving", "diver", "scuba", "snorkel"),
        "vertical": Vertical.TOURISM,
        "label": "Tourism Activity",
        "osm_tags": [{"tourism": "attraction"}, {"tourism": "information"}],
        "exclude_tags": [],
        "search_terms": ["elephant sanctuary", "diving", "scuba", "snorkel"],
        "website_keywords": ["contact", "booking", "tour", "activity"],
    },
]


def _slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return slug[:96] or "draft-recipe"


def _titleize(text: str) -> str:
    words = [chunk for chunk in re.split(r"[^A-Za-z0-9]+", text) if chunk]
    return " ".join(word.capitalize() for word in words[:8]) or "Draft Recipe"


def build_draft_from_prompt(prompt: str) -> DraftProposal:
    normalized = " ".join(prompt.strip().lower().split())
    if not normalized:
        raise ValueError("Prompt cannot be empty.")

    selected = None
    for candidate in KEYWORD_RECIPES:
        if any(phrase in normalized for phrase in candidate["match"]):
            selected = candidate
            break

    if selected is None:
        selected = {
            "vertical": Vertical.TOURISM,
            "label": _titleize(normalized),
            "osm_tags": [{"tourism": "information"}],
            "exclude_tags": [],
            "search_terms": [normalized],
            "website_keywords": ["contact", "about", "booking"],
        }
        rationale = [
            "No strong recipe family matched the prompt, so the draft starts from a broad tourism-oriented baseline.",
            "You should edit the OSM tags before trusting validation results.",
        ]
    else:
        rationale = [
            f"Matched the prompt against the '{selected['label']}' recipe family.",
            "Suggested tags are a starting point for sampled validation, not an activation-ready final recipe.",
        ]

    tokens = [token for token in re.split(r"[,/]| and ", normalized) if token.strip()]
    search_terms = list(dict.fromkeys([token.strip() for token in tokens if len(token.strip()) > 2] + list(selected["search_terms"])))[:8]
    website_keywords = list(dict.fromkeys(list(selected["website_keywords"]) + search_terms))[:10]
    language_hints: list[str] = []
    if any(country in normalized for country in ("thailand", "thai", "phuket", "chiang mai", "pattaya", "samui")):
        language_hints = ["en", "th"]
    elif any(country in normalized for country in ("germany", "berlin", "munich")):
        language_hints = ["en", "de"]
    elif any(country in normalized for country in ("france", "paris")):
        language_hints = ["en", "fr"]
    else:
        language_hints = ["en"]

    label = selected["label"]
    if label == "Tourism Activity":
        label = _titleize(normalized)

    return DraftProposal(
        prompt=prompt.strip(),
        slug=_slugify(normalized),
        label=label,
        description=f"Draft generated from prompt: {prompt.strip()}",
        vertical=selected["vertical"],
        adapter=RecipeAdapter.OVERPASS_PUBLIC,
        osm_tags=list(selected["osm_tags"]),
        exclude_tags=list(selected["exclude_tags"]),
        search_terms=search_terms,
        website_keywords=website_keywords,
        language_hints=language_hints,
        rationale=rationale,
    )
