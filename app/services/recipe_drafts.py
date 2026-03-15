from __future__ import annotations

from dataclasses import dataclass
import re

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import QueryRecipeVariantTemplate, RecipeAdapter, RecipeSourceStrategy
from app.services.recipe_variants import normalize_prompt_text


@dataclass(frozen=True)
class DraftProposal:
    prompt: str
    slug: str
    label: str
    description: str
    vertical: str
    cluster_slug: str | None
    adapter: RecipeAdapter
    source_strategy: RecipeSourceStrategy
    template_key: str
    sub_intent: str
    osm_tags: list[dict[str, str]]
    exclude_tags: list[dict[str, str]]
    search_terms: list[str]
    website_keywords: list[str]
    language_hints: list[str]
    rationale: list[str]
    variant_key: str
    template_score: int
    prompt_match_score: int
    fit_score: int
    fit_reasons: list[str]
    observed_validation_score: int = 0
    historical_validation_count: int = 0
    cluster_validation_score: int = 0
    cluster_validation_count: int = 0
    variant_adoption_count: int = 0
    cluster_adoption_count: int = 0
    prompt_selection_count: int = 0
    prompt_draft_count: int = 0
    prompt_activation_count: int = 0
    production_score: int = 0
    production_run_count: int = 0


@dataclass(frozen=True)
class ClusterCandidate:
    vertical: str
    cluster_slug: str
    score: int
    matched_aliases: tuple[str, ...]
    rationale: list[str]
    historical_seen_count: int = 0
    historical_selected_count: int = 0
    ambiguity_count: int = 0


@dataclass(frozen=True)
class VariantTemplate:
    key: str
    label: str
    vertical: str
    cluster_slug: str
    sub_intent: str
    source_strategy: RecipeSourceStrategy
    aliases: tuple[str, ...]
    osm_tags: tuple[dict[str, str], ...]
    exclude_tags: tuple[dict[str, str], ...]
    search_terms: tuple[str, ...]
    website_keywords: tuple[str, ...]
    language_hints: tuple[str, ...] = ()
    rationale: tuple[str, ...] = ()
    priority: int = 50


PROMPT_CLUSTER_HINTS: tuple[dict[str, object], ...] = (
    {"vertical": "beauty", "cluster_slug": "beauty_services", "aliases": ("beauty", "salon", "hair", "barber", "nail", "spa", "waxing", "lash", "brow"), "rationale": "Prompt signals beauty-service intent."},
    {"vertical": "beauty", "cluster_slug": "beauty_clinics", "aliases": ("aesthetic", "cosmetic", "laser", "derma", "skin clinic"), "rationale": "Prompt signals aesthetic-clinic intent."},
    {"vertical": "vehicle", "cluster_slug": "vehicle_rentals", "aliases": ("car rental", "car hire", "rent a car", "motorbike rental", "motorcycle rental", "scooter rental", "bike rental", "atv rental", "quad rental"), "rationale": "Prompt signals vehicle-rental intent."},
    {"vertical": "tourism", "cluster_slug": "tour_operators", "aliases": ("travel agency", "tour operator", "tour agency", "excursion", "tour guide", "private tour"), "rationale": "Prompt signals travel-operator intent."},
    {"vertical": "tourism", "cluster_slug": "tourism_activities", "aliases": ("diving", "scuba", "snorkel", "sanctuary", "adventure", "zipline", "attraction"), "rationale": "Prompt signals tourism-activity intent."},
    {"vertical": "food", "cluster_slug": "food_service", "aliases": ("restaurant", "cafe", "coffee", "bakery", "catering"), "rationale": "Prompt signals food-service intent."},
    {"vertical": "fitness", "cluster_slug": "fitness_studios", "aliases": ("gym", "fitness", "yoga", "pilates", "crossfit"), "rationale": "Prompt signals fitness intent."},
    {"vertical": "health", "cluster_slug": "wellness_clinics", "aliases": ("clinic", "dentist", "physio", "rehab", "medical"), "rationale": "Prompt signals clinic intent."},
    {"vertical": "real_estate", "cluster_slug": "property_agencies", "aliases": ("real estate", "property", "broker", "estate agent"), "rationale": "Prompt signals property-agency intent."},
    {"vertical": "education", "cluster_slug": "training_centers", "aliases": ("school", "academy", "training", "course", "tuition"), "rationale": "Prompt signals training-center intent."},
    {"vertical": "legal", "cluster_slug": "law_firms", "aliases": ("law", "lawyer", "attorney", "legal"), "rationale": "Prompt signals legal-services intent."},
    {"vertical": "retail", "cluster_slug": "specialty_retail", "aliases": ("shop", "boutique", "retail", "store"), "rationale": "Prompt signals retail intent."},
    {"vertical": "home_services", "cluster_slug": "property_services", "aliases": ("cleaning", "repair", "maintenance", "electrician", "plumber"), "rationale": "Prompt signals home-services intent."},
)


CLUSTER_VARIANTS: dict[str, tuple[VariantTemplate, ...]] = {
    "beauty_services": (
        VariantTemplate("nail-salon", "Nail Salon", "beauty", "beauty_services", "nails", RecipeSourceStrategy.OVERPASS_DISCOVERY_ENRICH, ("nail", "manicure", "pedicure"), ({"shop": "beauty"}, {"beauty": "nails"}), (), ("nail salon", "manicure", "pedicure"), ("nails", "manicure", "booking"), ("en", "th"), ("Best when OSM has service-level tagging and the website confirms booking/contact signals.",), 95),
        VariantTemplate("hair-salon", "Hair Salon", "beauty", "beauty_services", "hair", RecipeSourceStrategy.OVERPASS_DISCOVERY_ENRICH, ("hair", "haircut", "hairstyle"), ({"shop": "hairdresser"}, {"beauty": "hair"}), (), ("hair salon", "haircut", "hairstylist"), ("hair", "stylist", "booking"), ("en", "th"), ("Strong local-service fit with good website conversion potential.",), 94),
        VariantTemplate("beauty-salon", "Beauty Salon", "beauty", "beauty_services", "beauty-salon", RecipeSourceStrategy.HYBRID_DISCOVERY, ("beauty", "beauty salon", "beautician"), ({"shop": "beauty"}, {"beauty": "beauty_salon"}), (), ("beauty salon", "beauty studio"), ("beauty", "salon", "appointment"), ("en", "th"), ("Broad cluster anchor that should stay visible for full-service beauty prompts.",), 92),
        VariantTemplate("barber-shop", "Barber Shop", "beauty", "beauty_services", "barber", RecipeSourceStrategy.OVERPASS_DISCOVERY_ENRICH, ("barber", "men haircut"), ({"shop": "hairdresser"}, {"beauty": "barber"}), (), ("barber shop", "barber"), ("barber", "haircut", "booking"), ("en", "th"), ("Useful male-grooming sub-intent when the prompt is broad.",), 88),
        VariantTemplate("spa-wellness", "Spa & Wellness", "beauty", "beauty_services", "spa", RecipeSourceStrategy.BROWSER_ASSISTED_DISCOVERY, ("spa", "massage", "wellness", "facial"), ({"leisure": "spa"}, {"beauty": "spa"}), (), ("spa", "wellness spa", "facial"), ("spa", "massage", "facial"), ("en", "th"), ("Often needs richer website inspection because spa offerings are marketed through service pages.",), 86),
        VariantTemplate("lash-brow-studio", "Lash & Brow Studio", "beauty", "beauty_services", "lash-brow", RecipeSourceStrategy.BROWSER_ASSISTED_DISCOVERY, ("lash", "eyelash", "brow", "eyebrow"), ({"shop": "beauty"}, {"beauty": "eyelashes"}), (), ("lash studio", "brow studio"), ("lash", "brow", "beauty"), ("en", "th"), ("High-value niche that often needs website confirmation beyond OSM names.",), 82),
    ),
    "beauty_clinics": (
        VariantTemplate("aesthetic-clinic", "Aesthetic Clinic", "beauty", "beauty_clinics", "aesthetic", RecipeSourceStrategy.HYBRID_DISCOVERY, ("aesthetic", "cosmetic"), ({"healthcare": "clinic"}, {"beauty": "cosmetic"}), (), ("aesthetic clinic", "cosmetic clinic"), ("aesthetic", "clinic", "consultation"), ("en", "th"), (), 94),
        VariantTemplate("skin-clinic", "Skin Clinic", "beauty", "beauty_clinics", "skin", RecipeSourceStrategy.HYBRID_DISCOVERY, ("skin clinic", "dermatology"), ({"healthcare": "clinic"}, {"healthcare:speciality": "dermatology"}), (), ("skin clinic", "dermatology clinic"), ("skin", "clinic", "treatment"), ("en", "th"), (), 90),
        VariantTemplate("laser-clinic", "Laser Clinic", "beauty", "beauty_clinics", "laser", RecipeSourceStrategy.BROWSER_ASSISTED_DISCOVERY, ("laser", "hair removal"), ({"healthcare": "clinic"}, {"beauty": "laser"}), (), ("laser clinic", "hair removal clinic"), ("laser", "clinic", "hair removal"), ("en", "th"), (), 86),
    ),
    "vehicle_rentals": (
        VariantTemplate("car-rental", "Car Rental", "vehicle", "vehicle_rentals", "car-rental", RecipeSourceStrategy.OVERPASS_DISCOVERY_ENRICH, ("car rental", "car hire", "rent a car"), ({"amenity": "car_rental"},), (), ("car rental", "car hire", "rent a car"), ("car rental", "fleet", "booking"), ("en", "th"), (), 96),
        VariantTemplate("motorbike-rental", "Motorbike Rental", "vehicle", "vehicle_rentals", "motorbike-rental", RecipeSourceStrategy.OVERPASS_DISCOVERY_ENRICH, ("motorbike rental", "motorcycle rental", "bike rental"), ({"amenity": "motorcycle_rental"}, {"shop": "motorcycle_rental"}), (), ("motorbike rental", "motorcycle rental"), ("motorbike", "motorcycle", "booking"), ("en", "th"), (), 94),
        VariantTemplate("scooter-rental", "Scooter Rental", "vehicle", "vehicle_rentals", "scooter-rental", RecipeSourceStrategy.OVERPASS_DISCOVERY_ENRICH, ("scooter", "moped"), ({"amenity": "motorcycle_rental"}, {"shop": "motorcycle_rental"}), (), ("scooter rental", "moped rental"), ("scooter", "rent", "booking"), ("en", "th"), (), 90),
        VariantTemplate("bike-rental", "Bike Rental", "vehicle", "vehicle_rentals", "bike-rental", RecipeSourceStrategy.OVERPASS_DISCOVERY_ENRICH, ("bicycle", "cycle"), ({"amenity": "bicycle_rental"},), (), ("bike rental", "bicycle rental"), ("bike", "bicycle", "rent"), ("en", "th"), (), 86),
        VariantTemplate("atv-rental", "ATV / Quad Rental", "vehicle", "vehicle_rentals", "quad-rental", RecipeSourceStrategy.HYBRID_DISCOVERY, ("atv", "quad"), ({"amenity": "motorcycle_rental"}, {"sport": "motor"}), (), ("atv rental", "quad rental"), ("atv", "quad", "booking"), ("en", "th"), ("Often needs website-level wording to separate ATV tours from simple rentals.",), 80),
    ),
    "tour_operators": (
        VariantTemplate("travel-agency", "Travel Agency", "tourism", "tour_operators", "travel-agency", RecipeSourceStrategy.OVERPASS_DISCOVERY_ENRICH, ("travel agency", "travel agent", "holiday package"), ({"shop": "travel_agency"}, {"office": "travel_agent"}), ({"tourism": "information"},), ("travel agency", "travel agent"), ("travel", "package", "booking"), ("en", "th"), (), 96),
        VariantTemplate("tour-operator", "Tour Operator", "tourism", "tour_operators", "tour-operator", RecipeSourceStrategy.HYBRID_DISCOVERY, ("tour operator", "excursion", "private tour"), ({"shop": "travel_agency"}, {"office": "travel_agent"}), ({"tourism": "information"},), ("tour operator", "excursion", "private tour"), ("tour", "excursion", "booking"), ("en", "th"), (), 92),
        VariantTemplate("tour-guide", "Tour Guide Service", "tourism", "tour_operators", "tour-guide", RecipeSourceStrategy.BROWSER_ASSISTED_DISCOVERY, ("tour guide", "guide service", "local guide"), ({"tourism": "information"}, {"office": "travel_agent"}), (), ("tour guide", "guide service"), ("guide", "tour", "contact"), ("en", "th"), (), 88),
    ),
    "tourism_activities": (
        VariantTemplate("diving-center", "Diving Center", "tourism", "tourism_activities", "diving", RecipeSourceStrategy.BROWSER_ASSISTED_DISCOVERY, ("diving", "scuba", "dive center"), ({"sport": "scuba_diving"}, {"shop": "dive"}), (), ("diving center", "scuba diving"), ("diving", "scuba", "booking"), ("en", "th"), (), 95),
        VariantTemplate("snorkeling-tour", "Snorkeling Tour", "tourism", "tourism_activities", "snorkeling", RecipeSourceStrategy.BROWSER_ASSISTED_DISCOVERY, ("snorkel", "island tour"), ({"tourism": "attraction"}, {"shop": "travel_agency"}), (), ("snorkeling tour", "island tour"), ("snorkel", "trip", "booking"), ("en", "th"), (), 86),
        VariantTemplate("animal-sanctuary", "Animal Sanctuary", "tourism", "tourism_activities", "sanctuary", RecipeSourceStrategy.BROWSER_ASSISTED_DISCOVERY, ("sanctuary", "elephant", "wildlife"), ({"tourism": "attraction"}, {"tourism": "zoo"}), (), ("elephant sanctuary", "animal sanctuary"), ("sanctuary", "visit", "booking"), ("en", "th"), (), 85),
        VariantTemplate("adventure-activity", "Adventure Activity", "tourism", "tourism_activities", "adventure", RecipeSourceStrategy.BROWSER_ASSISTED_DISCOVERY, ("zipline", "adventure", "rafting"), ({"tourism": "attraction"}, {"leisure": "sports_centre"}), (), ("zipline", "adventure activity"), ("adventure", "activity", "booking"), ("en", "th"), (), 80),
    ),
}


GENERIC_CLUSTER_VARIANTS: dict[str, tuple[VariantTemplate, ...]] = {
    "food_service": (
        VariantTemplate("restaurant", "Restaurant", "food", "food_service", "restaurant", RecipeSourceStrategy.WEBSITE_FIRST, ("restaurant", "dining"), ({"amenity": "restaurant"},), (), ("restaurant", "dining"), ("menu", "reservation", "contact"), ("en",), (), 90),
        VariantTemplate("cafe", "Cafe", "food", "food_service", "cafe", RecipeSourceStrategy.WEBSITE_FIRST, ("cafe", "coffee"), ({"amenity": "cafe"},), (), ("cafe", "coffee shop"), ("coffee", "menu", "contact"), ("en",), (), 84),
        VariantTemplate("bakery", "Bakery", "food", "food_service", "bakery", RecipeSourceStrategy.WEBSITE_FIRST, ("bakery", "pastry"), ({"shop": "bakery"},), (), ("bakery", "pastry shop"), ("bakery", "menu", "order"), ("en",), (), 82),
        VariantTemplate("catering", "Catering Service", "food", "food_service", "catering", RecipeSourceStrategy.WEBSITE_FIRST, ("catering", "event food"), ({"craft": "caterer"}, {"office": "company"}), (), ("catering service", "event catering"), ("catering", "menu", "contact"), ("en",), (), 80),
    ),
    "fitness_studios": (
        VariantTemplate("gym", "Gym", "fitness", "fitness_studios", "gym", RecipeSourceStrategy.HYBRID_DISCOVERY, ("gym", "fitness"), ({"leisure": "fitness_centre"},), (), ("gym", "fitness center"), ("membership", "trainer", "class"), ("en",), (), 90),
        VariantTemplate("yoga-studio", "Yoga Studio", "fitness", "fitness_studios", "yoga", RecipeSourceStrategy.WEBSITE_FIRST, ("yoga", "yoga studio"), ({"sport": "yoga"}, {"leisure": "sports_centre"}), (), ("yoga studio", "yoga class"), ("yoga", "schedule", "class"), ("en",), (), 84),
        VariantTemplate("pilates-studio", "Pilates Studio", "fitness", "fitness_studios", "pilates", RecipeSourceStrategy.WEBSITE_FIRST, ("pilates", "pilates studio"), ({"sport": "pilates"}, {"leisure": "sports_centre"}), (), ("pilates studio", "pilates class"), ("pilates", "class", "schedule"), ("en",), (), 82),
        VariantTemplate("personal-training", "Personal Training", "fitness", "fitness_studios", "personal-training", RecipeSourceStrategy.WEBSITE_FIRST, ("personal training", "trainer"), ({"leisure": "fitness_centre"}, {"office": "company"}), (), ("personal trainer", "fitness coach"), ("trainer", "consultation", "booking"), ("en",), (), 78),
    ),
    "wellness_clinics": (
        VariantTemplate("general-clinic", "General Clinic", "health", "wellness_clinics", "general-clinic", RecipeSourceStrategy.HYBRID_DISCOVERY, ("clinic", "medical clinic"), ({"healthcare": "clinic"},), (), ("medical clinic", "health clinic"), ("clinic", "appointment", "contact"), ("en",), (), 90),
        VariantTemplate("dental-clinic", "Dental Clinic", "health", "wellness_clinics", "dental", RecipeSourceStrategy.HYBRID_DISCOVERY, ("dentist", "dental"), ({"healthcare": "dentist"}, {"healthcare": "clinic"}), (), ("dental clinic", "dentist"), ("dental", "appointment", "contact"), ("en",), (), 84),
        VariantTemplate("physio-clinic", "Physiotherapy Clinic", "health", "wellness_clinics", "physio", RecipeSourceStrategy.HYBRID_DISCOVERY, ("physio", "physiotherapy", "rehab"), ({"healthcare": "physiotherapist"}, {"healthcare": "clinic"}), (), ("physiotherapy clinic", "rehab clinic"), ("physio", "rehab", "appointment"), ("en",), (), 82),
    ),
    "property_agencies": (
        VariantTemplate("real-estate-agency", "Real Estate Agency", "real_estate", "property_agencies", "real-estate", RecipeSourceStrategy.WEBSITE_FIRST, ("real estate", "property agency"), ({"office": "estate_agent"},), (), ("real estate agency", "estate agent"), ("property", "listing", "contact"), ("en",), (), 90),
        VariantTemplate("property-management", "Property Management", "real_estate", "property_agencies", "property-management", RecipeSourceStrategy.WEBSITE_FIRST, ("property management", "rental management"), ({"office": "estate_agent"}, {"office": "company"}), (), ("property management", "rental management"), ("property", "management", "contact"), ("en",), (), 82),
    ),
    "training_centers": (
        VariantTemplate("training-center", "Training Center", "education", "training_centers", "training-center", RecipeSourceStrategy.WEBSITE_FIRST, ("training", "academy", "courses"), ({"office": "educational_institution"}, {"amenity": "school"}), (), ("training center", "academy"), ("course", "enroll", "contact"), ("en",), (), 88),
        VariantTemplate("language-school", "Language School", "education", "training_centers", "language-school", RecipeSourceStrategy.WEBSITE_FIRST, ("language school", "english school"), ({"amenity": "school"}, {"office": "educational_institution"}), (), ("language school", "english school"), ("course", "language", "contact"), ("en",), (), 83),
    ),
    "law_firms": (
        VariantTemplate("law-firm", "Law Firm", "legal", "law_firms", "law-firm", RecipeSourceStrategy.WEBSITE_FIRST, ("law firm", "lawyer", "attorney"), ({"office": "lawyer"},), (), ("law firm", "lawyer"), ("legal", "consultation", "contact"), ("en",), (), 88),
        VariantTemplate("immigration-law", "Immigration Law Firm", "legal", "law_firms", "immigration-law", RecipeSourceStrategy.WEBSITE_FIRST, ("immigration", "visa lawyer"), ({"office": "lawyer"},), (), ("immigration lawyer", "visa lawyer"), ("visa", "legal", "consultation"), ("en",), (), 82),
    ),
    "specialty_retail": (
        VariantTemplate("specialty-shop", "Specialty Shop", "retail", "specialty_retail", "specialty-shop", RecipeSourceStrategy.WEBSITE_FIRST, ("boutique", "specialty shop", "retail store"), ({"shop": "yes"},), (), ("boutique", "specialty shop"), ("shop", "catalog", "contact"), ("en",), ("Broad retail pattern; production validation should carry more weight than template score here.",), 82),
        VariantTemplate("fashion-boutique", "Fashion Boutique", "retail", "specialty_retail", "fashion", RecipeSourceStrategy.WEBSITE_FIRST, ("fashion", "boutique"), ({"shop": "clothes"}, {"shop": "boutique"}), (), ("fashion boutique", "boutique"), ("catalog", "collection", "contact"), ("en",), (), 80),
        VariantTemplate("gift-shop", "Gift Shop", "retail", "specialty_retail", "gift-shop", RecipeSourceStrategy.WEBSITE_FIRST, ("gift", "souvenir"), ({"shop": "gift"}, {"shop": "souvenir"}), (), ("gift shop", "souvenir shop"), ("gift", "catalog", "contact"), ("en",), (), 76),
    ),
    "property_services": (
        VariantTemplate("cleaning-service", "Cleaning Service", "home_services", "property_services", "cleaning", RecipeSourceStrategy.WEBSITE_FIRST, ("cleaning", "housekeeping"), ({"office": "company"}, {"craft": "cleaning"}), (), ("cleaning service", "housekeeping"), ("cleaning", "service", "quote"), ("en",), (), 86),
        VariantTemplate("repair-service", "Repair Service", "home_services", "property_services", "repair", RecipeSourceStrategy.WEBSITE_FIRST, ("repair", "maintenance"), ({"office": "company"}, {"craft": "electrician"}), (), ("repair service", "maintenance company"), ("repair", "maintenance", "contact"), ("en",), (), 82),
        VariantTemplate("electrician-service", "Electrician Service", "home_services", "property_services", "electrician", RecipeSourceStrategy.WEBSITE_FIRST, ("electrician", "electrical"), ({"craft": "electrician"}, {"office": "company"}), (), ("electrician service", "electrical contractor"), ("electrician", "quote", "contact"), ("en",), (), 80),
        VariantTemplate("plumber-service", "Plumbing Service", "home_services", "property_services", "plumber", RecipeSourceStrategy.WEBSITE_FIRST, ("plumber", "plumbing"), ({"craft": "plumber"}, {"office": "company"}), (), ("plumbing service", "plumber"), ("plumbing", "quote", "contact"), ("en",), (), 80),
    ),
}


def all_curated_variant_templates() -> tuple[VariantTemplate, ...]:
    merged: list[VariantTemplate] = []
    for template_group in CLUSTER_VARIANTS.values():
        merged.extend(template_group)
    for template_group in GENERIC_CLUSTER_VARIANTS.values():
        merged.extend(template_group)
    return tuple(merged)


def _slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return slug[:96] or "draft-recipe"


def _titleize(text: str) -> str:
    words = [chunk for chunk in re.split(r"[^A-Za-z0-9]+", text) if chunk]
    return " ".join(word.capitalize() for word in words[:8]) or "Draft Recipe"


def _extract_location_hint(normalized: str) -> str | None:
    match = re.search(r"\bin ([a-z][a-z\s-]+)$", normalized)
    if match:
        return match.group(1).strip()
    for place in ("thailand", "phuket", "chiang mai", "pattaya", "samui", "bangkok", "bali", "berlin", "paris"):
        if place in normalized:
            return place
    return None


def _normalized_alias(alias: str) -> str:
    return normalize_prompt_text(alias)


def _alias_matches(alias: str, normalized_text: str, normalized_tokens: set[str]) -> bool:
    normalized_alias = _normalized_alias(alias)
    if not normalized_alias:
        return False
    if normalized_alias in normalized_text:
        return True
    alias_tokens = set(normalized_alias.split())
    return bool(alias_tokens) and alias_tokens.issubset(normalized_tokens)


def _language_hints_for_prompt(normalized: str, base_hints: tuple[str, ...] = ()) -> list[str]:
    if base_hints:
        return list(dict.fromkeys(base_hints))
    if any(country in normalized for country in ("thailand", "thai", "phuket", "chiang mai", "pattaya", "samui", "bangkok")):
        return ["en", "th"]
    if any(country in normalized for country in ("germany", "berlin", "munich")):
        return ["en", "de"]
    if any(country in normalized for country in ("france", "paris")):
        return ["en", "fr"]
    return ["en"]


def _rank_clusters(normalized: str) -> list[ClusterCandidate]:
    scored: list[ClusterCandidate] = []
    normalized_tokens = set(normalized.split())
    for hint in PROMPT_CLUSTER_HINTS:
        matched_aliases = tuple(
            phrase
            for phrase in hint["aliases"]
            if _alias_matches(str(phrase), normalized, normalized_tokens)
        )
        if matched_aliases:
            score = (len(matched_aliases) * 100) + max(len(alias) for alias in matched_aliases)
            scored.append(
                ClusterCandidate(
                    vertical=str(hint["vertical"]),
                    cluster_slug=str(hint["cluster_slug"]),
                    score=score,
                    matched_aliases=matched_aliases,
                    rationale=[
                        str(hint["rationale"]),
                        f"Matched aliases: {', '.join(matched_aliases[:4])}.",
                    ],
                )
            )
    if scored:
        return sorted(scored, key=lambda item: (-item.score, item.cluster_slug))
    return [
        ClusterCandidate(
            vertical="tourism",
            cluster_slug="tour_operators",
            score=0,
            matched_aliases=(),
            rationale=[
                "No strong cluster match was found, so the draft falls back to a broad tourism baseline.",
                "Review the generated variants before validation.",
            ],
        )
    ]


def analyze_prompt_clusters(prompt: str) -> tuple[ClusterCandidate, list[ClusterCandidate]]:
    normalized = " ".join(prompt.strip().lower().split())
    if not normalized:
        raise ValueError("Prompt cannot be empty.")
    ranked = _rank_clusters(normalized)
    return ranked[0], ranked[1:]


def _variant_from_row(row: QueryRecipeVariantTemplate) -> VariantTemplate:
    return VariantTemplate(
        key=row.key,
        label=row.label,
        vertical=row.vertical,
        cluster_slug=row.cluster_slug or "",
        sub_intent=row.sub_intent,
        source_strategy=row.source_strategy,
        aliases=tuple(row.aliases or []),
        osm_tags=tuple(dict(item) for item in (row.osm_tags or [])),
        exclude_tags=tuple(dict(item) for item in (row.exclude_tags or [])),
        search_terms=tuple(row.search_terms or []),
        website_keywords=tuple(row.website_keywords or []),
        language_hints=tuple(row.language_hints or []),
        rationale=tuple(row.rationale or []),
        priority=row.template_score,
    )


def _templates_for_cluster(session: Session | None, cluster_slug: str) -> list[VariantTemplate]:
    if session is not None:
        rows = session.scalars(
            select(QueryRecipeVariantTemplate)
            .where(
                QueryRecipeVariantTemplate.is_active.is_(True),
                QueryRecipeVariantTemplate.cluster_slug == cluster_slug,
            )
            .order_by(QueryRecipeVariantTemplate.sort_order, QueryRecipeVariantTemplate.label)
        ).all()
        if rows:
            return [_variant_from_row(row) for row in rows]
    return list(CLUSTER_VARIANTS.get(cluster_slug, GENERIC_CLUSTER_VARIANTS.get(cluster_slug, ())))


def _strategy_bonus(strategy: RecipeSourceStrategy, normalized: str) -> tuple[int, str | None]:
    if strategy == RecipeSourceStrategy.BROWSER_ASSISTED_DISCOVERY:
        if any(token in normalized for token in ("experience", "luxury", "sanctuary", "wellness", "activity")):
            return 4, "Prompt hints that richer browser-assisted discovery may pay off."
        return 1, None
    if strategy == RecipeSourceStrategy.WEBSITE_FIRST:
        if any(token in normalized for token in ("service", "agency", "firm", "clinic", "studio")):
            return 3, "Prompt leans toward businesses that are better differentiated on their own websites."
        return 1, None
    if strategy == RecipeSourceStrategy.HYBRID_DISCOVERY:
        return 2, "Hybrid discovery keeps both map signals and website verification in play."
    return 0, None


def _variant_score(template: VariantTemplate, normalized: str, location_hint: str | None) -> tuple[int, int, int, list[str]]:
    reasons: list[str] = []
    normalized_tokens = set(normalized.split())
    alias_hits = [alias for alias in template.aliases if _alias_matches(alias, normalized, normalized_tokens)]
    template_score = template.priority
    prompt_match_score = 10 * len(alias_hits)
    if _alias_matches(template.sub_intent.replace("-", " "), normalized, normalized_tokens):
        prompt_match_score += 6
        reasons.append(f"Prompt directly references the '{template.sub_intent}' sub-intent.")
    if alias_hits:
        reasons.append(f"Matches intent terms: {', '.join(alias_hits[:3])}.")
    else:
        reasons.append("Added as a closely related variant inside the selected cluster.")
    if location_hint:
        prompt_match_score += 5
        reasons.append(f"Localized search terms for {location_hint}.")
    strategy_bonus, strategy_reason = _strategy_bonus(template.source_strategy, normalized)
    prompt_match_score += strategy_bonus
    if strategy_reason:
        reasons.append(strategy_reason)
    fit_score = template_score + prompt_match_score
    return template_score, prompt_match_score, fit_score, reasons


def _proposal_from_template(
    prompt: str,
    normalized: str,
    template: VariantTemplate,
    location_hint: str | None,
    cluster_rationale: list[str],
) -> DraftProposal:
    search_terms = list(template.search_terms)
    if location_hint:
        search_terms = [f"{term} {location_hint}" for term in template.search_terms[:3]] + search_terms
    search_terms = list(dict.fromkeys(search_terms))[:8]
    website_keywords = list(dict.fromkeys(list(template.website_keywords) + search_terms))[:10]
    template_score, prompt_match_score, fit_score, fit_reasons = _variant_score(template, normalized, location_hint)
    slug_source = f"{template.key} {location_hint or ''}".strip()
    return DraftProposal(
        prompt=prompt.strip(),
        slug=_slugify(slug_source),
        label=template.label,
        description=f"Draft generated from prompt: {prompt.strip()} ({template.label}).",
        vertical=template.vertical,
        cluster_slug=template.cluster_slug or None,
        adapter=RecipeAdapter.OVERPASS_PUBLIC,
        source_strategy=template.source_strategy,
        template_key=template.key,
        sub_intent=template.sub_intent,
        osm_tags=[dict(item) for item in template.osm_tags],
        exclude_tags=[dict(item) for item in template.exclude_tags],
        search_terms=search_terms,
        website_keywords=website_keywords,
        language_hints=_language_hints_for_prompt(normalized, template.language_hints),
        rationale=list(cluster_rationale)
        + [
            f"Generated '{template.label}' as a '{template.sub_intent}' candidate inside the {template.cluster_slug} cluster.",
            f"Planned source strategy: {template.source_strategy.value}.",
            *list(template.rationale),
            "Use multiple high-scoring variants when the niche spans several sub-services.",
        ],
        variant_key=template.key,
        template_score=template_score,
        prompt_match_score=prompt_match_score,
        fit_score=fit_score,
        fit_reasons=fit_reasons,
    )


def build_draft_variants_from_prompt(prompt: str, session: Session | None = None) -> list[DraftProposal]:
    normalized = normalize_prompt_text(prompt)
    if not normalized:
        raise ValueError("Prompt cannot be empty.")
    chosen = _rank_clusters(normalized)[0]
    templates = _templates_for_cluster(session, chosen.cluster_slug)
    if not templates:
        return [
            DraftProposal(
                prompt=prompt.strip(),
                slug=_slugify(normalized),
                label=_titleize(normalized),
                description=f"Draft generated from prompt: {prompt.strip()}",
                vertical=chosen.vertical,
                cluster_slug=chosen.cluster_slug,
                adapter=RecipeAdapter.OVERPASS_PUBLIC,
                source_strategy=RecipeSourceStrategy.HYBRID_DISCOVERY,
                template_key=_slugify(normalized),
                sub_intent="broad-intent",
                osm_tags=[{"tourism": "information"}],
                exclude_tags=[],
                search_terms=[normalized],
                website_keywords=["contact", "about", "booking"],
                language_hints=_language_hints_for_prompt(normalized),
                rationale=chosen.rationale,
                variant_key=_slugify(normalized),
                template_score=50,
                prompt_match_score=0,
                fit_score=50,
                fit_reasons=["Fallback draft generated because no curated variants exist for this cluster yet."],
            )
        ]
    location_hint = _extract_location_hint(normalized)
    proposals = [
        _proposal_from_template(prompt, normalized, template, location_hint, chosen.rationale)
        for template in templates
    ]
    proposals.sort(key=lambda item: (-item.fit_score, item.sub_intent, item.label))
    return proposals


def build_draft_from_prompt(prompt: str, session: Session | None = None) -> DraftProposal:
    return build_draft_variants_from_prompt(prompt, session=session)[0]


def select_draft_variant(
    prompt: str,
    selected_variant_key: str | None = None,
    session: Session | None = None,
) -> tuple[list[DraftProposal], DraftProposal]:
    proposals = build_draft_variants_from_prompt(prompt, session=session)
    if selected_variant_key:
        for proposal in proposals:
            if proposal.variant_key == selected_variant_key:
                return proposals, proposal
    return proposals, proposals[0]
