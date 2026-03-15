from __future__ import annotations

from dataclasses import dataclass
import re

from app.models import RecipeAdapter


@dataclass(frozen=True)
class DraftProposal:
    prompt: str
    slug: str
    label: str
    description: str
    vertical: str
    cluster_slug: str | None
    adapter: RecipeAdapter
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


@dataclass(frozen=True)
class VariantTemplate:
    key: str
    label: str
    vertical: str
    cluster_slug: str
    aliases: tuple[str, ...]
    osm_tags: tuple[dict[str, str], ...]
    exclude_tags: tuple[dict[str, str], ...]
    search_terms: tuple[str, ...]
    website_keywords: tuple[str, ...]
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
        VariantTemplate("nail-salon", "Nail Salon", "beauty", "beauty_services", ("nail", "manicure", "pedicure"), ({"shop": "beauty"}, {"beauty": "nails"}), (), ("nail salon", "manicure", "pedicure"), ("nails", "manicure", "booking"), 95),
        VariantTemplate("hair-salon", "Hair Salon", "beauty", "beauty_services", ("hair", "haircut", "hairstyle"), ({"shop": "hairdresser"}, {"beauty": "hair"}), (), ("hair salon", "haircut", "hairstylist"), ("hair", "stylist", "booking"), 94),
        VariantTemplate("beauty-salon", "Beauty Salon", "beauty", "beauty_services", ("beauty", "beauty salon", "beautician"), ({"shop": "beauty"}, {"beauty": "beauty_salon"}), (), ("beauty salon", "beauty studio"), ("beauty", "salon", "appointment"), 92),
        VariantTemplate("barber-shop", "Barber Shop", "beauty", "beauty_services", ("barber", "men haircut"), ({"shop": "hairdresser"}, {"beauty": "barber"}), (), ("barber shop", "barber"), ("barber", "haircut", "booking"), 88),
        VariantTemplate("spa-wellness", "Spa & Wellness", "beauty", "beauty_services", ("spa", "massage", "wellness", "facial"), ({"leisure": "spa"}, {"beauty": "spa"}), (), ("spa", "wellness spa", "facial"), ("spa", "massage", "facial"), 86),
        VariantTemplate("lash-brow-studio", "Lash & Brow Studio", "beauty", "beauty_services", ("lash", "eyelash", "brow", "eyebrow"), ({"shop": "beauty"}, {"beauty": "eyelashes"}), (), ("lash studio", "brow studio"), ("lash", "brow", "beauty"), 82),
    ),
    "beauty_clinics": (
        VariantTemplate("aesthetic-clinic", "Aesthetic Clinic", "beauty", "beauty_clinics", ("aesthetic", "cosmetic"), ({"healthcare": "clinic"}, {"beauty": "cosmetic"}), (), ("aesthetic clinic", "cosmetic clinic"), ("aesthetic", "clinic", "consultation"), 94),
        VariantTemplate("skin-clinic", "Skin Clinic", "beauty", "beauty_clinics", ("skin clinic", "dermatology"), ({"healthcare": "clinic"}, {"healthcare:speciality": "dermatology"}), (), ("skin clinic", "dermatology clinic"), ("skin", "clinic", "treatment"), 90),
        VariantTemplate("laser-clinic", "Laser Clinic", "beauty", "beauty_clinics", ("laser", "hair removal"), ({"healthcare": "clinic"}, {"beauty": "laser"}), (), ("laser clinic", "hair removal clinic"), ("laser", "clinic", "hair removal"), 86),
    ),
    "vehicle_rentals": (
        VariantTemplate("car-rental", "Car Rental", "vehicle", "vehicle_rentals", ("car rental", "car hire", "rent a car"), ({"amenity": "car_rental"},), (), ("car rental", "car hire", "rent a car"), ("car rental", "fleet", "booking"), 96),
        VariantTemplate("motorbike-rental", "Motorbike Rental", "vehicle", "vehicle_rentals", ("motorbike rental", "motorcycle rental", "bike rental"), ({"amenity": "motorcycle_rental"}, {"shop": "motorcycle_rental"}), (), ("motorbike rental", "motorcycle rental"), ("motorbike", "motorcycle", "booking"), 94),
        VariantTemplate("scooter-rental", "Scooter Rental", "vehicle", "vehicle_rentals", ("scooter", "moped"), ({"amenity": "motorcycle_rental"}, {"shop": "motorcycle_rental"}), (), ("scooter rental", "moped rental"), ("scooter", "rent", "booking"), 90),
        VariantTemplate("bike-rental", "Bike Rental", "vehicle", "vehicle_rentals", ("bicycle", "cycle"), ({"amenity": "bicycle_rental"},), (), ("bike rental", "bicycle rental"), ("bike", "bicycle", "rent"), 86),
        VariantTemplate("atv-rental", "ATV / Quad Rental", "vehicle", "vehicle_rentals", ("atv", "quad"), ({"amenity": "motorcycle_rental"}, {"sport": "motor"}), (), ("atv rental", "quad rental"), ("atv", "quad", "booking"), 80),
    ),
    "tour_operators": (
        VariantTemplate("travel-agency", "Travel Agency", "tourism", "tour_operators", ("travel agency", "travel agent", "holiday package"), ({"shop": "travel_agency"}, {"office": "travel_agent"}), ({"tourism": "information"},), ("travel agency", "travel agent"), ("travel", "package", "booking"), 96),
        VariantTemplate("tour-operator", "Tour Operator", "tourism", "tour_operators", ("tour operator", "excursion", "private tour"), ({"shop": "travel_agency"}, {"office": "travel_agent"}), ({"tourism": "information"},), ("tour operator", "excursion", "private tour"), ("tour", "excursion", "booking"), 92),
        VariantTemplate("tour-guide", "Tour Guide Service", "tourism", "tour_operators", ("tour guide", "guide service", "local guide"), ({"tourism": "information"}, {"office": "travel_agent"}), (), ("tour guide", "guide service"), ("guide", "tour", "contact"), 88),
    ),
    "tourism_activities": (
        VariantTemplate("diving-center", "Diving Center", "tourism", "tourism_activities", ("diving", "scuba", "dive center"), ({"sport": "scuba_diving"}, {"shop": "dive"}), (), ("diving center", "scuba diving"), ("diving", "scuba", "booking"), 95),
        VariantTemplate("snorkeling-tour", "Snorkeling Tour", "tourism", "tourism_activities", ("snorkel", "island tour"), ({"tourism": "attraction"}, {"shop": "travel_agency"}), (), ("snorkeling tour", "island tour"), ("snorkel", "trip", "booking"), 86),
        VariantTemplate("animal-sanctuary", "Animal Sanctuary", "tourism", "tourism_activities", ("sanctuary", "elephant", "wildlife"), ({"tourism": "attraction"}, {"tourism": "zoo"}), (), ("elephant sanctuary", "animal sanctuary"), ("sanctuary", "visit", "booking"), 85),
        VariantTemplate("adventure-activity", "Adventure Activity", "tourism", "tourism_activities", ("zipline", "adventure", "rafting"), ({"tourism": "attraction"}, {"leisure": "sports_centre"}), (), ("zipline", "adventure activity"), ("adventure", "activity", "booking"), 80),
    ),
}


GENERIC_CLUSTER_VARIANTS: dict[str, tuple[VariantTemplate, ...]] = {
    "food_service": (VariantTemplate("restaurant", "Restaurant", "food", "food_service", ("restaurant", "dining"), ({"amenity": "restaurant"},), (), ("restaurant", "dining"), ("menu", "reservation", "contact"), 90), VariantTemplate("cafe", "Cafe", "food", "food_service", ("cafe", "coffee"), ({"amenity": "cafe"},), (), ("cafe", "coffee shop"), ("coffee", "menu", "contact"), 84)),
    "fitness_studios": (VariantTemplate("gym", "Gym", "fitness", "fitness_studios", ("gym", "fitness"), ({"leisure": "fitness_centre"},), (), ("gym", "fitness center"), ("membership", "trainer", "class"), 90), VariantTemplate("yoga-studio", "Yoga Studio", "fitness", "fitness_studios", ("yoga", "yoga studio"), ({"sport": "yoga"}, {"leisure": "sports_centre"}), (), ("yoga studio", "yoga class"), ("yoga", "schedule", "class"), 84)),
    "wellness_clinics": (VariantTemplate("general-clinic", "General Clinic", "health", "wellness_clinics", ("clinic", "medical clinic"), ({"healthcare": "clinic"},), (), ("medical clinic", "health clinic"), ("clinic", "appointment", "contact"), 90), VariantTemplate("dental-clinic", "Dental Clinic", "health", "wellness_clinics", ("dentist", "dental"), ({"healthcare": "dentist"}, {"healthcare": "clinic"}), (), ("dental clinic", "dentist"), ("dental", "appointment", "contact"), 84)),
    "property_agencies": (VariantTemplate("real-estate-agency", "Real Estate Agency", "real_estate", "property_agencies", ("real estate", "property agency"), ({"office": "estate_agent"},), (), ("real estate agency", "estate agent"), ("property", "listing", "contact"), 90),),
    "training_centers": (VariantTemplate("training-center", "Training Center", "education", "training_centers", ("training", "academy", "courses"), ({"office": "educational_institution"}, {"amenity": "school"}), (), ("training center", "academy"), ("course", "enroll", "contact"), 88),),
    "law_firms": (VariantTemplate("law-firm", "Law Firm", "legal", "law_firms", ("law firm", "lawyer", "attorney"), ({"office": "lawyer"},), (), ("law firm", "lawyer"), ("legal", "consultation", "contact"), 88),),
    "specialty_retail": (VariantTemplate("specialty-shop", "Specialty Shop", "retail", "specialty_retail", ("boutique", "specialty shop", "retail store"), ({"shop": "yes"},), (), ("boutique", "specialty shop"), ("shop", "catalog", "contact"), 82),),
    "property_services": (VariantTemplate("cleaning-service", "Cleaning Service", "home_services", "property_services", ("cleaning", "housekeeping"), ({"office": "company"}, {"craft": "cleaning"}), (), ("cleaning service", "housekeeping"), ("cleaning", "service", "quote"), 86), VariantTemplate("repair-service", "Repair Service", "home_services", "property_services", ("repair", "maintenance"), ({"office": "company"}, {"craft": "electrician"}), (), ("repair service", "maintenance company"), ("repair", "maintenance", "contact"), 82)),
}


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


def _language_hints_for_prompt(normalized: str) -> list[str]:
    if any(country in normalized for country in ("thailand", "thai", "phuket", "chiang mai", "pattaya", "samui", "bangkok")):
        return ["en", "th"]
    if any(country in normalized for country in ("germany", "berlin", "munich")):
        return ["en", "de"]
    if any(country in normalized for country in ("france", "paris")):
        return ["en", "fr"]
    return ["en"]


def _match_cluster(normalized: str) -> tuple[str, str, list[str]]:
    scored: list[tuple[int, dict[str, object]]] = []
    for hint in PROMPT_CLUSTER_HINTS:
        hits = sum(1 for phrase in hint["aliases"] if phrase in normalized)
        if hits:
            scored.append((hits, hint))
    if scored:
        best = max(scored, key=lambda item: item[0])[1]
        return best["vertical"], best["cluster_slug"], [best["rationale"]]
    return "tourism", "tour_operators", [
        "No strong cluster match was found, so the draft falls back to a broad tourism baseline.",
        "Review the generated variants before validation.",
    ]


def _variant_score(template: VariantTemplate, normalized: str, location_hint: str | None) -> tuple[int, int, int, list[str]]:
    reasons: list[str] = []
    alias_hits = [alias for alias in template.aliases if alias in normalized]
    template_score = template.priority
    prompt_match_score = 10 * len(alias_hits)
    if alias_hits:
        reasons.append(f"Matches intent terms: {', '.join(alias_hits[:3])}.")
    else:
        reasons.append("Added as a closely related variant within the selected cluster.")
    if location_hint:
        prompt_match_score += 5
        reasons.append(f"Localized search terms for {location_hint}.")
    fit_score = template_score + prompt_match_score
    return template_score, prompt_match_score, fit_score, reasons


def _proposal_from_template(prompt: str, normalized: str, template: VariantTemplate, location_hint: str | None, cluster_rationale: list[str]) -> DraftProposal:
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
        cluster_slug=template.cluster_slug,
        adapter=RecipeAdapter.OVERPASS_PUBLIC,
        osm_tags=[dict(item) for item in template.osm_tags],
        exclude_tags=[dict(item) for item in template.exclude_tags],
        search_terms=search_terms,
        website_keywords=website_keywords,
        language_hints=_language_hints_for_prompt(normalized),
        rationale=list(cluster_rationale) + [
            f"Generated '{template.label}' as a candidate variant inside the {template.cluster_slug} cluster.",
            "Use multiple high-scoring variants when the niche spans several sub-services.",
        ],
        variant_key=template.key,
        template_score=template_score,
        prompt_match_score=prompt_match_score,
        fit_score=fit_score,
        fit_reasons=fit_reasons,
    )


def build_draft_variants_from_prompt(prompt: str) -> list[DraftProposal]:
    normalized = " ".join(prompt.strip().lower().split())
    if not normalized:
        raise ValueError("Prompt cannot be empty.")
    vertical, cluster_slug, cluster_rationale = _match_cluster(normalized)
    templates = list(CLUSTER_VARIANTS.get(cluster_slug, GENERIC_CLUSTER_VARIANTS.get(cluster_slug, ())))
    if not templates:
        return [
            DraftProposal(
                prompt=prompt.strip(),
                slug=_slugify(normalized),
                label=_titleize(normalized),
                description=f"Draft generated from prompt: {prompt.strip()}",
                vertical=vertical,
                cluster_slug=cluster_slug,
                adapter=RecipeAdapter.OVERPASS_PUBLIC,
                osm_tags=[{"tourism": "information"}],
                exclude_tags=[],
                search_terms=[normalized],
                website_keywords=["contact", "about", "booking"],
                language_hints=_language_hints_for_prompt(normalized),
                rationale=cluster_rationale,
                variant_key=_slugify(normalized),
                template_score=50,
                prompt_match_score=0,
                fit_score=50,
                fit_reasons=["Fallback draft generated because no curated variants exist for this cluster yet."],
            )
        ]
    location_hint = _extract_location_hint(normalized)
    proposals = [_proposal_from_template(prompt, normalized, template, location_hint, cluster_rationale) for template in templates]
    proposals.sort(key=lambda item: (-item.fit_score, item.label))
    return proposals


def build_draft_from_prompt(prompt: str) -> DraftProposal:
    return build_draft_variants_from_prompt(prompt)[0]


def select_draft_variant(prompt: str, selected_variant_key: str | None = None) -> tuple[list[DraftProposal], DraftProposal]:
    proposals = build_draft_variants_from_prompt(prompt)
    if selected_variant_key:
        for proposal in proposals:
            if proposal.variant_key == selected_variant_key:
                return proposals, proposal
    return proposals, proposals[0]
