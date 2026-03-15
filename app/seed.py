from sqlalchemy.exc import IntegrityError

from app.models import Category, QueryRecipe, QueryRecipeVariantTemplate, QueryRecipeVersion, RecipeAdapter, RecipeSourceStrategy, RecipeStatus, Region
from app.services.recipe_drafts import all_curated_variant_templates
from app.services.taxonomy import seed_taxonomy


DEFAULT_REGIONS = [
    {
        "code": "TH",
        "name": "Thailand",
        "country_code": "TH",
        "osm_admin_level": 2,
    },
]


DEFAULT_CATEGORIES = [
    {
        "slug": "car-rental-agency",
        "label": "Car Rental Agency",
        "vertical": "vehicle",
        "cluster_slug": "vehicle_rentals",
        "osm_tags": [{"amenity": "car_rental"}],
        "search_terms": ["car rental agency", "rent a car"],
    },
    {
        "slug": "motorcycle-rental-agency",
        "label": "Motorcycle Rental Agency",
        "vertical": "vehicle",
        "cluster_slug": "vehicle_rentals",
        "osm_tags": [{"shop": "motorcycle_rental"}],
        "search_terms": ["motorcycle rental agency", "motorbike rental"],
    },
    {
        "slug": "scooter-rental-service",
        "label": "Scooter Rental Service",
        "vertical": "vehicle",
        "cluster_slug": "vehicle_rentals",
        "osm_tags": [{"shop": "motorcycle_rental"}],
        "search_terms": ["scooter rental service"],
    },
    {
        "slug": "bike-rental",
        "label": "Bike Rental",
        "vertical": "vehicle",
        "cluster_slug": "vehicle_rentals",
        "osm_tags": [{"amenity": "bicycle_rental"}],
        "search_terms": ["bike rental"],
    },
    {
        "slug": "quad-rental",
        "label": "Quad Rental",
        "vertical": "vehicle",
        "cluster_slug": "vehicle_rentals",
        "osm_tags": [{"shop": "motorcycle_rental"}],
        "search_terms": ["quad rental", "ATV rental"],
    },
    {
        "slug": "tour-agency",
        "label": "Tour Agency",
        "vertical": "tourism",
        "cluster_slug": "tour_operators",
        "osm_tags": [{"shop": "travel_agency"}],
        "search_terms": ["tour agency", "tour operator"],
    },
    {
        "slug": "travel-agency",
        "label": "Travel Agency",
        "vertical": "tourism",
        "cluster_slug": "tour_operators",
        "osm_tags": [{"shop": "travel_agency"}],
        "search_terms": ["travel agency"],
    },
    {
        "slug": "tour-guide-service",
        "label": "Tour Guide Service",
        "vertical": "tourism",
        "cluster_slug": "tour_operators",
        "osm_tags": [{"tourism": "information"}],
        "search_terms": ["tour guide service", "excursions agency"],
    },
]


def _latest_recipe_version(recipe: QueryRecipe) -> QueryRecipeVersion | None:
    versions = sorted(recipe.versions, key=lambda item: item.version_number, reverse=True)
    return versions[0] if versions else None


def seed_defaults(session) -> None:
    seed_taxonomy(session)
    session.commit()
    _seed_recipe_variant_templates(session)
    session.commit()

    for region_data in DEFAULT_REGIONS:
        region = session.query(Region).filter(Region.code == region_data["code"]).one_or_none()
        if region is None:
            try:
                session.add(Region(**region_data))
                session.commit()
            except IntegrityError:
                session.rollback()

    for category_data in DEFAULT_CATEGORIES:
        category = session.query(Category).filter(Category.slug == category_data["slug"]).one_or_none()
        if category is None:
            try:
                session.add(Category(**category_data))
                session.commit()
            except IntegrityError:
                session.rollback()

    seeded_categories = session.query(Category).all()
    for category in seeded_categories:
        recipe = session.query(QueryRecipe).filter(QueryRecipe.slug == category.slug).one_or_none()
        if recipe is None:
            try:
                recipe = QueryRecipe(
                    slug=category.slug,
                    label=category.label,
                    description=f"Seeded platform recipe for {category.label}.",
                    vertical=category.vertical,
                    cluster_slug=category.cluster_slug,
                    status=RecipeStatus.ACTIVE,
                    is_platform_template=True,
                )
                session.add(recipe)
                session.flush()
                session.add(
                    QueryRecipeVersion(
                        recipe_id=recipe.id,
                        version_number=1,
                        status=RecipeStatus.ACTIVE,
                        adapter=RecipeAdapter.OVERPASS_LOCAL,
                        source_strategy=RecipeSourceStrategy.OVERPASS_DISCOVERY_ENRICH,
                        osm_tags=category.osm_tags,
                        search_terms=category.search_terms,
                        website_keywords=category.search_terms,
                        language_hints=[],
                        notes="Seeded from the built-in category catalog.",
                    )
                )
                category.seeded_recipe_id = recipe.id
                session.add(category)
                session.commit()
            except IntegrityError:
                session.rollback()
        elif category.seeded_recipe_id != recipe.id:
            category.seeded_recipe_id = recipe.id
            category.cluster_slug = recipe.cluster_slug
            session.add(category)
            session.commit()

    recipes = session.query(QueryRecipe).all()
    for recipe in recipes:
        if recipe.status != RecipeStatus.ACTIVE:
            continue
        version = _latest_recipe_version(recipe)
        if version is None:
            continue
        category = session.query(Category).filter(
            (Category.seeded_recipe_id == recipe.id) | (Category.slug == recipe.slug)
        ).one_or_none()
        if category is None:
            category = Category(
                slug=recipe.slug,
                label=recipe.label,
                vertical=recipe.vertical,
                cluster_slug=recipe.cluster_slug,
                osm_tags=version.osm_tags,
                search_terms=version.search_terms,
                is_active=True,
                seeded_recipe_id=recipe.id,
            )
            session.add(category)
            session.commit()
            continue
        category.slug = recipe.slug
        category.label = recipe.label
        category.vertical = recipe.vertical
        category.cluster_slug = recipe.cluster_slug
        category.osm_tags = version.osm_tags
        category.search_terms = version.search_terms
        category.is_active = True
        category.seeded_recipe_id = recipe.id
        session.add(category)
        session.commit()


def _seed_recipe_variant_templates(session) -> None:
    for sort_order, template in enumerate(all_curated_variant_templates(), start=1):
        row = session.query(QueryRecipeVariantTemplate).filter(
            QueryRecipeVariantTemplate.key == template.key
        ).one_or_none()
        if row is None:
            row = QueryRecipeVariantTemplate(
                key=template.key,
                label=template.label,
                vertical=template.vertical,
                cluster_slug=template.cluster_slug or None,
                sub_intent=template.sub_intent,
            )
            session.add(row)
            session.flush()
        row.label = template.label
        row.vertical = template.vertical
        row.cluster_slug = template.cluster_slug or None
        row.sub_intent = template.sub_intent
        row.source_strategy = template.source_strategy
        row.aliases = list(template.aliases)
        row.osm_tags = [dict(item) for item in template.osm_tags]
        row.exclude_tags = [dict(item) for item in template.exclude_tags]
        row.search_terms = list(template.search_terms)
        row.website_keywords = list(template.website_keywords)
        row.language_hints = list(template.language_hints)
        row.rationale = list(template.rationale)
        row.template_score = template.priority
        row.sort_order = sort_order
        row.is_active = True
        session.add(row)
