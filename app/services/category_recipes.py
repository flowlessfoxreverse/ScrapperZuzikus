from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Category, QueryRecipe, QueryRecipeVersion, RecipeAdapter, RecipeStatus, Vertical


@dataclass
class EffectiveCategoryConfig:
    osm_tags: list[dict[str, str]]
    search_terms: list[str]
    adapter: RecipeAdapter | None
    version_number: int | None
    source: str


def latest_recipe_version(recipe: QueryRecipe | None) -> QueryRecipeVersion | None:
    if recipe is None or not recipe.versions:
        return None
    return recipe.versions[0]


def effective_category_config(category: Category) -> EffectiveCategoryConfig:
    recipe = category.seeded_recipe
    version = latest_recipe_version(recipe)
    if (
        recipe is not None
        and recipe.status == RecipeStatus.ACTIVE
        and version is not None
        and version.status == RecipeStatus.ACTIVE
    ):
        return EffectiveCategoryConfig(
            osm_tags=version.osm_tags,
            search_terms=version.search_terms,
            adapter=version.adapter,
            version_number=version.version_number,
            source="recipe",
        )
    return EffectiveCategoryConfig(
        osm_tags=category.osm_tags,
        search_terms=category.search_terms,
        adapter=None,
        version_number=None,
        source="category",
    )


def sync_recipe_to_category(db: Session, recipe: QueryRecipe, version: QueryRecipeVersion) -> Category:
    category = db.scalar(
        select(Category).where(
            (Category.seeded_recipe_id == recipe.id) | (Category.slug == recipe.slug)
        ).limit(1)
    )
    if category is None:
        category = Category(
            slug=recipe.slug,
            label=recipe.label,
            vertical=recipe.vertical,
            osm_tags=version.osm_tags,
            search_terms=version.search_terms,
            is_active=True,
            seeded_recipe_id=recipe.id,
        )
        db.add(category)
        db.flush()
        return category

    category.slug = recipe.slug
    category.label = recipe.label
    category.vertical = recipe.vertical
    category.osm_tags = version.osm_tags
    category.search_terms = version.search_terms
    category.is_active = True
    category.seeded_recipe_id = recipe.id
    db.add(category)
    return category


def upsert_recipe_backed_category(
    db: Session,
    *,
    slug: str,
    label: str,
    vertical: Vertical,
    osm_tags: list[dict[str, str]],
    search_terms: list[str],
    description: str | None,
    adapter: RecipeAdapter,
    notes: str,
    recipe_status: RecipeStatus = RecipeStatus.ACTIVE,
) -> tuple[Category, QueryRecipe, QueryRecipeVersion]:
    normalized_slug = slug.strip().lower()
    normalized_label = label.strip()
    recipe = db.scalar(select(QueryRecipe).where(QueryRecipe.slug == normalized_slug))
    if recipe is None:
        recipe = QueryRecipe(
            slug=normalized_slug,
            label=normalized_label,
            description=(description or "").strip() or None,
            vertical=vertical,
            status=recipe_status,
            is_platform_template=True,
        )
        db.add(recipe)
        db.flush()
        next_version_number = 1
    else:
        next_version_number = latest_recipe_version(recipe).version_number + 1 if latest_recipe_version(recipe) else 1
        recipe.label = normalized_label
        recipe.description = (description or "").strip() or recipe.description
        recipe.vertical = vertical
        recipe.status = recipe_status
        db.add(recipe)

    version = QueryRecipeVersion(
        recipe_id=recipe.id,
        version_number=next_version_number,
        status=recipe_status,
        adapter=adapter,
        osm_tags=osm_tags,
        exclude_tags=[],
        search_terms=search_terms,
        website_keywords=search_terms,
        language_hints=[],
        notes=notes,
    )
    db.add(version)
    db.flush()
    category = sync_recipe_to_category(db, recipe, version)
    return category, recipe, version
