from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.models import DailyUsage, QueryRecipe, QueryRecipeValidation, QueryRecipeVersion, RecipeStatus, Region
from app.services.recipe_variants import record_variant_validation


settings = get_settings()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _sample_regions(session: Session) -> list[Region]:
    stmt = (
        select(Region)
        .where(Region.is_active.is_(True), Region.osm_admin_level > 2)
        .order_by(Region.country_code, Region.name)
        .limit(settings.recipe_validation_sample_regions)
    )
    rows = session.scalars(stmt).all()
    if rows:
        return rows
    fallback = session.scalars(
        select(Region)
        .where(Region.is_active.is_(True), Region.osm_admin_level == 2)
        .order_by(Region.name)
        .limit(settings.recipe_validation_sample_regions)
    ).all()
    return fallback


def _query_for_tags(region: Region, osm_tags: list[dict[str, str]]) -> str:
    tag_clauses = []
    for tag_map in osm_tags:
        if not tag_map:
            continue
        key, value = next(iter(tag_map.items()))
        tag_clauses.append(f'  nwr["{key}"="{value}"](area.searchArea);')
    if "-" in region.code:
        area_selector = f'area["ISO3166-2"="{region.code}"]->.searchArea;'
    else:
        area_selector = f'area["ISO3166-1"="{region.country_code}"]["admin_level"="{region.osm_admin_level}"]->.searchArea;'
    return "\n".join(
        [
            "[out:json][timeout:25];",
            area_selector,
            "(",
            *tag_clauses,
            ");",
            "out center tags;",
        ]
    )


def _cache_key(recipe: QueryRecipe, version: QueryRecipeVersion, regions: list[Region]) -> str:
    payload = {
        "recipe_slug": recipe.slug,
        "version": version.version_number,
        "adapter": version.adapter.value,
        "osm_tags": version.osm_tags,
        "exclude_tags": version.exclude_tags,
        "search_terms": version.search_terms,
        "regions": [region.code for region in regions],
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _get_cached_validation(session: Session, recipe_version_id: int, cache_key: str) -> QueryRecipeValidation | None:
    now = _utcnow()
    stmt = (
        select(QueryRecipeValidation)
        .where(
            QueryRecipeValidation.recipe_version_id == recipe_version_id,
            QueryRecipeValidation.cache_key == cache_key,
            QueryRecipeValidation.expires_at.is_not(None),
            QueryRecipeValidation.expires_at > now,
        )
        .order_by(QueryRecipeValidation.created_at.desc())
        .limit(1)
    )
    return session.scalar(stmt)


def _validation_quota_available(session: Session) -> bool:
    today = _utcnow().date()
    usage = session.scalar(
        select(DailyUsage).where(
            DailyUsage.usage_date == today,
            DailyUsage.provider == "overpass_public_validation",
        )
    )
    if usage is None:
        usage = DailyUsage(
            usage_date=today,
            provider="overpass_public_validation",
            units_used=0,
            cap=settings.recipe_validation_daily_cap,
            metadata_json={},
        )
        session.add(usage)
        session.flush()
    return usage.units_used < usage.cap


def _increment_validation_usage(session: Session, count: int) -> None:
    today = _utcnow().date()
    usage = session.scalar(
        select(DailyUsage).where(
            DailyUsage.usage_date == today,
            DailyUsage.provider == "overpass_public_validation",
        )
    )
    if usage is None:
        usage = DailyUsage(
            usage_date=today,
            provider="overpass_public_validation",
            units_used=0,
            cap=settings.recipe_validation_daily_cap,
            metadata_json={},
        )
        session.add(usage)
        session.flush()
    usage.units_used += count
    session.add(usage)


def _score(metrics: dict) -> tuple[int, RecipeStatus]:
    total_results = metrics.get("total_results", 0)
    website_rate = metrics.get("website_rate", 0)
    distinct_names = metrics.get("distinct_name_ratio", 0)
    if total_results == 0:
        return 10, RecipeStatus.CANDIDATE
    score = min(
        100,
        round(
            min(total_results, 40) * 1.5
            + website_rate * 35
            + distinct_names * 25
        ),
    )
    if score >= 70:
        return score, RecipeStatus.VALIDATED
    if score >= 35:
        return score, RecipeStatus.CANDIDATE
    return score, RecipeStatus.DRAFT


def get_validation_quota_snapshot(session: Session) -> dict[str, int]:
    today = _utcnow().date()
    usage = session.scalar(
        select(DailyUsage).where(
            DailyUsage.usage_date == today,
            DailyUsage.provider == "overpass_public_validation",
        )
    )
    used = usage.units_used if usage is not None else 0
    cap = usage.cap if usage is not None else settings.recipe_validation_daily_cap
    return {"used": used, "cap": cap, "remaining": max(cap - used, 0)}


def validate_recipe_version(session: Session, recipe_id: int) -> tuple[QueryRecipeValidation, bool]:
    recipe = session.get(QueryRecipe, recipe_id)
    if recipe is None:
        raise ValueError("Recipe not found.")
    version = recipe.versions[0] if recipe.versions else None
    if version is None:
        raise ValueError("Recipe has no version to validate.")

    sample_regions = _sample_regions(session)
    cache_key = _cache_key(recipe, version, sample_regions)
    cached = _get_cached_validation(session, version.id, cache_key)
    if cached is not None:
        return cached, True
    if not _validation_quota_available(session):
        raise RuntimeError("Public validation quota reached for today.")

    headers = {"User-Agent": settings.user_agent}
    sampled_metrics: list[dict] = []
    total_results = 0
    total_with_website = 0
    distinct_names: set[str] = set()
    request_count = 0

    with httpx.Client(timeout=min(settings.request_timeout_seconds, 20), headers=headers) as client:
        for region in sample_regions:
            query = _query_for_tags(region, version.osm_tags)
            response = client.get(settings.recipe_validation_overpass_url, params={"data": query})
            request_count += 1
            response.raise_for_status()
            payload = response.json()
            elements = payload.get("elements", [])
            result_count = len(elements)
            website_count = 0
            names: set[str] = set()
            for element in elements:
                tags = element.get("tags", {})
                if tags.get("website") or tags.get("contact:website"):
                    website_count += 1
                name = tags.get("name")
                if name:
                    names.add(name.strip().lower())
            total_results += result_count
            total_with_website += website_count
            distinct_names.update(names)
            sampled_metrics.append(
                {
                    "region_code": region.code,
                    "region_name": region.name,
                    "result_count": result_count,
                    "website_count": website_count,
                    "distinct_names": len(names),
                }
            )

    _increment_validation_usage(session, request_count)
    website_rate = (total_with_website / total_results) if total_results else 0
    distinct_ratio = (len(distinct_names) / total_results) if total_results else 0
    metrics = {
        "total_results": total_results,
        "total_with_website": total_with_website,
        "website_rate": round(website_rate, 4),
        "distinct_names": len(distinct_names),
        "distinct_name_ratio": round(distinct_ratio, 4),
        "sampled_regions": sampled_metrics,
    }
    score, derived_status = _score(metrics)
    validation = QueryRecipeValidation(
        recipe_version_id=version.id,
        status=derived_status,
        provider="overpass_public",
        sample_regions=[region.code for region in sample_regions],
        score=score,
        metrics_json=metrics,
        cache_key=cache_key,
        expires_at=_utcnow() + timedelta(hours=settings.recipe_validation_cache_hours),
    )
    version.status = derived_status
    recipe.status = derived_status if recipe.status == RecipeStatus.DRAFT or derived_status == RecipeStatus.VALIDATED else recipe.status
    session.add(version)
    session.add(recipe)
    session.add(validation)
    session.flush()
    record_variant_validation(session, recipe, validation, metrics)
    session.commit()
    session.refresh(validation)
    return validation, False
