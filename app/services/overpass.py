from __future__ import annotations

from dataclasses import dataclass
import time

import httpx

from app.config import get_settings
from app.models import Category, Region


settings = get_settings()


@dataclass
class OverpassResult:
    query: str
    elements: list[dict]


def _tag_clause(tag_map: dict[str, str]) -> str:
    key, value = next(iter(tag_map.items()))
    return f'nwr["{key}"="{value}"](area.searchArea);'


def build_query(region: Region, category: Category) -> str:
    tag_clauses = "\n".join(
        [f"  {_tag_clause(tag_map)}" for tag_map in category.osm_tags]
    )
    if "-" in region.code:
        area_selector = f'area["ISO3166-2"="{region.code}"]->.searchArea;'
    else:
        area_selector = f'area["ISO3166-1"="{region.country_code}"]["admin_level"="{region.osm_admin_level}"]->.searchArea;'
    return f"""
[out:json][timeout:90];
{area_selector}
(
{tag_clauses}
);
out center tags;
"""


def fetch_places(region: Region, category: Category, on_request=None) -> OverpassResult:
    query = build_query(region=region, category=category)
    headers = {"User-Agent": settings.user_agent}
    with httpx.Client(timeout=settings.request_timeout_seconds, headers=headers) as client:
        started = time.perf_counter()
        response = client.post(settings.overpass_url, content=query)
        duration_ms = int((time.perf_counter() - started) * 1000)
        if on_request:
            on_request(
                method="POST",
                url=settings.overpass_url,
                status_code=response.status_code,
                duration_ms=duration_ms,
                error=None if response.is_success else response.text.strip()[:2000],
            )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            detail = response.text.strip()
            raise RuntimeError(
                f"Overpass request failed with status {response.status_code} for category "
                f"{category.slug} in region {region.code}. Query: {query.strip()} Response: {detail}"
            ) from exc
        payload = response.json()
    return OverpassResult(query=query, elements=payload.get("elements", []))
