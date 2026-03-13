from __future__ import annotations

from dataclasses import dataclass

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
    return f"""
[out:json][timeout:90];
area["ISO3166-1"="{region.country_code}"]["admin_level"="{region.osm_admin_level}"]->.searchArea;
(
{tag_clauses}
);
out center tags;
"""


def fetch_places(region: Region, category: Category) -> OverpassResult:
    query = build_query(region=region, category=category)
    headers = {"User-Agent": settings.user_agent}
    with httpx.Client(timeout=settings.request_timeout_seconds, headers=headers) as client:
        response = client.post(settings.overpass_url, content=query)
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
