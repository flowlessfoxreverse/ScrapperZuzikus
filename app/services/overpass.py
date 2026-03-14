from __future__ import annotations

from dataclasses import dataclass
import json
import time

import httpx

from app.config import get_settings
from app.models import Category, Region


settings = get_settings()


@dataclass
class OverpassResult:
    query: str
    elements: list[dict]


@dataclass
class OverpassStatus:
    ok: bool
    status_code: int | None
    summary: str
    detail: str


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


def status_url() -> str:
    if settings.overpass_url.endswith("/interpreter"):
        return settings.overpass_url[: -len("/interpreter")] + "/status"
    return settings.overpass_url.rstrip("/") + "/status"


def fetch_status() -> OverpassStatus:
    url = status_url()
    headers = {"User-Agent": settings.user_agent}
    try:
        with httpx.Client(timeout=min(settings.request_timeout_seconds, 10), headers=headers) as client:
            response = client.get(url)
        lines = [line.strip() for line in response.text.splitlines() if line.strip()]
        detail = " | ".join(lines[:4])[:500] if lines else "No status details returned."
        summary = "healthy" if response.is_success else "unavailable"
        lowered = response.text.lower()
        if "currently running queries" in lowered or "slots available now" in lowered:
            summary = "healthy"
        elif "rate_limited" in lowered or "rate limited" in lowered:
            summary = "rate_limited"
        elif "dispatcher" in lowered or "database not opened" in lowered or "not ready" in lowered:
            summary = "bootstrapping"
        if not response.is_success:
            probe = _probe_interpreter(headers)
            if probe is not None:
                return probe
        return OverpassStatus(
            ok=response.is_success,
            status_code=response.status_code,
            summary=summary,
            detail=detail,
        )
    except Exception as exc:
        return OverpassStatus(
            ok=False,
            status_code=None,
            summary="unreachable",
            detail=str(exc)[:500],
        )


def _probe_interpreter(headers: dict[str, str]) -> OverpassStatus | None:
    probe_query = "[out:json][timeout:25];node(1);out;"
    try:
        with httpx.Client(timeout=min(settings.request_timeout_seconds, 10), headers=headers) as client:
            response = client.post(settings.overpass_url, content=probe_query)
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict) and "elements" in payload:
            return OverpassStatus(
                ok=True,
                status_code=response.status_code,
                summary="healthy",
                detail="Status endpoint unavailable, but interpreter probe succeeded.",
            )
    except Exception:
        return None
    return None


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
        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            content_type = response.headers.get("content-type", "unknown")
            detail = response.text.strip()[:2000] or "<empty response>"
            raise RuntimeError(
                f"Overpass returned non-JSON payload for category {category.slug} in region "
                f"{region.code}. Status: {response.status_code}. Content-Type: {content_type}. "
                f"Query: {query.strip()} Response: {detail}"
            ) from exc
    return OverpassResult(query=query, elements=payload.get("elements", []))
