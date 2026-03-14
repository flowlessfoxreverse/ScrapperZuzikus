from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
import json
from pathlib import Path
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
    stage: str
    ready: bool
    progress_percent: int | None
    progress_label: str | None
    files: dict[str, int | bool | None]


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


def _inspect_bootstrap_files() -> dict[str, int | bool | None]:
    base = Path(settings.overpass_data_path)
    pbf = base / "planet.osm.pbf"
    bz2 = base / "planet.osm.bz2"
    db_dir = base / "db"
    return {
        "data_path_exists": base.exists(),
        "pbf_exists": pbf.exists(),
        "pbf_size": pbf.stat().st_size if pbf.exists() else None,
        "bz2_exists": bz2.exists(),
        "bz2_size": bz2.stat().st_size if bz2.exists() else None,
        "db_exists": db_dir.exists(),
        "db_size": _directory_size(db_dir) if db_dir.exists() else None,
    }


def _directory_size(path: Path) -> int:
    total = 0
    for child in path.rglob("*"):
        if child.is_file():
            try:
                total += child.stat().st_size
            except OSError:
                continue
    return total


def _format_size(num_bytes: int | None) -> str:
    if not num_bytes:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(num_bytes)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TB"


def _bootstrap_status_from_files() -> OverpassStatus:
    files = _inspect_bootstrap_files()
    if not files["data_path_exists"]:
        return OverpassStatus(
            ok=False,
            status_code=None,
            summary="unreachable",
            detail="Overpass data volume is not mounted in the app container.",
            stage="unknown",
            ready=False,
            progress_percent=None,
            progress_label=None,
            files=files,
        )
    if files["pbf_exists"] and files["bz2_exists"]:
        progress_percent, progress_label = _conversion_progress(files)
        return OverpassStatus(
            ok=False,
            status_code=None,
            summary="bootstrapping",
            detail=(
                "Converting downloaded extract to Overpass format. "
                f"PBF: {_format_size(files['pbf_size'])}, OSM.BZ2: {_format_size(files['bz2_size'])}."
            ),
            stage="converting",
            ready=False,
            progress_percent=progress_percent,
            progress_label=progress_label,
            files=files,
        )
    if files["bz2_exists"] and not files["pbf_exists"]:
        progress_percent, progress_label = _import_progress(files)
        return OverpassStatus(
            ok=False,
            status_code=None,
            summary="bootstrapping",
            detail=(
                "Importing converted data into Overpass. "
                f"OSM.BZ2: {_format_size(files['bz2_size'])}. "
                f"Indexed DB: {_format_size(files['db_size'])}."
            ),
            stage="importing",
            ready=False,
            progress_percent=progress_percent,
            progress_label=progress_label,
            files=files,
        )
    return OverpassStatus(
        ok=False,
        status_code=None,
        summary="bootstrapping",
        detail="Downloading initial extract for Overpass.",
        stage="downloading",
        ready=False,
        progress_percent=5,
        progress_label="Waiting for initial extract",
        files=files,
    )


def _conversion_progress(files: dict[str, int | bool | None]) -> tuple[int | None, str | None]:
    pbf_size = files.get("pbf_size")
    bz2_size = files.get("bz2_size")
    if not isinstance(pbf_size, int) or pbf_size <= 0 or not isinstance(bz2_size, int):
        return None, None
    # Regional Overpass imports typically expand above the compressed PBF size.
    estimated_final_bz2 = max(int(pbf_size * 2), pbf_size)
    progress = min(95, max(1, round((bz2_size / estimated_final_bz2) * 100)))
    return progress, f"Approx. {progress}% of conversion complete"


def _import_progress(files: dict[str, int | bool | None]) -> tuple[int | None, str | None]:
    bz2_size = files.get("bz2_size")
    db_size = files.get("db_size")
    if not isinstance(bz2_size, int) or bz2_size <= 0 or not isinstance(db_size, int):
        return None, None
    # Overpass indexes typically settle at several times the compressed input size.
    estimated_db_size = max(int(bz2_size * 9), bz2_size)
    progress = min(99, max(55, round((db_size / estimated_db_size) * 100)))
    return progress, f"Approx. {progress}% of import complete"


def fetch_status() -> OverpassStatus:
    url = status_url()
    headers = {"User-Agent": settings.user_agent}
    files = _inspect_bootstrap_files()
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
            bootstrap = _bootstrap_status_from_files()
            bootstrap.status_code = response.status_code
            bootstrap.detail = detail or bootstrap.detail
            return bootstrap
        return OverpassStatus(
            ok=response.is_success,
            status_code=response.status_code,
            summary=summary,
            detail=detail,
            stage="ready",
            ready=True,
            progress_percent=100,
            progress_label="Ready",
            files=files,
        )
    except Exception as exc:
        bootstrap = _bootstrap_status_from_files()
        if bootstrap.stage != "unknown":
            bootstrap.detail = f"{bootstrap.detail} Probe error: {str(exc)[:200]}"
            return bootstrap
        return OverpassStatus(
            ok=False,
            status_code=None,
            summary="unreachable",
            detail=str(exc)[:500],
            stage="unknown",
            ready=False,
            progress_percent=None,
            progress_label=None,
            files=files,
        )


def _probe_interpreter(headers: dict[str, str]) -> OverpassStatus | None:
    probe_query = "[out:json][timeout:25];node(1);out;"
    try:
        with httpx.Client(timeout=min(settings.request_timeout_seconds, 10), headers=headers) as client:
            response = client.get(settings.overpass_url, params={"data": probe_query})
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict) and "elements" in payload:
            return OverpassStatus(
                ok=True,
                status_code=response.status_code,
                summary="healthy",
                detail="Status endpoint unavailable, but interpreter probe succeeded.",
                stage="ready",
                ready=True,
                progress_percent=100,
                progress_label="Ready",
                files=_inspect_bootstrap_files(),
            )
    except Exception:
        return None
    return None


def fetch_status_payload() -> dict:
    return asdict(fetch_status())


def fetch_places(region: Region, category: Category, on_request=None) -> OverpassResult:
    query = build_query(region=region, category=category)
    headers = {"User-Agent": settings.user_agent}
    with httpx.Client(timeout=settings.request_timeout_seconds, headers=headers) as client:
        last_exception: Exception | None = None
        for attempt in range(1, settings.overpass_connect_retries + 1):
            started = time.perf_counter()
            try:
                response = client.get(settings.overpass_url, params={"data": query})
            except httpx.HTTPError as exc:
                duration_ms = int((time.perf_counter() - started) * 1000)
                if on_request:
                    on_request(
                        method="GET",
                        url=settings.overpass_url,
                        status_code=None,
                        duration_ms=duration_ms,
                        error=f"attempt {attempt}/{settings.overpass_connect_retries}: {exc}",
                    )
                last_exception = exc
                if attempt < settings.overpass_connect_retries:
                    time.sleep(settings.overpass_retry_backoff_seconds * attempt)
                    continue
                raise RuntimeError(
                    f"Overpass connection failed for category {category.slug} in region {region.code} "
                    f"after {settings.overpass_connect_retries} attempts: {exc}"
                ) from exc

            duration_ms = int((time.perf_counter() - started) * 1000)
            if on_request:
                on_request(
                    method="GET",
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
            break
        else:
            raise RuntimeError(
                f"Overpass connection failed for category {category.slug} in region {region.code}: {last_exception}"
            )
    return OverpassResult(query=query, elements=payload.get("elements", []))
