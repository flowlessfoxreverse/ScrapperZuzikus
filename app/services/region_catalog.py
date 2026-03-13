from __future__ import annotations

import pycountry
from sqlalchemy.orm import Session

from app.config import get_settings
from app.models import Region


settings = get_settings()


def active_country_codes() -> set[str]:
    return {
        item.strip().upper()
        for item in settings.region_catalog_countries.split(",")
        if item.strip()
    }


def top_level_subdivisions(country_code: str):
    for subdivision in pycountry.subdivisions.get(country_code=country_code) or []:
        if getattr(subdivision, "parent_code", None):
            continue
        yield subdivision


def upsert_region(
    session: Session,
    *,
    code: str,
    name: str,
    country_code: str,
    osm_admin_level: int,
    is_active: bool,
) -> None:
    region = session.query(Region).filter(Region.code == code).one_or_none()
    if region is None:
        region = Region(
            code=code,
            name=name,
            country_code=country_code,
            osm_admin_level=osm_admin_level,
            is_active=is_active,
        )
    else:
        region.name = name
        region.country_code = country_code
        region.osm_admin_level = osm_admin_level
        region.is_active = is_active
    session.add(region)


def sync_region_catalog(session: Session) -> int:
    active_codes = active_country_codes()
    count = 0

    for country in pycountry.countries:
        code = getattr(country, "alpha_2", None)
        if not code:
            continue
        is_active = code in active_codes
        upsert_region(
            session,
            code=code,
            name=country.name,
            country_code=code,
            osm_admin_level=2,
            is_active=is_active,
        )
        count += 1

        for subdivision in top_level_subdivisions(code):
            upsert_region(
                session,
                code=subdivision.code,
                name=f"{subdivision.name} [{subdivision.code}], {country.name}",
                country_code=code,
                osm_admin_level=4,
                is_active=is_active,
            )
            count += 1

    session.commit()
    return count
