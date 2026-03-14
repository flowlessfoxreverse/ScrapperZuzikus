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


def country_catalog() -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for country in sorted(pycountry.countries, key=lambda item: item.name):
        code = getattr(country, "alpha_2", None)
        if not code:
            continue
        items.append(
            {
                "code": code,
                "name": country.name,
            }
        )
    return items


def get_country(country_code: str):
    return pycountry.countries.get(alpha_2=country_code.upper())


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


def upsert_country_with_subdivisions(session: Session, country_code: str, *, is_active: bool = True) -> int:
    country = get_country(country_code)
    if country is None:
        raise ValueError(f"Unknown country code: {country_code}")

    count = 0
    code = country.alpha_2.upper()
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
