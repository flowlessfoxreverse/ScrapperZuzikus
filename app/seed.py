from sqlalchemy.exc import IntegrityError

from app.models import Category, Region, Vertical


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
        "vertical": Vertical.VEHICLE,
        "osm_tags": [{"amenity": "car_rental"}],
        "search_terms": ["car rental agency", "rent a car"],
    },
    {
        "slug": "motorcycle-rental-agency",
        "label": "Motorcycle Rental Agency",
        "vertical": Vertical.VEHICLE,
        "osm_tags": [{"shop": "motorcycle_rental"}],
        "search_terms": ["motorcycle rental agency", "motorbike rental"],
    },
    {
        "slug": "scooter-rental-service",
        "label": "Scooter Rental Service",
        "vertical": Vertical.VEHICLE,
        "osm_tags": [{"shop": "motorcycle_rental"}],
        "search_terms": ["scooter rental service"],
    },
    {
        "slug": "bike-rental",
        "label": "Bike Rental",
        "vertical": Vertical.VEHICLE,
        "osm_tags": [{"amenity": "bicycle_rental"}],
        "search_terms": ["bike rental"],
    },
    {
        "slug": "quad-rental",
        "label": "Quad Rental",
        "vertical": Vertical.VEHICLE,
        "osm_tags": [{"shop": "motorcycle_rental"}],
        "search_terms": ["quad rental", "ATV rental"],
    },
    {
        "slug": "tour-agency",
        "label": "Tour Agency",
        "vertical": Vertical.TOURISM,
        "osm_tags": [{"shop": "travel_agency"}],
        "search_terms": ["tour agency", "tour operator"],
    },
    {
        "slug": "travel-agency",
        "label": "Travel Agency",
        "vertical": Vertical.TOURISM,
        "osm_tags": [{"shop": "travel_agency"}],
        "search_terms": ["travel agency"],
    },
    {
        "slug": "tour-guide-service",
        "label": "Tour Guide Service",
        "vertical": Vertical.TOURISM,
        "osm_tags": [{"tourism": "information"}],
        "search_terms": ["tour guide service", "excursions agency"],
    },
]


def seed_defaults(session) -> None:
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
