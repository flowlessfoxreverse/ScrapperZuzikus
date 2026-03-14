from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import NicheCluster, TaxonomyVertical


@dataclass(frozen=True)
class SeededVertical:
    slug: str
    label: str
    description: str
    sort_order: int


@dataclass(frozen=True)
class SeededCluster:
    slug: str
    vertical_slug: str
    label: str
    description: str
    sort_order: int


CURATED_VERTICALS: tuple[SeededVertical, ...] = (
    SeededVertical("vehicle", "Vehicle", "Vehicle rental, leasing, transport, and mobility businesses.", 10),
    SeededVertical("tourism", "Tourism", "Travel, tours, attractions, activities, and hospitality discovery.", 20),
    SeededVertical("beauty", "Beauty", "Beauty salons, nail studios, spas, and personal care services.", 30),
    SeededVertical("health", "Health", "Clinics, wellness providers, and patient-facing healthcare businesses.", 40),
    SeededVertical("food", "Food", "Restaurants, cafes, catering, and food-service businesses.", 50),
    SeededVertical("fitness", "Fitness", "Gyms, trainers, studios, and movement-focused businesses.", 60),
    SeededVertical("real_estate", "Real Estate", "Property agencies, brokers, developers, and rentals.", 70),
    SeededVertical("education", "Education", "Schools, training centers, tutors, and enrichment services.", 80),
    SeededVertical("legal", "Legal", "Law firms, legal consultants, and compliance-focused services.", 90),
    SeededVertical("retail", "Retail", "Shops, boutiques, specialty retail, and storefront businesses.", 100),
    SeededVertical("home_services", "Home Services", "Repair, maintenance, cleaning, and local contractor services.", 110),
)


CURATED_CLUSTERS: tuple[SeededCluster, ...] = (
    SeededCluster("vehicle_rentals", "vehicle", "Vehicle Rentals", "Car, motorcycle, bike, scooter, and ATV rental businesses.", 10),
    SeededCluster("vehicle_leasing", "vehicle", "Vehicle Leasing", "Long-term lease and fleet leasing providers.", 20),
    SeededCluster("tour_operators", "tourism", "Tour Operators", "Travel agencies, excursions, tours, and guide operators.", 10),
    SeededCluster("tourism_activities", "tourism", "Tourism Activities", "Diving, sanctuaries, attractions, and adventure experiences.", 20),
    SeededCluster("beauty_services", "beauty", "Beauty Services", "Hair, nails, beauty salons, and studio services.", 10),
    SeededCluster("beauty_clinics", "beauty", "Beauty Clinics", "Cosmetic, skincare, and aesthetic clinic providers.", 20),
    SeededCluster("wellness_clinics", "health", "Wellness Clinics", "Dental, rehab, physiotherapy, and general clinics.", 10),
    SeededCluster("food_service", "food", "Food Service", "Restaurants, cafes, and dine-in food businesses.", 10),
    SeededCluster("fitness_studios", "fitness", "Fitness Studios", "Gyms, yoga, pilates, and training studios.", 10),
    SeededCluster("property_agencies", "real_estate", "Property Agencies", "Real-estate agencies, brokers, and lettings.", 10),
    SeededCluster("training_centers", "education", "Training Centers", "Courses, tuition, and enrichment centers.", 10),
    SeededCluster("law_firms", "legal", "Law Firms", "Law offices and legal-service providers.", 10),
    SeededCluster("specialty_retail", "retail", "Specialty Retail", "Retail shops and direct-to-customer specialty stores.", 10),
    SeededCluster("property_services", "home_services", "Property Services", "Cleaning, repair, installation, and home maintenance.", 10),
)


def seed_taxonomy(session: Session) -> None:
    for seeded in CURATED_VERTICALS:
        vertical = session.scalar(select(TaxonomyVertical).where(TaxonomyVertical.slug == seeded.slug))
        if vertical is None:
            vertical = TaxonomyVertical(
                slug=seeded.slug,
                label=seeded.label,
                description=seeded.description,
                sort_order=seeded.sort_order,
                is_active=True,
            )
            session.add(vertical)
            session.flush()
        else:
            vertical.label = seeded.label
            vertical.description = seeded.description
            vertical.sort_order = seeded.sort_order
            vertical.is_active = True
            session.add(vertical)

    for seeded in CURATED_CLUSTERS:
        cluster = session.scalar(select(NicheCluster).where(NicheCluster.slug == seeded.slug))
        if cluster is None:
            cluster = NicheCluster(
                slug=seeded.slug,
                vertical_slug=seeded.vertical_slug,
                label=seeded.label,
                description=seeded.description,
                sort_order=seeded.sort_order,
                is_active=True,
            )
            session.add(cluster)
            session.flush()
        else:
            cluster.vertical_slug = seeded.vertical_slug
            cluster.label = seeded.label
            cluster.description = seeded.description
            cluster.sort_order = seeded.sort_order
            cluster.is_active = True
            session.add(cluster)


def list_active_verticals(session: Session) -> list[TaxonomyVertical]:
    return list(
        session.scalars(
            select(TaxonomyVertical)
            .where(TaxonomyVertical.is_active.is_(True))
            .order_by(TaxonomyVertical.sort_order, TaxonomyVertical.label)
        ).all()
    )


def list_active_clusters(session: Session) -> list[NicheCluster]:
    return list(
        session.scalars(
            select(NicheCluster)
            .where(NicheCluster.is_active.is_(True))
            .order_by(NicheCluster.vertical_slug, NicheCluster.sort_order, NicheCluster.label)
        ).all()
    )
