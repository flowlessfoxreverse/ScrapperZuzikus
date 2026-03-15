from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import Category, Company, CompanyCategory, ContactChannel, Email, Phone, QueryRecipe, QueryRecipeVariant, QueryRecipeVariantRunStat, RunCategory, RunCompany, RunCompanyStatus, ScrapeRun


def _score(
    *,
    discovered_count: int,
    crawled_count: int,
    website_company_count: int,
    contact_company_count: int,
    email_company_count: int,
    phone_company_count: int,
) -> int:
    base = max(discovered_count, 1)
    website_rate = website_company_count / base
    crawl_rate = crawled_count / base
    contact_rate = contact_company_count / base
    email_rate = email_company_count / base
    phone_rate = phone_company_count / base
    return round(
        (website_rate * 25)
        + (crawl_rate * 20)
        + (contact_rate * 15)
        + (email_rate * 30)
        + (phone_rate * 10)
    )


def _variant_for_category(session: Session, category: Category) -> QueryRecipeVariant | None:
    recipe = category.seeded_recipe or session.scalar(
        select(QueryRecipe).where(QueryRecipe.slug == category.slug).limit(1)
    )
    if recipe is None:
        return None
    return recipe.source_variant


def sync_variant_production_performance(session: Session, run_id: int) -> None:
    run = session.get(ScrapeRun, run_id)
    if run is None:
        return

    category_ids = session.scalars(
        select(RunCategory.category_id).where(RunCategory.run_id == run_id)
    ).all()
    if not category_ids:
        return

    categories = session.scalars(
        select(Category).where(Category.id.in_(category_ids))
    ).all()
    now = datetime.now(timezone.utc)
    touched_variant_ids: set[int] = set()

    for category in categories:
        variant = _variant_for_category(session, category)
        if variant is None:
            continue

        company_ids = session.scalars(
            select(Company.id)
            .join(CompanyCategory, CompanyCategory.company_id == Company.id)
            .where(
                Company.region_id == run.region_id,
                CompanyCategory.category_id == category.id,
            )
        ).all()
        company_ids = list(dict.fromkeys(company_ids))
        discovered_count = len(company_ids)
        if not company_ids:
            stat = session.scalar(
                select(QueryRecipeVariantRunStat).where(
                    QueryRecipeVariantRunStat.variant_id == variant.id,
                    QueryRecipeVariantRunStat.run_id == run_id,
                    QueryRecipeVariantRunStat.category_id == category.id,
                )
            )
            if stat is None:
                stat = QueryRecipeVariantRunStat(
                    variant_id=variant.id,
                    run_id=run_id,
                    category_id=category.id,
                    region_id=run.region_id,
                )
                session.add(stat)
            stat.discovered_count = 0
            stat.crawled_count = 0
            stat.website_company_count = 0
            stat.contact_company_count = 0
            stat.email_company_count = 0
            stat.phone_company_count = 0
            stat.score = 0
            stat.updated_at = now
            touched_variant_ids.add(variant.id)
            continue

        website_company_count = session.scalar(
            select(func.count())
            .select_from(Company)
            .where(Company.id.in_(company_ids), Company.website_url.is_not(None))
        ) or 0
        crawled_count = session.scalar(
            select(func.count())
            .select_from(RunCompany)
            .where(
                RunCompany.run_id == run_id,
                RunCompany.company_id.in_(company_ids),
                RunCompany.status == RunCompanyStatus.COMPLETED,
            )
        ) or 0
        email_company_count = session.scalar(
            select(func.count(func.distinct(Email.company_id)))
            .where(Email.company_id.in_(company_ids))
        ) or 0
        phone_company_count = session.scalar(
            select(func.count(func.distinct(Phone.company_id)))
            .where(Phone.company_id.in_(company_ids))
        ) or 0
        channel_company_count = session.scalar(
            select(func.count(func.distinct(ContactChannel.company_id)))
            .where(ContactChannel.company_id.in_(company_ids))
        ) or 0
        contact_company_count = len(
            {
                *session.scalars(select(Email.company_id).where(Email.company_id.in_(company_ids))).all(),
                *session.scalars(select(Phone.company_id).where(Phone.company_id.in_(company_ids))).all(),
                *session.scalars(select(ContactChannel.company_id).where(ContactChannel.company_id.in_(company_ids))).all(),
            }
        )
        score = _score(
            discovered_count=discovered_count,
            crawled_count=crawled_count,
            website_company_count=website_company_count,
            contact_company_count=contact_company_count,
            email_company_count=email_company_count,
            phone_company_count=max(phone_company_count, channel_company_count),
        )

        stat = session.scalar(
            select(QueryRecipeVariantRunStat).where(
                QueryRecipeVariantRunStat.variant_id == variant.id,
                QueryRecipeVariantRunStat.run_id == run_id,
                QueryRecipeVariantRunStat.category_id == category.id,
            )
        )
        if stat is None:
            stat = QueryRecipeVariantRunStat(
                variant_id=variant.id,
                run_id=run_id,
                category_id=category.id,
                region_id=run.region_id,
            )
            session.add(stat)
        stat.discovered_count = discovered_count
        stat.crawled_count = crawled_count
        stat.website_company_count = website_company_count
        stat.contact_company_count = contact_company_count
        stat.email_company_count = email_company_count
        stat.phone_company_count = max(phone_company_count, channel_company_count)
        stat.score = score
        stat.updated_at = now
        touched_variant_ids.add(variant.id)

    session.flush()

    for variant_id in touched_variant_ids:
        stats = session.scalars(
            select(QueryRecipeVariantRunStat).where(QueryRecipeVariantRunStat.variant_id == variant_id)
        ).all()
        variant = session.get(QueryRecipeVariant, variant_id)
        if variant is None:
            continue
        variant.production_run_count = len(stats)
        variant.production_discovered_total = sum(stat.discovered_count for stat in stats)
        variant.production_crawled_total = sum(stat.crawled_count for stat in stats)
        variant.production_website_company_total = sum(stat.website_company_count for stat in stats)
        variant.production_contact_company_total = sum(stat.contact_company_count for stat in stats)
        variant.production_email_company_total = sum(stat.email_company_count for stat in stats)
        variant.production_phone_company_total = sum(stat.phone_company_count for stat in stats)
        variant.observed_production_score = round(sum(stat.score for stat in stats) / len(stats)) if stats else 0
        variant.last_production_at = now
        session.add(variant)
