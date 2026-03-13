from __future__ import annotations

from dataclasses import dataclass

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import Category, Company, Email, Region, RunCategory, ScrapeRun, ValidationStatus, Vertical
from app.schemas import EmailRow
from app.services.overpass import fetch_status
from app.services.runs import find_active_run
from app.tasks import run_scrape, sync_region_catalog_task


templates = Jinja2Templates(directory="app/templates")
router = APIRouter(tags=["ui"])


@dataclass
class RegionStatsRow:
    id: int
    name: str
    code: str
    total_companies: int
    total_emails: int
    valid_emails: int
    last_run_status: str | None


def build_email_rows(db: Session, region_id: int | None = None) -> list[EmailRow]:
    stmt = (
        select(Email, Region)
        .join(Company, Email.company_id == Company.id)
        .join(Region, Company.region_id == Region.id)
        .order_by(desc(Email.last_seen_at))
    )
    if region_id:
        stmt = stmt.where(Region.id == region_id)

    rows = []
    for email, region in db.execute(stmt).all():
        rows.append(
            EmailRow(
                id=email.id,
                email=email.email,
                company_name=email.company.name,
                company_city=email.company.city,
                company_website=email.company.website_url,
                region_name=region.name,
                validation_status=email.validation_status,
                suppression_status=email.suppression_status,
                source_type=email.source_type,
                source_page_url=email.source_page_url,
                crawl_status=email.company.crawl_status,
                has_contact_form=email.company.has_contact_form,
                technical_metadata=email.technical_metadata,
            )
        )
    return rows


def build_region_stats(db: Session) -> list[RegionStatsRow]:
    rows = []
    regions = db.scalars(select(Region).where(Region.is_active.is_(True)).order_by(Region.name)).all()
    for region in regions:
        total_companies = db.scalar(select(func.count()).select_from(Company).where(Company.region_id == region.id)) or 0
        total_emails = db.scalar(
            select(func.count()).select_from(Email).join(Company, Email.company_id == Company.id).where(Company.region_id == region.id)
        ) or 0
        valid_emails = db.scalar(
            select(func.count()).select_from(Email).join(Company, Email.company_id == Company.id).where(
                Company.region_id == region.id,
                Email.validation_status == ValidationStatus.VALID,
            )
        ) or 0
        last_run = db.scalar(
            select(ScrapeRun.status).where(ScrapeRun.region_id == region.id).order_by(ScrapeRun.started_at.desc()).limit(1)
        )
        if total_companies == 0 and total_emails == 0 and last_run is None:
            continue
        rows.append(
            RegionStatsRow(
                id=region.id,
                name=region.name,
                code=region.code,
                total_companies=total_companies,
                total_emails=total_emails,
                valid_emails=valid_emails,
                last_run_status=last_run.value if last_run else None,
            )
        )
    return rows


@router.get("/", response_class=HTMLResponse)
def dashboard(
    request: Request,
    region_id: int | None = None,
    message: str | None = None,
    db: Session = Depends(get_db),
) -> HTMLResponse:
    regions = db.scalars(select(Region).where(Region.is_active.is_(True)).order_by(Region.name)).all()
    categories = db.scalars(select(Category).where(Category.is_active.is_(True)).order_by(Category.vertical, Category.label)).all()
    form_region_id = region_id or (regions[0].id if regions else None)
    detail_region = db.get(Region, region_id) if region_id else None
    emails = build_email_rows(db, detail_region.id if detail_region else None)
    runs = db.scalars(select(ScrapeRun).order_by(desc(ScrapeRun.started_at)).limit(10)).all()
    region_stats = build_region_stats(db)
    overpass_status = fetch_status()
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={
            "regions": regions,
            "categories": categories,
            "selected_region": form_region_id,
            "detail_region": detail_region,
            "emails": emails,
            "runs": runs,
            "region_stats": region_stats,
            "overpass_status": overpass_status,
            "message": message,
            "validation_statuses": list(ValidationStatus),
        },
    )


@router.post("/runs", response_class=HTMLResponse)
def queue_run(
    region_id: int = Form(...),
    category_ids: list[int] = Form(...),
    force_refresh: str | None = Form(None),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    active_run = find_active_run(db, region_id)
    if active_run is not None:
        return RedirectResponse(
            url=f"/?region_id={region_id}&message=Run+{active_run.id}+is+already+{active_run.status.value}+for+this+region.",
            status_code=303,
        )

    run = ScrapeRun(region_id=region_id)
    db.add(run)
    db.flush()
    for category_id in category_ids:
        db.add(RunCategory(run_id=run.id, category_id=category_id))
    db.commit()
    force_refresh_category_ids = category_ids if force_refresh == "1" else []
    run_scrape.send(run.id, force_refresh_category_ids=force_refresh_category_ids)
    return RedirectResponse(url=f"/?region_id={region_id}", status_code=303)


@router.post("/emails/{email_id}/status", response_class=HTMLResponse)
def update_email_status_html(
    request: Request,
    email_id: int,
    validation_status: ValidationStatus = Form(...),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    email = db.get(Email, email_id)
    email.validation_status = validation_status
    db.add(email)
    db.commit()
    return templates.TemplateResponse(
        request=request,
        name="partials/email_status.html",
        context={"email": email},
    )


@router.get("/categories", response_class=HTMLResponse)
def category_editor(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    categories = db.scalars(select(Category).order_by(Category.vertical, Category.label)).all()
    return templates.TemplateResponse(
        request=request,
        name="categories.html",
        context={"categories": categories},
    )


@router.get("/regions", response_class=HTMLResponse)
def region_editor(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    regions = db.scalars(select(Region).order_by(Region.country_code, Region.name)).all()
    return templates.TemplateResponse(
        request=request,
        name="regions.html",
        context={"regions": regions},
    )


@router.post("/regions/sync", response_class=HTMLResponse)
def sync_regions_html() -> RedirectResponse:
    sync_region_catalog_task.send()
    return RedirectResponse(url="/regions", status_code=303)


@router.post("/categories", response_class=HTMLResponse)
def create_category_html(
    slug: str = Form(...),
    label: str = Form(...),
    vertical: str = Form(...),
    osm_tags: str = Form(...),
    search_terms: str = Form(...),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    tag_pairs = []
    for row in osm_tags.splitlines():
        if "=" not in row:
            continue
        key, value = row.split("=", 1)
        tag_pairs.append({key.strip(): value.strip()})
    terms = [item.strip() for item in search_terms.split(",") if item.strip()]
    db.add(Category(slug=slug, label=label, vertical=Vertical(vertical), osm_tags=tag_pairs, search_terms=terms))
    db.commit()
    return RedirectResponse(url="/categories", status_code=303)


@router.post("/regions", response_class=HTMLResponse)
def create_region_html(
    code: str = Form(...),
    name: str = Form(...),
    country_code: str = Form(...),
    osm_admin_level: int = Form(2),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    db.add(
        Region(
            code=code.upper(),
            name=name,
            country_code=country_code.upper(),
            osm_admin_level=osm_admin_level,
        )
    )
    db.commit()
    return RedirectResponse(url="/regions", status_code=303)
