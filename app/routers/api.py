from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import Category, Email, Region, RunCategory, RunStatus, ScrapeRun, ValidationStatus
from app.schemas import CategoryCreate, CategoryOut, EmailStatusUpdate, RegionCreate, RegionOut, RunCreate, RunOut
from app.services.overpass import fetch_status_payload
from app.services.runs import find_active_run, request_run_cancellation
from app.tasks import run_scrape


router = APIRouter(prefix="/api", tags=["api"])


@router.get("/regions", response_model=list[RegionOut])
def list_regions(db: Session = Depends(get_db)) -> list[Region]:
    return db.scalars(select(Region).order_by(Region.name)).all()


@router.post("/regions", response_model=RegionOut)
def create_region(payload: RegionCreate, db: Session = Depends(get_db)) -> Region:
    region = Region(
        code=payload.code.upper(),
        name=payload.name,
        country_code=payload.country_code.upper(),
        osm_admin_level=payload.osm_admin_level,
    )
    db.add(region)
    db.commit()
    db.refresh(region)
    return region


@router.get("/categories", response_model=list[CategoryOut])
def list_categories(db: Session = Depends(get_db)) -> list[Category]:
    return db.scalars(select(Category).order_by(Category.vertical, Category.label)).all()


@router.post("/categories", response_model=CategoryOut)
def create_category(payload: CategoryCreate, db: Session = Depends(get_db)) -> Category:
    category = Category(
        slug=payload.slug,
        label=payload.label,
        vertical=payload.vertical,
        osm_tags=payload.osm_tags,
        search_terms=payload.search_terms,
    )
    db.add(category)
    db.commit()
    db.refresh(category)
    return category


@router.post("/runs", response_model=RunOut)
def create_run(payload: RunCreate, db: Session = Depends(get_db)) -> ScrapeRun:
    region = db.get(Region, payload.region_id)
    if region is None:
        raise HTTPException(status_code=404, detail="Region not found.")
    if not payload.category_ids:
        raise HTTPException(status_code=400, detail="Select at least one category.")
    active_run = find_active_run(db, payload.region_id)
    if active_run is not None:
        raise HTTPException(
            status_code=409,
            detail=f"Run {active_run.id} is already {active_run.status.value} for this region.",
        )

    run = ScrapeRun(region_id=payload.region_id)
    db.add(run)
    db.flush()

    for category_id in payload.category_ids:
        category = db.get(Category, category_id)
        if category is None:
            raise HTTPException(status_code=404, detail=f"Category {category_id} not found.")
        db.add(RunCategory(run_id=run.id, category_id=category_id))

    db.commit()
    db.refresh(run)
    run_scrape.send(run.id)
    return run


@router.post("/runs/{run_id}/cancel", response_model=dict[str, str])
def cancel_run(run_id: int, db: Session = Depends(get_db)) -> dict[str, str]:
    run = db.get(ScrapeRun, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found.")
    if run.status not in {RunStatus.PENDING, RunStatus.RUNNING}:
        raise HTTPException(status_code=409, detail=f"Run {run.id} is already {run.status.value}.")
    request_run_cancellation(db, run_id, "Stopped by user request.")
    db.commit()
    return {"status": "ok", "run_status": "cancel_requested"}


@router.patch("/emails/{email_id}", response_model=dict[str, str])
def update_email_status(email_id: int, payload: EmailStatusUpdate, db: Session = Depends(get_db)) -> dict[str, str]:
    email = db.get(Email, email_id)
    if email is None:
        raise HTTPException(status_code=404, detail="Email not found.")
    email.validation_status = payload.validation_status
    db.add(email)
    db.commit()
    return {"status": "ok", "validation_status": payload.validation_status.value}


@router.get("/system/overpass-status", response_model=dict)
def overpass_status() -> dict:
    return fetch_status_payload()
