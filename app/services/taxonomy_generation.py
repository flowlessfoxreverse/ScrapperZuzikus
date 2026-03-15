from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import re

from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.models import (
    NicheCluster,
    QueryRecipeVariantTemplate,
    QueryTaxonomyDraftCluster,
    QueryTaxonomyDraftVariantTemplate,
    QueryTaxonomyDraftVertical,
    QueryTaxonomyGeneration,
    RecipeSourceStrategy,
    TaxonomyDraftStatus,
    TaxonomyVertical,
)
from app.services.taxonomy import upsert_cluster, upsert_variant_template, upsert_vertical


settings = get_settings()
TAXONOMY_PROVIDER_OPTIONS = ("heuristic", "openai")


class PlannedTaxonomyVertical(BaseModel):
    slug: str
    label: str
    description: str | None = None
    rationale: list[str] = Field(default_factory=list)


class PlannedTaxonomyCluster(BaseModel):
    slug: str
    vertical_slug: str
    label: str
    description: str | None = None
    rationale: list[str] = Field(default_factory=list)


class PlannedTaxonomyVariantTemplate(BaseModel):
    template_key: str
    label: str
    vertical_slug: str
    cluster_slug: str | None = None
    sub_intent: str
    source_strategy: RecipeSourceStrategy
    aliases: list[str] = Field(default_factory=list)
    osm_tags: list[dict[str, str]] = Field(default_factory=list)
    exclude_tags: list[dict[str, str]] = Field(default_factory=list)
    search_terms: list[str] = Field(default_factory=list)
    website_keywords: list[str] = Field(default_factory=list)
    language_hints: list[str] = Field(default_factory=list)
    rationale: list[str] = Field(default_factory=list)
    template_score: int = 0


class PlannedTaxonomyPayload(BaseModel):
    prompt: str
    provider: str
    model_name: str
    summary: list[str] = Field(default_factory=list)
    verticals: list[PlannedTaxonomyVertical] = Field(default_factory=list)
    clusters: list[PlannedTaxonomyCluster] = Field(default_factory=list)
    variant_templates: list[PlannedTaxonomyVariantTemplate] = Field(default_factory=list)


@dataclass(frozen=True)
class TaxonomyGenerationResult:
    generation_id: int
    requested_provider: str
    provider: str
    model_name: str
    used_fallback: bool
    fallback_reason: str | None
    prompt_text: str
    focus_vertical_slug: str | None
    focus_cluster_slug: str | None
    payload: PlannedTaxonomyPayload


def _slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")[:64] or "draft"


def _keyify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")[:96] or "draft-template"


def _taxonomy_context_text(session: Session) -> str:
    verticals = session.scalars(
        select(TaxonomyVertical)
        .where(TaxonomyVertical.is_active.is_(True))
        .order_by(TaxonomyVertical.sort_order, TaxonomyVertical.label)
    ).all()
    clusters = session.scalars(
        select(NicheCluster)
        .where(NicheCluster.is_active.is_(True))
        .order_by(NicheCluster.vertical_slug, NicheCluster.sort_order, NicheCluster.label)
    ).all()
    templates = session.scalars(
        select(QueryRecipeVariantTemplate)
        .where(QueryRecipeVariantTemplate.is_active.is_(True))
        .order_by(QueryRecipeVariantTemplate.cluster_slug, QueryRecipeVariantTemplate.sort_order, QueryRecipeVariantTemplate.label)
    ).all()
    lines = ["Current active taxonomy:"]
    for vertical in verticals:
        lines.append(f"- vertical={vertical.slug} label={vertical.label}")
        vertical_clusters = [cluster for cluster in clusters if cluster.vertical_slug == vertical.slug]
        for cluster in vertical_clusters[:8]:
            lines.append(f"  - cluster={cluster.slug} label={cluster.label}")
            cluster_templates = [template for template in templates if template.cluster_slug == cluster.slug][:5]
            for template in cluster_templates:
                lines.append(
                    f"    - template={template.key} label={template.label} strategy={template.source_strategy.value}"
                )
    return "\n".join(lines)


def _heuristic_generation(
    session: Session,
    prompt: str,
    focus_vertical_slug: str | None,
    focus_cluster_slug: str | None,
    requested_model: str,
) -> tuple[PlannedTaxonomyPayload, str]:
    verticals: list[PlannedTaxonomyVertical] = []
    clusters: list[PlannedTaxonomyCluster] = []
    templates: list[PlannedTaxonomyVariantTemplate] = []

    if focus_vertical_slug:
        vertical = session.scalar(select(TaxonomyVertical).where(TaxonomyVertical.slug == focus_vertical_slug))
        if vertical is not None:
            verticals.append(
                PlannedTaxonomyVertical(
                    slug=vertical.slug,
                    label=vertical.label,
                    description=vertical.description,
                    rationale=["Expanded around an existing vertical focus."],
                )
            )
    else:
        new_vertical_slug = _slugify(prompt.split(" in ", 1)[0])
        verticals.append(
            PlannedTaxonomyVertical(
                slug=new_vertical_slug,
                label=prompt.strip().title()[:128],
                description=f"Draft taxonomy generated heuristically from prompt '{prompt.strip()}'.",
                rationale=["Created as a heuristic fallback because no LLM output was available."],
            )
        )
        focus_vertical_slug = new_vertical_slug

    if focus_cluster_slug:
        cluster = session.scalar(select(NicheCluster).where(NicheCluster.slug == focus_cluster_slug))
        if cluster is not None:
            clusters.append(
                PlannedTaxonomyCluster(
                    slug=cluster.slug,
                    vertical_slug=cluster.vertical_slug,
                    label=cluster.label,
                    description=cluster.description,
                    rationale=["Expanded around an existing cluster focus."],
                )
            )
    else:
        base_vertical = verticals[0].slug if verticals else (focus_vertical_slug or "general")
        cluster_slug = _slugify(prompt)
        clusters.append(
            PlannedTaxonomyCluster(
                slug=cluster_slug,
                vertical_slug=base_vertical,
                label=prompt.strip().title()[:128],
                description=f"Draft cluster generated heuristically from prompt '{prompt.strip()}'.",
                rationale=["Created as a heuristic fallback draft cluster."],
            )
        )
        focus_cluster_slug = cluster_slug

    base_cluster = clusters[0]
    for index, suffix in enumerate(("core", "premium", "specialty"), start=1):
        label = f"{base_cluster.label} {suffix.title()}"
        templates.append(
            PlannedTaxonomyVariantTemplate(
                template_key=_keyify(f"{base_cluster.slug}-{suffix}"),
                label=label[:128],
                vertical_slug=base_cluster.vertical_slug,
                cluster_slug=base_cluster.slug,
                sub_intent=suffix,
                source_strategy=RecipeSourceStrategy.OVERPASS_DISCOVERY_ENRICH,
                aliases=[label.lower(), prompt.lower()],
                search_terms=[label, prompt],
                website_keywords=[suffix, base_cluster.label.lower()],
                rationale=["Heuristic fallback template for taxonomy drafting."],
                template_score=max(40, 70 - (index * 5)),
            )
        )

    payload = PlannedTaxonomyPayload(
        prompt=prompt,
        provider="heuristic",
        model_name=requested_model,
        summary=["Heuristic taxonomy fallback used."],
        verticals=verticals,
        clusters=clusters,
        variant_templates=templates,
    )
    return payload, payload.model_dump_json()


def _openai_system_prompt(context_text: str, focus_vertical_slug: str | None, focus_cluster_slug: str | None) -> str:
    focus_lines = []
    if focus_vertical_slug:
        focus_lines.append(f"Focus vertical: {focus_vertical_slug}")
    if focus_cluster_slug:
        focus_lines.append(f"Focus cluster: {focus_cluster_slug}")
    focus_text = "\n".join(focus_lines) if focus_lines else "No existing focus was provided."
    return (
        "You are expanding an internal business taxonomy for a lead-generation scraping platform.\n"
        "Return only structured output matching the provided schema.\n"
        "Create draft taxonomy proposals, not final production truth.\n"
        "Prefer reusing existing vertical or cluster slugs when they already fit; create new slugs only when needed.\n"
        "Generate 1 to 3 vertical drafts, 2 to 8 cluster drafts, and 3 to 12 variant template drafts.\n"
        "Keep source_strategy realistic. Keep tags conservative and specific.\n"
        "Template scores must be integers from 0 to 100.\n"
        "Rationales should be short and concrete.\n\n"
        f"{focus_text}\n\n"
        f"{context_text}"
    )


def _openai_generation(
    session: Session,
    prompt: str,
    focus_vertical_slug: str | None,
    focus_cluster_slug: str | None,
    requested_model: str,
) -> tuple[PlannedTaxonomyPayload, str]:
    api_key = settings.taxonomy_generator_openai_api_key or settings.recipe_planner_openai_api_key
    if not api_key:
        raise RuntimeError("OpenAI taxonomy generator requires TAXONOMY_GENERATOR_OPENAI_API_KEY or RECIPE_PLANNER_OPENAI_API_KEY.")
    try:
        from openai import OpenAI
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("OpenAI taxonomy generator requires the 'openai' package to be installed.") from exc

    client = OpenAI(api_key=api_key, timeout=settings.taxonomy_generator_timeout_seconds)
    response = client.responses.parse(
        model=requested_model,
        input=[
            {
                "role": "system",
                "content": [{"type": "input_text", "text": _openai_system_prompt(_taxonomy_context_text(session), focus_vertical_slug, focus_cluster_slug)}],
            },
            {
                "role": "user",
                "content": [{"type": "input_text", "text": f"Expand the taxonomy for this admin prompt:\n{prompt}"}],
            },
        ],
        text_format=PlannedTaxonomyPayload,
    )
    parsed = response.output_parsed
    if parsed is None:
        raise RuntimeError("OpenAI taxonomy generator returned no parsed output.")
    parsed.prompt = prompt
    parsed.provider = "openai"
    parsed.model_name = requested_model
    raw_response = response.model_dump_json() if hasattr(response, "model_dump_json") else json.dumps(parsed.model_dump(mode="json"), sort_keys=True)
    return parsed, raw_response


def _run_provider(
    session: Session,
    prompt: str,
    focus_vertical_slug: str | None,
    focus_cluster_slug: str | None,
    requested_provider: str,
    requested_model: str,
) -> tuple[PlannedTaxonomyPayload, str, str, bool, str | None, str]:
    provider = (requested_provider or settings.taxonomy_generator_provider).strip().lower() or "openai"
    if provider == "openai":
        try:
            payload, raw_response = _openai_generation(
                session,
                prompt,
                focus_vertical_slug,
                focus_cluster_slug,
                requested_model,
            )
            return payload, "openai", requested_model, False, None, raw_response
        except Exception as exc:
            payload, raw_response = _heuristic_generation(
                session,
                prompt,
                focus_vertical_slug,
                focus_cluster_slug,
                settings.recipe_planner_model,
            )
            payload.provider = "heuristic"
            return payload, "heuristic", settings.recipe_planner_model, True, f"OpenAI taxonomy fallback: {exc}", raw_response
    payload, raw_response = _heuristic_generation(
        session,
        prompt,
        focus_vertical_slug,
        focus_cluster_slug,
        requested_model,
    )
    return payload, "heuristic", requested_model, False, None, raw_response


def generate_taxonomy_drafts(
    session: Session,
    prompt: str,
    *,
    requested_provider: str | None = None,
    requested_model: str | None = None,
    focus_vertical_slug: str | None = None,
    focus_cluster_slug: str | None = None,
) -> TaxonomyGenerationResult:
    prompt_text = prompt.strip()
    if not prompt_text:
        raise ValueError("Taxonomy prompt is required.")
    requested_provider_value = (requested_provider or settings.taxonomy_generator_provider).strip().lower() or "openai"
    requested_model_value = (requested_model or settings.taxonomy_generator_model).strip() or settings.taxonomy_generator_model
    payload, provider, model_name, used_fallback, fallback_reason, raw_response = _run_provider(
        session,
        prompt_text,
        focus_vertical_slug.strip() if focus_vertical_slug else None,
        focus_cluster_slug.strip() if focus_cluster_slug else None,
        requested_provider_value,
        requested_model_value,
    )
    generation = QueryTaxonomyGeneration(
        prompt_text=prompt_text,
        requested_provider=requested_provider_value,
        provider=provider,
        model_name=model_name,
        status="generated",
        focus_vertical_slug=focus_vertical_slug.strip() or None if focus_vertical_slug else None,
        focus_cluster_slug=focus_cluster_slug.strip() or None if focus_cluster_slug else None,
        raw_response=raw_response,
        parsed_output=payload.model_dump(mode="json"),
        used_fallback=used_fallback,
        fallback_reason=fallback_reason,
    )
    session.add(generation)
    session.flush()

    seen_verticals: set[str] = set()
    for draft in payload.verticals:
        if draft.slug in seen_verticals:
            continue
        seen_verticals.add(draft.slug)
        session.add(
            QueryTaxonomyDraftVertical(
                generation_id=generation.id,
                slug=draft.slug,
                label=draft.label,
                description=draft.description,
                rationale=draft.rationale,
                status=TaxonomyDraftStatus.DRAFT,
            )
        )

    seen_clusters: set[str] = set()
    for draft in payload.clusters:
        if draft.slug in seen_clusters:
            continue
        seen_clusters.add(draft.slug)
        session.add(
            QueryTaxonomyDraftCluster(
                generation_id=generation.id,
                vertical_slug=draft.vertical_slug,
                slug=draft.slug,
                label=draft.label,
                description=draft.description,
                rationale=draft.rationale,
                status=TaxonomyDraftStatus.DRAFT,
            )
        )

    seen_templates: set[str] = set()
    for draft in payload.variant_templates:
        if draft.template_key in seen_templates:
            continue
        seen_templates.add(draft.template_key)
        session.add(
            QueryTaxonomyDraftVariantTemplate(
                generation_id=generation.id,
                template_key=draft.template_key,
                label=draft.label,
                vertical_slug=draft.vertical_slug,
                cluster_slug=draft.cluster_slug,
                sub_intent=draft.sub_intent,
                source_strategy=draft.source_strategy,
                aliases=draft.aliases,
                osm_tags=draft.osm_tags,
                exclude_tags=draft.exclude_tags,
                search_terms=draft.search_terms,
                website_keywords=draft.website_keywords,
                language_hints=draft.language_hints,
                rationale=draft.rationale,
                template_score=draft.template_score,
                status=TaxonomyDraftStatus.DRAFT,
            )
        )
    session.flush()
    return TaxonomyGenerationResult(
        generation_id=generation.id,
        requested_provider=requested_provider_value,
        provider=provider,
        model_name=model_name,
        used_fallback=used_fallback,
        fallback_reason=fallback_reason,
        prompt_text=prompt_text,
        focus_vertical_slug=generation.focus_vertical_slug,
        focus_cluster_slug=generation.focus_cluster_slug,
        payload=payload,
    )


def approve_taxonomy_generation(session: Session, generation_id: int) -> None:
    generation = session.scalar(select(QueryTaxonomyGeneration).where(QueryTaxonomyGeneration.id == generation_id))
    if generation is None:
        raise ValueError("Taxonomy generation not found.")
    if generation.status == "rejected":
        raise ValueError("Rejected taxonomy generations cannot be approved.")

    verticals = session.scalars(
        select(QueryTaxonomyDraftVertical)
        .where(QueryTaxonomyDraftVertical.generation_id == generation_id)
        .order_by(QueryTaxonomyDraftVertical.slug)
    ).all()
    for index, draft in enumerate(verticals, start=1):
        vertical = upsert_vertical(
            session,
            slug=draft.slug,
            label=draft.label,
            description=draft.description,
            sort_order=1000 + index,
        )
        draft.status = TaxonomyDraftStatus.APPROVED
        draft.approved_vertical_slug = vertical.slug
        session.add(draft)

    clusters = session.scalars(
        select(QueryTaxonomyDraftCluster)
        .where(QueryTaxonomyDraftCluster.generation_id == generation_id)
        .order_by(QueryTaxonomyDraftCluster.vertical_slug, QueryTaxonomyDraftCluster.slug)
    ).all()
    for index, draft in enumerate(clusters, start=1):
        cluster = upsert_cluster(
            session,
            slug=draft.slug,
            vertical_slug=draft.vertical_slug,
            label=draft.label,
            description=draft.description,
            sort_order=1000 + index,
        )
        draft.status = TaxonomyDraftStatus.APPROVED
        draft.approved_cluster_slug = cluster.slug
        session.add(draft)

    templates = session.scalars(
        select(QueryTaxonomyDraftVariantTemplate)
        .where(QueryTaxonomyDraftVariantTemplate.generation_id == generation_id)
        .order_by(QueryTaxonomyDraftVariantTemplate.cluster_slug, QueryTaxonomyDraftVariantTemplate.template_key)
    ).all()
    for index, draft in enumerate(templates, start=1):
        template = upsert_variant_template(
            session,
            key=draft.template_key,
            label=draft.label,
            vertical=draft.vertical_slug,
            cluster_slug=draft.cluster_slug,
            sub_intent=draft.sub_intent,
            source_strategy=draft.source_strategy,
            aliases=list(draft.aliases or []),
            osm_tags=list(draft.osm_tags or []),
            exclude_tags=list(draft.exclude_tags or []),
            search_terms=list(draft.search_terms or []),
            website_keywords=list(draft.website_keywords or []),
            language_hints=list(draft.language_hints or []),
            rationale=list(draft.rationale or []),
            template_score=int(draft.template_score or 0),
            sort_order=1000 + index,
        )
        draft.status = TaxonomyDraftStatus.APPROVED
        draft.approved_template_key = template.key
        session.add(draft)

    generation.status = "approved"
    generation.updated_at = datetime.now(timezone.utc)
    session.add(generation)


def reject_taxonomy_generation(session: Session, generation_id: int) -> None:
    generation = session.scalar(select(QueryTaxonomyGeneration).where(QueryTaxonomyGeneration.id == generation_id))
    if generation is None:
        raise ValueError("Taxonomy generation not found.")
    for model in (QueryTaxonomyDraftVertical, QueryTaxonomyDraftCluster, QueryTaxonomyDraftVariantTemplate):
        rows = session.scalars(select(model).where(model.generation_id == generation_id)).all()
        for row in rows:
            row.status = TaxonomyDraftStatus.REJECTED
            session.add(row)
    generation.status = "rejected"
    generation.updated_at = datetime.now(timezone.utc)
    session.add(generation)
