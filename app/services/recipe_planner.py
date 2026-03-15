from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json

from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.models import (
    DailyUsage,
    NicheCluster,
    QueryRecipePlan,
    QueryRecipeVariantTemplate,
    RecipeAdapter,
    RecipeSourceStrategy,
    TaxonomyVertical,
)
from app.services.recipe_clusters import apply_cluster_decision_history
from app.services.recipe_drafts import (
    ClusterCandidate,
    DraftProposal,
    analyze_prompt_clusters,
    build_draft_variants_from_prompt,
)
from app.services.recipe_prompt_variants import apply_prompt_variant_history
from app.services.recipe_variants import apply_variant_history, prompt_fingerprint


settings = get_settings()
PLANNER_VERSION = "v1"


class PlannedClusterCandidate(BaseModel):
    vertical: str
    cluster_slug: str
    score: int
    matched_aliases: list[str] = Field(default_factory=list)
    rationale: list[str] = Field(default_factory=list)


class PlannedVariant(BaseModel):
    prompt: str
    slug: str
    label: str
    description: str
    vertical: str
    cluster_slug: str | None = None
    adapter: RecipeAdapter
    source_strategy: RecipeSourceStrategy
    template_key: str
    sub_intent: str
    osm_tags: list[dict[str, str]] = Field(default_factory=list)
    exclude_tags: list[dict[str, str]] = Field(default_factory=list)
    search_terms: list[str] = Field(default_factory=list)
    website_keywords: list[str] = Field(default_factory=list)
    language_hints: list[str] = Field(default_factory=list)
    rationale: list[str] = Field(default_factory=list)
    variant_key: str
    template_score: int
    prompt_match_score: int
    fit_score: int
    fit_reasons: list[str] = Field(default_factory=list)
    observed_validation_score: int = 0
    historical_validation_count: int = 0
    cluster_validation_score: int = 0
    cluster_validation_count: int = 0
    variant_adoption_count: int = 0
    cluster_adoption_count: int = 0
    planner_selection_count: int = 0
    planner_draft_count: int = 0
    planner_activation_count: int = 0
    prompt_selection_count: int = 0
    prompt_draft_count: int = 0
    prompt_activation_count: int = 0
    production_score: int = 0
    production_run_count: int = 0
    market_country_code: str | None = None
    market_production_score: int = 0
    market_production_run_count: int = 0
    strategy_production_score: int = 0
    strategy_production_run_count: int = 0
    recommendation_state: str = "experimental"
    recommendation_state_score: int = 0
    recommendation_reasons: list[str] = Field(default_factory=list)


class PlannedPromptPayload(BaseModel):
    prompt: str
    provider: str
    model_name: str
    planner_version: str
    cluster_choice: PlannedClusterCandidate
    alternate_clusters: list[PlannedClusterCandidate] = Field(default_factory=list)
    variants: list[PlannedVariant] = Field(default_factory=list)
    default_variant_key: str | None = None


@dataclass(frozen=True)
class RecipePromptPlanResult:
    prompt: str
    requested_provider: str
    requested_model: str
    provider: str
    model_name: str
    planner_version: str
    cache_hit: bool
    cache_expires_at: datetime | None
    used_fallback: bool
    fallback_reason: str | None
    plan_id: int | None
    cluster_choice: ClusterCandidate
    alternate_clusters: list[ClusterCandidate]
    draft_variants: list[DraftProposal]
    draft_proposal: DraftProposal


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _provider_name(requested_provider: str | None = None) -> str:
    value = requested_provider if requested_provider is not None else settings.recipe_planner_provider
    return value.strip().lower() or "heuristic"


def _provider_model(requested_provider: str, requested_model: str | None = None) -> str:
    if requested_model and requested_model.strip():
        return requested_model.strip()
    if requested_provider == "openai":
        return settings.recipe_planner_openai_model
    return settings.recipe_planner_model


def _planner_cache_key(prompt: str, requested_provider: str, requested_model: str) -> str:
    payload = {
        "prompt_fingerprint": prompt_fingerprint(prompt),
        "requested_provider": requested_provider,
        "requested_model": requested_model,
        "planner_version": PLANNER_VERSION,
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _get_cached_plan(session: Session, cache_key: str) -> QueryRecipePlan | None:
    now = _utcnow()
    stmt = (
        select(QueryRecipePlan)
        .where(
            QueryRecipePlan.cache_key == cache_key,
            QueryRecipePlan.status == "success",
            QueryRecipePlan.expires_at.is_not(None),
            QueryRecipePlan.expires_at > now,
        )
        .order_by(QueryRecipePlan.created_at.desc())
        .limit(1)
    )
    return session.scalar(stmt)


def _get_usage_row(session: Session, provider: str) -> DailyUsage:
    today = _utcnow().date()
    usage = session.scalar(
        select(DailyUsage).where(
            DailyUsage.usage_date == today,
            DailyUsage.provider == f"recipe_planner_{provider}",
        )
    )
    if usage is None:
        usage = DailyUsage(
            usage_date=today,
            provider=f"recipe_planner_{provider}",
            units_used=0,
            cap=settings.recipe_planner_daily_cap,
            metadata_json={},
        )
        session.add(usage)
        session.flush()
    return usage


def _check_quota(session: Session, provider: str) -> None:
    if settings.recipe_planner_daily_cap <= 0:
        return
    usage = _get_usage_row(session, provider)
    if usage.units_used >= usage.cap:
        raise RuntimeError("Recipe planner daily quota reached.")


def _increment_quota(session: Session, provider: str) -> None:
    if settings.recipe_planner_daily_cap <= 0:
        return
    usage = _get_usage_row(session, provider)
    usage.units_used += 1
    session.add(usage)


def _cluster_to_model(candidate: ClusterCandidate) -> PlannedClusterCandidate:
    return PlannedClusterCandidate(
        vertical=candidate.vertical,
        cluster_slug=candidate.cluster_slug,
        score=candidate.score,
        matched_aliases=list(candidate.matched_aliases),
        rationale=list(candidate.rationale),
    )


def _variant_to_model(proposal: DraftProposal) -> PlannedVariant:
    return PlannedVariant(**proposal.__dict__)


def _model_to_cluster(candidate: PlannedClusterCandidate) -> ClusterCandidate:
    return ClusterCandidate(
        vertical=candidate.vertical,
        cluster_slug=candidate.cluster_slug,
        score=candidate.score,
        matched_aliases=tuple(candidate.matched_aliases),
        rationale=list(candidate.rationale),
    )


def _model_to_variant(variant: PlannedVariant) -> DraftProposal:
    return DraftProposal(
        prompt=variant.prompt,
        slug=variant.slug,
        label=variant.label,
        description=variant.description,
        vertical=variant.vertical,
        cluster_slug=variant.cluster_slug,
        adapter=variant.adapter,
        source_strategy=variant.source_strategy,
        template_key=variant.template_key,
        sub_intent=variant.sub_intent,
        osm_tags=variant.osm_tags,
        exclude_tags=variant.exclude_tags,
        search_terms=variant.search_terms,
        website_keywords=variant.website_keywords,
        language_hints=variant.language_hints,
        rationale=variant.rationale,
        variant_key=variant.variant_key,
        template_score=variant.template_score,
        prompt_match_score=variant.prompt_match_score,
        fit_score=variant.fit_score,
        fit_reasons=variant.fit_reasons,
        observed_validation_score=variant.observed_validation_score,
        historical_validation_count=variant.historical_validation_count,
        cluster_validation_score=variant.cluster_validation_score,
        cluster_validation_count=variant.cluster_validation_count,
        variant_adoption_count=variant.variant_adoption_count,
        cluster_adoption_count=variant.cluster_adoption_count,
        planner_selection_count=variant.planner_selection_count,
        planner_draft_count=variant.planner_draft_count,
        planner_activation_count=variant.planner_activation_count,
        prompt_selection_count=variant.prompt_selection_count,
        prompt_draft_count=variant.prompt_draft_count,
        prompt_activation_count=variant.prompt_activation_count,
        production_score=variant.production_score,
        production_run_count=variant.production_run_count,
        market_country_code=variant.market_country_code,
        market_production_score=variant.market_production_score,
        market_production_run_count=variant.market_production_run_count,
        strategy_production_score=variant.strategy_production_score,
        strategy_production_run_count=variant.strategy_production_run_count,
        recommendation_state=variant.recommendation_state,
        recommendation_state_score=variant.recommendation_state_score,
        recommendation_reasons=variant.recommendation_reasons,
    )


def _run_heuristic_provider(session: Session, prompt: str, requested_model: str) -> tuple[PlannedPromptPayload, str]:
    cluster_choice, alternate_clusters = analyze_prompt_clusters(prompt)
    variants = build_draft_variants_from_prompt(prompt, session=session)
    payload = PlannedPromptPayload(
        prompt=prompt,
        provider="heuristic",
        model_name=requested_model,
        planner_version=PLANNER_VERSION,
        cluster_choice=_cluster_to_model(cluster_choice),
        alternate_clusters=[_cluster_to_model(candidate) for candidate in alternate_clusters],
        variants=[_variant_to_model(proposal) for proposal in variants],
        default_variant_key=variants[0].variant_key if variants else None,
    )
    return payload, payload.model_dump_json()


def _taxonomy_context_text(session: Session) -> str:
    verticals = session.scalars(
        select(TaxonomyVertical).where(TaxonomyVertical.is_active.is_(True)).order_by(TaxonomyVertical.sort_order, TaxonomyVertical.label)
    ).all()
    clusters = session.scalars(
        select(NicheCluster).where(NicheCluster.is_active.is_(True)).order_by(NicheCluster.vertical_slug, NicheCluster.sort_order, NicheCluster.label)
    ).all()
    templates = session.scalars(
        select(QueryRecipeVariantTemplate)
        .where(QueryRecipeVariantTemplate.is_active.is_(True))
        .order_by(
            QueryRecipeVariantTemplate.cluster_slug,
            QueryRecipeVariantTemplate.sort_order.desc(),
            QueryRecipeVariantTemplate.template_score.desc(),
        )
    ).all()

    cluster_by_vertical: dict[str, list[NicheCluster]] = {}
    for cluster in clusters:
        cluster_by_vertical.setdefault(cluster.vertical_slug, []).append(cluster)

    templates_by_cluster: dict[str, list[QueryRecipeVariantTemplate]] = {}
    for template in templates:
        if template.cluster_slug is None:
            continue
        bucket = templates_by_cluster.setdefault(template.cluster_slug, [])
        if len(bucket) < 6:
            bucket.append(template)

    lines = ["Available taxonomy:"]
    for vertical in verticals:
        lines.append(f"- vertical={vertical.slug} label={vertical.label}")
        for cluster in cluster_by_vertical.get(vertical.slug, []):
            lines.append(f"  - cluster={cluster.slug} label={cluster.label}")
            for template in templates_by_cluster.get(cluster.slug, []):
                alias_text = ", ".join(template.aliases[:4]) if template.aliases else "-"
                tag_text = ", ".join(
                    f"{key}={value}"
                    for tag in template.osm_tags[:3]
                    for key, value in tag.items()
                ) or "-"
                lines.append(
                    f"    - template={template.key} label={template.label} "
                    f"strategy={template.source_strategy.value} aliases={alias_text} tags={tag_text}"
                )
    lines.append("Allowed source_strategy values:")
    for strategy in RecipeSourceStrategy:
        lines.append(f"- {strategy.value}")
    lines.append("Allowed adapter values:")
    for adapter in RecipeAdapter:
        lines.append(f"- {adapter.value}")
    return "\n".join(lines)


def _openai_system_prompt(context_text: str) -> str:
    return (
        "You are a recipe planner for a lead-generation scraping platform.\n"
        "Return only structured recipe planning output using the provided schema.\n"
        "Choose exactly one primary cluster and up to three alternate clusters.\n"
        "Generate 3 to 8 candidate variants.\n"
        "Use only listed verticals, clusters, source strategies, and adapters.\n"
        "Prefer existing template keys when they fit; otherwise create a short kebab-case template_key.\n"
        "Keep OSM tags realistic and specific. Avoid invented high-confidence tags when uncertain.\n"
        "Scores must be integers from 0 to 100.\n"
        "Template score reflects intrinsic template quality; prompt match reflects fit to the user prompt; fit score is the combined ranking signal.\n"
        "Rationales and fit reasons should be short, concrete sentences.\n\n"
        f"{context_text}"
    )


def _run_openai_provider(session: Session, prompt: str, requested_model: str) -> tuple[PlannedPromptPayload, str, str, bool, str | None, str]:
    api_key = settings.recipe_planner_openai_api_key
    if not api_key:
        raise RuntimeError("OpenAI planner is configured but RECIPE_PLANNER_OPENAI_API_KEY is not set.")

    try:
        from openai import OpenAI
    except ImportError as exc:  # pragma: no cover - dependency check
        raise RuntimeError("OpenAI planner requires the 'openai' package to be installed.") from exc

    client = OpenAI(api_key=api_key, timeout=settings.recipe_planner_timeout_seconds)
    response = client.responses.parse(
        model=requested_model,
        input=[
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": _openai_system_prompt(_taxonomy_context_text(session)),
                    }
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": f"Build recipe variants for this prompt:\n{prompt}",
                    }
                ],
            },
        ],
        text_format=PlannedPromptPayload,
    )
    parsed = response.output_parsed
    if parsed is None:
        raise RuntimeError("OpenAI planner returned no parsed output.")
    parsed.prompt = prompt
    parsed.provider = "openai"
    parsed.model_name = requested_model
    parsed.planner_version = PLANNER_VERSION
    raw_response = response.model_dump_json() if hasattr(response, "model_dump_json") else json.dumps(parsed.model_dump(mode="json"), sort_keys=True)
    return parsed, "openai", requested_model, False, None, raw_response


def _run_provider(session: Session, prompt: str, requested_provider: str, requested_model: str) -> tuple[PlannedPromptPayload, str, str, bool, str | None, str]:
    if requested_provider == "heuristic":
        payload, raw_response = _run_heuristic_provider(session, prompt, requested_model)
        return payload, "heuristic", requested_model, False, None, raw_response
    if requested_provider == "openai":
        try:
            return _run_openai_provider(session, prompt, requested_model)
        except Exception as exc:
            heuristic_model = _provider_model("heuristic")
            payload, raw_response = _run_heuristic_provider(session, prompt, heuristic_model)
            payload.provider = "heuristic"
            return (
                payload,
                "heuristic",
                heuristic_model,
                True,
                f"OpenAI planner fallback: {exc}",
                raw_response,
            )

    fallback_reason = f"Planner provider '{requested_provider}' is not configured yet; used heuristic fallback."
    heuristic_model = _provider_model("heuristic")
    payload, raw_response = _run_heuristic_provider(session, prompt, heuristic_model)
    payload.provider = "heuristic"
    return payload, "heuristic", heuristic_model, True, fallback_reason, raw_response


def _persist_plan(
    session: Session,
    *,
    prompt: str,
    cache_key: str,
    requested_provider: str,
    payload: PlannedPromptPayload,
    actual_provider: str,
    model_name: str,
    used_fallback: bool,
    fallback_reason: str | None,
    raw_response: str,
) -> QueryRecipePlan:
    plan = QueryRecipePlan(
        prompt_text=prompt,
        prompt_fingerprint=prompt_fingerprint(prompt),
        requested_provider=requested_provider,
        provider=actual_provider,
        model_name=model_name,
        planner_version=PLANNER_VERSION,
        status="success",
        cache_key=cache_key,
        raw_response=raw_response,
        parsed_output=payload.model_dump(mode="json"),
        used_fallback=used_fallback,
        fallback_reason=fallback_reason,
        expires_at=_utcnow() + timedelta(hours=settings.recipe_planner_cache_hours),
    )
    session.add(plan)
    session.flush()
    return plan


def _persist_plan_error(
    session: Session,
    *,
    prompt: str,
    cache_key: str,
    requested_provider: str,
    error_text: str,
) -> QueryRecipePlan:
    plan = QueryRecipePlan(
        prompt_text=prompt,
        prompt_fingerprint=prompt_fingerprint(prompt),
        requested_provider=requested_provider,
        provider=requested_provider,
        model_name=settings.recipe_planner_model,
        planner_version=PLANNER_VERSION,
        status="error",
        cache_key=cache_key,
        raw_response=None,
        parsed_output={},
        error_text=error_text,
        expires_at=None,
    )
    session.add(plan)
    session.flush()
    return plan


def plan_recipe_prompt(
    session: Session,
    prompt: str,
    *,
    selected_variant_slug: str | None = None,
    requested_provider: str | None = None,
    requested_model: str | None = None,
) -> RecipePromptPlanResult:
    prompt_text = prompt.strip()
    if not prompt_text:
        raise ValueError("Prompt is required.")

    requested_provider = _provider_name(requested_provider)
    requested_model = _provider_model(requested_provider, requested_model)
    cache_key = _planner_cache_key(prompt_text, requested_provider, requested_model)
    cached = _get_cached_plan(session, cache_key)
    if cached is not None:
        payload = PlannedPromptPayload.model_validate(cached.parsed_output)
        cache_hit = True
        plan_id = cached.id
        cache_expires_at = cached.expires_at
        used_fallback = cached.used_fallback
        fallback_reason = cached.fallback_reason
        actual_provider = cached.provider
        model_name = cached.model_name
    else:
        try:
            _check_quota(session, requested_provider)
            payload, actual_provider, model_name, used_fallback, fallback_reason, raw_response = _run_provider(
                session, prompt_text, requested_provider, requested_model
            )
            _increment_quota(session, requested_provider)
            persisted = _persist_plan(
                session,
                prompt=prompt_text,
                cache_key=cache_key,
                requested_provider=requested_provider,
                payload=payload,
                actual_provider=actual_provider,
                model_name=model_name,
                used_fallback=used_fallback,
                fallback_reason=fallback_reason,
                raw_response=raw_response,
            )
        except Exception as exc:
            _persist_plan_error(
                session,
                prompt=prompt_text,
                cache_key=cache_key,
                requested_provider=requested_provider,
                error_text=str(exc),
            )
            raise
        cache_hit = False
        plan_id = persisted.id
        cache_expires_at = persisted.expires_at

    cluster_choice = _model_to_cluster(payload.cluster_choice)
    alternate_clusters = [_model_to_cluster(candidate) for candidate in payload.alternate_clusters]
    cluster_choice, alternate_clusters = apply_cluster_decision_history(
        session, prompt_text, cluster_choice, alternate_clusters
    )

    draft_variants = [_model_to_variant(variant) for variant in payload.variants]
    draft_variants = apply_prompt_variant_history(session, prompt_text, draft_variants)
    draft_variants = apply_variant_history(session, draft_variants)
    if not draft_variants:
        raise ValueError("No recipe variants were generated for this prompt.")

    selected_key = selected_variant_slug or payload.default_variant_key or draft_variants[0].variant_key
    draft_proposal = next(
        (proposal for proposal in draft_variants if proposal.variant_key == selected_key),
        draft_variants[0],
    )
    return RecipePromptPlanResult(
        prompt=prompt_text,
        requested_provider=requested_provider,
        requested_model=requested_model,
        provider=actual_provider,
        model_name=model_name,
        planner_version=PLANNER_VERSION,
        cache_hit=cache_hit,
        cache_expires_at=cache_expires_at,
        used_fallback=used_fallback,
        fallback_reason=fallback_reason,
        plan_id=plan_id,
        cluster_choice=cluster_choice,
        alternate_clusters=alternate_clusters,
        draft_variants=draft_variants,
        draft_proposal=draft_proposal,
    )
