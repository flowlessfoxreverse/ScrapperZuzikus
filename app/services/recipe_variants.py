from __future__ import annotations

from dataclasses import replace
import hashlib

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import QueryRecipe, QueryRecipeValidation, QueryRecipeVariant
from app.services.recipe_drafts import DraftProposal


def prompt_fingerprint(prompt: str) -> str:
    normalized = " ".join(prompt.strip().lower().split())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:24]


def upsert_prompt_variants(session: Session, prompt: str, proposals: list[DraftProposal]) -> dict[str, QueryRecipeVariant]:
    fingerprint = prompt_fingerprint(prompt)
    existing = {
        variant.variant_key: variant
        for variant in session.scalars(
            select(QueryRecipeVariant).where(QueryRecipeVariant.prompt_fingerprint == fingerprint)
        ).all()
    }

    saved: dict[str, QueryRecipeVariant] = {}
    for proposal in proposals:
        variant = existing.get(proposal.variant_key)
        if variant is None:
            variant = QueryRecipeVariant(
                prompt_text=proposal.prompt,
                prompt_fingerprint=fingerprint,
                variant_key=proposal.variant_key,
                slug=proposal.slug,
                label=proposal.label,
                vertical=proposal.vertical,
                cluster_slug=proposal.cluster_slug,
            )
            session.add(variant)

        variant.prompt_text = proposal.prompt
        variant.slug = proposal.slug
        variant.label = proposal.label
        variant.vertical = proposal.vertical
        variant.cluster_slug = proposal.cluster_slug
        variant.template_key = proposal.template_key
        variant.sub_intent = proposal.sub_intent
        variant.source_strategy = proposal.source_strategy
        variant.template_score = proposal.template_score
        variant.prompt_match_score = proposal.prompt_match_score
        variant.rank_score = proposal.fit_score
        variant.fit_reasons = proposal.fit_reasons
        variant.rationale = proposal.rationale
        variant.osm_tags = proposal.osm_tags
        variant.exclude_tags = proposal.exclude_tags
        variant.search_terms = proposal.search_terms
        variant.website_keywords = proposal.website_keywords
        variant.language_hints = proposal.language_hints
        saved[proposal.variant_key] = variant

    session.flush()
    return saved


def _validation_bonus(score: int, validation_count: int) -> int:
    if score <= 0 or validation_count <= 0:
        return 0
    confidence_factor = min(validation_count, 5) / 5
    return round(score * 0.4 * confidence_factor)


def _adoption_bonus(adoption_count: int, cluster_adoption_count: int) -> int:
    variant_bonus = min(adoption_count, 6) * 2
    cluster_bonus = min(cluster_adoption_count, 12)
    return variant_bonus + cluster_bonus


def _production_bonus(score: int, run_count: int) -> int:
    if score <= 0 or run_count <= 0:
        return 0
    confidence_factor = min(run_count, 5) / 5
    return round(score * 0.3 * confidence_factor)


def apply_variant_history(session: Session, proposals: list[DraftProposal]) -> list[DraftProposal]:
    if not proposals:
        return proposals

    by_key: dict[str, list[QueryRecipeVariant]] = {}
    by_cluster: dict[str, list[QueryRecipeVariant]] = {}
    for variant in session.scalars(
        select(QueryRecipeVariant).where(
            QueryRecipeVariant.variant_key.in_([proposal.variant_key for proposal in proposals])
        )
    ).all():
        by_key.setdefault(variant.variant_key, []).append(variant)
    for variant in session.scalars(
        select(QueryRecipeVariant).where(
            QueryRecipeVariant.cluster_slug.in_(
                [proposal.cluster_slug for proposal in proposals if proposal.cluster_slug]
            )
        )
    ).all():
        if variant.cluster_slug:
            by_cluster.setdefault(variant.cluster_slug, []).append(variant)

    adoption_by_variant = {
        variant_id: recipe_count
        for variant_id, recipe_count in session.execute(
            select(QueryRecipe.source_variant_id, func.count(QueryRecipe.id))
            .where(
                QueryRecipe.source_variant_id.is_not(None),
                QueryRecipe.source_variant_id.in_([row.id for rows in by_key.values() for row in rows]),
            )
            .group_by(QueryRecipe.source_variant_id)
        ).all()
        if variant_id is not None
    }
    cluster_adoption_counts = {
        cluster_slug: recipe_count
        for cluster_slug, recipe_count in session.execute(
            select(QueryRecipeVariant.cluster_slug, func.count(QueryRecipe.id))
            .select_from(QueryRecipe)
            .join(QueryRecipeVariant, QueryRecipe.source_variant_id == QueryRecipeVariant.id)
            .where(
                QueryRecipeVariant.cluster_slug.in_(
                    [proposal.cluster_slug for proposal in proposals if proposal.cluster_slug]
                )
            )
            .group_by(QueryRecipeVariant.cluster_slug)
        ).all()
        if cluster_slug
    }

    adjusted: list[DraftProposal] = []
    for proposal in proposals:
        history_rows = by_key.get(proposal.variant_key, [])
        total_runs = sum(max(row.validation_count, 0) for row in history_rows)
        weighted_sum = sum(max(row.validation_count, 0) * max(row.observed_validation_score, 0) for row in history_rows)
        observed_score = round(weighted_sum / total_runs) if total_runs else 0
        validation_bonus = _validation_bonus(observed_score, total_runs)
        cluster_rows = by_cluster.get(proposal.cluster_slug or "", [])
        cluster_runs = sum(max(row.validation_count, 0) for row in cluster_rows)
        cluster_weighted_sum = sum(
            max(row.validation_count, 0) * max(row.observed_validation_score, 0) for row in cluster_rows
        )
        cluster_score = round(cluster_weighted_sum / cluster_runs) if cluster_runs else 0
        cluster_bonus = round(cluster_score * 0.15 * (min(cluster_runs, 8) / 8)) if cluster_runs else 0
        adoption_count = sum(adoption_by_variant.get(row.id, 0) for row in history_rows)
        cluster_adoption_count = cluster_adoption_counts.get(proposal.cluster_slug or "", 0)
        adoption_bonus = _adoption_bonus(adoption_count, cluster_adoption_count)
        production_score = round(
            sum(max(row.production_run_count, 0) * max(row.observed_production_score, 0) for row in history_rows)
            / sum(max(row.production_run_count, 0) for row in history_rows)
        ) if any(max(row.production_run_count, 0) for row in history_rows) else 0
        production_runs = sum(max(row.production_run_count, 0) for row in history_rows)
        production_bonus = _production_bonus(production_score, production_runs)
        fit_reasons = list(proposal.fit_reasons)
        if total_runs:
            fit_reasons.append(
                f"Historical validation score {observed_score}/100 across {total_runs} validation run(s)."
            )
        if cluster_runs:
            fit_reasons.append(
                f"Cluster baseline {cluster_score}/100 across {cluster_runs} validation run(s)."
            )
        if adoption_count:
            fit_reasons.append(
                f"Variant already reused in {adoption_count} recipe draft(s)."
            )
        if cluster_adoption_count:
            fit_reasons.append(
                f"Cluster already reused in {cluster_adoption_count} recipe draft(s)."
            )
        if production_runs:
            fit_reasons.append(
                f"Production yield score {production_score}/100 across {production_runs} completed production run(s)."
            )
        adjusted.append(
            replace(
                proposal,
                observed_validation_score=observed_score,
                historical_validation_count=total_runs,
                cluster_validation_score=cluster_score,
                cluster_validation_count=cluster_runs,
                variant_adoption_count=adoption_count,
                cluster_adoption_count=cluster_adoption_count,
                production_score=production_score,
                production_run_count=production_runs,
                fit_score=proposal.template_score + proposal.prompt_match_score + validation_bonus + cluster_bonus + adoption_bonus + production_bonus,
                fit_reasons=fit_reasons,
            )
        )

    adjusted.sort(
        key=lambda item: (
            -item.fit_score,
            -item.production_score,
            -item.observed_validation_score,
            -item.cluster_validation_score,
            item.label,
        )
    )
    return adjusted


def record_variant_validation(
    session: Session,
    recipe: QueryRecipe,
    validation: QueryRecipeValidation,
    metrics: dict,
) -> None:
    variant = recipe.source_variant
    if variant is None:
        return

    previous_count = max(variant.validation_count, 0)
    previous_score = max(variant.observed_validation_score, 0)
    new_score = validation.score or 0
    new_count = previous_count + 1
    observed_average = round(((previous_score * previous_count) + new_score) / new_count) if new_count else 0

    variant.validation_count = new_count
    variant.observed_validation_score = observed_average
    variant.latest_validation_score = validation.score
    variant.latest_validation_status = validation.status.value
    variant.latest_total_results = int(metrics.get("total_results", 0))
    variant.latest_website_rate = float(metrics.get("website_rate", 0) or 0)
    variant.last_validated_at = validation.created_at
    variant.rank_score = variant.template_score + variant.prompt_match_score + _validation_bonus(observed_average, new_count)
    session.add(variant)


def prompt_variant_recipe_map(session: Session, prompt: str) -> dict[str, str]:
    fingerprint = prompt_fingerprint(prompt)
    stmt = (
        select(QueryRecipeVariant.variant_key, QueryRecipe.slug)
        .join(QueryRecipe, QueryRecipe.source_variant_id == QueryRecipeVariant.id)
        .where(QueryRecipeVariant.prompt_fingerprint == fingerprint)
    )
    return {variant_key: recipe_slug for variant_key, recipe_slug in session.execute(stmt).all()}
