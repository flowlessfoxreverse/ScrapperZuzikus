from __future__ import annotations

from dataclasses import replace
import hashlib

from sqlalchemy import case, func, select
from sqlalchemy.orm import Session

from app.models import QueryRecipe, QueryRecipePlanVariantOutcome, QueryRecipeValidation, QueryRecipeVariant, QueryRecipeVariantRunStat, Region
from app.services.recipe_drafts import DraftProposal
from app.services.recipe_prompt_normalization import normalize_prompt_text, resolve_prompt_country_code
from app.config import get_settings


settings = get_settings()


def prompt_fingerprint(prompt: str) -> str:
    normalized = normalize_prompt_text(prompt)
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


def _planner_conversion_bonus(selection_count: int, drafted_count: int, activated_count: int) -> int:
    selection_bonus = min(selection_count, 8)
    drafted_bonus = min(drafted_count, 8) * 2
    activated_bonus = min(activated_count, 8) * 4
    return selection_bonus + drafted_bonus + activated_bonus


def _production_bonus(score: int, run_count: int) -> int:
    if score <= 0 or run_count <= 0:
        return 0
    confidence_factor = min(run_count, 5) / 5
    return round(score * 0.3 * confidence_factor)


def _market_bonus(score: int, run_count: int) -> int:
    if score <= 0 or run_count <= 0:
        return 0
    confidence_factor = min(run_count, 4) / 4
    return round(score * 0.25 * confidence_factor)


def _strategy_bonus_from_outcomes(score: int, run_count: int) -> int:
    if score <= 0 or run_count <= 0:
        return 0
    confidence_factor = min(run_count, 6) / 6
    return round(score * 0.2 * confidence_factor)


def _source_strategy_thresholds(source_strategy) -> dict[str, int]:
    thresholds = {
        "validation_score": settings.recipe_activation_min_validation_score,
        "validation_runs": settings.recipe_activation_min_validation_runs,
        "production_score": settings.recipe_activation_min_production_score,
        "production_runs": settings.recipe_activation_min_production_runs,
    }
    if source_strategy is None:
        return thresholds

    overrides = {
        "overpass_discovery_enrich": {
            "validation_score": 55,
            "validation_runs": 1,
            "production_score": 0,
            "production_runs": 0,
        },
        "hybrid_discovery": {
            "validation_score": 58,
            "validation_runs": 1,
            "production_score": 5,
            "production_runs": 1,
        },
        "website_first": {
            "validation_score": 52,
            "validation_runs": 1,
            "production_score": 10,
            "production_runs": 1,
        },
        "browser_assisted_discovery": {
            "validation_score": 65,
            "validation_runs": 1,
            "production_score": 15,
            "production_runs": 1,
        },
        "directory_expansion": {
            "validation_score": 60,
            "validation_runs": 1,
            "production_score": 10,
            "production_runs": 1,
        },
    }
    strategy_value = getattr(source_strategy, "value", str(source_strategy))
    for key, value in overrides.get(strategy_value, {}).items():
        thresholds[key] = max(thresholds[key], value)
    return thresholds


def derive_recommendation_state(
    *,
    source_strategy,
    observed_validation_score: int,
    historical_validation_count: int,
    production_score: int,
    production_run_count: int,
    planner_selection_count: int,
    planner_draft_count: int,
    planner_activation_count: int,
    prompt_selection_count: int,
    prompt_draft_count: int,
    prompt_activation_count: int,
    market_production_score: int,
    market_production_run_count: int,
    strategy_production_score: int,
    strategy_production_run_count: int,
) -> tuple[str, int, list[str], int]:
    thresholds = _source_strategy_thresholds(source_strategy)
    reasons: list[str] = []
    recommendation_score = 0
    state = "experimental"

    validation_ready = (
        historical_validation_count >= thresholds["validation_runs"]
        and observed_validation_score >= thresholds["validation_score"]
    )
    production_required = thresholds["production_runs"] > 0
    production_ready = (
        not production_required
        or (
            production_run_count >= thresholds["production_runs"]
            and production_score >= thresholds["production_score"]
        )
    )
    activation_signal = planner_activation_count + prompt_activation_count
    draft_signal = planner_draft_count + prompt_draft_count
    selection_signal = planner_selection_count + prompt_selection_count

    if validation_ready:
        recommendation_score += 30
        reasons.append(
            f"Validation clears the {thresholds['validation_score']}/100 floor for {getattr(source_strategy, 'value', source_strategy)}."
        )
    elif historical_validation_count:
        gap = max(0, thresholds["validation_score"] - observed_validation_score)
        reasons.append(f"Validation is still {gap} point(s) below the promotion floor.")
    else:
        reasons.append("No validation evidence yet.")

    if production_run_count:
        recommendation_score += min(production_score // 5, 20)
        if production_ready:
            reasons.append("Production results meet the current strategy gate.")
        else:
            reasons.append("Production results exist, but are not yet strong enough for promotion.")
    elif production_required:
        reasons.append("No production evidence yet for a strategy that requires it.")

    if market_production_run_count:
        recommendation_score += min(market_production_score // 8, 10)
        reasons.append("Market-specific production evidence is available.")

    if strategy_production_run_count:
        recommendation_score += min(strategy_production_score // 10, 8)
        reasons.append("Source-strategy performance supports this variant.")

    if activation_signal:
        recommendation_score += min(activation_signal * 6, 18)
        reasons.append("Users have already activated this variant from planner output.")
    elif draft_signal:
        recommendation_score += min(draft_signal * 4, 12)
        reasons.append("Users repeatedly turn this variant into draft recipes.")
    elif selection_signal:
        recommendation_score += min(selection_signal * 2, 8)
        reasons.append("Users repeatedly select this variant during planning.")

    if (
        historical_validation_count >= max(2, thresholds["validation_runs"])
        and observed_validation_score < max(35, thresholds["validation_score"] - 15)
        and production_run_count >= max(1, thresholds["production_runs"])
        and production_score < max(5, thresholds["production_score"] - 5)
    ):
        state = "suppressed"
        recommendation_score = max(recommendation_score - 25, 0)
        reasons.append("Repeated validation and production evidence suggest this variant is weak.")
        return state, recommendation_score, reasons, -20

    if (
        validation_ready
        and historical_validation_count >= max(2, thresholds["validation_runs"])
        and production_ready
        and production_run_count >= max(1, thresholds["production_runs"])
        and (activation_signal >= 1 or draft_signal >= 2 or market_production_run_count >= 2)
    ):
        state = "trusted"
        reasons.append("This variant has both strong evidence and repeated downstream adoption.")
        return state, recommendation_score, reasons, 12

    if validation_ready and (production_ready or activation_signal >= 1 or draft_signal >= 1):
        state = "recommended"
        reasons.append("This variant has enough evidence to recommend, but not yet enough to fully trust.")
        return state, recommendation_score, reasons, 5

    reasons.append("Keep this variant visible while more validation or production evidence accumulates.")
    return state, recommendation_score, reasons, 0


def apply_variant_history(session: Session, proposals: list[DraftProposal]) -> list[DraftProposal]:
    if not proposals:
        return proposals

    prompt_market_country = resolve_prompt_country_code(session, proposals[0].prompt)

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
    planner_conversion_counts = {
        variant_key: (selected_count, drafted_count, activated_count)
        for variant_key, selected_count, drafted_count, activated_count in session.execute(
            select(
                QueryRecipePlanVariantOutcome.variant_key,
                func.count(case((QueryRecipePlanVariantOutcome.was_selected.is_(True), 1))).label("selected_count"),
                func.count(case((QueryRecipePlanVariantOutcome.was_drafted.is_(True), 1))).label("drafted_count"),
                func.count(case((QueryRecipePlanVariantOutcome.was_activated.is_(True), 1))).label("activated_count"),
            )
            .where(QueryRecipePlanVariantOutcome.variant_key.in_([proposal.variant_key for proposal in proposals]))
            .group_by(QueryRecipePlanVariantOutcome.variant_key)
        ).all()
    }
    template_keys = [proposal.template_key for proposal in proposals if proposal.template_key]
    cluster_strategy_pairs = {
        (proposal.cluster_slug, proposal.source_strategy.value)
        for proposal in proposals
        if proposal.cluster_slug
    }
    market_rows = []
    if prompt_market_country and template_keys:
        market_rows = session.execute(
            select(
                QueryRecipeVariant.template_key,
                func.count(QueryRecipeVariantRunStat.id),
                func.avg(QueryRecipeVariantRunStat.score),
            )
            .select_from(QueryRecipeVariantRunStat)
            .join(QueryRecipeVariant, QueryRecipeVariant.id == QueryRecipeVariantRunStat.variant_id)
            .join(Region, Region.id == QueryRecipeVariantRunStat.region_id)
            .where(
                QueryRecipeVariant.template_key.in_(template_keys),
                Region.country_code == prompt_market_country,
            )
            .group_by(QueryRecipeVariant.template_key)
        ).all()
    market_stats_by_template = {
        template_key: (int(run_count or 0), round(avg_score or 0))
        for template_key, run_count, avg_score in market_rows
        if template_key
    }
    strategy_rows = []
    if cluster_strategy_pairs:
        strategy_query = (
            select(
                QueryRecipeVariant.cluster_slug,
                QueryRecipeVariant.source_strategy,
                func.count(QueryRecipeVariantRunStat.id),
                func.avg(QueryRecipeVariantRunStat.score),
            )
            .select_from(QueryRecipeVariantRunStat)
            .join(QueryRecipeVariant, QueryRecipeVariant.id == QueryRecipeVariantRunStat.variant_id)
            .where(
                QueryRecipeVariant.cluster_slug.in_([pair[0] for pair in cluster_strategy_pairs]),
                QueryRecipeVariant.source_strategy.in_([pair[1] for pair in cluster_strategy_pairs]),
            )
            .group_by(QueryRecipeVariant.cluster_slug, QueryRecipeVariant.source_strategy)
        )
        if prompt_market_country:
            strategy_query = strategy_query.join(Region, Region.id == QueryRecipeVariantRunStat.region_id).where(
                Region.country_code == prompt_market_country
            )
        strategy_rows = session.execute(strategy_query).all()
    strategy_stats = {
        (
            cluster_slug,
            source_strategy.value if hasattr(source_strategy, "value") else str(source_strategy),
        ): (int(run_count or 0), round(avg_score or 0))
        for cluster_slug, source_strategy, run_count, avg_score in strategy_rows
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
        market_runs, market_score = market_stats_by_template.get(proposal.template_key, (0, 0))
        market_bonus = _market_bonus(market_score, market_runs)
        strategy_runs, strategy_score = strategy_stats.get(
            (proposal.cluster_slug or "", proposal.source_strategy.value),
            (0, 0),
        )
        strategy_bonus = _strategy_bonus_from_outcomes(strategy_score, strategy_runs)
        planner_selection_count, planner_draft_count, planner_activation_count = planner_conversion_counts.get(
            proposal.variant_key,
            (0, 0, 0),
        )
        planner_conversion_bonus = _planner_conversion_bonus(
            int(planner_selection_count or 0),
            int(planner_draft_count or 0),
            int(planner_activation_count or 0),
        )
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
        if planner_selection_count:
            fit_reasons.append(
                f"Historically selected in {planner_selection_count} planner run(s)."
            )
        if planner_draft_count:
            fit_reasons.append(
                f"Historically turned into {planner_draft_count} draft recipe(s) across planner runs."
            )
        if planner_activation_count:
            fit_reasons.append(
                f"Historically activated {planner_activation_count} time(s) across planner runs."
            )
        if production_runs:
            fit_reasons.append(
                f"Production yield score {production_score}/100 across {production_runs} completed production run(s)."
            )
        if market_runs and prompt_market_country:
            fit_reasons.append(
                f"{prompt_market_country} market yield {market_score}/100 across {market_runs} production run(s)."
            )
        if strategy_runs:
            fit_reasons.append(
                f"{proposal.source_strategy.value} strategy yield {strategy_score}/100 across {strategy_runs} production run(s)"
                + (f" in {prompt_market_country}." if prompt_market_country else ".")
            )
        recommendation_state, recommendation_state_score, recommendation_reasons, recommendation_bonus = (
            derive_recommendation_state(
                source_strategy=proposal.source_strategy,
                observed_validation_score=observed_score,
                historical_validation_count=total_runs,
                production_score=production_score,
                production_run_count=production_runs,
                planner_selection_count=int(planner_selection_count or 0),
                planner_draft_count=int(planner_draft_count or 0),
                planner_activation_count=int(planner_activation_count or 0),
                prompt_selection_count=proposal.prompt_selection_count,
                prompt_draft_count=proposal.prompt_draft_count,
                prompt_activation_count=proposal.prompt_activation_count,
                market_production_score=market_score,
                market_production_run_count=market_runs,
                strategy_production_score=strategy_score,
                strategy_production_run_count=strategy_runs,
            )
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
                planner_selection_count=int(planner_selection_count or 0),
                planner_draft_count=int(planner_draft_count or 0),
                planner_activation_count=int(planner_activation_count or 0),
                production_score=production_score,
                production_run_count=production_runs,
                market_country_code=prompt_market_country,
                market_production_score=market_score,
                market_production_run_count=market_runs,
                strategy_production_score=strategy_score,
                strategy_production_run_count=strategy_runs,
                fit_score=proposal.template_score + proposal.prompt_match_score + validation_bonus + cluster_bonus + adoption_bonus + planner_conversion_bonus + production_bonus + market_bonus + strategy_bonus + recommendation_bonus,
                fit_reasons=fit_reasons,
                recommendation_state=recommendation_state,
                recommendation_state_score=recommendation_state_score,
                recommendation_reasons=recommendation_reasons,
            )
        )

    adjusted.sort(
        key=lambda item: (
            -item.fit_score,
            item.recommendation_state != "trusted",
            item.recommendation_state == "suppressed",
            -item.planner_activation_count,
            -item.planner_draft_count,
            -item.market_production_score,
            -item.strategy_production_score,
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
