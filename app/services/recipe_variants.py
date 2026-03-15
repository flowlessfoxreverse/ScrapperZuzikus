from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib

from sqlalchemy import case, func, select
from sqlalchemy.orm import Session

from app.models import (
    QueryRecipe,
    QueryRecipePlanVariantOutcome,
    QueryRecipeRecommendationPolicy,
    QueryRecipeValidation,
    QueryRecipeVariant,
    QueryRecipeVariantRunStat,
    RecipeSourceStrategy,
    Region,
)
from app.services.recipe_drafts import DraftProposal
from app.services.recipe_prompt_normalization import normalize_prompt_text, resolve_prompt_country_code
from app.config import get_settings


settings = get_settings()


@dataclass(frozen=True)
class RecommendationDecision:
    state: str
    score: int
    reasons: list[str]
    rank_bonus: int
    policy_key: str
    policy_label: str
    blockers: list[str]


DEFAULT_RECOMMENDATION_POLICIES: dict[str, dict[str, object]] = {
    "global": {
        "label": "Global Baseline",
        "source_strategy": None,
        "recommended_validation_score": 55,
        "recommended_validation_runs": 1,
        "recommended_production_score": 0,
        "recommended_production_runs": 0,
        "recommended_activation_count": 0,
        "trusted_validation_score": 65,
        "trusted_validation_runs": 2,
        "trusted_production_score": 15,
        "trusted_production_runs": 1,
        "trusted_activation_count": 1,
        "suppression_validation_score_max": 40,
        "suppression_validation_runs_min": 2,
        "suppression_production_score_max": 5,
        "suppression_production_runs_min": 1,
    },
    "overpass_discovery_enrich": {
        "label": "Overpass Discovery",
        "source_strategy": "overpass_discovery_enrich",
        "recommended_validation_score": 55,
        "recommended_validation_runs": 1,
        "recommended_production_score": 0,
        "recommended_production_runs": 0,
        "recommended_activation_count": 0,
        "trusted_validation_score": 65,
        "trusted_validation_runs": 2,
        "trusted_production_score": 10,
        "trusted_production_runs": 1,
        "trusted_activation_count": 1,
        "suppression_validation_score_max": 38,
        "suppression_validation_runs_min": 2,
        "suppression_production_score_max": 5,
        "suppression_production_runs_min": 1,
    },
    "hybrid_discovery": {
        "label": "Hybrid Discovery",
        "source_strategy": "hybrid_discovery",
        "recommended_validation_score": 58,
        "recommended_validation_runs": 1,
        "recommended_production_score": 5,
        "recommended_production_runs": 1,
        "recommended_activation_count": 0,
        "trusted_validation_score": 68,
        "trusted_validation_runs": 2,
        "trusted_production_score": 18,
        "trusted_production_runs": 1,
        "trusted_activation_count": 1,
        "suppression_validation_score_max": 40,
        "suppression_validation_runs_min": 2,
        "suppression_production_score_max": 5,
        "suppression_production_runs_min": 1,
    },
    "website_first": {
        "label": "Website First",
        "source_strategy": "website_first",
        "recommended_validation_score": 52,
        "recommended_validation_runs": 1,
        "recommended_production_score": 10,
        "recommended_production_runs": 1,
        "recommended_activation_count": 0,
        "trusted_validation_score": 62,
        "trusted_validation_runs": 2,
        "trusted_production_score": 20,
        "trusted_production_runs": 1,
        "trusted_activation_count": 1,
        "suppression_validation_score_max": 40,
        "suppression_validation_runs_min": 2,
        "suppression_production_score_max": 8,
        "suppression_production_runs_min": 1,
    },
    "browser_assisted_discovery": {
        "label": "Browser Assisted",
        "source_strategy": "browser_assisted_discovery",
        "recommended_validation_score": 65,
        "recommended_validation_runs": 1,
        "recommended_production_score": 15,
        "recommended_production_runs": 1,
        "recommended_activation_count": 1,
        "trusted_validation_score": 75,
        "trusted_validation_runs": 2,
        "trusted_production_score": 25,
        "trusted_production_runs": 2,
        "trusted_activation_count": 1,
        "suppression_validation_score_max": 45,
        "suppression_validation_runs_min": 2,
        "suppression_production_score_max": 10,
        "suppression_production_runs_min": 1,
    },
    "directory_expansion": {
        "label": "Directory Expansion",
        "source_strategy": "directory_expansion",
        "recommended_validation_score": 60,
        "recommended_validation_runs": 1,
        "recommended_production_score": 10,
        "recommended_production_runs": 1,
        "recommended_activation_count": 0,
        "trusted_validation_score": 70,
        "trusted_validation_runs": 2,
        "trusted_production_score": 20,
        "trusted_production_runs": 1,
        "trusted_activation_count": 1,
        "suppression_validation_score_max": 40,
        "suppression_validation_runs_min": 2,
        "suppression_production_score_max": 8,
        "suppression_production_runs_min": 1,
    },
}


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


def ensure_default_recommendation_policies(session: Session) -> dict[str, QueryRecipeRecommendationPolicy]:
    existing = {
        row.policy_key: row
        for row in session.scalars(select(QueryRecipeRecommendationPolicy)).all()
    }
    changed = False
    for policy_key, defaults in DEFAULT_RECOMMENDATION_POLICIES.items():
        row = existing.get(policy_key)
        if row is None:
            row = QueryRecipeRecommendationPolicy(
                policy_key=policy_key,
                label=str(defaults["label"]),
                source_strategy=RecipeSourceStrategy(str(defaults["source_strategy"])) if defaults["source_strategy"] else None,
                recommended_validation_score=int(defaults["recommended_validation_score"]),
                recommended_validation_runs=int(defaults["recommended_validation_runs"]),
                recommended_production_score=int(defaults["recommended_production_score"]),
                recommended_production_runs=int(defaults["recommended_production_runs"]),
                recommended_activation_count=int(defaults["recommended_activation_count"]),
                trusted_validation_score=int(defaults["trusted_validation_score"]),
                trusted_validation_runs=int(defaults["trusted_validation_runs"]),
                trusted_production_score=int(defaults["trusted_production_score"]),
                trusted_production_runs=int(defaults["trusted_production_runs"]),
                trusted_activation_count=int(defaults["trusted_activation_count"]),
                suppression_validation_score_max=int(defaults["suppression_validation_score_max"]),
                suppression_validation_runs_min=int(defaults["suppression_validation_runs_min"]),
                suppression_production_score_max=int(defaults["suppression_production_score_max"]),
                suppression_production_runs_min=int(defaults["suppression_production_runs_min"]),
                is_active=True,
            )
            session.add(row)
            existing[policy_key] = row
            changed = True
    if changed:
        session.flush()
    return existing


def recommendation_policy_map(session: Session) -> dict[str, QueryRecipeRecommendationPolicy]:
    return ensure_default_recommendation_policies(session)


def resolve_recommendation_policy(policy_map: dict[str, QueryRecipeRecommendationPolicy], source_strategy) -> QueryRecipeRecommendationPolicy | None:
    strategy_value = getattr(source_strategy, "value", str(source_strategy)) if source_strategy else None
    if strategy_value and strategy_value in policy_map and policy_map[strategy_value].is_active:
        return policy_map[strategy_value]
    global_policy = policy_map.get("global")
    if global_policy and global_policy.is_active:
        return global_policy
    return None


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
) -> RecommendationDecision:
    policy = None
    thresholds = _source_strategy_thresholds(source_strategy)
    if isinstance(source_strategy, QueryRecipeRecommendationPolicy):
        policy = source_strategy
    elif hasattr(source_strategy, "recommended_validation_score"):
        policy = source_strategy
    if policy is not None:
        thresholds = {
            "validation_score": int(policy.recommended_validation_score),
            "validation_runs": int(policy.recommended_validation_runs),
            "production_score": int(policy.recommended_production_score),
            "production_runs": int(policy.recommended_production_runs),
        }
    reasons: list[str] = []
    blockers: list[str] = []
    recommendation_score = 0
    state = "experimental"
    policy_key = str(getattr(policy, "policy_key", getattr(source_strategy, "value", "global") or "global"))
    policy_label = str(getattr(policy, "label", getattr(source_strategy, "value", "Global Baseline") or "Global Baseline"))

    recommended_validation_score = int(getattr(policy, "recommended_validation_score", thresholds["validation_score"])) if policy is not None else thresholds["validation_score"]
    recommended_validation_runs = int(getattr(policy, "recommended_validation_runs", thresholds["validation_runs"])) if policy is not None else thresholds["validation_runs"]
    recommended_production_score = int(getattr(policy, "recommended_production_score", thresholds["production_score"])) if policy is not None else thresholds["production_score"]
    recommended_production_runs = int(getattr(policy, "recommended_production_runs", thresholds["production_runs"])) if policy is not None else thresholds["production_runs"]
    recommended_activation_count = int(getattr(policy, "recommended_activation_count", 0)) if policy is not None else 0
    trusted_validation_score = int(getattr(policy, "trusted_validation_score", max(recommended_validation_score + 10, 65))) if policy is not None else max(recommended_validation_score + 10, 65)
    trusted_validation_runs = int(getattr(policy, "trusted_validation_runs", max(recommended_validation_runs + 1, 2))) if policy is not None else max(recommended_validation_runs + 1, 2)
    trusted_production_score = int(getattr(policy, "trusted_production_score", max(recommended_production_score + 10, 15))) if policy is not None else max(recommended_production_score + 10, 15)
    trusted_production_runs = int(getattr(policy, "trusted_production_runs", max(recommended_production_runs, 1))) if policy is not None else max(recommended_production_runs, 1)
    trusted_activation_count = int(getattr(policy, "trusted_activation_count", 1)) if policy is not None else 1
    suppression_validation_score_max = int(getattr(policy, "suppression_validation_score_max", max(35, recommended_validation_score - 15))) if policy is not None else max(35, recommended_validation_score - 15)
    suppression_validation_runs_min = int(getattr(policy, "suppression_validation_runs_min", max(2, recommended_validation_runs))) if policy is not None else max(2, recommended_validation_runs)
    suppression_production_score_max = int(getattr(policy, "suppression_production_score_max", max(5, recommended_production_score - 5))) if policy is not None else max(5, recommended_production_score - 5)
    suppression_production_runs_min = int(getattr(policy, "suppression_production_runs_min", max(1, recommended_production_runs))) if policy is not None else max(1, recommended_production_runs)

    validation_ready = (
        historical_validation_count >= recommended_validation_runs
        and observed_validation_score >= recommended_validation_score
    )
    production_required = recommended_production_runs > 0
    production_ready = (
        not production_required
        or (
            production_run_count >= recommended_production_runs
            and production_score >= recommended_production_score
        )
    )
    activation_signal = planner_activation_count + prompt_activation_count
    draft_signal = planner_draft_count + prompt_draft_count
    selection_signal = planner_selection_count + prompt_selection_count

    if validation_ready:
        recommendation_score += 30
        reasons.append(
            f"Validation clears the {recommended_validation_score}/100 floor for {getattr(source_strategy, 'value', source_strategy)}."
        )
    elif historical_validation_count:
        gap = max(0, recommended_validation_score - observed_validation_score)
        reasons.append(f"Validation is still {gap} point(s) below the promotion floor.")
        blockers.append(
            f"{policy_label}: validation score {observed_validation_score}/100 is below the recommended floor {recommended_validation_score}/100."
        )
    else:
        reasons.append("No validation evidence yet.")
        blockers.append(
            f"{policy_label}: needs at least {recommended_validation_runs} validation run(s) before recommendation."
        )

    if 0 < historical_validation_count < recommended_validation_runs:
        blockers.append(
            f"{policy_label}: only {historical_validation_count} validation run(s); needs {recommended_validation_runs}."
        )

    if production_run_count:
        recommendation_score += min(production_score // 5, 20)
        if production_ready:
            reasons.append("Production results meet the current strategy gate.")
        else:
            reasons.append("Production results exist, but are not yet strong enough for promotion.")
            if production_run_count < recommended_production_runs:
                blockers.append(
                    f"{policy_label}: only {production_run_count} production run(s); needs {recommended_production_runs}."
                )
            if production_score < recommended_production_score:
                blockers.append(
                    f"{policy_label}: production score {production_score}/100 is below the recommended floor {recommended_production_score}/100."
                )
    elif production_required:
        reasons.append("No production evidence yet for a strategy that requires it.")
        blockers.append(
            f"{policy_label}: needs at least {recommended_production_runs} production run(s) with score >= {recommended_production_score}/100."
        )

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
    elif recommended_activation_count > 0:
        blockers.append(
            f"{policy_label}: needs at least {recommended_activation_count} planner or prompt activation signal(s)."
        )

    if (
        historical_validation_count >= suppression_validation_runs_min
        and observed_validation_score <= suppression_validation_score_max
        and production_run_count >= suppression_production_runs_min
        and production_score <= suppression_production_score_max
    ):
        state = "suppressed"
        recommendation_score = max(recommendation_score - 25, 0)
        reasons.append("Repeated validation and production evidence suggest this variant is weak.")
        blockers.append(
            f"{policy_label}: suppression triggered by validation <= {suppression_validation_score_max}/100 and production <= {suppression_production_score_max}/100."
        )
        return RecommendationDecision(state, recommendation_score, reasons, -20, policy_key, policy_label, blockers)

    if (
        historical_validation_count >= trusted_validation_runs
        and observed_validation_score >= trusted_validation_score
        and production_run_count >= trusted_production_runs
        and production_score >= trusted_production_score
        and (activation_signal >= trusted_activation_count or draft_signal >= max(trusted_activation_count + 1, 2) or market_production_run_count >= 2)
    ):
        state = "trusted"
        reasons.append("This variant has both strong evidence and repeated downstream adoption.")
        return RecommendationDecision(state, recommendation_score, reasons, 12, policy_key, policy_label, blockers)

    if validation_ready and (production_ready or activation_signal >= recommended_activation_count or draft_signal >= max(recommended_activation_count, 1)):
        state = "recommended"
        reasons.append("This variant has enough evidence to recommend, but not yet enough to fully trust.")
        return RecommendationDecision(state, recommendation_score, reasons, 5, policy_key, policy_label, blockers)

    reasons.append("Keep this variant visible while more validation or production evidence accumulates.")
    if observed_validation_score < trusted_validation_score or historical_validation_count < trusted_validation_runs:
        blockers.append(
            f"{policy_label}: trusted state needs validation >= {trusted_validation_score}/100 across {trusted_validation_runs} run(s)."
        )
    if production_score < trusted_production_score or production_run_count < trusted_production_runs:
        blockers.append(
            f"{policy_label}: trusted state needs production >= {trusted_production_score}/100 across {trusted_production_runs} run(s)."
        )
    if activation_signal < trusted_activation_count:
        blockers.append(
            f"{policy_label}: trusted state needs {trusted_activation_count} activation signal(s)."
        )
    return RecommendationDecision(state, recommendation_score, reasons, 0, policy_key, policy_label, blockers)


def apply_variant_history(session: Session, proposals: list[DraftProposal]) -> list[DraftProposal]:
    if not proposals:
        return proposals

    policy_map = recommendation_policy_map(session)
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
        recommendation = derive_recommendation_state(
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
                source_strategy=resolve_recommendation_policy(policy_map, proposal.source_strategy) or proposal.source_strategy,
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
                fit_score=proposal.template_score + proposal.prompt_match_score + validation_bonus + cluster_bonus + adoption_bonus + planner_conversion_bonus + production_bonus + market_bonus + strategy_bonus + recommendation.rank_bonus,
                fit_reasons=fit_reasons,
                recommendation_state=recommendation.state,
                recommendation_state_score=recommendation.score,
                recommendation_reasons=recommendation.reasons,
                recommendation_policy_key=recommendation.policy_key,
                recommendation_policy_label=recommendation.policy_label,
                recommendation_blockers=recommendation.blockers,
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
