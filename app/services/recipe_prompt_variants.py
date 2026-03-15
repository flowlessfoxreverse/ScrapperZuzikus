from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import (
    QueryPromptVariantDecision,
    QueryRecipe,
    QueryRecipePlanVariantOutcome,
    QueryRecipeVariant,
)
from app.services.recipe_drafts import DraftProposal
from app.services.recipe_prompt_normalization import resolve_prompt_country_code
from app.services.recipe_variants import prompt_fingerprint


def _prompt_variant_bonus(selected_count: int, draft_created_count: int, activated_count: int) -> int:
    selection_bonus = min(selected_count, 6)
    draft_bonus = min(draft_created_count, 6) * 2
    activation_bonus = min(activated_count, 6) * 4
    return selection_bonus + draft_bonus + activation_bonus


def _market_prompt_variant_bonus(selected_count: int, draft_created_count: int, activated_count: int) -> int:
    selection_bonus = min(selected_count, 4)
    draft_bonus = min(draft_created_count, 4)
    activation_bonus = min(activated_count, 4) * 2
    return selection_bonus + draft_bonus + activation_bonus


def apply_prompt_variant_history(session: Session, prompt: str, proposals: list[DraftProposal]) -> list[DraftProposal]:
    if not prompt.strip() or not proposals:
        return proposals

    fingerprint = prompt_fingerprint(prompt)
    prompt_market_country = resolve_prompt_country_code(session, prompt)
    history = {
        row.variant_key: row
        for row in session.scalars(
            select(QueryPromptVariantDecision).where(
                QueryPromptVariantDecision.prompt_fingerprint == fingerprint,
                QueryPromptVariantDecision.variant_key.in_([proposal.variant_key for proposal in proposals]),
            )
        ).all()
    }
    market_history: dict[str, QueryPromptVariantDecision] = {}
    if prompt_market_country:
        market_history = {
            row.variant_key: row
            for row in session.scalars(
                select(QueryPromptVariantDecision).where(
                    QueryPromptVariantDecision.prompt_fingerprint == fingerprint,
                    QueryPromptVariantDecision.market_country_code == prompt_market_country,
                    QueryPromptVariantDecision.variant_key.in_([proposal.variant_key for proposal in proposals]),
                )
            ).all()
        }

    adjusted: list[DraftProposal] = []
    for proposal in proposals:
        row = history.get(proposal.variant_key)
        fit_reasons = list(proposal.fit_reasons)
        selection_count = max(row.selected_count, 0) if row is not None else 0
        draft_created_count = max(row.draft_created_count, 0) if row is not None else 0
        activated_count = max(row.activated_count, 0) if row is not None else 0
        bonus = _prompt_variant_bonus(selection_count, draft_created_count, activated_count)
        market_row = market_history.get(proposal.variant_key)
        market_selection_count = max(market_row.selected_count, 0) if market_row is not None else 0
        market_draft_created_count = max(market_row.draft_created_count, 0) if market_row is not None else 0
        market_activated_count = max(market_row.activated_count, 0) if market_row is not None else 0
        market_bonus = _market_prompt_variant_bonus(
            market_selection_count,
            market_draft_created_count,
            market_activated_count,
        )
        if selection_count:
            fit_reasons.append(
                f"Historically selected {selection_count} time(s) for prompts matching this fingerprint."
            )
        if draft_created_count:
            fit_reasons.append(
                f"Historically turned into {draft_created_count} draft recipe(s) from this prompt."
            )
        if activated_count:
            fit_reasons.append(
                f"Historically activated {activated_count} time(s) from this prompt."
            )
        if prompt_market_country and market_selection_count:
            fit_reasons.append(
                f"Historically selected {market_selection_count} time(s) for this prompt in {prompt_market_country}."
            )
        if prompt_market_country and market_draft_created_count:
            fit_reasons.append(
                f"Historically drafted {market_draft_created_count} time(s) for this prompt in {prompt_market_country}."
            )
        if prompt_market_country and market_activated_count:
            fit_reasons.append(
                f"Historically activated {market_activated_count} time(s) for this prompt in {prompt_market_country}."
            )
        adjusted.append(
            replace(
                proposal,
                market_prompt_selection_count=market_selection_count,
                market_prompt_draft_count=market_draft_created_count,
                market_prompt_activation_count=market_activated_count,
                prompt_selection_count=selection_count,
                prompt_draft_count=draft_created_count,
                prompt_activation_count=activated_count,
                fit_score=proposal.fit_score + bonus + market_bonus,
                fit_reasons=fit_reasons,
            )
        )

    adjusted.sort(
        key=lambda item: (
            -item.fit_score,
            -item.market_prompt_activation_count,
            -item.market_prompt_draft_count,
            -item.prompt_activation_count,
            -item.prompt_draft_count,
            -item.prompt_selection_count,
            item.label,
        )
    )
    return adjusted


def record_prompt_variant_decisions(
    session: Session,
    prompt: str,
    variants_by_key: dict[str, QueryRecipeVariant],
    *,
    selected_variant_keys: list[str] | None = None,
    drafted_variant_keys: list[str] | None = None,
) -> None:
    prompt_text = prompt.strip()
    if not prompt_text or not variants_by_key:
        return

    selected_keys = set(selected_variant_keys or [])
    drafted_keys = set(drafted_variant_keys or [])
    affected_keys = selected_keys | drafted_keys
    if not affected_keys:
        return

    fingerprint = prompt_fingerprint(prompt_text)
    prompt_market_country = resolve_prompt_country_code(session, prompt_text)
    existing = {
        row.variant_key: row
        for row in session.scalars(
            select(QueryPromptVariantDecision).where(
                QueryPromptVariantDecision.prompt_fingerprint == fingerprint,
                QueryPromptVariantDecision.market_country_code == prompt_market_country,
                QueryPromptVariantDecision.variant_key.in_(list(affected_keys)),
            )
        ).all()
    }
    now = datetime.now(timezone.utc)
    for variant_key in affected_keys:
        variant = variants_by_key.get(variant_key)
        if variant is None:
            continue
        row = existing.get(variant_key)
        if row is None:
            row = QueryPromptVariantDecision(
                prompt_text=prompt_text,
                prompt_fingerprint=fingerprint,
                market_country_code=prompt_market_country,
                vertical=variant.vertical,
                cluster_slug=variant.cluster_slug,
                variant_key=variant.variant_key,
                source_variant_id=variant.id,
            )
            session.add(row)
        row.prompt_text = prompt_text
        row.market_country_code = prompt_market_country
        row.vertical = variant.vertical
        row.cluster_slug = variant.cluster_slug
        row.source_variant_id = variant.id
        if variant_key in selected_keys:
            row.selected_count = max(row.selected_count, 0) + 1
            row.last_selected_at = now
        if variant_key in drafted_keys:
            row.draft_created_count = max(row.draft_created_count, 0) + 1
            row.last_drafted_at = now
        row.updated_at = now


def record_prompt_variant_activation(session: Session, recipe: QueryRecipe) -> None:
    variant = recipe.source_variant
    if variant is None or not variant.prompt_text.strip():
        return

    fingerprint = prompt_fingerprint(variant.prompt_text)
    prompt_market_country = resolve_prompt_country_code(session, variant.prompt_text)
    row = session.scalar(
        select(QueryPromptVariantDecision).where(
            QueryPromptVariantDecision.prompt_fingerprint == fingerprint,
            QueryPromptVariantDecision.market_country_code == prompt_market_country,
            QueryPromptVariantDecision.variant_key == variant.variant_key,
        )
    )
    now = datetime.now(timezone.utc)
    if row is None:
        row = QueryPromptVariantDecision(
            prompt_text=variant.prompt_text,
            prompt_fingerprint=fingerprint,
            market_country_code=prompt_market_country,
            vertical=variant.vertical,
            cluster_slug=variant.cluster_slug,
            variant_key=variant.variant_key,
            source_variant_id=variant.id,
        )
        session.add(row)
    row.prompt_text = variant.prompt_text
    row.market_country_code = prompt_market_country
    row.vertical = variant.vertical
    row.cluster_slug = variant.cluster_slug
    row.source_variant_id = variant.id
    row.activated_count = max(row.activated_count, 0) + 1
    row.last_activated_at = now
    row.updated_at = now


def sync_plan_variant_outcomes(
    session: Session,
    plan,
    variants_by_key: dict[str, QueryRecipeVariant],
) -> None:
    plan_id = getattr(plan, "plan_id", None)
    if not plan_id:
        return

    proposals = list(getattr(plan, "draft_variants", []) or [])
    if not proposals:
        return

    keys = [proposal.variant_key for proposal in proposals]
    existing = {
        row.variant_key: row
        for row in session.scalars(
            select(QueryRecipePlanVariantOutcome).where(
                QueryRecipePlanVariantOutcome.plan_id == plan_id,
                QueryRecipePlanVariantOutcome.variant_key.in_(keys),
            )
        ).all()
    }
    now = datetime.now(timezone.utc)
    for rank_position, proposal in enumerate(proposals, start=1):
        variant = variants_by_key.get(proposal.variant_key)
        row = existing.get(proposal.variant_key)
        if row is None:
            row = QueryRecipePlanVariantOutcome(
                plan_id=plan_id,
                prompt_fingerprint=prompt_fingerprint(plan.prompt),
                requested_provider=plan.requested_provider,
                provider=plan.provider,
                model_name=plan.model_name,
                market_country_code=getattr(plan, "market_country_code", None),
                vertical=proposal.vertical,
                cluster_slug=proposal.cluster_slug,
                variant_key=proposal.variant_key,
                source_variant_id=variant.id if variant is not None else None,
                variant_label=proposal.label,
            )
            session.add(row)
        row.prompt_fingerprint = prompt_fingerprint(plan.prompt)
        row.requested_provider = plan.requested_provider
        row.provider = plan.provider
        row.model_name = plan.model_name
        row.market_country_code = getattr(plan, "market_country_code", None)
        row.vertical = proposal.vertical
        row.cluster_slug = proposal.cluster_slug
        row.source_variant_id = variant.id if variant is not None else None
        row.variant_label = proposal.label
        row.rank_position = rank_position
        row.template_score = proposal.template_score
        row.prompt_match_score = proposal.prompt_match_score
        row.rank_score = proposal.fit_score
        row.updated_at = now


def record_plan_variant_decisions(
    session: Session,
    plan_id: int | None,
    *,
    selected_variant_keys: list[str] | None = None,
    drafted_variant_keys: list[str] | None = None,
) -> None:
    if not plan_id:
        return

    selected_keys = set(selected_variant_keys or [])
    drafted_keys = set(drafted_variant_keys or [])
    affected_keys = selected_keys | drafted_keys
    if not affected_keys:
        return

    rows = {
        row.variant_key: row
        for row in session.scalars(
            select(QueryRecipePlanVariantOutcome).where(
                QueryRecipePlanVariantOutcome.plan_id == plan_id,
                QueryRecipePlanVariantOutcome.variant_key.in_(list(affected_keys)),
            )
        ).all()
    }
    now = datetime.now(timezone.utc)
    for variant_key in affected_keys:
        row = rows.get(variant_key)
        if row is None:
            continue
        if variant_key in selected_keys:
            row.was_selected = True
            row.selected_at = now
        if variant_key in drafted_keys:
            row.was_drafted = True
            row.drafted_at = now
        row.updated_at = now


def record_plan_variant_activation(session: Session, recipe: QueryRecipe) -> None:
    variant = recipe.source_variant
    plan = recipe.source_plan
    if variant is None or plan is None:
        return

    row = session.scalar(
        select(QueryRecipePlanVariantOutcome).where(
            QueryRecipePlanVariantOutcome.plan_id == plan.id,
            QueryRecipePlanVariantOutcome.variant_key == variant.variant_key,
        )
    )
    if row is None:
        return

    now = datetime.now(timezone.utc)
    row.was_activated = True
    row.activated_at = now
    row.updated_at = now
