from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import QueryPromptVariantDecision, QueryRecipe, QueryRecipeVariant
from app.services.recipe_drafts import DraftProposal
from app.services.recipe_variants import prompt_fingerprint


def _prompt_variant_bonus(selected_count: int, draft_created_count: int, activated_count: int) -> int:
    selection_bonus = min(selected_count, 6)
    draft_bonus = min(draft_created_count, 6) * 2
    activation_bonus = min(activated_count, 6) * 4
    return selection_bonus + draft_bonus + activation_bonus


def apply_prompt_variant_history(session: Session, prompt: str, proposals: list[DraftProposal]) -> list[DraftProposal]:
    if not prompt.strip() or not proposals:
        return proposals

    fingerprint = prompt_fingerprint(prompt)
    history = {
        row.variant_key: row
        for row in session.scalars(
            select(QueryPromptVariantDecision).where(
                QueryPromptVariantDecision.prompt_fingerprint == fingerprint,
                QueryPromptVariantDecision.variant_key.in_([proposal.variant_key for proposal in proposals]),
            )
        ).all()
    }

    adjusted: list[DraftProposal] = []
    for proposal in proposals:
        row = history.get(proposal.variant_key)
        if row is None:
            adjusted.append(proposal)
            continue
        selection_count = max(row.selected_count, 0)
        draft_created_count = max(row.draft_created_count, 0)
        activated_count = max(row.activated_count, 0)
        bonus = _prompt_variant_bonus(selection_count, draft_created_count, activated_count)
        fit_reasons = list(proposal.fit_reasons)
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
        adjusted.append(
            replace(
                proposal,
                prompt_selection_count=selection_count,
                prompt_draft_count=draft_created_count,
                prompt_activation_count=activated_count,
                fit_score=proposal.fit_score + bonus,
                fit_reasons=fit_reasons,
            )
        )

    adjusted.sort(
        key=lambda item: (
            -item.fit_score,
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
    existing = {
        row.variant_key: row
        for row in session.scalars(
            select(QueryPromptVariantDecision).where(
                QueryPromptVariantDecision.prompt_fingerprint == fingerprint,
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
                vertical=variant.vertical,
                cluster_slug=variant.cluster_slug,
                variant_key=variant.variant_key,
                source_variant_id=variant.id,
            )
            session.add(row)
        row.prompt_text = prompt_text
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
    row = session.scalar(
        select(QueryPromptVariantDecision).where(
            QueryPromptVariantDecision.prompt_fingerprint == fingerprint,
            QueryPromptVariantDecision.variant_key == variant.variant_key,
        )
    )
    now = datetime.now(timezone.utc)
    if row is None:
        row = QueryPromptVariantDecision(
            prompt_text=variant.prompt_text,
            prompt_fingerprint=fingerprint,
            vertical=variant.vertical,
            cluster_slug=variant.cluster_slug,
            variant_key=variant.variant_key,
            source_variant_id=variant.id,
        )
        session.add(row)
    row.prompt_text = variant.prompt_text
    row.vertical = variant.vertical
    row.cluster_slug = variant.cluster_slug
    row.source_variant_id = variant.id
    row.activated_count = max(row.activated_count, 0) + 1
    row.last_activated_at = now
    row.updated_at = now
