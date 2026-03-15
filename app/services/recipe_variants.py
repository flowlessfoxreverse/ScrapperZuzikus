from __future__ import annotations

import hashlib

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import QueryRecipe, QueryRecipeVariant
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


def prompt_variant_recipe_map(session: Session, prompt: str) -> dict[str, str]:
    fingerprint = prompt_fingerprint(prompt)
    stmt = (
        select(QueryRecipeVariant.variant_key, QueryRecipe.slug)
        .join(QueryRecipe, QueryRecipe.source_variant_id == QueryRecipeVariant.id)
        .where(QueryRecipeVariant.prompt_fingerprint == fingerprint)
    )
    return {variant_key: recipe_slug for variant_key, recipe_slug in session.execute(stmt).all()}
