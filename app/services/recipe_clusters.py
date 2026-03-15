from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import QueryPromptClusterDecision
from app.services.recipe_drafts import ClusterCandidate
from app.services.recipe_prompt_normalization import resolve_prompt_country_code
from app.services.recipe_variants import prompt_fingerprint


def _is_ambiguous(chosen: ClusterCandidate, alternates: list[ClusterCandidate]) -> bool:
    if not alternates:
        return False
    nearest = alternates[0]
    return nearest.score >= max(chosen.score - 25, 1)


def apply_cluster_decision_history(
    session: Session,
    prompt: str,
    chosen: ClusterCandidate,
    alternates: list[ClusterCandidate],
) -> tuple[ClusterCandidate, list[ClusterCandidate]]:
    fingerprint = prompt_fingerprint(prompt)
    prompt_market_country = resolve_prompt_country_code(session, prompt)
    history = {
        row.cluster_slug: row
        for row in session.scalars(
            select(QueryPromptClusterDecision).where(
                QueryPromptClusterDecision.prompt_fingerprint == fingerprint,
                QueryPromptClusterDecision.market_country_code.is_(None),
            )
        ).all()
    }
    market_history: dict[str, QueryPromptClusterDecision] = {}
    if prompt_market_country:
        market_history = {
            row.cluster_slug: row
            for row in session.scalars(
                select(QueryPromptClusterDecision).where(
                    QueryPromptClusterDecision.prompt_fingerprint == fingerprint,
                    QueryPromptClusterDecision.market_country_code == prompt_market_country,
                )
            ).all()
        }

    def decorate(candidate: ClusterCandidate) -> ClusterCandidate:
        row = history.get(candidate.cluster_slug)
        market_row = market_history.get(candidate.cluster_slug)
        if row is None and market_row is None:
            return replace(candidate, market_country_code=prompt_market_country)
        bonus = 0
        rationale = list(candidate.rationale)
        historical_seen_count = 0
        historical_selected_count = 0
        ambiguity_count = 0
        market_historical_seen_count = 0
        market_historical_selected_count = 0
        if row is not None:
            bonus += min(row.times_selected, 5) * 3
            historical_seen_count = row.times_seen
            historical_selected_count = row.times_selected
            ambiguity_count = row.ambiguity_count
            rationale.append(
                f"Historically selected {row.times_selected} of {row.times_seen} prompt run(s)."
            )
            if row.ambiguity_count:
                rationale.append(
                    f"This prompt was ambiguous across {row.ambiguity_count} prior run(s)."
                )
        if market_row is not None:
            bonus += min(market_row.times_selected, 5) * 4
            market_historical_seen_count = market_row.times_seen
            market_historical_selected_count = market_row.times_selected
            rationale.append(
                f"In {prompt_market_country}, this cluster was selected {market_row.times_selected} of {market_row.times_seen} similar prompt run(s)."
            )
        return replace(
            candidate,
            score=candidate.score + bonus,
            rationale=rationale,
            market_country_code=prompt_market_country,
            historical_seen_count=historical_seen_count,
            historical_selected_count=historical_selected_count,
            market_historical_seen_count=market_historical_seen_count,
            market_historical_selected_count=market_historical_selected_count,
            ambiguity_count=ambiguity_count,
        )

    ranked = [decorate(chosen)] + [decorate(candidate) for candidate in alternates]
    ranked.sort(key=lambda item: (-item.score, item.cluster_slug))
    return ranked[0], ranked[1:]


def record_cluster_decision(
    session: Session,
    prompt: str,
    chosen: ClusterCandidate,
    alternates: list[ClusterCandidate],
) -> None:
    prompt_text = prompt.strip()
    fingerprint = prompt_fingerprint(prompt_text)
    prompt_market_country = resolve_prompt_country_code(session, prompt_text)
    ambiguous = _is_ambiguous(chosen, alternates)
    ranked = [chosen] + alternates
    existing = {
        (row.cluster_slug, row.market_country_code): row
        for row in session.scalars(
            select(QueryPromptClusterDecision).where(
                QueryPromptClusterDecision.prompt_fingerprint == fingerprint,
            )
        ).all()
    }
    now = datetime.now(timezone.utc)
    for candidate in ranked:
        for market_code in (None, prompt_market_country):
            if market_code is None or prompt_market_country:
                row = existing.get((candidate.cluster_slug, market_code))
                if row is None:
                    row = QueryPromptClusterDecision(
                        prompt_text=prompt_text,
                        prompt_fingerprint=fingerprint,
                        market_country_code=market_code,
                        vertical=candidate.vertical,
                        cluster_slug=candidate.cluster_slug,
                    )
                    session.add(row)
                row.prompt_text = prompt_text
                row.market_country_code = market_code
                row.vertical = candidate.vertical
                row.match_score = candidate.score
                row.matched_aliases = list(candidate.matched_aliases)
                row.rationale = candidate.rationale
                row.times_seen = max(row.times_seen, 0) + 1
                if candidate.cluster_slug == chosen.cluster_slug:
                    row.times_selected = max(row.times_selected, 0) + 1
                if ambiguous:
                    row.ambiguity_count = max(row.ambiguity_count, 0) + 1
                row.last_seen_at = now
