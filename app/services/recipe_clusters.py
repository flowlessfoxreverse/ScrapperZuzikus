from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import QueryPromptClusterDecision
from app.services.recipe_drafts import ClusterCandidate
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
    history = {
        row.cluster_slug: row
        for row in session.scalars(
            select(QueryPromptClusterDecision).where(
                QueryPromptClusterDecision.prompt_fingerprint == fingerprint
            )
        ).all()
    }

    def decorate(candidate: ClusterCandidate) -> ClusterCandidate:
        row = history.get(candidate.cluster_slug)
        if row is None:
            return candidate
        bonus = min(row.times_selected, 5) * 3
        rationale = list(candidate.rationale)
        rationale.append(
            f"Historically selected {row.times_selected} of {row.times_seen} prompt run(s)."
        )
        if row.ambiguity_count:
            rationale.append(
                f"This prompt was ambiguous across {row.ambiguity_count} prior run(s)."
            )
        return replace(
            candidate,
            score=candidate.score + bonus,
            rationale=rationale,
            historical_seen_count=row.times_seen,
            historical_selected_count=row.times_selected,
            ambiguity_count=row.ambiguity_count,
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
    ambiguous = _is_ambiguous(chosen, alternates)
    ranked = [chosen] + alternates
    existing = {
        row.cluster_slug: row
        for row in session.scalars(
            select(QueryPromptClusterDecision).where(
                QueryPromptClusterDecision.prompt_fingerprint == fingerprint
            )
        ).all()
    }
    now = datetime.now(timezone.utc)
    for candidate in ranked:
        row = existing.get(candidate.cluster_slug)
        if row is None:
            row = QueryPromptClusterDecision(
                prompt_text=prompt_text,
                prompt_fingerprint=fingerprint,
                vertical=candidate.vertical,
                cluster_slug=candidate.cluster_slug,
            )
            session.add(row)
        row.prompt_text = prompt_text
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

