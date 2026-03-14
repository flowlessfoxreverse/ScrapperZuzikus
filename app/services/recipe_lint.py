from __future__ import annotations

from dataclasses import dataclass
import re


TAG_KEY_PATTERN = re.compile(r"^[A-Za-z0-9:_-]+$")
TAG_VALUE_PATTERN = re.compile(r"^[A-Za-z0-9:_\-/ ]+$")


@dataclass
class RecipeLintResult:
    passed: bool
    errors: list[str]
    warnings: list[str]


def parse_tag_block(raw_text: str) -> tuple[list[dict[str, str]], list[str]]:
    tags: list[dict[str, str]] = []
    errors: list[str] = []
    for line_no, row in enumerate(raw_text.splitlines(), start=1):
        stripped = row.strip()
        if not stripped:
            continue
        if "=" not in stripped:
            errors.append(f"Line {line_no}: expected key=value format.")
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or not value:
            errors.append(f"Line {line_no}: key and value are both required.")
            continue
        tags.append({key: value})
    return tags, errors


def lint_recipe_content(
    *,
    osm_tags: list[dict[str, str]],
    exclude_tags: list[dict[str, str]],
    search_terms: list[str],
    website_keywords: list[str],
) -> RecipeLintResult:
    errors: list[str] = []
    warnings: list[str] = []

    if not osm_tags:
        errors.append("At least one OSM include tag is required.")

    include_pairs: set[tuple[str, str]] = set()
    exclude_pairs: set[tuple[str, str]] = set()

    def inspect_tag_map(tag_map: dict[str, str], *, label: str, bucket: set[tuple[str, str]]) -> None:
        if len(tag_map) != 1:
            errors.append(f"{label} entries must contain exactly one key=value pair.")
            return
        key, value = next(iter(tag_map.items()))
        if not TAG_KEY_PATTERN.match(key):
            errors.append(f"{label} tag key '{key}' contains unsupported characters.")
        if not TAG_VALUE_PATTERN.match(value):
            errors.append(f"{label} tag value '{value}' contains unsupported characters.")
        pair = (key.strip(), value.strip())
        if pair in bucket:
            warnings.append(f"Duplicate {label.lower()} tag '{key}={value}'.")
        bucket.add(pair)
        if value.lower() in {"yes", "*"}:
            warnings.append(f"{label} tag '{key}={value}' is very broad and may create noisy results.")

    for tag_map in osm_tags:
        inspect_tag_map(tag_map, label="Include", bucket=include_pairs)
    for tag_map in exclude_tags:
        inspect_tag_map(tag_map, label="Exclude", bucket=exclude_pairs)

    overlaps = include_pairs & exclude_pairs
    for key, value in sorted(overlaps):
        errors.append(f"Tag '{key}={value}' cannot be both included and excluded.")

    if not search_terms:
        warnings.append("No search terms were provided; website enrichment quality may be lower.")
    if not website_keywords:
        warnings.append("No website keywords were provided; later website filtering may be weak.")

    return RecipeLintResult(passed=not errors, errors=errors, warnings=warnings)
