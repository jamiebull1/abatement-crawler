"""Quality scoring for AbatementRecord objects."""

from __future__ import annotations

import math
from datetime import datetime, UTC

from .models import AbatementRecord

# Key fields that indicate evidence completeness
_KEY_FIELDS = [
    "abatement_potential_tco2e",
    "capex",
    "opex_delta",
    "mac",
    "lifetime_years",
    "baseline_description",
]

# Source type quality priors
_SOURCE_PRIORS: dict[str, float] = {
    "academic": 1.0,
    "government": 0.85,
    "technology_catalogue": 0.75,
    "consultancy": 0.70,
    "ngo": 0.65,
    "industry_body": 0.60,
    "company_report": 0.50,
}
_DEFAULT_SOURCE_PRIOR = 0.40

# Data recency: exponential decay with half-life ~7 years
_RECENCY_HALF_LIFE_YEARS = 7.0


def _current_year() -> int:
    return datetime.now(UTC).year


def _evidence_completeness(record: AbatementRecord) -> float:
    """Fraction of key fields that are populated."""
    populated = sum(
        1 for f in _KEY_FIELDS if getattr(record, f, None) is not None
    )
    return populated / len(_KEY_FIELDS)


def _source_type_prior(record: AbatementRecord) -> float:
    """Quality prior based on source type."""
    return _SOURCE_PRIORS.get(record.source_type, _DEFAULT_SOURCE_PRIOR)


def _peer_review_score(record: AbatementRecord) -> float:
    """1.0 if peer-reviewed, 0.5 if grey literature."""
    return 1.0 if record.peer_reviewed else 0.5


def _data_recency(record: AbatementRecord) -> float:
    """Exponential decay from 1.0; half-life ~7 years from current year."""
    year = record.data_year or record.publication_year
    age = max(0, _current_year() - year)
    return math.exp(-math.log(2) * age / _RECENCY_HALF_LIFE_YEARS)


def _cost_data_present(record: AbatementRecord) -> float:
    """1.0 if both capex and opex present, 0.5 if only one, 0.0 if neither."""
    has_capex = record.capex is not None
    has_opex = record.opex_delta is not None or record.opex_fixed is not None
    if has_capex and has_opex:
        return 1.0
    if has_capex or has_opex:
        return 0.5
    return 0.0


def _geography_specificity(record: AbatementRecord) -> float:
    """Country-level > region > global."""
    geo = (record.geography or "").upper()
    if geo in ("GLOBAL", "WORLDWIDE", "INTERNATIONAL", ""):
        return 0.3
    if geo in ("EU", "EUROPE", "NORTH AMERICA", "ASIA", "AFRICA", "LATIN AMERICA"):
        return 0.6
    return 1.0


def _extraction_confidence(record: AbatementRecord) -> float:
    """LLM self-reported extraction confidence, clamped to [0, 1]."""
    return min(1.0, max(0.0, record.extraction_confidence or 0.0))


def score_quality(record: AbatementRecord) -> tuple[float, list[str]]:
    """Compute quality score and collect quality flags.

    quality = (
        0.20 × evidence_completeness
      + 0.20 × source_type_prior
      + 0.15 × peer_review_flag
      + 0.15 × data_recency
      + 0.15 × cost_data_present
      + 0.10 × geography_specificity
      + 0.05 × extraction_confidence
    )

    Returns:
        Tuple of (score: float in [0, 1], flags: list[str]).
    """
    score = (
        0.20 * _evidence_completeness(record)
        + 0.20 * _source_type_prior(record)
        + 0.15 * _peer_review_score(record)
        + 0.15 * _data_recency(record)
        + 0.15 * _cost_data_present(record)
        + 0.10 * _geography_specificity(record)
        + 0.05 * _extraction_confidence(record)
    )
    score = min(1.0, max(0.0, score))

    flags: list[str] = []

    if record.capex is None:
        flags.append("no_capex")
    if record.opex_delta is None and record.opex_fixed is None:
        flags.append("no_opex")
    if record.abatement_potential_tco2e is None and record.abatement_percentage is None:
        flags.append("no_carbon_data")

    # Old data flag: more than 10 years old
    year = record.data_year or record.publication_year
    if _current_year() - year > 10:
        flags.append("old_data")

    if record.source_url in ("", None):
        flags.append("no_source_url")

    if not record.baseline_description:
        flags.append("no_baseline")

    return score, flags
