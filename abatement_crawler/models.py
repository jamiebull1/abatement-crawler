"""Data models for the abatement crawler."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import uuid4

from pydantic import BaseModel, Field


@dataclass
class ScopeConfig:
    """Configuration for the scope of the crawl."""

    industry: str | None = None
    process: str | None = None
    asset_type: str | None = None
    company: str | None = None
    geography: list[str] = field(default_factory=list)
    sectors: list[str] = field(default_factory=list)
    abatement_types: list[str] = field(default_factory=list)
    year_range: tuple[int, int] = (2015, 2025)
    languages: list[str] = field(default_factory=lambda: ["en"])


class AbatementRecord(BaseModel):
    """A single abatement measure with associated cost and carbon performance data."""

    # Identity
    record_id: str = Field(default_factory=lambda: str(uuid4()))
    measure_name: str
    measure_slug: str
    abatement_category: str  # fuel_switch | efficiency | behaviour | carbon_capture | process_change | material_sub

    # Scope mapping
    sector: str
    sub_sector: str = ""
    asset_type: str | None = None
    process: str | None = None
    scope_tag: str  # scope_1 | scope_2 | scope_3 | multiple

    # Geography & time
    geography: str
    geography_notes: str | None = None
    publication_year: int
    data_year: int | None = None

    # Carbon performance
    abatement_potential_tco2e: float | None = None
    abatement_unit: str = ""
    abatement_percentage: float | None = None
    baseline_description: str | None = None
    carbon_intensity_baseline: float | None = None
    carbon_intensity_post: float | None = None

    # Cost data
    capex: float | None = None
    capex_unit: str | None = None
    capex_notes: str | None = None
    opex_fixed: float | None = None
    opex_variable: float | None = None
    opex_unit: str | None = None
    opex_delta: float | None = None
    lifetime_years: int | None = None
    discount_rate: float | None = None
    mac: float | None = None
    mac_notes: str | None = None
    currency: str = "GBP"
    price_base_year: int | None = None

    # Enabling conditions
    dependencies: list[str] = Field(default_factory=list)
    co_benefits: list[str] = Field(default_factory=list)
    barriers: list[str] = Field(default_factory=list)
    implementation_complexity: str = "medium"  # low | medium | high
    lead_time_years: float | None = None

    # Source provenance
    source_url: str
    source_title: str
    source_type: str  # academic | government | consultancy | ngo | industry_body | company_report | technology_catalogue
    source_organisation: str = ""
    authors: list[str] = Field(default_factory=list)
    doi: str | None = None
    retrieved_date: str = Field(default_factory=lambda: datetime.now(UTC).date().isoformat())

    # Quality
    quality_score: float = 0.0
    quality_flags: list[str] = Field(default_factory=list)
    evidence_type: str = "modelled"  # modelled | empirical | expert_elicitation | literature_review
    peer_reviewed: bool = False

    # Extraction metadata
    extraction_method: str = "llm_structured"
    extraction_confidence: float = 0.0
    raw_excerpt: str = ""
    notes: str | None = None

    # Licence flag
    full_text_restricted: bool = False
