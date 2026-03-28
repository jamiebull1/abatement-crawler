"""Configuration management for the abatement crawler."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

from .models import ScopeConfig


class PipelineConfig(BaseModel):
    """Configuration for pipeline mode (Layers 1–4)."""

    sector: str | None = None  # overrides scope.industry when set
    include_analogue_sectors: bool = True  # append analogue-sector query variants
    max_queries_per_archetype: int | None = None  # cap per archetype (None = unlimited)
    synthesis_enabled: bool = True  # run Layer 4 synthesis after Layer 3 crawl
    include_activity_search: bool = True  # search for activity intensity data when abatement missing


class CrawlerConfig(BaseModel):
    """Main configuration for the crawler, loaded from YAML."""

    scope: ScopeConfig = Field(default_factory=ScopeConfig)
    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)
    search_api: str = "duckduckgo"
    search_api_key: str = ""
    max_search_queries: int = 200
    results_per_query: int = 10
    max_depth: int = 4
    relevance_threshold: float = 0.3
    reflection_interval: int = 50
    max_total_documents: int = 2000
    llm_model: str = "claude-sonnet-4-20250514"
    llm_api_key: str = ""
    extraction_temperature: float = 0
    max_retries: int = 2
    base_currency: str = "GBP"
    base_year: int = 2023
    min_quality_for_export: float = 0.3
    db_path: str = "./abatement_records.db"
    output_dir: str = "./output/"
    requests_per_second: float = 2.0
    pdf_timeout_seconds: int = 30
    respect_robots_txt: bool = True
    captcha_queue_enabled: bool = True
    request_jitter: float = 0.5
    max_workers: int = 5

    model_config = {"arbitrary_types_allowed": True}

    @classmethod
    def from_yaml(cls, path: str) -> "CrawlerConfig":
        """Load configuration from a YAML file."""
        with open(path, "r", encoding="utf-8") as f:
            data: dict[str, Any] = yaml.safe_load(f) or {}

        # Handle nested scope config
        scope_data = data.pop("scope", {})

        # Convert year_range from list to tuple if loaded from YAML
        if "year_range" in scope_data and isinstance(scope_data["year_range"], list):
            scope_data["year_range"] = tuple(scope_data["year_range"])

        scope = ScopeConfig(**scope_data) if scope_data else ScopeConfig()

        # Handle nested pipeline config
        pipeline_data = data.pop("pipeline", {})
        pipeline = PipelineConfig(**pipeline_data) if pipeline_data else PipelineConfig()

        # Allow environment variable overrides for sensitive keys
        if not data.get("search_api_key"):
            data["search_api_key"] = os.environ.get("SEARCH_API_KEY", "")
        if not data.get("llm_api_key"):
            data["llm_api_key"] = os.environ.get("ANTHROPIC_API_KEY", "")

        return cls(scope=scope, pipeline=pipeline, **data)

    def validate(self, mode: str = "search") -> tuple[list[str], list[str]]:
        """Validate configuration before starting a crawl.

        Returns:
            A tuple of (errors, warnings). Errors are fatal; warnings are advisory.
        """
        errors: list[str] = []
        warnings: list[str] = []

        if not self.llm_api_key:
            errors.append(
                "llm_api_key is required (set ANTHROPIC_API_KEY env var or llm_api_key in config)"
            )

        _KEY_REQUIRED_APIS = {"serpapi", "google_cse", "google", "bing"}
        if mode in ("search", "pipeline") and self.search_api.lower() in _KEY_REQUIRED_APIS:
            if not self.search_api_key:
                errors.append(
                    f"search_api_key is required for {self.search_api}"
                    " (set SEARCH_API_KEY env var or search_api_key in config)"
                )

        yr = self.scope.year_range
        if yr is not None:
            if len(yr) != 2 or yr[0] > yr[1]:
                errors.append(
                    f"scope.year_range must be [start, end] with start <= end, got {list(yr)}"
                )
            elif not (1990 <= yr[0] <= 2050 and 1990 <= yr[1] <= 2050):
                warnings.append(
                    f"scope.year_range {list(yr)} is outside the expected range 1990–2050"
                )

        try:
            Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            warnings.append(f"Cannot create output_dir '{self.output_dir}': {exc}")

        try:
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            warnings.append(f"Cannot create directory for db_path '{self.db_path}': {exc}")

        return errors, warnings

    def to_yaml(self, path: str) -> None:
        """Persist configuration to a YAML file."""
        import dataclasses

        scope_dict = dataclasses.asdict(self.scope)
        data = self.model_dump(exclude={"scope", "pipeline"})
        data["scope"] = scope_dict
        data["pipeline"] = self.pipeline.model_dump()
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, default_flow_style=False)
