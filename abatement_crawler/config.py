"""Configuration management for the abatement crawler."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

from .models import ScopeConfig


class CrawlerConfig(BaseModel):
    """Main configuration for the crawler, loaded from YAML."""

    scope: ScopeConfig = Field(default_factory=ScopeConfig)
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

        # Allow environment variable overrides for sensitive keys
        if not data.get("search_api_key"):
            data["search_api_key"] = os.environ.get("SEARCH_API_KEY", "")
        if not data.get("llm_api_key"):
            data["llm_api_key"] = os.environ.get("ANTHROPIC_API_KEY", "")

        return cls(scope=scope, **data)

    def to_yaml(self, path: str) -> None:
        """Persist configuration to a YAML file."""
        import dataclasses

        scope_dict = dataclasses.asdict(self.scope)
        data = self.model_dump(exclude={"scope"})
        data["scope"] = scope_dict
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            yaml.dump(data, f, default_flow_style=False)
