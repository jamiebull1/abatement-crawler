"""LLM extraction pipeline using Anthropic Claude."""

from __future__ import annotations

import json
import logging
import re
import time
from typing import Any

from .config import CrawlerConfig
from .models import AbatementRecord

logger = logging.getLogger(__name__)

SCHEMA_DESCRIPTION = """
{
  "measure_name": "string – human-readable name of the abatement measure",
  "measure_slug": "string – url-friendly slug",
  "abatement_category": "one of: fuel_switch | efficiency | behaviour | carbon_capture | process_change | material_sub",
  "sector": "string",
  "sub_sector": "string (optional)",
  "asset_type": "string (optional)",
  "process": "string (optional)",
  "scope_tag": "one of: scope_1 | scope_2 | scope_3 | multiple",
  "geography": "string – country or region",
  "publication_year": "integer",
  "data_year": "integer (optional)",
  "abatement_potential_tco2e": "number (optional) – annual abatement in tCO2e",
  "abatement_unit": "string – unit for abatement potential",
  "abatement_percentage": "number (optional) – % reduction",
  "baseline_description": "string (optional)",
  "carbon_intensity_baseline": "number (optional)",
  "carbon_intensity_post": "number (optional)",
  "capex": "number (optional) – capital expenditure",
  "capex_unit": "string (optional)",
  "capex_notes": "string (optional)",
  "opex_fixed": "number (optional)",
  "opex_variable": "number (optional)",
  "opex_unit": "string (optional)",
  "opex_delta": "number (optional) – incremental operating cost vs baseline",
  "lifetime_years": "integer (optional)",
  "discount_rate": "number (optional) – as decimal e.g. 0.035",
  "mac": "number (optional) – marginal abatement cost in currency/tCO2e",
  "mac_notes": "string (optional)",
  "currency": "string – ISO 4217 code, default GBP",
  "price_base_year": "integer (optional)",
  "dependencies": ["list of strings"],
  "co_benefits": ["list of strings"],
  "barriers": ["list of strings"],
  "implementation_complexity": "one of: low | medium | high",
  "lead_time_years": "number (optional)",
  "source_url": "string",
  "source_title": "string",
  "source_type": "one of: academic | government | consultancy | ngo | industry_body | company_report | technology_catalogue",
  "source_organisation": "string (optional)",
  "authors": ["list of strings"],
  "doi": "string (optional)",
  "evidence_type": "one of: modelled | empirical | expert_elicitation | literature_review",
  "peer_reviewed": "boolean",
  "extraction_confidence": "number 0-1 – your confidence in this extraction",
  "raw_excerpt": "string – verbatim text supporting cost/carbon figures",
  "notes": "string (optional)"
}
"""

EXTRACTION_PROMPT = """You are extracting carbon abatement data from a document chunk.

For each distinct abatement measure described, extract a JSON object with these fields:
{schema_description}

Return a JSON array of records. For uncertain values, add an "_uncertain" key with value true alongside the field.
Include raw_excerpt with the verbatim text supporting each cost/carbon figure.
If no abatement measures with cost or carbon data are found, return an empty array [].

Document source URL: {source_url}
Document title: {source_title}

Document chunk:
{chunk}

Return only valid JSON. No markdown, no explanation outside the JSON.
"""


class LLMExtractor:
    """Extracts AbatementRecord objects from document chunks using Anthropic Claude."""

    def __init__(self, config: CrawlerConfig) -> None:
        self.config = config
        self._client = None
        self._init_client()

    def _init_client(self) -> None:
        """Initialise Anthropic client if API key is available."""
        if not self.config.llm_api_key:
            logger.warning(
                "No LLM API key configured. LLM extraction will be unavailable."
            )
            return
        try:
            import anthropic  # noqa: PLC0415

            self._client = anthropic.Anthropic(api_key=self.config.llm_api_key)
        except ImportError:
            logger.warning("anthropic library not installed. LLM extraction unavailable.")

    def extract(
        self, chunk: str, source_url: str, source_title: str
    ) -> list[AbatementRecord]:
        """Extract AbatementRecord list from a document chunk.

        Returns an empty list if extraction fails or no records are found.
        """
        if not self._client:
            logger.debug("LLM client not available; skipping extraction.")
            return []

        prompt = EXTRACTION_PROMPT.format(
            schema_description=SCHEMA_DESCRIPTION,
            source_url=source_url,
            source_title=source_title,
            chunk=chunk[:16000],  # guard against extremely long inputs
        )

        for attempt in range(self.config.max_retries + 1):
            try:
                raw = self._call_llm(prompt)
                records_data = self._validate_and_parse(raw)
                results = []
                for data in records_data:
                    data.setdefault("source_url", source_url)
                    data.setdefault("source_title", source_title)
                    data["measure_slug"] = self._make_slug(
                        data.get("measure_name", "unknown")
                    )
                    # Remove uncertainty markers before validation
                    cleaned = {
                        k: v
                        for k, v in data.items()
                        if not k.endswith("_uncertain")
                    }
                    try:
                        record = AbatementRecord(**cleaned)
                        results.append(record)
                    except Exception as parse_exc:
                        logger.debug(
                            "Failed to parse record from LLM output: %s", parse_exc
                        )
                return results
            except Exception as exc:
                logger.warning(
                    "LLM extraction attempt %d/%d failed: %s",
                    attempt + 1,
                    self.config.max_retries + 1,
                    exc,
                )
                if attempt < self.config.max_retries:
                    time.sleep(2 ** attempt)

        return []

    def _call_llm(self, prompt: str) -> str:
        """Call the Anthropic API and return the response text."""
        message = self._client.messages.create(
            model=self.config.llm_model,
            max_tokens=4096,
            temperature=self.config.extraction_temperature,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text

    def _validate_and_parse(self, json_str: str) -> list[dict[str, Any]]:
        """Parse and basic-validate the LLM JSON output."""
        # Strip markdown code fences if present
        json_str = re.sub(r"^```(?:json)?\s*", "", json_str.strip(), flags=re.IGNORECASE)
        json_str = re.sub(r"\s*```$", "", json_str.strip())

        data = json.loads(json_str)
        if not isinstance(data, list):
            if isinstance(data, dict):
                data = [data]
            else:
                raise ValueError(f"Expected JSON array, got {type(data)}")

        validated = []
        for item in data:
            if not isinstance(item, dict):
                continue
            if "measure_name" not in item:
                continue
            validated.append(item)
        return validated

    @staticmethod
    def _make_slug(measure_name: str) -> str:
        """Create a URL-friendly slug from a measure name."""
        try:
            from slugify import slugify  # noqa: PLC0415

            return slugify(measure_name)
        except ImportError:
            slug = measure_name.lower()
            slug = re.sub(r"[^\w\s-]", "", slug)
            slug = re.sub(r"[\s_]+", "-", slug)
            return slug.strip("-")
