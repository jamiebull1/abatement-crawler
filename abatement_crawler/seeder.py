"""LLM-only seed record generator — no web crawl required."""

from __future__ import annotations

import dataclasses
import logging
import time
import uuid
from datetime import UTC, datetime

from .config import CrawlerConfig
from .extraction import SCHEMA_DESCRIPTION, LLMExtractor, _has_paired_data
from .models import AbatementRecord
from .normalisation import Normaliser
from .quality import score_quality
from .storage import StorageManager
from .taxonomy import CATEGORIES, AbatementCategory

logger = logging.getLogger(__name__)

_SEED_PROMPT = """\
You are a carbon abatement expert producing a representative "order-of-magnitude" estimate \
for a single abatement measure in the following category.

Category slug:    {category_slug}
Category name:    {category_name}
Description:      {category_description}
Subcategories:    {subcategories}

Produce ONE well-chosen, representative measure for this category — the most widely-applicable \
and well-understood example. This is a deliberate "finger in the air" estimate for initialising \
a database; precise values are not expected.

You MUST include both a cost estimate AND an abatement quantity. If exact figures are unknown, \
provide a plausible order-of-magnitude range and record your uncertainty in the notes field.

Use the following JSON schema (one record, wrapped in a JSON array):
{schema_description}

Hard requirements:
- abatement_category: "{category_slug}"  (must match exactly)
- geography: "GLOBAL"
- source_url: "urn:llm-seed:{category_slug}"
- source_title: "LLM-generated seed estimate — {category_name}"
- source_type: "llm_estimate"
- evidence_type: "expert_elicitation"
- peer_reviewed: false
- extraction_confidence: 0.3
- extraction_method: "llm_seed"
- publication_year: {year}
- data_year: {year}
- mac: a GBP/tCO2e order-of-magnitude estimate (required)
- abatement_percentage OR abatement_potential_tco2e: a rough quantity (required)
- baseline_description: short description of the counterfactual (required)
- lifetime_years: typical measure lifetime (required)
- notes: MUST state explicitly that this is an LLM-generated order-of-magnitude estimate, \
not sourced from literature
- raw_excerpt: one-sentence summary of the estimation basis

Return a JSON array containing exactly ONE record.
Return only valid JSON. No markdown fences, no explanation outside the JSON.
"""


class LLMSeeder:
    """Generates one representative AbatementRecord per taxonomy category using the LLM.

    No web crawl or document fetching is performed. Records are persisted via
    StorageManager and will carry low quality scores that reflect their synthetic origin.
    """

    def __init__(self, config: CrawlerConfig) -> None:
        self.config = config
        self._extractor = LLMExtractor(config)
        self._normaliser = Normaliser(config.base_currency, config.base_year)
        self._storage = StorageManager(config.db_path)
        self._session_id = str(uuid.uuid4())
        self._start_time = datetime.now(UTC)

    def run(self, categories: list[AbatementCategory] | None = None) -> dict:
        """Generate and persist seed records.

        Args:
            categories: Subset of taxonomy categories to seed. Defaults to all 13.

        Returns:
            Stats dict compatible with StorageManager.save_session.
        """
        targets = categories if categories is not None else CATEGORIES
        records: list[AbatementRecord] = []

        for cat in targets:
            logger.info("Generating seed record for category: %s", cat.slug)
            record = self._generate_for_category(cat)
            if record is None:
                logger.warning("No valid seed record produced for category: %s", cat.slug)
                continue

            try:
                record = self._normaliser.normalise(record)
            except Exception as exc:
                logger.debug("Normalisation skipped for %s: %s", cat.slug, exc)

            quality, flags = score_quality(record)
            record = record.model_copy(update={"quality_score": quality, "quality_flags": flags})
            self._storage.save_record(record)
            records.append(record)
            logger.info(
                "Saved '%s'  quality=%.2f  flags=%s",
                record.measure_name,
                quality,
                flags,
            )

        stats = {
            "session_id": self._session_id,
            "total_records": len(records),
            "qualified_records": sum(
                1 for r in records if r.quality_score >= self.config.min_quality_for_export
            ),
            "documents_processed": 0,
            "seed_generation": True,
            "start_time": self._start_time.isoformat(),
            "end_time": datetime.now(UTC).isoformat(),
        }
        self._storage.save_session(
            self._session_id,
            dataclasses.asdict(self.config.scope),
            stats,
        )
        self._storage.close()
        return stats

    def _generate_for_category(self, cat: AbatementCategory) -> AbatementRecord | None:
        """Call the LLM to generate a seed record for one category.

        Retries up to config.max_retries times on failure.
        Returns None if all attempts fail or produce no valid record.
        """
        prompt = _SEED_PROMPT.format(
            category_slug=cat.slug,
            category_name=cat.name,
            category_description=cat.description,
            subcategories=", ".join(cat.subcategories),
            schema_description=SCHEMA_DESCRIPTION,
            year=datetime.now(UTC).year,
        )

        for attempt in range(self.config.max_retries + 1):
            try:
                raw = self._extractor._call_llm(prompt)
                records_data = self._extractor._validate_and_parse(raw)
                for data in records_data:
                    data["measure_slug"] = LLMExtractor._make_slug(
                        data.get("measure_name", cat.slug)
                    )
                    cleaned = {k: v for k, v in data.items() if not k.endswith("_uncertain")}
                    cleaned.setdefault("extraction_method", "llm_seed")
                    try:
                        record = AbatementRecord(**cleaned)
                        if _has_paired_data(record):
                            return record
                        logger.debug(
                            "Seed record for %s missing cost or abatement data — retrying.",
                            cat.slug,
                        )
                    except Exception as exc:
                        logger.debug("Record parse failed for %s: %s", cat.slug, exc)
            except Exception as exc:
                logger.warning(
                    "Seed generation attempt %d/%d failed for %s: %s",
                    attempt + 1,
                    self.config.max_retries + 1,
                    cat.slug,
                    exc,
                )
                if attempt < self.config.max_retries:
                    time.sleep(2**attempt)

        return None
