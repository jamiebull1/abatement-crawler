"""Layer 4: per-archetype synthesis of best-estimate abatement records."""

from __future__ import annotations

import json
import logging
import re
import time
from uuid import uuid4

from .config import CrawlerConfig
from .extraction import SCHEMA_DESCRIPTION
from .models import AbatementArchetype, AbatementRecord

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

SYNTHESIS_PROMPT = """You are synthesising a best-estimate carbon abatement record for a \
specific decarbonisation measure. Your job is to combine all available evidence into one \
coherent, well-parameterised record, and to be explicit about every assumption you make.

ARCHETYPE DEFINITION:
  Name:             {name}
  Category:         {category}
  Asset group:      {asset_group}
  Mechanism:        {mechanism}
  Baseline:         {baseline}
  Abatement driver: {abatement_driver}
  Typical abatement range: {pct_min}–{pct_max}%
  Key variables needed: {key_variables}
  Known constraints: {constraints}

EVIDENCE ({n_complete} complete record(s), {n_fragments} partial fragment(s)):
{evidence_summary}

{activity_section}TASK:
Synthesise a single best-estimate AbatementRecord using this schema:
{schema_description}

Rules:
1. Prefer values from higher-quality sources (government > academic > consultancy > industry).
2. Where a key variable is absent from evidence, use the archetype's typical range as a \
fallback and list it in synthesis_assumptions.
3. If abatement_potential_tco2e is missing but abatement_percentage is available, leave \
abatement_potential_tco2e null and explain in notes.
4. Derive mac from capex + opex_delta + lifetime_years if mac is not directly available \
(the normaliser will recalculate it downstream).
5. Set extraction_method to "llm_synthesis".
6. Set is_synthesised to true.
7. Set synthesis_sources to the list of record_id / fragment_id values you drew on.
8. Set synthesis_assumptions — list EVERY value that was inferred rather than directly \
measured. Be specific: "abatement_percentage assumed 80% from archetype typical range \
(no direct measurement found)" is the right level of detail.
9. Set extraction_confidence:
   - 0.8–1.0: strong empirical evidence from multiple sources
   - 0.5–0.8: moderate evidence with some gaps filled by inference
   - 0.2–0.5: primarily inferred from archetype definition and analogues
   - 0.0–0.2: almost entirely assumed, very thin evidence
10. If evidence is completely absent AND the archetype has no typical range, return null.
11. Set source_type to "technology_catalogue" unless a single source dominates.

Return a single JSON object matching the schema, or the literal null if evidence is \
insufficient. No markdown, no explanation outside the JSON.
"""

ACTIVITY_DATA_SECTION = """ACTIVITY INTENSITY DATA (from targeted search for missing key variables):
{activity_summary}

"""


def _fmt_record_summary(record: AbatementRecord, label: str) -> str:
    """One-line summary of an AbatementRecord for inclusion in the synthesis prompt."""
    parts = [f"[{label}] {record.measure_name}"]
    if record.mac is not None:
        parts.append(f"MAC={record.mac:.0f} {record.currency}/tCO2e")
    if record.capex is not None:
        parts.append(f"capex={record.capex:.0f}")
    if record.abatement_percentage is not None:
        parts.append(f"abatement={record.abatement_percentage:.0f}%")
    if record.abatement_potential_tco2e is not None:
        parts.append(f"potential={record.abatement_potential_tco2e:.1f} tCO2e")
    parts.append(f"quality={record.quality_score:.2f}")
    parts.append(f"source={record.source_type}")
    parts.append(f"id={record.record_id[:8]}")
    return "  " + " | ".join(parts)


class ArchetypeSynthesiser:
    """Synthesises a best-estimate AbatementRecord for each archetype from all evidence."""

    def __init__(self, config: CrawlerConfig) -> None:
        self.config = config
        self._client = None
        self._init_client()

    def _init_client(self) -> None:
        if not self.config.llm_api_key:
            logger.warning("No LLM API key configured. Synthesis will be unavailable.")
            return
        try:
            import anthropic  # noqa: PLC0415

            self._client = anthropic.Anthropic(api_key=self.config.llm_api_key)
        except ImportError:
            logger.warning("anthropic library not installed. Synthesis unavailable.")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def synthesise(
        self,
        archetype: AbatementArchetype,
        records: list[AbatementRecord],
        fragments: list[AbatementRecord],
        activity_summary: str = "",
    ) -> AbatementRecord | None:
        """Synthesise a best-estimate record for an archetype from all available evidence.

        Args:
            archetype: The archetype definition (mechanism, baseline, key_variables, etc.)
            records: Complete AbatementRecords extracted during Layer 3.
            fragments: Partial evidence fragments (failed paired-data gate).
            activity_summary: Optional plain-text summary of activity intensity data
                              retrieved by a targeted search for missing key variables.

        Returns:
            A synthesised AbatementRecord, or None if evidence is too thin.
        """
        if not self._client:
            logger.warning("LLM client not available; skipping synthesis for '%s'.", archetype.name)
            return None

        n_complete = len(records)
        n_fragments = len(fragments)
        logger.info(
            "Synthesising archetype '%s' from %d records + %d fragments.",
            archetype.name,
            n_complete,
            n_fragments,
        )

        evidence_summary = self._build_evidence_summary(records, fragments)
        activity_section = (
            ACTIVITY_DATA_SECTION.format(activity_summary=activity_summary)
            if activity_summary
            else ""
        )

        pct_min = archetype.typical_abatement_pct_min
        pct_max = archetype.typical_abatement_pct_max
        pct_range = (
            f"{pct_min}–{pct_max}"
            if pct_min is not None and pct_max is not None
            else "unknown"
        )

        prompt = SYNTHESIS_PROMPT.format(
            name=archetype.name,
            category=archetype.category,
            asset_group=archetype.asset_group,
            mechanism=archetype.mechanism,
            baseline=archetype.baseline,
            abatement_driver=archetype.abatement_driver,
            pct_min=pct_min if pct_min is not None else "?",
            pct_max=pct_max if pct_max is not None else "?",
            key_variables=", ".join(archetype.key_variables) or "none specified",
            constraints=", ".join(archetype.constraints) or "none specified",
            evidence_summary=evidence_summary,
            activity_section=activity_section,
            n_complete=n_complete,
            n_fragments=n_fragments,
            schema_description=SCHEMA_DESCRIPTION,
            pct_range=pct_range,
        )

        raw = self._call_llm(prompt)
        return self._parse_synthesised_record(raw, archetype, records, fragments)

    def build_activity_queries(
        self, archetype: AbatementArchetype, geography: list[str]
    ) -> list[str]:
        """Build search queries for activity intensity data for missing key variables.

        Generates 1–2 queries per key_variable designed to find typical operating
        parameters (e.g. 'average fire engine annual mileage UK statistics').
        """
        geo_str = geography[0] if geography else "UK"
        queries: list[str] = []
        for variable in archetype.key_variables:
            # Strip unit hints like "(km/year)" for the query
            clean_var = re.sub(r"\s*\([^)]+\)", "", variable).strip()
            queries.append(f"typical {clean_var} {archetype.asset_group} {geo_str}")
            queries.append(
                f"average {clean_var} {archetype.asset_group} statistics annual"
            )
        return queries

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_evidence_summary(
        self, records: list[AbatementRecord], fragments: list[AbatementRecord]
    ) -> str:
        """Build a structured text summary of all evidence for the prompt."""
        lines: list[str] = []
        if records:
            lines.append("Complete records (both cost and abatement data present):")
            for r in sorted(records, key=lambda x: x.quality_score, reverse=True):
                lines.append(_fmt_record_summary(r, "complete"))
        if fragments:
            lines.append("Partial fragments (cost OR abatement data only):")
            for f in fragments:
                lines.append(_fmt_record_summary(f, "fragment"))
        if not lines:
            return "No evidence found for this archetype."
        return "\n".join(lines)

    def _parse_synthesised_record(
        self,
        raw: str,
        archetype: AbatementArchetype,
        records: list[AbatementRecord],
        fragments: list[AbatementRecord],
    ) -> AbatementRecord | None:
        """Parse the LLM response into an AbatementRecord, or return None."""
        raw = raw.strip()
        # Strip markdown fences
        raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"\s*```$", "", raw)
        raw = raw.strip()

        if raw.lower() == "null" or raw == "":
            logger.info("Synthesis returned null for archetype '%s' — insufficient evidence.", archetype.name)
            return None

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.warning("Synthesis JSON parse error for '%s': %s", archetype.name, exc)
            return None

        if not isinstance(data, dict):
            logger.warning("Synthesis returned non-dict for '%s'.", archetype.name)
            return None

        # Ensure required provenance fields
        data.setdefault("record_id", str(uuid4()))
        data.setdefault("measure_name", archetype.name)
        data.setdefault("measure_slug", _make_slug(archetype.name))
        data.setdefault("abatement_category", archetype.category)
        data.setdefault("extraction_method", "llm_synthesis")
        data.setdefault("source_url", "synthesised")
        data.setdefault("source_title", f"Synthesised: {archetype.name}")
        data.setdefault("source_type", "technology_catalogue")
        data.setdefault("sector", archetype.asset_group)
        data.setdefault("scope_tag", "scope_1")
        data.setdefault("geography", "GBR")
        data.setdefault("publication_year", 2024)

        # Force synthesis flags
        data["is_synthesised"] = True
        data["archetype_slug"] = _make_slug(archetype.name)

        # Merge source IDs from evidence
        existing_sources = data.get("synthesis_sources", [])
        for r in records:
            if r.record_id not in existing_sources:
                existing_sources.append(r.record_id)
        for f in fragments:
            if f.record_id not in existing_sources:
                existing_sources.append(f.record_id)
        data["synthesis_sources"] = existing_sources

        # Remove uncertainty markers
        cleaned = {k: v for k, v in data.items() if not k.endswith("_uncertain")}

        try:
            return AbatementRecord(**cleaned)
        except Exception as exc:
            logger.warning("Failed to build synthesised record for '%s': %s", archetype.name, exc)
            return None

    def _call_llm(self, prompt: str) -> str:
        """Call the Anthropic API and return the response text."""
        for attempt in range(self.config.max_retries + 1):
            try:
                message = self._client.messages.create(
                    model=self.config.llm_model,
                    max_tokens=4096,
                    temperature=0,
                    messages=[{"role": "user", "content": prompt}],
                )
                return message.content[0].text
            except Exception as exc:
                logger.warning(
                    "Synthesis LLM call attempt %d/%d failed: %s",
                    attempt + 1,
                    self.config.max_retries + 1,
                    exc,
                )
                if attempt < self.config.max_retries:
                    time.sleep(2**attempt)
        raise RuntimeError(f"All synthesis LLM call attempts failed after {self.config.max_retries + 1} tries.")


def _make_slug(text: str) -> str:
    """Simple slug from arbitrary text."""
    try:
        from slugify import slugify  # noqa: PLC0415

        return slugify(text)
    except ImportError:
        slug = text.lower()
        slug = re.sub(r"[^\w\s-]", "", slug)
        slug = re.sub(r"[\s_]+", "-", slug)
        return slug.strip("-")
