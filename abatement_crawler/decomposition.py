"""3-layer pipeline: sector decomposition (Layer 1) and archetype mapping (Layer 2)."""

from __future__ import annotations

import dataclasses
import json
import logging
import re
import time
from pathlib import Path

from .config import CrawlerConfig
from .models import AbatementArchetype, AssetGroup, SectorDecomposition

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

DECOMPOSITION_PROMPT = """You are an expert in greenhouse gas emissions and decarbonisation.

Break down the following sector into its emissions-relevant asset groups and activities.
Be specific to this sector — avoid generic labels like "transport" or "buildings".
Capture operational nuances (e.g. idling for standby readiness, blue-light duty cycles,
standby power loads, refrigerants, supply-chain procurement).

Sector: {sector}
Geography: {geography}

Return a JSON array where each element is an asset group:
[
  {{
    "name": "string – concise asset group label",
    "description": "string – 1-2 sentences on what it covers and why it matters for emissions",
    "emission_sources": ["list of specific emission mechanisms, e.g. 'diesel combustion during response'"],
    "scope_tag": "scope_1 | scope_2 | scope_3 | multiple"
  }}
]

Order groups from highest to lowest estimated emissions significance.
Return only valid JSON. No markdown, no explanation outside the JSON.
"""

ARCHETYPE_PROMPT = """You are an expert in carbon abatement measures and decarbonisation pathways.

For each asset group below, identify all practically relevant abatement archetypes.
For each archetype generate the fields described. Be specific and quantitative where possible.
Include both near-term (available now) and medium-term (5–10 year horizon) measures.
Do NOT skip niche but high-impact measures (e.g. idle reduction, F-gas management).

Sector: {sector}
Geography: {geography}

Asset groups:
{asset_groups_json}

Return a JSON array where each element is an abatement archetype:
[
  {{
    "name": "string – concise archetype label, e.g. 'Diesel → HVO (heavy fleet)'",
    "category": "fuel_switch | efficiency | behaviour | carbon_capture | process_change | material_sub",
    "asset_group": "string – must exactly match one of the asset group names above",
    "mechanism": "string – how the abatement works",
    "baseline": "string – what technology/fuel/practice is being replaced",
    "abatement_driver": "string – what drives the emissions reduction",
    "typical_abatement_pct_min": number or null,
    "typical_abatement_pct_max": number or null,
    "key_variables": ["list of parameters needed to quantify: e.g. 'annual mileage (km/year)'"],
    "cost_drivers": ["list of cost factors"],
    "constraints": ["list of barriers or constraints"],
    "search_queries": [
      "5-8 specific search queries optimised to find cost + abatement data for this archetype",
      "include variants with £/tCO2e, marginal abatement cost, CAPEX, lifecycle cost",
      "include sector-specific terms",
      "e.g. 'HVO fuel switching fire appliance fleet lifecycle emissions cost UK'"
    ],
    "analogue_sectors": ["list of analogous sectors whose evidence can be borrowed, e.g. 'refuse collection vehicles'"]
  }}
]

Return only valid JSON. No markdown, no explanation outside the JSON.
"""


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


class SectorDecomposer:
    """Runs Layer 1 (sector decomposition) and Layer 2 (archetype mapping) via Claude."""

    def __init__(self, config: CrawlerConfig) -> None:
        self.config = config
        self._client = None
        self._init_client()

    def _init_client(self) -> None:
        if not self.config.llm_api_key:
            logger.warning("No LLM API key configured. Decomposition will be unavailable.")
            return
        try:
            import anthropic  # noqa: PLC0415

            self._client = anthropic.Anthropic(api_key=self.config.llm_api_key)
        except ImportError:
            logger.warning("anthropic library not installed. Decomposition unavailable.")

    # ------------------------------------------------------------------
    # Layer 1
    # ------------------------------------------------------------------

    def decompose(self, sector: str, geography: list[str]) -> SectorDecomposition:
        """Layer 1: Ask Claude to break a sector into emissions-relevant asset groups.

        Persists the result to ``{output_dir}/decomposition_{sector_slug}.json``.

        Returns:
            SectorDecomposition with populated asset_groups.
        """
        geo_str = ", ".join(geography) if geography else "global"
        logger.info("Layer 1: decomposing sector '%s' (%s).", sector, geo_str)

        raw_groups = self._call_llm(
            DECOMPOSITION_PROMPT.format(sector=sector, geography=geo_str),
            max_tokens=2048,
        )
        groups_data = self._parse_json_array(raw_groups)

        asset_groups: list[AssetGroup] = []
        for item in groups_data:
            if not isinstance(item, dict) or "name" not in item:
                continue
            asset_groups.append(
                AssetGroup(
                    name=item.get("name", ""),
                    description=item.get("description", ""),
                    emission_sources=item.get("emission_sources", []),
                    scope_tag=item.get("scope_tag", "scope_1"),
                )
            )

        decomposition = SectorDecomposition(
            sector=sector,
            geography=geography,
            asset_groups=asset_groups,
        )

        self._persist(
            f"decomposition_{_make_slug(sector)}.json",
            [dataclasses.asdict(ag) for ag in asset_groups],
        )
        logger.info("Layer 1 complete: %d asset groups identified.", len(asset_groups))
        return decomposition

    # ------------------------------------------------------------------
    # Layer 2
    # ------------------------------------------------------------------

    def map_archetypes(self, decomposition: SectorDecomposition) -> list[AbatementArchetype]:
        """Layer 2: Map each asset group to abatement archetypes.

        Persists the result to ``{output_dir}/archetypes_{sector_slug}.json``.

        Returns:
            List of AbatementArchetype objects, one per (asset × measure) combination.
        """
        geo_str = ", ".join(decomposition.geography) if decomposition.geography else "global"
        logger.info(
            "Layer 2: mapping archetypes for %d asset groups.", len(decomposition.asset_groups)
        )

        asset_groups_json = json.dumps(
            [dataclasses.asdict(ag) for ag in decomposition.asset_groups],
            indent=2,
        )
        raw_archetypes = self._call_llm(
            ARCHETYPE_PROMPT.format(
                sector=decomposition.sector,
                geography=geo_str,
                asset_groups_json=asset_groups_json,
            ),
            max_tokens=4096,
        )
        archetypes_data = self._parse_json_array(raw_archetypes)

        archetypes: list[AbatementArchetype] = []
        for item in archetypes_data:
            if not isinstance(item, dict) or "name" not in item:
                continue
            archetypes.append(
                AbatementArchetype(
                    name=item.get("name", ""),
                    category=item.get("category", "efficiency"),
                    asset_group=item.get("asset_group", ""),
                    mechanism=item.get("mechanism", ""),
                    baseline=item.get("baseline", ""),
                    abatement_driver=item.get("abatement_driver", ""),
                    typical_abatement_pct_min=item.get("typical_abatement_pct_min"),
                    typical_abatement_pct_max=item.get("typical_abatement_pct_max"),
                    key_variables=item.get("key_variables", []),
                    cost_drivers=item.get("cost_drivers", []),
                    constraints=item.get("constraints", []),
                    search_queries=item.get("search_queries", []),
                    analogue_sectors=item.get("analogue_sectors", []),
                )
            )

        self._persist(
            f"archetypes_{_make_slug(decomposition.sector)}.json",
            [dataclasses.asdict(a) for a in archetypes],
        )
        logger.info("Layer 2 complete: %d archetypes generated.", len(archetypes))
        for a in archetypes:
            logger.info("  [%s] %s", a.category, a.name)
        return archetypes

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _call_llm(self, prompt: str, max_tokens: int = 4096) -> str:
        """Call Claude and return the response text."""
        if not self._client:
            raise RuntimeError("LLM client not available — check llm_api_key.")

        for attempt in range(self.config.max_retries + 1):
            try:
                message = self._client.messages.create(
                    model=self.config.llm_model,
                    max_tokens=max_tokens,
                    temperature=0,
                    messages=[{"role": "user", "content": prompt}],
                )
                return message.content[0].text
            except Exception as exc:
                logger.warning(
                    "LLM call attempt %d/%d failed: %s",
                    attempt + 1,
                    self.config.max_retries + 1,
                    exc,
                )
                if attempt < self.config.max_retries:
                    time.sleep(2**attempt)
        raise RuntimeError("All LLM call attempts failed.")

    @staticmethod
    def _parse_json_array(raw: str) -> list[dict]:
        """Strip markdown fences and parse JSON array from LLM response."""
        raw = re.sub(r"^```(?:json)?\s*", "", raw.strip(), flags=re.IGNORECASE)
        raw = re.sub(r"\s*```$", "", raw.strip())
        data = json.loads(raw)
        if isinstance(data, dict):
            # Some models wrap the array in a key
            for v in data.values():
                if isinstance(v, list):
                    return v
            return [data]
        if isinstance(data, list):
            return data
        return []

    def _persist(self, filename: str, data: list[dict]) -> None:
        """Write data as JSON to the configured output directory."""
        output_path = Path(self.config.output_dir) / filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info("Saved %s", output_path)
