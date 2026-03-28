"""LLM-based lookup of applicable abatement categories for a given context."""

from __future__ import annotations

import json
import logging
import re

from .config import CrawlerConfig
from .extraction import LLMExtractor
from .taxonomy import CATEGORIES, CATEGORY_LOOKUP, AbatementCategory

logger = logging.getLogger(__name__)

_APPLICABILITY_PROMPT = """\
You are a carbon abatement expert. Given the context below, identify which of the \
provided abatement categories are applicable.

Context:
  Sector:     {sector}
  Process:    {process}
  Asset type: {asset_type}

Available abatement categories (slug — name — description):
{category_list}

For each category, decide whether it is applicable to this context.
A category is applicable if it could plausibly reduce emissions for the given \
sector / process / asset, even if the potential is small.

Return a JSON object with two keys:
  "applicable": list of slugs that ARE applicable
  "rationale": object mapping each applicable slug to a one-sentence explanation

Return only valid JSON. No markdown fences, no explanation outside the JSON.
"""


def get_applicable_categories(
    config: CrawlerConfig,
    sector: str = "",
    process: str = "",
    asset_type: str = "",
) -> tuple[list[AbatementCategory], dict[str, str]]:
    """Ask the LLM which taxonomy categories apply to the given context.

    Args:
        config: Crawler configuration (provides LLM credentials/model).
        sector: e.g. "steel manufacturing", "commercial buildings", "agriculture"
        process: e.g. "electric arc furnace", "HVAC", "anaerobic digestion"
        asset_type: e.g. "furnace", "chiller", "dairy herd"

    Returns:
        Tuple of:
          - list of applicable AbatementCategory objects (ordered as in taxonomy)
          - dict mapping slug → one-sentence rationale
    """
    extractor = LLMExtractor(config)
    if not extractor._client:
        raise RuntimeError("LLM client not available — check ANTHROPIC_API_KEY.")

    category_list = "\n".join(
        f"  {c.slug} — {c.name} — {c.description}" for c in CATEGORIES
    )

    prompt = _APPLICABILITY_PROMPT.format(
        sector=sector or "(not specified)",
        process=process or "(not specified)",
        asset_type=asset_type or "(not specified)",
        category_list=category_list,
    )

    raw = extractor._call_llm(prompt)

    # Strip markdown fences if present
    raw = re.sub(r"^```(?:json)?\s*", "", raw.strip(), flags=re.IGNORECASE)
    raw = re.sub(r"\s*```$", "", raw.strip())

    result = json.loads(raw)
    applicable_slugs: list[str] = result.get("applicable", [])
    rationale: dict[str, str] = result.get("rationale", {})

    # Preserve taxonomy ordering and ignore unknown slugs
    applicable = [c for c in CATEGORIES if c.slug in applicable_slugs]
    unknown = [s for s in applicable_slugs if s not in CATEGORY_LOOKUP]
    if unknown:
        logger.warning("LLM returned unknown category slugs: %s", unknown)

    return applicable, rationale
