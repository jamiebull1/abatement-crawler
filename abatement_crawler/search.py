"""Query construction and search API client."""

from __future__ import annotations

import logging
import time
from itertools import product
from typing import Any

from .config import CrawlerConfig
from .models import ScopeConfig

logger = logging.getLogger(__name__)

ABATEMENT_TERMS = [
    "carbon abatement",
    "decarbonisation",
    "GHG reduction",
    "emission reduction",
]

COST_TERMS = [
    "cost",
    "CAPEX",
    "marginal abatement cost",
    "£/tCO2e",
]

GEOGRAPHY_MAP: dict[str, str] = {
    "uk": "UK",
    "united kingdom": "UK",
    "gb": "UK",
    "us": "USA",
    "united states": "USA",
    "eu": "Europe",
    "global": "global",
}


class QueryBuilder:
    """Builds a matrix of search queries from a ScopeConfig."""

    def __init__(self, scope: ScopeConfig) -> None:
        self.scope = scope

    def build_queries(self) -> list[str]:
        """Build query matrix from scope config.

        query = [abatement_type | "carbon abatement" | "decarbonisation"]
              × [asset_type | process | sector]
              × ["cost" | "CAPEX" | "marginal abatement cost" | "£/tCO2e"]
              × [geography (optional)]
        """
        abatement_axis: list[str] = (
            self.scope.abatement_types if self.scope.abatement_types else ABATEMENT_TERMS[:2]
        )
        topic_axis: list[str] = []
        if self.scope.asset_type:
            topic_axis.append(self.scope.asset_type)
        if self.scope.process:
            topic_axis.append(self.scope.process)
        topic_axis.extend(self.scope.sectors)
        if not topic_axis:
            topic_axis = ["industry"]

        cost_axis = COST_TERMS
        geo_axis: list[str | None] = (
            [g for g in self.scope.geography] if self.scope.geography else [None]
        )

        queries: list[str] = []
        for abatement, topic, cost, geo in product(
            abatement_axis, topic_axis, cost_axis, geo_axis
        ):
            parts = [abatement, topic, cost]
            if geo:
                parts.append(geo)
            queries.append(" ".join(parts))

        # Deduplicate while preserving order
        seen: set[str] = set()
        unique: list[str] = []
        for q in queries:
            if q not in seen:
                seen.add(q)
                unique.append(q)

        if self.scope.company:
            unique.extend(self.build_company_queries())

        return unique

    def build_company_queries(self) -> list[str]:
        """Build company-specific queries."""
        company = self.scope.company
        if not company:
            return []
        base = [
            f"{company} carbon abatement cost",
            f"{company} decarbonisation plan cost",
            f"{company} net zero cost CAPEX",
            f"{company} GHG reduction marginal abatement cost",
        ]
        geo_queries: list[str] = []
        for geo in self.scope.geography:
            geo_queries.append(f"{company} {geo} carbon abatement")
        return base + geo_queries


class SearchClient:
    """Wraps DuckDuckGo / SerpAPI / Google CSE / Bing search."""

    def __init__(self, config: CrawlerConfig) -> None:
        self.config = config
        self._rate_limit_delay = 1.0 / max(config.requests_per_second, 0.1)

    def search(self, query: str) -> list[dict[str, str]]:
        """Execute a search query and return results.

        Returns:
            List of dicts with keys: url, title, snippet.
        """
        api = self.config.search_api.lower()
        if api == "duckduckgo":
            return self._search_duckduckgo(query)
        elif api == "serpapi":
            return self._search_serpapi(query)
        elif api in ("google_cse", "google"):
            return self._search_google_cse(query)
        elif api == "bing":
            return self._search_bing(query)
        else:
            logger.warning("Unknown search API '%s', returning empty results.", api)
            return []

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _search_duckduckgo(self, query: str) -> list[dict[str, str]]:
        try:
            from ddgs import DDGS  # noqa: PLC0415
        except ImportError:
            logger.error("ddgs library not available; install ddgs")
            return []

        try:
            time.sleep(self._rate_limit_delay)
            with DDGS() as ddgs:
                items = list(ddgs.text(query, max_results=self.config.results_per_query))
            return [
                {
                    "url": item.get("href", ""),
                    "title": item.get("title", ""),
                    "snippet": item.get("body", ""),
                }
                for item in (items or [])
            ]
        except Exception as exc:
            logger.error("DuckDuckGo search failed: %s", exc)
            return []

    def _search_serpapi(self, query: str) -> list[dict[str, str]]:
        try:
            import requests  # noqa: PLC0415
        except ImportError:
            logger.error("requests library not available")
            return []

        if not self.config.search_api_key:
            logger.warning("No search API key configured; skipping search.")
            return []

        params = {
            "q": query,
            "api_key": self.config.search_api_key,
            "num": self.config.results_per_query,
            "hl": "en",
        }
        try:
            time.sleep(self._rate_limit_delay)
            resp = requests.get(
                "https://serpapi.com/search", params=params, timeout=15
            )
            resp.raise_for_status()
            data = resp.json()
            results = []
            for item in data.get("organic_results", []):
                results.append(
                    {
                        "url": item.get("link", ""),
                        "title": item.get("title", ""),
                        "snippet": item.get("snippet", ""),
                    }
                )
            return results
        except Exception as exc:
            logger.error("SerpAPI search failed: %s", exc)
            return []

    def _search_google_cse(self, query: str) -> list[dict[str, str]]:
        try:
            import requests  # noqa: PLC0415
        except ImportError:
            return []

        if not self.config.search_api_key:
            return []

        params: dict[str, Any] = {
            "q": query,
            "key": self.config.search_api_key,
            "num": min(self.config.results_per_query, 10),
        }
        try:
            time.sleep(self._rate_limit_delay)
            resp = requests.get(
                "https://www.googleapis.com/customsearch/v1", params=params, timeout=15
            )
            resp.raise_for_status()
            data = resp.json()
            return [
                {
                    "url": item.get("link", ""),
                    "title": item.get("title", ""),
                    "snippet": item.get("snippet", ""),
                }
                for item in data.get("items", [])
            ]
        except Exception as exc:
            logger.error("Google CSE search failed: %s", exc)
            return []

    def _search_bing(self, query: str) -> list[dict[str, str]]:
        try:
            import requests  # noqa: PLC0415
        except ImportError:
            return []

        if not self.config.search_api_key:
            return []

        headers = {"Ocp-Apim-Subscription-Key": self.config.search_api_key}
        params = {"q": query, "count": self.config.results_per_query}
        try:
            time.sleep(self._rate_limit_delay)
            resp = requests.get(
                "https://api.bing.microsoft.com/v7.0/search",
                headers=headers,
                params=params,
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            return [
                {
                    "url": item.get("url", ""),
                    "title": item.get("name", ""),
                    "snippet": item.get("snippet", ""),
                }
                for item in data.get("webPages", {}).get("value", [])
            ]
        except Exception as exc:
            logger.error("Bing search failed: %s", exc)
            return []
