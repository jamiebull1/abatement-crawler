"""Pre-fetch relevance scoring."""

from __future__ import annotations

from urllib.parse import urlparse

from .models import ScopeConfig

COST_TERMS: list[str] = [
    "cost",
    "capex",
    "opex",
    "marginal abatement cost",
    "£/tco2e",
    "$/tco2e",
    "mac",
    "capital cost",
    "levelised cost",
    "abatement cost",
]

CARBON_TERMS: list[str] = [
    "carbon",
    "co2",
    "ghg",
    "greenhouse gas",
    "emission",
    "abatement",
    "decarbonisation",
    "net zero",
    "climate",
]

# Domain quality priors (0–1 scale)
QUALITY_DOMAINS: dict[str, float] = {
    "beis.gov.uk": 0.9,
    "iea.org": 0.9,
    "ipcc.ch": 0.95,
    "gov.uk": 0.85,
    "academic.oup.com": 0.8,
    "sciencedirect.com": 0.8,
    "springer.com": 0.8,
    "nature.com": 0.85,
    "wiley.com": 0.75,
    "tandfonline.com": 0.75,
    "pubs.acs.org": 0.8,
    "nrel.gov": 0.85,
    "epa.gov": 0.85,
    "ec.europa.eu": 0.85,
    "eur-lex.europa.eu": 0.8,
    "worldbank.org": 0.8,
    "un.org": 0.75,
    "irena.org": 0.85,
    "carbonbrief.org": 0.7,
    "climateactiontracker.org": 0.75,
    "ccc.gov.uk": 0.9,
    "theccc.org.uk": 0.9,
    "nesta.org.uk": 0.7,
    "rmi.org": 0.75,
    "mckinsey.com": 0.7,
    "deloitte.com": 0.65,
    "pwc.com": 0.65,
    "accenture.com": 0.65,
    "bnef.com": 0.8,
    "woodmac.com": 0.75,
}

_DEFAULT_DOMAIN_SCORE = 0.4


def _keyword_density(text: str, keywords: list[str]) -> float:
    """Return fraction of keywords present in text (case-insensitive)."""
    if not text or not keywords:
        return 0.0
    text_lower = text.lower()
    matches = sum(1 for kw in keywords if kw.lower() in text_lower)
    return matches / len(keywords)


def _domain_prior(url: str) -> float:
    """Return quality prior for the URL's domain."""
    try:
        hostname = urlparse(url).hostname or ""
    except Exception:
        return _DEFAULT_DOMAIN_SCORE

    for domain, score in QUALITY_DOMAINS.items():
        if hostname == domain or hostname.endswith("." + domain):
            return score
    return _DEFAULT_DOMAIN_SCORE


def _scope_keywords(scope: ScopeConfig) -> list[str]:
    """Derive relevant keywords from scope configuration."""
    keywords: list[str] = list(CARBON_TERMS)
    if scope.industry:
        keywords.append(scope.industry)
    if scope.asset_type:
        keywords.append(scope.asset_type)
    if scope.process:
        keywords.append(scope.process)
    if scope.company:
        keywords.append(scope.company)
    keywords.extend(scope.sectors)
    keywords.extend(scope.abatement_types)
    return keywords


def score_relevance(
    title: str,
    snippet: str,
    url: str,
    scope: ScopeConfig,
    anchor_text: str = "",
) -> float:
    """Compute pre-fetch relevance score for a search result.

    relevance = weighted_sum(
        keyword_match(title, scope_keywords),       weight 0.4
        keyword_match(snippet, cost_terms),          weight 0.3
        source_domain_prior(url),                    weight 0.2
        citation_context_match(anchor_text),         weight 0.1
    )

    Returns:
        Float in [0.0, 1.0].
    """
    s_keywords = _scope_keywords(scope)

    title_score = _keyword_density(title, s_keywords)
    snippet_cost_score = _keyword_density(snippet, COST_TERMS)
    domain_score = _domain_prior(url)
    anchor_score = _keyword_density(anchor_text, COST_TERMS + CARBON_TERMS) if anchor_text else 0.0

    score = (
        0.4 * title_score
        + 0.3 * snippet_cost_score
        + 0.2 * domain_score
        + 0.1 * anchor_score
    )

    return min(1.0, max(0.0, score))
