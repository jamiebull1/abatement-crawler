"""Tests for relevance scoring."""

import pytest

from abatement_crawler.models import ScopeConfig
from abatement_crawler.relevance import score_relevance, COST_TERMS, QUALITY_DOMAINS


def make_scope(**kwargs) -> ScopeConfig:
    defaults = dict(
        sectors=["steel", "industry"],
        abatement_types=["fuel switch", "efficiency"],
        geography=["UK"],
    )
    defaults.update(kwargs)
    return ScopeConfig(**defaults)


class TestScoreRelevance:
    def test_returns_float_between_0_and_1(self):
        scope = make_scope()
        score = score_relevance(
            title="Carbon abatement cost in steel industry",
            snippet="Marginal abatement cost for fuel switching in UK steel sector",
            url="https://beis.gov.uk/report",
            scope=scope,
        )
        assert 0.0 <= score <= 1.0

    def test_high_score_for_relevant_document(self):
        scope = make_scope()
        score = score_relevance(
            title="Marginal abatement cost steel industry fuel switch UK",
            snippet="CAPEX cost efficiency abatement carbon emissions tCO2e",
            url="https://beis.gov.uk/carbon-report",
            scope=scope,
        )
        assert score > 0.3

    def test_low_score_for_irrelevant_document(self):
        scope = make_scope()
        score = score_relevance(
            title="Football match results Saturday Premier League",
            snippet="Goals scored and match highlights from weekend football",
            url="https://random-sports-site.com/football",
            scope=scope,
        )
        assert score < 0.4

    def test_high_quality_domain_increases_score(self):
        scope = make_scope()
        beis_score = score_relevance(
            title="report",
            snippet="",
            url="https://beis.gov.uk/document",
            scope=scope,
        )
        random_score = score_relevance(
            title="report",
            snippet="",
            url="https://random-site-xyz.com/document",
            scope=scope,
        )
        assert beis_score > random_score

    def test_cost_terms_in_snippet_increase_score(self):
        scope = make_scope()
        with_cost = score_relevance(
            title="Steel decarbonisation",
            snippet="marginal abatement cost CAPEX capital cost analysis",
            url="https://example.com",
            scope=scope,
        )
        without_cost = score_relevance(
            title="Steel decarbonisation",
            snippet="general overview of the industry sector",
            url="https://example.com",
            scope=scope,
        )
        assert with_cost > without_cost

    def test_anchor_text_contribution(self):
        scope = make_scope()
        with_anchor = score_relevance(
            title="Report",
            snippet="",
            url="https://example.com",
            scope=scope,
            anchor_text="marginal abatement cost carbon",
        )
        without_anchor = score_relevance(
            title="Report",
            snippet="",
            url="https://example.com",
            scope=scope,
            anchor_text="",
        )
        assert with_anchor >= without_anchor

    def test_quality_domains_dict_not_empty(self):
        assert len(QUALITY_DOMAINS) > 0
        assert "beis.gov.uk" in QUALITY_DOMAINS
        assert "iea.org" in QUALITY_DOMAINS

    def test_cost_terms_list_not_empty(self):
        assert len(COST_TERMS) > 0
        assert "capex" in COST_TERMS
        assert "mac" in COST_TERMS

    def test_scope_with_company(self):
        scope = make_scope(company="ACME Steel")
        score = score_relevance(
            title="ACME Steel carbon abatement cost analysis",
            snippet="CAPEX for fuel switching at ACME Steel facilities",
            url="https://example.com",
            scope=scope,
        )
        assert score > 0.2
