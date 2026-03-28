"""Tests for quality scoring."""

import pytest

from abatement_crawler.models import AbatementRecord
from abatement_crawler.quality import score_quality


def make_record(**kwargs) -> AbatementRecord:
    defaults = dict(
        measure_name="Test measure",
        measure_slug="test-measure",
        abatement_category="efficiency",
        sector="industry",
        scope_tag="scope_1",
        geography="GBR",
        publication_year=2022,
        source_url="https://example.com",
        source_title="Test",
        source_type="academic",
        peer_reviewed=True,
        extraction_confidence=0.9,
    )
    defaults.update(kwargs)
    return AbatementRecord(**defaults)


class TestScoreQuality:
    def test_score_is_between_0_and_1(self):
        record = make_record()
        score, _ = score_quality(record)
        assert 0.0 <= score <= 1.0

    def test_full_record_scores_higher_than_sparse(self):
        full_record = make_record(
            capex=10000.0,
            opex_delta=-100.0,
            mac=25.0,
            lifetime_years=20,
            abatement_potential_tco2e=500.0,
            baseline_description="Coal-fired heating system",
            source_type="academic",
            peer_reviewed=True,
            extraction_confidence=0.95,
            geography="GBR",
        )
        sparse_record = make_record(
            source_type="company_report",
            peer_reviewed=False,
            extraction_confidence=0.2,
            geography="GLOBAL",
            publication_year=2010,
        )
        full_score, _ = score_quality(full_record)
        sparse_score, _ = score_quality(sparse_record)
        assert full_score > sparse_score

    def test_no_capex_flag(self):
        record = make_record(capex=None)
        _, flags = score_quality(record)
        assert "no_capex" in flags

    def test_no_opex_flag(self):
        record = make_record(opex_delta=None, opex_fixed=None)
        _, flags = score_quality(record)
        assert "no_opex" in flags

    def test_no_carbon_data_flag(self):
        record = make_record(
            abatement_potential_tco2e=None, abatement_percentage=None
        )
        _, flags = score_quality(record)
        assert "no_carbon_data" in flags

    def test_old_data_flag(self):
        record = make_record(publication_year=2005, data_year=2005)
        _, flags = score_quality(record)
        assert "old_data" in flags

    def test_recent_data_no_old_data_flag(self):
        record = make_record(publication_year=2023)
        _, flags = score_quality(record)
        assert "old_data" not in flags

    def test_source_type_priors(self):
        academic = make_record(source_type="academic")
        company = make_record(source_type="company_report")
        a_score, _ = score_quality(academic)
        c_score, _ = score_quality(company)
        assert a_score > c_score

    def test_peer_reviewed_scores_higher(self):
        reviewed = make_record(peer_reviewed=True)
        not_reviewed = make_record(peer_reviewed=False)
        r_score, _ = score_quality(reviewed)
        nr_score, _ = score_quality(not_reviewed)
        assert r_score > nr_score

    def test_country_geography_scores_higher_than_global(self):
        country = make_record(geography="GBR")
        global_ = make_record(geography="GLOBAL")
        c_score, _ = score_quality(country)
        g_score, _ = score_quality(global_)
        assert c_score > g_score

    def test_capex_and_opex_both_present_max_cost_score(self):
        both = make_record(capex=1000.0, opex_delta=-50.0)
        only_capex = make_record(capex=1000.0, opex_delta=None)
        both_score, _ = score_quality(both)
        only_capex_score, _ = score_quality(only_capex)
        assert both_score > only_capex_score
