"""Tests for AbatementRecord and ScopeConfig models."""

import re
from datetime import date

from abatement_crawler.models import AbatementRecord, ScopeConfig


def make_minimal_record(**kwargs) -> AbatementRecord:
    """Create an AbatementRecord with minimum required fields."""
    defaults = dict(
        measure_name="Electric boiler replacement",
        measure_slug="electric-boiler-replacement",
        abatement_category="fuel_switch",
        sector="buildings",
        scope_tag="scope_1",
        geography="GBR",
        publication_year=2022,
        source_url="https://example.com/report.pdf",
        source_title="Test Report",
        source_type="government",
    )
    defaults.update(kwargs)
    return AbatementRecord(**defaults)


class TestAbatementRecord:
    def test_create_with_required_fields(self):
        record = make_minimal_record()
        assert record.measure_name == "Electric boiler replacement"
        assert record.sector == "buildings"

    def test_record_id_is_auto_generated_uuid(self):
        record1 = make_minimal_record()
        record2 = make_minimal_record()
        # Both should be valid UUIDs
        uuid_pattern = re.compile(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        )
        assert uuid_pattern.match(record1.record_id)
        assert uuid_pattern.match(record2.record_id)
        # Each should be unique
        assert record1.record_id != record2.record_id

    def test_retrieved_date_is_today(self):
        record = make_minimal_record()
        today = date.today().isoformat()
        assert record.retrieved_date == today

    def test_optional_fields_default_to_none(self):
        record = make_minimal_record()
        assert record.capex is None
        assert record.opex_delta is None
        assert record.mac is None
        assert record.doi is None
        assert record.asset_type is None

    def test_list_fields_default_to_empty(self):
        record = make_minimal_record()
        assert record.dependencies == []
        assert record.co_benefits == []
        assert record.barriers == []
        assert record.authors == []
        assert record.quality_flags == []

    def test_default_currency_is_gbp(self):
        record = make_minimal_record()
        assert record.currency == "GBP"

    def test_default_quality_score(self):
        record = make_minimal_record()
        assert record.quality_score == 0.0

    def test_default_implementation_complexity(self):
        record = make_minimal_record()
        assert record.implementation_complexity == "medium"

    def test_full_text_restricted_defaults_false(self):
        record = make_minimal_record()
        assert record.full_text_restricted is False

    def test_explicit_record_id_is_respected(self):
        record = make_minimal_record(record_id="custom-id-123")
        assert record.record_id == "custom-id-123"

    def test_cost_fields_can_be_set(self):
        record = make_minimal_record(
            capex=50000.0,
            capex_unit="£/unit",
            opex_delta=-200.0,
            lifetime_years=15,
            mac=35.5,
        )
        assert record.capex == 50000.0
        assert record.opex_delta == -200.0
        assert record.lifetime_years == 15
        assert record.mac == 35.5


class TestScopeConfig:
    def test_default_scope_config(self):
        scope = ScopeConfig()
        assert scope.industry is None
        assert scope.geography == []
        assert scope.languages == ["en"]

    def test_scope_config_with_values(self):
        scope = ScopeConfig(
            industry="steel",
            sectors=["industry", "manufacturing"],
            geography=["UK", "EU"],
            year_range=(2018, 2023),
        )
        assert scope.industry == "steel"
        assert scope.sectors == ["industry", "manufacturing"]
        assert scope.geography == ["UK", "EU"]
        assert scope.year_range == (2018, 2023)
