"""Tests for normalisation routines."""

import pytest

from abatement_crawler.models import AbatementRecord
from abatement_crawler.normalisation import Normaliser


def make_record(**kwargs) -> AbatementRecord:
    defaults = dict(
        measure_name="Heat pump installation",
        measure_slug="heat-pump-installation",
        abatement_category="fuel_switch",
        sector="buildings",
        scope_tag="scope_1",
        geography="United Kingdom",
        publication_year=2022,
        source_url="https://example.com",
        source_title="Test",
        source_type="government",
    )
    defaults.update(kwargs)
    return AbatementRecord(**defaults)


class TestNormaliser:
    def setup_method(self):
        self.normaliser = Normaliser(base_currency="GBP", base_year=2023)

    def test_currency_conversion_usd_to_gbp(self):
        amount_gbp = self.normaliser.convert_currency(100.0, "USD", 2022)
        # USD rate to GBP is ~0.79
        assert 70.0 < amount_gbp < 90.0

    def test_currency_conversion_gbp_identity(self):
        amount = self.normaliser.convert_currency(100.0, "GBP", 2022)
        assert amount == pytest.approx(100.0)

    def test_deflate_price_2020_to_2023(self):
        # 2020 price should increase when deflated to 2023 (deflator_base > deflator_from)
        amount_2023 = self.normaliser.deflate_price(100.0, from_year=2020)
        assert amount_2023 > 100.0

    def test_deflate_price_same_year_is_identity(self):
        amount = self.normaliser.deflate_price(100.0, from_year=2023)
        assert amount == pytest.approx(100.0)

    def test_mac_recalculation_populates_mac(self):
        record = make_record(
            capex=10000.0,
            opex_delta=0.0,
            abatement_potential_tco2e=100.0,
            lifetime_years=10,
            discount_rate=0.035,
            mac=None,
        )
        updated = self.normaliser.recalculate_mac(record)
        assert updated.mac is not None
        assert updated.mac > 0

    def test_mac_recalculation_flags_divergence(self):
        # Source MAC is 10, calculated MAC will be much higher
        record = make_record(
            capex=100000.0,
            opex_delta=0.0,
            abatement_potential_tco2e=100.0,
            lifetime_years=10,
            discount_rate=0.035,
            mac=10.0,  # This will diverge greatly from calculated
        )
        updated = self.normaliser.recalculate_mac(record)
        assert "mac_divergence" in updated.quality_flags

    def test_mac_no_divergence_flag_when_close(self):
        # Calculate expected MAC first
        discount_rate = 0.035
        capex = 10000.0
        lifetime = 10
        abatement = 100.0
        crf = discount_rate / (1 - (1 + discount_rate) ** (-lifetime))
        expected_mac = (capex * crf) / abatement

        record = make_record(
            capex=capex,
            opex_delta=0.0,
            abatement_potential_tco2e=abatement,
            lifetime_years=lifetime,
            discount_rate=discount_rate,
            mac=expected_mac * 1.05,  # within 20%
        )
        updated = self.normaliser.recalculate_mac(record)
        assert "mac_divergence" not in updated.quality_flags

    def test_geography_standardisation_uk(self):
        result = self.normaliser.standardise_geography("United Kingdom")
        assert result == "GBR"

    def test_geography_standardisation_us(self):
        result = self.normaliser.standardise_geography("us")
        assert result == "USA"

    def test_geography_standardisation_unknown(self):
        result = self.normaliser.standardise_geography("Wakanda")
        assert result == "Wakanda"

    def test_normalise_record_converts_currency(self):
        record = make_record(
            capex=10000.0,
            currency="USD",
            price_base_year=2022,
        )
        updated = self.normaliser.normalise_record(record)
        assert updated.currency == "GBP"
        assert updated.capex != 10000.0  # should have been converted
        assert updated.capex is not None

    def test_normalise_record_standardises_geography(self):
        record = make_record(geography="united kingdom")
        updated = self.normaliser.normalise_record(record)
        assert updated.geography == "GBR"
