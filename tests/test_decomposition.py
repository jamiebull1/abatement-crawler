"""Tests for the 3-layer pipeline decomposition module."""

from __future__ import annotations

import json
from unittest.mock import patch

from abatement_crawler.decomposition import SectorDecomposer
from abatement_crawler.models import (
    AbatementArchetype,
    AssetGroup,
    ScopeConfig,
    SectorDecomposition,
)
from abatement_crawler.search import QueryBuilder

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_ASSET_GROUPS_JSON = json.dumps([
    {
        "name": "Emergency response vehicles",
        "description": "Pump ladders and aerial platforms responding to incidents.",
        "emission_sources": ["diesel combustion", "engine idling during standby"],
        "scope_tag": "scope_1",
    },
    {
        "name": "Light-duty fleet",
        "description": "Cars and vans used for personnel transport.",
        "emission_sources": ["petrol/diesel combustion"],
        "scope_tag": "scope_1",
    },
])

SAMPLE_ARCHETYPES_JSON = json.dumps([
    {
        "name": "Diesel → HVO (heavy fleet)",
        "category": "fuel_switch",
        "asset_group": "Emergency response vehicles",
        "mechanism": "Replace mineral diesel with hydrotreated vegetable oil",
        "baseline": "Diesel (B7)",
        "abatement_driver": "Lower lifecycle emission factor",
        "typical_abatement_pct_min": 70,
        "typical_abatement_pct_max": 90,
        "key_variables": ["annual mileage (km/year)", "fuel consumption (L/km)"],
        "cost_drivers": ["HVO price premium (p/L)", "supply availability"],
        "constraints": ["engine certification", "feedstock sustainability"],
        "search_queries": [
            "HVO fire appliance lifecycle emissions UK",
            "hydrotreated vegetable oil heavy fleet cost per tonne CO2",
        ],
        "analogue_sectors": ["refuse collection vehicles", "ambulance fleet"],
    },
    {
        "name": "Idle reduction systems",
        "category": "efficiency",
        "asset_group": "Emergency response vehicles",
        "mechanism": "Reduce unnecessary engine idling at station",
        "baseline": "Continuous engine running for readiness",
        "abatement_driver": "Fuel consumption reduction",
        "typical_abatement_pct_min": 5,
        "typical_abatement_pct_max": 15,
        "key_variables": ["idle time share", "engine fuel burn at idle (L/hr)"],
        "cost_drivers": ["capex per vehicle (£1k–£5k)"],
        "constraints": ["operational readiness requirement"],
        "search_queries": [
            "idle reduction fire engine fuel savings",
            "engine idle management emergency vehicles cost effectiveness",
        ],
        "analogue_sectors": ["municipal fleet", "refuse trucks"],
    },
])


def _make_config(tmp_path):
    """Return a minimal CrawlerConfig with a temp output dir."""
    from abatement_crawler.config import CrawlerConfig

    return CrawlerConfig(
        llm_api_key="test-key",
        output_dir=str(tmp_path),
        scope=ScopeConfig(geography=["UK"]),
    )


# ---------------------------------------------------------------------------
# Layer 1: decompose()
# ---------------------------------------------------------------------------

class TestDecompose:
    def test_parses_asset_groups(self, tmp_path):
        config = _make_config(tmp_path)
        decomposer = SectorDecomposer(config)

        with patch.object(decomposer, "_call_llm", return_value=SAMPLE_ASSET_GROUPS_JSON):
            result = decomposer.decompose("fire and rescue services", ["UK"])

        assert isinstance(result, SectorDecomposition)
        assert result.sector == "fire and rescue services"
        assert result.geography == ["UK"]
        assert len(result.asset_groups) == 2

        ev = result.asset_groups[0]
        assert ev.name == "Emergency response vehicles"
        assert "diesel combustion" in ev.emission_sources
        assert ev.scope_tag == "scope_1"

    def test_persists_json_file(self, tmp_path):
        config = _make_config(tmp_path)
        decomposer = SectorDecomposer(config)

        with patch.object(decomposer, "_call_llm", return_value=SAMPLE_ASSET_GROUPS_JSON):
            decomposer.decompose("fire and rescue services", ["UK"])

        output_file = tmp_path / "decomposition_fire-and-rescue-services.json"
        assert output_file.exists()
        data = json.loads(output_file.read_text())
        assert len(data) == 2
        assert data[0]["name"] == "Emergency response vehicles"

    def test_skips_items_without_name(self, tmp_path):
        config = _make_config(tmp_path)
        decomposer = SectorDecomposer(config)

        bad_json = json.dumps([{"description": "no name field"}, {"name": "Valid group", "description": "ok",
                                "emission_sources": [], "scope_tag": "scope_1"}])
        with patch.object(decomposer, "_call_llm", return_value=bad_json):
            result = decomposer.decompose("test sector", [])

        assert len(result.asset_groups) == 1
        assert result.asset_groups[0].name == "Valid group"

    def test_strips_markdown_fences(self, tmp_path):
        config = _make_config(tmp_path)
        decomposer = SectorDecomposer(config)

        wrapped = f"```json\n{SAMPLE_ASSET_GROUPS_JSON}\n```"
        with patch.object(decomposer, "_call_llm", return_value=wrapped):
            result = decomposer.decompose("fire and rescue services", ["UK"])

        assert len(result.asset_groups) == 2


# ---------------------------------------------------------------------------
# Layer 2: map_archetypes()
# ---------------------------------------------------------------------------

class TestMapArchetypes:
    def _make_decomposition(self) -> SectorDecomposition:
        return SectorDecomposition(
            sector="fire and rescue services",
            geography=["UK"],
            asset_groups=[
                AssetGroup(
                    name="Emergency response vehicles",
                    description="Pump ladders.",
                    emission_sources=["diesel combustion"],
                    scope_tag="scope_1",
                )
            ],
        )

    def test_parses_archetypes(self, tmp_path):
        config = _make_config(tmp_path)
        decomposer = SectorDecomposer(config)
        decomposition = self._make_decomposition()

        with patch.object(decomposer, "_call_llm", return_value=SAMPLE_ARCHETYPES_JSON):
            archetypes = decomposer.map_archetypes(decomposition)

        assert len(archetypes) == 2
        hvo = archetypes[0]
        assert isinstance(hvo, AbatementArchetype)
        assert hvo.name == "Diesel → HVO (heavy fleet)"
        assert hvo.category == "fuel_switch"
        assert hvo.asset_group == "Emergency response vehicles"
        assert hvo.typical_abatement_pct_min == 70
        assert hvo.typical_abatement_pct_max == 90
        assert len(hvo.search_queries) == 2
        assert "refuse collection vehicles" in hvo.analogue_sectors

    def test_persists_archetypes_json(self, tmp_path):
        config = _make_config(tmp_path)
        decomposer = SectorDecomposer(config)
        decomposition = self._make_decomposition()

        with patch.object(decomposer, "_call_llm", return_value=SAMPLE_ARCHETYPES_JSON):
            decomposer.map_archetypes(decomposition)

        output_file = tmp_path / "archetypes_fire-and-rescue-services.json"
        assert output_file.exists()
        data = json.loads(output_file.read_text())
        assert len(data) == 2

    def test_null_abatement_pct_allowed(self, tmp_path):
        config = _make_config(tmp_path)
        decomposer = SectorDecomposer(config)
        decomposition = self._make_decomposition()

        null_pct = json.dumps([{
            "name": "Partial electrification",
            "category": "fuel_switch",
            "asset_group": "Emergency response vehicles",
            "mechanism": "BEV light fleet",
            "baseline": "Petrol",
            "abatement_driver": "Zero tailpipe emissions",
            "typical_abatement_pct_min": None,
            "typical_abatement_pct_max": None,
            "key_variables": [],
            "cost_drivers": [],
            "constraints": [],
            "search_queries": ["BEV fire service fleet cost UK"],
            "analogue_sectors": [],
        }])

        with patch.object(decomposer, "_call_llm", return_value=null_pct):
            archetypes = decomposer.map_archetypes(decomposition)

        assert archetypes[0].typical_abatement_pct_min is None
        assert archetypes[0].typical_abatement_pct_max is None


# ---------------------------------------------------------------------------
# QueryBuilder: build_archetype_queries()
# ---------------------------------------------------------------------------

class TestBuildArchetypeQueries:
    def _make_archetype(self) -> AbatementArchetype:
        return AbatementArchetype(
            name="Diesel → HVO (heavy fleet)",
            category="fuel_switch",
            asset_group="Emergency response vehicles",
            mechanism="Replace diesel with HVO",
            baseline="Diesel",
            abatement_driver="Lower lifecycle EF",
            typical_abatement_pct_min=70,
            typical_abatement_pct_max=90,
            key_variables=["mileage"],
            cost_drivers=["price premium"],
            constraints=["certification"],
            search_queries=[
                "HVO fire appliance lifecycle emissions UK",
                "hydrotreated vegetable oil heavy fleet cost per tonne CO2",
            ],
            analogue_sectors=["refuse collection vehicles"],
        )

    def test_returns_base_queries(self):
        scope = ScopeConfig(geography=[])
        qb = QueryBuilder(scope)
        archetype = self._make_archetype()

        queries = qb.build_archetype_queries(archetype, include_analogues=False)

        assert "HVO fire appliance lifecycle emissions UK" in queries
        assert "hydrotreated vegetable oil heavy fleet cost per tonne CO2" in queries

    def test_appends_geo_variants(self):
        scope = ScopeConfig(geography=["UK", "EU"])
        qb = QueryBuilder(scope)
        archetype = self._make_archetype()

        queries = qb.build_archetype_queries(archetype, include_analogues=False)

        # geo should be appended to queries that don't already contain it
        assert any("EU" in q for q in queries)

    def test_no_duplicate_geo_if_already_present(self):
        scope = ScopeConfig(geography=["UK"])
        qb = QueryBuilder(scope)
        archetype = self._make_archetype()

        queries = qb.build_archetype_queries(archetype, include_analogues=False)

        # "HVO fire appliance lifecycle emissions UK" already contains UK — no duplicate
        uk_variants = [q for q in queries if q == "HVO fire appliance lifecycle emissions UK UK"]
        assert uk_variants == []

    def test_appends_analogue_variants(self):
        scope = ScopeConfig(geography=[])
        qb = QueryBuilder(scope)
        archetype = self._make_archetype()

        queries = qb.build_archetype_queries(archetype, include_analogues=True)

        assert any("refuse collection vehicles" in q for q in queries)

    def test_deduplicates(self):
        scope = ScopeConfig(geography=[])
        qb = QueryBuilder(scope)
        archetype = self._make_archetype()

        queries = qb.build_archetype_queries(archetype, include_analogues=False)

        assert len(queries) == len(set(queries))

    def test_max_queries_cap(self):
        scope = ScopeConfig(geography=["UK", "EU", "USA"])
        qb = QueryBuilder(scope)
        archetype = self._make_archetype()

        queries = qb.build_archetype_queries(archetype, include_analogues=True, max_queries=3)

        assert len(queries) <= 3


# ---------------------------------------------------------------------------
# Archetype slug propagation
# ---------------------------------------------------------------------------

class TestArchetypeSlugPropagation:
    """Verify archetype_slug is set on records extracted from archetype-seeded URLs."""

    def test_archetype_slug_on_crawl_item(self):
        """CrawlItem accepts and stores archetype_slug."""
        from abatement_crawler.snowball import CrawlItem

        item = CrawlItem(priority=-0.9, url="https://example.com", depth=0,
                         archetype_slug="diesel-to-hvo-heavy-fleet")
        assert item.archetype_slug == "diesel-to-hvo-heavy-fleet"

    def test_archetype_slug_defaults_none(self):
        from abatement_crawler.snowball import CrawlItem

        item = CrawlItem(priority=-0.9, url="https://example.com", depth=0)
        assert item.archetype_slug is None

    def test_archetype_slug_field_on_record(self):
        """AbatementRecord accepts archetype_slug."""
        from abatement_crawler.models import AbatementRecord

        record = AbatementRecord(
            measure_name="HVO fleet switch",
            measure_slug="hvo-fleet-switch",
            abatement_category="fuel_switch",
            sector="transport",
            scope_tag="scope_1",
            geography="GBR",
            publication_year=2023,
            source_url="https://example.com",
            source_title="Test",
            source_type="government",
            archetype_slug="diesel-to-hvo-heavy-fleet",
        )
        assert record.archetype_slug == "diesel-to-hvo-heavy-fleet"

    def test_archetype_slug_defaults_none_on_record(self):
        from abatement_crawler.models import AbatementRecord

        record = AbatementRecord(
            measure_name="Generic measure",
            measure_slug="generic-measure",
            abatement_category="efficiency",
            sector="transport",
            scope_tag="scope_1",
            geography="GBR",
            publication_year=2023,
            source_url="https://example.com",
            source_title="Test",
            source_type="government",
        )
        assert record.archetype_slug is None
