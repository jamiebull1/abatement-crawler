"""Tests for Layer 4 synthesis (ArchetypeSynthesiser) and supporting infrastructure."""

from __future__ import annotations

import json
from unittest.mock import patch

from abatement_crawler.models import AbatementArchetype, AbatementRecord
from abatement_crawler.synthesis import ArchetypeSynthesiser

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_config(tmp_path):
    from abatement_crawler.config import CrawlerConfig
    return CrawlerConfig(llm_api_key="test-key", output_dir=str(tmp_path))


def _make_archetype(**kwargs) -> AbatementArchetype:
    defaults = dict(
        name="Diesel → HVO (heavy fleet)",
        category="fuel_switch",
        asset_group="Emergency response vehicles",
        mechanism="Replace mineral diesel with HVO",
        baseline="Diesel (B7)",
        abatement_driver="Lower lifecycle emission factor",
        typical_abatement_pct_min=70,
        typical_abatement_pct_max=90,
        key_variables=["annual mileage (km/year)", "fuel consumption (L/km)"],
        cost_drivers=["HVO price premium"],
        constraints=["engine certification"],
        search_queries=["HVO fire fleet lifecycle cost UK"],
        analogue_sectors=["refuse collection vehicles"],
    )
    defaults.update(kwargs)
    return AbatementArchetype(**defaults)


def _make_record(**kwargs) -> AbatementRecord:
    defaults = dict(
        measure_name="HVO fleet switch",
        measure_slug="hvo-fleet-switch",
        abatement_category="fuel_switch",
        sector="transport",
        scope_tag="scope_1",
        geography="GBR",
        publication_year=2023,
        source_url="https://example.com",
        source_title="Test source",
        source_type="government",
        quality_score=0.7,
        mac=45.0,
        capex=5000.0,
        abatement_percentage=80.0,
        archetype_slug="diesel-hvo-heavy-fleet",
    )
    defaults.update(kwargs)
    return AbatementRecord(**defaults)


GOOD_SYNTHESIS_JSON = json.dumps({
    "measure_name": "Diesel → HVO (fire appliance fleet)",
    "measure_slug": "diesel-hvo-fire-appliance-fleet",
    "abatement_category": "fuel_switch",
    "sector": "fire and rescue services",
    "sub_sector": "emergency response",
    "asset_type": "heavy fire appliances",
    "scope_tag": "scope_1",
    "geography": "GBR",
    "publication_year": 2024,
    "abatement_percentage": 80.0,
    "baseline_description": "Diesel (B7/B10) combustion",
    "capex": 0,
    "opex_delta": 2500.0,
    "lifetime_years": 10,
    "mac": 55.0,
    "currency": "GBP",
    "source_url": "synthesised",
    "source_title": "Synthesised: Diesel → HVO (heavy fleet)",
    "source_type": "technology_catalogue",
    "extraction_method": "llm_synthesis",
    "extraction_confidence": 0.65,
    "is_synthesised": True,
    "synthesis_sources": ["abc12345", "def67890"],
    "synthesis_assumptions": [
        "abatement_percentage assumed 80% from HVO lifecycle analysis (Neste 2022)",
        "opex_delta derived from HVO price premium of ~15p/L × typical fleet consumption",
    ],
    "notes": "Synthesised from 2 extracted records and archetype typical range.",
    "archetype_slug": "diesel-hvo-heavy-fleet",
})


# ---------------------------------------------------------------------------
# AbatementRecord synthesis fields
# ---------------------------------------------------------------------------

class TestSynthesisFields:
    def test_defaults_on_normal_record(self):
        record = _make_record()
        assert record.is_synthesised is False
        assert record.synthesis_sources == []
        assert record.synthesis_assumptions == []

    def test_synthesis_fields_can_be_set(self):
        record = _make_record(
            is_synthesised=True,
            synthesis_sources=["id1", "id2"],
            synthesis_assumptions=["assumed 80% from typical range"],
        )
        assert record.is_synthesised is True
        assert len(record.synthesis_sources) == 2
        assert "assumed 80%" in record.synthesis_assumptions[0]

    def test_synthesis_fields_round_trip_json(self):
        record = _make_record(
            is_synthesised=True,
            synthesis_assumptions=["assumption A"],
        )
        loaded = AbatementRecord.model_validate_json(record.model_dump_json())
        assert loaded.is_synthesised is True
        assert loaded.synthesis_assumptions == ["assumption A"]


# ---------------------------------------------------------------------------
# ArchetypeSynthesiser.synthesise()
# ---------------------------------------------------------------------------

class TestArchetypeSynthesiser:
    def test_synthesise_returns_record_on_valid_json(self, tmp_path):
        config = _make_config(tmp_path)
        synthesiser = ArchetypeSynthesiser(config)
        archetype = _make_archetype()
        records = [_make_record()]

        with patch.object(synthesiser, "_call_llm", return_value=GOOD_SYNTHESIS_JSON):
            result = synthesiser.synthesise(archetype, records, fragments=[])

        assert result is not None
        assert isinstance(result, AbatementRecord)
        assert result.is_synthesised is True
        assert result.extraction_method == "llm_synthesis"
        assert result.archetype_slug == "diesel-hvo-heavy-fleet"

    def test_synthesise_returns_none_on_null_response(self, tmp_path):
        config = _make_config(tmp_path)
        synthesiser = ArchetypeSynthesiser(config)
        archetype = _make_archetype()

        with patch.object(synthesiser, "_call_llm", return_value="null"):
            result = synthesiser.synthesise(archetype, records=[], fragments=[])

        assert result is None

    def test_synthesise_merges_source_ids_from_records(self, tmp_path):
        config = _make_config(tmp_path)
        synthesiser = ArchetypeSynthesiser(config)
        archetype = _make_archetype()
        r1 = _make_record(record_id="aaa-111")
        r2 = _make_record(record_id="bbb-222", measure_name="Another HVO record")

        with patch.object(synthesiser, "_call_llm", return_value=GOOD_SYNTHESIS_JSON):
            result = synthesiser.synthesise(archetype, records=[r1, r2], fragments=[])

        assert result is not None
        assert "aaa-111" in result.synthesis_sources
        assert "bbb-222" in result.synthesis_sources

    def test_synthesise_includes_fragment_ids_in_sources(self, tmp_path):
        config = _make_config(tmp_path)
        synthesiser = ArchetypeSynthesiser(config)
        archetype = _make_archetype()
        fragment = _make_record(
            record_id="frag-001",
            measure_name="Partial HVO cost data",
            abatement_percentage=None,
        )

        with patch.object(synthesiser, "_call_llm", return_value=GOOD_SYNTHESIS_JSON):
            result = synthesiser.synthesise(archetype, records=[], fragments=[fragment])

        assert result is not None
        assert "frag-001" in result.synthesis_sources

    def test_synthesise_strips_markdown_fences(self, tmp_path):
        config = _make_config(tmp_path)
        synthesiser = ArchetypeSynthesiser(config)
        archetype = _make_archetype()

        wrapped = f"```json\n{GOOD_SYNTHESIS_JSON}\n```"
        with patch.object(synthesiser, "_call_llm", return_value=wrapped):
            result = synthesiser.synthesise(archetype, records=[], fragments=[])

        assert result is not None

    def test_synthesise_handles_json_parse_error(self, tmp_path):
        config = _make_config(tmp_path)
        synthesiser = ArchetypeSynthesiser(config)
        archetype = _make_archetype()

        with patch.object(synthesiser, "_call_llm", return_value="not valid json {{{{"):
            result = synthesiser.synthesise(archetype, records=[], fragments=[])

        assert result is None

    def test_synthesise_with_no_client_returns_none(self, tmp_path):
        config = _make_config(tmp_path)
        config = config.model_copy(update={"llm_api_key": ""})
        synthesiser = ArchetypeSynthesiser(config)

        result = synthesiser.synthesise(_make_archetype(), records=[], fragments=[])
        assert result is None

    def test_synthesise_assumptions_list_preserved(self, tmp_path):
        config = _make_config(tmp_path)
        synthesiser = ArchetypeSynthesiser(config)
        archetype = _make_archetype()

        with patch.object(synthesiser, "_call_llm", return_value=GOOD_SYNTHESIS_JSON):
            result = synthesiser.synthesise(archetype, records=[_make_record()], fragments=[])

        assert result is not None
        assert len(result.synthesis_assumptions) == 2
        assert any("80%" in a for a in result.synthesis_assumptions)


# ---------------------------------------------------------------------------
# build_activity_queries()
# ---------------------------------------------------------------------------

class TestBuildActivityQueries:
    def test_generates_queries_for_each_key_variable(self, tmp_path):
        config = _make_config(tmp_path)
        synthesiser = ArchetypeSynthesiser(config)
        archetype = _make_archetype()

        queries = synthesiser.build_activity_queries(archetype, geography=["UK"])

        assert len(queries) > 0
        # One pair of queries per key_variable
        assert len(queries) == 2 * len(archetype.key_variables)

    def test_strips_unit_hints_from_variable_names(self, tmp_path):
        config = _make_config(tmp_path)
        synthesiser = ArchetypeSynthesiser(config)
        archetype = _make_archetype(key_variables=["fuel consumption (L/km)"])

        queries = synthesiser.build_activity_queries(archetype, geography=["UK"])

        # "(L/km)" should not appear in the queries
        assert all("(L/km)" not in q for q in queries)
        assert any("fuel consumption" in q for q in queries)

    def test_includes_geography_in_queries(self, tmp_path):
        config = _make_config(tmp_path)
        synthesiser = ArchetypeSynthesiser(config)
        archetype = _make_archetype(key_variables=["annual mileage"])

        queries = synthesiser.build_activity_queries(archetype, geography=["Scotland"])

        assert any("Scotland" in q for q in queries)

    def test_returns_empty_for_no_key_variables(self, tmp_path):
        config = _make_config(tmp_path)
        synthesiser = ArchetypeSynthesiser(config)
        archetype = _make_archetype(key_variables=[])

        queries = synthesiser.build_activity_queries(archetype, geography=["UK"])

        assert queries == []


# ---------------------------------------------------------------------------
# Storage: evidence_fragments
# ---------------------------------------------------------------------------

class TestEvidenceFragmentStorage:
    def test_save_and_retrieve_fragment(self, tmp_path):
        from abatement_crawler.storage import StorageManager

        storage = StorageManager(str(tmp_path / "test.db"))
        fragment = _make_record(
            measure_name="Partial HVO cost only",
            abatement_percentage=None,
            archetype_slug="diesel-hvo-heavy-fleet",
        )
        storage.save_fragment(fragment)

        retrieved = storage.get_fragments_for_archetype("diesel-hvo-heavy-fleet")
        assert len(retrieved) == 1
        assert retrieved[0].measure_name == "Partial HVO cost only"
        storage.close()

    def test_fragment_filtered_by_archetype_slug(self, tmp_path):
        from abatement_crawler.storage import StorageManager

        storage = StorageManager(str(tmp_path / "test.db"))
        f1 = _make_record(measure_name="F1", archetype_slug="archetype-a")
        f2 = _make_record(measure_name="F2", archetype_slug="archetype-b",
                          record_id="different-id")
        storage.save_fragment(f1)
        storage.save_fragment(f2)

        result_a = storage.get_fragments_for_archetype("archetype-a")
        result_b = storage.get_fragments_for_archetype("archetype-b")

        assert len(result_a) == 1
        assert result_a[0].measure_name == "F1"
        assert len(result_b) == 1
        assert result_b[0].measure_name == "F2"
        storage.close()

    def test_get_synthesised_records(self, tmp_path):
        from abatement_crawler.storage import StorageManager

        storage = StorageManager(str(tmp_path / "test.db"))
        normal = _make_record(measure_name="Normal", quality_score=0.7)
        synthesised = _make_record(
            measure_name="Synthesised",
            record_id="synth-001",
            is_synthesised=True,
            quality_score=0.6,
        )
        storage.save_record(normal)
        storage.save_record(synthesised)

        results = storage.get_synthesised_records(min_quality=0.0)
        assert len(results) == 1
        assert results[0].measure_name == "Synthesised"
        storage.close()


# ---------------------------------------------------------------------------
# Extraction: extract_fragments() and _extract_raw() caching
# ---------------------------------------------------------------------------

class TestExtractFragments:
    def test_extract_returns_only_paired(self, tmp_path):
        """extract() must still only return paired records (regression guard)."""
        from abatement_crawler.extraction import LLMExtractor
        config = _make_config(tmp_path)
        extractor = LLMExtractor(config)

        paired_json = json.dumps([{
            "measure_name": "Paired record",
            "abatement_category": "efficiency",
            "sector": "transport",
            "scope_tag": "scope_1",
            "geography": "UK",
            "publication_year": 2023,
            "source_url": "https://example.com",
            "source_title": "Test",
            "source_type": "government",
            "capex": 1000.0,
            "abatement_percentage": 30.0,
        }, {
            "measure_name": "Partial — cost only",
            "abatement_category": "efficiency",
            "sector": "transport",
            "scope_tag": "scope_1",
            "geography": "UK",
            "publication_year": 2023,
            "source_url": "https://example.com",
            "source_title": "Test",
            "source_type": "government",
            "capex": 1000.0,
            # No abatement data → partial
        }])

        with patch.object(extractor, "_call_llm", return_value=paired_json):
            complete = extractor.extract("some chunk", "https://example.com", "Test")

        assert len(complete) == 1
        assert complete[0].measure_name == "Paired record"

    def test_extract_fragments_returns_only_partial(self, tmp_path):
        """extract_fragments() returns records that failed the paired-data gate."""
        from abatement_crawler.extraction import LLMExtractor
        config = _make_config(tmp_path)
        extractor = LLMExtractor(config)

        paired_json = json.dumps([{
            "measure_name": "Paired record",
            "abatement_category": "efficiency",
            "sector": "transport",
            "scope_tag": "scope_1",
            "geography": "UK",
            "publication_year": 2023,
            "source_url": "https://example.com",
            "source_title": "Test",
            "source_type": "government",
            "capex": 1000.0,
            "abatement_percentage": 30.0,
        }, {
            "measure_name": "Partial — cost only",
            "abatement_category": "efficiency",
            "sector": "transport",
            "scope_tag": "scope_1",
            "geography": "UK",
            "publication_year": 2023,
            "source_url": "https://example.com",
            "source_title": "Test",
            "source_type": "government",
            "capex": 1000.0,
        }])

        with patch.object(extractor, "_call_llm", return_value=paired_json) as mock_llm:
            _ = extractor.extract("some chunk", "https://example.com", "Test")
            fragments = extractor.extract_fragments("some chunk", "https://example.com", "Test")
            # Should only make ONE LLM call (cached)
            assert mock_llm.call_count == 1

        assert len(fragments) == 1
        assert fragments[0].measure_name == "Partial — cost only"

    def test_no_double_llm_call_for_same_chunk(self, tmp_path):
        """extract() + extract_fragments() on same chunk = 1 LLM call total."""
        from abatement_crawler.extraction import LLMExtractor
        config = _make_config(tmp_path)
        extractor = LLMExtractor(config)

        with patch.object(extractor, "_call_llm", return_value="[]") as mock_llm:
            extractor.extract("chunk A", "https://a.com", "Title A")
            extractor.extract_fragments("chunk A", "https://a.com", "Title A")
            assert mock_llm.call_count == 1

    def test_different_chunks_make_separate_llm_calls(self, tmp_path):
        from abatement_crawler.extraction import LLMExtractor
        config = _make_config(tmp_path)
        extractor = LLMExtractor(config)

        with patch.object(extractor, "_call_llm", return_value="[]") as mock_llm:
            extractor.extract("chunk A", "https://a.com", "Title A")
            extractor.extract("chunk B", "https://a.com", "Title A")
            assert mock_llm.call_count == 2
