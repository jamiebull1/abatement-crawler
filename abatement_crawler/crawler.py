"""Main crawler orchestration."""

from __future__ import annotations

import dataclasses
import logging
import threading
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

from .config import CrawlerConfig
from .decomposition import SectorDecomposer
from .export import Exporter
from .extraction import LLMExtractor
from .ingestion import DocumentIngester
from .models import AbatementRecord
from .normalisation import Normaliser
from .relevance import score_relevance
from .search import QueryBuilder, SearchClient
from .snowball import SnowballCrawler
from .storage import StorageManager

logger = logging.getLogger(__name__)


def _safe_slug(text: str) -> str:
    """Best-effort slug for an archetype name (mirrors decomposition._make_slug)."""
    try:
        from slugify import slugify  # noqa: PLC0415

        return slugify(text)
    except Exception:
        import re as _re  # noqa: PLC0415

        slug = _re.sub(r"[^\w-]", "-", text.lower())
        return slug.strip("-")


class AbatementCrawler:
    """Main orchestration class for the abatement data crawler."""

    def __init__(
        self,
        config: CrawlerConfig,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> None:
        self.config = config
        self.storage = StorageManager(config.db_path)
        self.ingester = DocumentIngester(config)
        self.extractor = LLMExtractor(config)
        self.normaliser = Normaliser(config.base_currency, config.base_year)
        self.exporter = Exporter(config.output_dir)
        self.snowball = SnowballCrawler(
            config=config,
            ingester=self.ingester,
            extractor=self.extractor,
            normaliser=self.normaliser,
            storage=self.storage,
            on_progress=progress_callback,
        )
        self._session_id = str(uuid.uuid4())
        self._start_time = datetime.now(UTC)

    def run(self) -> dict:
        """Run in search mode (default).

        1. Build initial search queries from scope.
        2. Execute search queries.
        3. Add results to snowball queue.
        4. Run snowball traversal.
        5. Export results.

        Returns:
            Stats dict.
        """
        return self.run_search_mode()

    def run_seed_mode(self, seed_urls: list[str]) -> dict:
        """Start the crawler from a curated list of seed URLs.

        Returns:
            Stats dict.
        """
        logger.info("Starting seed mode with %d seeds.", len(seed_urls))
        for url in seed_urls:
            self.snowball.add_seed(url, score=1.0)

        records = self.snowball.run()
        return self._finalise(records)

    def run_search_mode(self) -> dict:
        """Construct and execute search queries, then run snowball traversal.

        Returns:
            Stats dict.
        """
        query_builder = QueryBuilder(self.config.scope)
        queries = query_builder.build_queries()[: self.config.max_search_queries]
        logger.info("Built %d search queries.", len(queries))

        search_client = SearchClient(self.config)
        seen_urls: set[str] = set()
        seen_lock = threading.Lock()
        seeds_queued = 0

        def _run_query(query: str) -> list[dict]:
            logger.debug("Search query: %s", query)
            return search_client.search(query)

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as pool:
            futures = {pool.submit(_run_query, q): q for q in queries}
            for future in as_completed(futures):
                for result in (future.result() or []):
                    url = result.get("url", "")
                    if not url:
                        continue
                    with seen_lock:
                        if url in seen_urls:
                            continue
                        seen_urls.add(url)

                    relevance = score_relevance(
                        title=result.get("title", ""),
                        snippet=result.get("snippet", ""),
                        url=url,
                        scope=self.config.scope,
                    )
                    logger.debug("Relevance %.3f (threshold %.2f): %s", relevance, self.config.relevance_threshold, url)
                    if relevance >= self.config.relevance_threshold:
                        prev = len(self.snowball._queued_urls)
                        self.snowball.add_seed(url, score=relevance)
                        if len(self.snowball._queued_urls) > prev:
                            seeds_queued += 1

        logger.info(
            "Search found %d unique URLs; %d passed relevance threshold and queued for crawl.",
            len(seen_urls),
            seeds_queued,
        )
        records = self.snowball.run()
        return self._finalise(records)

    def run_pipeline_mode(self, sector: str | None = None) -> dict:
        """Layer 1→2→3 pipeline: decompose sector → map archetypes → populate via crawl.

        Layer 1: Ask Claude to break the sector into emissions-relevant asset groups.
        Layer 2: Ask Claude to map each asset group to specific abatement archetypes,
                 each with pre-generated search queries and analogue sectors.
        Layer 3: Run the existing search→snowball crawler using archetype-specific queries,
                 tagging extracted records with the archetype slug.

        Args:
            sector: Sector name to decompose. Falls back to config.scope.industry,
                    then the first entry in config.scope.sectors.

        Returns:
            Stats dict extended with archetypes_generated and per-archetype record counts.
        """
        # Resolve sector name
        resolved_sector = (
            sector
            or getattr(self.config, "pipeline", None) and self.config.pipeline.sector
            or self.config.scope.industry
            or (self.config.scope.sectors[0] if self.config.scope.sectors else None)
        )
        if not resolved_sector:
            raise ValueError(
                "Pipeline mode requires a sector name. Provide --sector, set pipeline.sector "
                "in config, or set scope.industry."
            )

        logger.info("Pipeline mode: sector='%s'", resolved_sector)

        # Layer 1: Sector decomposition
        decomposer = SectorDecomposer(self.config)
        decomposition = decomposer.decompose(resolved_sector, self.config.scope.geography)

        # Layer 2: Archetype mapping
        archetypes = decomposer.map_archetypes(decomposition)

        if not archetypes:
            logger.warning("No archetypes generated; falling back to search mode.")
            return self.run_search_mode()

        # Layer 3: Build archetype queries and run search→snowball
        pipeline_cfg = getattr(self.config, "pipeline", None)
        include_analogues = getattr(pipeline_cfg, "include_analogue_sectors", True)
        max_per_archetype = getattr(pipeline_cfg, "max_queries_per_archetype", None)

        query_builder = QueryBuilder(self.config.scope)
        all_queries: list[tuple[str, str]] = []  # (query, archetype_slug)
        for archetype in archetypes:
            slug = _safe_slug(archetype.name)
            queries = query_builder.build_archetype_queries(
                archetype,
                include_analogues=include_analogues,
                max_queries=max_per_archetype,
            )
            for q in queries:
                all_queries.append((q, slug))

        # Cap total queries
        capped = all_queries[: self.config.max_search_queries]
        logger.info(
            "Pipeline: %d archetypes → %d queries (%d after cap).",
            len(archetypes),
            len(all_queries),
            len(capped),
        )

        search_client = SearchClient(self.config)
        seen_urls: set[str] = set()
        seen_lock = threading.Lock()
        seeds_queued = 0

        def _run_query(q_slug: tuple[str, str]) -> list[tuple[dict, str]]:
            query, slug = q_slug
            logger.debug("Pipeline query [%s]: %s", slug, query)
            results = search_client.search(query)
            return [(r, slug) for r in (results or [])]

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as pool:
            futures = {pool.submit(_run_query, qs): qs for qs in capped}
            for future in as_completed(futures):
                for result, archetype_slug in (future.result() or []):
                    url = result.get("url", "")
                    if not url:
                        continue
                    with seen_lock:
                        if url in seen_urls:
                            continue
                        seen_urls.add(url)

                    relevance = score_relevance(
                        title=result.get("title", ""),
                        snippet=result.get("snippet", ""),
                        url=url,
                        scope=self.config.scope,
                    )
                    if relevance >= self.config.relevance_threshold:
                        prev = len(self.snowball._queued_urls)
                        self.snowball.add_seed(url, score=relevance, archetype_slug=archetype_slug)
                        if len(self.snowball._queued_urls) > prev:
                            seeds_queued += 1

        logger.info(
            "Pipeline search: %d unique URLs; %d queued for crawl.",
            len(seen_urls),
            seeds_queued,
        )

        records = self.snowball.run()

        # Layer 4: synthesis (if enabled)
        synthesised: list[AbatementRecord] = []
        pipeline_cfg = getattr(self.config, "pipeline", None)
        if getattr(pipeline_cfg, "synthesis_enabled", True):
            synthesised = self._run_synthesis(archetypes, records)
        else:
            logger.info("Synthesis disabled via pipeline.synthesis_enabled=false.")

        all_records = records + synthesised
        stats = self._finalise(all_records)

        # Count records per archetype
        archetype_counts: dict[str, int] = {}
        for r in records:
            if r.archetype_slug:
                archetype_counts[r.archetype_slug] = archetype_counts.get(r.archetype_slug, 0) + 1

        stats["archetypes_generated"] = len(archetypes)
        stats["archetypes_populated"] = sum(1 for a in archetypes if archetype_counts.get(
            _safe_slug(a.name), 0
        ) > 0)
        stats["archetype_record_counts"] = archetype_counts
        stats["synthesised_records"] = len(synthesised)
        return stats

    def _run_synthesis(
        self,
        archetypes: list,
        records: list[AbatementRecord],
    ) -> list[AbatementRecord]:
        """Layer 4: synthesise one best-estimate record per archetype.

        For each archetype, gathers complete extracted records and partial evidence
        fragments, then calls ArchetypeSynthesiser to produce a single canonical
        best-estimate AbatementRecord with explicit assumptions.

        Returns:
            List of synthesised records (empty if synthesis is unavailable).
        """
        from .quality import score_quality  # noqa: PLC0415
        from .synthesis import ArchetypeSynthesiser  # noqa: PLC0415

        synthesiser = ArchetypeSynthesiser(self.config)
        synthesised: list[AbatementRecord] = []
        pipeline_cfg = getattr(self.config, "pipeline", None)
        include_activity = getattr(pipeline_cfg, "include_activity_search", True)

        for archetype in archetypes:
            slug = _safe_slug(archetype.name)
            arch_records = [r for r in records if r.archetype_slug == slug]
            arch_fragments = self.storage.get_fragments_for_archetype(slug)

            # Optional: fetch activity intensity data for missing key variables
            activity_summary = ""
            if include_activity and not arch_records and archetype.key_variables:
                activity_summary = self._fetch_activity_data(archetype, synthesiser)

            result = synthesiser.synthesise(
                archetype, arch_records, arch_fragments, activity_summary=activity_summary
            )
            if result is None:
                logger.warning(
                    "Synthesis returned nothing for archetype '%s' "
                    "(%d records, %d fragments).",
                    archetype.name, len(arch_records), len(arch_fragments),
                )
                continue

            result = self.normaliser.normalise_record(result)
            quality, flags = score_quality(result)
            data = result.model_dump()
            data["quality_score"] = quality
            data["quality_flags"] = list(set(result.quality_flags + flags))
            result = AbatementRecord(**data)
            self.storage.save_record(result)
            synthesised.append(result)
            logger.info(
                "Synthesised '%s': quality=%.2f, assumptions=%d, sources=%d.",
                archetype.name, result.quality_score,
                len(result.synthesis_assumptions), len(result.synthesis_sources),
            )

        return synthesised

    def _fetch_activity_data(self, archetype, synthesiser) -> str:  # type: ignore[type-arg]
        """Run targeted searches for activity intensity data for an archetype.

        Called when an archetype has no complete records and key_variables suggest
        activity data is needed to derive abatement potential (e.g. km/year, L/km).

        Returns a plain-text summary for inclusion in the synthesis prompt.
        """
        from .search import SearchClient  # noqa: PLC0415

        queries = synthesiser.build_activity_queries(archetype, self.config.scope.geography)
        if not queries:
            return ""

        search_client = SearchClient(self.config)
        snippets: list[str] = []
        for query in queries[:4]:  # cap at 4 queries to avoid excess API calls
            results = search_client.search(query)
            for r in (results or [])[:2]:
                title = r.get("title", "")
                snippet = r.get("snippet", "")
                if title or snippet:
                    snippets.append(f"• {title}: {snippet}")

        if not snippets:
            return ""
        return "\n".join(snippets[:8])  # cap at 8 snippets

    def _finalise(self, records: list[AbatementRecord]) -> dict:
        """Export results and save session stats."""
        qualified = [
            r for r in records if r.quality_score >= self.config.min_quality_for_export
        ]
        self.exporter.export_jsonl(qualified)
        self.exporter.export_csv(qualified)

        stats = {
            "session_id": self._session_id,
            "total_records": len(records),
            "qualified_records": len(qualified),
            "documents_processed": self.snowball._docs_processed,
            "start_time": self._start_time.isoformat(),
            "end_time": datetime.now(UTC).isoformat(),
        }
        scope_dict = dataclasses.asdict(self.config.scope)
        self.storage.save_session(self._session_id, scope_dict, stats)
        logger.info(
            "Crawl complete. %d total records, %d exported.", len(records), len(qualified)
        )
        return stats
