"""Main crawler orchestration."""

from __future__ import annotations

import dataclasses
import logging
import uuid
from datetime import UTC, datetime

from .config import CrawlerConfig
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


class AbatementCrawler:
    """Main orchestration class for the abatement data crawler."""

    def __init__(self, config: CrawlerConfig) -> None:
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

        for i, query in enumerate(queries):
            logger.debug("Search query %d/%d: %s", i + 1, len(queries), query)
            results = search_client.search(query)
            for result in results:
                url = result.get("url", "")
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)

                relevance = score_relevance(
                    title=result.get("title", ""),
                    snippet=result.get("snippet", ""),
                    url=url,
                    scope=self.config.scope,
                )
                if relevance >= self.config.relevance_threshold:
                    self.snowball.add_seed(url, score=relevance)

        logger.info("Added %d URLs to snowball queue.", len(seen_urls))
        records = self.snowball.run()
        return self._finalise(records)

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
