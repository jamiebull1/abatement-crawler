"""Snowball traversal with priority queue."""

from __future__ import annotations

import dataclasses
import heapq
import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

from .captcha import CaptchaDetected
from .config import CrawlerConfig
from .extraction import LLMExtractor
from .ingestion import DocumentIngester
from .models import AbatementRecord
from .normalisation import Normaliser
from .quality import score_quality
from .relevance import score_relevance
from .storage import StorageManager

logger = logging.getLogger(__name__)


@dataclass(order=True)
class CrawlItem:
    """Item in the snowball priority queue."""

    priority: float  # negative score for min-heap (higher score = lower priority value)
    url: str = field(compare=False)
    depth: int = field(compare=False)
    source_url: str = field(compare=False, default="")
    anchor_text: str = field(compare=False, default="")


class SnowballCrawler:
    """Priority queue-based snowball traversal.

    Seeds URLs are scored and placed in a min-heap (negated score).
    Each iteration pops the highest-priority URL, fetches and processes it,
    extracts outbound links, scores them, and adds qualifying links to the queue.
    """

    def __init__(
        self,
        config: CrawlerConfig,
        ingester: DocumentIngester,
        extractor: LLMExtractor,
        normaliser: Normaliser,
        storage: StorageManager,
        on_progress: Callable[[int, int], None] | None = None,
    ) -> None:
        self.config = config
        self.ingester = ingester
        self.extractor = extractor
        self.normaliser = normaliser
        self.storage = storage
        self.on_progress = on_progress
        self._heap: list[CrawlItem] = []
        self._queued_urls: set[str] = set()
        self._docs_processed = 0
        self._records_found = 0
        self._recent_measures: list[str] = []

    def add_seed(self, url: str, score: float = 1.0) -> None:
        """Add a seed URL to the queue."""
        if url not in self._queued_urls and not self.storage.is_url_visited(url):
            item = CrawlItem(priority=-score, url=url, depth=0, source_url="")
            heapq.heappush(self._heap, item)
            self._queued_urls.add(url)

    def run(self, max_documents: int | None = None) -> list[AbatementRecord]:
        """Run snowball traversal until queue is empty or limit reached.

        Fetches a batch of documents concurrently (up to ``max_workers``),
        then processes each result sequentially for extraction and storage.

        Returns:
            All AbatementRecord objects extracted during the run.
        """
        limit = max_documents or self.config.max_total_documents
        n_workers = getattr(self.config, "max_workers", 5)
        all_records: list[AbatementRecord] = []

        while self._heap and self._docs_processed < limit:
            # Pop a batch of unvisited items
            batch: list[CrawlItem] = []
            while self._heap and len(batch) < n_workers:
                item = heapq.heappop(self._heap)
                if not self.storage.is_url_visited(item.url):
                    batch.append(item)

            if not batch:
                continue

            # Fetch all documents in the batch concurrently
            fetch_results: dict[CrawlItem, tuple[dict | None, Exception | None]] = {}
            with ThreadPoolExecutor(max_workers=len(batch)) as pool:
                futures = {pool.submit(self._fetch, item): item for item in batch}
                for future in as_completed(futures):
                    item = futures[future]
                    try:
                        fetch_results[item] = (future.result(), None)
                    except Exception as exc:
                        fetch_results[item] = (None, exc)

            # Process each result sequentially
            for item in batch:
                doc, error = fetch_results[item]
                records = self._handle_fetch_result(item, doc, error)
                all_records.extend(records)
                self._records_found += len(records)
                if self.on_progress:
                    self.on_progress(self._docs_processed, self._records_found)

                if (
                    self._docs_processed % self.config.reflection_interval == 0
                    and self._docs_processed > 0
                ):
                    self._reflection_step()

        logger.info(
            "Snowball complete. Processed %d documents, extracted %d records.",
            self._docs_processed,
            len(all_records),
        )
        return all_records

    def _fetch(self, item: CrawlItem) -> dict:
        """Fetch and parse a document. Raises on captcha/forbidden."""
        logger.info("Fetching [depth=%d] %s", item.depth, item.url)
        return self.ingester.ingest(item.url, referer=item.source_url or None)

    def _handle_fetch_result(
        self, item: CrawlItem, doc: dict | None, error: Exception | None
    ) -> list[AbatementRecord]:
        """Process the result of a fetch: extract, normalise, score, store."""
        if isinstance(error, CaptchaDetected):
            logger.warning(
                "Blocked %s (type=%s) — queued for human review.",
                item.url,
                error.captcha_type,
            )
            self.storage.add_to_captcha_queue(
                url=item.url,
                captcha_type=error.captcha_type,
                notes=f"Source: {item.source_url}" if item.source_url else "",
            )
            # Do NOT mark as visited so the URL can be retried after resolution
            self._docs_processed += 1
            return []

        if error is not None or doc is None:
            logger.warning("Failed to fetch %s: %s", item.url, error)
            self._docs_processed += 1
            return []

        self.storage.mark_url_visited(item.url, doc["metadata"].get("status_code", 0))
        self._docs_processed += 1

        if not doc["content"]:
            return []

        records: list[AbatementRecord] = []
        chunks = self.ingester.chunk_text(doc["content"])

        for chunk in chunks:
            extracted = self.extractor.extract(
                chunk,
                source_url=item.url,
                source_title=doc["metadata"].get("title", item.url),
            )
            for record in extracted:
                record = self.normaliser.normalise_record(record)
                quality, flags = score_quality(record)
                data = record.model_dump()
                data["quality_score"] = quality
                data["quality_flags"] = list(set(record.quality_flags + flags))
                record = AbatementRecord(**data)
                self.storage.save_record(record)
                records.append(record)

        # Track recent measure names for reflection
        for r in records:
            self._recent_measures.append(r.measure_name)
        if len(self._recent_measures) > 50:
            self._recent_measures = self._recent_measures[-50:]

        # Queue outbound links if below max depth
        if item.depth < self.config.max_depth:
            self._extract_and_queue_links(item.url, doc["content"], item.depth)

        return records

    def _extract_and_queue_links(
        self, url: str, content: str, depth: int
    ) -> None:
        """Score outbound links and enqueue qualifying ones."""
        links = self.ingester._extract_links(url, content)

        for link in links:
            if link in self._queued_urls or self.storage.is_url_visited(link):
                continue

            relevance = score_relevance(
                title="",
                snippet="",
                url=link,
                scope=self.config.scope,
                anchor_text="",
            )

            if relevance >= self.config.relevance_threshold:
                item = CrawlItem(
                    priority=-relevance,
                    url=link,
                    depth=depth + 1,
                    source_url=url,
                )
                heapq.heappush(self._heap, item)
                self._queued_urls.add(link)

    def _reflection_step(self) -> None:
        """Log progress and call Claude for a brief mid-crawl reflection."""
        queue_size = len(self._heap)
        logger.info(
            "[Reflection] Docs processed: %d | Queue size: %d",
            self._docs_processed,
            queue_size,
        )
        scope_parts = [
            f"{k}={v}"
            for k, v in dataclasses.asdict(self.config.scope).items()
            if v is not None and v != [] and v != ()
        ]
        scope_summary = "; ".join(scope_parts) or "no specific scope defined"
        reflection = self.extractor.reflect(
            docs_processed=self._docs_processed,
            queue_size=queue_size,
            recent_measures=list(self._recent_measures[-20:]),
            scope_summary=scope_summary,
        )
        if reflection:
            logger.info("[Reflection result]\n%s", reflection)
