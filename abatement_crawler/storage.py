"""SQLite storage layer for AbatementRecord objects."""

from __future__ import annotations

import json
import logging
import sqlite3

from .models import AbatementRecord

logger = logging.getLogger(__name__)

_CREATE_RECORDS_TABLE = """
CREATE TABLE IF NOT EXISTS abatement_records (
    record_id TEXT PRIMARY KEY,
    measure_name TEXT NOT NULL,
    measure_slug TEXT,
    sector TEXT,
    geography TEXT,
    publication_year INTEGER,
    source_url TEXT,
    source_type TEXT,
    quality_score REAL,
    mac REAL,
    capex REAL,
    currency TEXT,
    data_json TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);
"""

_CREATE_URL_CACHE_TABLE = """
CREATE TABLE IF NOT EXISTS url_cache (
    url TEXT PRIMARY KEY,
    status_code INTEGER,
    visited_at TEXT DEFAULT (datetime('now'))
);
"""

_CREATE_CRAWL_SESSIONS_TABLE = """
CREATE TABLE IF NOT EXISTS crawl_sessions (
    session_id TEXT PRIMARY KEY,
    scope_config TEXT,
    stats TEXT,
    started_at TEXT DEFAULT (datetime('now'))
);
"""

_CREATE_DUPLICATE_CLUSTERS_TABLE = """
CREATE TABLE IF NOT EXISTS duplicate_clusters (
    cluster_id INTEGER,
    record_id TEXT,
    PRIMARY KEY (cluster_id, record_id)
);
"""


class StorageManager:
    """SQLite storage for AbatementRecord objects."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._conn: sqlite3.Connection | None = None
        self.init_db()

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def init_db(self) -> None:
        """Create tables if they don't exist."""
        conn = self._get_conn()
        with conn:
            conn.execute(_CREATE_RECORDS_TABLE)
            conn.execute(_CREATE_URL_CACHE_TABLE)
            conn.execute(_CREATE_CRAWL_SESSIONS_TABLE)
            conn.execute(_CREATE_DUPLICATE_CLUSTERS_TABLE)
        logger.debug("Database initialised at %s", self.db_path)

    def save_record(self, record: AbatementRecord) -> str:
        """Persist a record and return its record_id."""
        conn = self._get_conn()
        data_json = record.model_dump_json()
        with conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO abatement_records
                    (record_id, measure_name, measure_slug, sector, geography,
                     publication_year, source_url, source_type, quality_score,
                     mac, capex, currency, data_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.record_id,
                    record.measure_name,
                    record.measure_slug,
                    record.sector,
                    record.geography,
                    record.publication_year,
                    record.source_url,
                    record.source_type,
                    record.quality_score,
                    record.mac,
                    record.capex,
                    record.currency,
                    data_json,
                ),
            )
        return record.record_id

    def get_record(self, record_id: str) -> AbatementRecord | None:
        """Retrieve a record by its ID."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT data_json FROM abatement_records WHERE record_id = ?", (record_id,)
        ).fetchone()
        if row is None:
            return None
        return AbatementRecord.model_validate_json(row["data_json"])

    def get_all_records(self, min_quality: float = 0.0) -> list[AbatementRecord]:
        """Retrieve all records above a minimum quality threshold."""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT data_json FROM abatement_records WHERE quality_score >= ?",
            (min_quality,),
        ).fetchall()
        records = []
        for row in rows:
            try:
                records.append(AbatementRecord.model_validate_json(row["data_json"]))
            except Exception as exc:
                logger.warning("Failed to deserialise record: %s", exc)
        return records

    def mark_url_visited(self, url: str, status_code: int) -> None:
        """Record that a URL has been visited."""
        conn = self._get_conn()
        with conn:
            conn.execute(
                "INSERT OR REPLACE INTO url_cache (url, status_code) VALUES (?, ?)",
                (url, status_code),
            )

    def is_url_visited(self, url: str) -> bool:
        """Check whether a URL has been visited."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT 1 FROM url_cache WHERE url = ?", (url,)
        ).fetchone()
        return row is not None

    def find_duplicates(self) -> list[list[str]]:
        """Find groups of potentially duplicate records by slug similarity.

        Returns a list of lists, each inner list containing record_ids for a
        cluster of suspected duplicates.
        """
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT record_id, measure_slug FROM abatement_records ORDER BY measure_slug"
        ).fetchall()

        clusters: dict[str, list[str]] = {}
        for row in rows:
            slug = row["measure_slug"] or ""
            if slug not in clusters:
                clusters[slug] = []
            clusters[slug].append(row["record_id"])

        return [ids for ids in clusters.values() if len(ids) > 1]

    def save_session(self, session_id: str, scope_config: dict, stats: dict) -> None:
        """Persist a crawl session record."""
        conn = self._get_conn()
        with conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO crawl_sessions (session_id, scope_config, stats)
                VALUES (?, ?, ?)
                """,
                (session_id, json.dumps(scope_config), json.dumps(stats)),
            )

    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
