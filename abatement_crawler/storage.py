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
    is_deleted INTEGER NOT NULL DEFAULT 0,
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

_CREATE_CAPTCHA_QUEUE_TABLE = """
CREATE TABLE IF NOT EXISTS captcha_queue (
    url TEXT PRIMARY KEY,
    captcha_type TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    detected_at TEXT DEFAULT (datetime('now')),
    resolved_at TEXT,
    notes TEXT
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
            conn.execute(_CREATE_CAPTCHA_QUEUE_TABLE)
            # Migrate existing databases: add is_deleted column if absent
            try:
                conn.execute(
                    "ALTER TABLE abatement_records ADD COLUMN is_deleted INTEGER NOT NULL DEFAULT 0"
                )
            except Exception:
                pass  # Column already exists
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
        """Retrieve all non-deleted records above a minimum quality threshold."""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT data_json FROM abatement_records WHERE quality_score >= ? AND is_deleted = 0",
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

    def mark_record_deleted(self, record_id: str) -> None:
        """Soft-delete a record by setting its is_deleted flag."""
        conn = self._get_conn()
        with conn:
            conn.execute(
                "UPDATE abatement_records SET is_deleted = 1 WHERE record_id = ?",
                (record_id,),
            )

    def find_duplicates(self) -> list[list[str]]:
        """Find groups of potentially duplicate records by slug similarity.

        Returns a list of lists, each inner list containing record_ids for a
        cluster of suspected duplicates.
        """
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT record_id, measure_slug FROM abatement_records"
            " WHERE is_deleted = 0 ORDER BY measure_slug"
        ).fetchall()

        clusters: dict[str, list[str]] = {}
        for row in rows:
            slug = row["measure_slug"] or ""
            if slug not in clusters:
                clusters[slug] = []
            clusters[slug].append(row["record_id"])

        return [ids for ids in clusters.values() if len(ids) > 1]

    def save_duplicate_clusters(self, clusters: list[list[str]]) -> None:
        """Persist duplicate clusters to the database, replacing any prior data."""
        conn = self._get_conn()
        with conn:
            conn.execute("DELETE FROM duplicate_clusters")
            for cluster_id, record_ids in enumerate(clusters):
                for record_id in record_ids:
                    conn.execute(
                        "INSERT INTO duplicate_clusters (cluster_id, record_id) VALUES (?, ?)",
                        (cluster_id, record_id),
                    )

    def deduplicate_records(self) -> int:
        """Deduplicate records by slug, keeping the highest quality_score per cluster.

        Soft-deletes lower-quality duplicates and saves clusters to the
        duplicate_clusters table. Returns the number of records soft-deleted.
        """
        clusters = self.find_duplicates()
        if not clusters:
            return 0
        self.save_duplicate_clusters(clusters)
        removed = 0
        conn = self._get_conn()
        for cluster in clusters:
            rows = conn.execute(
                "SELECT record_id, quality_score FROM abatement_records"
                f" WHERE record_id IN ({','.join('?' * len(cluster))})"
                " AND is_deleted = 0"
                " ORDER BY quality_score DESC",
                cluster,
            ).fetchall()
            for row in rows[1:]:
                self.mark_record_deleted(row["record_id"])
                removed += 1
        logger.info("Deduplication soft-deleted %d records.", removed)
        return removed

    def list_sessions(self) -> list[dict]:
        """Return a summary of all past crawl sessions, most recent first."""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT session_id, scope_config, stats, started_at"
            " FROM crawl_sessions ORDER BY started_at DESC"
        ).fetchall()
        results = []
        for row in rows:
            results.append(
                {
                    "session_id": row["session_id"],
                    "started_at": row["started_at"],
                    "scope_config": json.loads(row["scope_config"] or "{}"),
                    "stats": json.loads(row["stats"] or "{}"),
                }
            )
        return results

    def clear_url_cache(self) -> int:
        """Delete all URL cache entries. Returns the number of rows removed."""
        conn = self._get_conn()
        with conn:
            cursor = conn.execute("DELETE FROM url_cache")
        return cursor.rowcount

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

    def add_to_captcha_queue(
        self, url: str, captcha_type: str, notes: str = ""
    ) -> None:
        """Add a captcha-blocked URL to the queue with status 'pending'.

        Uses INSERT OR IGNORE so re-detection of an already-queued URL is a no-op.
        """
        conn = self._get_conn()
        with conn:
            conn.execute(
                "INSERT OR IGNORE INTO captcha_queue (url, captcha_type, notes)"
                " VALUES (?, ?, ?)",
                (url, captcha_type, notes),
            )

    def list_captcha_queue(self, status: str | None = None) -> list[dict]:
        """Return captcha queue entries ordered by detection time (newest first).

        Args:
            status: Optional filter — one of ``'pending'``, ``'resolved'``,
                    ``'skipped'``. If ``None``, all entries are returned.
        """
        conn = self._get_conn()
        if status:
            rows = conn.execute(
                "SELECT * FROM captcha_queue WHERE status = ?"
                " ORDER BY detected_at DESC",
                (status,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM captcha_queue ORDER BY detected_at DESC"
            ).fetchall()
        return [dict(row) for row in rows]

    def update_captcha_status(
        self, url: str, status: str, notes: str | None = None
    ) -> None:
        """Update the status of a captcha queue entry.

        Args:
            url: The URL to update.
            status: New status — ``'resolved'`` or ``'skipped'``.
            notes: Optional note to record alongside the status change.
        """
        conn = self._get_conn()
        with conn:
            if notes is not None:
                conn.execute(
                    "UPDATE captcha_queue"
                    " SET status = ?, resolved_at = datetime('now'), notes = ?"
                    " WHERE url = ?",
                    (status, notes, url),
                )
            else:
                conn.execute(
                    "UPDATE captcha_queue"
                    " SET status = ?, resolved_at = datetime('now')"
                    " WHERE url = ?",
                    (status, url),
                )

    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def get_category_stats(self) -> list[dict]:
        """Return per-category record counts and average quality scores.

        Categories with zero records are absent — the caller merges against
        taxonomy.CATEGORY_SLUGS to build the full 13-row view.

        Returns a list of dicts with keys: slug, count, avg_quality, with_cost.
        """
        conn = self._get_conn()
        rows = conn.execute(
            """
            SELECT
                json_extract(data_json, '$.abatement_category') AS slug,
                COUNT(*)                                          AS count,
                AVG(quality_score)                               AS avg_quality,
                SUM(CASE WHEN capex IS NOT NULL
                              OR mac  IS NOT NULL THEN 1 ELSE 0 END) AS with_cost
            FROM abatement_records
            WHERE is_deleted = 0
            GROUP BY slug
            """
        ).fetchall()
        return [dict(r) for r in rows]
