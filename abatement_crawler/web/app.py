"""Flask web UI for the Abatement Crawler."""

from __future__ import annotations

import logging
import os
import threading
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, redirect, render_template, request, url_for

logger = logging.getLogger(__name__)

# Global crawl state shared across requests
_crawl_status: dict[str, Any] = {
    "running": False,
    "message": "No crawl has been started yet.",
    "records_found": 0,
    "documents_processed": 0,
    "error": None,
}
_crawl_lock = threading.Lock()

_MASKED = "***set***"


def create_app(config_path: str | None = None) -> Flask:
    """Create and configure the Flask application."""
    app = Flask(__name__)
    app.secret_key = os.urandom(24)

    default_config_path = config_path or os.environ.get(
        "CRAWLER_CONFIG_PATH", "./config/config.yaml"
    )
    app.config["CRAWLER_CONFIG_PATH"] = default_config_path

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _load_config():
        from ..config import CrawlerConfig  # noqa: PLC0415

        cfg_path = app.config["CRAWLER_CONFIG_PATH"]
        if Path(cfg_path).exists():
            return CrawlerConfig.from_yaml(cfg_path)
        return CrawlerConfig()

    # ------------------------------------------------------------------
    # Routes
    # ------------------------------------------------------------------

    @app.route("/")
    def index():
        return render_template("index.html")

    # ---- Config -------------------------------------------------------

    @app.route("/config", methods=["GET"])
    def config_view():
        error = None
        saved = request.args.get("saved", False)
        try:
            config = _load_config()
        except Exception as exc:
            from ..config import CrawlerConfig  # noqa: PLC0415

            error = str(exc)
            config = CrawlerConfig()

        return render_template(
            "config.html",
            config=config,
            config_path=app.config["CRAWLER_CONFIG_PATH"],
            masked=_MASKED,
            error=error,
            saved=saved,
        )

    @app.route("/config", methods=["POST"])
    def config_save():
        from ..config import CrawlerConfig  # noqa: PLC0415
        from ..models import ScopeConfig  # noqa: PLC0415

        cfg_path = app.config["CRAWLER_CONFIG_PATH"]
        try:
            # Preserve existing secrets if the masked placeholder is submitted
            try:
                existing = _load_config()
            except Exception:
                existing = CrawlerConfig()

            def _list_field(name: str) -> list[str]:
                val = request.form.get(name, "")
                return [x.strip() for x in val.split(",") if x.strip()]

            year_start = int(request.form.get("year_range_start", 2015))
            year_end = int(request.form.get("year_range_end", 2025))

            scope = ScopeConfig(
                industry=request.form.get("industry") or None,
                process=request.form.get("process") or None,
                asset_type=request.form.get("asset_type") or None,
                company=request.form.get("company") or None,
                geography=_list_field("geography"),
                sectors=_list_field("sectors"),
                abatement_types=_list_field("abatement_types"),
                year_range=(year_start, year_end),
                languages=_list_field("languages") or ["en"],
            )

            search_api_key = request.form.get("search_api_key", "")
            if not search_api_key or search_api_key == _MASKED:
                search_api_key = existing.search_api_key

            llm_api_key = request.form.get("llm_api_key", "")
            if not llm_api_key or llm_api_key == _MASKED:
                llm_api_key = existing.llm_api_key

            config = CrawlerConfig(
                scope=scope,
                search_api=request.form.get("search_api", "duckduckgo"),
                search_api_key=search_api_key,
                max_search_queries=int(request.form.get("max_search_queries", 200)),
                results_per_query=int(request.form.get("results_per_query", 10)),
                max_depth=int(request.form.get("max_depth", 4)),
                relevance_threshold=float(request.form.get("relevance_threshold", 0.3)),
                reflection_interval=int(request.form.get("reflection_interval", 50)),
                max_total_documents=int(request.form.get("max_total_documents", 2000)),
                llm_model=request.form.get("llm_model", "claude-sonnet-4-20250514"),
                llm_api_key=llm_api_key,
                extraction_temperature=float(
                    request.form.get("extraction_temperature", 0)
                ),
                max_retries=int(request.form.get("max_retries", 2)),
                base_currency=request.form.get("base_currency", "GBP"),
                base_year=int(request.form.get("base_year", 2023)),
                min_quality_for_export=float(
                    request.form.get("min_quality_for_export", 0.3)
                ),
                db_path=request.form.get("db_path", "./abatement_records.db"),
                output_dir=request.form.get("output_dir", "./output/"),
                requests_per_second=float(request.form.get("requests_per_second", 2.0)),
                pdf_timeout_seconds=int(request.form.get("pdf_timeout_seconds", 30)),
                respect_robots_txt="respect_robots_txt" in request.form,
            )

            config.to_yaml(cfg_path)
            return redirect(url_for("config_view", saved=1))

        except Exception as exc:
            logger.exception("Error saving config")
            from ..config import CrawlerConfig  # noqa: PLC0415

            return render_template(
                "config.html",
                config=CrawlerConfig(),
                config_path=cfg_path,
                masked=_MASKED,
                error=str(exc),
                saved=False,
            )

    # ---- Results ------------------------------------------------------

    @app.route("/results")
    def results():
        error = None
        records = []
        total = 0
        try:
            from ..storage import StorageManager  # noqa: PLC0415

            config = _load_config()
            storage = StorageManager(config.db_path)

            q = request.args.get("q", "").lower().strip()
            geography = request.args.get("geography", "").strip()
            sector = request.args.get("sector", "").strip()
            min_quality = float(request.args.get("min_quality", 0.0))

            records = storage.get_all_records(min_quality=min_quality)

            if q:
                records = [
                    r
                    for r in records
                    if q in (r.measure_name or "").lower()
                    or q in (r.abatement_category or "").lower()
                    or q in (r.notes or "").lower()
                ]
            if geography:
                records = [
                    r
                    for r in records
                    if geography.lower() in (r.geography or "").lower()
                ]
            if sector:
                records = [
                    r
                    for r in records
                    if sector.lower() in (r.sector or "").lower()
                ]

            total = len(records)

            page = max(1, int(request.args.get("page", 1)))
            per_page = 25
            records = records[(page - 1) * per_page : page * per_page]

        except Exception as exc:
            logger.exception("Error loading results")
            error = str(exc)
            page = 1
            per_page = 25

        return render_template(
            "results.html",
            records=records,
            total=total,
            page=page,
            per_page=per_page,
            q=request.args.get("q", ""),
            geography=request.args.get("geography", ""),
            sector=request.args.get("sector", ""),
            min_quality=request.args.get("min_quality", "0"),
            error=error,
        )

    @app.route("/results/<record_id>")
    def result_detail(record_id: str):
        error = None
        record = None
        try:
            from ..storage import StorageManager  # noqa: PLC0415

            config = _load_config()
            storage = StorageManager(config.db_path)
            record = storage.get_record(record_id)
            if record is None:
                error = f"Record '{record_id}' not found."
        except Exception as exc:
            logger.exception("Error loading record detail")
            error = str(exc)

        return render_template("result_detail.html", record=record, error=error)

    # ---- Crawl --------------------------------------------------------

    @app.route("/crawl", methods=["GET"])
    def crawl_view():
        return render_template("crawl.html", status=_crawl_status)

    @app.route("/crawl/start", methods=["POST"])
    def crawl_start():
        global _crawl_status  # noqa: PLW0603

        with _crawl_lock:
            if _crawl_status["running"]:
                return jsonify({"error": "A crawl is already running."}), 400

            _crawl_status = {
                "running": True,
                "message": "Starting…",
                "records_found": 0,
                "documents_processed": 0,
                "error": None,
            }

        cfg_path = app.config["CRAWLER_CONFIG_PATH"]
        mode = request.form.get("mode", "search")
        seed_urls_raw = request.form.get("seed_urls", "")
        seed_urls = [u.strip() for u in seed_urls_raw.splitlines() if u.strip()]
        fresh = "fresh" in request.form

        def _run() -> None:
            global _crawl_status  # noqa: PLW0603
            try:
                from ..config import CrawlerConfig  # noqa: PLC0415
                from ..crawler import AbatementCrawler  # noqa: PLC0415
                from ..storage import StorageManager  # noqa: PLC0415

                config = CrawlerConfig.from_yaml(cfg_path)
                if fresh:
                    StorageManager(config.db_path).clear_url_cache()

                def _on_progress(docs_processed: int, records_found: int) -> None:
                    with _crawl_lock:
                        _crawl_status["documents_processed"] = docs_processed
                        _crawl_status["records_found"] = records_found

                crawler = AbatementCrawler(config, progress_callback=_on_progress)

                with _crawl_lock:
                    _crawl_status["message"] = "Crawling…"

                if mode == "seed":
                    stats = crawler.run_seed_mode(seed_urls)
                else:
                    stats = crawler.run_search_mode()

                with _crawl_lock:
                    _crawl_status.update(
                        {
                            "running": False,
                            "message": "Completed successfully.",
                            "records_found": stats.get("total_records", 0),
                            "documents_processed": stats.get(
                                "documents_processed", 0
                            ),
                            "error": None,
                        }
                    )
            except Exception as exc:
                logger.exception("Crawl failed")
                with _crawl_lock:
                    _crawl_status.update(
                        {
                            "running": False,
                            "message": "Failed with an error.",
                            "error": str(exc),
                        }
                    )

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()

        return redirect(url_for("crawl_view"))

    @app.route("/crawl/status")
    def crawl_status():
        with _crawl_lock:
            return jsonify(dict(_crawl_status))

    # ---- Captcha Queue ------------------------------------------------

    @app.route("/captcha-queue", methods=["GET", "POST"])
    def captcha_queue_view():
        from ..storage import StorageManager  # noqa: PLC0415

        config = _load_config()
        storage = StorageManager(config.db_path)

        if request.method == "POST":
            url = request.form.get("url", "")
            action = request.form.get("action", "")
            if url and action in ("resolve", "skip"):
                new_status = "resolved" if action == "resolve" else "skipped"
                storage.update_captcha_status(url, new_status)
            storage.close()
            return redirect(url_for("captcha_queue_view"))

        filter_status = request.args.get("status") or None
        entries = storage.list_captcha_queue(status=filter_status)
        counts = {
            "pending": sum(1 for e in entries if e["status"] == "pending"),
            "resolved": sum(1 for e in entries if e["status"] == "resolved"),
            "skipped": sum(1 for e in entries if e["status"] == "skipped"),
        }
        # When a filter is active the counts above reflect only the filtered set;
        # re-fetch totals unfiltered for the badge display.
        all_entries = storage.list_captcha_queue()
        total_counts = {
            "pending": sum(1 for e in all_entries if e["status"] == "pending"),
            "resolved": sum(1 for e in all_entries if e["status"] == "resolved"),
            "skipped": sum(1 for e in all_entries if e["status"] == "skipped"),
        }
        storage.close()

        return render_template(
            "captcha_queue.html",
            entries=entries,
            counts=counts,
            total_counts=total_counts,
            filter_status=filter_status,
        )

    return app
