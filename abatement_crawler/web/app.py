"""Flask web UI for the Abatement Crawler."""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import threading
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, make_response, redirect, render_template, request, url_for

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

# Global seed state shared across requests
_seed_status: dict[str, Any] = {
    "running": False,
    "message": "No seed run has been started yet.",
    "categories_total": 0,
    "categories_done": 0,
    "records_saved": 0,
    "error": None,
}
_seed_lock = threading.Lock()

# Global synthesis state
_synth_status: dict[str, Any] = {
    "running": False,
    "message": "No synthesis has been run yet.",
    "synthesised_count": 0,
    "archetypes_processed": 0,
    "error": None,
}
_synth_lock = threading.Lock()

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

    def _get_category_stats():
        """Return (stats_by_slug, overall) for the dashboard. Returns ({}, {}) on error."""
        try:
            from ..storage import StorageManager  # noqa: PLC0415

            config = _load_config()
            storage = StorageManager(config.db_path)
            rows = storage.get_category_stats()
            storage.close()
            stats_by_slug = {r["slug"]: r for r in rows}
            total = sum(r["count"] for r in rows)
            avg_q = (
                sum(r["avg_quality"] * r["count"] for r in rows) / total
            ) if total else 0.0
            cats_with_data = sum(1 for r in rows if r["count"] > 0)
            overall = {"total": total, "avg_quality": avg_q, "cats_with_data": cats_with_data}
            return stats_by_slug, overall
        except Exception:
            return {}, {"total": 0, "avg_quality": 0.0, "cats_with_data": 0}

    # ------------------------------------------------------------------
    # Context processors
    # ------------------------------------------------------------------

    @app.context_processor
    def inject_scope():
        try:
            config = _load_config()
            return {"scope_chips": config.scope.describe()}
        except Exception:
            return {"scope_chips": []}

    # ------------------------------------------------------------------
    # Routes
    # ------------------------------------------------------------------

    @app.route("/")
    def index():
        from ..taxonomy import CATEGORIES  # noqa: PLC0415

        stats_by_slug, overall = _get_category_stats()
        return render_template(
            "index.html",
            categories=CATEGORIES,
            stats_by_slug=stats_by_slug,
            overall=overall,
        )

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
            category = request.args.get("category", "").strip()
            min_quality = float(request.args.get("min_quality", 0.0))
            synthesised_filter = request.args.get("synthesised", "all")  # all | yes | no

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
            if category:
                records = [r for r in records if r.abatement_category == category]
            if synthesised_filter == "yes":
                records = [r for r in records if r.is_synthesised]
            elif synthesised_filter == "no":
                records = [r for r in records if not r.is_synthesised]

            total = len(records)

            page = max(1, int(request.args.get("page", 1)))
            per_page = 25
            records = records[(page - 1) * per_page : page * per_page]

        except Exception as exc:
            logger.exception("Error loading results")
            error = str(exc)
            page = 1
            per_page = 25
            synthesised_filter = "all"

        return render_template(
            "results.html",
            records=records,
            total=total,
            page=page,
            per_page=per_page,
            q=request.args.get("q", ""),
            geography=request.args.get("geography", ""),
            sector=request.args.get("sector", ""),
            category=request.args.get("category", ""),
            min_quality=request.args.get("min_quality", "0"),
            synthesised_filter=synthesised_filter,
            error=error,
        )

    @app.route("/results/<record_id>")
    def result_detail(record_id: str):
        from ..quality import (  # noqa: PLC0415
            _cost_data_present,
            _data_recency,
            _evidence_completeness,
            _extraction_confidence,
            _geography_specificity,
            _peer_review_score,
            _source_type_prior,
        )

        error = None
        record = None
        quality_components = []
        try:
            from ..storage import StorageManager  # noqa: PLC0415

            config = _load_config()
            storage = StorageManager(config.db_path)
            record = storage.get_record(record_id)
            if record is None:
                error = f"Record '{record_id}' not found."
            else:
                quality_components = [
                    ("Evidence completeness", 0.20, _evidence_completeness(record)),
                    ("Source type prior",     0.20, _source_type_prior(record)),
                    ("Peer review",           0.15, _peer_review_score(record)),
                    ("Data recency",          0.15, _data_recency(record)),
                    ("Cost data present",     0.15, _cost_data_present(record)),
                    ("Geography specificity", 0.10, _geography_specificity(record)),
                    ("Extraction confidence", 0.05, _extraction_confidence(record)),
                ]
        except Exception as exc:
            logger.exception("Error loading record detail")
            error = str(exc)

        return render_template(
            "result_detail.html",
            record=record,
            error=error,
            quality_components=quality_components,
        )

    # ---- Categories ---------------------------------------------------

    @app.route("/categories")
    def categories_view():
        from ..taxonomy import CATEGORIES  # noqa: PLC0415

        stats_by_slug, _ = _get_category_stats()
        return render_template(
            "categories.html",
            categories=CATEGORIES,
            stats_by_slug=stats_by_slug,
        )

    # ---- Archetypes ---------------------------------------------------

    @app.route("/archetypes")
    def archetypes_view():
        from ..storage import StorageManager  # noqa: PLC0415

        config = _load_config()
        storage = StorageManager(config.db_path)
        archetypes = storage.get_archetypes()
        counts = storage.get_archetype_record_counts()
        storage.close()
        return render_template("archetypes.html", archetypes=archetypes, counts=counts)

    # ---- Seed ---------------------------------------------------------

    @app.route("/seed", methods=["GET"])
    def seed_view():
        from ..taxonomy import CATEGORIES, CATEGORY_SLUGS  # noqa: PLC0415

        stats_by_slug, _ = _get_category_stats()
        counts_by_slug = {slug: stats_by_slug.get(slug, {}).get("count", 0) for slug in CATEGORY_SLUGS}

        preselect_raw = request.args.get("categories", "")
        if preselect_raw:
            preselect = [s for s in preselect_raw.split(",") if s in CATEGORY_SLUGS]
        else:
            # Default to scope.abatement_types if configured
            scope_types = _load_config().scope.abatement_types
            preselect = [s for s in scope_types if s in CATEGORY_SLUGS] if scope_types else []

        with _seed_lock:
            status = dict(_seed_status)

        return render_template(
            "seed.html",
            categories=CATEGORIES,
            counts_by_slug=counts_by_slug,
            status=status,
            preselect=preselect,
        )

    @app.route("/seed/start", methods=["POST"])
    def seed_start():
        global _seed_status  # noqa: PLW0603
        from ..taxonomy import CATEGORY_LOOKUP  # noqa: PLC0415

        with _seed_lock:
            if _seed_status["running"]:
                return redirect(url_for("seed_view"))

        slugs = request.form.getlist("categories")
        slugs = [s for s in slugs if s in CATEGORY_LOOKUP]
        if not slugs:
            return redirect(url_for("seed_view"))

        selected_categories = [CATEGORY_LOOKUP[s] for s in slugs]

        with _seed_lock:
            _seed_status.update({
                "running": True,
                "message": "Starting…",
                "categories_total": len(selected_categories),
                "categories_done": 0,
                "records_saved": 0,
                "error": None,
            })

        cfg_path = app.config["CRAWLER_CONFIG_PATH"]

        def _run() -> None:
            global _seed_status  # noqa: PLW0603
            try:
                from ..config import CrawlerConfig  # noqa: PLC0415
                from ..quality import score_quality  # noqa: PLC0415
                from ..seeder import LLMSeeder  # noqa: PLC0415

                config = CrawlerConfig.from_yaml(cfg_path)
                seeder = LLMSeeder(config)
                saved = 0

                for i, cat in enumerate(selected_categories):
                    with _seed_lock:
                        _seed_status["message"] = f"Generating: {cat.name}…"

                    record = seeder._generate_for_category(cat)
                    if record is not None:
                        try:
                            record = seeder._normaliser.normalise(record)
                        except Exception:
                            pass
                        quality, flags = score_quality(record)
                        record = record.model_copy(
                            update={"quality_score": quality, "quality_flags": flags}
                        )
                        seeder._storage.save_record(record)
                        saved += 1

                    with _seed_lock:
                        _seed_status["categories_done"] = i + 1
                        _seed_status["records_saved"] = saved

                seeder._storage.close()

                with _seed_lock:
                    _seed_status.update({
                        "running": False,
                        "message": f"Completed — {saved} records saved.",
                        "error": None,
                    })

            except Exception as exc:
                logger.exception("Seed run failed")
                with _seed_lock:
                    _seed_status.update({
                        "running": False,
                        "message": "Failed with an error.",
                        "error": str(exc),
                    })

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()

        return redirect(url_for("seed_view"))

    @app.route("/seed/status")
    def seed_status():
        with _seed_lock:
            return jsonify(dict(_seed_status))

    # ---- Applicable categories ----------------------------------------

    @app.route("/applicable-categories", methods=["GET", "POST"])
    def applicable_categories_view():
        # Pre-populate form fields from scope config
        config = _load_config()
        scope = config.scope
        sector = scope.industry or (scope.sectors[0] if scope.sectors else "")
        process = scope.process or ""
        asset_type = scope.asset_type or ""

        results = None
        seed_url = None
        error = None

        if request.method == "POST":
            sector = request.form.get("sector", "").strip()
            process = request.form.get("process", "").strip()
            asset_type = request.form.get("asset_type", "").strip()

        # Run lookup on POST, or on GET when scope provides enough context
        run_lookup = request.method == "POST" or bool(sector or process or asset_type)
        if run_lookup:
            try:
                from ..applicability import get_applicable_categories  # noqa: PLC0415

                applicable, rationale = get_applicable_categories(
                    config, sector=sector, process=process, asset_type=asset_type
                )
                results = [(cat, rationale.get(cat.slug, "")) for cat in applicable]
                if applicable:
                    seed_url = url_for("seed_view") + "?categories=" + ",".join(
                        c.slug for c in applicable
                    )
            except RuntimeError as exc:
                error = str(exc)
            except Exception as exc:
                logger.exception("Applicable categories lookup failed")
                error = str(exc)

        return render_template(
            "applicable_categories.html",
            sector=sector,
            process=process,
            asset_type=asset_type,
            results=results,
            seed_url=seed_url,
            error=error,
        )

    # ---- Export -------------------------------------------------------

    @app.route("/export", methods=["GET", "POST"])
    def export_view():
        from ..taxonomy import CATEGORIES  # noqa: PLC0415

        error = None

        if request.method == "GET":
            try:
                from ..storage import StorageManager  # noqa: PLC0415

                config = _load_config()
                storage = StorageManager(config.db_path)
                record_count = len(storage.get_all_records(min_quality=0.0))
                storage.close()
            except Exception:
                record_count = 0
            return render_template(
                "export.html",
                categories=CATEGORIES,
                record_count=record_count,
                fmt=request.args.get("format", "csv"),
                min_quality=0.3,
                selected_categories=[],
                error=None,
            )

        # POST — build and stream the file
        fmt = request.form.get("format", "csv")
        try:
            min_quality = float(request.form.get("min_quality", 0.0))
        except ValueError:
            min_quality = 0.0
        selected_cats = request.form.getlist("categories")

        try:
            from ..storage import StorageManager  # noqa: PLC0415

            config = _load_config()
            storage = StorageManager(config.db_path)
            records = storage.get_all_records(min_quality=min_quality)
            storage.close()

            if selected_cats:
                records = [r for r in records if r.abatement_category in selected_cats]

            if fmt == "jsonl":
                buf = io.BytesIO()
                for r in records:
                    buf.write((r.model_dump_json() + "\n").encode())
                buf.seek(0)
                resp = make_response(buf.read())
                resp.headers["Content-Type"] = "application/x-ndjson"
                resp.headers["Content-Disposition"] = "attachment; filename=records.jsonl"
                return resp

            elif fmt == "csv":
                si = io.StringIO()
                if records:
                    fieldnames = list(records[0].model_fields.keys())
                    writer = csv.DictWriter(si, fieldnames=fieldnames)
                    writer.writeheader()
                    for r in records:
                        row = r.model_dump()
                        for k, v in row.items():
                            if isinstance(v, list):
                                row[k] = "; ".join(str(x) for x in v)
                        writer.writerow(row)
                buf = io.BytesIO(si.getvalue().encode())
                resp = make_response(buf.read())
                resp.headers["Content-Type"] = "text/csv"
                resp.headers["Content-Disposition"] = "attachment; filename=records.csv"
                return resp

            elif fmt == "parquet":
                try:
                    import pandas as pd  # noqa: PLC0415
                except ImportError:
                    raise RuntimeError("pandas is not installed — parquet export unavailable.")
                rows = []
                for r in records:
                    row = r.model_dump()
                    for k, v in row.items():
                        if isinstance(v, list):
                            row[k] = json.dumps(v)
                    rows.append(row)
                df = pd.DataFrame(rows)
                buf = io.BytesIO()
                df.to_parquet(buf, index=False)
                buf.seek(0)
                resp = make_response(buf.read())
                resp.headers["Content-Type"] = "application/octet-stream"
                resp.headers["Content-Disposition"] = "attachment; filename=records.parquet"
                return resp

            elif fmt == "markdown":
                from ..export import Exporter  # noqa: PLC0415

                exporter = Exporter(output_dir="/tmp")
                md = exporter.export_markdown_report(records, scope=config.scope)
                buf = io.BytesIO(md.encode())
                resp = make_response(buf.read())
                resp.headers["Content-Type"] = "text/markdown"
                resp.headers["Content-Disposition"] = "attachment; filename=report.md"
                return resp

        except Exception as exc:
            logger.exception("Export failed")
            error = str(exc)

        try:
            from ..storage import StorageManager  # noqa: PLC0415

            config = _load_config()
            storage = StorageManager(config.db_path)
            record_count = len(storage.get_all_records(min_quality=0.0))
            storage.close()
        except Exception:
            record_count = 0

        return render_template(
            "export.html",
            categories=CATEGORIES,
            record_count=record_count,
            fmt=fmt,
            min_quality=min_quality,
            selected_categories=selected_cats,
            error=error,
        )

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

    # ---- Synthesise ---------------------------------------------------

    @app.route("/synthesise/start", methods=["POST"])
    def synthesise_start():
        global _synth_status  # noqa: PLW0603

        with _synth_lock:
            if _synth_status["running"]:
                return jsonify({"error": "Synthesis is already running."}), 400

            _synth_status = {
                "running": True,
                "message": "Starting…",
                "synthesised_count": 0,
                "archetypes_processed": 0,
                "error": None,
            }

        cfg_path = app.config["CRAWLER_CONFIG_PATH"]
        sector = request.form.get("sector", "").strip() or None

        def _run() -> None:
            global _synth_status  # noqa: PLW0603
            try:
                import json  # noqa: PLC0415
                from pathlib import Path  # noqa: PLC0415

                from ..config import CrawlerConfig  # noqa: PLC0415
                from ..crawler import _safe_slug  # noqa: PLC0415
                from ..models import AbatementArchetype  # noqa: PLC0415
                from ..normalisation import Normaliser  # noqa: PLC0415
                from ..quality import score_quality  # noqa: PLC0415
                from ..storage import StorageManager  # noqa: PLC0415
                from ..synthesis import ArchetypeSynthesiser  # noqa: PLC0415

                config = CrawlerConfig.from_yaml(cfg_path)

                resolved_sector = (
                    sector
                    or (config.pipeline.sector if config.pipeline else None)
                    or config.scope.industry
                    or (config.scope.sectors[0] if config.scope.sectors else None)
                )
                if not resolved_sector:
                    raise ValueError(
                        "No sector configured. Set pipeline.sector or scope.industry in config."
                    )

                sector_slug = _safe_slug(resolved_sector)
                archetypes_path = Path(config.output_dir) / f"archetypes_{sector_slug}.json"
                if archetypes_path.exists():
                    archetypes_data = json.loads(archetypes_path.read_text())
                else:
                    # Fall back to archetypes stored in the database
                    _tmp_storage = StorageManager(config.db_path)
                    archetypes_data = _tmp_storage.get_archetypes(sector=resolved_sector)
                    _tmp_storage.close()
                    if not archetypes_data:
                        raise FileNotFoundError(
                            f"No archetypes found for sector '{resolved_sector}'. "
                            "Run pipeline mode first to generate archetypes."
                        )
                    logger.info(
                        "Archetypes file not found at %s; loaded %d archetypes from database.",
                        archetypes_path,
                        len(archetypes_data),
                    )
                _archetype_fields = {f.name for f in AbatementArchetype.__dataclass_fields__.values()}
                archetypes = [
                    AbatementArchetype(**{k: v for k, v in d.items() if k in _archetype_fields})
                    for d in archetypes_data
                ]

                storage = StorageManager(config.db_path)
                synthesiser = ArchetypeSynthesiser(config)
                normaliser = Normaliser(config.base_currency, config.base_year)
                all_records = storage.get_all_records(min_quality=0.0)

                with _synth_lock:
                    _synth_status["message"] = f"Synthesising {len(archetypes)} archetypes…"

                n_synthesised = 0
                for i, archetype in enumerate(archetypes, 1):
                    with _synth_lock:
                        _synth_status["archetypes_processed"] = i
                        _synth_status["message"] = (
                            f"Archetype {i}/{len(archetypes)}: {archetype.name}…"
                        )

                    slug = _safe_slug(archetype.name)
                    arch_records = [
                        r for r in all_records if r.archetype_slug == slug and not r.is_synthesised
                    ]
                    arch_fragments = storage.get_fragments_for_archetype(slug)

                    result = synthesiser.synthesise(archetype, arch_records, arch_fragments)
                    if result is None:
                        continue

                    result = normaliser.normalise_record(result)
                    quality, flags = score_quality(result)
                    data = result.model_dump()
                    data["quality_score"] = quality
                    data["quality_flags"] = list(set(result.quality_flags + flags))
                    from ..models import AbatementRecord  # noqa: PLC0415
                    storage.save_record(AbatementRecord(**data))
                    n_synthesised += 1

                    with _synth_lock:
                        _synth_status["synthesised_count"] = n_synthesised

                storage.close()

                with _synth_lock:
                    _synth_status.update(
                        {
                            "running": False,
                            "message": f"Completed: {n_synthesised} records synthesised.",
                            "synthesised_count": n_synthesised,
                            "error": None,
                        }
                    )
            except Exception as exc:
                logger.exception("Synthesis failed")
                with _synth_lock:
                    _synth_status.update(
                        {
                            "running": False,
                            "message": "Failed with an error.",
                            "error": str(exc),
                        }
                    )

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()

        return redirect(url_for("crawl_view"))

    @app.route("/synthesise/status")
    def synthesise_status():
        with _synth_lock:
            return jsonify(dict(_synth_status))

    @app.route("/captcha-queue/resolve", methods=["GET", "POST"])
    def captcha_resolve():
        from ..storage import StorageManager  # noqa: PLC0415

        blocked_url = request.args.get("url") or request.form.get("url", "")
        if not blocked_url:
            return redirect(url_for("captcha_queue_view"))

        if request.method == "GET":
            return render_template("captcha_resolve.html", url=blocked_url)

        action = request.form.get("action", "")
        config = _load_config()
        storage = StorageManager(config.db_path)

        if action == "skip":
            storage.update_captcha_status(blocked_url, "skipped")
            storage.close()
            return redirect(url_for("captcha_queue_view"))

        uploaded = request.files.get("file")
        if action == "upload" and uploaded and uploaded.filename:
            from ..extraction import LLMExtractor  # noqa: PLC0415
            from ..ingestion import DocumentIngester  # noqa: PLC0415
            from ..models import AbatementRecord  # noqa: PLC0415
            from ..normalisation import Normaliser  # noqa: PLC0415
            from ..quality import score_quality  # noqa: PLC0415

            ingester = DocumentIngester(config)
            extractor = LLMExtractor(config)
            normaliser = Normaliser(config.base_currency, config.base_year)

            raw = uploaded.read()
            fmt = ingester._detect_format(uploaded.filename, uploaded.content_type or "")
            parse = {
                "pdf": ingester._ingest_pdf,
                "xlsx": ingester._ingest_excel,
                "xls": ingester._ingest_excel,
                "docx": ingester._ingest_docx,
                "json": ingester._ingest_json,
            }.get(fmt, ingester._ingest_html)
            text = parse(blocked_url, raw)

            records_saved = 0
            if text:
                for chunk in ingester.chunk_text(text):
                    for record in extractor.extract(chunk, source_url=blocked_url, source_title=uploaded.filename):
                        record = normaliser.normalise_record(record)
                        quality, flags = score_quality(record)
                        data = record.model_dump()
                        data["quality_score"] = quality
                        data["quality_flags"] = list(set(record.quality_flags + flags))
                        storage.save_record(AbatementRecord(**data))
                        records_saved += 1

            storage.update_captcha_status(blocked_url, "resolved")
            storage.close()
            return redirect(url_for("captcha_queue_view") + f"?resolved={records_saved}")

        storage.close()
        return redirect(url_for("captcha_resolve") + f"?url={blocked_url}")

    return app
