"""CLI entry point for the abatement crawler."""

from __future__ import annotations

import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _cmd_crawl(args: argparse.Namespace) -> int:
    from .config import CrawlerConfig  # noqa: PLC0415
    from .crawler import AbatementCrawler  # noqa: PLC0415
    from .storage import StorageManager  # noqa: PLC0415

    config = CrawlerConfig.from_yaml(args.config)

    errors, warnings = config.validate(mode=args.mode)
    for w in warnings:
        print(f"Warning: {w}", file=sys.stderr)
    if errors:
        for e in errors:
            print(f"Error: {e}", file=sys.stderr)
        return 1

    if getattr(args, "fresh", False):
        storage = StorageManager(config.db_path)
        n = storage.clear_url_cache()
        storage.close()
        print(f"Cleared {n} cached URLs — starting fresh crawl.")

    crawler = AbatementCrawler(config)

    if args.mode == "seed":
        if not args.seed_urls:
            print("Error: --seed-urls required for seed mode.", file=sys.stderr)
            return 1
        stats = crawler.run_seed_mode(args.seed_urls)
    elif args.mode == "pipeline":
        stats = crawler.run_pipeline_mode(sector=getattr(args, "sector", None))
    else:
        stats = crawler.run_search_mode()

    print(f"Crawl complete: {stats}")
    return 0


def _cmd_web(args: argparse.Namespace) -> int:
    from .web.app import create_app  # noqa: PLC0415

    app = create_app(config_path=args.config)
    app.run(host=args.host, port=args.port, debug=args.debug)
    return 0


def _cmd_sessions(args: argparse.Namespace) -> int:
    from .config import CrawlerConfig  # noqa: PLC0415
    from .storage import StorageManager  # noqa: PLC0415

    config = CrawlerConfig.from_yaml(args.config)
    storage = StorageManager(config.db_path)
    sessions = storage.list_sessions()
    storage.close()

    if not sessions:
        print("No crawl sessions found.")
        return 0

    for s in sessions:
        stats = s["stats"]
        print(
            f"{s['session_id']}"
            f"  started={s['started_at']}"
            f"  records={stats.get('total_records', '?')}"
            f"  exported={stats.get('qualified_records', '?')}"
        )
    return 0


def _cmd_captcha_queue(args: argparse.Namespace) -> int:
    from .config import CrawlerConfig  # noqa: PLC0415
    from .storage import StorageManager  # noqa: PLC0415

    config = CrawlerConfig.from_yaml(args.config)
    storage = StorageManager(config.db_path)

    if args.resolve:
        storage.update_captcha_status(args.resolve, "resolved")
        print(f"Marked as resolved: {args.resolve}")
    elif args.skip:
        storage.update_captcha_status(args.skip, "skipped")
        print(f"Marked as skipped: {args.skip}")
    else:
        entries = storage.list_captcha_queue(status=args.status or None)
        if not entries:
            print("No captcha queue entries.")
        else:
            for e in entries:
                print(
                    f"[{e['status']:8s}] {e['captcha_type'] or 'unknown':15s}"
                    f"  {e['detected_at']}  {e['url']}"
                )

    storage.close()
    return 0


def _cmd_seed(args: argparse.Namespace) -> int:
    from .config import CrawlerConfig  # noqa: PLC0415
    from .seeder import LLMSeeder  # noqa: PLC0415
    from .taxonomy import CATEGORIES, CATEGORY_LOOKUP  # noqa: PLC0415

    config = CrawlerConfig.from_yaml(args.config)
    errors, warnings = config.validate(mode="search")
    for w in warnings:
        print(f"Warning: {w}", file=sys.stderr)
    if errors:
        for e in errors:
            print(f"Error: {e}", file=sys.stderr)
        return 1

    if args.categories:
        slugs = [s.strip() for s in args.categories.split(",")]
        unknown = [s for s in slugs if s not in CATEGORY_LOOKUP]
        if unknown:
            print(f"Error: unknown category slugs: {', '.join(unknown)}", file=sys.stderr)
            return 1
        targets = [CATEGORY_LOOKUP[s] for s in slugs]
    else:
        targets = list(CATEGORIES)

    seeder = LLMSeeder(config)
    stats = seeder.run(categories=targets)
    print(
        f"Seed complete: {stats['total_records']} records generated, "
        f"{stats['qualified_records']} above quality threshold."
    )
    return 0


def _cmd_applicable_categories(args: argparse.Namespace) -> int:
    from .applicability import get_applicable_categories  # noqa: PLC0415
    from .config import CrawlerConfig  # noqa: PLC0415

    config = CrawlerConfig.from_yaml(args.config)
    errors, warnings = config.validate(mode="search")
    for w in warnings:
        print(f"Warning: {w}", file=sys.stderr)
    if errors:
        for e in errors:
            print(f"Error: {e}", file=sys.stderr)
        return 1

    applicable, rationale = get_applicable_categories(
        config,
        sector=args.sector or "",
        process=args.process or "",
        asset_type=args.asset_type or "",
    )

    if not applicable:
        print("No applicable categories identified.")
        return 0

    print(f"\nApplicable abatement categories ({len(applicable)} of 13):\n")
    for cat in applicable:
        note = rationale.get(cat.slug, "")
        print(f"  [{cat.slug}]  {cat.name}")
        if note:
            print(f"    {note}")
    print()
    return 0


def _cmd_export(args: argparse.Namespace) -> int:
    from .config import CrawlerConfig  # noqa: PLC0415
    from .export import Exporter  # noqa: PLC0415
    from .storage import StorageManager  # noqa: PLC0415

    config = CrawlerConfig.from_yaml(args.config)
    storage = StorageManager(config.db_path)
    records = storage.get_all_records(min_quality=args.min_quality)
    exporter = Exporter(config.output_dir)

    fmt = args.format
    if fmt == "jsonl":
        exporter.export_jsonl(records)
    elif fmt == "csv":
        exporter.export_csv(records)
    elif fmt == "parquet":
        exporter.export_parquet(records)
    elif fmt == "markdown":
        exporter.export_markdown_report(records, scope=config.scope)
    else:
        print(f"Unknown format: {fmt}", file=sys.stderr)
        return 1

    print(f"Exported {len(records)} records in {fmt} format.")
    return 0


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Abatement Data Crawler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")

    # crawl sub-command
    crawl_parser = subparsers.add_parser("crawl", help="Run the crawler")
    crawl_parser.add_argument("--config", required=True, help="Path to config YAML")
    crawl_parser.add_argument(
        "--mode",
        choices=["seed", "search", "pipeline"],
        default="search",
        help="Crawl mode (default: search)",
    )
    crawl_parser.add_argument(
        "--seed-urls", nargs="*", help="Seed URLs for seed mode"
    )
    crawl_parser.add_argument(
        "--sector",
        help="Sector name for pipeline mode (overrides config pipeline.sector and scope.industry)",
    )
    crawl_parser.add_argument(
        "--fresh",
        action="store_true",
        help="Clear the URL cache before crawling so all URLs are revisited",
    )

    # web sub-command
    web_parser = subparsers.add_parser("web", help="Launch the web UI")
    web_parser.add_argument(
        "--config",
        default="./config/config.yaml",
        help="Path to config YAML (default: ./config/config.yaml)",
    )
    web_parser.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    web_parser.add_argument("--port", type=int, default=5000, help="Port (default: 5000)")
    web_parser.add_argument("--debug", action="store_true", help="Enable Flask debug mode")

    # export sub-command
    export_parser = subparsers.add_parser("export", help="Export records from the database")
    export_parser.add_argument("--config", required=True, help="Path to config YAML")
    export_parser.add_argument(
        "--format",
        choices=["jsonl", "csv", "parquet", "markdown"],
        default="jsonl",
        help="Output format (default: jsonl)",
    )
    export_parser.add_argument(
        "--min-quality",
        type=float,
        default=0.3,
        help="Minimum quality score for export (default: 0.3)",
    )

    # sessions sub-command
    sessions_parser = subparsers.add_parser(
        "sessions", help="List past crawl sessions"
    )
    sessions_parser.add_argument("--config", required=True, help="Path to config YAML")

    # captcha-queue sub-command
    cq_parser = subparsers.add_parser(
        "captcha-queue", help="Manage captcha-blocked URLs"
    )
    cq_parser.add_argument("--config", required=True, help="Path to config YAML")
    cq_parser.add_argument(
        "--status",
        choices=["pending", "resolved", "skipped"],
        help="Filter queue by status",
    )
    cq_parser.add_argument(
        "--resolve", metavar="URL", help="Mark a URL as resolved"
    )
    cq_parser.add_argument(
        "--skip", metavar="URL", help="Mark a URL as skipped"
    )

    # applicable-categories sub-command
    ac_parser = subparsers.add_parser(
        "applicable-categories",
        help="List abatement categories applicable to a sector, process, or asset type",
    )
    ac_parser.add_argument("--config", required=True, help="Path to config YAML")
    ac_parser.add_argument("--sector", default="", help="Sector (e.g. 'steel manufacturing')")
    ac_parser.add_argument("--process", default="", help="Process (e.g. 'electric arc furnace')")
    ac_parser.add_argument("--asset-type", default="", dest="asset_type", help="Asset type (e.g. 'furnace')")

    # seed sub-command
    seed_parser = subparsers.add_parser(
        "seed", help="Generate LLM-only seed records (one per taxonomy category, no crawl)"
    )
    seed_parser.add_argument("--config", required=True, help="Path to config YAML")
    seed_parser.add_argument(
        "--categories",
        metavar="SLUG,SLUG,...",
        help="Comma-separated category slugs to seed (default: all 13)",
    )

    args = parser.parse_args()

    if args.command == "web":
        sys.exit(_cmd_web(args))
    elif args.command == "crawl":
        sys.exit(_cmd_crawl(args))
    elif args.command == "seed":
        sys.exit(_cmd_seed(args))
    elif args.command == "export":
        sys.exit(_cmd_export(args))
    elif args.command == "sessions":
        sys.exit(_cmd_sessions(args))
    elif args.command == "captcha-queue":
        sys.exit(_cmd_captcha_queue(args))
    elif args.command == "applicable-categories":
        sys.exit(_cmd_applicable_categories(args))
    else:
        parser.print_help()
        sys.exit(0)


if __name__ == "__main__":
    main()
