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

    config = CrawlerConfig.from_yaml(args.config)
    crawler = AbatementCrawler(config)

    if args.mode == "seed":
        if not args.seed_urls:
            print("Error: --seed-urls required for seed mode.", file=sys.stderr)
            return 1
        stats = crawler.run_seed_mode(args.seed_urls)
    else:
        stats = crawler.run_search_mode()

    print(f"Crawl complete: {stats}")
    return 0


def _cmd_web(args: argparse.Namespace) -> int:
    from .web.app import create_app  # noqa: PLC0415

    app = create_app(config_path=args.config)
    app.run(host=args.host, port=args.port, debug=args.debug)
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
        choices=["seed", "search"],
        default="search",
        help="Crawl mode (default: search)",
    )
    crawl_parser.add_argument(
        "--seed-urls", nargs="*", help="Seed URLs for seed mode"
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

    args = parser.parse_args()

    if args.command == "web":
        sys.exit(_cmd_web(args))
    elif args.command == "crawl":
        sys.exit(_cmd_crawl(args))
    elif args.command == "export":
        sys.exit(_cmd_export(args))
    else:
        parser.print_help()
        sys.exit(0)


if __name__ == "__main__":
    main()
