"""Export AbatementRecord collections to various formats."""

from __future__ import annotations

import csv
import json
import logging
import os
from pathlib import Path
from typing import Any

from .models import AbatementRecord, ScopeConfig

logger = logging.getLogger(__name__)


class Exporter:
    """Exports AbatementRecord collections to JSONL, CSV, Parquet, and Markdown."""

    def __init__(self, output_dir: str) -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, filename: str) -> Path:
        return self.output_dir / filename

    def export_jsonl(
        self, records: list[AbatementRecord], filename: str = "records.jsonl"
    ) -> None:
        """Write records as newline-delimited JSON."""
        path = self._path(filename)
        with open(path, "w", encoding="utf-8") as f:
            for record in records:
                f.write(record.model_dump_json() + "\n")
        logger.info("Exported %d records to %s", len(records), path)

    def export_csv(
        self, records: list[AbatementRecord], filename: str = "records.csv"
    ) -> None:
        """Write records as CSV."""
        if not records:
            logger.warning("No records to export as CSV.")
            return

        path = self._path(filename)
        fieldnames = list(records[0].model_fields.keys())

        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for record in records:
                row = record.model_dump()
                # Flatten list fields to semicolon-separated strings
                for k, v in row.items():
                    if isinstance(v, list):
                        row[k] = "; ".join(str(x) for x in v)
                writer.writerow(row)
        logger.info("Exported %d records to %s", len(records), path)

    def export_parquet(
        self, records: list[AbatementRecord], filename: str = "records.parquet"
    ) -> None:
        """Write records as Parquet using pandas + pyarrow."""
        try:
            import pandas as pd  # noqa: PLC0415
        except ImportError:
            logger.error("pandas is required for Parquet export")
            return

        if not records:
            logger.warning("No records to export as Parquet.")
            return

        path = self._path(filename)
        rows = []
        for record in records:
            row = record.model_dump()
            # Flatten lists to strings for Parquet compatibility
            for k, v in row.items():
                if isinstance(v, list):
                    row[k] = json.dumps(v)
            rows.append(row)

        df = pd.DataFrame(rows)
        df.to_parquet(path, index=False)
        logger.info("Exported %d records to %s", len(records), path)

    def export_markdown_report(
        self,
        records: list[AbatementRecord],
        scope: ScopeConfig,
        filename: str = "report.md",
    ) -> str:
        """Generate a coverage gap report in Markdown.

        Returns the Markdown string and writes it to file.
        """
        lines: list[str] = []

        lines.append("# Abatement Data Coverage Report\n")

        # Summary statistics
        lines.append("## Summary\n")
        lines.append(f"- **Total records**: {len(records)}")

        if records:
            avg_quality = sum(r.quality_score for r in records) / len(records)
            lines.append(f"- **Average quality score**: {avg_quality:.2f}")
            with_mac = sum(1 for r in records if r.mac is not None)
            lines.append(f"- **Records with MAC data**: {with_mac} ({100*with_mac//len(records)}%)")
            with_capex = sum(1 for r in records if r.capex is not None)
            lines.append(f"- **Records with CAPEX data**: {with_capex} ({100*with_capex//len(records)}%)")
        lines.append("")

        # Scope
        lines.append("## Scope Configuration\n")
        if scope.industry:
            lines.append(f"- **Industry**: {scope.industry}")
        if scope.sectors:
            lines.append(f"- **Sectors**: {', '.join(scope.sectors)}")
        if scope.geography:
            lines.append(f"- **Geographies**: {', '.join(scope.geography)}")
        if scope.abatement_types:
            lines.append(f"- **Abatement types**: {', '.join(scope.abatement_types)}")
        lines.append("")

        # Coverage by sector
        if records:
            lines.append("## Coverage by Sector\n")
            from collections import Counter  # noqa: PLC0415
            sector_counts = Counter(r.sector for r in records)
            lines.append("| Sector | Records |")
            lines.append("|--------|---------|")
            for sector, count in sector_counts.most_common():
                lines.append(f"| {sector} | {count} |")
            lines.append("")

            # Coverage gaps
            lines.append("## Coverage Gaps\n")
            wanted_sectors = set(scope.sectors)
            covered_sectors = {r.sector for r in records}
            missing = wanted_sectors - covered_sectors
            if missing:
                lines.append("### Missing sectors\n")
                for s in sorted(missing):
                    lines.append(f"- {s}")
            else:
                lines.append("All requested sectors have at least one record.")
            lines.append("")

            # Records without cost data
            no_cost = [r for r in records if r.capex is None and r.mac is None]
            if no_cost:
                lines.append(f"### Records without cost data: {len(no_cost)}\n")
                for r in no_cost[:10]:
                    lines.append(f"- {r.measure_name} ({r.sector}, {r.geography})")
                if len(no_cost) > 10:
                    lines.append(f"  … and {len(no_cost) - 10} more")
            lines.append("")

        report = "\n".join(lines)
        path = self._path(filename)
        with open(path, "w", encoding="utf-8") as f:
            f.write(report)
        logger.info("Coverage report written to %s", path)
        return report
