#!/usr/bin/env python3
"""
Unified DC Data Pipeline - Complete ETL workflow in a single repository.

This script orchestrates the complete data workflow:
1. Scrape: Fetch data from DC government APIs and other sources
2. Transform: Build bronze -> silver -> gold -> diamond layers
3. Load: Load data into PostgreSQL warehouse

Usage:
    python scripts/unified_pipeline.py                     # Run full pipeline
    python scripts/unified_pipeline.py --stage scrape      # Run only scraping
    python scripts/unified_pipeline.py --stage transform   # Run only transformations
    python scripts/unified_pipeline.py --stage load        # Run only warehouse loading
    python scripts/unified_pipeline.py --sources dc_contracts,dc_payments
    python scripts/unified_pipeline.py --dry-run
"""

from __future__ import annotations

import argparse
import importlib
import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

console = Console()


def setup_logging(level: str = "INFO") -> None:
    """Configure logging with rich handler."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True, console=console)],
    )


@dataclass
class StageResult:
    """Result from a pipeline stage."""

    name: str
    status: Literal["success", "failed", "skipped"] = "skipped"
    started_at: datetime = field(default_factory=datetime.now)
    completed_at: datetime | None = None
    records_processed: int = 0
    error: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def duration_seconds(self) -> float:
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return 0.0


@dataclass
class PipelineConfig:
    """Configuration for the unified pipeline."""

    # Stages to run
    stages: list[str] = field(default_factory=lambda: ["scrape", "transform", "load"])

    # Scraper settings
    sources: list[str] | None = None
    incremental: bool = False
    max_records: int | None = None
    scraper_workers: int = 3
    scraper_timeout: int = 3600

    # Transform settings
    transform_layers: list[str] = field(
        default_factory=lambda: ["silver", "gold", "diamond"]
    )
    force_rebuild: bool = False
    validate_contracts: bool = True

    # Warehouse settings
    load_dimensions: bool = True
    load_facts: bool = True
    load_gold: bool = True
    load_diamond: bool = True
    truncate_tables: bool = False

    # General settings
    dry_run: bool = False
    verbose: bool = False
    log_level: str = "INFO"

    # Paths
    project_root: Path = field(default_factory=lambda: PROJECT_ROOT)
    data_path: Path = field(default_factory=lambda: PROJECT_ROOT / "data")


class UnifiedPipeline:
    """Orchestrates the complete DC data pipeline."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.results: list[StageResult] = []
        self.logger = logging.getLogger("unified_pipeline")

    def run(self) -> list[StageResult]:
        """Run the configured pipeline stages."""
        console.print(
            Panel(
                "[bold blue]DC Data Platform - Unified Pipeline[/bold blue]\n"
                f"Stages: {', '.join(self.config.stages)}",
                border_style="blue",
            )
        )

        if self.config.dry_run:
            console.print("[yellow]DRY RUN - no changes will be made[/yellow]\n")

        start_time = time.time()

        for stage in self.config.stages:
            if stage == "scrape":
                result = self._run_scrape_stage()
            elif stage == "transform":
                result = self._run_transform_stage()
            elif stage == "load":
                result = self._run_load_stage()
            else:
                self.logger.warning(f"Unknown stage: {stage}")
                continue

            self.results.append(result)

            # Stop on failure unless dry run
            if result.status == "failed" and not self.config.dry_run:
                console.print(f"[red]Stage '{stage}' failed, stopping pipeline[/red]")
                break

        total_duration = time.time() - start_time
        self._print_summary(total_duration)

        return self.results

    def _run_scrape_stage(self) -> StageResult:
        """Run the scraping stage."""
        result = StageResult(name="scrape")
        console.print("\n[bold cyan]Stage 1: Scraping Data[/bold cyan]")

        if self.config.dry_run:
            console.print("  [yellow]Would run scrapers for sources:[/yellow]")
            sources = self.config.sources or ["all enabled"]
            for s in sources:
                console.print(f"    - {s}")
            result.status = "success"
            result.completed_at = datetime.now()
            return result

        try:
            # Import scraper components
            from src.core.orchestrator import Orchestrator, OrchestratorConfig
            from src.storage.writer import StorageConfig

            # Import adapters to register them
            importlib.import_module("src.adapters.dc_arcgis")
            importlib.import_module("src.adapters.demographics")
            importlib.import_module("src.adapters.real_estate")

            # Configure
            storage_config = StorageConfig(lakehouse_root=self.config.data_path)

            orchestrator_config = OrchestratorConfig(
                max_concurrent_sources=self.config.scraper_workers,
                max_records_per_source=self.config.max_records,
                storage_config=storage_config,
                source_timeout_seconds=self.config.scraper_timeout,
            )

            orchestrator = Orchestrator(orchestrator_config)

            # Run scrapers
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                TimeElapsedColumn(),
                console=console,
            ) as progress:
                task = progress.add_task("Running scrapers...", total=None)

                scraper_results = orchestrator.run_all(
                    self.config.sources, incremental=self.config.incremental
                )

                progress.update(task, completed=True)

            # Collect stats
            total_records = sum(
                r.report.records_validated for r in scraper_results if r.report
            )
            failed_sources = sum(1 for r in scraper_results if r.status.value == "failed")

            result.records_processed = total_records
            result.details = {
                "sources_run": len(scraper_results),
                "sources_failed": failed_sources,
                "records_validated": total_records,
            }

            if failed_sources > 0:
                result.status = (
                    "failed" if failed_sources == len(scraper_results) else "success"
                )
                result.error = f"{failed_sources} sources failed"
            else:
                result.status = "success"

            console.print(
                f"  [green]Scraped {total_records:,} records from {len(scraper_results)} sources[/green]"
            )

        except ImportError as e:
            result.status = "failed"
            result.error = f"Import error: {e}"
            console.print(f"  [red]Error: {result.error}[/red]")
        except Exception as e:
            result.status = "failed"
            result.error = str(e)
            console.print(f"  [red]Error: {e}[/red]")
            self.logger.exception("Scrape stage failed")

        result.completed_at = datetime.now()
        return result

    def _run_transform_stage(self) -> StageResult:
        """Run the transformation stage."""
        result = StageResult(name="transform")
        console.print("\n[bold cyan]Stage 2: Transforming Data[/bold cyan]")

        if self.config.dry_run:
            console.print("  [yellow]Would build layers:[/yellow]")
            for layer in self.config.transform_layers:
                console.print(f"    - {layer}")
            result.status = "success"
            result.completed_at = datetime.now()
            return result

        try:
            from config.settings import Settings
            from pipelines.tasks.build_diamond import build_diamond_layer_direct
            from pipelines.tasks.build_gold import build_gold_layer_direct
            from pipelines.tasks.build_silver import build_silver_layer_direct

            settings = Settings()
            layer_records = {}

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                TimeElapsedColumn(),
                console=console,
            ) as progress:
                for layer in self.config.transform_layers:
                    task = progress.add_task(f"Building {layer} layer...", total=None)

                    if layer == "silver":
                        counts = build_silver_layer_direct(settings, self.config.sources)
                    elif layer == "gold":
                        counts = build_gold_layer_direct(settings, None)
                    elif layer == "diamond":
                        counts = build_diamond_layer_direct(settings, None)
                    else:
                        continue

                    if isinstance(counts, dict):
                        # Sum only numeric values, ignore strings like "exists" or "discovered"
                        layer_records[layer] = sum(
                            v for v in counts.values() if isinstance(v, (int, float))
                        )
                    else:
                        layer_records[layer] = 0
                    progress.update(task, completed=True)

            total_records = sum(layer_records.values())
            result.records_processed = total_records
            result.details = {"layer_records": layer_records}
            result.status = "success"

            for layer, count in layer_records.items():
                console.print(f"  [green]{layer}: {count:,} records[/green]")

        except ImportError as e:
            result.status = "failed"
            result.error = f"Import error: {e}"
            console.print(f"  [red]Error: {result.error}[/red]")
        except Exception as e:
            result.status = "failed"
            result.error = str(e)
            console.print(f"  [red]Error: {e}[/red]")
            self.logger.exception("Transform stage failed")

        result.completed_at = datetime.now()
        return result

    def _run_load_stage(self) -> StageResult:
        """Run the warehouse loading stage."""
        result = StageResult(name="load")
        console.print("\n[bold cyan]Stage 3: Loading Warehouse[/bold cyan]")

        if self.config.dry_run:
            console.print("  [yellow]Would load:[/yellow]")
            if self.config.load_dimensions:
                console.print("    - Dimension tables")
            if self.config.load_facts:
                console.print("    - Fact tables")
            if self.config.load_gold:
                console.print("    - Gold tables")
            if self.config.load_diamond:
                console.print("    - Diamond tables")
            result.status = "success"
            result.completed_at = datetime.now()
            return result

        try:
            from warehouse.config import config as wh_config
            from warehouse.loaders import (
                DimContractLoader,
                DimGeographyLoader,
                DimSupplierLoader,
                FactSpendLoader,
                GoldTableLoader,
            )

            load_results = {}
            total_loaded = 0

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                TimeElapsedColumn(),
                console=console,
            ) as progress:
                # Load dimensions
                if self.config.load_dimensions:
                    for name, LoaderClass in [
                        ("dim_supplier", DimSupplierLoader),
                        ("dim_contract", DimContractLoader),
                        ("dim_geography", DimGeographyLoader),
                    ]:
                        task = progress.add_task(f"Loading {name}...", total=None)
                        loader = LoaderClass()
                        lr = loader.run()
                        load_results[name] = lr
                        total_loaded += lr.get("total", lr.get("rows_loaded", 0))
                        progress.update(task, completed=True)

                # Load facts
                if self.config.load_facts:
                    task = progress.add_task("Loading fact_spend...", total=None)
                    loader = FactSpendLoader()
                    lr = loader.run()
                    load_results["fact_spend"] = lr
                    total_loaded += lr.get("total", lr.get("rows_loaded", 0))
                    progress.update(task, completed=True)

                # Load gold tables
                if self.config.load_gold:
                    gold_loader = GoldTableLoader()
                    for name in gold_loader.GOLD_MAPPINGS.keys():
                        task = progress.add_task(f"Loading gold_{name}...", total=None)
                        lr = gold_loader.load_dataset(name)
                        load_results[f"gold_{name}"] = lr
                        total_loaded += lr.get("total", lr.get("rows_loaded", 0))
                        progress.update(task, completed=True)

            result.records_processed = total_loaded
            result.details = {"tables_loaded": load_results}
            result.status = "success"

            console.print(
                f"  [green]Loaded {total_loaded:,} rows into {len(load_results)} tables[/green]"
            )

        except ImportError as e:
            result.status = "failed"
            result.error = f"Import error: {e}"
            console.print(f"  [red]Error: {result.error}[/red]")
        except Exception as e:
            result.status = "failed"
            result.error = str(e)
            console.print(f"  [red]Error: {e}[/red]")
            self.logger.exception("Load stage failed")

        result.completed_at = datetime.now()
        return result

    def _print_summary(self, total_duration: float) -> None:
        """Print pipeline execution summary."""
        console.print("\n")

        table = Table(title="Pipeline Execution Summary", border_style="blue")
        table.add_column("Stage", style="cyan")
        table.add_column("Status", justify="center")
        table.add_column("Duration", justify="right")
        table.add_column("Records", justify="right")
        table.add_column("Error")

        for result in self.results:
            status_style = {
                "success": "[green]SUCCESS[/green]",
                "failed": "[red]FAILED[/red]",
                "skipped": "[yellow]SKIPPED[/yellow]",
            }.get(result.status, result.status)

            table.add_row(
                result.name,
                status_style,
                f"{result.duration_seconds:.1f}s",
                f"{result.records_processed:,}" if result.records_processed else "-",
                result.error[:40] if result.error else "-",
            )

        console.print(table)

        # Overall status
        failed = sum(1 for r in self.results if r.status == "failed")
        success = sum(1 for r in self.results if r.status == "success")

        if failed == 0:
            console.print(
                f"\n[bold green]Pipeline completed successfully in {total_duration:.1f}s[/bold green]"
            )
        else:
            console.print(
                f"\n[bold red]Pipeline failed ({failed} stages failed, {success} succeeded)[/bold red]"
            )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Unified DC Data Pipeline - Scrape, Transform, Load",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run full pipeline
    python scripts/unified_pipeline.py

    # Run only scraping
    python scripts/unified_pipeline.py --stage scrape

    # Run specific sources
    python scripts/unified_pipeline.py --sources dc_contracts,dc_payments

    # Dry run to see what would happen
    python scripts/unified_pipeline.py --dry-run

    # Run incrementally (use checkpoints)
    python scripts/unified_pipeline.py --incremental
        """,
    )

    # Stage selection
    parser.add_argument(
        "--stage",
        type=str,
        action="append",
        dest="stages",
        choices=["scrape", "transform", "load"],
        help="Stage(s) to run (can specify multiple). Default: all",
    )

    # Scraper options
    parser.add_argument(
        "--sources",
        type=str,
        help="Comma-separated list of sources to scrape (default: all enabled)",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Use checkpoints for incremental loading",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        help="Maximum records per source",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=3,
        help="Maximum concurrent scraper workers (default: 3)",
    )

    # Transform options
    parser.add_argument(
        "--layers",
        type=str,
        default="silver,gold,diamond",
        help="Comma-separated layers to build (default: silver,gold,diamond)",
    )
    parser.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Force rebuild even if up to date",
    )

    # Load options
    parser.add_argument(
        "--skip-dimensions",
        action="store_true",
        help="Skip loading dimension tables",
    )
    parser.add_argument(
        "--skip-facts",
        action="store_true",
        help="Skip loading fact tables",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate tables before loading",
    )

    # General options
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without executing",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose output",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level (default: INFO)",
    )

    return parser.parse_args()


def main() -> int:
    """Main entry point."""
    args = parse_args()

    # Setup logging
    log_level = "DEBUG" if args.verbose else args.log_level
    setup_logging(log_level)

    # Build config
    config = PipelineConfig(
        stages=args.stages or ["scrape", "transform", "load"],
        sources=[s.strip() for s in args.sources.split(",")] if args.sources else None,
        incremental=args.incremental,
        max_records=args.max_records,
        scraper_workers=args.workers,
        transform_layers=[l.strip() for l in args.layers.split(",")],
        force_rebuild=args.force_rebuild,
        load_dimensions=not args.skip_dimensions,
        load_facts=not args.skip_facts,
        truncate_tables=args.truncate,
        dry_run=args.dry_run,
        verbose=args.verbose,
        log_level=log_level,
    )

    # Run pipeline
    pipeline = UnifiedPipeline(config)
    results = pipeline.run()

    # Return exit code based on results
    failed = sum(1 for r in results if r.status == "failed")
    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
