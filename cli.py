"""Factory CLI - orchestration and governance commands."""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(
    name="factory",
    help="DC Data Platform - Orchestration & Governance",
    add_completion=False,
)
console = Console()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("factory")


def get_settings():
    """Load configuration settings."""
    from config.settings import Settings

    return Settings()


@app.command()
def status():
    """Show platform status and path availability."""
    settings = get_settings()

    console.print("[bold blue]DC Data Platform Status[/bold blue]\n")

    # Check paths
    paths = [
        ("Scrapers Unified", settings.scrapers_unified_path),
        ("Warehouse", settings.warehouse_path),
        ("Bronze Data", settings.bronze_path),
        ("Silver Data", settings.silver_path),
        ("Gold Data", settings.gold_path),
        ("Diamond Data", settings.diamond_path),
    ]

    table = Table(title="Path Status")
    table.add_column("Component")
    table.add_column("Path")
    table.add_column("Status")

    for name, path in paths:
        exists = path.exists() if path else False
        status_icon = "[green]Available[/green]" if exists else "[red]Not Found[/red]"
        table.add_row(name, str(path) if path else "Not configured", status_icon)

    console.print(table)

    # Show environment
    console.print(f"\n[bold]Environment:[/bold] {settings.environment}")
    console.print(f"[bold]Log Level:[/bold] {settings.log_level}")


@app.command()
def build_gold(
    datasets: Optional[list[str]] = typer.Option(
        None,
        "--dataset",
        "-d",
        help="Specific datasets to build",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Force rebuild even if up to date",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Show verbose output",
    ),
):
    """Build gold layer from silver data."""
    from pipelines.tasks.build_gold import build_gold_layer_direct

    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    settings = get_settings()

    console.print("[bold blue]Building Gold Layer[/bold blue]")

    try:
        record_counts = build_gold_layer_direct(settings, datasets)

        table = Table(title="Gold Layer Build Results")
        table.add_column("Dataset")
        table.add_column("Records", justify="right")

        for ds, count in record_counts.items():
            count_str = f"{count:,}" if isinstance(count, int) else str(count)
            table.add_row(ds, count_str)

        console.print(table)
        console.print("[green]Gold layer build complete[/green]")

    except Exception as e:
        console.print(f"[red]Gold layer build failed: {e}[/red]")
        sys.exit(1)


@app.command()
def check_quality(
    layer: str = typer.Option(
        "gold",
        "--layer",
        "-l",
        help="Layer to check: silver, gold, diamond",
    ),
    dataset: Optional[str] = typer.Option(
        None,
        "--dataset",
        "-d",
        help="Specific dataset to check",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Show verbose output",
    ),
):
    """Run quality checks on data layer."""
    from metrics.collectors.parquet_stats import collect_parquet_metrics

    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    settings = get_settings()

    console.print(f"[bold blue]Quality Check: {layer} layer[/bold blue]")

    # Determine path
    layer_paths = {
        "bronze": settings.bronze_path,
        "silver": settings.silver_path,
        "gold": settings.gold_path,
        "diamond": settings.diamond_path,
    }

    base_path = layer_paths.get(layer)
    if not base_path:
        console.print(f"[red]Unknown layer: {layer}[/red]")
        sys.exit(1)

    if not base_path.exists():
        console.print(f"[red]Path not found: {base_path}[/red]")
        sys.exit(1)

    # Find datasets
    if dataset:
        datasets = [base_path / dataset]
    else:
        datasets = [d for d in base_path.iterdir() if d.is_dir()]

    table = Table(title=f"{layer.title()} Layer Quality Report")
    table.add_column("Dataset")
    table.add_column("Rows", justify="right")
    table.add_column("Files", justify="right")
    table.add_column("Size", justify="right")
    table.add_column("Max Null Rate", justify="right")
    table.add_column("Status")

    for ds_path in sorted(datasets):
        if not ds_path.exists():
            console.print(f"[yellow]Dataset not found: {ds_path.name}[/yellow]")
            continue

        metrics = collect_parquet_metrics(ds_path, ds_path.name)

        if metrics:
            max_null = metrics.max_null_rate
            if max_null < 0.1:
                status = "[green]Good[/green]"
            elif max_null < 0.3:
                status = "[yellow]Warning[/yellow]"
            else:
                status = "[red]High Nulls[/red]"

            # Format size
            if metrics.size_mb >= 1:
                size_str = f"{metrics.size_mb:.1f} MB"
            else:
                size_str = f"{metrics.total_bytes / 1024:.1f} KB"

            table.add_row(
                ds_path.name,
                f"{metrics.row_count:,}",
                str(metrics.file_count),
                size_str,
                f"{max_null:.1%}",
                status,
            )
        else:
            table.add_row(ds_path.name, "-", "-", "-", "-", "[red]Error[/red]")

    console.print(table)


@app.command()
def run_pipeline(
    pipeline: str = typer.Argument(
        "full_refresh",
        help="Pipeline to run: full_refresh, incremental, gold_only",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show what would be executed without running",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Show verbose output",
    ),
):
    """Run a pipeline definition."""
    from pipelines.adapters.local import LocalAdapter

    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    settings = get_settings()
    adapter = LocalAdapter(settings)

    console.print(f"[bold blue]Running Pipeline: {pipeline}[/bold blue]")

    if dry_run:
        console.print("[yellow]DRY RUN - no changes will be made[/yellow]\n")

    try:
        result = adapter.run_pipeline(pipeline, dry_run=dry_run)

        # Show results
        table = Table(title="Pipeline Execution Results")
        table.add_column("Task")
        table.add_column("Status")
        table.add_column("Duration")
        table.add_column("Error")

        for task_result in result.task_results:
            status_color = {
                "success": "green",
                "failed": "red",
                "skipped": "yellow",
            }.get(task_result.status.value, "white")

            duration = (
                f"{task_result.duration_seconds:.1f}s"
                if task_result.duration_seconds
                else "-"
            )

            table.add_row(
                task_result.task_name,
                f"[{status_color}]{task_result.status.value}[/{status_color}]",
                duration,
                task_result.error[:50] if task_result.error else "-",
            )

        console.print(table)

        if result.status.value == "success":
            console.print(
                f"\n[green]Pipeline completed successfully in "
                f"{result.duration_seconds:.1f}s[/green]"
            )
        else:
            console.print(f"\n[red]Pipeline failed: {result.error}[/red]")
            sys.exit(1)

    except FileNotFoundError as e:
        console.print(f"[red]Pipeline not found: {e}[/red]")
        console.print(f"Available pipelines: {', '.join(adapter.list_pipelines())}")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Pipeline execution failed: {e}[/red]")
        sys.exit(1)


@app.command()
def list_pipelines():
    """List available pipeline definitions."""
    from pipelines.adapters.local import LocalAdapter

    settings = get_settings()
    adapter = LocalAdapter(settings)

    pipelines = adapter.list_pipelines()

    console.print("[bold blue]Available Pipelines[/bold blue]\n")

    for name in sorted(pipelines):
        console.print(f"  - {name}")


@app.command()
def validate_contracts(
    layer: Optional[str] = typer.Option(
        None,
        "--layer",
        "-l",
        help="Layer to validate: silver, gold, diamond",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Show verbose output",
    ),
):
    """Validate data against contracts."""
    from contracts.base import DataContract, validate_data_against_contract

    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    settings = get_settings()

    console.print("[bold blue]Validating Data Contracts[/bold blue]\n")

    fabric_base = Path(__file__).parent
    contracts_base = fabric_base / "contracts" / "schemas"

    layers = [layer] if layer else ["silver", "gold", "diamond"]

    table = Table(title="Contract Validation Results")
    table.add_column("Layer")
    table.add_column("Dataset")
    table.add_column("Valid")
    table.add_column("Errors")
    table.add_column("Warnings")

    for layer_name in layers:
        layer_path = getattr(settings, f"{layer_name}_path", None)
        contracts_path = contracts_base / layer_name

        if not layer_path or not layer_path.exists():
            continue

        if not contracts_path.exists():
            continue

        for contract_file in contracts_path.glob("*.yaml"):
            contract = DataContract.from_yaml(contract_file)
            data_path = layer_path / contract.name

            if not data_path.exists():
                table.add_row(
                    layer_name,
                    contract.name,
                    "[yellow]N/A[/yellow]",
                    "Data not found",
                    "-",
                )
                continue

            result = validate_data_against_contract(data_path, contract)

            valid_str = "[green]Yes[/green]" if result.valid else "[red]No[/red]"
            errors = str(len(result.errors)) if result.errors else "-"
            warnings = str(len(result.warnings)) if result.warnings else "-"

            table.add_row(layer_name, contract.name, valid_str, errors, warnings)

            if verbose and (result.errors or result.warnings):
                for err in result.errors:
                    console.print(f"  [red]ERROR:[/red] {err}")
                for warn in result.warnings:
                    console.print(f"  [yellow]WARN:[/yellow] {warn}")

    console.print(table)


@app.command()
def generate_report(
    report_type: str = typer.Option(
        "full",
        "--type",
        "-t",
        help="Report type: quality, freshness, full",
    ),
    output: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="Output file path (JSON or HTML based on extension)",
    ),
    html: bool = typer.Option(
        False,
        "--html",
        help="Generate HTML report",
    ),
):
    """Generate data quality and freshness reports."""
    from metrics.reports.generator import (
        export_report_html,
        export_report_json,
        generate_freshness_report,
        generate_full_report,
        generate_quality_report,
    )

    settings = get_settings()

    console.print(f"[bold blue]Generating {report_type} Report[/bold blue]\n")

    report_funcs = {
        "quality": generate_quality_report,
        "freshness": generate_freshness_report,
        "full": generate_full_report,
    }

    if report_type not in report_funcs:
        console.print(f"[red]Unknown report type: {report_type}[/red]")
        sys.exit(1)

    report = report_funcs[report_type](settings)

    if output:
        if output.suffix == ".html" or html:
            export_report_html(report, output)
        else:
            export_report_json(report, output)
        console.print(f"[green]Report saved to {output}[/green]")
    else:
        # Print summary to console
        console.print(json.dumps(report.get("summary", report), indent=2))


@app.command()
def version():
    """Show factory version information."""
    console.print("[bold]DC Data Platform - Factory Layer[/bold]")
    console.print("Version: 0.1.0")
    console.print("Python: " + sys.version.split()[0])


def main():
    """Main entry point."""
    app()


if __name__ == "__main__":
    main()
